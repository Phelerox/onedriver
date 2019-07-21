package graph

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/jstaf/onedriver/logger"
)

// DriveItemParent describes a DriveItem's parent in the Graph API (just another
// DriveItem's ID and its path)
type DriveItemParent struct {
	//TODO Path is technically available, but we shouldn't use it
	Path string `json:"path,omitempty"`
	ID   string `json:"id,omitempty"`
}

// Folder is used for parsing only
type Folder struct {
	ChildCount uint32 `json:"childCount,omitempty"`
}

// File is used for parsing only
type File struct {
	MimeType string `json:"mimeType,omitempty"`
}

// Deleted is used for detecting when items get deleted on the server
type Deleted struct {
	State string `json:"state,omitempty"`
}

// DriveItem represents a file or folder fetched from the Graph API. All struct
// fields are pointers so as to avoid including them when marshaling to JSON
// if not present. Fields named "xxxxxInternal" should never be accessed, they
// are there for JSON umarshaling/marshaling only. This struct's methods are NOT
// thread-safe.
type DriveItem struct {
	nodefs.File      `json:"-"`
	cache            *Cache
	content          *DriveItemContent `json:"-"` // actual file contents
	uploadSession    *UploadSession    // current upload session, or nil
	hasChanges       bool              // used to trigger an upload on flush
	IDInternal       string            `json:"id,omitempty"`
	NameInternal     string            `json:"name,omitempty"`
	SizeInternal     uint64            `json:"size,omitempty"`
	ModTimeInternal  *time.Time        `json:"lastModifiedDatetime,omitempty"`
	mode             uint32            // do not set manually
	Parent           *DriveItemParent  `json:"parentReference,omitempty"`
	children         []string          // a slice of ids, nil when uninitialized
	subdir           uint32            // used purely by NLink()
	Folder           *Folder           `json:"folder,omitempty"`
	FileInternal     *File             `json:"file,omitempty"`
	Deleted          *Deleted          `json:"deleted,omitempty"`
	ConflictBehavior string            `json:"@microsoft.graph.conflictBehavior,omitempty"`
}

// NewDriveItem initializes a new DriveItem
func NewDriveItem(name string, mode uint32, parent *DriveItem) *DriveItem {
	itemParent := &DriveItemParent{ID: "", Path: ""}
	var cache *Cache
	if parent != nil {
		itemParent.ID = parent.IDInternal
		itemParent.Path = parent.Path()
		cache = parent.cache
	}

	currentTime := time.Now()
	return &DriveItem{
		File:            nodefs.NewDefaultFile(),
		IDInternal:      localID(),
		NameInternal:    name,
		cache:           cache, //TODO: find a way to do uploads without this field
		Parent:          itemParent,
		children:        make([]string, 0),
		ModTimeInternal: &currentTime,
		mode:            mode,
	}
}

// String is only used for debugging by go-fuse
func (d DriveItem) String() string {
	return d.Name()
}

// Name is used to ensure thread-safe access to the NameInternal field.
func (d DriveItem) Name() string {
	return d.NameInternal
}

// SetName sets the name of the item in a thread-safe manner.
func (d *DriveItem) SetName(name string) {
	d.NameInternal = name
}

var charset = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randString(length int) string {
	out := make([]byte, length)
	for i := 0; i < length; i++ {
		out[i] = charset[rand.Intn(len(charset))]
	}
	return string(out)
}

func localID() string {
	return "local-" + randString(20)
}

func isLocalID(id string) bool {
	return strings.HasPrefix(id, "local-") || id == ""
}

// ID returns the internal ID of the item
func (d *DriveItem) ID() string {
	return d.IDInternal
}

// RemoteID uploads an empty file to obtain a Onedrive ID if it doesn't already
// have one. This is necessary to avoid race conditions against uploads if the
// file has not already been uploaded. You can use an empty Auth object if
// you're sure that the item already has an ID or otherwise don't need to fetch
// an ID (such as when deleting an item that is only local).
//TODO: move this to cache methods, it's not needed here
func (d *DriveItem) RemoteID(auth *Auth) (string, error) {
	if d.IsDir() {
		// Due to the nature of how they are created, directories will always
		// have an ID.
		return d.IDInternal, nil
	}

	if isLocalID(d.IDInternal) && auth.AccessToken != "" {
		uploadPath := fmt.Sprintf("/me/drive/items/%s:/%s:/content", d.Parent.ID, d.Name())
		resp, err := Put(uploadPath, auth, strings.NewReader(""))
		if err != nil {
			if strings.Contains(err.Error(), "nameAlreadyExists") {
				// This likely got fired off just as an initial upload completed.
				// Check both our local copy and the server.

				// Do we have it (from another thread)?
				item, _ := d.cache.GetPath(d.Path(), &Auth{})
				if !isLocalID(item.ID()) {
					return item.ID(), nil
				}

				// Does the server have it?
				latest, err := GetItem(d.Path(), auth)
				if err == nil {
					// hooray!
					err := d.cache.MoveID(d.IDInternal, latest.IDInternal)
					return latest.IDInternal, err
				}
			}
			// failed to obtain an ID, return whatever it was beforehand
			return d.IDInternal, err
		}

		// we use a new DriveItem to unmarshal things into or it will fuck
		// with the existing object (namely its size)
		unsafe := NewDriveItem(d.Name(), 0644, nil)
		err = json.Unmarshal(resp, unsafe)
		if err != nil {
			return d.IDInternal, err
		}
		// this is all we really wanted from this transaction
		err = d.cache.MoveID(d.IDInternal, unsafe.IDInternal)
		return unsafe.IDInternal, err
	}
	return d.IDInternal, nil
}

// Path returns an item's full Path
func (d DriveItem) Path() string {
	// special case when it's the root item
	if d.Parent.ID == "" && d.Name() == "root" {
		return "/"
	}

	// all paths come prefixed with "/drive/root:"
	prepath := strings.TrimPrefix(d.Parent.Path+"/"+d.Name(), "/drive/root:")
	return strings.Replace(prepath, "//", "/", -1)
}

// GetAttr returns a the DriveItem as a UNIX stat. Holds the read mutex for all
// of the "metadata fetch" operations.
func (d DriveItem) GetAttr(out *fuse.Attr) fuse.Status {
	out.Size = d.Size()
	out.Nlink = d.NLink()
	out.Atime = d.ModTime()
	out.Mtime = d.ModTime()
	out.Ctime = d.ModTime()
	out.Mode = d.Mode()
	out.Owner = fuse.Owner{
		Uid: uint32(os.Getuid()),
		Gid: uint32(os.Getgid()),
	}
	return fuse.OK
}

// Utimens sets the access/modify times of a file
func (d *DriveItem) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	logger.Trace(d.Path())
	d.ModTimeInternal = mtime
	d.cache.InsertID(d.ID(), d)
	return fuse.OK
}

// IsDir returns if it is a directory (true) or file (false).
func (d DriveItem) IsDir() bool {
	// following statement returns 0 if the dir bit is not set
	return d.Mode()&fuse.S_IFDIR > 0
}

// Mode returns the permissions/mode of the file.
func (d DriveItem) Mode() uint32 {
	if d.mode == 0 { // only 0 if fetched from Graph API
		if d.FileInternal == nil { // nil if a folder
			d.mode = fuse.S_IFDIR | 0755
		} else {
			d.mode = fuse.S_IFREG | 0644
		}
	}
	return d.mode
}

// Chmod changes the mode of a file
func (d *DriveItem) Chmod(perms uint32) fuse.Status {
	logger.Trace(d.Path())
	if d.IsDir() {
		d.mode = fuse.S_IFDIR | perms
	} else {
		d.mode = fuse.S_IFREG | perms
	}
	d.cache.InsertID(d.ID(), d)
	return fuse.OK
}

// ModTime returns the Unix timestamp of last modification (to get a time.Time
// struct, use time.Unix(int64(d.ModTime()), 0))
func (d DriveItem) ModTime() uint64 {
	return uint64(d.ModTimeInternal.Unix())
}

// NLink gives the number of hard links to an inode (or child count if a
// directory)
func (d DriveItem) NLink() uint32 {
	if d.IsDir() {
		// we precompute d.subdir due to mutex lock contention with NLink and
		// other ops. d.subdir is modified by cache Insert/Delete and GetChildren.
		return 2 + d.subdir
	}
	return 1
}

// Size pretends that folders are 4096 bytes, even though they're 0 (since
// they actually don't exist).
func (d DriveItem) Size() uint64 {
	if d.IsDir() {
		return 4096
	}
	return d.SizeInternal
}
