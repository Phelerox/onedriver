package graph

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/jstaf/onedriver/logger"
)

// these files will never exist, and we should ignore them
func ignore(path string) bool {
	ignoredFiles := []string{
		"/BDMV",
		"/.Trash",
		"/.Trash-1000",
		"/.xdg-volume-info",
		"/autorun.inf",
		"/.localized",
		"/.DS_Store",
		"/._.",
		"/.hidden",
	}
	for _, ignore := range ignoredFiles {
		if path == ignore {
			return true
		}
	}
	return false
}

func leadingSlash(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

// FuseFs is a memory-backed filesystem for Microsoft Graph
type FuseFs struct {
	pathfs.FileSystem
	*Auth
	items *Cache
}

// NewFS initializes a new Graph Filesystem to be used by go-fuse.
// Each method is executed concurrently as a goroutine.
func NewFS() *FuseFs {
	auth := Authenticate()
	cache := NewCache(auth)
	//go cache.deltaLoop() //TODO: disabled for now
	return &FuseFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		Auth:       auth,
		items:      cache,
	}
}

// OnUnmount runs when the filesystem is unmounted and performs any required
// cleanup.
func (fs *FuseFs) OnUnmount() {
	// close and delete the boltdb cache
	//TODO: examine keeping the boltdb cache for later/offline use
	fs.items.Close()
	os.Remove("onedriver.db")
}

// DriveQuota is used to parse the User's current storage quotas from the API
// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/resources/quota
type DriveQuota struct {
	Deleted   uint64 `json:"deleted"`   // bytes in recycle bin
	FileCount uint64 `json:"fileCount"` // unavailable on personal accounts
	Remaining uint64 `json:"remaining"`
	State     string `json:"state"` // normal | nearing | critical | exceeded
	Total     uint64 `json:"total"`
	Used      uint64 `json:"used"`
}

// Drive has some general information about the user's OneDrive
// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/resources/drive
type Drive struct {
	ID        string     `json:"id"`
	DriveType string     `json:"driveType"` // personal or business
	Quota     DriveQuota `json:"quota,omitempty"`
}

// StatFs returns information about the filesystem. Mainly useful for checking
// quotas and storage limits.
func (fs FuseFs) StatFs(name string) *fuse.StatfsOut {
	logger.Trace(leadingSlash(name))
	resp, err := Get("/me/drive", fs.Auth)
	if err != nil {
		logger.Error("Could not fetch filesystem details:", err)
	}
	drive := Drive{}
	json.Unmarshal(resp, &drive)

	if drive.DriveType == "personal" {
		logger.Warn("Personal OneDrive accounts do not show number of files, " +
			"inode counts reported by onedriver will be bogus.")
	}

	// limits are pasted from https://support.microsoft.com/en-us/help/3125202
	var blkSize uint64 = 4096 // default ext4 block size
	return &fuse.StatfsOut{
		Bsize:   uint32(blkSize),
		Blocks:  drive.Quota.Total / blkSize,
		Bfree:   drive.Quota.Remaining / blkSize,
		Bavail:  drive.Quota.Remaining / blkSize,
		Files:   100000,
		Ffree:   100000 - drive.Quota.FileCount,
		NameLen: 260,
	}
}

// GetAttr returns a stat structure for the specified file
func (fs *FuseFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	name = leadingSlash(name)
	if ignore(name) {
		return nil, fuse.ENOENT
	}

	item, err := fs.items.GetPath(name, fs.Auth)
	if err != nil || item == nil {
		// this is where non-existent files are caught - called before any other
		// method when accessing a file
		return nil, fuse.ENOENT
	}
	logger.Trace(name)

	attr := fuse.Attr{}
	status := item.GetAttr(&attr)

	return &attr, status
}

// Rename is used by mv operations (move, rename)
func (fs *FuseFs) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
	oldName, newName = leadingSlash(oldName), leadingSlash(newName)
	logger.Trace(oldName, "->", newName)

	// grab item being renamed
	item, _ := fs.items.GetPath(oldName, fs.Auth)
	id, err := item.RemoteID(fs.Auth)
	if isLocalID(id) || err != nil {
		// uploads will fail without an id
		logger.Error("ID of item to move cannot be local and we failed to obtain an ID:", err)
		return fuse.EBADF
	}

	// start creating patch content for server
	patchContent := DriveItem{ConflictBehavior: "replace"} // wipe existing content

	if newDir := filepath.Dir(newName); filepath.Dir(oldName) != newDir {
		// we are moving the item, add the new parent ID to the patch
		newParent, err := fs.items.GetPath(newDir, fs.Auth)
		if err != nil {
			logger.Errorf("Failed to fetch \"%s\": %s\n", newDir, err)
			return fuse.EREMOTEIO
		}
		parentID, err := newParent.RemoteID(fs.Auth)
		if isLocalID(parentID) || err != nil {
			logger.Error("ID of destination folder cannot be local:", err)
			return fuse.EBADF
		}
		patchContent.Parent = &DriveItemParent{ID: parentID}
	}

	if newBase := filepath.Base(newName); filepath.Base(oldName) != newBase {
		// we are renaming the item, add the new name to the patch
		// mutex for patchContent is uninitialized and we have the only copy
		patchContent.NameInternal = newBase
		item.SetName(newBase)
	}

	// apply patch to server copy - note that we don't actually care about the
	// response content
	jsonPatch, _ := json.Marshal(patchContent)
	_, err = Patch("/me/drive/items/"+id, fs.Auth, bytes.NewReader(jsonPatch))
	if err != nil {
		if strings.Contains(err.Error(), "resourceModified") {
			// Wait a second, then retry the request. The Onedrive servers
			// sometimes aren't quick enough here if the object has been
			// recently created (<1 second ago).
			time.Sleep(time.Second)
			logger.Warn("Patch failed, retrying:", err.Error())
			_, err = Patch("/me/drive/items/"+id, fs.Auth, bytes.NewReader(jsonPatch))
			if err != nil {
				// if retrying the request failed to recover things, or the request
				// failed due to another reason than the etag bug
				logger.Error(err)
				item.SetName(filepath.Base(oldName)) // unrename things locally
				return fuse.EREMOTEIO
			}
		}
	}

	// now rename local copy
	if err := fs.items.MovePath(oldName, newName, fs.Auth); err != nil {
		logger.Error("Failed to rename local item:", err)
		return fuse.EIO
	}
	return fuse.OK
}

// Chown currently does nothing - it is not a valid option, since fuse is single-user anyways
func (fs *FuseFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	return fuse.ENOSYS
}

// Chmod changes mode purely for convenience/compatibility - it has no effect on
// server contents (onedrive has no notion of permissions).
func (fs *FuseFs) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	name = leadingSlash(name)
	item, _ := fs.items.GetPath(name, fs.Auth)
	return item.Chmod(mode)
}

// OpenDir returns a list of directory entries
func (fs *FuseFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	name = leadingSlash(name)
	logger.Trace(name)

	children, err := fs.items.GetChildrenPath(name, fs.Auth)
	if err != nil {
		// not an item not found error (GetAttr() will always be called before
		// OpenDir()), something has happened to our connection
		logger.Errorf("Error during OpenDir(\"%s\"): %s\n", name, err)
		return nil, fuse.EREMOTEIO
	}

	entries := make([]fuse.DirEntry, 0)
	for _, child := range children {
		// mutex lock is unnecessary now that GetChildrenPath fetches a fresh
		// copy held by no one else
		entry := fuse.DirEntry{
			Name: child.Name(),
			Mode: child.Mode(),
		}
		entries = append(entries, entry)
	}
	return entries, fuse.OK
}

// Mkdir creates a directory, mode is ignored
func (fs *FuseFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	name = leadingSlash(name)
	logger.Trace(name)

	// create a new folder on the server
	newFolderPost := DriveItem{
		NameInternal: filepath.Base(name),
		Folder:       &Folder{},
	}
	bytePayload, _ := json.Marshal(newFolderPost)
	resp, err := Post(ChildrenPath(filepath.Dir(name)), fs.Auth, bytes.NewReader(bytePayload))
	if err != nil {
		logger.Error("Error during directory creation:", err)
		return fuse.EREMOTEIO
	}

	// create the new folder locally
	created, code := fs.Create(name, 0, mode|fuse.S_IFDIR, context)
	if code != fuse.OK {
		return code
	}

	// Now unmarshal the response into the new folder so that it has an ID
	// (otherwise things involving this folder will fail later). Mutexes are not
	// required here since no other thread will proceed until the directory has
	// been created.
	item := created.(*DriveItem)
	oldID := item.ID()
	json.Unmarshal(resp, item)

	// Move the directory to be stored under the non-local ID.
	//TODO: eliminate the need for renames after Create()
	fs.items.MoveID(oldID, item.ID())

	return fuse.OK
}

// Rmdir removes a directory
func (fs *FuseFs) Rmdir(name string, context *fuse.Context) fuse.Status {
	name = leadingSlash(name)
	logger.Trace(name)

	err := Delete(ResourcePath(name), fs.Auth)
	if err != nil {
		logger.Error("Error during delete:", err)
		return fuse.EREMOTEIO
	}

	fs.items.DeletePath(name)

	return fuse.OK
}

// Open populates a DriveItem's Data field with actual data
func (fs *FuseFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	name = leadingSlash(name)
	logger.Trace(name)

	item, err := fs.items.GetPath(name, fs.Auth)
	if err != nil {
		// We know the file exists, GetAttr() has already been called
		logger.Error("Error while getting item", err)
		return nil, fuse.EREMOTEIO
	}

	// check for if file has already been populated
	if item.content == nil {
		// it is unpopulated, grab from api
		if err != nil {
			logger.Errorf("Failed to fetch content for '%s': %s\n", item.ID(), err)
			return nil, fuse.EIO
		}
	}
	return item, fuse.OK
}

// Create a new local file. The server doesn't have this yet.
func (fs *FuseFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	name = leadingSlash(name)
	logger.Trace(name)

	// fetch details about the new item's parent (need the ID from the remote)
	parent, err := fs.items.GetPath(filepath.Dir(name), fs.Auth)
	if err != nil {
		logger.Error("Error while fetching parent:", err)
		return nil, fuse.EREMOTEIO
	}

	item := NewDriveItem(filepath.Base(name), mode, parent)
	logger.Tracef("Created \"%s\" as \"%s\"", name, item.ID())
	err = fs.items.InsertPath(name, fs.Auth, item)
	if err != nil {
		logger.Error(err)
	}
	return item, fuse.OK
}

// Unlink deletes a file
func (fs *FuseFs) Unlink(name string, context *fuse.Context) fuse.Status {
	name = leadingSlash(name)
	logger.Trace(name)

	item, err := fs.items.GetPath(name, fs.Auth)
	// allow safely calling Unlink on items that don't actually exist
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		return fuse.ENOENT
	}

	// if no ID, the item is local-only, and does not need to be deleted on the
	// server
	if !isLocalID(item.ID()) {
		err = Delete(ResourcePath(name), fs.Auth)
		if err != nil {
			logger.Error(err)
			return fuse.EREMOTEIO
		}
	}
	fs.items.DeletePath(name)

	return fuse.OK
}
