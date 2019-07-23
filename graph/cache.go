package graph

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jstaf/onedriver/logger"
	bolt "go.etcd.io/bbolt"
)

// Cache caches DriveItems for a filesystem. This cache never expires so
// that local changes can persist. Should be created using the NewCache()
// constructor.
type Cache struct {
	*bolt.DB
	metadataName []byte   // boltdb bucket name for filesystem metadata
	contentName  []byte   // boltdb bucket name for inactive file content (as bytes)
	metadata     sync.Map // live file metadata
	content      sync.Map // live content for currently open files
	root         string   // the id of the filesystem's root item
	auth         *Auth
	deltaLink    string
}

// NewCache creates a new Cache
func NewCache(auth *Auth) *Cache {
	// initialize the boltdb instance used internally
	boltdb, err := bolt.Open("onedriver.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		logger.Fatal(err)
	}
	cache := &Cache{
		DB:           boltdb,
		metadataName: []byte("metadata"),
		contentName:  []byte("content"),
		auth:         auth,
	}
	// create buckets
	cache.DB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(cache.metadataName)
		tx.CreateBucketIfNotExists(cache.contentName)
		return nil
	})

	// add the root item to the cache
	root, err := GetItem("/", auth)
	if err != nil {
		logger.Fatal("Could not fetch root item of filesystem!:", err.Error())
	}
	root.cache = cache
	cache.root = root.ID()
	cache.InsertID(cache.root, root)

	// using token=latest because we don't care about existing items - they'll
	// be downloaded on-demand by the cache
	cache.deltaLink = "/me/drive/root/delta?token=latest"

	// deltaloop is started manually
	return cache
}

// GetID gets an item from the cache by ID. No fetching is performed. Result is
// nil if no item is found.
func (c *Cache) GetID(id string) *DriveItem {
	entry, exists := c.metadata.Load(id)
	if !exists {
		return nil
	}
	item := entry.(*DriveItem)
	return item
}

// InsertID inserts a single item into the cache by ID
func (c *Cache) InsertID(id string, item *DriveItem) {
	c.metadata.Store(id, item)
}

// DeleteID deletes an item from the cache
func (c *Cache) DeleteID(id string) {
	c.metadata.Delete(id)
}

// GetContentID fetches content from either the server, memory, or the
// file-backed cache
func (c *Cache) GetContentID(key string, auth *Auth) (*DriveItemContent, error) {
	// do we have it in the memory-backed cache?
	val, exists := c.content.Load(key)
	if exists {
		return val.(*DriveItemContent), nil
	}

	// do we have it on disk?
	found := false
	var content *DriveItemContent
	c.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.contentName)
		byteData := b.Get([]byte(key))
		if byteData != nil {
			found = true
			// must create copy, otherwise data is toast as soon as tx finishes
			cp := make([]byte, len(byteData))
			copy(cp, byteData)
			content = NewDriveItemContent(cp)
		}
		return nil
	})
	if found {
		c.InsertContentID(key, content)
		return content, nil
	}

	// do we have it on the server?
	item := c.GetID(key)
	if item == nil {
		// item not found locally
		return nil, errors.New("Metadata for item \"%s\" not found in cache")
	}
	id, err := item.RemoteID(auth)
	if err != nil {
		// error while swapping ids
		logger.Error("Could not obtain ID:", err.Error())
		return nil, err
	}
	logger.Info("Fetching remote content for", item.Name())
	body, err := Get("/me/drive/items/"+id+"/content", auth)
	if err != nil {
		// something went wrong with our get request
		return nil, err
	}
	// if we made it here, we got it from the server
	content = NewDriveItemContent(body)
	c.InsertContentID(key, content)
	return content, nil
}

// InsertContentID inserts content into the memory-backed cache, but not the db
func (c *Cache) InsertContentID(key string, content *DriveItemContent) {
	c.content.Store(key, content)
}

// FlushContentID removes content from the memory-backed cache, and flushes it
// to disk. If flush is called on a file that is not in memory, it will be
// reloaded from disk and written to disk again. Flush is typically called when
// a file descriptor is closed. This is responsible for triggering uploads of
// file contents.
func (c *Cache) FlushContentID(key string) {
	content, err := c.GetContentID(key, nil)
	if err != nil {
		return
	}

	// add item to disk
	c.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.contentName)
		b.Put([]byte(key), content.data)
		return nil
	})

	// flush item from memory
	c.content.Delete(key)
}

// DeleteContentID deletes all content from the local computer
func (c *Cache) DeleteContentID(key string) {
	c.content.Delete(key)
	c.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.contentName)
		b.Delete([]byte(key))
		return nil
	})
}

// only used for parsing
type driveChildren struct {
	Children []*DriveItem `json:"value"`
}

// GetChildrenID grabs all DriveItems that are the children of the given ID. If
// items are not found, they are fetched.
func (c *Cache) GetChildrenID(id string, auth *Auth) (map[string]*DriveItem, error) {
	// fetch item and catch common errors
	item := c.GetID(id)
	children := make(map[string]*DriveItem)
	if item == nil {
		msg := id + " does not exist in cache"
		logger.Error(msg)
		return children, errors.New(msg)
	} else if !item.IsDir() {
		// Normal files are treated as empty folders. This only gets called if
		// we messed up and tried to get the children of a plain-old file.
		logger.Warn("Attepted to get children of ordinary file")
		return children, nil
	}

	// If item.children is not nil, it means we have the item's children
	// already and can fetch them directly from the cache
	if item.children != nil {
		for _, id := range item.children {
			child := c.GetID(id)
			if child == nil {
				// will be nil if deleted or never existed
				continue
			}
			children[strings.ToLower(child.Name())] = child
		}
		return children, nil
	}

	// check that we have a valid auth before proceeding
	if auth == nil || auth.AccessToken == "" {
		return nil, errors.New("Auth was nil/zero and children of \"" +
			item.Path() +
			"\" were not in cache. Could not fetch item as a result.")
	}

	// We haven't fetched the children for this item yet, get them from the
	// server.
	body, err := Get(ChildrenPathID(id), auth)
	var fetched driveChildren
	if err != nil {
		return nil, err
	}
	json.Unmarshal(body, &fetched)

	item.children = make([]string, 0)
	for _, child := range fetched.Children {
		// Initialize item and store in cache. Note that we will always have an id
		// after fetching from the server
		c.InsertID(child.ID(), child)

		// store in result map
		children[strings.ToLower(child.Name())] = child

		// store id in parent item and increment parents subdirectory count
		item.children = append(item.children, child.IDInternal)
		if child.IsDir() {
			item.subdir++
		}
	}
	return children, nil
}

// GetChildrenPath grabs all DriveItems that are the children of the resource at
// the path. If items are not found, they are fetched.
func (c *Cache) GetChildrenPath(path string, auth *Auth) (map[string]*DriveItem, error) {
	item, err := c.GetPath(path, auth)
	if err != nil {
		return make(map[string]*DriveItem), err
	}
	return c.GetChildrenID(item.ID(), auth)
}

// GetPath fetches a given DriveItem in the cache for the given fileystem path,
// if any items along the way are not found, they are fetched.
func (c *Cache) GetPath(path string, auth *Auth) (*DriveItem, error) {
	lastID := c.root
	if path == "/" {
		return c.GetID(lastID), nil
	}

	// from the root directory, traverse the chain of items till we reach our
	// target ID.
	path = strings.TrimSuffix(strings.ToLower(path), "/")
	split := strings.Split(path, "/")[1:] //omit leading "/"
	var item *DriveItem
	for i := 0; i < len(split); i++ {
		// fetches children
		children, err := c.GetChildrenID(lastID, auth)
		if err != nil {
			return nil, err
		}

		var exists bool // if we use ":=", item is shadowed
		item, exists = children[split[i]]
		if !exists {
			// the item still doesn't exist after fetching from server. it
			// doesn't exist
			return nil, errors.New(strings.Join(split[:i+1], "/") +
				" does not exist on server or in local cache")
		}
		lastID = item.ID()
	}
	return item, nil
}

// addToParent adds an object as a child of a parent
func (c *Cache) setParent(item *DriveItem, parent *DriveItem) {
	if item.IsDir() {
		parent.subdir++
	}
	parent.children = append(parent.children, item.IDInternal)
	item.Parent.ID = parent.IDInternal
}

// removeParent removes a given item from its parent
func (c *Cache) removeParent(item *DriveItem) {
	if item != nil { // item can be nil in some scenarios
		id := item.ID()
		parent := c.GetID(item.Parent.ID)
		for i, childID := range parent.children {
			if childID == id {
				parent.children = append(parent.children[:i], parent.children[i+1:]...)
				break
			}
		}
		if item.IsDir() {
			parent.subdir--
		}
	}
}

// DeletePath an item from the cache by path
func (c *Cache) DeletePath(key string) {
	key = strings.ToLower(key)
	// Uses empty auth, since we actually don't want to waste time fetching
	// items that are only being fetched so they can be deleted.
	item, err := c.GetPath(key, &Auth{})
	if err != nil {
		c.removeParent(item)
	}
	c.DeleteID(item.ID())
}

// InsertPath lets us manually insert an item to the cache (like if it was
// created locally). Overwrites a cached item if present.
func (c *Cache) InsertPath(key string, auth *Auth, item *DriveItem) error {
	key = strings.ToLower(key)
	parent, err := c.GetPath(filepath.Dir(key), auth)
	if err != nil {
		return err
	} else if parent == nil {
		return errors.New("Parent of key was nil! Did we accidentally use an ID for the key?")
	}

	c.setParent(item, parent)
	c.InsertID(item.ID(), item)
	return nil
}

// MoveID moves an item to a new ID name
func (c *Cache) MoveID(oldID string, newID string) error {
	item := c.GetID(oldID)
	if item == nil {
		return errors.New("Could not get item: " + oldID)
	}

	// need to rename the child under the parent
	parent := c.GetID(item.Parent.ID)
	for i, child := range parent.children {
		if child == oldID {
			parent.children[i] = newID
			break
		}
	}

	// rename actual item and move it in the cache
	item.IDInternal = newID
	c.InsertID(newID, item)
	c.DeleteID(oldID)
	return nil
}

// MovePath an item to a new position
func (c *Cache) MovePath(oldPath string, newPath string, auth *Auth) error {
	item, err := c.GetPath(oldPath, auth)
	if err != nil {
		return err
	}

	c.DeletePath(oldPath)
	if err = c.InsertPath(newPath, auth, item); err != nil {
		// insert failed, reinsert in old location
		c.InsertPath(oldPath, auth, item)
		return err
	}
	return nil
}

// deltaLoop should be called as a goroutine
func (c *Cache) deltaLoop() {
	logger.Trace("Starting delta goroutine...")
	for { // eva
		// get deltas
		logger.Trace("Syncing deltas from server...")
		for {
			cont, err := c.pollDeltas(c.auth)
			if err != nil {
				logger.Error(err)
				break
			}
			if !cont {
				break
			}
		}
		logger.Trace("Sync complete!")

		// go to sleep until next poll interval
		time.Sleep(30 * time.Second)
	}
}

type deltaResponse struct {
	NextLink  string      `json:"@odata.nextLink,omitempty"`
	DeltaLink string      `json:"@odata.deltaLink,omitempty"`
	Values    []DriveItem `json:"value,omitempty"`
}

// Polls the delta endpoint and return whether or not to continue polling
func (c *Cache) pollDeltas(auth *Auth) (bool, error) {
	resp, err := Get(c.deltaLink, auth)
	if err != nil {
		logger.Error("Could not fetch server deltas:", err)
		return false, err
	}

	page := deltaResponse{}
	json.Unmarshal(resp, &page)
	for _, item := range page.Values {
		c.applyDelta(item)
	}

	// If the server does not provide a `@odata.nextLink` item, it means we've
	// reached the end of this polling cycle and should not continue until the
	// next poll interval.
	if page.NextLink != "" {
		c.deltaLink = strings.TrimPrefix(page.NextLink, graphURL)
		return true, nil
	}
	c.deltaLink = strings.TrimPrefix(page.DeltaLink, graphURL)
	return false, nil
}

// apply a server-side change to our local state
func (c *Cache) applyDelta(item DriveItem) error {
	logger.Trace("Applying delta for", item.Name())
	//TODO stub
	return nil
}
