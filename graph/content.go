package graph

import (
	"sync"

	"github.com/hanwen/go-fuse/fuse"
)

// DriveItemContent represents the actual content of a file in the filesystem.
// It's just a loose container around []byte with a mutex lock to prevent
// concurrent write ops.
type DriveItemContent struct {
	sync.RWMutex
	data       []byte
	size       uint64
	hasChanges bool
}

// NewDriveItemContent creates a new actual "file" that stores actual contents
func NewDriveItemContent(contents []byte) *DriveItemContent {
	return &DriveItemContent{
		data: contents,
		size: uint64(len(contents)),
	}
}

// Read from a DriveItemContent like a file
func (c *DriveItemContent) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	end := int(off) + int(len(buf))
	c.RLock()
	defer c.RUnlock()
	if end > len(c.data) {
		end = len(c.data)
	}
	return fuse.ReadResultData((c.data)[off:end]), fuse.OK
}

// Write to a DriveItemContent like a file. Note that changes are 100% local.
func (c *DriveItemContent) Write(data []byte, off int64) (uint32, fuse.Status) {
	nWrite := len(data)
	offset := int(off)

	c.Lock()
	defer c.Unlock()
	if offset+nWrite > int(c.size)-1 {
		// we've exceeded the file size, overwrite via append
		c.data = append((c.data)[:offset], data...)
	} else {
		// writing inside the current file, overwrite in place
		copy((c.data)[offset:], data)
	}
	// probably a better way to do this, but whatever
	c.size = uint64(len(c.data))
	c.hasChanges = true
	return uint32(nWrite), fuse.OK
}

// Truncate cuts a file in place
func (c *DriveItemContent) Truncate(size uint64) fuse.Status {
	c.Lock()
	defer c.Unlock()
	c.data = (c.data)[:size]
	c.size = size
	c.hasChanges = true
	return fuse.OK
}
