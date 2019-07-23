package graph

import (
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/jstaf/onedriver/logger"
)

const (
	mountLoc = "mount"
	TestDir  = mountLoc + "/onedriver_tests"
)

var auth *Auth

// Tests are done in the main project directory with a mounted filesystem to
// avoid having to repeatedly recreate auth_tokens.json and juggle mutliple auth
// sessions.
func TestMain(m *testing.M) {
	os.Chdir("..")
	// attempt to unmount regardless of what happens (in case previous tests
	// failed and didn't clean themselves up)
	exec.Command("fusermount", "-u", mountLoc).Run()
	os.Mkdir(mountLoc, 0755)

	fusefs := NewFS()
	auth = fusefs.Auth
	fs := pathfs.NewPathNodeFs(fusefs, nil)
	server, _, _ := nodefs.MountRoot(mountLoc, fs.Root(), nil)

	// setup sigint handler for graceful unmount on interrupt/terminate
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go UnmountHandler(sigChan, server)

	// mount fs in background thread
	go server.Serve()
	// cleanup from last run
	os.RemoveAll(TestDir)
	os.Mkdir(TestDir, 0755)
	// we do not cd into the mounted directory or it will hang indefinitely on
	// unmount with "device or resource busy"

	logFile, _ := os.OpenFile("fusefs_tests.log", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	log.SetOutput(logFile)
	logger.SetLogLevel(logger.TRACE)
	logger.Info("Test session start -----------------------------------")

	// run tests
	code := m.Run()

	logger.Info("Test session end -----------------------------------")

	// unmount
	err := server.Unmount()
	if err != nil {
		log.Println("Failed to unmount test fuse server, attempting lazy unmount")
		exec.Command("fusermount", "-zu", "mount").Run()
	}
	logger.Info("Successfully unmounted fuse server.")
	os.Exit(code)
}

// convenience handler to fail tests if an error is not nil
func failOnErr(t *testing.T, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("Test failed at %s:%d:\n", file, line)
		t.Fatal(err)
	}
}
