//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package filefs implements a file-backed EROFS mount driver with fanotify
// on-demand data loading. It mounts EROFS filesystem images directly from
// regular files using the kernel's file-backed mount feature (no loop devices,
// no FUSE), and uses fanotify FAN_PRE_CONTENT hooks to fetch blob data on demand.
package filefs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/containerd/log"
	"github.com/containerd/nydus-snapshotter/pkg/rafs"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// Manager manages file-backed EROFS mounts with fanotify on-demand loading.
type Manager struct {
	mu               sync.Mutex
	snapshots        map[string]*snapshotState
	snapshotContexts map[string]*snapshotContext // per-snapshot image ref and labels
	cacheDirPath     string
	insecure         bool
	fetcher          *DataFetcher
}

// snapshotContext holds per-snapshot metadata needed for on-demand blob fetching.
type snapshotContext struct {
	imageRef string
	labels   map[string]string
}

type snapshotState struct {
	mountPoint     string
	backingFile    string             // path to the EROFS image file (bootstrap)
	blobs          []blobInfo         // blob metadata from EROFS device table
	blobDir        string             // directory containing blob sparse files
	fanotifyFd     int                // fanotify file descriptor
	stopCh         chan struct{}      // signals fanotify goroutine to stop
	wg             sync.WaitGroup     // tracks fanotify goroutine lifecycle
	cancelPrefetch context.CancelFunc // cancels in-progress blob prefetch
}

// NewManager creates a new file-backed EROFS mount manager.
func NewManager(cacheDirPath string, insecure bool) *Manager {
	return &Manager{
		snapshots:        make(map[string]*snapshotState),
		snapshotContexts: make(map[string]*snapshotContext),
		cacheDirPath:     cacheDirPath,
		insecure:         insecure,
		fetcher:          NewDataFetcher(cacheDirPath, insecure),
	}
}

// MountFileErofs mounts an EROFS image file directly using file-backed mount
// and sets up fanotify pre-content hooks for on-demand data loading.
//
// For images with external blobs, sparse placeholder files are created for each
// blob and passed as device= mount options. A background prefetch fills these
// files from the remote registry. Fanotify FAN_PRE_CONTENT events block until
// the needed blob data is available, enabling lazy loading.
func (m *Manager) MountFileErofs(snapshotID string, r *rafs.Rafs) error {
	bootstrapFile, err := r.BootstrapFile()
	if err != nil {
		return errors.Wrapf(err, "find bootstrap for snapshot %s", snapshotID)
	}

	mountPoint := filepath.Join(r.GetSnapshotDir(), "mnt")
	if err := os.MkdirAll(mountPoint, 0750); err != nil {
		return errors.Wrapf(err, "create mount dir %s", mountPoint)
	}

	// Parse the EROFS device table to discover referenced blobs (IDs + sizes).
	blobs, err := parseDeviceTable(bootstrapFile)
	if err != nil {
		log.L.WithError(err).Warnf("filefs: failed to parse device table from %s, on-demand fetching may not work", bootstrapFile)
	}

	// Create sparse placeholder files for each blob and build device= mount options.
	blobDir := filepath.Join(r.GetSnapshotDir(), "blobs")
	var deviceOpts []string
	if len(blobs) > 0 {
		log.L.Infof("filefs: discovered %d blob(s) in EROFS device table for snapshot %s", len(blobs), snapshotID)
		if err := os.MkdirAll(blobDir, 0750); err != nil {
			return errors.Wrapf(err, "create blob dir %s", blobDir)
		}
		for _, blob := range blobs {
			blobPath := filepath.Join(blobDir, blob.ID)
			if err := createSparseFile(blobPath, blob.Size); err != nil {
				return errors.Wrapf(err, "create sparse blob file %s", blobPath)
			}
			deviceOpts = append(deviceOpts, "device="+blobPath)
		}
	}

	// File-backed EROFS mount: use the bootstrap file as the source.
	// The kernel mounts the EROFS image directly from the file without
	// intermediate loopback block devices. External blob data is referenced
	// via device= options pointing to regular files (sparse, filled lazily).
	mountOpts := "source=" + bootstrapFile
	if len(deviceOpts) > 0 {
		mountOpts += "," + strings.Join(deviceOpts, ",")
	}
	if err := unix.Mount("none", mountPoint, "erofs", unix.MS_RDONLY, mountOpts); err != nil {
		return errors.Wrapf(err, "file-backed mount erofs at %s from %s (opts: %s)", mountPoint, bootstrapFile, mountOpts)
	}
	log.L.Infof("File-backed EROFS mounted at %s from %s with %d device(s)", mountPoint, bootstrapFile, len(deviceOpts))

	// Create cancellable context for background prefetch.
	prefetchCtx, cancelPrefetch := context.WithCancel(context.Background())

	st := &snapshotState{
		mountPoint:     mountPoint,
		backingFile:    bootstrapFile,
		blobs:          blobs,
		blobDir:        blobDir,
		fanotifyFd:     -1,
		stopCh:         make(chan struct{}),
		cancelPrefetch: cancelPrefetch,
	}

	// Set up fanotify for on-demand data loading.
	if err := m.setupFanotify(st, mountPoint); err != nil {
		cancelPrefetch()
		if umountErr := unix.Unmount(mountPoint, 0); umountErr != nil {
			log.L.WithError(umountErr).Errorf("failed to unmount %s during fanotify setup cleanup", mountPoint)
		}
		return errors.Wrapf(err, "setup fanotify for %s", mountPoint)
	}

	// Register per-snapshot context for the fetcher to use during on-demand loading.
	sctx := &snapshotContext{
		imageRef: r.ImageID,
		labels:   r.Annotations,
	}

	m.mu.Lock()
	m.snapshots[snapshotID] = st
	m.snapshotContexts[snapshotID] = sctx
	m.mu.Unlock()

	// Start background prefetch — downloads blob data into the sparse files.
	// Fanotify events will block until the needed blobs are fully downloaded.
	if len(blobs) > 0 {
		m.fetcher.StartPrefetch(prefetchCtx, r.ImageID, blobs, blobDir)
	}

	r.SetMountpoint(mountPoint)
	return nil
}

// createSparseFile creates a sparse file at path with the given size.
// If the file already exists with the correct size, it's left as-is.
func createSparseFile(path string, size int64) error {
	fi, err := os.Stat(path)
	if err == nil && fi.Size() == size {
		return nil // already exists with correct size
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
}

// GetSnapshotContext returns the image reference for a snapshot, used by the
// fetcher to obtain auth credentials for remote blob downloads.
func (m *Manager) GetSnapshotContext(snapshotID string) (imageRef string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sctx, ok := m.snapshotContexts[snapshotID]; ok {
		return sctx.imageRef, sctx.labels
	}
	return "", nil
}

// UmountFileErofs unmounts a file-backed EROFS mount and stops the fanotify listener.
func (m *Manager) UmountFileErofs(snapshotID string) error {
	m.mu.Lock()
	st, ok := m.snapshots[snapshotID]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	delete(m.snapshots, snapshotID)
	delete(m.snapshotContexts, snapshotID)
	m.mu.Unlock()

	// Cancel in-progress blob prefetches.
	if st.cancelPrefetch != nil {
		st.cancelPrefetch()
	}

	// Signal the fanotify goroutine to stop and wait for it to exit.
	close(st.stopCh)
	st.wg.Wait()

	// Close fanotify fd after the goroutine has exited.
	if st.fanotifyFd >= 0 {
		unix.Close(st.fanotifyFd)
		st.fanotifyFd = -1
	}

	// Unmount the filesystem.
	if st.mountPoint != "" {
		if err := unix.Unmount(st.mountPoint, 0); err != nil {
			return errors.Wrapf(err, "umount file-backed erofs %s", st.mountPoint)
		}
		log.L.Infof("Unmounted file-backed EROFS at %s", st.mountPoint)
	}

	return nil
}

// TeardownAll unmounts all active file-backed EROFS mounts.
func (m *Manager) TeardownAll() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.snapshots))
	for id := range m.snapshots {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	for _, id := range ids {
		if err := m.UmountFileErofs(id); err != nil {
			log.L.WithError(err).Errorf("failed to teardown file-backed erofs for snapshot %s", id)
		}
	}
}
