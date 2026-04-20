//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	m := NewManager(t.TempDir(), false)
	require.NotNil(t, m)
	assert.NotNil(t, m.snapshots)
	assert.NotNil(t, m.snapshotContexts)
	assert.NotNil(t, m.fetcher)
	assert.False(t, m.insecure)
}

func TestManager_GetSnapshotContext_NotFound(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	imageRef, labels := m.GetSnapshotContext("nonexistent")
	assert.Empty(t, imageRef)
	assert.Nil(t, labels)
}

func TestManager_GetSnapshotContext_Found(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	// Manually register a snapshot context.
	m.mu.Lock()
	m.snapshotContexts["snap-1"] = &snapshotContext{
		imageRef: "docker.io/library/nginx:latest",
		labels:   map[string]string{"key": "val"},
	}
	m.mu.Unlock()

	imageRef, labels := m.GetSnapshotContext("snap-1")
	assert.Equal(t, "docker.io/library/nginx:latest", imageRef)
	assert.Equal(t, "val", labels["key"])
}

func TestManager_UmountFileErofs_NotFound(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	// Umounting a nonexistent snapshot should be a no-op (return nil).
	err := m.UmountFileErofs("nonexistent")
	assert.NoError(t, err)
}

func TestManager_TeardownAll_Empty(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	// TeardownAll on empty manager should not panic.
	m.TeardownAll()

	m.mu.Lock()
	assert.Empty(t, m.snapshots)
	m.mu.Unlock()
}

func TestManager_TeardownAll_CleansSnapshots(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	// Manually add snapshot states (without real mounts — we test state tracking only).
	m.mu.Lock()
	m.snapshots["snap-1"] = &snapshotState{
		mountPoint: "",
		fanotifyFd: -1,
		stopCh:     make(chan struct{}),
	}
	m.snapshots["snap-2"] = &snapshotState{
		mountPoint: "",
		fanotifyFd: -1,
		stopCh:     make(chan struct{}),
	}
	m.snapshotContexts["snap-1"] = &snapshotContext{imageRef: "img1"}
	m.snapshotContexts["snap-2"] = &snapshotContext{imageRef: "img2"}
	m.mu.Unlock()

	m.TeardownAll()

	m.mu.Lock()
	assert.Empty(t, m.snapshots, "all snapshots should be removed after teardown")
	assert.Empty(t, m.snapshotContexts, "all snapshot contexts should be removed after teardown")
	m.mu.Unlock()
}

func TestManager_ConcurrentAccess(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	var wg sync.WaitGroup
	const goroutines = 20

	// Half the goroutines add snapshot contexts, half remove them.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			snapshotID := "snap-concurrent"

			if id%2 == 0 {
				m.mu.Lock()
				m.snapshotContexts[snapshotID] = &snapshotContext{
					imageRef: "test-image",
				}
				m.mu.Unlock()
			} else {
				m.GetSnapshotContext(snapshotID)
			}
		}(i)
	}

	wg.Wait()
	// If we get here without -race failures, concurrent access is safe.
}

func TestManager_SnapshotStateTracking(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	// Register a snapshot.
	st := &snapshotState{
		mountPoint:  "/mnt/test",
		backingFile: "/data/bootstrap",
		fanotifyFd:  -1,
		stopCh:      make(chan struct{}),
	}
	m.mu.Lock()
	m.snapshots["snap-track"] = st
	m.mu.Unlock()

	// Verify it's tracked.
	m.mu.Lock()
	got, ok := m.snapshots["snap-track"]
	m.mu.Unlock()
	require.True(t, ok)
	assert.Equal(t, "/mnt/test", got.mountPoint)
	assert.Equal(t, "/data/bootstrap", got.backingFile)

	// Umount removes it (no real mount to undo since mountPoint won't exist).
	_ = m.UmountFileErofs("snap-track")

	m.mu.Lock()
	_, ok = m.snapshots["snap-track"]
	m.mu.Unlock()
	assert.False(t, ok, "snapshot should be removed after umount")
}
