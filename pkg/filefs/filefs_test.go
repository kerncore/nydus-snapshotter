//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"context"
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
	err := m.UmountFileErofs("nonexistent")
	assert.NoError(t, err)
}

func TestManager_TeardownAll_Empty(t *testing.T) {
	m := NewManager(t.TempDir(), false)
	m.TeardownAll()

	m.mu.Lock()
	assert.Empty(t, m.snapshots)
	m.mu.Unlock()
}

func makeTestSnapshotState() *snapshotState {
	_, cancel := context.WithCancel(context.Background())
	return &snapshotState{
		mountPoint:     "",
		fanotifyFd:     -1,
		stopCh:         make(chan struct{}),
		cancelPrefetch: cancel,
	}
}

func TestManager_TeardownAll_CleansSnapshots(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	m.mu.Lock()
	m.snapshots["snap-1"] = makeTestSnapshotState()
	m.snapshots["snap-2"] = makeTestSnapshotState()
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
}

func TestManager_SnapshotStateTracking(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	st := makeTestSnapshotState()
	st.mountPoint = "/mnt/test"
	st.backingFile = "/data/bootstrap"
	m.mu.Lock()
	m.snapshots["snap-track"] = st
	m.mu.Unlock()

	m.mu.Lock()
	got, ok := m.snapshots["snap-track"]
	m.mu.Unlock()
	require.True(t, ok)
	assert.Equal(t, "/mnt/test", got.mountPoint)
	assert.Equal(t, "/data/bootstrap", got.backingFile)

	// Umount removes it (no real mount since mountPoint is empty string).
	_ = m.UmountFileErofs("snap-track")

	m.mu.Lock()
	_, ok = m.snapshots["snap-track"]
	m.mu.Unlock()
	assert.False(t, ok, "snapshot should be removed after umount")
}

func TestManager_UmountCancelsPrefetch(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	ctx, cancel := context.WithCancel(context.Background())
	st := &snapshotState{
		mountPoint:     "",
		fanotifyFd:     -1,
		stopCh:         make(chan struct{}),
		cancelPrefetch: cancel,
	}

	m.mu.Lock()
	m.snapshots["snap-cancel"] = st
	m.mu.Unlock()

	err := m.UmountFileErofs("snap-cancel")
	assert.NoError(t, err)

	// Verify the context was cancelled.
	assert.Error(t, ctx.Err())
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}
