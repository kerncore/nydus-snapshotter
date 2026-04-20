//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDataFetcher(t *testing.T) {
	f := NewDataFetcher("/tmp/cache", false)
	require.NotNil(t, f)
	assert.Equal(t, "/tmp/cache", f.cacheDirPath)
	assert.False(t, f.insecure)
	assert.NotNil(t, f.fetched)
	assert.NotNil(t, f.downloads)
	assert.NotNil(t, f.downloadErrs)
	assert.NotNil(t, f.sem)

	f2 := NewDataFetcher("/tmp/insecure", true)
	assert.True(t, f2.insecure)
}

func TestWaitForBlobs_Empty(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)
	err := f.WaitForBlobs(context.Background(), nil)
	assert.NoError(t, err)
}

func TestWaitForBlobs_AllAlreadyFetched(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	f.mu.Lock()
	f.fetched["blob1"] = true
	f.fetched["blob2"] = true
	f.mu.Unlock()

	blobs := []blobInfo{
		{ID: "blob1", Size: 1024},
		{ID: "blob2", Size: 2048},
	}
	err := f.WaitForBlobs(context.Background(), blobs)
	assert.NoError(t, err)
}

func TestWaitForBlobs_WaitsForDownload(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	ch := make(chan struct{})
	f.mu.Lock()
	f.downloads["blob1"] = ch
	f.mu.Unlock()

	blobs := []blobInfo{{ID: "blob1", Size: 1024}}

	var wg sync.WaitGroup
	var waitErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitErr = f.WaitForBlobs(context.Background(), blobs)
	}()

	// Simulate download completion.
	f.mu.Lock()
	f.fetched["blob1"] = true
	f.mu.Unlock()
	close(ch)

	wg.Wait()
	assert.NoError(t, waitErr)
}

func TestWaitForBlobs_DownloadError(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	ch := make(chan struct{})
	f.mu.Lock()
	f.downloads["blob1"] = ch
	f.downloadErrs["blob1"] = assert.AnError
	f.mu.Unlock()
	close(ch)

	blobs := []blobInfo{{ID: "blob1", Size: 1024}}
	err := f.WaitForBlobs(context.Background(), blobs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download failed")
}

func TestWaitForBlobs_ContextCancelled(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	ch := make(chan struct{}) // never closes
	f.mu.Lock()
	f.downloads["blob1"] = ch
	f.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	blobs := []blobInfo{{ID: "blob1", Size: 1024}}
	err := f.WaitForBlobs(ctx, blobs)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestStartPrefetch_SkipsAlreadyFetched(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	f.mu.Lock()
	f.fetched["blob1"] = true
	f.mu.Unlock()

	blobs := []blobInfo{{ID: "blob1", Size: 1024}}
	f.StartPrefetch(context.Background(), "test-image", blobs, t.TempDir())

	f.mu.RLock()
	_, exists := f.downloads["blob1"]
	f.mu.RUnlock()
	assert.False(t, exists, "should not create download for already-fetched blob")
}

func TestStartPrefetch_SkipsDuplicateDownloads(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	existing := make(chan struct{})
	f.mu.Lock()
	f.downloads["blob1"] = existing
	f.mu.Unlock()

	blobs := []blobInfo{{ID: "blob1", Size: 1024}}
	f.StartPrefetch(context.Background(), "test-image", blobs, t.TempDir())

	f.mu.RLock()
	ch := f.downloads["blob1"]
	f.mu.RUnlock()
	assert.Equal(t, existing, ch, "should keep existing download channel")
}

func TestFetchBlobToFile_InvalidBlobID(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	err := f.fetchBlobToFile(context.Background(), "test-image", "not-a-hex", "/tmp/target")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid blob ID")
}

func TestFetchBlobToFile_CancelledContext(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := f.fetchBlobToFile(ctx, "test-image",
		"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
		"/tmp/target")
	assert.Error(t, err)
}

func TestWaitForBlobs_MultipleBlobs(t *testing.T) {
	f := NewDataFetcher(t.TempDir(), true)

	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	f.mu.Lock()
	f.downloads["blob1"] = ch1
	f.downloads["blob2"] = ch2
	f.mu.Unlock()

	blobs := []blobInfo{
		{ID: "blob1", Size: 1024},
		{ID: "blob2", Size: 2048},
	}

	var wg sync.WaitGroup
	var waitErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitErr = f.WaitForBlobs(context.Background(), blobs)
	}()

	// Complete both downloads.
	f.mu.Lock()
	f.fetched["blob1"] = true
	f.fetched["blob2"] = true
	f.mu.Unlock()
	close(ch1)
	close(ch2)

	wg.Wait()
	assert.NoError(t, waitErr)
}

func TestBlobDir_SparseFileCreation(t *testing.T) {
	blobDir := t.TempDir()
	blobID := "test1234test1234test1234test1234test1234test1234test1234test1234"
	blobPath := filepath.Join(blobDir, blobID)

	require.NoError(t, createSparseFile(blobPath, 4096))

	fi, err := os.Stat(blobPath)
	require.NoError(t, err)
	assert.Equal(t, int64(4096), fi.Size())
}
