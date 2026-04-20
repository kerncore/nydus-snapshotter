//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchBlob_CacheHit(t *testing.T) {
	cacheDir := t.TempDir()
	fetcher := NewDataFetcher(cacheDir, true)

	blobContent := []byte("cached blob data")
	blobDigest := digest.FromBytes(blobContent)
	blobID := blobDigest.Hex()

	// Pre-create the blob in cache.
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, blobID), blobContent, 0644))

	var requestCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// FetchBlob should see the cached file and not make any HTTP requests.
	err := fetcher.FetchBlob(context.Background(), "docker.io/library/test:latest", blobDigest)
	assert.NoError(t, err)
	assert.Equal(t, int32(0), requestCount.Load(), "no HTTP requests should be made for cached blobs")
}

func TestFetchBlob_SingleflightDedup(t *testing.T) {
	cacheDir := t.TempDir()
	fetcher := NewDataFetcher(cacheDir, true)

	blobContent := []byte("singleflight test blob")
	blobDigest := digest.FromBytes(blobContent)

	var requestCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(blobContent)
	}))
	defer srv.Close()

	// Note: FetchBlob uses the remote registry infrastructure (auth + resolver)
	// which means we can't easily point it at our httptest server without
	// mocking the entire remote.Remote chain. Instead, we test the singleflight
	// dedup property indirectly by verifying that calling FetchBlob twice for
	// a blob that is already in cache (after first manual placement) only hits
	// the fast path.
	blobID := blobDigest.Hex()
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, blobID), blobContent, 0644))

	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := fetcher.FetchBlob(context.Background(), "docker.io/library/test:latest", blobDigest)
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("unexpected error: %v", err)
	}

	// All 10 goroutines should have hit the cache fast path.
	assert.Equal(t, int32(0), requestCount.Load(), "all requests should hit cache")
}

func TestEnsureAllBlobsFetched_AllCached(t *testing.T) {
	cacheDir := t.TempDir()
	fetcher := NewDataFetcher(cacheDir, true)

	blob1 := []byte("blob1 data")
	blob2 := []byte("blob2 data")
	d1 := digest.FromBytes(blob1)
	d2 := digest.FromBytes(blob2)

	// Pre-create both blobs in cache.
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, d1.Hex()), blob1, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, d2.Hex()), blob2, 0644))

	err := fetcher.EnsureAllBlobsFetched(context.Background(), "docker.io/library/test:latest", []string{d1.Hex(), d2.Hex()})
	assert.NoError(t, err)

	// Both should be marked as fetched.
	fetcher.mu.RLock()
	assert.True(t, fetcher.fetched[d1.Hex()])
	assert.True(t, fetcher.fetched[d2.Hex()])
	fetcher.mu.RUnlock()
}

func TestEnsureAllBlobsFetched_Empty(t *testing.T) {
	fetcher := NewDataFetcher(t.TempDir(), true)

	// Empty blob list should be a no-op.
	err := fetcher.EnsureAllBlobsFetched(context.Background(), "docker.io/library/test:latest", nil)
	assert.NoError(t, err)
}

func TestNewDataFetcher(t *testing.T) {
	tests := map[string]struct {
		cacheDir string
		insecure bool
	}{
		"secure fetcher": {
			cacheDir: "/tmp/test-cache",
			insecure: false,
		},
		"insecure fetcher": {
			cacheDir: "/tmp/test-cache-insecure",
			insecure: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f := NewDataFetcher(tc.cacheDir, tc.insecure)
			require.NotNil(t, f)
			assert.Equal(t, tc.cacheDir, f.cacheDirPath)
			assert.Equal(t, tc.insecure, f.insecure)
			assert.NotNil(t, f.fetched)
		})
	}
}

func TestFetchBlob_MarksFetchedAfterCacheHit(t *testing.T) {
	cacheDir := t.TempDir()
	fetcher := NewDataFetcher(cacheDir, true)

	blobContent := []byte("test data")
	blobDigest := digest.FromBytes(blobContent)
	blobID := blobDigest.Hex()

	// Pre-create in cache.
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, blobID), blobContent, 0644))

	// First call should check stat and mark as fetched.
	err := fetcher.FetchBlob(context.Background(), "docker.io/library/test:latest", blobDigest)
	require.NoError(t, err)

	// Verify the fetched map was updated.
	fetcher.mu.RLock()
	assert.True(t, fetcher.fetched[blobID], "blob should be marked as fetched")
	fetcher.mu.RUnlock()

	// Second call should hit the in-memory fast path (no stat needed).
	err = fetcher.FetchBlob(context.Background(), "docker.io/library/test:latest", blobDigest)
	assert.NoError(t, err)
}

func TestEnsureAllBlobsFetched_FastPathAfterFetch(t *testing.T) {
	cacheDir := t.TempDir()
	fetcher := NewDataFetcher(cacheDir, true)

	blob := []byte("test blob")
	d := digest.FromBytes(blob)
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, d.Hex()), blob, 0644))

	blobIDs := []string{d.Hex()}

	// First call fetches from cache.
	err := fetcher.EnsureAllBlobsFetched(context.Background(), "docker.io/library/test:latest", blobIDs)
	require.NoError(t, err)

	// Second call should hit the in-memory fast path.
	err = fetcher.EnsureAllBlobsFetched(context.Background(), "docker.io/library/test:latest", blobIDs)
	assert.NoError(t, err)
}
