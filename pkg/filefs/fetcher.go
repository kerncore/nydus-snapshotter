//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/log"
	"github.com/containerd/nydus-snapshotter/pkg/auth"
	"github.com/containerd/nydus-snapshotter/pkg/remote"
	"github.com/containerd/nydus-snapshotter/pkg/remote/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

const (
	// Maximum number of concurrent blob downloads.
	maxConcurrentDownloads = 3
)

// DataFetcher handles on-demand data retrieval for file-backed EROFS mounts.
// It downloads blob data from remote registries into local files that the
// kernel's EROFS driver reads via file-backed mount device= references.
type DataFetcher struct {
	cacheDirPath string
	insecure     bool

	mu sync.RWMutex
	// Track blobs that have been fully fetched to avoid redundant work.
	fetched map[string]bool
	// Per-blob download completion channels: closed when download finishes.
	downloads map[string]chan struct{}
	// Per-blob download errors (set before channel close).
	downloadErrs map[string]error

	// Dedup concurrent fetches for the same blob.
	sg singleflight.Group
	// Limit concurrent downloads.
	sem *semaphore.Weighted
}

// NewDataFetcher creates a DataFetcher backed by the given cache directory.
func NewDataFetcher(cacheDirPath string, insecure bool) *DataFetcher {
	return &DataFetcher{
		cacheDirPath: cacheDirPath,
		insecure:     insecure,
		fetched:      make(map[string]bool),
		downloads:    make(map[string]chan struct{}),
		downloadErrs: make(map[string]error),
		sem:          semaphore.NewWeighted(maxConcurrentDownloads),
	}
}

// StartPrefetch starts background goroutines to download all blobs for a snapshot.
// Each blob is downloaded into blobDir/<blobID> (the sparse file passed as device=
// to the EROFS mount). Downloads are rate-limited to maxConcurrentDownloads.
func (f *DataFetcher) StartPrefetch(ctx context.Context, imageRef string, blobs []blobInfo, blobDir string) {
	log.L.Infof("filefs: starting background prefetch for %d blob(s)", len(blobs))

	for _, blob := range blobs {
		blobID := blob.ID
		targetPath := filepath.Join(blobDir, blobID)

		// Create completion channel.
		f.mu.Lock()
		if f.fetched[blobID] {
			f.mu.Unlock()
			continue
		}
		if _, exists := f.downloads[blobID]; exists {
			f.mu.Unlock()
			continue // already downloading
		}
		ch := make(chan struct{})
		f.downloads[blobID] = ch
		f.mu.Unlock()

		go func(id string, path string, done chan struct{}) {
			err := f.fetchBlobToFile(ctx, imageRef, id, path)

			f.mu.Lock()
			if err != nil {
				f.downloadErrs[id] = err
				log.L.WithError(err).Errorf("filefs: failed to prefetch blob %s", id)
			} else {
				f.fetched[id] = true
				log.L.Infof("filefs: prefetched blob %s to %s", id, path)
			}
			f.mu.Unlock()

			close(done)
		}(blobID, targetPath, ch)
	}
}

// WaitForBlobs blocks until all specified blobs are fully downloaded.
// Returns the first error encountered, or nil if all blobs are available.
func (f *DataFetcher) WaitForBlobs(ctx context.Context, blobs []blobInfo) error {
	if len(blobs) == 0 {
		return nil
	}

	// Fast path: all blobs already fetched.
	f.mu.RLock()
	allFetched := true
	for _, b := range blobs {
		if !f.fetched[b.ID] {
			allFetched = false
			break
		}
	}
	f.mu.RUnlock()
	if allFetched {
		return nil
	}

	// Wait for each blob's download to complete.
	for _, b := range blobs {
		f.mu.RLock()
		if f.fetched[b.ID] {
			f.mu.RUnlock()
			continue
		}
		ch := f.downloads[b.ID]
		f.mu.RUnlock()

		if ch == nil {
			// No download in progress and not fetched — shouldn't happen
			// if StartPrefetch was called, but handle gracefully.
			return errors.Errorf("blob %s has no active download", b.ID)
		}

		select {
		case <-ch:
			// Download completed — check for errors.
			f.mu.RLock()
			err := f.downloadErrs[b.ID]
			f.mu.RUnlock()
			if err != nil {
				return errors.Wrapf(err, "blob %s download failed", b.ID)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// fetchBlobToFile downloads a blob from the remote registry and writes it
// directly to targetPath (the sparse file mounted as a device= for EROFS).
// Uses singleflight to dedup and a semaphore to limit concurrency.
func (f *DataFetcher) fetchBlobToFile(ctx context.Context, imageRef, blobID, targetPath string) error {
	// Acquire semaphore slot for rate limiting.
	if err := f.sem.Acquire(ctx, 1); err != nil {
		return errors.Wrap(err, "acquire download semaphore")
	}
	defer f.sem.Release(1)

	blobDigest, err := digest.Parse("sha256:" + blobID)
	if err != nil {
		return errors.Wrapf(err, "invalid blob ID %s", blobID)
	}

	// Use singleflight to dedup concurrent fetches for the same blob.
	_, sfErr, _ := f.sg.Do(blobID, func() (interface{}, error) {
		return nil, f.downloadBlob(ctx, imageRef, blobDigest, targetPath)
	})
	return sfErr
}

// downloadBlob downloads a blob from the remote registry and writes it
// directly to targetPath. The target file must already exist (as a sparse
// placeholder created at mount time). We write to the SAME inode rather than
// using tmp+rename, because the kernel already holds an fd to this file
// (opened at mount via device=). A rename would create a new inode that
// the kernel's fd cannot see.
func (f *DataFetcher) downloadBlob(ctx context.Context, imageRef string, blobDigest digest.Digest, targetPath string) error {
	// 1. Get auth credentials for the image reference.
	keyChain, err := auth.GetKeyChainByRef(imageRef, nil)
	if err != nil {
		log.L.WithError(err).Warnf("filefs: failed to get keychain for %s, trying without auth", imageRef)
		keyChain = nil
	}

	// 2. Create remote client with auth.
	r := remote.New(keyChain, f.insecure)

	// 3. Fetch the blob by digest, with HTTP fallback retry.
	rc, err := f.getBlobStream(ctx, r, imageRef, blobDigest)
	if err != nil && r.RetryWithPlainHTTP(imageRef, err) {
		rc, err = f.getBlobStream(ctx, r, imageRef, blobDigest)
	}
	if err != nil {
		return errors.Wrapf(err, "fetch blob %s from %s", blobDigest, imageRef)
	}
	defer rc.Close()

	// 4. Write directly to the sparse blob file (same inode the kernel holds).
	blobFile, err := os.OpenFile(targetPath, os.O_WRONLY, 0)
	if err != nil {
		return errors.Wrapf(err, "open blob file %s for writing", targetPath)
	}

	if _, err := io.Copy(blobFile, rc); err != nil {
		blobFile.Close()
		return errors.Wrapf(err, "write blob data to %s", targetPath)
	}

	// 5. Sync to disk before fanotify ALLOW — the kernel must see the data
	// when it reads from this file immediately after we respond.
	if err := blobFile.Sync(); err != nil {
		blobFile.Close()
		return errors.Wrapf(err, "sync blob file %s", targetPath)
	}

	if err := blobFile.Close(); err != nil {
		return errors.Wrapf(err, "close blob file %s", targetPath)
	}

	log.L.Infof("filefs: downloaded blob %s to %s", blobDigest, targetPath)
	return nil
}

// getBlobStream fetches a blob stream by digest from the remote registry.
// Same pattern as pkg/tarfs/tarfs.go:199-211.
func (f *DataFetcher) getBlobStream(ctx context.Context, r *remote.Remote, ref string, contentDigest digest.Digest) (io.ReadCloser, error) {
	fetcher, err := r.Fetcher(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "get remote fetcher")
	}

	fetcherByDigest, ok := fetcher.(remotes.FetcherByDigest)
	if !ok {
		return nil, errors.Errorf("fetcher %T does not implement FetcherByDigest", fetcher)
	}

	rc, _, err := fetcherByDigest.FetchByDigest(ctx, contentDigest)
	if err != nil {
		return nil, errors.Wrapf(err, "fetch blob %s", contentDigest)
	}

	return rc, nil
}
