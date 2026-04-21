//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcFdPath(t *testing.T) {
	tests := map[string]struct {
		fd       int32
		expected string
	}{
		"fd 0":       {fd: 0, expected: "/proc/self/fd/0"},
		"fd 1":       {fd: 1, expected: "/proc/self/fd/1"},
		"fd 42":      {fd: 42, expected: "/proc/self/fd/42"},
		"fd 1234567": {fd: 1234567, expected: "/proc/self/fd/1234567"},
		"fd 10":      {fd: 10, expected: "/proc/self/fd/10"},
		"negative":   {fd: -1, expected: ""},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, procFdPath(tc.fd))
		})
	}
}

func TestReadlinkSafe(t *testing.T) {
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "target")
	require.NoError(t, os.WriteFile(targetFile, []byte("data"), 0644))

	symlink := filepath.Join(tmpDir, "link")
	require.NoError(t, os.Symlink(targetFile, symlink))

	result, err := readlinkSafe(symlink)
	require.NoError(t, err)
	assert.Equal(t, targetFile, result)
}

func TestReadlinkSafe_NotExist(t *testing.T) {
	_, err := readlinkSafe("/nonexistent/symlink/path")
	assert.Error(t, err)
}

func TestFanotifyResponseString(t *testing.T) {
	assert.Equal(t, "ALLOW", fanotifyResponseString(fanotifyResponseAllow))
	assert.Equal(t, "DENY", fanotifyResponseString(fanotifyResponseDeny))
	assert.Equal(t, "0xff", fanotifyResponseString(0xff))
}

func TestDrainPendingEvents_ClosedFd(t *testing.T) {
	m := NewManager(t.TempDir(), false)
	st := &snapshotState{fanotifyFd: -1}
	m.drainPendingEvents(st) // should not panic
}
