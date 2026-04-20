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
		"fd 0": {
			fd:       0,
			expected: "/proc/self/fd/0",
		},
		"fd 1": {
			fd:       1,
			expected: "/proc/self/fd/1",
		},
		"fd 42": {
			fd:       42,
			expected: "/proc/self/fd/42",
		},
		"fd 1234567": {
			fd:       1234567,
			expected: "/proc/self/fd/1234567",
		},
		"fd 10": {
			fd:       10,
			expected: "/proc/self/fd/10",
		},
		"negative fd": {
			fd:       -1,
			expected: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := procFdPath(tc.fd)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestReadlinkSafe(t *testing.T) {
	// Create a temp file and a symlink to it.
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "target")
	require.NoError(t, os.WriteFile(targetFile, []byte("data"), 0644))

	symlink := filepath.Join(tmpDir, "link")
	require.NoError(t, os.Symlink(targetFile, symlink))

	// readlinkSafe should resolve the symlink.
	result, err := readlinkSafe(symlink)
	require.NoError(t, err)
	assert.Equal(t, targetFile, result)
}

func TestReadlinkSafe_NotExist(t *testing.T) {
	_, err := readlinkSafe("/nonexistent/symlink/path")
	assert.Error(t, err)
}

func TestReadlinkSafe_LongPath(t *testing.T) {
	// Create a target with a long path name to test buffer growth.
	tmpDir := t.TempDir()
	longName := ""
	for i := 0; i < 50; i++ {
		longName += "abcde"
	}
	targetFile := filepath.Join(tmpDir, longName)
	require.NoError(t, os.WriteFile(targetFile, []byte("data"), 0644))

	symlink := filepath.Join(tmpDir, "link")
	require.NoError(t, os.Symlink(targetFile, symlink))

	result, err := readlinkSafe(symlink)
	require.NoError(t, err)
	assert.Equal(t, targetFile, result)
}

func TestFanotifyResponseString(t *testing.T) {
	assert.Equal(t, "ALLOW", fanotifyResponseString(fanotifyResponseAllow))
	assert.Equal(t, "DENY", fanotifyResponseString(fanotifyResponseDeny))
	assert.Equal(t, "0xff", fanotifyResponseString(0xff))
}

func TestDrainPendingEvents_ClosedFd(t *testing.T) {
	m := NewManager(t.TempDir(), false)

	// drainPendingEvents with fd=-1 should be a no-op.
	st := &snapshotState{fanotifyFd: -1}
	m.drainPendingEvents(st) // should not panic
}
