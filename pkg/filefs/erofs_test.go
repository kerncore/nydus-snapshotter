//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTestErofsImage creates a minimal EROFS image file with the given
// superblock fields and device table entries for testing.
func writeTestErofsImage(t *testing.T, sb erofsSuperBlock, blobIDs []string) string {
	t.Helper()
	imgPath := filepath.Join(t.TempDir(), "test.boot")
	f, err := os.Create(imgPath)
	require.NoError(t, err)
	defer f.Close()

	// Write padding up to superblock offset (1024 bytes).
	padding := make([]byte, erofsSuperOffset)
	_, err = f.Write(padding)
	require.NoError(t, err)

	// Write superblock.
	require.NoError(t, binary.Write(f, binary.LittleEndian, &sb))

	// If there are blob IDs, write device slots at the offset indicated by DevtSlotOff.
	if len(blobIDs) > 0 {
		devTableOffset := int64(sb.DevtSlotOff) * erofsDeviceSlotSize
		currentPos := int64(erofsSuperOffset) + int64(erofsSuperReadSize)
		if devTableOffset > currentPos {
			gap := make([]byte, devTableOffset-currentPos)
			_, err = f.Write(gap)
			require.NoError(t, err)
		}
		// Seek to exact position in case of overlap.
		_, err = f.Seek(devTableOffset, 0)
		require.NoError(t, err)

		for _, id := range blobIDs {
			var slot erofsDeviceSlot
			copy(slot.Tag[:], id)
			require.NoError(t, binary.Write(f, binary.LittleEndian, &slot))
		}
	}

	return imgPath
}

func TestParseDeviceTable_ValidImage(t *testing.T) {
	blobIDs := []string{
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
	}

	// Place device table at slot offset 9 (byte offset 9*128=1152, after the superblock).
	sb := erofsSuperBlock{
		Magic:           erofsSuperMagicV1,
		FeatureIncompat: erofsFeatureIncompatDeviceTable,
		ExtraDevices:    2,
		DevtSlotOff:     9,
	}

	imgPath := writeTestErofsImage(t, sb, blobIDs)

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	assert.Equal(t, blobIDs, got)
}

func TestParseDeviceTable_NoDevices(t *testing.T) {
	sb := erofsSuperBlock{
		Magic:           erofsSuperMagicV1,
		FeatureIncompat: erofsFeatureIncompatDeviceTable,
		ExtraDevices:    0,
	}

	imgPath := writeTestErofsImage(t, sb, nil)

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestParseDeviceTable_NoDeviceTableFeature(t *testing.T) {
	sb := erofsSuperBlock{
		Magic:           erofsSuperMagicV1,
		FeatureIncompat: 0, // no device table feature
		ExtraDevices:    2, // ignored since feature flag is off
	}

	imgPath := writeTestErofsImage(t, sb, nil)

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestParseDeviceTable_BadMagic(t *testing.T) {
	sb := erofsSuperBlock{
		Magic: 0xDEADBEEF,
	}

	imgPath := writeTestErofsImage(t, sb, nil)

	_, err := parseDeviceTable(imgPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid erofs magic")
}

func TestParseDeviceTable_FileNotFound(t *testing.T) {
	_, err := parseDeviceTable("/nonexistent/bootstrap")
	assert.Error(t, err)
}

func TestParseDeviceTable_EmptyTagsSkipped(t *testing.T) {
	// One valid blob ID, one empty tag.
	sb := erofsSuperBlock{
		Magic:           erofsSuperMagicV1,
		FeatureIncompat: erofsFeatureIncompatDeviceTable,
		ExtraDevices:    2,
		DevtSlotOff:     9,
	}

	// Write image with two slots, but second has empty tag.
	imgPath := filepath.Join(t.TempDir(), "test.boot")
	f, err := os.Create(imgPath)
	require.NoError(t, err)
	defer f.Close()

	// Padding to superblock.
	padding := make([]byte, erofsSuperOffset)
	_, err = f.Write(padding)
	require.NoError(t, err)

	// Superblock.
	require.NoError(t, binary.Write(f, binary.LittleEndian, &sb))

	// Seek to device table.
	_, err = f.Seek(int64(sb.DevtSlotOff)*erofsDeviceSlotSize, 0)
	require.NoError(t, err)

	// Slot 1: valid blob ID.
	var slot1 erofsDeviceSlot
	copy(slot1.Tag[:], "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	require.NoError(t, binary.Write(f, binary.LittleEndian, &slot1))

	// Slot 2: empty tag (all zeros).
	var slot2 erofsDeviceSlot
	require.NoError(t, binary.Write(f, binary.LittleEndian, &slot2))

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	assert.Equal(t, []string{"abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"}, got)
}
