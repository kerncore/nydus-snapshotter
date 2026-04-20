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
func writeTestErofsImage(t *testing.T, sb erofsSuperBlock, slots []erofsDeviceSlot) string {
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

	// If there are slots, write them at the offset indicated by DevtSlotOff.
	if len(slots) > 0 {
		devTableOffset := int64(sb.DevtSlotOff) * erofsDeviceSlotSize
		_, err = f.Seek(devTableOffset, 0)
		require.NoError(t, err)

		for _, slot := range slots {
			require.NoError(t, binary.Write(f, binary.LittleEndian, &slot))
		}
	}

	return imgPath
}

// makeSlot creates a device slot with the given blob ID, block count and hi bits.
func makeSlot(blobID string, blocksLo uint32, blocksHi uint16) erofsDeviceSlot {
	var slot erofsDeviceSlot
	copy(slot.Tag[:], blobID)
	slot.BlocksLo = blocksLo
	slot.BlocksHi = blocksHi
	return slot
}

func TestParseDeviceTable_ValidImage(t *testing.T) {
	blobID1 := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	blobID2 := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	// BlkSzBits=12 means block size = 4096 bytes.
	sb := erofsSuperBlock{
		Magic:           erofsSuperMagicV1,
		BlkSzBits:       12,
		FeatureIncompat: erofsFeatureIncompatDeviceTable,
		ExtraDevices:    2,
		DevtSlotOff:     9,
	}

	slots := []erofsDeviceSlot{
		makeSlot(blobID1, 256, 0), // 256 blocks * 4096 = 1048576 bytes (1MB)
		makeSlot(blobID2, 512, 0), // 512 blocks * 4096 = 2097152 bytes (2MB)
	}

	imgPath := writeTestErofsImage(t, sb, slots)

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, blobID1, got[0].ID)
	assert.Equal(t, int64(256*4096), got[0].Size)

	assert.Equal(t, blobID2, got[1].ID)
	assert.Equal(t, int64(512*4096), got[1].Size)
}

func TestParseDeviceTable_BlobSizeWithHighBits(t *testing.T) {
	blobID := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	sb := erofsSuperBlock{
		Magic:           erofsSuperMagicV1,
		BlkSzBits:       12,
		FeatureIncompat: erofsFeatureIncompatDeviceTable,
		ExtraDevices:    1,
		DevtSlotOff:     9,
	}

	// BlocksHi=1, BlocksLo=0 → blocks = 1<<32 = 4294967296, size = 4294967296 * 4096
	slots := []erofsDeviceSlot{
		makeSlot(blobID, 0, 1),
	}

	imgPath := writeTestErofsImage(t, sb, slots)

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, int64((uint64(1)<<32)<<12), got[0].Size)
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
		FeatureIncompat: 0,
		ExtraDevices:    2,
	}

	imgPath := writeTestErofsImage(t, sb, nil)

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestParseDeviceTable_BadMagic(t *testing.T) {
	sb := erofsSuperBlock{Magic: 0xDEADBEEF}
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
	sb := erofsSuperBlock{
		Magic:           erofsSuperMagicV1,
		BlkSzBits:       12,
		FeatureIncompat: erofsFeatureIncompatDeviceTable,
		ExtraDevices:    2,
		DevtSlotOff:     9,
	}

	validID := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	slots := []erofsDeviceSlot{
		makeSlot(validID, 100, 0),
		{}, // empty tag — should be skipped
	}

	imgPath := writeTestErofsImage(t, sb, slots)

	got, err := parseDeviceTable(imgPath)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, validID, got[0].ID)
	assert.Equal(t, int64(100*4096), got[0].Size)
}

func TestCreateSparseFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparse.blob")

	err := createSparseFile(path, 1024*1024) // 1MB
	require.NoError(t, err)

	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(1024*1024), fi.Size())

	// Calling again with same size should be a no-op.
	err = createSparseFile(path, 1024*1024)
	require.NoError(t, err)
}

func TestCreateSparseFile_ZeroSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "zero.blob")

	err := createSparseFile(path, 0)
	require.NoError(t, err)

	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(0), fi.Size())
}
