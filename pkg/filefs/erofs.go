//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
)

// EROFS on-disk constants from linux/fs/erofs/erofs_fs.h.
const (
	erofsSuperOffset  = 1024
	erofsSuperMagicV1 = 0xE0F5E1E2

	// EROFS_FEATURE_INCOMPAT_DEVICE_TABLE indicates the image has a device table
	// mapping external blob devices.
	erofsFeatureIncompatDeviceTable = 0x00000008

	// Size of struct erofs_deviceslot on disk.
	erofsDeviceSlotSize = 128

	// Size of the on-disk superblock struct we need to read.
	// We only need fields up to devt_slotoff at offset 88 (2 bytes), so 90 bytes suffice.
	erofsSuperReadSize = 90
)

// erofsSuperBlock mirrors the first 90 bytes of the on-disk erofs_super_block.
// Field offsets match the kernel's struct erofs_super_block exactly.
type erofsSuperBlock struct {
	Magic           uint32   // offset 0
	Checksum        uint32   // offset 4
	FeatureCompat   uint32   // offset 8
	BlkSzBits       uint8    // offset 12
	SbExtSlots      uint8    // offset 13
	RbRootNid       uint16   // offset 14 (union)
	Inos            uint64   // offset 16
	Epoch           uint64   // offset 24
	FixedNsec       uint32   // offset 32
	BlocksLo        uint32   // offset 36
	MetaBlkAddr     uint32   // offset 40
	XattrBlkAddr    uint32   // offset 44
	UUID            [16]byte // offset 48
	VolumeName      [16]byte // offset 64
	FeatureIncompat uint32   // offset 80
	U1              uint16   // offset 84 (union: available_compr_algs / lz4_max_distance)
	ExtraDevices    uint16   // offset 86
	DevtSlotOff     uint16   // offset 88
}

// erofsDeviceSlot mirrors the kernel's struct erofs_deviceslot (128 bytes).
// Only the Tag field is needed for blob ID extraction.
type erofsDeviceSlot struct {
	Tag       [64]byte // sha256 hex blob ID, null-padded
	BlocksLo  uint32
	UniAddr   uint32
	BlocksHi  uint16
	UniAddrHi uint16
	Reserved  [52]byte
}

// blobInfo holds metadata for an external blob referenced by the EROFS device table.
type blobInfo struct {
	ID   string // sha256 hex blob ID from device slot Tag
	Size int64  // blob size in bytes, computed from BlocksLo/BlocksHi + BlkSzBits
}

// parseDeviceTable reads the EROFS superblock from the bootstrap file and
// extracts blob IDs and sizes from the device table. Returns an empty slice
// (not an error) if the image has no device table or no external devices.
func parseDeviceTable(bootstrapPath string) ([]blobInfo, error) {
	f, err := os.Open(bootstrapPath)
	if err != nil {
		return nil, errors.Wrapf(err, "open bootstrap %s", bootstrapPath)
	}
	defer f.Close()

	// Read superblock at EROFS_SUPER_OFFSET (1024 bytes from start).
	if _, err := f.Seek(erofsSuperOffset, io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "seek to erofs superblock")
	}

	var sb erofsSuperBlock
	if err := binary.Read(f, binary.LittleEndian, &sb); err != nil {
		return nil, errors.Wrap(err, "read erofs superblock")
	}

	if sb.Magic != erofsSuperMagicV1 {
		return nil, errors.Errorf("invalid erofs magic: 0x%08X (expected 0x%08X)", sb.Magic, erofsSuperMagicV1)
	}

	// If no device table feature, there are no external blob references.
	if sb.FeatureIncompat&erofsFeatureIncompatDeviceTable == 0 {
		return nil, nil
	}

	if sb.ExtraDevices == 0 {
		return nil, nil
	}

	// Device table is at absolute byte offset devt_slotoff * 128 in the image file.
	devTableOffset := int64(sb.DevtSlotOff) * erofsDeviceSlotSize
	if _, err := f.Seek(devTableOffset, io.SeekStart); err != nil {
		return nil, errors.Wrapf(err, "seek to device table at offset %d", devTableOffset)
	}

	blobs := make([]blobInfo, 0, sb.ExtraDevices)
	for i := uint16(0); i < sb.ExtraDevices; i++ {
		var slot erofsDeviceSlot
		if err := binary.Read(f, binary.LittleEndian, &slot); err != nil {
			return nil, errors.Wrapf(err, "read device slot %d", i)
		}

		// Extract blob ID from the tag field (null-terminated sha256 hex string).
		blobID := string(bytes.TrimRight(slot.Tag[:], "\x00"))
		if blobID == "" {
			continue
		}

		// Compute blob size: (BlocksHi<<32 | BlocksLo) << BlkSzBits
		blocks := (uint64(slot.BlocksHi) << 32) | uint64(slot.BlocksLo)
		blobSize := int64(blocks << sb.BlkSzBits)

		blobs = append(blobs, blobInfo{
			ID:   blobID,
			Size: blobSize,
		})
	}

	return blobs, nil
}
