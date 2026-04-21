//go:build linux

/*
 * Copyright (c) 2024. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filefs

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/containerd/log"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// fanotifyResponseDeny prevents the pending I/O operation from proceeding.
const fanotifyResponseDeny = 0x02

// Fanotify constants for pre-content access control.
// These may not yet be defined in golang.org/x/sys/unix for newer kernel features.
const (
	// FAN_CLASS_PRE_CONTENT allows the listener to receive events before
	// the file content is made available to the requesting process.
	fanClassPreContent = 0x00000008

	// FAN_PRE_ACCESS is the event mask for pre-content access events.
	// When a process tries to read file content, this event fires before
	// the data is delivered, allowing the listener to populate the backing store.
	fanPreAccess = 0x00080000

	// FAN_MARK_FILESYSTEM marks the entire filesystem for monitoring.
	fanMarkFilesystem = 0x00000100

	// fanotifyResponseAllow permits the pending I/O operation to proceed.
	fanotifyResponseAllow = 0x01

	// Size of struct fanotify_event_metadata.
	fanotifyEventMetadataSize = 24

	// Maximum number of times to restart the event loop after a panic.
	maxEventLoopRestarts = 3
)

// fanotifyEventMetadata mirrors the kernel's struct fanotify_event_metadata.
type fanotifyEventMetadata struct {
	EventLen    uint32
	VersInField uint8
	Reserved    uint8
	MetadataLen uint16
	Mask        uint64
	Fd          int32
	Pid         int32
}

// fanotifyResponse mirrors the kernel's struct fanotify_response.
type fanotifyResponse struct {
	Fd       int32
	Response uint32
}

// setupFanotify initializes fanotify with FAN_CLASS_PRE_CONTENT on the given
// mountpoint and starts the event loop goroutine.
func (m *Manager) setupFanotify(st *snapshotState, mountPoint string) error {
	// Initialize fanotify group with pre-content class.
	// FAN_NONBLOCK allows non-blocking reads for clean shutdown.
	fd, err := unix.FanotifyInit(fanClassPreContent|unix.FAN_NONBLOCK, unix.O_RDONLY|unix.O_LARGEFILE)
	if err != nil {
		return errors.Wrap(err, "fanotify_init with FAN_CLASS_PRE_CONTENT")
	}
	st.fanotifyFd = fd

	// Open the mountpoint directory to get an fd for fanotify_mark.
	mntFd, err := unix.Open(mountPoint, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		unix.Close(fd)
		return errors.Wrapf(err, "open mountpoint %s for fanotify_mark", mountPoint)
	}

	// Mark the entire filesystem at the mountpoint for pre-access events.
	err = unix.FanotifyMark(fd,
		unix.FAN_MARK_ADD|fanMarkFilesystem,
		fanPreAccess,
		mntFd, "")
	unix.Close(mntFd)
	if err != nil {
		unix.Close(fd)
		return errors.Wrapf(err, "fanotify_mark FAN_PRE_ACCESS on %s", mountPoint)
	}

	log.L.Infof("Fanotify pre-content listener set up on %s (fd=%d)", mountPoint, fd)

	// Start the event processing goroutine with panic recovery and restart.
	// A panicked or crashed event loop would leave kernel I/O blocked
	// indefinitely, so we recover, drain pending events, and retry.
	st.wg.Add(1)
	go func() {
		defer st.wg.Done()
		for attempt := 0; attempt <= maxEventLoopRestarts; attempt++ {
			normalExit := m.runEventLoop(st, attempt)
			if normalExit {
				return // stopCh was closed, clean shutdown
			}
			// Check if we should stop before restarting.
			select {
			case <-st.stopCh:
				return
			default:
			}
		}
		log.L.Errorf("fanotify: event loop exhausted %d restart attempts", maxEventLoopRestarts)
	}()

	return nil
}

// runEventLoop runs the fanotify event loop with panic recovery.
// Returns true if the loop exited normally (stopCh closed), false if it panicked.
func (m *Manager) runEventLoop(st *snapshotState, attempt int) (normalExit bool) {
	defer func() {
		if r := recover(); r != nil {
			log.L.Errorf("fanotify: event loop panicked (attempt %d/%d): %v",
				attempt+1, maxEventLoopRestarts+1, r)
			m.drainPendingEvents(st)
			normalExit = false
		}
	}()
	m.fanotifyEventLoop(st)
	return true
}

// fanotifyEventLoop reads fanotify events and handles on-demand data loading.
func (m *Manager) fanotifyEventLoop(st *snapshotState) {
	buf := make([]byte, 4096*fanotifyEventMetadataSize)

	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		log.L.WithError(err).Error("fanotify: failed to create epoll fd")
		return
	}
	defer unix.Close(epollFd)

	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(st.fanotifyFd),
	}
	if err := unix.EpollCtl(epollFd, unix.EPOLL_CTL_ADD, st.fanotifyFd, &event); err != nil {
		log.L.WithError(err).Error("fanotify: failed to add fd to epoll")
		return
	}

	events := make([]unix.EpollEvent, 1)

	for {
		select {
		case <-st.stopCh:
			log.L.Debug("fanotify: event loop stopped by signal")
			return
		default:
		}

		// Wait for events with a 200ms timeout to periodically check stopCh.
		n, err := unix.EpollWait(epollFd, events, 200)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			// If the fanotify fd was closed, exit gracefully.
			select {
			case <-st.stopCh:
				return
			default:
			}
			log.L.WithError(err).Error("fanotify: epoll_wait failed")
			return
		}
		if n == 0 {
			continue
		}

		nRead, err := unix.Read(st.fanotifyFd, buf)
		if err != nil {
			if err == unix.EAGAIN {
				continue
			}
			select {
			case <-st.stopCh:
				return
			default:
			}
			log.L.WithError(err).Error("fanotify: read failed")
			return
		}

		m.handleFanotifyEvents(st, buf[:nRead])
	}
}

// handleFanotifyEvents parses fanotify event metadata from the buffer,
// fetches needed data, and sends allow/deny responses.
// Each event is handled independently — a failure in one event does not
// prevent processing of subsequent events.
func (m *Manager) handleFanotifyEvents(st *snapshotState, buf []byte) {
	offset := 0

	for offset+fanotifyEventMetadataSize <= len(buf) {
		meta := (*fanotifyEventMetadata)(unsafe.Pointer(&buf[offset]))

		if meta.EventLen < fanotifyEventMetadataSize {
			break
		}

		if meta.Mask&fanPreAccess != 0 {
			m.handlePreAccessEvent(st, meta)
		} else if meta.Fd >= 0 {
			// Unknown event type — allow and close fd.
			m.sendFanotifyResponse(st, meta.Fd, fanotifyResponseAllow)
			unix.Close(int(meta.Fd))
		}

		offset += int(meta.EventLen)
	}
}

// handlePreAccessEvent processes a single FAN_PRE_ACCESS event.
// It ensures all blobs for the snapshot are fetched, then allows the access.
// The event fd is always closed to prevent fd leaks.
func (m *Manager) handlePreAccessEvent(st *snapshotState, meta *fanotifyEventMetadata) {
	if meta.Fd < 0 {
		m.sendFanotifyResponse(st, meta.Fd, fanotifyResponseAllow)
		return
	}

	// Ensure the event fd is always closed regardless of outcome.
	defer unix.Close(int(meta.Fd))

	// Ensure all blobs for this snapshot are available.
	if err := m.ensureSnapshotBlobsFetched(st); err != nil {
		log.L.WithError(err).Warn("fanotify: blob fetch failed, denying access")
		m.sendFanotifyResponse(st, meta.Fd, fanotifyResponseDeny)
		return
	}

	m.sendFanotifyResponse(st, meta.Fd, fanotifyResponseAllow)
}

// ensureSnapshotBlobsFetched waits for all blobs referenced by the snapshot
// to be fully downloaded. Downloads are started at mount time by StartPrefetch;
// this method blocks until they complete.
func (m *Manager) ensureSnapshotBlobsFetched(st *snapshotState) error {
	if len(st.blobs) == 0 {
		return nil
	}
	return m.fetcher.WaitForBlobs(context.Background(), st.blobs)
}

// sendFanotifyResponse writes a fanotify_response struct to allow or deny an event.
func (m *Manager) sendFanotifyResponse(st *snapshotState, fd int32, response uint32) {
	resp := fanotifyResponse{
		Fd:       fd,
		Response: response,
	}
	respBytes := (*[unsafe.Sizeof(resp)]byte)(unsafe.Pointer(&resp))
	if _, err := unix.Write(st.fanotifyFd, respBytes[:]); err != nil {
		log.L.WithError(err).Warn("fanotify: failed to write response")
	}
}

// procFdPath returns the /proc/self/fd/<fd> path for resolving an fd's target.
func procFdPath(fd int32) string {
	if fd < 0 {
		return ""
	}
	// Use a byte buffer to avoid fmt.Sprintf allocations in the hot path.
	const prefix = "/proc/self/fd/"
	buf := make([]byte, len(prefix)+10)
	copy(buf, prefix)
	n := len(prefix)
	val := uint32(fd)
	if val == 0 {
		buf[n] = '0'
		n++
	} else {
		start := n
		for val > 0 {
			buf[n] = byte('0' + val%10)
			val /= 10
			n++
		}
		// Reverse the digits.
		for i, j := start, n-1; i < j; i, j = i+1, j-1 {
			buf[i], buf[j] = buf[j], buf[i]
		}
	}
	return string(buf[:n])
}

// readlinkSafe reads a symlink target, returning the resolved path.
func readlinkSafe(path string) (string, error) {
	buf := make([]byte, 256)
	for {
		n, err := unix.Readlink(path, buf)
		if err != nil {
			return "", err
		}
		if n < len(buf) {
			return string(buf[:n]), nil
		}
		buf = make([]byte, len(buf)*2)
	}
}

// drainPendingEvents reads any remaining fanotify events and responds with
// FAN_DENY to unblock kernel I/O. This is a safety net used when the event
// loop exits unexpectedly (panic, unrecoverable error).
func (m *Manager) drainPendingEvents(st *snapshotState) {
	if st.fanotifyFd < 0 {
		return
	}

	buf := make([]byte, 4096*fanotifyEventMetadataSize)
	for {
		n, err := unix.Read(st.fanotifyFd, buf)
		if err != nil {
			// EAGAIN means no more events; any other error means fd is closed or broken.
			return
		}
		if n == 0 {
			return
		}

		offset := 0
		for offset+fanotifyEventMetadataSize <= n {
			meta := (*fanotifyEventMetadata)(unsafe.Pointer(&buf[offset]))
			if meta.EventLen < fanotifyEventMetadataSize {
				break
			}
			if meta.Fd >= 0 {
				m.sendFanotifyResponse(st, meta.Fd, fanotifyResponseDeny)
				unix.Close(int(meta.Fd))
			}
			offset += int(meta.EventLen)
		}
	}
}

// fanotifyResponseString returns a string representation for fanotify response codes (for debugging).
func fanotifyResponseString(r uint32) string {
	switch r {
	case fanotifyResponseAllow:
		return "ALLOW"
	case fanotifyResponseDeny:
		return "DENY"
	default:
		return fmt.Sprintf("0x%02x", r)
	}
}
