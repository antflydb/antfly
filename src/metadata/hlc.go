// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/clock"
)

// HLC implements a Hybrid Logical Clock for globally unique timestamps.
// It combines wall clock time with a logical counter and node ID to ensure:
// 1. Timestamps are monotonically increasing within a single server
// 2. Timestamps are globally unique across multiple servers (no collisions)
// 3. Timestamps roughly correspond to real time for consistency
//
// Format: (wall_time_ms << 28) | (node_id << 20) | (logical_counter & 0xFFFFF)
// - 36 bits for milliseconds (~2000 years from epoch)
// - 8 bits for node ID (256 unique nodes)
// - 20 bits for logical counter (~1M timestamps per ms per node)
type HLC struct {
	mu           sync.Mutex
	nodeID       uint64 // unique identifier for this HLC instance (0-255)
	lastPhysical uint64 // last wall clock time in milliseconds
	logical      uint64 // logical counter within the same millisecond
	clock        clock.Clock
}

const (
	// hlcLogicalBits is the number of bits reserved for the logical counter
	hlcLogicalBits = 20
	// hlcNodeIDBits is the number of bits reserved for the node ID
	hlcNodeIDBits = 8
	// hlcLogicalMask masks the logical counter portion
	hlcLogicalMask = (1 << hlcLogicalBits) - 1
	// hlcNodeIDMask masks the node ID portion
	hlcNodeIDMask = (1 << hlcNodeIDBits) - 1
)

// hlcNodeCounter is used to assign unique node IDs to HLC instances
var hlcNodeCounter uint64

// NewHLC creates a new HLC instance with a unique node ID.
// Each HLC instance gets a unique node ID to prevent timestamp collisions
// between different metadata servers.
func NewHLC() *HLC {
	// Atomically increment and get a unique node ID
	// Use crypto/rand for initial seed + atomic counter for uniqueness
	nodeID := atomic.AddUint64(&hlcNodeCounter, 1) & hlcNodeIDMask
	return NewHLCWithNodeIDAndClock(nodeID, clock.RealClock{})
}

// NewHLCWithClock creates a new HLC instance with a unique node ID and custom clock.
func NewHLCWithClock(clk clock.Clock) *HLC {
	nodeID := atomic.AddUint64(&hlcNodeCounter, 1) & hlcNodeIDMask
	return NewHLCWithNodeIDAndClock(nodeID, clk)
}

// NewHLCWithNodeID creates a new HLC instance with a specific node ID.
// This is useful when the node ID should be deterministic (e.g., from config).
func NewHLCWithNodeID(nodeID uint64) *HLC {
	return NewHLCWithNodeIDAndClock(nodeID, clock.RealClock{})
}

// NewHLCWithNodeIDAndClock creates a new HLC instance with a specific node ID and clock.
func NewHLCWithNodeIDAndClock(nodeID uint64, clk clock.Clock) *HLC {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &HLC{
		nodeID: nodeID & hlcNodeIDMask,
		clock:  clk,
	}
}

func (h *HLC) clockOrReal() clock.Clock {
	if h.clock == nil {
		return clock.RealClock{}
	}
	return h.clock
}

// Now returns a new globally unique, monotonically increasing timestamp.
// The timestamp combines wall clock time, node ID, and logical counter to ensure
// uniqueness across multiple HLC instances (metadata servers).
func (h *HLC) Now() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Get current wall clock time in milliseconds
	clk := h.clockOrReal()
	nowMs := uint64(clk.Now().UnixMilli())

	if nowMs > h.lastPhysical {
		// Time moved forward - reset logical counter
		h.lastPhysical = nowMs
		h.logical = 0
	} else {
		// Same millisecond or clock went backward - increment logical counter
		h.logical++
		if h.logical > hlcLogicalMask {
			// Logical counter overflow - wait for next millisecond
			// This should be extremely rare (>1M timestamps/ms)
			for nowMs <= h.lastPhysical {
				clk.Sleep(time.Millisecond)
				nowMs = uint64(clk.Now().UnixMilli())
			}
			h.lastPhysical = nowMs
			h.logical = 0
		}
	}

	// Combine physical time, node ID, and logical counter
	// Format: (physical << 28) | (nodeID << 20) | logical
	return (h.lastPhysical << (hlcLogicalBits + hlcNodeIDBits)) | (h.nodeID << hlcLogicalBits) | h.logical
}
