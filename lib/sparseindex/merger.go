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

package sparseindex

import (
	"bytes"
	"io"
	"math"
	"sort"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/cockroachdb/pebble/v2"
)

// RegisterChunkMerger registers the sparse index chunk merge strategy with
// the given pebbleutils.Registry. The prefix should match the SparseIndex's
// key prefix (or nil if the sparse index owns the entire DB).
func RegisterChunkMerger(reg *pebbleutils.Registry, prefix []byte) {
	// Register for "inv:" keys under the prefix. The merge function further
	// checks for ":chunk" to only merge chunk keys, falling back to
	// last-write-wins for meta keys.
	invPrefix := append(bytes.Clone(prefix), "inv:"...)
	reg.Register(invPrefix, func(key, value []byte) (pebble.ValueMerger, error) {
		if !isChunkKey(key) {
			return pebbleutils.NewLastWriteWins(value), nil
		}
		return newChunkValueMerger(value)
	})
}

// isChunkKey returns true if the key contains ":chunk" (as opposed to ":meta").
func isChunkKey(key []byte) bool {
	return bytes.Contains(key, []byte(":chunk"))
}

// chunkValueMerger implements pebble.ValueMerger and pebble.DeletableValueMerger
// for sparse index chunk keys. It accumulates add/delete merge operands and
// resolves them in Finish().
type chunkValueMerger struct {
	// base holds the initial value. If version 1, it's a full encoded chunk.
	// If version 2, it's a merge operand.
	base []byte
	// operands accumulates subsequent merge operands in encounter order.
	operands [][]byte
}

func newChunkValueMerger(value []byte) (*chunkValueMerger, error) {
	return &chunkValueMerger{
		base: append([]byte(nil), value...),
	}, nil
}

func (m *chunkValueMerger) MergeNewer(value []byte) error {
	m.operands = append(m.operands, append([]byte(nil), value...))
	return nil
}

func (m *chunkValueMerger) MergeOlder(value []byte) error {
	m.operands = append(m.operands, append([]byte(nil), value...))
	return nil
}

// resolve collects all entries from the base value and operands, applies
// deletes, and returns the resolved doc nums, weights, and whether the
// result is empty.
func (m *chunkValueMerger) resolve() (docNums []uint64, weights []float32, empty bool, err error) {
	type entry struct {
		docNum uint64
		weight float32
	}
	var entries []entry
	deleteSet := make(map[uint32]struct{})

	// Process a single encoded value (either chunk v1 or operand v2).
	process := func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		version := data[0]
		switch version {
		case formatVersion: // v1: full encoded chunk
			dn, w, _, decErr := decodeChunk(data)
			if decErr != nil {
				return decErr
			}
			for i, d := range dn {
				entries = append(entries, entry{docNum: d, weight: w[i]})
			}
		case mergeOperandVersion: // v2: merge operand
			opType, dns, ws, decErr := decodeMergeOperand(data)
			if decErr != nil {
				return decErr
			}
			switch opType {
			case opTypeAdd:
				for i, dn := range dns {
					entries = append(entries, entry{docNum: uint64(dn), weight: ws[i]})
				}
			case opTypeDelete:
				for _, dn := range dns {
					deleteSet[dn] = struct{}{}
				}
			}
		default:
			// Unknown version — treat as opaque (shouldn't happen).
			return nil
		}
		return nil
	}

	if err := process(m.base); err != nil {
		return nil, nil, false, err
	}
	for _, op := range m.operands {
		if err := process(op); err != nil {
			return nil, nil, false, err
		}
	}

	// Apply deletes
	if len(deleteSet) > 0 {
		filtered := entries[:0]
		for _, e := range entries {
			if _, deleted := deleteSet[uint32(e.docNum)]; !deleted { //nolint:gosec // G115: bounded value, cannot overflow in practice
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}

	if len(entries) == 0 {
		return nil, nil, true, nil
	}

	// Deduplicate by docNum (keep last occurrence — newest wins)
	seen := make(map[uint64]int, len(entries))
	for i, e := range entries {
		seen[e.docNum] = i
	}
	if len(seen) < len(entries) {
		deduped := make([]entry, 0, len(seen))
		for i, e := range entries {
			if seen[e.docNum] == i {
				deduped = append(deduped, e)
			}
		}
		entries = deduped
	}

	// Sort by doc num
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].docNum < entries[j].docNum
	})

	docNums = make([]uint64, len(entries))
	weights = make([]float32, len(entries))
	for i, e := range entries {
		docNums[i] = e.docNum
		weights[i] = e.weight
	}
	return docNums, weights, false, nil
}

func (m *chunkValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if includesBase {
		return m.finishFull()
	}
	return m.finishPartial()
}

// finishFull produces a standard v1 encoded chunk from all resolved entries.
func (m *chunkValueMerger) finishFull() ([]byte, io.Closer, error) {
	docNums, weights, empty, err := m.resolve()
	if err != nil {
		return nil, nil, err
	}
	if empty {
		// Return a minimal valid chunk. DeletableFinish handles true deletion.
		return encodeMergeOperand(opTypeAdd, nil, nil), nil, nil
	}
	c := &chunk{DocNums: docNums, Weights: weights}
	data, err := encodeChunk(c)
	if err != nil {
		return nil, nil, err
	}
	return data, nil, nil
}

// finishPartial produces a consolidated v2 merge operand for partial compaction.
func (m *chunkValueMerger) finishPartial() ([]byte, io.Closer, error) {
	docNums, weights, empty, err := m.resolve()
	if err != nil {
		return nil, nil, err
	}
	if empty {
		return encodeMergeOperand(opTypeAdd, nil, nil), nil, nil
	}
	// Encode as a single add operand with absolute doc nums and raw weights.
	dns := make([]uint32, len(docNums))
	for i, dn := range docNums {
		if dn > math.MaxUint32 {
			dns[i] = math.MaxUint32
		} else {
			dns[i] = uint32(dn)
		}
	}
	return encodeMergeOperand(opTypeAdd, dns, weights), nil, nil
}

// DeletableFinish implements pebble.DeletableValueMerger. When all entries
// have been deleted, it returns delete=true to elide the key during compaction.
func (m *chunkValueMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	docNums, weights, empty, err := m.resolve()
	if err != nil {
		return nil, false, nil, err
	}
	if empty && includesBase {
		return nil, true, nil, nil
	}
	if empty {
		data := encodeMergeOperand(opTypeAdd, nil, nil)
		return data, false, nil, nil
	}

	if includesBase {
		c := &chunk{DocNums: docNums, Weights: weights}
		data, err := encodeChunk(c)
		if err != nil {
			return nil, false, nil, err
		}
		return data, false, nil, nil
	}

	dns := make([]uint32, len(docNums))
	for i, dn := range docNums {
		if dn > math.MaxUint32 {
			dns[i] = math.MaxUint32
		} else {
			dns[i] = uint32(dn)
		}
	}
	return encodeMergeOperand(opTypeAdd, dns, weights), false, nil, nil
}

// Verify interface compliance at compile time.
var _ pebble.ValueMerger = (*chunkValueMerger)(nil)
