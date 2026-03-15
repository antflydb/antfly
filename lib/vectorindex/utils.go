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

package vectorindex

import (
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
)

// PriorityItem represents a node with its calculated distance
type PriorityItem struct {
	ID          uint64
	Distance    float32
	ErrorBounds float32
	Metadata    []byte // Optional metadata
	Removed     bool   // Marks items superseded by better results for same collapse key
}

func (pi *PriorityItem) String() string {
	return fmt.Sprintf("PriorityItem{ ID: %d, Distance: %f }", pi.ID, pi.Distance)
}

// PriorityQueue implements heap.Interface and holds PriorityItems.
// It can function as both a min-heap (for results) and a max-heap (for candidates).
type PriorityQueue struct {
	items     []*PriorityItem
	isMaxHeap bool // true for max-heap (candidates), false for min-heap (results)
}

// NewPriorityQueue creates a new PriorityQueue.
func NewPriorityQueue(isMaxHeap bool, cap int) *PriorityQueue {
	pq := &PriorityQueue{
		items:     make([]*PriorityItem, 0, cap),
		isMaxHeap: isMaxHeap,
	}
	heap.Init(pq)
	return pq
}

func (pq *PriorityQueue) ShrinkTo(k int) {
	count := pq.Len()
	for count > k {
		heap.Pop(pq)
		count--
	}
}

func (pq *PriorityQueue) Clone(asMaxHeap bool) *PriorityQueue {
	// Create a new PriorityQueue with the same properties
	newPQ := &PriorityQueue{
		items:     make([]*PriorityItem, len(pq.items)),
		isMaxHeap: asMaxHeap,
	}
	copy(newPQ.items, pq.items)
	if asMaxHeap && !pq.isMaxHeap {
		heap.Init(newPQ)
	}
	return newPQ
}

func (pq *PriorityQueue) Items(k int) []*PriorityItem {
	k = min(k, pq.Len())
	// Return a copy of the items to avoid external modification
	itemsCopy := make([]*PriorityItem, pq.Len())
	copy(itemsCopy, pq.items)
	slices.SortFunc(itemsCopy, func(a, b *PriorityItem) int {
		// if pq.isMaxHeap {
		// 	// For max-heap, we want higher distances first
		// 	return cmp.Compare(a.Distance, b.Distance)
		// }
		// // For min-heap, we want lower distances first
		return cmp.Compare(a.Distance, b.Distance)
	})
	return itemsCopy[:k]
}

func (pq *PriorityQueue) Len() int { return len(pq.items) }

func (pq *PriorityQueue) Less(i, j int) bool {
	if pq.isMaxHeap {
		// Max heap compares distances directly (higher distance = higher priority)
		// Note: Often for candidates, we store negative distance and use min-heap logic.
		// Let's stick to explicit max-heap logic here for clarity.
		return pq.items[i].Distance > pq.items[j].Distance
	}
	// Min heap compares distances directly (lower distance = higher priority)
	return pq.items[i].Distance < pq.items[j].Distance
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(*PriorityItem)
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() any {
	n := len(pq.items)
	item := pq.items[n-1]
	pq.items[n-1] = nil // avoid memory leak
	pq.items = pq.items[0 : n-1]
	return item
}

// FIXME (ajr) This doesn't work at all
// Peek returns the bottom item without removing it.
// func (pq *PriorityQueue) PeekLast() *PriorityItem {
// 	if len(pq.items) == 0 {
// 		return nil
// 	}
// 	return pq.items[len(pq.items)-1]
// }

// cleanTopRemoved removes tombstoned items from the top of the heap
func (pq *PriorityQueue) cleanTopRemoved() {
	for len(pq.items) > 0 && pq.items[0].Removed {
		heap.Pop(pq)
	}
}

// Peek returns the top item without removing it, skipping tombstoned items
func (pq *PriorityQueue) Peek() *PriorityItem {
	pq.cleanTopRemoved()
	if len(pq.items) == 0 {
		return nil
	}
	return pq.items[0]
}

// PopNonRemoved pops items until finding a non-removed one, or returns nil
func (pq *PriorityQueue) PopNonRemoved() *PriorityItem {
	for len(pq.items) > 0 {
		item := heap.Pop(pq).(*PriorityItem)
		if !item.Removed {
			return item
		}
	}
	return nil
}

func GetVector(db pebble.Reader, key []byte, dst vector.T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				// FIXME (probably need to work out better shutdown semantics, this has a code smell)
				if errors.Is(e, pebble.ErrClosed) {
					err = e
					return
				}
			}
			panic(r)
		}
	}()
	// Use iterator with bounds instead of Get
	iterOpts := &pebble.IterOptions{
		LowerBound: key,
		UpperBound: utils.PrefixSuccessor(key),
	}

	iter, err := db.NewIterWithContext(context.Background(), iterOpts)
	if err != nil {
		return fmt.Errorf("creating iterator for %s: %w", key, err)
	}
	defer func() {
		_ = iter.Close()
	}()

	// Seek to the key
	if !iter.First() {
		return pebble.ErrNotFound
	}

	// Verify we found the exact key
	if !bytes.Equal(iter.Key(), key) {
		return pebble.ErrNotFound
	}

	// Skip hashID (first 8 bytes) before decoding vector
	// Embeddings are stored as [hashID:uint64][dimension:uint32][float32_data]
	value := iter.Value()
	if len(value) < 8 {
		return fmt.Errorf("value too short for %s: expected at least 8 bytes for hashID", key)
	}
	value = value[8:] // Skip the hashID

	if _, err := vector.DecodeTo(value, dst); err != nil {
		return fmt.Errorf("decoding vector for %s: %w", key, err)
	}

	return nil
}

// EncodeEmbeddingWithHashID encodes an embedding vector with its hashID prefix.
// Format: [hashID:uint64][dimension:uint32][float32_data...]
// This format is consistent with chunks and summaries which also use hashID prefix.
func EncodeEmbeddingWithHashID(appendTo []byte, embedding vector.T, hashID uint64) ([]byte, error) {
	// Encode hashID first
	appendTo = encoding.EncodeUint64Ascending(appendTo, hashID)
	// Then encode vector (dimension + float32 data)
	return vector.Encode(appendTo, embedding)
}

// DecodeEmbeddingWithHashID decodes an embedding value with hashID prefix.
// Format: [hashID:uint64][dimension:uint32][float32_data...]
// Returns the hashID and the decoded vector.
func DecodeEmbeddingWithHashID(value []byte) (hashID uint64, embedding vector.T, remaining []byte, err error) {
	// Decode hashID first (8 bytes)
	value, hashID, err = encoding.DecodeUint64Ascending(value)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("decoding hashID: %w", err)
	}
	// Then decode vector
	remaining, embedding, err = vector.Decode(value)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("decoding vector: %w", err)
	}
	return hashID, embedding, remaining, nil
}
