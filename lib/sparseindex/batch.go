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
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"
)

// Batch performs a batch of inserts and deletes atomically.
// Inserts and deletes are coalesced by chunk key to minimize Pebble I/O.
func (si *SparseIndex) Batch(inserts []BatchInsert, deletes [][]byte) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	batch := si.db.NewIndexedBatchWithSize(4 << 20)
	defer func() { _ = batch.Close() }()

	// Process deletes first
	for _, docID := range deletes {
		var err error
		if si.useMerge {
			err = si.deleteDocMerge(batch, docID)
		} else {
			err = si.deleteDoc(batch, docID)
		}
		if err != nil {
			return fmt.Errorf("deleting doc %s: %w", docID, err)
		}
	}

	// Assign doc numbers and process inserts
	docCount, err := si.getDocCountFrom(batch)
	if err != nil {
		return fmt.Errorf("getting doc count: %w", err)
	}

	// Collect all chunk modifications keyed by (termID, chunkNum) to avoid
	// string key allocations from fmt.Appendf + string conversion.
	type chunkModKey struct {
		termID   uint32
		chunkNum uint64
	}
	type chunkMod struct {
		docNum uint64
		docID  []byte
		weight float32
	}
	chunkMods := make(map[chunkModKey][]chunkMod)

	for _, ins := range inserts {
		sortSparseVec(ins.Vec)

		docNum := docCount
		docCount++

		// Write forward index
		fwd := &fwdEntry{
			DocNum: docNum,
			Vec:    ins.Vec,
		}
		fwdKey := si.fwdKey(ins.DocID)
		if err := batch.Set(fwdKey, encodeFwdEntry(fwd), nil); err != nil {
			return fmt.Errorf("writing forward index for %s: %w", ins.DocID, err)
		}

		// Write reverse mapping: docNum → docID for efficient lookup during search
		revKey := si.revKey(docNum)
		if err := batch.Set(revKey, ins.DocID, nil); err != nil {
			return fmt.Errorf("writing reverse index for docNum %d: %w", docNum, err)
		}
		si.revCache[docNum] = bytes.Clone(ins.DocID)

		// Collect chunk modifications for each term
		chunkNum := uint64(docNum) / uint64(si.chunkSize) //nolint:gosec // G115: bounded value, cannot overflow in practice
		indices := ins.Vec.GetIndices()
		values := ins.Vec.GetValues()
		for i, termID := range indices {
			ck := chunkModKey{termID: termID, chunkNum: chunkNum}
			chunkMods[ck] = append(chunkMods[ck], chunkMod{
				docNum: docNum,
				docID:  ins.DocID,
				weight: values[i],
			})
		}
	}

	// Apply chunk modifications
	termMaxWeights := make(map[uint32]float32) // track max weight per term
	termChunkCounts := make(map[uint32]uint32) // track chunk count per term

	if si.useMerge {
		// Merge path: encode new entries as merge operands and call
		// batch.Merge(). No Pebble reads needed — deltas are resolved
		// lazily during compaction or search reads.
		for ck, mods := range chunkMods {
			chunkKey := si.invChunkKey(ck.termID, ck.chunkNum)

			// Build merge operand from this batch's entries
			docNums := make([]uint32, len(mods))
			weights := make([]float32, len(mods))
			for i, mod := range mods {
				docNums[i] = uint32(mod.docNum) //nolint:gosec // G115: bounded value, cannot overflow in practice
				weights[i] = mod.weight
			}
			operand := encodeMergeOperand(opTypeAdd, docNums, weights)

			if err := batch.Merge(chunkKey, operand, nil); err != nil {
				return fmt.Errorf("merging chunk for term %d: %w", ck.termID, err)
			}

			// Invalidate chunk cache — the cached entry is now stale
			if si.ccache != nil {
				si.ccache.Delete(string(chunkKey))
			}

			// Track term max weights for metadata update
			for _, w := range weights {
				if w > termMaxWeights[ck.termID] {
					termMaxWeights[ck.termID] = w
				}
			}
		}
	} else {
		// Read-modify-write path: read existing chunk, merge entries, write back.
		for ck, mods := range chunkMods {
			chunkKey := si.invChunkKey(ck.termID, ck.chunkNum)
			termID := ck.termID
			chunkNum := ck.chunkNum

			// Read existing chunk if any
			var existingDocNums []uint64
			var existingDocIDs [][]byte
			var existingWeights []float32

			chunkCacheKey := string(chunkKey)
			if si.ccache != nil {
				if dc, ok := si.ccache.Get(chunkCacheKey); ok {
					existingDocNums = dc.DocNums
					existingWeights = dc.Weights
					existingDocIDs = make([][]byte, len(existingDocNums))
				}
			}
			if existingDocNums == nil {
				val, closer, err := batch.Get(chunkKey)
				if err == nil {
					chunkData := bytes.Clone(val)
					_ = closer.Close()
					existingDocNums, existingWeights, _, err = decodeChunk(chunkData)
					if err != nil {
						return fmt.Errorf("decoding chunk for term %d chunk %d: %w", termID, chunkNum, err)
					}
					existingDocIDs = make([][]byte, len(existingDocNums))
				} else if err != pebble.ErrNotFound {
					return fmt.Errorf("reading chunk for term %d chunk %d: %w", termID, chunkNum, err)
				}
			}

			// Merge existing + new entries
			allDocNums := append(existingDocNums, make([]uint64, len(mods))...)
			allDocIDs := append(existingDocIDs, make([][]byte, len(mods))...)
			allWeights := append(existingWeights, make([]float32, len(mods))...)
			base := len(existingDocNums)
			for i, mod := range mods {
				allDocNums[base+i] = mod.docNum
				allDocIDs[base+i] = mod.docID
				allWeights[base+i] = mod.weight
			}

			// Sort by doc num. In append-only workloads (no interleaved deletes),
			// new docNums are always greater than existing docNums, so the
			// concatenation is already sorted. Only sort when needed.
			needsSort := len(existingDocNums) > 0 && len(mods) > 0 &&
				existingDocNums[len(existingDocNums)-1] >= mods[0].docNum
			if needsSort {
				sortChunkEntries(allDocNums, allDocIDs, allWeights)
			}

			// Check for chunk overflow: split if needed
			if len(allDocNums) > si.chunkSize {
				// Split into two chunks
				mid := len(allDocNums) / 2
				leftChunk := &chunk{
					DocNums: allDocNums[:mid],
					DocIDs:  allDocIDs[:mid],
					Weights: allWeights[:mid],
				}
				rightChunk := &chunk{
					DocNums: allDocNums[mid:],
					DocIDs:  allDocIDs[mid:],
					Weights: allWeights[mid:],
				}

				leftData, err := encodeChunk(leftChunk)
				if err != nil {
					return fmt.Errorf("encoding left chunk: %w", err)
				}
				if err := batch.Set(chunkKey, leftData, nil); err != nil {
					return fmt.Errorf("writing left chunk: %w", err)
				}
				if si.ccache != nil {
					si.ccache.Set(chunkCacheKey, &decodedChunk{
						DocNums:   leftChunk.DocNums,
						Weights:   leftChunk.Weights,
						MaxWeight: maxFloat32(leftChunk.Weights),
					})
				}

				// Right chunk gets a new chunk key based on its lowest doc num.
				// Probe forward to avoid colliding with existing chunk keys.
				rightChunkNum := rightChunk.DocNums[0] / uint64(si.chunkSize) //nolint:gosec // G115: bounded value, cannot overflow in practice
				if rightChunkNum <= chunkNum {
					rightChunkNum = chunkNum + 1
				}
				rightKey := si.invChunkKey(termID, rightChunkNum)
				for {
					if _, closer, err := batch.Get(rightKey); err == pebble.ErrNotFound {
						break // key is free
					} else if err == nil {
						_ = closer.Close()
						rightChunkNum++
						rightKey = si.invChunkKey(termID, rightChunkNum)
					} else {
						return fmt.Errorf("probing chunk key for term %d chunk %d: %w", termID, rightChunkNum, err)
					}
				}
				rightData, err := encodeChunk(rightChunk)
				if err != nil {
					return fmt.Errorf("encoding right chunk: %w", err)
				}
				if err := batch.Set(rightKey, rightData, nil); err != nil {
					return fmt.Errorf("writing right chunk: %w", err)
				}
				if si.ccache != nil {
					si.ccache.Set(string(rightKey), &decodedChunk{
						DocNums:   rightChunk.DocNums,
						Weights:   rightChunk.Weights,
						MaxWeight: maxFloat32(rightChunk.Weights),
					})
				}

				// Update term tracking
				updateTermMax(termMaxWeights, termID, leftChunk.Weights)
				updateTermMax(termMaxWeights, termID, rightChunk.Weights)
				// Count chunks (at least 2 now)
				if _, ok := termChunkCounts[termID]; !ok {
					termChunkCounts[termID] = 2
				} else {
					termChunkCounts[termID]++
				}
			} else {
				// Write merged chunk
				c := &chunk{
					DocNums: allDocNums,
					DocIDs:  allDocIDs,
					Weights: allWeights,
				}
				data, err := encodeChunk(c)
				if err != nil {
					return fmt.Errorf("encoding chunk: %w", err)
				}
				if err := batch.Set(chunkKey, data, nil); err != nil {
					return fmt.Errorf("writing chunk: %w", err)
				}
				if si.ccache != nil {
					si.ccache.Set(chunkCacheKey, &decodedChunk{
						DocNums:   allDocNums,
						Weights:   allWeights,
						MaxWeight: maxFloat32(allWeights),
					})
				}

				updateTermMax(termMaxWeights, termID, allWeights)
				if _, ok := termChunkCounts[termID]; !ok {
					termChunkCounts[termID] = 1
				}
			}
		}
	} // end else (non-merge path)

	// Update term metadata
	for termID, maxW := range termMaxWeights {
		// Check in-memory cache first (authoritative under mu.Lock)
		existingMeta, cached := si.termMetaCache[termID]
		if !cached {
			existingMeta = &termMeta{}
			metaKey := si.invMetaKey(termID)
			val, closer, err := batch.Get(metaKey)
			if err == nil {
				existingMeta, err = decodeTermMeta(val)
				_ = closer.Close()
				if err != nil {
					return fmt.Errorf("decoding term meta for %d: %w", termID, err)
				}
			} else if err != pebble.ErrNotFound {
				return fmt.Errorf("reading term meta for %d: %w", termID, err)
			}
		} else {
			// Clone so we don't mutate the cached pointer until we're ready
			existingMeta = &termMeta{
				MaxWeight:  existingMeta.MaxWeight,
				ChunkCount: existingMeta.ChunkCount,
			}
		}

		changed := false
		if maxW > existingMeta.MaxWeight {
			existingMeta.MaxWeight = maxW
			changed = true
		}
		if cc, ok := termChunkCounts[termID]; ok && cc > existingMeta.ChunkCount {
			existingMeta.ChunkCount = cc
			changed = true
		}

		if changed {
			if err := batch.Set(si.invMetaKey(termID), encodeTermMeta(existingMeta), nil); err != nil {
				return fmt.Errorf("writing term meta for %d: %w", termID, err)
			}
		}
		si.termMetaCache[termID] = existingMeta
	}

	// Update doc count
	if err := batch.Set(si.docCountKey(), encodeUint64(docCount), nil); err != nil {
		return fmt.Errorf("writing doc count: %w", err)
	}

	return batch.Commit(si.syncOpt)
}

// deleteDoc removes a document from the index by reading its forward index
// and removing entries from all affected posting list chunks.
func (si *SparseIndex) deleteDoc(batch *pebble.Batch, docID []byte) error {
	fwdKey := si.fwdKey(docID)

	val, closer, err := batch.Get(fwdKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil // document not in index
		}
		return fmt.Errorf("reading forward index: %w", err)
	}
	fwdData := bytes.Clone(val)
	_ = closer.Close()

	fwd, err := decodeFwdEntry(fwdData)
	if err != nil {
		return fmt.Errorf("decoding forward entry: %w", err)
	}

	// Remove from each term's posting list chunks.
	// We scan all chunks for the term because chunk splitting may have moved
	// the doc to a different chunk than docNum/chunkSize would predict.
	for _, termID := range fwd.Vec.GetIndices() {
		prefix := fmt.Appendf(append([]byte(nil), si.prefix...), "inv:%d:chunk", termID)
		iter, err := batch.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixEnd(prefix),
		})
		if err != nil {
			return fmt.Errorf("creating iterator for term %d delete: %w", termID, err)
		}

		found := false
		for iter.First(); iter.Valid(); iter.Next() {
			chunkKey := bytes.Clone(iter.Key())
			chunkData := bytes.Clone(iter.Value())

			docNums, weights, _, decErr := decodeChunk(chunkData)
			if decErr != nil {
				_ = iter.Close()
				return fmt.Errorf("decoding chunk for term %d: %w", termID, decErr)
			}

			// Find the entry for this doc
			idx := -1
			for i, dn := range docNums {
				if dn == fwd.DocNum {
					idx = i
					break
				}
			}
			if idx == -1 {
				continue
			}

			found = true

			// Remove entry
			newDocNums := append(docNums[:idx], docNums[idx+1:]...)
			newWeights := append(weights[:idx], weights[idx+1:]...)

			if len(newDocNums) == 0 {
				if err := batch.Delete(chunkKey, nil); err != nil {
					_ = iter.Close()
					return fmt.Errorf("deleting empty chunk: %w", err)
				}
			} else {
				c := &chunk{
					DocNums: newDocNums,
					Weights: newWeights,
				}
				data, encErr := encodeChunk(c)
				if encErr != nil {
					_ = iter.Close()
					return fmt.Errorf("re-encoding chunk: %w", encErr)
				}
				if err := batch.Set(chunkKey, data, nil); err != nil {
					_ = iter.Close()
					return fmt.Errorf("writing updated chunk: %w", err)
				}
			}
			if si.ccache != nil {
				si.ccache.Delete(string(chunkKey))
			}
			break // found and removed, no need to scan more chunks
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("closing delete iterator for term %d: %w", termID, err)
		}
		_ = found // doc may not be in any chunk if already deleted
	}

	// Delete forward and reverse indexes
	if err := batch.Delete(fwdKey, nil); err != nil {
		return fmt.Errorf("deleting forward index: %w", err)
	}
	revKey := si.revKey(fwd.DocNum)
	if err := batch.Delete(revKey, nil); err != nil {
		return fmt.Errorf("deleting reverse index: %w", err)
	}
	delete(si.revCache, fwd.DocNum)

	return nil
}

// deleteDocMerge removes a document using merge operands instead of
// read-modify-write. It reads the forward index to determine the doc's
// terms and docNum, then emits opTypeDelete merge operands to the
// predicted chunk key (docNum / chunkSize) for each term.
func (si *SparseIndex) deleteDocMerge(batch *pebble.Batch, docID []byte) error {
	fwdKey := si.fwdKey(docID)

	val, closer, err := batch.Get(fwdKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil // document not in index
		}
		return fmt.Errorf("reading forward index: %w", err)
	}
	fwdData := bytes.Clone(val)
	_ = closer.Close()

	fwd, err := decodeFwdEntry(fwdData)
	if err != nil {
		return fmt.Errorf("decoding forward entry: %w", err)
	}

	// Emit delete merge operands for each term's chunk.
	// The doc was inserted at chunkKey = inv:<termID>:chunk<docNum/chunkSize>.
	chunkNum := fwd.DocNum / uint64(si.chunkSize)                                   //nolint:gosec // G115: bounded value, cannot overflow in practice
	deleteOp := encodeMergeOperand(opTypeDelete, []uint32{uint32(fwd.DocNum)}, nil) //nolint:gosec // G115: bounded value, cannot overflow in practice

	for _, termID := range fwd.Vec.GetIndices() {
		chunkKey := si.invChunkKey(termID, chunkNum)
		if err := batch.Merge(chunkKey, deleteOp, nil); err != nil {
			return fmt.Errorf("emitting delete merge for term %d: %w", termID, err)
		}
		if si.ccache != nil {
			si.ccache.Delete(string(chunkKey))
		}
	}

	// Delete forward and reverse indexes
	if err := batch.Delete(fwdKey, nil); err != nil {
		return fmt.Errorf("deleting forward index: %w", err)
	}
	revKey := si.revKey(fwd.DocNum)
	if err := batch.Delete(revKey, nil); err != nil {
		return fmt.Errorf("deleting reverse index: %w", err)
	}
	delete(si.revCache, fwd.DocNum)

	return nil
}

// sortChunkEntries sorts parallel arrays by doc num.
func sortChunkEntries(docNums []uint64, docIDs [][]byte, weights []float32) {
	n := len(docNums)
	if n <= 1 {
		return
	}

	// Build indices and sort
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return docNums[indices[i]] < docNums[indices[j]]
	})

	// Apply permutation
	tmpNums := make([]uint64, n)
	tmpIDs := make([][]byte, n)
	tmpWeights := make([]float32, n)
	for i, idx := range indices {
		tmpNums[i] = docNums[idx]
		tmpIDs[i] = docIDs[idx]
		tmpWeights[i] = weights[idx]
	}
	copy(docNums, tmpNums)
	copy(docIDs, tmpIDs)
	copy(weights, tmpWeights)
}

func updateTermMax(m map[uint32]float32, termID uint32, weights []float32) {
	for _, w := range weights {
		if w > m[termID] {
			m[termID] = w
		}
	}
}

func maxFloat32(s []float32) float32 {
	if len(s) == 0 {
		return 0
	}
	m := s[0]
	for _, v := range s[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
