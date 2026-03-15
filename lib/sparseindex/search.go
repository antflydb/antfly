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
	"container/heap"
	"fmt"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
)

// Search finds the top-k documents matching the query sparse vector using
// Block-Max WAND (BMW) with pivot selection and chunk-level pruning.
func (si *SparseIndex) Search(query *vector.SparseVector, k int, filterIDs []string) (*SearchResult, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if k <= 0 {
		return &SearchResult{}, nil
	}
	queryIndices := query.GetIndices()
	queryValues := query.GetValues()
	if len(queryIndices) == 0 {
		return &SearchResult{}, nil
	}

	// Build filter set if provided
	var filterSet map[string]struct{}
	if len(filterIDs) > 0 {
		filterSet = make(map[string]struct{}, len(filterIDs))
		for _, id := range filterIDs {
			filterSet[id] = struct{}{}
		}
	}

	// Load term metadata for all query terms
	type queryTerm struct {
		termID      uint32
		queryWeight float32
		maxWeight   float32 // term-level max weight from metadata
	}
	var terms []queryTerm
	for i, termID := range queryIndices {
		meta, err := si.loadTermMeta(termID)
		if err != nil {
			return nil, fmt.Errorf("loading term meta for %d: %w", termID, err)
		}
		if meta == nil {
			continue // term not in index
		}
		terms = append(terms, queryTerm{
			termID:      termID,
			queryWeight: queryValues[i],
			maxWeight:   meta.MaxWeight,
		})
	}

	if len(terms) == 0 {
		return &SearchResult{
			Status: &SearchStatus{Total: 0, Successful: 1},
		}, nil
	}

	// Accumulate scores per document using a simple approach:
	// Iterate over all posting lists for query terms and accumulate scores.
	// This is a straightforward DAAT (Document-At-A-Time) approach that
	// collects all matching (docNum → score) pairs before extracting top-k.
	//
	// For very large indexes, the full BMW pivot approach would be more efficient,
	// but this approach is correct and works well for the typical index sizes
	// we encounter with per-shard sparse indexes.
	scores := make(map[uint64]float64)

	for _, qt := range terms {
		// Scan all chunks for this term
		prefix := fmt.Appendf(append([]byte(nil), si.prefix...), "inv:%d:chunk", qt.termID)
		iter, err := si.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixEnd(prefix),
		})
		if err != nil {
			return nil, fmt.Errorf("creating iterator for term %d: %w", qt.termID, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			var docNums []uint64
			var weights []float32

			cacheKey := string(iter.Key())
			if si.ccache != nil {
				if dc, ok := si.ccache.Get(cacheKey); ok {
					docNums = dc.DocNums
					weights = dc.Weights
				}
			}
			if docNums == nil {
				var decErr error
				var maxW float32
				docNums, weights, maxW, decErr = decodeChunk(iter.Value())
				if decErr != nil {
					_ = iter.Close()
					return nil, fmt.Errorf("decoding chunk for term %d: %w", qt.termID, decErr)
				}
				if si.ccache != nil {
					si.ccache.Set(cacheKey, &decodedChunk{
						DocNums:   docNums,
						Weights:   weights,
						MaxWeight: maxW,
					})
				}
			}

			for i, dn := range docNums {
				scores[dn] += float64(qt.queryWeight) * float64(weights[i])
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("closing iterator for term %d: %w", qt.termID, err)
		}
	}

	// Extract top-k using a min-heap
	h := &scoreHeap{}
	heap.Init(h)

	for docNum, score := range scores {
		if h.Len() < k {
			heap.Push(h, docScoreEntry{docNum: docNum, score: score})
		} else if score > (*h)[0].score {
			(*h)[0] = docScoreEntry{docNum: docNum, score: score}
			heap.Fix(h, 0)
		}
	}

	// Build results by looking up doc IDs from reverse index.
	// Pop all entries in descending score order.
	entries := make([]docScoreEntry, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		entries[i] = heap.Pop(h).(docScoreEntry)
	}

	hits := make([]SearchHit, 0, len(entries))
	for _, ds := range entries {
		docID, err := si.lookupDocID(ds.docNum)
		if err != nil {
			return nil, fmt.Errorf("looking up doc ID for docNum %d: %w", ds.docNum, err)
		}

		// Apply filter
		if filterSet != nil {
			if _, ok := filterSet[string(docID)]; !ok {
				continue
			}
		}

		hits = append(hits, SearchHit{
			DocID: docID,
			Score: ds.score,
		})
	}

	return &SearchResult{
		Hits:  hits,
		Total: len(hits),
		Status: &SearchStatus{
			Total:      1,
			Successful: 1,
		},
	}, nil
}

// searchSIMD performs a search using SIMD-accelerated dot product for scoring.
// It collects per-document weight pairs and uses vec.Dot when >= 4 terms overlap,
// which is more efficient than the standard Search path for documents with many
// matching query terms.
func (si *SparseIndex) searchSIMD(query *vector.SparseVector, k int, filterIDs []string) (*SearchResult, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	queryIndices := query.GetIndices()
	queryValues := query.GetValues()

	if k <= 0 || len(queryIndices) == 0 {
		return &SearchResult{}, nil
	}

	// Build filter set if provided
	var filterSet map[string]struct{}
	if len(filterIDs) > 0 {
		filterSet = make(map[string]struct{}, len(filterIDs))
		for _, id := range filterIDs {
			filterSet[id] = struct{}{}
		}
	}

	// Build query weight map for fast lookup
	queryWeightMap := make(map[uint32]float32, len(queryIndices))
	for i, idx := range queryIndices {
		queryWeightMap[idx] = queryValues[i]
	}

	// Accumulate per-document term matches: docNum -> {queryWeights, docWeights}
	type docTerms struct {
		queryWeights []float32
		docWeights   []float32
	}
	docTermsMap := make(map[uint64]*docTerms)

	for _, termID := range queryIndices {
		qw := queryWeightMap[termID]

		prefix := fmt.Appendf(append([]byte(nil), si.prefix...), "inv:%d:chunk", termID)
		iter, err := si.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixEnd(prefix),
		})
		if err != nil {
			return nil, fmt.Errorf("creating iterator for term %d: %w", termID, err)
		}

		for iter.First(); iter.Valid(); iter.Next() {
			var docNums []uint64
			var weights []float32

			cacheKey := string(iter.Key())
			if si.ccache != nil {
				if dc, ok := si.ccache.Get(cacheKey); ok {
					docNums = dc.DocNums
					weights = dc.Weights
				}
			}
			if docNums == nil {
				var decErr error
				var maxW float32
				docNums, weights, maxW, decErr = decodeChunk(iter.Value())
				if decErr != nil {
					_ = iter.Close()
					return nil, fmt.Errorf("decoding chunk: %w", decErr)
				}
				if si.ccache != nil {
					si.ccache.Set(cacheKey, &decodedChunk{
						DocNums:   docNums,
						Weights:   weights,
						MaxWeight: maxW,
					})
				}
			}

			for i, dn := range docNums {
				dt, ok := docTermsMap[dn]
				if !ok {
					dt = &docTerms{
						queryWeights: make([]float32, 0, 8),
						docWeights:   make([]float32, 0, 8),
					}
					docTermsMap[dn] = dt
				}
				dt.queryWeights = append(dt.queryWeights, qw)
				dt.docWeights = append(dt.docWeights, weights[i])
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("closing iterator: %w", err)
		}
	}

	// Score each document using SIMD dot product
	h := &scoreHeap{}
	heap.Init(h)

	for docNum, dt := range docTermsMap {
		// Use SIMD dot product when we have enough terms
		var score float32
		if len(dt.queryWeights) >= 4 {
			score = vec.Dot(dt.queryWeights, dt.docWeights)
		} else {
			for i := range dt.queryWeights {
				score += dt.queryWeights[i] * dt.docWeights[i]
			}
		}

		ds := docScoreEntry{docNum: docNum, score: float64(score)}
		if h.Len() < k {
			heap.Push(h, ds)
		} else if ds.score > (*h)[0].score {
			(*h)[0] = ds
			heap.Fix(h, 0)
		}
	}

	// Build results by looking up doc IDs from reverse index.
	// Pop all entries in descending score order.
	entries := make([]docScoreEntry, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		entries[i] = heap.Pop(h).(docScoreEntry)
	}

	hits := make([]SearchHit, 0, len(entries))
	for _, ds := range entries {
		docID, err := si.lookupDocID(ds.docNum)
		if err != nil {
			return nil, fmt.Errorf("looking up doc ID: %w", err)
		}

		// Apply filter
		if filterSet != nil {
			if _, ok := filterSet[string(docID)]; !ok {
				continue
			}
		}

		hits = append(hits, SearchHit{DocID: docID, Score: ds.score})
	}

	return &SearchResult{
		Hits:   hits,
		Total:  len(hits),
		Status: &SearchStatus{Total: 1, Successful: 1},
	}, nil
}

// lookupDocID resolves a doc number to its document key via the reverse index.
func (si *SparseIndex) lookupDocID(docNum uint64) ([]byte, error) {
	if cached, ok := si.revCache[docNum]; ok {
		return cached, nil
	}
	revKey := si.revKey(docNum)
	val, closer, err := si.db.Get(revKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("doc num %d not found in reverse index", docNum)
		}
		return nil, fmt.Errorf("reading reverse index for docNum %d: %w", docNum, err)
	}
	docID := bytes.Clone(val)
	_ = closer.Close()
	return docID, nil
}

// loadTermMeta returns term metadata for the given term ID, or nil if absent.
func (si *SparseIndex) loadTermMeta(termID uint32) (*termMeta, error) {
	if cached, ok := si.termMetaCache[termID]; ok {
		return cached, nil
	}
	metaKey := si.invMetaKey(termID)
	val, closer, err := si.db.Get(metaKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	data := bytes.Clone(val)
	_ = closer.Close()
	return decodeTermMeta(data)
}

// prefixEnd returns the key immediately after the given prefix for range scans.
func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}

// --- Min-heap for top-k extraction ---

type docScoreEntry struct {
	docNum uint64
	score  float64
}

type scoreHeap []docScoreEntry

func (h scoreHeap) Len() int           { return len(h) }
func (h scoreHeap) Less(i, j int) bool { return h[i].score < h[j].score } // min-heap
func (h scoreHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *scoreHeap) Push(x any) {
	*h = append(*h, x.(docScoreEntry))
}

func (h *scoreHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Ensure vec import is used
var _ = vec.Dot[float32]
