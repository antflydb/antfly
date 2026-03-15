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
	"context"
	"fmt"

	"github.com/antflydb/antfly/lib/vector"
)

type Batch struct {
	Vectors      []vector.T
	MetadataList [][]byte
	IDs          []uint64

	Deletes []uint64
}

func (b *Batch) Reset() {
	b.Vectors = b.Vectors[:0]
	b.MetadataList = b.MetadataList[:0]
	b.IDs = b.IDs[:0]
	b.Deletes = b.Deletes[:0]
}

func (b *Batch) Insert(ids []uint64, vectors []vector.T, metadata [][]byte) {
	b.Vectors = append(b.Vectors, vectors...)
	b.MetadataList = append(b.MetadataList, metadata...)
	b.IDs = append(b.IDs, ids...)
}

func (b *Batch) InsertSingle(id uint64, vector vector.T, metadata []byte) {
	b.Vectors = append(b.Vectors, vector)
	b.MetadataList = append(b.MetadataList, metadata)
	b.IDs = append(b.IDs, id)
}

func (b *Batch) Delete(ids ...uint64) {
	b.Deletes = append(b.Deletes, ids...)
}

type Result struct {
	ID         uint64
	Distance   float32
	ErrorBound float32 // Optional, can be nil if not needed
	Metadata   []byte
	Vector     vector.T // Optional, can be nil if not needed
}

func (r *PriorityItem) MaybeUnder(dist float32) bool {
	return r.Distance-r.ErrorBounds <= dist
}

func (r *PriorityItem) DefinitelyUnder(dist float32) bool {
	return r.Distance+r.ErrorBounds <= dist
}

func (r *PriorityItem) MaybeOver(dist float32) bool {
	return r.Distance+r.ErrorBounds >= dist
}

func (r *PriorityItem) DefinitelyOver(dist float32) bool {
	return r.Distance-r.ErrorBounds >= dist
}

func (r *PriorityItem) MaybeCloser(r2 *PriorityItem) bool {
	return r.Distance-r.ErrorBounds <= r2.Distance+r2.ErrorBounds
}

func (r *PriorityItem) DefinitelyCloser(r2 *PriorityItem) bool {
	return r.Distance+r.ErrorBounds < r2.Distance-r2.ErrorBounds
}

func (r *Result) String() string {
	return fmt.Sprintf(
		"Result{ ID: %d, Distance: %f, Metadata: %s }",
		r.ID,
		r.Distance,
		string(r.Metadata),
	)
}

// VectorIndex defines the common interface for vector search indexes,
// supporting operations like insertion, searching, deletion, and retrieval.
type VectorIndex interface {
	Name() string
	// BatchInsert adds multiple vectors and their corresponding metadata to the index.
	// It should be more efficient than calling Insert repeatedly.
	// Returns a slice of assigned IDs or an error. The length of the returned IDs
	// slice might be shorter than the input if an error occurred partway through.
	Batch(ctx context.Context, batch *Batch) error

	// Search finds the k nearest neighbors for the given query vector.
	// It returns the IDs of the neighbors, their corresponding distances, and an error.
	Search(req *SearchRequest) ([]*Result, error)

	// Delete marks a node (identified by its ID) as deleted.
	// Depending on the implementation, this might be a soft delete.
	// Returns an error if the deletion fails (e.g., ID not found).
	Delete(ids ...uint64) error

	// GetVector retrieves metadata associated with a given ID.
	GetMetadata(id uint64) ([]byte, error)

	// Stats returns a map containing statistics about the index's state and performance.
	// The specific keys in the map depend on the implementation.
	Stats() map[string]any

	TotalVectors() uint64

	Close() error
	// Note: A Close() method might be needed for implementations that manage
	// resources like file handles (e.g., DiskANNIndex), but it's not included
	// here as not all implementations require it (e.g., HnswIndex).
	// Implementations requiring cleanup should implement io.Closer separately.
}
