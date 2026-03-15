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
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func benchmarkInsert(b *testing.B, numDocs int) {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	idx := New(db, Config{ChunkSize: DefaultChunkSize})

	// Generate random sparse vectors
	inserts := make([]BatchInsert, numDocs)
	for i := range inserts {
		nnz := 50 + rand.IntN(100) // 50-150 non-zero terms
		indices := make([]uint32, nnz)
		values := make([]float32, nnz)
		for j := range indices {
			indices[j] = uint32(rand.Int32N(30000)) // vocab size ~30k
			values[j] = rand.Float32()
		}
		inserts[i] = BatchInsert{
			DocID: fmt.Appendf(nil, "doc%d", i),
			Vec:   vector.NewSparseVector(indices, values),
		}
	}

	b.ResetTimer()
	for range b.N {
		if err := idx.Batch(inserts, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert100(b *testing.B)  { benchmarkInsert(b, 100) }
func BenchmarkInsert1000(b *testing.B) { benchmarkInsert(b, 1000) }

func benchmarkSearch(b *testing.B, numDocs int) {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	idx := New(db, Config{ChunkSize: DefaultChunkSize})

	// Insert docs
	inserts := make([]BatchInsert, numDocs)
	for i := range inserts {
		nnz := 50 + rand.IntN(100)
		indices := make([]uint32, nnz)
		values := make([]float32, nnz)
		for j := range indices {
			indices[j] = uint32(rand.Int32N(30000))
			values[j] = rand.Float32()
		}
		inserts[i] = BatchInsert{
			DocID: fmt.Appendf(nil, "doc%d", i),
			Vec:   vector.NewSparseVector(indices, values),
		}
	}

	// Batch insert in groups of 100
	for i := 0; i < len(inserts); i += 100 {
		end := min(i+100, len(inserts))
		if err := idx.Batch(inserts[i:end], nil); err != nil {
			b.Fatal(err)
		}
	}

	// Generate query
	queryNnz := 20
	queryIndices := make([]uint32, queryNnz)
	queryValues := make([]float32, queryNnz)
	for j := range queryIndices {
		queryIndices[j] = uint32(rand.Int32N(30000))
		queryValues[j] = rand.Float32()
	}
	query := vector.NewSparseVector(queryIndices, queryValues)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, err := idx.Search(query, 10, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSearch1K(b *testing.B)  { benchmarkSearch(b, 1000) }
func BenchmarkSearch10K(b *testing.B) { benchmarkSearch(b, 10000) }
