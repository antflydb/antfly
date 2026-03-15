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

package allocator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncPoolAllocator(t *testing.T) {
	allocator := NewSyncPoolAllocator()

	// Test alloc/free vectors.
	vectors := allocator.AllocVectorSet(3, 2)
	require.EqualValues(t, 3, vectors.GetCount())
	require.EqualValues(t, 2, vectors.GetDims())
	require.Len(t, vectors.GetData(), 6)

	// Test alloc/free floats.
	floats := allocator.AllocFloat32s(5)
	require.Len(t, floats, 5)

	allocator.FreeFloat32s(floats)
	allocator.FreeVectorSet(vectors)

	// Test alloc/free uint64s.
	uint64s := allocator.AllocUint64s(4)
	require.Len(t, uint64s, 4)
	allocator.FreeUint64s(uint64s)

	// Verify everything is cleared
	require.True(t, allocator.IsClear())
}

func TestSyncPoolAllocatorNonLIFO(t *testing.T) {
	allocator := NewSyncPoolAllocator()

	// Unlike stack allocator, sync pool can handle non-LIFO deallocation
	floats1 := allocator.AllocFloat32s(5)
	floats2 := allocator.AllocFloat32s(3)
	floats3 := allocator.AllocFloat32s(7)

	require.Len(t, floats1, 5)
	require.Len(t, floats2, 3)
	require.Len(t, floats3, 7)

	// Free in different order - this would panic with stack allocator
	allocator.FreeFloat32s(floats2)
	allocator.FreeFloat32s(floats3)
	allocator.FreeFloat32s(floats1)

	require.True(t, allocator.IsClear())
}

func TestSyncPoolAllocatorConcurrent(t *testing.T) {
	allocator := NewSyncPoolAllocator()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := range numOperations {
				// Allocate different sizes to test pool creation
				size := (id*10+j)%20 + 1

				// Test float allocations
				floats := allocator.AllocFloat32s(size)
				require.Len(t, floats, size)
				allocator.FreeFloat32s(floats)

				// Test uint64 allocations
				uint64s := allocator.AllocUint64s(size)
				require.Len(t, uint64s, size)
				allocator.FreeUint64s(uint64s)

				// Test vector allocations
				vec := allocator.AllocVector(size)
				require.Len(t, vec, size)
				allocator.FreeVector(vec)
			}
		}(i)
	}

	wg.Wait()

	// After all goroutines complete, allocator should be clear
	require.True(t, allocator.IsClear())
}

func TestSyncPoolAllocatorReuse(t *testing.T) {
	allocator := NewSyncPoolAllocator()

	// Allocate and free the same size multiple times
	// to verify pool reuse
	size := 10

	// First allocation creates new slice
	floats1 := allocator.AllocFloat32s(size)
	require.Len(t, floats1, size)
	ptr1 := &floats1[0]
	allocator.FreeFloat32s(floats1)

	// Second allocation should reuse from pool
	floats2 := allocator.AllocFloat32s(size)
	require.Len(t, floats2, size)
	ptr2 := &floats2[0]

	// The underlying array should be the same (reused from pool)
	require.Equal(t, ptr1, ptr2, "Expected slice to be reused from pool")

	allocator.FreeFloat32s(floats2)
	require.True(t, allocator.IsClear())
}

func BenchmarkSyncPoolAllocator(b *testing.B) {
	allocator := NewSyncPoolAllocator()

	b.Run("AllocFree", func(b *testing.B) {
		for b.Loop() {
			floats := allocator.AllocFloat32s(100)
			allocator.FreeFloat32s(floats)
		}
	})

	b.Run("AllocFreeConcurrent", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				floats := allocator.AllocFloat32s(100)
				allocator.FreeFloat32s(floats)
			}
		})
	})
}

func BenchmarkWorkspaceT(b *testing.B) {
	var workspace StackAllocator

	b.Run("AllocFree", func(b *testing.B) {
		for b.Loop() {
			floats := workspace.AllocFloat32s(100)
			workspace.FreeFloat32s(floats)
		}
	})
}
