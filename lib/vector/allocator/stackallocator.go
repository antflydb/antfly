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
	"github.com/antflydb/antfly/lib/vector"
)

var _ Allocator = (*StackAllocator)(nil)

// StackAllocator provides a vector allocator for threads that only need to use it
// within the context of their current stack. Allocated memory is stack-
// based and must be explicitly freed in the same order it was allocated.
//
// For example:
//
//	 func threadSafeFunction() {
//		var workspace workspace.StackAllocator
//		tempVector := workspace.AllocVector(2)
//		defer workspace.FreeVector(tempVector)
//	   	<-- do work with tempVector here -->
//	 }
//
// NOTE: Obviously the StackAllocator is thus not thread-safe.
type StackAllocator struct {
	float32Stack stackBasedAlloc[float32]
	uint64Stack  stackBasedAlloc[uint64]
}

// IsClear returns true if there is no temp memory currently in use (i.e. all
// memory has been freed). This is useful in testing to validate there are no leaks.
func (w *StackAllocator) IsClear() bool {
	return w.float32Stack.IsEmpty() && w.uint64Stack.IsEmpty()
}

// AllocVector returns a vector having the given number of dimensions.
//
// NOTE: Slice data is undefined and should not assumed to be zeroed.
func (w *StackAllocator) AllocVector(dims int) vector.T {
	return w.AllocFloat32s(dims)
}

// FreeVector reclaims a vector that was previously allocated.
func (w *StackAllocator) FreeVector(vec vector.T) {
	w.FreeFloat32s(vec)
}

// AllocVectorSet returns a vector set having the given number of
// vectors with the given number of dimensions.
//
// NOTE: Slice data is undefined and should not assumed to be zeroed.
func (w *StackAllocator) AllocVectorSet(count, dims int) *vector.Set {
	floats := w.AllocFloat32s(count * dims)
	return vector.MakeSetFromRawData(floats, dims)
}

// FreeVectorSet reclaims a vector set that was previously allocated.
func (w *StackAllocator) FreeVectorSet(vectors *vector.Set) {
	w.FreeFloat32s(vectors.GetData())
}

// AllocFloat32s returns a slice of float32 values of the given size.
//
// NOTE: Slice data is undefined and should not assumed to be zeroed.
func (w *StackAllocator) AllocFloat32s(count int) []float32 {
	floats := w.float32Stack.Alloc(count)
	scribbleFloat32s(floats)
	return floats
}

// FreeFloat32s reclaims a float32 slice that was previously allocated.
func (w *StackAllocator) FreeFloat32s(floats []float32) {
	scribbleFloat32s(floats)
	w.float32Stack.Free(floats)
}

// AllocUint64s returns a slice of uint64 values of the given size.
//
// NOTE: Slice data is undefined and should not assumed to be zeroed.
func (w *StackAllocator) AllocUint64s(count int) []uint64 {
	uints := w.uint64Stack.Alloc(count)
	scribbleUint64s(uints)
	return uints
}

// FreeUint64s reclaims a uint64 slice that was previously allocated.
func (w *StackAllocator) FreeUint64s(uints []uint64) {
	scribbleUint64s(uints)
	w.uint64Stack.Free(uints)
}

// stackBasedAlloc allocates memory using a stack. Callers must deallocate memory in
// the inverse order of allocation. For example, if a caller allocates objects
// A and then B, it must free B and then A.
type stackBasedAlloc[U any] []U

func (s *stackBasedAlloc[U]) Alloc(count int) []U {
	start := len(*s)
	end := start + count
	if end > cap(*s) {
		// Need a new, larger array. Note that it's not necessary to copy the
		// existing data, as it's temporary.
		*s = make([]U, end, max(end*3/2, 16))
	}
	*s = (*s)[:end]
	return (*s)[start:end]
}

func (s *stackBasedAlloc[U]) Free(r []U) {
	*s = (*s)[:len(*s)-len(r)]
}

func (s *stackBasedAlloc[U]) IsEmpty() bool {
	return len(*s) == 0
}
