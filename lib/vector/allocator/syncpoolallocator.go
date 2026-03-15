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
	"sync/atomic"

	"github.com/antflydb/antfly/lib/vector"
)

var _ Allocator = (*SyncPoolAllocator)(nil)

// SyncPoolAllocator provides temporary memory using sync.Pool for better
// concurrency. Unlike T which uses stack allocation, this allocator can
// handle allocations and deallocations in any order, making it more flexible
// but potentially less efficient for strictly LIFO usage patterns.
//
// SyncPoolAllocator is thread-safe due to the underlying sync.Pool.
type SyncPoolAllocator struct {
	floatPools  map[int]*sync.Pool
	uint64Pools map[int]*sync.Pool
	mu          sync.RWMutex

	// Track allocations for IsClear check
	allocCount int32
}

// NewSyncPoolAllocator creates a new sync.Pool based allocator.
func NewSyncPoolAllocator() *SyncPoolAllocator {
	return &SyncPoolAllocator{
		floatPools:  make(map[int]*sync.Pool),
		uint64Pools: make(map[int]*sync.Pool),
	}
}

// getFloatPool returns a pool for float32 slices of the given size,
// creating one if necessary.
func (s *SyncPoolAllocator) getFloatPool(size int) *sync.Pool {
	s.mu.RLock()
	pool, ok := s.floatPools[size]
	s.mu.RUnlock()

	if ok {
		return pool
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if pool, ok := s.floatPools[size]; ok {
		return pool
	}

	pool = &sync.Pool{
		New: func() any {
			return make([]float32, size)
		},
	}
	s.floatPools[size] = pool
	return pool
}

// getUint64Pool returns a pool for uint64 slices of the given size,
// creating one if necessary.
func (s *SyncPoolAllocator) getUint64Pool(size int) *sync.Pool {
	s.mu.RLock()
	pool, ok := s.uint64Pools[size]
	s.mu.RUnlock()

	if ok {
		return pool
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if pool, ok := s.uint64Pools[size]; ok {
		return pool
	}

	pool = &sync.Pool{
		New: func() any {
			return make([]uint64, size)
		},
	}
	s.uint64Pools[size] = pool
	return pool
}

// IsClear returns true if there is no temp memory currently in use.
// Note: This is best-effort as sync.Pool doesn't provide a way to track
// outstanding allocations precisely.
func (s *SyncPoolAllocator) IsClear() bool {
	return atomic.LoadInt32(&s.allocCount) == 0
}

// AllocVector returns a temporary vector having the given number of dimensions.
// NOTE: Vector data is undefined; callers should not assume it's zeroed.
func (s *SyncPoolAllocator) AllocVector(dims int) vector.T {
	return s.AllocFloat32s(dims)
}

// FreeVector reclaims a temporary vector that was previously allocated.
func (s *SyncPoolAllocator) FreeVector(vec vector.T) {
	s.FreeFloat32s(vec)
}

// AllocVectorSet returns a temporary vector set having the given number of
// vectors with the given number of dimensions.
// NOTE: Vector data is undefined; callers should not assume it's zeroed.
func (s *SyncPoolAllocator) AllocVectorSet(count, dims int) *vector.Set {
	floats := s.AllocFloat32s(count * dims)
	return vector.MakeSetFromRawData(floats, dims)
}

// FreeVectorSet reclaims a temporary vector set that was previously allocated.
func (s *SyncPoolAllocator) FreeVectorSet(vectors *vector.Set) {
	s.FreeFloat32s(vectors.GetData())
}

// AllocFloat32s returns a temporary slice of float32 values of the given size.
// NOTE: Slice data is undefined; callers should not assume it's zeroed.
func (s *SyncPoolAllocator) AllocFloat32s(count int) []float32 {
	pool := s.getFloatPool(count)
	floats := pool.Get().([]float32) //nolint:staticcheck // SA6002

	// Ensure the slice has the correct length
	if len(floats) != count {
		floats = floats[:count]
	}

	scribbleFloat32s(floats)
	atomic.AddInt32(&s.allocCount, 1)
	return floats
}

// FreeFloat32s reclaims a temporary float32 slice that was previously allocated.
func (s *SyncPoolAllocator) FreeFloat32s(floats []float32) {
	scribbleFloat32s(floats)

	size := cap(floats)
	pool := s.getFloatPool(size)

	// Reset slice to full capacity before returning to pool
	floats = floats[:cap(floats)]
	pool.Put(floats) //nolint:staticcheck // SA6002

	atomic.AddInt32(&s.allocCount, -1)
}

// AllocUint64s returns a temporary slice of uint64 values of the given size.
// NOTE: Slice data is undefined; callers should not assume it's zeroed.
func (s *SyncPoolAllocator) AllocUint64s(count int) []uint64 {
	pool := s.getUint64Pool(count)
	uints := pool.Get().([]uint64) //nolint:staticcheck // SA6002

	// Ensure the slice has the correct length
	if len(uints) != count {
		uints = uints[:count]
	}
	scribbleUint64s(uints)
	atomic.AddInt32(&s.allocCount, 1)
	return uints
}

// FreeUint64s reclaims a temporary uint64 slice that was previously allocated.
func (s *SyncPoolAllocator) FreeUint64s(uints []uint64) {
	scribbleUint64s(uints)

	size := cap(uints)
	pool := s.getUint64Pool(size)

	// Reset slice to full capacity before returning to pool
	uints = uints[:cap(uints)]
	pool.Put(uints) //nolint:staticcheck // SA6002

	atomic.AddInt32(&s.allocCount, -1)
}
