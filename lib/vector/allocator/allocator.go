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
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
)

// Allocator provides an interface for temporary memory allocation.
// Implementations may use different strategies (stack-based, pool-based, etc.)
// for managing temporary memory.
//
// All allocated memory must be explicitly freed when no longer needed.
// The specific ordering requirements for freeing depend on the implementation.
type Allocator interface {
	// IsClear returns true if there is no temp memory currently in use (i.e. all
	// memory has been freed). This can be called to validate that there are no
	// leaks.
	IsClear() bool

	// AllocVector returns a temporary vector having the given number of dimensions.
	// NOTE: Vector data is undefined; callers should not assume it's zeroed.
	AllocVector(dims int) vector.T

	// FreeVector reclaims a temporary vector that was previously allocated.
	FreeVector(vec vector.T)

	// AllocVectorSet returns a temporary vector set having the given number of
	// vectors with the given number of dimensions.
	// NOTE: Vector data is undefined; callers should not assume it's zeroed.
	AllocVectorSet(count, dims int) *vector.Set

	// FreeVectorSet reclaims a temporary vector set that was previously allocated.
	FreeVectorSet(vectors *vector.Set)

	// AllocFloat32s returns a temporary slice of float32 values of the given size.
	// NOTE: Slice data is undefined; callers should not assume it's zeroed.
	AllocFloat32s(count int) []float32

	// FreeFloat32s reclaims a temporary float32 slice that was previously allocated.
	FreeFloat32s(floats []float32)

	// AllocUint64s returns a temporary slice of uint64 values of the given size.
	// NOTE: Slice data is undefined; callers should not assume it's zeroed.
	AllocUint64s(count int) []uint64

	// FreeUint64s reclaims a temporary uint64 slice that was previously allocated.
	FreeUint64s(uint64s []uint64)
}

func scribbleFloat32s(floats []float32) {
	if utils.AfdbTestBuild {
		for i := range floats {
			floats[i] = 0xDEADBEEF
		}
	}
}

func scribbleUint64s(uints []uint64) {
	if utils.AfdbTestBuild {
		for i := range uints {
			uints[i] = 0xDEADBEEF
		}
	}
}
