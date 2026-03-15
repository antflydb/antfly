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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStackAllocator(t *testing.T) {
	var allocator StackAllocator

	// Test alloc/free vectors.
	vectors := allocator.AllocVectorSet(3, 2)
	require.EqualValues(t, 3, vectors.GetCount())
	require.EqualValues(t, 2, vectors.GetDims())
	require.Len(t, vectors.GetData(), 6)
	require.Len(t, allocator.float32Stack, 6)

	// Test alloc/free floats.
	floats := allocator.AllocFloat32s(5)
	require.Len(t, floats, 5)
	require.Len(t, allocator.float32Stack, 11)

	allocator.FreeFloat32s(floats)
	require.Len(t, allocator.float32Stack, 6)
	allocator.FreeVectorSet(vectors)
	require.Empty(t, allocator.float32Stack)

	// Test alloc/free uint64s.
	uint64s := allocator.AllocUint64s(4)
	require.Len(t, uint64s, 4)
	require.Len(t, allocator.uint64Stack, 4)
	allocator.FreeUint64s(uint64s)
	require.Empty(t, allocator.uint64Stack)
}
