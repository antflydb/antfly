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

package quantize

import (
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/stretchr/testify/require"
)

func TestNonQuantizedVectorSet(t *testing.T) {
	vs := vector.MakeSet(2)
	quantizedSet := NonQuantizedVectorSet_builder{
		Vectors: vs,
	}.Build()

	// Add vectors
	vectors := vector.MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
	quantizedSet.AddSet(vectors)
	require.Equal(t, 3, quantizedSet.GetCount())

	vectors = vector.MakeSetFromRawData([]float32{7, 8, 9, 10}, 2)
	quantizedSet.AddSet(vectors)
	require.Equal(t, 5, quantizedSet.GetCount())

	// Ensure that cloning does not disturb anything
	cloned := quantizedSet.Clone().(*NonQuantizedVectorSet)
	copy(cloned.GetVectors().At(0), vector.T{0, 0})
	cloned.ReplaceWithLast(1)

	// Remove vector
	quantizedSet.ReplaceWithLast(2)
	require.Equal(t, 4, quantizedSet.GetCount())

	// Check that clone is unaffected
	require.Equal(
		t,
		vector.Set_builder{Dims: 2, Count: 4, Data: []float32{0, 0, 9, 10, 5, 6, 7, 8}}.Build(),
		cloned.GetVectors(),
	)
}
