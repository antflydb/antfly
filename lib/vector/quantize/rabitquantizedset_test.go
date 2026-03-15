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

func TestRaBitCodeSet(t *testing.T) {
	cs := MakeRaBitQCodeSet(65)
	require.EqualValues(t, 0, cs.GetCount())
	require.EqualValues(t, 2, cs.GetWidth())

	// Add code.
	cs.Add(RaBitQCode{1, 2})
	require.EqualValues(t, 1, cs.GetCount())

	// Add additional codes.
	cs.AddUndefined(2)
	copy(cs.At(1), []uint64{3, 4})
	copy(cs.At(2), []uint64{5, 6})
	require.EqualValues(t, 3, cs.GetCount())
	require.Equal(t, RaBitQCode{5, 6}, cs.At(2))

	// Remove codes.
	cs.ReplaceWithLast(1)
	require.EqualValues(t, 2, cs.GetCount())
	require.Equal(t, RaBitQCode{5, 6}, cs.At(1))

	cs.ReplaceWithLast(0)
	require.EqualValues(t, 1, cs.GetCount())
	require.Equal(t, RaBitQCode{5, 6}, cs.At(0))

	cs.ReplaceWithLast(0)
	require.EqualValues(t, 0, cs.GetCount())

	data := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	cs = MakeRaBitQCodeSetFromRawData(data, 3)
	require.EqualValues(t, 4, cs.GetCount())
	require.EqualValues(t, 3, cs.GetWidth())
	require.Equal(t, data, cs.GetData())

	// Clone.
	data = []uint64{1, 2, 3, 4, 5, 6}
	cs = MakeRaBitQCodeSetFromRawData(data, 2)
	cs2 := cs.Clone()
	cs.ReplaceWithLast(0)
	cs2.Add(RaBitQCode{10, 20})
	require.EqualValues(t, 2, cs.GetCount())
	require.EqualValues(t, 2, cs.GetWidth())
	require.Equal(t, []uint64{5, 6, 3, 4}, cs.GetData())
	require.EqualValues(t, 4, cs2.GetCount())
	require.EqualValues(t, 2, cs2.GetWidth())
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 10, 20}, cs2.GetData())
}

func TestRaBitQuantizedVectorSet(t *testing.T) {
	var quantizedSet RaBitQuantizedVectorSet
	quantizedSet.SetMetric(vector.DistanceMetric_L2Squared)
	quantizedSet.SetCentroid([]float32{1, 2, 3})
	quantizedSet.SetCodes(RaBitQCodeSet_builder{
		Width: 3,
	}.Build())

	quantizedSet.AddUndefined(5)
	copy(quantizedSet.GetCodes().At(4), []uint64{1, 2, 3})
	quantizedSet.GetCodeCounts()[4] = 15
	quantizedSet.GetCentroidDistances()[4] = 1.23
	quantizedSet.GetQuantizedDotProducts()[4] = 4.56
	require.EqualValues(t, 5, quantizedSet.GetCodes().GetCount())
	require.Len(t, quantizedSet.GetCodeCounts(), 5)
	require.Len(t, quantizedSet.GetCentroidDistances(), 5)
	require.Len(t, quantizedSet.GetQuantizedDotProducts(), 5)
	require.Nil(t, quantizedSet.GetCentroidDotProducts())

	// Ensure that cloning does not disturb anything.
	cloned := quantizedSet.Clone().(*RaBitQuantizedVectorSet)
	copy(cloned.GetCodes().At(0), []uint64{10, 20, 30})
	cloned.GetCodeCounts()[0] = 10
	cloned.GetCentroidDistances()[0] = 10
	cloned.GetQuantizedDotProducts()[0] = 10
	cloned.ReplaceWithLast(1)
	cloned.ReplaceWithLast(1)
	cloned.ReplaceWithLast(1)
	cloned.ReplaceWithLast(1)

	quantizedSet.ReplaceWithLast(2)
	require.EqualValues(t, 4, quantizedSet.GetCodes().GetCount())
	require.Equal(t, RaBitQCode{1, 2, 3}, quantizedSet.GetCodes().At(2))
	require.Len(t, quantizedSet.GetCodeCounts(), 4)
	require.Equal(t, uint32(15), quantizedSet.GetCodeCounts()[2])
	require.Len(t, quantizedSet.GetCentroidDistances(), 4)
	require.Equal(t, float32(1.23), quantizedSet.GetCentroidDistances()[2])
	require.Len(t, quantizedSet.GetQuantizedDotProducts(), 4)
	require.Equal(t, float32(4.56), quantizedSet.GetQuantizedDotProducts()[2])

	// Check that clone is unaffected.
	require.Equal(t, []float32{1, 2, 3}, cloned.GetCentroid())
	require.Equal(
		t,
		RaBitQCodeSet_builder{Count: 1, Width: 3, Data: []uint64{10, 20, 30}}.Build(),
		cloned.GetCodes(),
	)
	require.Equal(t, []uint32{10}, cloned.GetCodeCounts())
	require.Equal(t, []float32{10}, cloned.GetCentroidDistances())
	require.Equal(t, []float32{10}, cloned.GetQuantizedDotProducts())

	// Clear the set and ensure that norm is not updated.
	quantizedSet.Clear(quantizedSet.GetCentroid())
	require.Equal(t, float32(0), quantizedSet.GetCentroidNorm())

	// Test InnerProduct distance metric, which uses the CentroidDotProducts
	// field (L2Squared does not use it).
	quantizedSet.Clear(quantizedSet.GetCentroid())
	quantizedSet.SetMetric(vector.DistanceMetric_InnerProduct)
	quantizedSet.Clear(quantizedSet.GetCentroid())
	require.Equal(t, float32(0), quantizedSet.GetCentroidNorm())
	quantizedSet.AddUndefined(2)
	copy(quantizedSet.GetCodes().At(1), []uint64{1, 2, 3})
	quantizedSet.GetCodeCounts()[1] = 15
	quantizedSet.GetCentroidDistances()[1] = 1.23
	quantizedSet.GetQuantizedDotProducts()[1] = 4.56
	quantizedSet.GetCentroidDotProducts()[1] = 7.89
	require.Len(t, quantizedSet.GetCentroidDotProducts(), 2)

	cloned = quantizedSet.Clone().(*RaBitQuantizedVectorSet)
	require.Equal(t, quantizedSet.GetCentroidDotProducts(), cloned.GetCentroidDotProducts())
	quantizedSet.ReplaceWithLast(0)
	require.Equal(t, float32(7.89), quantizedSet.GetCentroidDotProducts()[0])
	require.Len(t, cloned.GetCentroidDotProducts(), 2)
	cloned.Clear(quantizedSet.GetCentroid())
	require.Empty(t, cloned.GetCentroidDotProducts())

	// Update the centroid and ensure that norm is updated.
	quantizedSet.Clear([]float32{2, 3, 6})
	require.Equal(t, float32(7), quantizedSet.GetCentroidNorm())
}
