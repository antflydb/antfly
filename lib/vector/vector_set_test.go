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

package vector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorSet(t *testing.T) {
	t.Run("Add methods", func(t *testing.T) {
		vs := MakeSet(2)
		require.EqualValues(t, 2, vs.GetDims())
		require.EqualValues(t, 0, vs.GetCount())
		require.Equal(t, T{0, 0}, vs.Centroid(T{-1, -1}))

		// Add methods.
		v1 := T{1, 2}
		v2 := T{5, 3}
		v3 := T{6, 6}
		vs.Add(v1)
		vs.Add(v2)
		vs.Add(v3)
		require.EqualValues(t, 3, vs.GetCount())
		require.Equal(t, []float32{1, 2, 5, 3, 6, 6}, vs.GetData())

		vs.AddSet(vs)
		require.EqualValues(t, 6, vs.GetCount())
		require.Equal(t, []float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6}, vs.GetData())

		vs.AddUndefined(2)
		copy(vs.At(6), []float32{3, 1})
		copy(vs.At(7), []float32{4, 4})
		vs.AddUndefined(0)
		require.EqualValues(t, 8, vs.GetCount())
		require.Equal(t, []float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6, 3, 1, 4, 4}, vs.GetData())

		vs2 := MakeSetFromRawData([]float32{0, 1, -1, 3}, 2)
		vs2.AddSet(vs)
		require.EqualValues(t, 10, vs2.GetCount())
		require.Equal(
			t,
			[]float32{0, 1, -1, 3, 1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6, 3, 1, 4, 4},
			vs2.GetData(),
		)
	})

	t.Run("Centroid method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 4, 5, 3, 6, 2, 0, 0}, 2)
		require.Equal(t, T{3, 2.25}, vs.Centroid(T{-1, -1}))

		vs2 := T{-10.5}.AsSet()
		require.Equal(t, T{-10.5}, vs2.Centroid(T{-1}))
	})

	t.Run("ReplaceWithLast and Clear methods", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 5, 3, 6, 6, 1, 2, 5, 3, 6, 6, 3, 1, 4, 4}, 2)

		// ReplaceWithLast.
		vs.ReplaceWithLast(1)
		vs.ReplaceWithLast(4)
		vs.ReplaceWithLast(5)
		require.EqualValues(t, 5, vs.GetCount())
		require.Equal(t, []float32{1, 2, 4, 4, 6, 6, 1, 2, 3, 1}, vs.GetData())

		// Clear.
		vs.Clear()
		require.EqualValues(t, 2, vs.GetDims())
		require.EqualValues(t, 0, vs.GetCount())
	})

	t.Run("EnsureCapacity method", func(t *testing.T) {
		vs := MakeSet(3)
		vs.EnsureCapacity(5)
		require.Empty(t, vs.GetData())
		require.GreaterOrEqual(t, cap(vs.GetData()), 15)
		vs.AddUndefined(2)
		copy(vs.At(0), []float32{3, 1, 2})
		copy(vs.At(1), []float32{4, 4, 4})
		require.EqualValues(t, 2, vs.GetCount())
		require.Len(t, vs.GetData(), 6)
	})

	t.Run("AsSet method", func(t *testing.T) {
		vs5 := T{1, 2, 3}.AsSet()
		require.Equal(t, 3, cap(vs5.GetData()))
	})

	t.Run("Slice method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		require.Equal(t, Set_builder{Dims: 2, Count: 0, Data: []float32{}}.Build(), vs.Slice(0, 0))
		require.Equal(t, Set_builder{Dims: 2, Count: 0, Data: []float32{}}.Build(), vs.Slice(2, 0))
		require.Equal(
			t,
			Set_builder{Dims: 2, Count: 1, Data: []float32{1, 2}}.Build(),
			vs.Slice(0, 1),
		)
		require.Equal(
			t,
			Set_builder{Dims: 2, Count: 1, Data: []float32{3, 4}}.Build(),
			vs.Slice(1, 1),
		)
		require.Equal(
			t,
			Set_builder{Dims: 2, Count: 2, Data: []float32{3, 4, 5, 6}}.Build(),
			vs.Slice(1, 2),
		)
		require.Equal(
			t,
			Set_builder{Dims: 2, Count: 3, Data: []float32{1, 2, 3, 4, 5, 6}}.Build(),
			vs.Slice(0, 3),
		)
	})

	t.Run("Clone method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		vs2 := vs.Clone()

		vs.Add(T{0, 1})
		vs.ReplaceWithLast(1)
		add := MakeSetFromRawData([]float32{7, 8, 9, 10}, 2)
		vs2.AddSet(add)
		vs2.ReplaceWithLast(0)

		// Ensure that changes to each did not impact the other.
		require.EqualValues(t, 3, vs.GetCount())
		require.Equal(t, []float32{1, 2, 0, 1, 5, 6}, vs.GetData())

		require.EqualValues(t, 4, vs2.GetCount())
		require.Equal(t, []float32{9, 10, 3, 4, 5, 6, 7, 8}, vs2.GetData())
	})

	t.Run("Equal method", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		vs2 := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		vs3 := vs.Clone()
		require.True(t, vs.Equal(vs))
		require.True(t, vs.Equal(vs2))
		require.True(t, vs.Equal(vs3))

		vs.Add(T{7, 8})
		require.False(t, vs.Equal(vs2))
		require.True(t, vs.Equal(vs))
		vs2.Add(T{7, 8})
		require.True(t, vs.Equal(vs2))
		vs.ReplaceWithLast(1)
		require.False(t, vs.Equal(vs2))
		require.True(t, vs.Equal(vs))
		vs2.ReplaceWithLast(1)
		require.True(t, vs.Equal(vs2))
	})

	t.Run("check that invalid operations will panic", func(t *testing.T) {
		vs := MakeSetFromRawData([]float32{1, 2, 3, 4, 5, 6}, 2)
		require.Panics(t, func() { vs.At(-1) })
		require.Panics(t, func() { vs.AddUndefined(-1) })
		require.Panics(t, func() { vs.ReplaceWithLast(-1) })
		require.Panics(t, func() { vs.Centroid([]float32{0, 0, 0}) })

		vs2 := MakeSet(2)
		require.Panics(t, func() { vs2.At(0) })
		require.Panics(t, func() { vs2.ReplaceWithLast(0) })

		vs3 := MakeSet(-1)
		require.Panics(t, func() { vs3.Add(vs.At(0)) })
		require.Panics(t, func() { vs3.AddUndefined(1) })
	})
}
