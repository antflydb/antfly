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

// NewSparseVector creates a new SparseVector from indices and values slices.
func NewSparseVector(indices []uint32, values []float32) *SparseVector {
	return SparseVector_builder{
		Indices: indices,
		Values:  values,
	}.Build()
}

// Len returns the number of non-zero elements in the sparse vector.
func (sv *SparseVector) Len() int {
	return len(sv.GetIndices())
}
