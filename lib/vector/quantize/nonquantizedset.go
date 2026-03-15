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

import "github.com/antflydb/antfly/lib/vector"

// GetCount implements the QuantizedVectorSet interface.
func (vs *NonQuantizedVectorSet) GetCount() int {
	return int(vs.GetVectors().GetCount())
}

// Clone implements the QuantizedVectorSet interface.
func (vs *NonQuantizedVectorSet) Clone() QuantizedVectorSet {
	return NonQuantizedVectorSet_builder{
		Vectors: vs.GetVectors().Clone(),
	}.Build()
}

// Clear implements the QuantizedVectorSet interface.
func (vs *NonQuantizedVectorSet) Clear(_ vector.T /* centroid */) {
	vs.GetVectors().Clear()
}

// AddSet adds the given set of vectors to this set.
func (vs *NonQuantizedVectorSet) AddSet(vectors *vector.Set) {
	vs.GetVectors().AddSet(vectors)
}

// ReplaceWithLast implements the QuantizedVectorSet interface.
func (vs *NonQuantizedVectorSet) ReplaceWithLast(offset int) {
	vs.GetVectors().ReplaceWithLast(offset)
}
