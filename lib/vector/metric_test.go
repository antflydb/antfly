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

func TestMetric(t *testing.T) {
	l2Sq := MeasureDistance(DistanceMetric_L2Squared, T{1, 2}, T{4, 3})
	require.Equal(t, float32(10), l2Sq)

	ip := MeasureDistance(DistanceMetric_InnerProduct, T{1, 2}, T{4, 3})
	require.Equal(t, float32(-10), ip)

	cos := MeasureDistance(DistanceMetric_Cosine, T{1, 2}, T{4, 3})
	require.Equal(t, float32(-9), cos)

	cos = MeasureDistance(DistanceMetric_Cosine, T{1, 0}, T{0.7071, 0.7071})
	require.InDelta(t, float32(0.2929), cos, 1e-4)

	// Test zero product of norms.
	cos = MeasureDistance(DistanceMetric_Cosine, T{1, 0}, T{0, 1})
	require.Equal(t, float32(1), cos)
}
