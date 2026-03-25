// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package vectorindex

import (
	"context"
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubVectorIndex struct {
	name    string
	total   uint64
	results []*Result
}

func (s *stubVectorIndex) Name() string { return s.name }

func (s *stubVectorIndex) Batch(context.Context, *Batch) error { return nil }

func (s *stubVectorIndex) Search(*SearchRequest) ([]*Result, error) { return s.results, nil }

func (s *stubVectorIndex) Delete(...uint64) error { return nil }

func (s *stubVectorIndex) GetMetadata(uint64) ([]byte, error) { return nil, nil }

func (s *stubVectorIndex) Stats() map[string]any { return nil }

func (s *stubVectorIndex) TotalVectors() uint64 { return s.total }

func (s *stubVectorIndex) Close() error { return nil }

//go:fix inline
func f32(v float32) *float32 { return new(v) }

func TestShouldAutoRerank(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		results       []*Result
		k             int
		distanceOver  *float32
		distanceUnder *float32
		want          bool
	}{
		{
			name: "stable ordering and boundary",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.01},
				{ID: 2, Distance: 2.0, ErrorBound: 0.01},
				{ID: 3, Distance: 3.0, ErrorBound: 0.01},
			},
			k:    2,
			want: false,
		},
		{
			name: "top k order ambiguity alone does not rerank",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.3},
				{ID: 2, Distance: 1.2, ErrorBound: 0.3},
				{ID: 3, Distance: 4.0, ErrorBound: 0.01},
			},
			k:    2,
			want: false,
		},
		{
			name: "top k boundary ambiguous",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.05},
				{ID: 2, Distance: 2.0, ErrorBound: 0.05},
				{ID: 3, Distance: 2.02, ErrorBound: 0.2},
			},
			k:    2,
			want: true,
		},
		{
			name: "distance over threshold ambiguous",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.2},
			},
			k:            1,
			distanceOver: f32(1.1),
			want:         true,
		},
		{
			name: "distance under threshold ambiguous",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.2},
			},
			k:             1,
			distanceUnder: f32(0.9),
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				tt.want,
				shouldAutoRerank(tt.results, tt.k, tt.distanceOver, tt.distanceUnder),
			)
		})
	}
}

func TestSelectAutoRerankCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		results       []*Result
		k             int
		distanceOver  *float32
		distanceUnder *float32
		want          []int
	}{
		{
			name: "stable ordering and boundary",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.01},
				{ID: 2, Distance: 2.0, ErrorBound: 0.01},
				{ID: 3, Distance: 3.0, ErrorBound: 0.01},
			},
			k:    2,
			want: nil,
		},
		{
			name: "top k order ambiguity alone does not rerank",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.3},
				{ID: 2, Distance: 1.2, ErrorBound: 0.3},
				{ID: 3, Distance: 3.0, ErrorBound: 0.01},
			},
			k:    2,
			want: nil,
		},
		{
			name: "top k boundary ambiguous reranks boundary pair",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.05},
				{ID: 2, Distance: 2.0, ErrorBound: 0.05},
				{ID: 3, Distance: 2.02, ErrorBound: 0.2},
				{ID: 4, Distance: 4.0, ErrorBound: 0.01},
			},
			k:    2,
			want: []int{1, 2},
		},
		{
			name: "multiple boundary overlaps rerank only boundary set",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.01},
				{ID: 2, Distance: 2.0, ErrorBound: 0.05},
				{ID: 3, Distance: 2.02, ErrorBound: 0.2},
				{ID: 4, Distance: 2.04, ErrorBound: 0.2},
				{ID: 5, Distance: 5.0, ErrorBound: 0.01},
			},
			k:    2,
			want: []int{1, 2, 3},
		},
		{
			name: "distance over threshold ambiguity reranks retained set",
			results: []*Result{
				{ID: 1, Distance: 1.0, ErrorBound: 0.2},
				{ID: 2, Distance: 2.0, ErrorBound: 0.2},
			},
			k:            1,
			distanceOver: f32(1.1),
			want:         []int{0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				tt.want,
				selectAutoRerankCandidates(tt.results, tt.k, tt.distanceOver, tt.distanceUnder),
			)
		})
	}
}

func TestSearchInContextPropagatesErrorBound(t *testing.T) {
	t.Parallel()

	debug := &SearchDebugInfo{
		ResolvedRerankPolicy: RerankPolicyBoundary,
		RerankCandidateCount: 1,
		ApproxTop: []SearchDebugHit{
			{ID: 7, Distance: 1.25, ErrorBound: 0.15, LowerBound: 1.10, UpperBound: 1.40},
		},
	}
	idx := &stubVectorIndex{
		name:  "stub",
		total: 1,
		results: []*Result{
			{ID: 7, Distance: 1.25, ErrorBound: 0.15, Metadata: []byte("doc:7")},
		},
	}

	result, err := SearchInContext(context.Background(), idx, &SearchRequest{
		Embedding: vector.T{0, 1},
		K:         1,
		Debug:     debug,
	})
	require.NoError(t, err)
	require.Len(t, result.Hits, 1)
	assert.Equal(t, float32(1.25), result.Hits[0].Distance)
	assert.Equal(t, float32(0.15), result.Hits[0].ErrorBound)
	assert.Equal(t, "doc:7", result.Hits[0].ID)
	require.NotNil(t, result.Debug)
	assert.Equal(t, RerankPolicyBoundary, result.Debug.ResolvedRerankPolicy)
	assert.Equal(t, 1, result.Debug.RerankCandidateCount)
	require.Len(t, result.Debug.ApproxTop, 1)
	assert.Equal(t, uint64(7), result.Debug.ApproxTop[0].ID)
}

func TestPopulateApproxSearchDebug(t *testing.T) {
	t.Parallel()

	debug := &SearchDebugInfo{}
	results := []*Result{
		{ID: 1, Distance: 1.0, ErrorBound: 0.2},
		{ID: 2, Distance: 1.15, ErrorBound: 0.2},
		{ID: 3, Distance: 1.4, ErrorBound: 0.4},
	}

	populateApproxSearchDebug(debug, results, 2, nil, nil, RerankPolicyBoundary)

	assert.Equal(t, RerankPolicyBoundary, debug.ResolvedRerankPolicy)
	assert.Equal(t, 3, debug.ApproxCandidateCount)
	assert.Equal(t, 2, debug.TopKCount)
	require.Len(t, debug.ApproxTop, 2)
	assert.Equal(t, uint64(1), debug.ApproxTop[0].ID)
	assert.Equal(t, float32(0.8), debug.ApproxTop[0].LowerBound)
	assert.Equal(t, float32(1.2), debug.ApproxTop[0].UpperBound)
	assert.Equal(t, 1, debug.AmbiguousTopKPairs)
	assert.Equal(t, 1, debug.AmbiguousBoundaryPairs)
	require.NotNil(t, debug.ClosestPairTopK)
	assert.True(t, debug.ClosestPairTopK.Overlaps)
	require.NotNil(t, debug.BoundaryPair)
	assert.Equal(t, uint64(2), debug.BoundaryPair.Left.ID)
	assert.Equal(t, uint64(3), debug.BoundaryPair.Right.ID)
}
