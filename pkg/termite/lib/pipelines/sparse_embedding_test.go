// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipelines

import (
	"math"
	"testing"
)

func TestApplySoftplusAndSparsify_KnownValues(t *testing.T) {
	// softplus(0) = ln(2) ≈ 0.6931
	// softplus(1) = ln(1+e) ≈ 1.3133
	// softplus(-5) ≈ 0.0067
	// softplus(20) ≈ 20.0 (saturates)
	input := []float32{0, 1, -5, 20}
	result := applySoftplusAndSparsify(input, 256, 0.0)

	if len(result.Indices) == 0 {
		t.Fatal("expected non-empty sparse vector")
	}

	// All inputs should produce positive softplus outputs
	for i, v := range result.Values {
		if v <= 0 {
			t.Errorf("expected positive value at index %d, got %f", result.Indices[i], v)
		}
	}

	// Build a map for easier lookup
	valueMap := make(map[uint32]float32)
	for i, idx := range result.Indices {
		valueMap[idx] = result.Values[i]
	}

	// softplus(0) ≈ 0.6931
	if v, ok := valueMap[0]; ok {
		if math.Abs(float64(v)-0.6931) > 0.01 {
			t.Errorf("softplus(0) = %f, want ≈ 0.6931", v)
		}
	} else {
		t.Error("expected index 0 in result (softplus(0) > 0)")
	}

	// softplus(1) ≈ 1.3133
	if v, ok := valueMap[1]; ok {
		if math.Abs(float64(v)-1.3133) > 0.01 {
			t.Errorf("softplus(1) = %f, want ≈ 1.3133", v)
		}
	} else {
		t.Error("expected index 1 in result")
	}

	// softplus(20) ≈ 20.0
	if v, ok := valueMap[3]; ok {
		if math.Abs(float64(v)-20.0) > 0.1 {
			t.Errorf("softplus(20) = %f, want ≈ 20.0", v)
		}
	} else {
		t.Error("expected index 3 in result (softplus(20) ≈ 20)")
	}
}

func TestApplySoftplusAndSparsify_TopK(t *testing.T) {
	// Create input with 10 distinct positive values
	input := make([]float32, 10)
	for i := range input {
		input[i] = float32(i) // 0, 1, 2, ..., 9
	}

	// With topK=3, should keep only the 3 largest softplus outputs
	result := applySoftplusAndSparsify(input, 3, 0.0)

	if len(result.Indices) != 3 {
		t.Fatalf("expected 3 entries with topK=3, got %d", len(result.Indices))
	}

	// The top-3 should be indices 7, 8, 9 (largest input values)
	// Output is sorted by index ascending
	expectedIndices := []uint32{7, 8, 9}
	for i, expected := range expectedIndices {
		if result.Indices[i] != expected {
			t.Errorf("result.Indices[%d] = %d, want %d", i, result.Indices[i], expected)
		}
	}
}

func TestApplySoftplusAndSparsify_MinWeight(t *testing.T) {
	// softplus(-5) ≈ 0.0067, softplus(0) ≈ 0.6931, softplus(5) ≈ 5.0067
	input := []float32{-5, 0, 5}

	// With minWeight=0.5, should exclude softplus(-5) ≈ 0.0067
	result := applySoftplusAndSparsify(input, 256, 0.5)

	if len(result.Indices) != 2 {
		t.Fatalf("expected 2 entries with minWeight=0.5, got %d (indices: %v, values: %v)",
			len(result.Indices), result.Indices, result.Values)
	}

	// Should have indices 1 (softplus(0)≈0.69) and 2 (softplus(5)≈5.0)
	for i, idx := range result.Indices {
		if result.Values[i] <= 0.5 {
			t.Errorf("value at index %d = %f, expected > 0.5", idx, result.Values[i])
		}
	}
}

func TestApplySoftplusAndSparsify_EmptyInput(t *testing.T) {
	result := applySoftplusAndSparsify(nil, 256, 0.0)
	if len(result.Indices) != 0 || len(result.Values) != 0 {
		t.Errorf("expected empty result for nil input, got %d entries", len(result.Indices))
	}

	result = applySoftplusAndSparsify([]float32{}, 256, 0.0)
	if len(result.Indices) != 0 || len(result.Values) != 0 {
		t.Errorf("expected empty result for empty input, got %d entries", len(result.Indices))
	}
}

func TestApplySoftplusAndSparsify_IndicesSorted(t *testing.T) {
	// Verify output indices are sorted ascending regardless of input order
	input := []float32{5, 1, 10, 3, 7}
	result := applySoftplusAndSparsify(input, 256, 0.0)

	for i := 1; i < len(result.Indices); i++ {
		if result.Indices[i] <= result.Indices[i-1] {
			t.Errorf("indices not sorted ascending at position %d: %v", i, result.Indices)
			break
		}
	}
}

func TestMaxPoolOverSequence_Basic(t *testing.T) {
	// batch=1, seq=2, hidden=3
	hiddenStates := [][][]float32{
		{
			{1, 2, 3},
			{4, 1, 2},
		},
	}
	mask := [][]int32{{1, 1}}

	result := maxPoolOverSequence(hiddenStates, mask)

	if len(result) != 1 {
		t.Fatalf("expected batch size 1, got %d", len(result))
	}

	expected := []float32{4, 2, 3}
	for i, v := range result[0] {
		if v != expected[i] {
			t.Errorf("result[0][%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestMaxPoolOverSequence_WithMask(t *testing.T) {
	// batch=1, seq=3, hidden=2
	// Third position is padding (mask=0)
	hiddenStates := [][][]float32{
		{
			{1, 2},
			{3, 1},
			{100, 100}, // padding — should be ignored
		},
	}
	mask := [][]int32{{1, 1, 0}}

	result := maxPoolOverSequence(hiddenStates, mask)

	expected := []float32{3, 2}
	for i, v := range result[0] {
		if v != expected[i] {
			t.Errorf("result[0][%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestMaxPoolOverSequence_AllMasked(t *testing.T) {
	// All positions masked → should get zeros
	hiddenStates := [][][]float32{
		{
			{5, 10},
			{20, 30},
		},
	}
	mask := [][]int32{{0, 0}}

	result := maxPoolOverSequence(hiddenStates, mask)

	for i, v := range result[0] {
		if v != 0 {
			t.Errorf("result[0][%d] = %f, want 0 (all masked)", i, v)
		}
	}
}

func TestMaxPoolOverSequence_Batch(t *testing.T) {
	// batch=2
	hiddenStates := [][][]float32{
		{{1, 5}, {3, 2}},
		{{10, 1}, {2, 20}},
	}
	mask := [][]int32{{1, 1}, {1, 1}}

	result := maxPoolOverSequence(hiddenStates, mask)

	if len(result) != 2 {
		t.Fatalf("expected batch size 2, got %d", len(result))
	}

	expected0 := []float32{3, 5}
	expected1 := []float32{10, 20}

	for i, v := range result[0] {
		if v != expected0[i] {
			t.Errorf("result[0][%d] = %f, want %f", i, v, expected0[i])
		}
	}
	for i, v := range result[1] {
		if v != expected1[i] {
			t.Errorf("result[1][%d] = %f, want %f", i, v, expected1[i])
		}
	}
}

func TestMaxPoolOverSequence_NilMask(t *testing.T) {
	// When mask is nil, all positions should be included
	hiddenStates := [][][]float32{
		{
			{1, 2},
			{3, 1},
		},
	}

	result := maxPoolOverSequence(hiddenStates, nil)

	expected := []float32{3, 2}
	for i, v := range result[0] {
		if v != expected[i] {
			t.Errorf("result[0][%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestMaxPoolOverSequence_EmptySequence(t *testing.T) {
	hiddenStates := [][][]float32{{}}
	mask := [][]int32{{}}

	result := maxPoolOverSequence(hiddenStates, mask)
	if result[0] != nil {
		t.Errorf("expected nil for empty sequence, got %v", result[0])
	}
}

func TestDefaultSparseEmbeddingPipelineConfig(t *testing.T) {
	cfg := DefaultSparseEmbeddingPipelineConfig()
	if cfg.MaxLength != 512 {
		t.Errorf("MaxLength = %d, want 512", cfg.MaxLength)
	}
	if cfg.TopK != 256 {
		t.Errorf("TopK = %d, want 256", cfg.TopK)
	}
	if cfg.MinWeight != 0.0 {
		t.Errorf("MinWeight = %f, want 0.0", cfg.MinWeight)
	}
}
