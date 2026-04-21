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

package backends

import (
	"reflect"
	"testing"

	"github.com/gomlx/gomlx/pkg/core/tensors/bucketing"
)

func TestDefaultBucketStrategy(t *testing.T) {
	// defaultBucketStrategy is Exponential(1.4).
	// Verify it produces values >= input.
	strategy := defaultBucketStrategy

	tests := []struct {
		name  string
		input int
		// We verify output >= input (exact values depend on Exponential implementation).
	}{
		{name: "small", input: 1},
		{name: "medium", input: 10},
		{name: "large", input: 100},
		{name: "very large", input: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := strategy.Bucket(tt.input)
			if got < tt.input {
				t.Errorf("Bucket(%d) = %d, want >= %d", tt.input, got, tt.input)
			}
		})
	}
}

func TestBucketStrategyMonotonic(t *testing.T) {
	// Verify that bucketed values are monotonically non-decreasing.
	strategy := bucketing.Exponential(1.4)

	prev := 0
	for i := 1; i <= 100; i++ {
		got := strategy.Bucket(i)
		if got < prev {
			t.Errorf("Bucket(%d) = %d < Bucket(%d) = %d: not monotonic", i, got, i-1, prev)
		}
		prev = got
	}
}

func TestResolveBucketConfig(t *testing.T) {
	tests := []struct {
		name         string
		backendType  BackendType
		config       *LoadConfig
		wantEnabled  bool
		wantMaxCache int
		wantBatch    bool // true if batchStrategy should be non-nil
		wantSeq      bool // true if seqStrategy should be non-nil
	}{
		{
			name:         "Go backend defaults: disabled",
			backendType:  BackendGo,
			config:       &LoadConfig{},
			wantEnabled:  false,
			wantMaxCache: -1,
		},
		{
			name:        "Go backend with explicit batch strategy: enabled",
			backendType: BackendGo,
			config:      &LoadConfig{BatchBucketing: bucketing.Pow2()},
			wantEnabled: true,
			wantBatch:   true,
			wantSeq:     true, // defaults to defaultBucketStrategy
		},
		{
			name:        "Go backend with explicit seq strategy: enabled",
			backendType: BackendGo,
			config:      &LoadConfig{SeqBucketing: bucketing.Linear(64)},
			wantEnabled: true,
			wantBatch:   true, // defaults to defaultBucketStrategy
			wantSeq:     true,
		},
		{
			name:         "XLA backend defaults: enabled",
			backendType:  BackendXLA,
			config:       &LoadConfig{},
			wantEnabled:  true,
			wantMaxCache: 0, // use GoMLX default
			wantBatch:    true,
			wantSeq:      true,
		},
		{
			name:        "XLA backend with custom strategies",
			backendType: BackendXLA,
			config: &LoadConfig{
				BatchBucketing: bucketing.Pow2(),
				SeqBucketing:   bucketing.Linear(32),
			},
			wantEnabled: true,
			wantBatch:   true,
			wantSeq:     true,
		},
		{
			name:         "XLA backend with explicit max cache",
			backendType:  BackendXLA,
			config:       &LoadConfig{MaxCacheSize: 50},
			wantEnabled:  true,
			wantMaxCache: 50,
			wantBatch:    true,
			wantSeq:      true,
		},
		{
			name:         "XLA backend with unlimited cache",
			backendType:  BackendXLA,
			config:       &LoadConfig{MaxCacheSize: -1},
			wantEnabled:  true,
			wantMaxCache: -1,
			wantBatch:    true,
			wantSeq:      true,
		},
		{
			name:        "CoreML backend defaults: same as XLA",
			backendType: BackendCoreML,
			config:      &LoadConfig{},
			wantEnabled: true,
			wantBatch:   true,
			wantSeq:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveBucketConfig(tt.backendType, tt.config)

			if got.enabled != tt.wantEnabled {
				t.Errorf("enabled = %v, want %v", got.enabled, tt.wantEnabled)
			}
			if got.maxCacheSize != tt.wantMaxCache {
				t.Errorf("maxCacheSize = %d, want %d", got.maxCacheSize, tt.wantMaxCache)
			}
			if tt.wantBatch && got.batchStrategy == nil {
				t.Error("batchStrategy is nil, want non-nil")
			}
			if tt.wantSeq && got.seqStrategy == nil {
				t.Error("seqStrategy is nil, want non-nil")
			}
			if !tt.wantEnabled {
				// Disabled config should have nil strategies.
				if got.batchStrategy != nil {
					t.Error("disabled config should have nil batchStrategy")
				}
				if got.seqStrategy != nil {
					t.Error("disabled config should have nil seqStrategy")
				}
			}

			// Verify strategies produce valid buckets when enabled.
			if tt.wantEnabled && got.batchStrategy != nil {
				bucketed := got.batchStrategy.Bucket(5)
				if bucketed < 5 {
					t.Errorf("batchStrategy.Bucket(5) = %d, want >= 5", bucketed)
				}
			}
			if tt.wantEnabled && got.seqStrategy != nil {
				bucketed := got.seqStrategy.Bucket(100)
				if bucketed < 100 {
					t.Errorf("seqStrategy.Bucket(100) = %d, want >= 100", bucketed)
				}
			}
		})
	}
}

func TestPadModelInputs(t *testing.T) {
	t.Run("no padding needed", func(t *testing.T) {
		inputs := &ModelInputs{
			InputIDs:      [][]int32{{1, 2, 3}},
			AttentionMask: [][]int32{{1, 1, 1}},
		}
		got := padModelInputs(inputs, 1, 3)
		// Should return the same pointer when no padding needed.
		if got != inputs {
			t.Error("expected same pointer when no padding needed")
		}
	})

	t.Run("pad sequence only", func(t *testing.T) {
		inputs := &ModelInputs{
			InputIDs:      [][]int32{{1, 2}},
			AttentionMask: [][]int32{{1, 1}},
		}
		got := padModelInputs(inputs, 1, 4)

		if len(got.InputIDs) != 1 || len(got.InputIDs[0]) != 4 {
			t.Fatalf("InputIDs shape = [%d, %d], want [1, 4]", len(got.InputIDs), len(got.InputIDs[0]))
		}
		wantIDs := []int32{1, 2, 0, 0}
		if !reflect.DeepEqual(got.InputIDs[0], wantIDs) {
			t.Errorf("InputIDs[0] = %v, want %v", got.InputIDs[0], wantIDs)
		}
		wantMask := []int32{1, 1, 0, 0}
		if !reflect.DeepEqual(got.AttentionMask[0], wantMask) {
			t.Errorf("AttentionMask[0] = %v, want %v", got.AttentionMask[0], wantMask)
		}
	})

	t.Run("pad batch only", func(t *testing.T) {
		inputs := &ModelInputs{
			InputIDs:      [][]int32{{10, 20}},
			AttentionMask: [][]int32{{1, 1}},
		}
		got := padModelInputs(inputs, 3, 2)

		if len(got.InputIDs) != 3 {
			t.Fatalf("InputIDs batch = %d, want 3", len(got.InputIDs))
		}
		// Original row preserved.
		if !reflect.DeepEqual(got.InputIDs[0], []int32{10, 20}) {
			t.Errorf("InputIDs[0] = %v, want [10, 20]", got.InputIDs[0])
		}
		// Padded rows are zeros.
		if !reflect.DeepEqual(got.InputIDs[1], []int32{0, 0}) {
			t.Errorf("InputIDs[1] = %v, want [0, 0]", got.InputIDs[1])
		}
		if !reflect.DeepEqual(got.InputIDs[2], []int32{0, 0}) {
			t.Errorf("InputIDs[2] = %v, want [0, 0]", got.InputIDs[2])
		}
		// Attention mask padded rows should be zero.
		if !reflect.DeepEqual(got.AttentionMask[1], []int32{0, 0}) {
			t.Errorf("AttentionMask[1] = %v, want [0, 0]", got.AttentionMask[1])
		}
	})

	t.Run("pad both dimensions", func(t *testing.T) {
		inputs := &ModelInputs{
			InputIDs:      [][]int32{{1, 2}, {3, 4}},
			AttentionMask: [][]int32{{1, 1}, {1, 1}},
		}
		got := padModelInputs(inputs, 4, 4)

		if len(got.InputIDs) != 4 || len(got.InputIDs[0]) != 4 {
			t.Fatalf("InputIDs shape = [%d, %d], want [4, 4]", len(got.InputIDs), len(got.InputIDs[0]))
		}
		// Check original data preserved.
		if got.InputIDs[0][0] != 1 || got.InputIDs[0][1] != 2 {
			t.Errorf("InputIDs[0] = %v, want [1, 2, 0, 0]", got.InputIDs[0])
		}
		if got.InputIDs[1][0] != 3 || got.InputIDs[1][1] != 4 {
			t.Errorf("InputIDs[1] = %v, want [3, 4, 0, 0]", got.InputIDs[1])
		}
		// Padded batch rows.
		if got.InputIDs[2][0] != 0 || got.InputIDs[3][0] != 0 {
			t.Error("padded batch rows should be zero")
		}
	})

	t.Run("with TokenTypeIDs", func(t *testing.T) {
		inputs := &ModelInputs{
			InputIDs:      [][]int32{{1}},
			AttentionMask: [][]int32{{1}},
			TokenTypeIDs:  [][]int32{{0}},
		}
		got := padModelInputs(inputs, 2, 3)

		if len(got.TokenTypeIDs) != 2 || len(got.TokenTypeIDs[0]) != 3 {
			t.Fatalf("TokenTypeIDs shape = [%d, %d], want [2, 3]", len(got.TokenTypeIDs), len(got.TokenTypeIDs[0]))
		}
		if got.TokenTypeIDs[0][0] != 0 {
			t.Errorf("TokenTypeIDs[0][0] = %d, want 0", got.TokenTypeIDs[0][0])
		}
	})

	t.Run("nil TokenTypeIDs stays nil", func(t *testing.T) {
		inputs := &ModelInputs{
			InputIDs:      [][]int32{{1}},
			AttentionMask: [][]int32{{1}},
		}
		got := padModelInputs(inputs, 2, 2)
		if got.TokenTypeIDs != nil {
			t.Error("TokenTypeIDs should remain nil when original is nil")
		}
	})
}

func TestTrimModelOutput(t *testing.T) {
	t.Run("nil output", func(t *testing.T) {
		got := trimModelOutput(nil, 1, 1)
		if got != nil {
			t.Error("expected nil for nil input")
		}
	})

	t.Run("trim LastHiddenState batch and seq", func(t *testing.T) {
		output := &ModelOutput{
			LastHiddenState: [][][]float32{
				{{1, 2}, {3, 4}, {0, 0}}, // seq=3 (padded from 2)
				{{5, 6}, {7, 8}, {0, 0}}, // seq=3
				{{0, 0}, {0, 0}, {0, 0}}, // batch padding
				{{0, 0}, {0, 0}, {0, 0}}, // batch padding
			},
		}
		got := trimModelOutput(output, 2, 2)

		if len(got.LastHiddenState) != 2 {
			t.Fatalf("batch = %d, want 2", len(got.LastHiddenState))
		}
		if len(got.LastHiddenState[0]) != 2 {
			t.Fatalf("seq = %d, want 2", len(got.LastHiddenState[0]))
		}
		if got.LastHiddenState[0][0][0] != 1 || got.LastHiddenState[1][1][1] != 8 {
			t.Error("data values incorrect after trim")
		}
	})

	t.Run("trim Embeddings batch", func(t *testing.T) {
		output := &ModelOutput{
			Embeddings: [][]float32{
				{1, 2, 3},
				{4, 5, 6},
				{0, 0, 0}, // batch padding
			},
		}
		got := trimModelOutput(output, 2, 1)

		if len(got.Embeddings) != 2 {
			t.Fatalf("batch = %d, want 2", len(got.Embeddings))
		}
		if !reflect.DeepEqual(got.Embeddings[0], []float32{1, 2, 3}) {
			t.Errorf("Embeddings[0] = %v, want [1, 2, 3]", got.Embeddings[0])
		}
	})

	t.Run("trim Logits batch", func(t *testing.T) {
		output := &ModelOutput{
			Logits: [][]float32{
				{0.9, 0.1},
				{0.0, 0.0}, // batch padding
			},
		}
		got := trimModelOutput(output, 1, 1)

		if len(got.Logits) != 1 {
			t.Fatalf("batch = %d, want 1", len(got.Logits))
		}
	})

	t.Run("no trim when already correct size", func(t *testing.T) {
		output := &ModelOutput{
			Embeddings: [][]float32{{1, 2}},
			Logits:     [][]float32{{0.5}},
		}
		got := trimModelOutput(output, 1, 10)

		if len(got.Embeddings) != 1 {
			t.Error("should not trim when already at size")
		}
		if len(got.Logits) != 1 {
			t.Error("should not trim when already at size")
		}
	})

	t.Run("nil fields stay nil", func(t *testing.T) {
		output := &ModelOutput{
			Embeddings: [][]float32{{1}},
		}
		got := trimModelOutput(output, 1, 1)

		if got.LastHiddenState != nil {
			t.Error("LastHiddenState should remain nil")
		}
		if got.Logits != nil {
			t.Error("Logits should remain nil")
		}
		if got.EncoderOutput != nil {
			t.Error("EncoderOutput should remain nil")
		}
		if got.PastKeyValues != nil {
			t.Error("PastKeyValues should remain nil")
		}
	})
}
