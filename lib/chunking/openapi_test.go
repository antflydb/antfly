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

package chunking

import (
	"testing"

	libafchunking "github.com/antflydb/antfly/pkg/libaf/chunking"
	termchunking "github.com/antflydb/antfly/pkg/termite/lib/chunking"
)

func TestNewChunkerConfig(t *testing.T) {
	tests := []struct {
		name         string
		config       any
		wantErr      bool
		wantProvider ChunkerProvider
	}{
		{
			name: "termite config",
			config: TermiteChunkerConfig{
				Model: termchunking.ModelFixedBert,
			},
			wantErr:      false,
			wantProvider: ChunkerProviderTermite,
		},
		{
			name:    "unknown config type",
			config:  "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewChunkerConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewChunkerConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.Provider != tt.wantProvider {
				t.Errorf("NewChunkerConfig() provider = %v, want %v", got.Provider, tt.wantProvider)
			}
		})
	}
}

func TestGetProviderConfig(t *testing.T) {
	targetTokens := 500
	overlapTokens := 50
	termiteConfig := TermiteChunkerConfig{
		Model: termchunking.ModelFixedBert,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  targetTokens,
			OverlapTokens: overlapTokens,
		},
	}

	chunkerConfig, err := NewChunkerConfig(termiteConfig)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	got, err := GetProviderConfig(*chunkerConfig)
	if err != nil {
		t.Fatalf("GetProviderConfig() failed: %v", err)
	}

	termite, ok := got.(TermiteChunkerConfig)
	if !ok {
		t.Fatalf("GetProviderConfig() returned wrong type: %T", got)
	}

	if termite.Model != termchunking.ModelFixedBert {
		t.Errorf("Model = %v, want %v", termite.Model, termchunking.ModelFixedBert)
	}
	if termite.Text.TargetTokens != 500 {
		t.Errorf("TargetTokens = %v, want 500", termite.Text.TargetTokens)
	}
	if termite.Text.OverlapTokens != 50 {
		t.Errorf("OverlapTokens = %v, want 50", termite.Text.OverlapTokens)
	}
}

func TestRoundTrip(t *testing.T) {
	// Test that we can round-trip a config through NewChunkerConfig and GetProviderConfig
	targetTokens := 1000
	maxChunks := 100
	original := TermiteChunkerConfig{
		Model:     "chonky-mmbert-small-multilingual-1", // Use actual model name (directory-based)
		MaxChunks: maxChunks,
		Text: libafchunking.TextChunkOptions{
			TargetTokens: targetTokens,
		},
	}

	unified, err := NewChunkerConfig(original)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	extracted, err := GetProviderConfig(*unified)
	if err != nil {
		t.Fatalf("GetProviderConfig() failed: %v", err)
	}

	termite, ok := extracted.(TermiteChunkerConfig)
	if !ok {
		t.Fatalf("GetProviderConfig() returned wrong type: %T", extracted)
	}

	if termite.Model != original.Model {
		t.Errorf("Model mismatch: got %v, want %v", termite.Model, original.Model)
	}
	if termite.Text.TargetTokens != original.Text.TargetTokens {
		t.Errorf("TargetTokens mismatch: got %v, want %v", termite.Text.TargetTokens, original.Text.TargetTokens)
	}
	if termite.MaxChunks != original.MaxChunks {
		t.Errorf("MaxChunks mismatch: got %v, want %v", termite.MaxChunks, original.MaxChunks)
	}
}
