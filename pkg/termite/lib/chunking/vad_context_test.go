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

package chunking

import (
	"context"
	"testing"
)

func TestVADConfigContext_RoundTrip(t *testing.T) {
	original := VADConfig{
		Threshold:            0.7,
		MinSpeechDurationMs:  500,
		MinSilenceDurationMs: 400,
		SpeechPadMs:          60,
		MaxSegmentDurationMs: 15000,
	}

	ctx := WithVADConfig(context.Background(), original)
	got, ok := VADConfigFromContext(ctx)
	if !ok {
		t.Fatal("VADConfigFromContext returned false for context with VADConfig")
	}
	if got != original {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

func TestVADConfigContext_Missing(t *testing.T) {
	_, ok := VADConfigFromContext(context.Background())
	if ok {
		t.Fatal("VADConfigFromContext returned true for context without VADConfig")
	}
}

// TestVADContextOverride_Full verifies that a full context-injected VADConfig
// overrides all fields of the construction-time config.
func TestVADContextOverride_Full(t *testing.T) {
	constructionConfig := DefaultVADConfig()

	ctxOverride := VADConfig{
		MinSilenceDurationMs: 1000,
		MinSpeechDurationMs:  500,
		SpeechPadMs:          100,
		MaxSegmentDurationMs: 5000,
	}

	// Simulate the apply-block logic from ChunkPCM
	config := constructionConfig
	if ctxOverride.MinSilenceDurationMs > 0 {
		config.MinSilenceDurationMs = ctxOverride.MinSilenceDurationMs
	}
	if ctxOverride.MinSpeechDurationMs > 0 {
		config.MinSpeechDurationMs = ctxOverride.MinSpeechDurationMs
	}
	if ctxOverride.SpeechPadMs > 0 {
		config.SpeechPadMs = ctxOverride.SpeechPadMs
	}
	if ctxOverride.MaxSegmentDurationMs > 0 {
		config.MaxSegmentDurationMs = ctxOverride.MaxSegmentDurationMs
	}

	if config.MinSilenceDurationMs != 1000 {
		t.Errorf("MinSilenceDurationMs = %d, want 1000", config.MinSilenceDurationMs)
	}
	if config.MinSpeechDurationMs != 500 {
		t.Errorf("MinSpeechDurationMs = %d, want 500", config.MinSpeechDurationMs)
	}
	if config.SpeechPadMs != 100 {
		t.Errorf("SpeechPadMs = %d, want 100", config.SpeechPadMs)
	}
	if config.MaxSegmentDurationMs != 5000 {
		t.Errorf("MaxSegmentDurationMs = %d, want 5000", config.MaxSegmentDurationMs)
	}
	// Threshold should be unchanged (not in context override)
	if config.Threshold != constructionConfig.Threshold {
		t.Errorf("Threshold = %v, want %v (unchanged)", config.Threshold, constructionConfig.Threshold)
	}
}

// TestVADContextOverride_Partial verifies that a partial context-injected VADConfig
// only overrides non-zero fields, leaving others at construction-time defaults.
func TestVADContextOverride_Partial(t *testing.T) {
	constructionConfig := DefaultVADConfig()

	// Only override MinSilenceDurationMs
	ctxOverride := VADConfig{
		MinSilenceDurationMs: 2000,
	}

	config := constructionConfig
	if ctxOverride.MinSilenceDurationMs > 0 {
		config.MinSilenceDurationMs = ctxOverride.MinSilenceDurationMs
	}
	if ctxOverride.MinSpeechDurationMs > 0 {
		config.MinSpeechDurationMs = ctxOverride.MinSpeechDurationMs
	}
	if ctxOverride.SpeechPadMs > 0 {
		config.SpeechPadMs = ctxOverride.SpeechPadMs
	}
	if ctxOverride.MaxSegmentDurationMs > 0 {
		config.MaxSegmentDurationMs = ctxOverride.MaxSegmentDurationMs
	}

	if config.MinSilenceDurationMs != 2000 {
		t.Errorf("MinSilenceDurationMs = %d, want 2000", config.MinSilenceDurationMs)
	}
	// All other fields should remain at defaults
	if config.MinSpeechDurationMs != constructionConfig.MinSpeechDurationMs {
		t.Errorf("MinSpeechDurationMs = %d, want %d (default)", config.MinSpeechDurationMs, constructionConfig.MinSpeechDurationMs)
	}
	if config.SpeechPadMs != constructionConfig.SpeechPadMs {
		t.Errorf("SpeechPadMs = %d, want %d (default)", config.SpeechPadMs, constructionConfig.SpeechPadMs)
	}
	if config.MaxSegmentDurationMs != constructionConfig.MaxSegmentDurationMs {
		t.Errorf("MaxSegmentDurationMs = %d, want %d (default)", config.MaxSegmentDurationMs, constructionConfig.MaxSegmentDurationMs)
	}
}

// TestVADContextOverride_None verifies that when no context VADConfig is present,
// construction-time defaults are preserved entirely.
func TestVADContextOverride_None(t *testing.T) {
	constructionConfig := DefaultVADConfig()

	// Simulate: VADConfigFromContext returns false
	config := constructionConfig
	if _, ok := VADConfigFromContext(context.Background()); ok {
		t.Fatal("unexpected VADConfig in empty context")
	}
	// config should be identical to construction defaults
	if config != constructionConfig {
		t.Errorf("config = %+v, want %+v", config, constructionConfig)
	}
}

// TestVADContextOverride_AffectsMergeVADFrames tests the end-to-end effect:
// a context-injected MinSilenceDurationMs override changes MergeVADFrames behavior.
func TestVADContextOverride_AffectsMergeVADFrames(t *testing.T) {
	// Two speech regions separated by a short silence gap (2 frames = 64ms)
	probs := []float32{0.9, 0.9, 0.9, 0.9, 0.9, 0.1, 0.1, 0.9, 0.9, 0.9, 0.9, 0.9}

	// With default config (MinSilenceDurationMs=300), gap of 64ms should be merged
	defaultConfig := DefaultVADConfig()
	defaultConfig.SpeechPadMs = 0
	defaultConfig.MinSpeechDurationMs = 0
	segments := MergeVADFrames(probs, 512, 16000, defaultConfig)
	if len(segments) != 1 {
		t.Fatalf("with defaults: expected 1 merged segment, got %d", len(segments))
	}

	// Simulate context override: MinSilenceDurationMs=10 (very low), so gap of 64ms > 10ms → two segments
	ctx := WithVADConfig(context.Background(), VADConfig{
		MinSilenceDurationMs: 10,
	})
	vadCfg, ok := VADConfigFromContext(ctx)
	if !ok {
		t.Fatal("VADConfigFromContext returned false")
	}

	config := defaultConfig
	if vadCfg.MinSilenceDurationMs > 0 {
		config.MinSilenceDurationMs = vadCfg.MinSilenceDurationMs
	}

	segments = MergeVADFrames(probs, 512, 16000, config)
	if len(segments) != 2 {
		t.Fatalf("with context override: expected 2 segments, got %d", len(segments))
	}
}
