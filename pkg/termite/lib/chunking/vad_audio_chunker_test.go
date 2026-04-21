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
	"testing"
)

func TestMergeVADFrames_BasicMerging(t *testing.T) {
	// 10 frames, frames 2-5 are speech
	probs := []float32{0.1, 0.1, 0.9, 0.8, 0.7, 0.9, 0.1, 0.1, 0.1, 0.1}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 0,
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}
	if segments[0].StartSample != 2*512 {
		t.Errorf("expected start %d, got %d", 2*512, segments[0].StartSample)
	}
	if segments[0].EndSample != 6*512 {
		t.Errorf("expected end %d, got %d", 6*512, segments[0].EndSample)
	}
}

func TestMergeVADFrames_MultipleSegments(t *testing.T) {
	// Two separate speech regions: frames 1-2 and frames 7-8
	probs := []float32{0.1, 0.9, 0.8, 0.1, 0.1, 0.1, 0.1, 0.9, 0.7, 0.1}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 0,
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	if segments[0].StartSample != 1*512 || segments[0].EndSample != 3*512 {
		t.Errorf("segment 0: got [%d, %d], want [%d, %d]",
			segments[0].StartSample, segments[0].EndSample, 1*512, 3*512)
	}
	if segments[1].StartSample != 7*512 || segments[1].EndSample != 9*512 {
		t.Errorf("segment 1: got [%d, %d], want [%d, %d]",
			segments[1].StartSample, segments[1].EndSample, 7*512, 9*512)
	}
}

func TestMergeVADFrames_MinSpeechDuration(t *testing.T) {
	// One very short speech segment (1 frame = 32ms at 16kHz) that should be filtered
	probs := []float32{0.1, 0.9, 0.1, 0.1, 0.1}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  250, // 250ms minimum
		MinSilenceDurationMs: 0,
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 0 {
		t.Fatalf("expected 0 segments (too short), got %d", len(segments))
	}
}

func TestMergeVADFrames_MinSpeechDuration_Keeps(t *testing.T) {
	// Speech region spanning 10 frames = 320ms, above 250ms threshold
	probs := make([]float32, 15)
	for i := 2; i < 12; i++ {
		probs[i] = 0.9
	}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  250,
		MinSilenceDurationMs: 0,
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}
}

func TestMergeVADFrames_MinSilenceMerging(t *testing.T) {
	// Two speech regions separated by a short silence gap (2 frames = 64ms)
	probs := []float32{0.9, 0.9, 0.9, 0.9, 0.9, 0.1, 0.1, 0.9, 0.9, 0.9, 0.9, 0.9}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 300, // 300ms — gap is only 64ms, so segments should merge
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 1 {
		t.Fatalf("expected 1 merged segment, got %d", len(segments))
	}
	if segments[0].StartSample != 0 || segments[0].EndSample != 12*512 {
		t.Errorf("expected [0, %d], got [%d, %d]", 12*512, segments[0].StartSample, segments[0].EndSample)
	}
}

func TestMergeVADFrames_MinSilence_KeepsSeparate(t *testing.T) {
	// Two speech regions separated by a long silence gap (20 frames = 640ms)
	probs := make([]float32, 35)
	for i := range 5 {
		probs[i] = 0.9
	}
	for i := 25; i < 30; i++ {
		probs[i] = 0.9
	}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 300, // 300ms — gap is 640ms, so segments stay separate
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
}

func TestMergeVADFrames_Padding(t *testing.T) {
	// Speech at frames 5-7
	probs := make([]float32, 15)
	probs[5] = 0.9
	probs[6] = 0.9
	probs[7] = 0.9
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 0,
		SpeechPadMs:          50, // 50ms padding = 800 samples at 16kHz
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}

	padSamples := 50 * 16000 / 1000 // 800
	expectedStart := 5*512 - padSamples
	expectedEnd := 8*512 + padSamples

	if segments[0].StartSample != expectedStart {
		t.Errorf("expected start %d, got %d", expectedStart, segments[0].StartSample)
	}
	if segments[0].EndSample != expectedEnd {
		t.Errorf("expected end %d, got %d", expectedEnd, segments[0].EndSample)
	}
}

func TestMergeVADFrames_PaddingClampedToZero(t *testing.T) {
	// Speech at frame 0 — padding should not go below 0
	probs := []float32{0.9, 0.9, 0.1, 0.1}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 0,
		SpeechPadMs:          100,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}
	if segments[0].StartSample != 0 {
		t.Errorf("expected start clamped to 0, got %d", segments[0].StartSample)
	}
}

func TestMergeVADFrames_MaxSegmentDuration(t *testing.T) {
	// Long speech region: 100 frames = 3.2s at 16kHz
	probs := make([]float32, 100)
	for i := range probs {
		probs[i] = 0.9
	}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 0,
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 1000, // 1 second max
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	// 3.2s / 1s = should produce 4 segments (last one shorter)
	if len(segments) < 3 {
		t.Fatalf("expected at least 3 segments, got %d", len(segments))
	}

	// Each segment except possibly the last should be <= 1s = 16000 samples
	for i, seg := range segments {
		dur := seg.EndSample - seg.StartSample
		if i < len(segments)-1 && dur > 16000 {
			t.Errorf("segment %d: duration %d samples exceeds max 16000", i, dur)
		}
	}
}

func TestMergeVADFrames_AllSilence(t *testing.T) {
	probs := []float32{0.1, 0.2, 0.1, 0.3, 0.1}
	config := DefaultVADConfig()

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 0 {
		t.Fatalf("expected 0 segments for all silence, got %d", len(segments))
	}
}

func TestMergeVADFrames_Empty(t *testing.T) {
	segments := MergeVADFrames(nil, 512, 16000, DefaultVADConfig())
	if segments != nil {
		t.Fatalf("expected nil for empty input, got %v", segments)
	}
}

func TestMergeVADFrames_AllSpeech(t *testing.T) {
	probs := []float32{0.9, 0.8, 0.9, 0.7, 0.9}
	config := VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  0,
		MinSilenceDurationMs: 0,
		SpeechPadMs:          0,
		MaxSegmentDurationMs: 0,
	}

	segments := MergeVADFrames(probs, 512, 16000, config)

	if len(segments) != 1 {
		t.Fatalf("expected 1 segment for all speech, got %d", len(segments))
	}
	if segments[0].StartSample != 0 || segments[0].EndSample != 5*512 {
		t.Errorf("expected [0, %d], got [%d, %d]", 5*512, segments[0].StartSample, segments[0].EndSample)
	}
}
