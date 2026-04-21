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

package audio

import (
	"math"
	"testing"
)

func TestResample_Identity(t *testing.T) {
	samples := []float32{0.1, 0.2, 0.3, 0.4, 0.5}
	result := Resample(samples, 16000, 16000)
	if len(result) != len(samples) {
		t.Fatalf("expected %d samples, got %d", len(samples), len(result))
	}
	for i, s := range result {
		if s != samples[i] {
			t.Errorf("sample %d: expected %f, got %f", i, samples[i], s)
		}
	}
}

func TestResample_Downsample(t *testing.T) {
	// 100 samples at 44100 -> 16000 should produce ~36 samples
	n := 100
	samples := make([]float32, n)
	for i := range n {
		samples[i] = float32(i) / float32(n)
	}

	result := Resample(samples, 44100, 16000)
	expected := int(float64(n) * 16000.0 / 44100.0)
	if math.Abs(float64(len(result)-expected)) > 1 {
		t.Errorf("expected ~%d samples, got %d", expected, len(result))
	}
}

func TestResample_Upsample(t *testing.T) {
	// 100 samples at 8000 -> 16000 should produce ~200 samples
	n := 100
	samples := make([]float32, n)
	for i := range n {
		samples[i] = float32(i) / float32(n)
	}

	result := Resample(samples, 8000, 16000)
	expected := int(float64(n) * 16000.0 / 8000.0)
	if math.Abs(float64(len(result)-expected)) > 1 {
		t.Errorf("expected ~%d samples, got %d", expected, len(result))
	}

	// Upsampled values should be interpolated between original values
	for i, s := range result {
		if s < 0 || s > 1.0 {
			t.Errorf("sample %d out of range: %f", i, s)
		}
	}
}

func TestResample_Empty(t *testing.T) {
	result := Resample(nil, 44100, 16000)
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d samples", len(result))
	}
}
