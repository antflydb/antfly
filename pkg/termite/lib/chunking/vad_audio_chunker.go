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
	"fmt"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/audio"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

const (
	// vadSampleRate is the sample rate expected by Silero VAD.
	vadSampleRate = 16000
	// vadFrameSize is the number of samples per VAD frame at 16kHz (32ms).
	vadFrameSize = 512
	// vadHiddenDim is the LSTM hidden dimension per layer (Silero VAD v5 uses 128).
	vadHiddenDim = 128
	// vadNumLayers is the number of LSTM layers.
	vadNumLayers = 2
)

// VADConfig holds configuration for VAD-based audio chunking.
type VADConfig struct {
	// Threshold is the speech probability threshold (0.0-1.0).
	Threshold float32
	// MinSpeechDurationMs is the minimum speech segment duration in ms.
	MinSpeechDurationMs int
	// MinSilenceDurationMs is the minimum silence duration to split segments.
	MinSilenceDurationMs int
	// SpeechPadMs is padding added before and after detected speech.
	SpeechPadMs int
	// MaxSegmentDurationMs enforces a maximum segment length (for Whisper compatibility).
	MaxSegmentDurationMs int
}

// DefaultVADConfig returns sensible defaults for Silero VAD.
func DefaultVADConfig() VADConfig {
	return VADConfig{
		Threshold:            0.5,
		MinSpeechDurationMs:  250,
		MinSilenceDurationMs: 300,
		SpeechPadMs:          30,
		MaxSegmentDurationMs: 30000,
	}
}

// SpeechSegment represents a detected speech region in sample indices.
type SpeechSegment struct {
	StartSample int
	EndSample   int
}

// VADAudioChunker segments audio using Silero VAD for voice activity detection.
// It runs the ONNX model frame-by-frame, detects speech regions, then extracts
// each region as a WAV chunk.
type VADAudioChunker struct {
	session backends.Session
	config  VADConfig
}

// NewVADAudioChunker creates a VAD-based audio chunker.
func NewVADAudioChunker(session backends.Session, config VADConfig) *VADAudioChunker {
	return &VADAudioChunker{
		session: session,
		config:  config,
	}
}

// ChunkMedia implements the MediaChunker interface by dispatching to the
// appropriate format-specific method based on MIME type.
func (v *VADAudioChunker) ChunkMedia(ctx context.Context, data []byte, mimeType string, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	switch normalizeAudioMIME(mimeType) {
	case "wav":
		return v.ChunkAudio(ctx, data, opts)
	case "mp3":
		return v.ChunkMP3(ctx, data, opts)
	default:
		return nil, fmt.Errorf("VAD chunker does not support MIME type %q", mimeType)
	}
}

// ChunkAudio parses a WAV file and segments it using VAD.
func (v *VADAudioChunker) ChunkAudio(ctx context.Context, data []byte, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	samples, format, err := audio.ParseWAV(data)
	if err != nil {
		return nil, fmt.Errorf("parsing WAV: %w", err)
	}
	return v.ChunkPCM(ctx, samples, format, opts)
}

// ChunkMP3 decodes an MP3 file and segments it using VAD.
func (v *VADAudioChunker) ChunkMP3(ctx context.Context, data []byte, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	samples, format, err := audio.ParseMP3(data)
	if err != nil {
		return nil, fmt.Errorf("parsing MP3: %w", err)
	}
	return v.ChunkPCM(ctx, samples, format, opts)
}

// ChunkPCM segments decoded PCM samples using VAD.
func (v *VADAudioChunker) ChunkPCM(ctx context.Context, samples []float32, format audio.Format, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("audio contains no samples")
	}

	// Resample to 16kHz for VAD
	samples16k := audio.Resample(samples, format.SampleRate, vadSampleRate)

	// Run VAD to get per-frame speech probabilities
	probs, err := v.runVAD(ctx, samples16k)
	if err != nil {
		return nil, fmt.Errorf("running VAD: %w", err)
	}

	// Allow per-request overrides of VAD config
	config := v.config
	if opts.Threshold > 0 {
		config.Threshold = opts.Threshold
	}
	if vadCfg, ok := VADConfigFromContext(ctx); ok {
		if vadCfg.MinSilenceDurationMs > 0 {
			config.MinSilenceDurationMs = vadCfg.MinSilenceDurationMs
		}
		if vadCfg.MinSpeechDurationMs > 0 {
			config.MinSpeechDurationMs = vadCfg.MinSpeechDurationMs
		}
		if vadCfg.SpeechPadMs > 0 {
			config.SpeechPadMs = vadCfg.SpeechPadMs
		}
		if vadCfg.MaxSegmentDurationMs > 0 {
			config.MaxSegmentDurationMs = vadCfg.MaxSegmentDurationMs
		}
	}

	// Merge probabilities into speech segments (in 16kHz sample space)
	segments := MergeVADFrames(probs, vadFrameSize, vadSampleRate, config)

	if len(segments) == 0 {
		return nil, nil
	}

	// Map segments from 16kHz back to original sample rate and extract chunks
	ratio := float64(format.SampleRate) / float64(vadSampleRate)
	var chunks []chunking.Chunk

	for _, seg := range segments {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Map to original sample indices
		origStart := int(float64(seg.StartSample) * ratio)
		origEnd := int(float64(seg.EndSample) * ratio)

		// Clamp to bounds
		if origStart < 0 {
			origStart = 0
		}
		if origEnd > len(samples) {
			origEnd = len(samples)
		}
		if origStart >= origEnd {
			continue
		}

		windowData := samples[origStart:origEnd]

		wavBytes, err := audio.EncodeWAV(windowData, audio.Format{
			SampleRate:    format.SampleRate,
			BitsPerSample: format.BitsPerSample,
			NumChannels:   1,
		})
		if err != nil {
			return nil, fmt.Errorf("encoding WAV chunk %d: %w", len(chunks), err)
		}

		startTimeMs := float32(origStart) * 1000.0 / float32(format.SampleRate)
		endTimeMs := float32(origEnd) * 1000.0 / float32(format.SampleRate)

		var c chunking.Chunk
		c.Id = uint32(len(chunks))
		c.MimeType = "audio/wav"
		_ = c.FromBinaryContent(chunking.BinaryContent{
			Data:        wavBytes,
			StartTimeMs: startTimeMs,
			EndTimeMs:   endTimeMs,
		})

		chunks = append(chunks, c)

		if opts.MaxChunks > 0 && len(chunks) >= opts.MaxChunks {
			break
		}
	}

	return chunks, nil
}

// runVAD runs Silero VAD frame-by-frame and returns per-frame speech probabilities.
// Silero VAD v5 inputs: input [1, 512], state [2, 1, 128], sr scalar int64.
// Silero VAD v5 outputs: output [1, 1], stateN [2, 1, 128].
func (v *VADAudioChunker) runVAD(ctx context.Context, samples16k []float32) ([]float32, error) {
	// Initialize combined LSTM state to zeros: [2, 1, 128] (h and c stacked)
	stateSize := vadNumLayers * 1 * vadHiddenDim
	state := make([]float32, stateSize)

	numFrames := len(samples16k) / vadFrameSize
	if numFrames == 0 {
		return nil, nil
	}

	probs := make([]float32, numFrames)

	for i := range numFrames {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		frameStart := i * vadFrameSize
		frame := samples16k[frameStart : frameStart+vadFrameSize]

		inputs := []backends.NamedTensor{
			{Name: "input", Shape: []int64{1, vadFrameSize}, Data: frame},
			{Name: "state", Shape: []int64{vadNumLayers, 1, vadHiddenDim}, Data: state},
			{Name: "sr", Shape: []int64{1}, Data: []int64{vadSampleRate}},
		}

		outputs, err := v.session.Run(inputs)
		if err != nil {
			return nil, fmt.Errorf("VAD inference frame %d: %w", i, err)
		}

		// Extract outputs by name
		for _, out := range outputs {
			switch out.Name {
			case "output":
				if probData, ok := out.Data.([]float32); ok && len(probData) > 0 {
					probs[i] = probData[0]
				}
			case "stateN":
				if stateData, ok := out.Data.([]float32); ok {
					state = stateData
				}
			}
		}
	}

	return probs, nil
}

// MergeVADFrames converts per-frame speech probabilities into speech segments.
// Exported for unit testing without an ONNX model.
func MergeVADFrames(probs []float32, frameSizeSamples, sampleRate int, config VADConfig) []SpeechSegment {
	if len(probs) == 0 {
		return nil
	}

	// Step 1: Threshold into speech/silence
	speech := make([]bool, len(probs))
	for i, p := range probs {
		speech[i] = p >= config.Threshold
	}

	// Step 2: Group consecutive speech frames into raw segments
	var raw []SpeechSegment
	inSpeech := false
	var segStart int

	for i, s := range speech {
		if s && !inSpeech {
			segStart = i
			inSpeech = true
		} else if !s && inSpeech {
			raw = append(raw, SpeechSegment{
				StartSample: segStart * frameSizeSamples,
				EndSample:   i * frameSizeSamples,
			})
			inSpeech = false
		}
	}
	if inSpeech {
		raw = append(raw, SpeechSegment{
			StartSample: segStart * frameSizeSamples,
			EndSample:   len(probs) * frameSizeSamples,
		})
	}

	if len(raw) == 0 {
		return nil
	}

	// Step 3: Filter out segments shorter than MinSpeechDurationMs
	minSpeechSamples := config.MinSpeechDurationMs * sampleRate / 1000
	var filtered []SpeechSegment
	for _, seg := range raw {
		if seg.EndSample-seg.StartSample >= minSpeechSamples {
			filtered = append(filtered, seg)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	// Step 4: Merge segments separated by silence shorter than MinSilenceDurationMs
	minSilenceSamples := config.MinSilenceDurationMs * sampleRate / 1000
	merged := []SpeechSegment{filtered[0]}
	for _, seg := range filtered[1:] {
		last := &merged[len(merged)-1]
		gap := seg.StartSample - last.EndSample
		if gap < minSilenceSamples {
			last.EndSample = seg.EndSample
		} else {
			merged = append(merged, seg)
		}
	}

	// Step 5: Add padding
	padSamples := config.SpeechPadMs * sampleRate / 1000
	totalSamples := len(probs) * frameSizeSamples
	for i := range merged {
		merged[i].StartSample -= padSamples
		if merged[i].StartSample < 0 {
			merged[i].StartSample = 0
		}
		merged[i].EndSample += padSamples
		if merged[i].EndSample > totalSamples {
			merged[i].EndSample = totalSamples
		}
	}

	// Step 6: Split segments exceeding MaxSegmentDurationMs
	if config.MaxSegmentDurationMs <= 0 {
		return merged
	}

	maxSamples := config.MaxSegmentDurationMs * sampleRate / 1000
	var result []SpeechSegment
	for _, seg := range merged {
		duration := seg.EndSample - seg.StartSample
		if duration <= maxSamples {
			result = append(result, seg)
			continue
		}
		// Split into sub-segments
		for start := seg.StartSample; start < seg.EndSample; start += maxSamples {
			end := min(start+maxSamples, seg.EndSample)
			result = append(result, SpeechSegment{
				StartSample: start,
				EndSample:   end,
			})
		}
	}

	return result
}

// Close releases resources associated with the VAD session.
func (v *VADAudioChunker) Close() error {
	if v.session != nil {
		return v.session.Close()
	}
	return nil
}
