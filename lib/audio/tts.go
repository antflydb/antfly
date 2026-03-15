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

package audio

import (
	"context"
	"fmt"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/s3"
)

// TTS is the interface for text-to-speech providers.
type TTS interface {
	// Synthesize generates audio from text.
	// Returns audio bytes with format info, or uploads to S3 if S3Output is set.
	Synthesize(ctx context.Context, req SynthesizeRequest) (*SynthesizeResponse, error)

	// SynthesizeStream returns audio chunks as they're generated for real-time playback.
	SynthesizeStream(ctx context.Context, req SynthesizeRequest) (<-chan AudioChunk, error)

	// ListVoices returns available voices for this provider.
	ListVoices(ctx context.Context) ([]Voice, error)

	// Capabilities returns what this TTS provider supports.
	Capabilities() TTSCapabilities
}

// SynthesizeRequest contains parameters for speech synthesis.
type SynthesizeRequest struct {
	// Text is the text to synthesize into speech.
	Text string

	// Voice is the voice ID to use. Provider-specific.
	Voice string

	// Format is the desired output audio format.
	Format AudioFormat

	// Speed is the playback speed multiplier (0.25-4.0, default 1.0).
	Speed float64

	// Pitch adjustment in semitones (-20.0 to 20.0, Google only).
	Pitch float64

	// Output destination.
	// If S3Output is empty, returns inline audio bytes.
	S3Output string // S3 URI to upload audio (s3://bucket/key)

	// S3Credentials provides credentials for S3 output. Falls back to server defaults.
	S3Credentials *s3.Credentials
}

// SynthesizeResponse contains the result of speech synthesis.
type SynthesizeResponse struct {
	// Audio contains the synthesized audio bytes (if S3Output was not set).
	Audio []byte

	// Format is the audio format of the response.
	Format AudioFormat

	// S3URL is the S3 URI where audio was written (if S3Output was set).
	S3URL string

	// Duration is the duration of the synthesized audio.
	Duration time.Duration

	// CharactersUsed is the number of characters processed (for billing tracking).
	CharactersUsed int
}

// AudioChunk represents a chunk of streaming audio data.
type AudioChunk struct {
	// Data contains the audio bytes for this chunk.
	Data []byte

	// Offset is the time offset of this chunk from the start.
	Offset time.Duration

	// Error is set if an error occurred during streaming.
	Error error
}

// TTSCapabilities describes what a TTS provider supports.
type TTSCapabilities struct {
	// SupportedFormats lists the audio formats this provider can output.
	SupportedFormats []AudioFormat

	// MaxTextLength is the maximum text length per request.
	MaxTextLength int

	// SupportsSSML indicates if SSML markup is supported.
	SupportsSSML bool

	// SupportsStreaming indicates if real-time streaming is supported.
	SupportsStreaming bool

	// SupportsPitch indicates if pitch adjustment is supported.
	SupportsPitch bool

	// MinSpeed is the minimum speed multiplier.
	MinSpeed float64

	// MaxSpeed is the maximum speed multiplier.
	MaxSpeed float64
}

// TTSRegistry holds registered TTS provider constructors.
var TTSRegistry = map[TTSProvider]func(config TTSConfig) (TTS, error){}

// RegisterTTS registers a TTS provider constructor.
func RegisterTTS(typ TTSProvider, constructor func(config TTSConfig) (TTS, error)) {
	if _, exists := TTSRegistry[typ]; exists {
		panic(fmt.Sprintf("TTS provider %s already registered", typ))
	}
	TTSRegistry[typ] = constructor
}

// DeregisterTTS removes a TTS provider from the registry.
func DeregisterTTS(typ TTSProvider) {
	delete(TTSRegistry, typ)
}

// NewTTS creates a new TTS provider from configuration.
func NewTTS(conf TTSConfig) (TTS, error) {
	if conf.Provider == "" {
		return nil, fmt.Errorf("provider not specified")
	}
	constructor, ok := TTSRegistry[conf.Provider]
	if !ok {
		return nil, fmt.Errorf("no TTS provider registered for type %s", conf.Provider)
	}
	return constructor(conf)
}
