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

// STT is the interface for speech-to-text providers.
type STT interface {
	// Transcribe converts audio to text.
	Transcribe(ctx context.Context, req TranscribeRequest) (*TranscribeResponse, error)

	// TranscribeStream processes audio stream in real-time.
	// Audio chunks are sent via the input channel, transcripts are received via the output channel.
	TranscribeStream(ctx context.Context, audioStream <-chan []byte, opts StreamOptions) (<-chan TranscriptChunk, error)

	// Capabilities returns what this STT provider supports.
	Capabilities() STTCapabilities
}

// TranscribeRequest contains parameters for speech transcription.
type TranscribeRequest struct {
	// Input source (one of these must be set)
	Audio []byte // Inline audio bytes
	URL   string // URL to audio file (supports http://, https://, or s3:// URIs)

	// S3Credentials provides credentials for s3:// URLs. Falls back to server defaults.
	S3Credentials *s3.Credentials

	// Format is the audio format hint (optional, auto-detected if not set).
	Format AudioFormat

	// Language is the ISO 639-1 language code (e.g., "en", "es", "fr").
	// Leave empty for auto-detection.
	Language string

	// Timestamps enables word-level timestamp generation.
	Timestamps bool

	// Diarization enables speaker diarization (identifying different speakers).
	Diarization bool

	// MinSpeakers is the minimum number of speakers for diarization (Google only).
	MinSpeakers int

	// MaxSpeakers is the maximum number of speakers for diarization (Google only).
	MaxSpeakers int

	// Prompt is optional context/prompt to guide transcription (OpenAI only).
	Prompt string
}

// TranscribeResponse contains the transcription result.
type TranscribeResponse struct {
	// Text is the full transcribed text.
	Text string

	// Language is the detected or specified language code.
	Language string

	// Duration is the duration of the audio.
	Duration time.Duration

	// Segments contains timestamped segments of the transcription.
	Segments []TranscriptSegment

	// Speakers contains identified speakers (if diarization was enabled).
	Speakers []Speaker
}

// StreamOptions contains options for streaming transcription.
type StreamOptions struct {
	// Format is the audio format of the stream.
	Format AudioFormat

	// SampleRate is the audio sample rate in Hz (e.g., 16000, 44100).
	SampleRate int

	// Language is the expected language code.
	Language string

	// InterimResults enables receiving non-final transcription results.
	InterimResults bool

	// Diarization enables speaker diarization in streaming mode.
	Diarization bool
}

// TranscriptChunk represents a chunk of streaming transcription.
type TranscriptChunk struct {
	// Text is the transcribed text for this chunk.
	Text string

	// IsFinal indicates if this is a final result (won't change).
	IsFinal bool

	// Confidence is the confidence score (0.0-1.0) if available.
	Confidence float64

	// Speaker is the speaker ID if diarization is enabled.
	Speaker string

	// Start is the start time of this chunk.
	Start time.Duration

	// End is the end time of this chunk.
	End time.Duration

	// Error is set if an error occurred during streaming.
	Error error
}

// STTCapabilities describes what an STT provider supports.
type STTCapabilities struct {
	// SupportedFormats lists the audio formats this provider can process.
	SupportedFormats []AudioFormat

	// MaxDurationSeconds is the maximum audio duration per request.
	MaxDurationSeconds int

	// MaxFileSizeBytes is the maximum file size per request.
	MaxFileSizeBytes int64

	// SupportsStreaming indicates if real-time streaming is supported.
	SupportsStreaming bool

	// SupportsDiarization indicates if speaker diarization is supported.
	SupportsDiarization bool

	// SupportsTimestamps indicates if word-level timestamps are supported.
	SupportsTimestamps bool

	// SupportedLanguages lists the supported language codes (empty means all).
	SupportedLanguages []string
}

// STTRegistry holds registered STT provider constructors.
var STTRegistry = map[STTProvider]func(config STTConfig) (STT, error){}

// defaultSTT is the package-level default STT provider for use by template helpers.
var defaultSTT STT

// GetDefaultSTT returns the default STT provider.
// Returns nil if no provider has been configured.
func GetDefaultSTT() STT {
	return defaultSTT
}

// SetDefaultSTT sets the package-level default STT provider.
// This is typically called at application startup from configuration.
func SetDefaultSTT(stt STT) {
	defaultSTT = stt
}

// RegisterSTT registers an STT provider constructor.
func RegisterSTT(typ STTProvider, constructor func(config STTConfig) (STT, error)) {
	if _, exists := STTRegistry[typ]; exists {
		panic(fmt.Sprintf("STT provider %s already registered", typ))
	}
	STTRegistry[typ] = constructor
}

// DeregisterSTT removes an STT provider from the registry.
func DeregisterSTT(typ STTProvider) {
	delete(STTRegistry, typ)
}

// NewSTT creates a new STT provider from configuration.
func NewSTT(conf STTConfig) (STT, error) {
	if conf.Provider == "" {
		return nil, fmt.Errorf("provider not specified")
	}
	constructor, ok := STTRegistry[conf.Provider]
	if !ok {
		return nil, fmt.Errorf("no STT provider registered for type %s", conf.Provider)
	}
	return constructor(conf)
}
