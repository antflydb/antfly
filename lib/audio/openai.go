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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/packages/param"
)

func init() {
	RegisterSTT(STTProviderOpenai, NewOpenAISTT)
}

// OpenAISTT implements the STT interface using OpenAI's Whisper API.
type OpenAISTT struct {
	client *openai.Client
	model  string
}

// NewOpenAISTT creates a new OpenAI STT provider (Whisper).
func NewOpenAISTT(config STTConfig) (STT, error) {
	c, err := config.AsOpenAISTTConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	opts := []option.RequestOption{}

	// Set base URL if provided
	var baseURL string
	if c.BaseUrl != "" {
		baseURL = c.BaseUrl
	} else if envURL := os.Getenv("OPENAI_BASE_URL"); envURL != "" {
		baseURL = envURL
	}
	if baseURL != "" {
		opts = append(opts, option.WithBaseURL(baseURL))
	}

	// Set API key
	var apiKey string
	if c.ApiKey != "" {
		apiKey = c.ApiKey
	} else {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if apiKey != "" {
		opts = append(opts, option.WithAPIKey(apiKey))
	}

	model := c.Model
	if model == "" {
		model = "whisper-1"
	}

	client := openai.NewClient(opts...)
	return &OpenAISTT{
		client: &client,
		model:  model,
	}, nil
}

// Capabilities returns what this STT provider supports.
func (s *OpenAISTT) Capabilities() STTCapabilities {
	return STTCapabilities{
		SupportedFormats: []AudioFormat{
			AudioFormatMp3,
			AudioFormatWav,
			AudioFormatWebm,
			AudioFormatOgg,
			AudioFormatFlac,
		},
		MaxDurationSeconds:  7200,             // 2 hours
		MaxFileSizeBytes:    25 * 1024 * 1024, // 25 MB
		SupportsStreaming:   false,
		SupportsDiarization: false,
		SupportsTimestamps:  true,
		SupportedLanguages:  []string{}, // Whisper supports 50+ languages
	}
}

// openaiVerboseTranscription represents the verbose JSON response from OpenAI.
type openaiVerboseTranscription struct {
	Text     string  `json:"text"`
	Language string  `json:"language"`
	Duration float64 `json:"duration"`
	Segments []struct {
		Text  string  `json:"text"`
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"segments"`
	Words []struct {
		Word  string  `json:"word"`
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"words"`
}

// Transcribe converts audio to text.
func (s *OpenAISTT) Transcribe(ctx context.Context, req TranscribeRequest) (*TranscribeResponse, error) {
	// Resolve audio data
	audioData, _, err := s.resolveAudio(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("resolving audio: %w", err)
	}

	params := openai.AudioTranscriptionNewParams{
		File:  bytes.NewReader(audioData),
		Model: openai.AudioModel(s.model),
	}

	// Set language if specified
	if req.Language != "" {
		params.Language = param.NewOpt(req.Language)
	}

	// Set prompt if specified
	if req.Prompt != "" {
		params.Prompt = param.NewOpt(req.Prompt)
	}

	// Request verbose JSON for timestamps
	if req.Timestamps {
		params.ResponseFormat = openai.AudioResponseFormatVerboseJSON
		params.TimestampGranularities = []string{"word", "segment"}
	}

	resp, err := s.client.Audio.Transcriptions.New(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("transcribing: %w", err)
	}

	response := &TranscribeResponse{
		Text: resp.Text,
	}

	// If timestamps were requested, parse the verbose JSON response
	if req.Timestamps {
		var verbose openaiVerboseTranscription
		if err := json.Unmarshal([]byte(resp.RawJSON()), &verbose); err == nil {
			response.Language = verbose.Language
			response.Duration = time.Duration(verbose.Duration * float64(time.Second))

			// Convert segments if available
			if len(verbose.Segments) > 0 {
				response.Segments = make([]TranscriptSegment, len(verbose.Segments))
				for i, seg := range verbose.Segments {
					response.Segments[i] = TranscriptSegment{
						Text:    seg.Text,
						StartMs: int(seg.Start * 1000),
						EndMs:   int(seg.End * 1000),
					}
				}
			}

			// Convert word timestamps if available
			if len(verbose.Words) > 0 && len(response.Segments) > 0 {
				words := make([]WordTimestamp, len(verbose.Words))
				for i, w := range verbose.Words {
					words[i] = WordTimestamp{
						Word:    w.Word,
						StartMs: int(w.Start * 1000),
						EndMs:   int(w.End * 1000),
					}
				}
				// Assign to first segment for simplicity
				response.Segments[0].Words = words
			}
		}
	}

	return response, nil
}

// TranscribeStream is not supported by OpenAI Whisper API.
func (s *OpenAISTT) TranscribeStream(ctx context.Context, audioStream <-chan []byte, opts StreamOptions) (<-chan TranscriptChunk, error) {
	return nil, fmt.Errorf("streaming transcription not supported by OpenAI Whisper API")
}

// resolveAudio gets audio bytes from the request's input source.
func (s *OpenAISTT) resolveAudio(ctx context.Context, req TranscribeRequest) ([]byte, AudioFormat, error) {
	// Check which input source is provided
	if len(req.Audio) > 0 {
		format := req.Format
		if format == "" {
			format = AudioFormatMp3 // Default
		}
		return req.Audio, format, nil
	}

	if req.URL != "" {
		return DownloadAudio(ctx, req.URL, req.S3Credentials)
	}

	return nil, "", fmt.Errorf("no audio input provided")
}
