/*
Copyright 2025 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package audio

import (
	"context"
	"fmt"
	"net/http"

	libtermite "github.com/antflydb/antfly/lib/termite"
	"github.com/antflydb/termite/pkg/client"
)

func init() {
	RegisterSTT(STTProviderTermite, NewTermiteSTT)
}

// TermiteSTT implements the STT interface using Termite's transcriber API.
type TermiteSTT struct {
	client *client.TermiteClient
	model  string
}

// NewTermiteSTT creates a new Termite STT provider.
func NewTermiteSTT(config STTConfig) (STT, error) {
	c, err := config.AsTermiteSTTConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Resolve API URL
	apiURL := libtermite.ResolveURL(c.ApiUrl)
	if apiURL == "" {
		return nil, fmt.Errorf("termite api_url not configured (set api_url or ANTFLY_TERMITE_URL)")
	}

	termiteClient, err := client.NewTermiteClient(apiURL, http.DefaultClient)
	if err != nil {
		return nil, fmt.Errorf("creating termite client: %w", err)
	}

	return &TermiteSTT{
		client: termiteClient,
		model:  c.Model,
	}, nil
}

// Capabilities returns what this STT provider supports.
func (s *TermiteSTT) Capabilities() STTCapabilities {
	return STTCapabilities{
		SupportedFormats: []AudioFormat{
			AudioFormatWav,
			AudioFormatMp3,
			AudioFormatFlac,
			AudioFormatAac,
		},
		MaxDurationSeconds:  3600,              // 1 hour (depends on model/memory)
		MaxFileSizeBytes:    100 * 1024 * 1024, // 100 MB
		SupportsStreaming:   false,
		SupportsDiarization: false,
		SupportsTimestamps:  false,      // Termite transcribe API doesn't return timestamps yet
		SupportedLanguages:  []string{}, // Whisper supports 50+ languages
	}
}

// Transcribe converts audio to text.
func (s *TermiteSTT) Transcribe(ctx context.Context, req TranscribeRequest) (*TranscribeResponse, error) {
	// Resolve audio data
	audioData, _, err := s.resolveAudio(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("resolving audio: %w", err)
	}

	// Call Termite transcribe API
	resp, err := s.client.Transcribe(ctx, s.model, audioData, req.Language)
	if err != nil {
		return nil, fmt.Errorf("transcribing: %w", err)
	}

	return &TranscribeResponse{
		Text:     resp.Text,
		Language: resp.Language,
	}, nil
}

// TranscribeStream is not supported by Termite's transcribe API.
func (s *TermiteSTT) TranscribeStream(ctx context.Context, audioStream <-chan []byte, opts StreamOptions) (<-chan TranscriptChunk, error) {
	return nil, fmt.Errorf("streaming transcription not supported by Termite")
}

// resolveAudio gets audio bytes from the request's input source.
func (s *TermiteSTT) resolveAudio(ctx context.Context, req TranscribeRequest) ([]byte, AudioFormat, error) {
	// Check which input source is provided
	if len(req.Audio) > 0 {
		format := req.Format
		if format == "" {
			format = AudioFormatWav // Default for Termite
		}
		return req.Audio, format, nil
	}

	if req.URL != "" {
		return DownloadAudio(ctx, req.URL, req.S3Credentials)
	}

	return nil, "", fmt.Errorf("no audio input provided")
}
