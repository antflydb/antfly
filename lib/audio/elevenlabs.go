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

// ElevenLabs TTS implementation.
package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

const elevenlabsDefaultBaseURL = "https://api.elevenlabs.io"

func init() {
	RegisterTTS(TTSProviderElevenlabs, NewElevenLabsTTS)
}

// ElevenLabsTTS implements the TTS interface using ElevenLabs API.
type ElevenLabsTTS struct {
	client          *http.Client
	apiKey          string
	baseURL         string
	defaultVoiceID  string
	modelID         string
	stability       float32
	similarityBoost float32
	style           float32
}

// NewElevenLabsTTS creates a new ElevenLabs TTS provider.
func NewElevenLabsTTS(config TTSConfig) (TTS, error) {
	c, err := config.AsElevenLabsTTSConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	apiKey := c.ApiKey
	if apiKey == "" {
		apiKey = os.Getenv("ELEVENLABS_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("ElevenLabs API key is required")
	}

	modelID := string(c.ModelId)
	if modelID == "" {
		modelID = "eleven_turbo_v2_5"
	}

	stability := c.Stability
	if stability == 0 {
		stability = 0.5
	}

	similarityBoost := c.SimilarityBoost
	if similarityBoost == 0 {
		similarityBoost = 0.75
	}

	return &ElevenLabsTTS{
		client:          &http.Client{},
		apiKey:          apiKey,
		baseURL:         elevenlabsDefaultBaseURL,
		defaultVoiceID:  c.VoiceId,
		modelID:         modelID,
		stability:       stability,
		similarityBoost: similarityBoost,
		style:           c.Style,
	}, nil
}

// Capabilities returns what this TTS provider supports.
func (t *ElevenLabsTTS) Capabilities() TTSCapabilities {
	return TTSCapabilities{
		SupportedFormats: []AudioFormat{
			AudioFormatMp3,
			AudioFormatPcm,
			AudioFormatOpus,
		},
		MaxTextLength:     5000,
		SupportsSSML:      true,
		SupportsStreaming: true,
		SupportsPitch:     false,
		MinSpeed:          0.7,
		MaxSpeed:          1.2,
	}
}

// elevenlabsSynthesisRequest is the request body for ElevenLabs TTS API.
type elevenlabsSynthesisRequest struct {
	Text          string                   `json:"text"`
	ModelID       string                   `json:"model_id"`
	VoiceSettings *elevenlabsVoiceSettings `json:"voice_settings,omitempty"`
}

type elevenlabsVoiceSettings struct {
	Stability       float64 `json:"stability"`
	SimilarityBoost float64 `json:"similarity_boost"`
	Style           float64 `json:"style,omitempty"`
	UseSpeakerBoost bool    `json:"use_speaker_boost,omitempty"`
}

// Synthesize generates audio from text.
func (t *ElevenLabsTTS) Synthesize(ctx context.Context, req SynthesizeRequest) (*SynthesizeResponse, error) {
	voiceID := req.Voice
	if voiceID == "" {
		voiceID = t.defaultVoiceID
	}
	if voiceID == "" {
		return nil, fmt.Errorf("voice ID is required")
	}

	format := req.Format
	if format == "" {
		format = AudioFormatMp3
	}

	// Build request body
	body := elevenlabsSynthesisRequest{
		Text:    req.Text,
		ModelID: t.modelID,
		VoiceSettings: &elevenlabsVoiceSettings{
			Stability:       float64(t.stability),
			SimilarityBoost: float64(t.similarityBoost),
			Style:           float64(t.style),
		},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	// Build URL with output format
	url := fmt.Sprintf("%s/v1/text-to-speech/%s", t.baseURL, voiceID)
	if format != AudioFormatMp3 {
		url += fmt.Sprintf("?output_format=%s", elevenlabsMapFormat(format))
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	httpReq.Header.Set("xi-api-key", t.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", format.MIMEType())

	resp, err := t.client.Do(httpReq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("making request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ElevenLabs API error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	audioData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	response := &SynthesizeResponse{
		Format:         format,
		CharactersUsed: len(req.Text),
	}

	// Handle S3 upload if requested
	if req.S3Output != "" {
		if err := UploadAudioToS3(ctx, req.S3Output, req.S3Credentials, audioData, format); err != nil {
			return nil, fmt.Errorf("uploading to S3: %w", err)
		}
		response.S3URL = req.S3Output
	} else {
		response.Audio = audioData
	}

	return response, nil
}

// SynthesizeStream returns audio chunks using streaming synthesis.
func (t *ElevenLabsTTS) SynthesizeStream(ctx context.Context, req SynthesizeRequest) (<-chan AudioChunk, error) {
	voiceID := req.Voice
	if voiceID == "" {
		voiceID = t.defaultVoiceID
	}
	if voiceID == "" {
		return nil, fmt.Errorf("voice ID is required")
	}

	format := req.Format
	if format == "" {
		format = AudioFormatMp3
	}

	// Build request body
	body := elevenlabsSynthesisRequest{
		Text:    req.Text,
		ModelID: t.modelID,
		VoiceSettings: &elevenlabsVoiceSettings{
			Stability:       float64(t.stability),
			SimilarityBoost: float64(t.similarityBoost),
			Style:           float64(t.style),
		},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	// Use streaming endpoint
	url := fmt.Sprintf("%s/v1/text-to-speech/%s/stream", t.baseURL, voiceID)
	if format != AudioFormatMp3 {
		url += fmt.Sprintf("?output_format=%s", elevenlabsMapFormat(format))
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	httpReq.Header.Set("xi-api-key", t.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", format.MIMEType())

	resp, err := t.client.Do(httpReq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("making request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("ElevenLabs API error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	ch := make(chan AudioChunk, 100)

	go func() {
		defer close(ch)
		defer func() { _ = resp.Body.Close() }()

		buf := make([]byte, 4096)
		var s3Buffer bytes.Buffer

		for {
			select {
			case <-ctx.Done():
				ch <- AudioChunk{Error: ctx.Err()}
				return
			default:
			}

			n, err := resp.Body.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])

				// Buffer for S3 if needed
				if req.S3Output != "" {
					s3Buffer.Write(chunk)
				}

				ch <- AudioChunk{Data: chunk}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				ch <- AudioChunk{Error: fmt.Errorf("reading stream: %w", err)}
				return
			}
		}

		// Upload to S3 if requested
		if req.S3Output != "" {
			if err := UploadAudioToS3(ctx, req.S3Output, req.S3Credentials, s3Buffer.Bytes(), format); err != nil {
				ch <- AudioChunk{Error: fmt.Errorf("uploading to S3: %w", err)}
			}
		}
	}()

	return ch, nil
}

// elevenlabsVoicesResponse is the response from the ElevenLabs voices API.
type elevenlabsVoicesResponse struct {
	Voices []elevenlabsVoiceInfo `json:"voices"`
}

type elevenlabsVoiceInfo struct {
	VoiceID        string            `json:"voice_id"`
	Name           string            `json:"name"`
	Category       string            `json:"category"`
	Description    string            `json:"description"`
	PreviewURL     string            `json:"preview_url"`
	Labels         map[string]string `json:"labels"`
	FineTuning     map[string]any    `json:"fine_tuning"`
	HighQualityURL string            `json:"high_quality_base_model_ids"`
}

// ListVoices returns available voices from ElevenLabs.
func (t *ElevenLabsTTS) ListVoices(ctx context.Context) ([]Voice, error) {
	url := fmt.Sprintf("%s/v1/voices", t.baseURL)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	httpReq.Header.Set("xi-api-key", t.apiKey)

	resp, err := t.client.Do(httpReq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("making request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ElevenLabs API error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var voicesResp elevenlabsVoicesResponse
	if err := json.NewDecoder(resp.Body).Decode(&voicesResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	voices := make([]Voice, len(voicesResp.Voices))
	for i, v := range voicesResp.Voices {
		gender := VoiceGender("")
		if g, ok := v.Labels["gender"]; ok {
			gender = VoiceGender(g)
		}

		language := ""
		if l, ok := v.Labels["language"]; ok {
			language = l
		}

		voices[i] = Voice{
			Id:          v.VoiceID,
			Name:        v.Name,
			Language:    language,
			Gender:      gender,
			Description: v.Description,
			PreviewUrl:  v.PreviewURL,
		}
	}

	return voices, nil
}

// elevenlabsMapFormat converts AudioFormat to ElevenLabs output format string.
func elevenlabsMapFormat(f AudioFormat) string {
	switch f {
	case AudioFormatMp3:
		return "mp3_44100_128"
	case AudioFormatPcm:
		return "pcm_16000"
	case AudioFormatOpus:
		return "opus_64000"
	default:
		return "mp3_44100_128"
	}
}
