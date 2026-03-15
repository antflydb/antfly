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
	"io"
	"os"
	"strings"
	"time"

	speech "cloud.google.com/go/speech/apiv1"
	"cloud.google.com/go/speech/apiv1/speechpb"
	"github.com/antflydb/antfly/lib/vertex"
	"google.golang.org/api/option"
)

func init() {
	RegisterSTT(STTProviderVertex, NewVertexSTT)
}

// VertexSTT implements the STT interface using Google Cloud Speech-to-Text.
type VertexSTT struct {
	projectID                  string
	location                   string
	languageCode               string
	enableAutomaticPunctuation bool
	useEnhanced                bool
	model                      string
	clientOpts                 []option.ClientOption
}

// NewVertexSTT creates a new Google Cloud STT provider.
func NewVertexSTT(config STTConfig) (STT, error) {
	c, err := config.AsVertexSTTConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	var clientOpts []option.ClientOption

	// Set credentials (file path → env var → ADC)
	creds, err := vertex.LoadCredentials(&c.CredentialsPath, []string{vertex.CloudPlatformScope})
	if err != nil {
		return nil, fmt.Errorf("resolving credentials: %w", err)
	}
	clientOpts = append(clientOpts, vertex.AuthClientOption(creds))

	// Resolve project ID
	projectID := c.ProjectId
	if projectID == "" {
		projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}

	// Default location
	location := c.Location
	if location == "" {
		location = os.Getenv("GOOGLE_CLOUD_LOCATION")
		if location == "" {
			location = "us-central1"
		}
	}

	// Default language code
	languageCode := c.LanguageCode
	if languageCode == "" {
		languageCode = "en-US"
	}

	return &VertexSTT{
		projectID:                  projectID,
		location:                   location,
		languageCode:               languageCode,
		enableAutomaticPunctuation: c.EnableAutomaticPunctuation,
		useEnhanced:                c.UseEnhanced,
		model:                      c.Model,
		clientOpts:                 clientOpts,
	}, nil
}

// Capabilities returns what this STT provider supports.
func (s *VertexSTT) Capabilities() STTCapabilities {
	return STTCapabilities{
		SupportedFormats: []AudioFormat{
			AudioFormatWav,
			AudioFormatFlac,
			AudioFormatMp3,
			AudioFormatOgg,
			AudioFormatWebm,
		},
		MaxDurationSeconds:  480 * 60, // 8 hours with async
		MaxFileSizeBytes:    480 * 1024 * 1024,
		SupportsStreaming:   true,
		SupportsDiarization: true,
		SupportsTimestamps:  true,
		SupportedLanguages:  []string{}, // Google supports 125+ languages
	}
}

// Transcribe converts audio to text.
func (s *VertexSTT) Transcribe(ctx context.Context, req TranscribeRequest) (*TranscribeResponse, error) {
	client, err := speech.NewClient(ctx, s.clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	defer func() { _ = client.Close() }()

	// Resolve audio data
	audioData, format, err := s.resolveAudio(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("resolving audio: %w", err)
	}

	// Build recognition config
	recognitionConfig := &speechpb.RecognitionConfig{
		Encoding:                   vertexMapEncoding(format),
		LanguageCode:               s.getLanguageCode(req.Language),
		EnableAutomaticPunctuation: s.enableAutomaticPunctuation,
		EnableWordTimeOffsets:      req.Timestamps,
	}

	// Set model if specified
	if s.model != "" {
		recognitionConfig.Model = s.model
	}

	// Enable enhanced model if configured
	if s.useEnhanced {
		recognitionConfig.UseEnhanced = true
	}

	// Configure diarization if requested
	if req.Diarization {
		minSpeakers := int32(2)
		maxSpeakers := int32(6)
		if req.MinSpeakers > 0 {
			minSpeakers = int32(req.MinSpeakers) //nolint:gosec // G115: bounded value, cannot overflow in practice
		}
		if req.MaxSpeakers > 0 {
			maxSpeakers = int32(req.MaxSpeakers) //nolint:gosec // G115: bounded value, cannot overflow in practice
		}

		recognitionConfig.DiarizationConfig = &speechpb.SpeakerDiarizationConfig{
			EnableSpeakerDiarization: true,
			MinSpeakerCount:          minSpeakers,
			MaxSpeakerCount:          maxSpeakers,
		}
	}

	// Use synchronous recognition for shorter audio
	resp, err := client.Recognize(ctx, &speechpb.RecognizeRequest{
		Config: recognitionConfig,
		Audio: &speechpb.RecognitionAudio{
			AudioSource: &speechpb.RecognitionAudio_Content{
				Content: audioData,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("recognizing: %w", err)
	}

	return s.buildResponse(resp.Results, req.Diarization), nil
}

// TranscribeStream processes audio stream in real-time.
func (s *VertexSTT) TranscribeStream(ctx context.Context, audioStream <-chan []byte, opts StreamOptions) (<-chan TranscriptChunk, error) {
	client, err := speech.NewClient(ctx, s.clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("creating stream: %w", err)
	}

	// Determine sample rate
	sampleRate := opts.SampleRate
	if sampleRate == 0 {
		sampleRate = 16000 // Default for speech
	}

	// Build streaming config
	streamingConfig := &speechpb.StreamingRecognitionConfig{
		Config: &speechpb.RecognitionConfig{
			Encoding:                   vertexMapEncoding(opts.Format),
			SampleRateHertz:            int32(sampleRate),
			LanguageCode:               s.getLanguageCode(opts.Language),
			EnableAutomaticPunctuation: s.enableAutomaticPunctuation,
			EnableWordTimeOffsets:      true,
		},
		InterimResults: opts.InterimResults,
	}

	// Configure diarization if requested
	if opts.Diarization {
		streamingConfig.Config.DiarizationConfig = &speechpb.SpeakerDiarizationConfig{
			EnableSpeakerDiarization: true,
			MinSpeakerCount:          2,
			MaxSpeakerCount:          6,
		}
	}

	// Send initial config
	if err := stream.Send(&speechpb.StreamingRecognizeRequest{
		StreamingRequest: &speechpb.StreamingRecognizeRequest_StreamingConfig{
			StreamingConfig: streamingConfig,
		},
	}); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("sending config: %w", err)
	}

	ch := make(chan TranscriptChunk, 100)

	// Goroutine to send audio chunks
	go func() {
		defer func() { _ = stream.CloseSend() }()

		for chunk := range audioStream {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := stream.Send(&speechpb.StreamingRecognizeRequest{
				StreamingRequest: &speechpb.StreamingRecognizeRequest_AudioContent{
					AudioContent: chunk,
				},
			}); err != nil {
				return
			}
		}
	}()

	// Goroutine to receive transcripts
	go func() {
		defer close(ch)
		defer func() { _ = client.Close() }()

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				select {
				case <-ctx.Done():
				case ch <- TranscriptChunk{Error: fmt.Errorf("receiving: %w", err)}:
				}
				return
			}

			for _, result := range resp.Results {
				if len(result.Alternatives) == 0 {
					continue
				}

				alt := result.Alternatives[0]
				chunk := TranscriptChunk{
					Text:       alt.Transcript,
					IsFinal:    result.IsFinal,
					Confidence: float64(alt.Confidence),
				}

				// Add timing if available
				if len(alt.Words) > 0 {
					chunk.Start = time.Duration(alt.Words[0].StartTime.AsDuration())
					chunk.End = time.Duration(alt.Words[len(alt.Words)-1].EndTime.AsDuration())

					// Add speaker if diarization is enabled
					if opts.Diarization && alt.Words[0].SpeakerTag > 0 { //nolint:staticcheck // SA1019: SpeakerTag deprecated in proto but no v1 replacement
						chunk.Speaker = fmt.Sprintf("speaker_%d", alt.Words[0].SpeakerTag) //nolint:staticcheck // SA1019
					}
				}

				select {
				case <-ctx.Done():
					return
				case ch <- chunk:
				}
			}
		}
	}()

	return ch, nil
}

// resolveAudio gets audio bytes from the request's input source.
func (s *VertexSTT) resolveAudio(ctx context.Context, req TranscribeRequest) ([]byte, AudioFormat, error) {
	if len(req.Audio) > 0 {
		format := req.Format
		if format == "" {
			format = AudioFormatWav
		}
		return req.Audio, format, nil
	}

	if req.URL != "" {
		return DownloadAudio(ctx, req.URL, req.S3Credentials)
	}

	return nil, "", fmt.Errorf("no audio input provided")
}

// getLanguageCode returns the language code to use.
func (s *VertexSTT) getLanguageCode(reqLanguage string) string {
	if reqLanguage != "" {
		return reqLanguage
	}
	return s.languageCode
}

// buildResponse converts Google's recognition results to our response format.
func (s *VertexSTT) buildResponse(results []*speechpb.SpeechRecognitionResult, diarization bool) *TranscribeResponse {
	var fullText strings.Builder
	var segments []TranscriptSegment
	speakerMap := make(map[int32]bool)

	for _, result := range results {
		if len(result.Alternatives) == 0 {
			continue
		}

		alt := result.Alternatives[0]
		fullText.WriteString(alt.Transcript + " ")

		segment := TranscriptSegment{
			Text: alt.Transcript,
		}

		// Process words for timestamps and diarization
		if len(alt.Words) > 0 {
			segment.StartMs = int(alt.Words[0].StartTime.AsDuration().Milliseconds())
			segment.EndMs = int(alt.Words[len(alt.Words)-1].EndTime.AsDuration().Milliseconds())

			segment.Words = make([]WordTimestamp, len(alt.Words))
			for i, w := range alt.Words {
				segment.Words[i] = WordTimestamp{
					Word:    w.Word,
					StartMs: int(w.StartTime.AsDuration().Milliseconds()),
					EndMs:   int(w.EndTime.AsDuration().Milliseconds()),
				}

				// Track speakers
				if diarization && w.SpeakerTag > 0 { //nolint:staticcheck // SA1019: SpeakerTag deprecated in proto but no v1 replacement
					speakerMap[w.SpeakerTag] = true //nolint:staticcheck // SA1019
					if segment.Speaker == "" {
						segment.Speaker = fmt.Sprintf("speaker_%d", w.SpeakerTag) //nolint:staticcheck // SA1019
					}
				}
			}
		}

		segments = append(segments, segment)
	}

	response := &TranscribeResponse{
		Text:     fullText.String(),
		Segments: segments,
	}

	// Build speaker list
	if diarization {
		for tag := range speakerMap {
			response.Speakers = append(response.Speakers, Speaker{
				Id:    fmt.Sprintf("speaker_%d", tag),
				Label: fmt.Sprintf("Speaker %d", tag),
			})
		}
	}

	// Calculate total duration from segments
	if len(segments) > 0 {
		response.Duration = time.Duration(segments[len(segments)-1].EndMs) * time.Millisecond
	}

	return response
}

// vertexMapEncoding converts AudioFormat to Google encoding.
func vertexMapEncoding(f AudioFormat) speechpb.RecognitionConfig_AudioEncoding {
	switch f {
	case AudioFormatWav, AudioFormatPcm:
		return speechpb.RecognitionConfig_LINEAR16
	case AudioFormatFlac:
		return speechpb.RecognitionConfig_FLAC
	case AudioFormatMp3:
		return speechpb.RecognitionConfig_MP3
	case AudioFormatOgg:
		return speechpb.RecognitionConfig_OGG_OPUS
	case AudioFormatWebm:
		return speechpb.RecognitionConfig_WEBM_OPUS
	default:
		return speechpb.RecognitionConfig_ENCODING_UNSPECIFIED
	}
}
