# Add Audio TTS/STT Support

**Date**: 2025-12-12
**Goal**: Add text-to-speech (TTS) and speech-to-text (STT) capabilities with support for OpenAI, Google Vertex, and ElevenLabs providers

## Summary

Add audio processing capabilities to Antfly following the existing registry-based plugin pattern used for embeddings, reranking, and chunking. Support streaming output, inline audio data, and remote files via S3.

## Usage

```go
import (
    "github.com/antflydb/antfly/lib/audio"
    _ "github.com/antflydb/antfly/lib/audio/openai"     // Register OpenAI provider
    _ "github.com/antflydb/antfly/lib/audio/google"     // Register Google provider
    _ "github.com/antflydb/antfly/lib/audio/elevenlabs" // Register ElevenLabs provider
)

// Text-to-Speech
ttsProvider, _ := audio.NewTTS(audio.TTSConfig{
    Provider: audio.TTSProviderOpenAI,
    OpenAI: &audio.OpenAITTSConfig{
        Model: "tts-1-hd",
        Voice: "nova",
    },
})

// Inline response
resp, _ := ttsProvider.Synthesize(ctx, audio.SynthesizeRequest{
    Text:   "Hello, welcome to Antfly!",
    Format: audio.FormatMP3,
    Inline: true,
})
// resp.Audio contains MP3 bytes

// Stream to S3
resp, _ := ttsProvider.Synthesize(ctx, audio.SynthesizeRequest{
    Text:   longText,
    Format: audio.FormatOpus,
    S3Output: &audio.S3Info{
        Bucket: "my-bucket",
        Key:    "audio/output.opus",
    },
})
// resp.S3Info contains the uploaded location

// Streaming chunks (for real-time playback)
chunks, _ := ttsProvider.SynthesizeStream(ctx, req)
for chunk := range chunks {
    websocket.Write(chunk.Data)
}
```

```go
// Speech-to-Text
sttProvider, _ := audio.NewSTT(audio.STTConfig{
    Provider: audio.STTProviderOpenAI,
    OpenAI: &audio.OpenAISTTConfig{
        Model: "whisper-1",
    },
})

// From inline bytes
resp, _ := sttProvider.Transcribe(ctx, audio.TranscribeRequest{
    Audio:      audioBytes,
    Format:     audio.FormatWAV,
    Timestamps: true,
})

// From S3
resp, _ := sttProvider.Transcribe(ctx, audio.TranscribeRequest{
    S3Input: &audio.S3Info{
        Bucket: "my-bucket",
        Key:    "recordings/meeting.mp3",
    },
    Diarization: true,
})

// Real-time streaming (Google only)
opts := audio.StreamOptions{Format: audio.FormatPCM, SampleRate: 16000}
transcripts, _ := sttProvider.TranscribeStream(ctx, audioStream, opts)
for chunk := range transcripts {
    fmt.Printf("[%s] %s\n", chunk.Speaker, chunk.Text)
}
```

## Directory Structure

```
lib/audio/
├── audio.go                 # Shared types (AudioFormat, S3Info, Voice, etc.)
├── tts.go                   # TTS interface and registry
├── stt.go                   # STT interface and registry
├── s3.go                    # S3 upload/download helpers
├── openapi.yaml             # Config schema for all providers
├── openai/
│   ├── tts.go               # OpenAI TTS (tts-1, tts-1-hd)
│   └── stt.go               # OpenAI Whisper STT
├── google/
│   ├── tts.go               # Google Cloud TTS
│   └── stt.go               # Google Cloud STT (with streaming)
└── elevenlabs/
    └── tts.go               # ElevenLabs TTS (no STT available)
```

## Implementation Steps

### Phase 1: Core Types and Interfaces

1. **Create shared types** in `lib/audio/common.go`:
   ```go
   package audio

   type AudioFormat string

   const (
       FormatMP3  AudioFormat = "mp3"
       FormatWAV  AudioFormat = "wav"
       FormatOGG  AudioFormat = "ogg"
       FormatOpus AudioFormat = "opus"
       FormatFLAC AudioFormat = "flac"
       FormatPCM  AudioFormat = "pcm"
       FormatAAC  AudioFormat = "aac"
   )

   func (f AudioFormat) MIMEType() string {
       switch f {
       case FormatMP3:  return "audio/mpeg"
       case FormatWAV:  return "audio/wav"
       case FormatOGG:  return "audio/ogg"
       case FormatOpus: return "audio/opus"
       case FormatFLAC: return "audio/flac"
       case FormatPCM:  return "audio/pcm"
       case FormatAAC:  return "audio/aac"
       default:         return "application/octet-stream"
       }
   }

   type S3Info struct {
       Bucket      string
       Key         string
       Region      string
       Endpoint    string        // For MinIO/S3-compatible
       Credentials *S3Credentials
   }

   type S3Credentials struct {
       AccessKeyID     string
       SecretAccessKey string
       SessionToken    string
   }
   ```

2. **Create TTS interface** in `lib/audio/tts/plugin.go`:
   ```go
   package tts

   type TTSProvider interface {
       Synthesize(ctx context.Context, req SynthesizeRequest) (*SynthesizeResponse, error)
       SynthesizeStream(ctx context.Context, req SynthesizeRequest) (<-chan AudioChunk, error)
       ListVoices(ctx context.Context) ([]Voice, error)
       Capabilities() TTSCapabilities
   }

   type SynthesizeRequest struct {
       Text     string
       Voice    string
       Format   audio.AudioFormat
       Speed    float64  // 0.25-4.0, default 1.0

       // Output destination (mutually exclusive)
       Inline   bool
       S3Output *audio.S3Info
   }

   type SynthesizeResponse struct {
       Audio    []byte            // If Inline=true
       Format   audio.AudioFormat
       S3Info   *audio.S3Info     // If S3Output was set
       Duration time.Duration
   }

   type AudioChunk struct {
       Data   []byte
       Offset time.Duration
       Error  error
   }

   type Voice struct {
       ID          string
       Name        string
       Language    string
       Gender      string
       PreviewURL  string
   }

   type TTSCapabilities struct {
       SupportedFormats  []audio.AudioFormat
       MaxTextLength     int
       SupportsSSML      bool
       SupportsStreaming bool
   }
   ```

3. **Create STT interface** in `lib/audio/stt/plugin.go`:
   ```go
   package stt

   type STTProvider interface {
       Transcribe(ctx context.Context, req TranscribeRequest) (*TranscribeResponse, error)
       TranscribeStream(ctx context.Context, audioStream <-chan []byte) (<-chan TranscriptChunk, error)
       Capabilities() STTCapabilities
   }

   type TranscribeRequest struct {
       // Input source (one required)
       Audio   []byte
       S3Input *audio.S3Info
       URL     string

       // Options
       Format      audio.AudioFormat
       Language    string  // ISO 639-1 code or empty for auto-detect
       Timestamps  bool
       Diarization bool
   }

   type TranscribeResponse struct {
       Text     string
       Language string
       Duration time.Duration
       Segments []TranscriptSegment
       Speakers []Speaker
   }

   type TranscriptSegment struct {
       Text    string
       Start   time.Duration
       End     time.Duration
       Speaker string
       Words   []WordTimestamp
   }

   type WordTimestamp struct {
       Word  string
       Start time.Duration
       End   time.Duration
   }

   type Speaker struct {
       ID    string
       Label string
   }

   type TranscriptChunk struct {
       Text    string
       IsFinal bool
       Error   error
   }

   type STTCapabilities struct {
       SupportedFormats   []audio.AudioFormat
       MaxDurationSeconds int
       SupportsStreaming  bool
       SupportsDiarization bool
       SupportedLanguages []string
   }
   ```

4. **Create registry pattern** (following `lib/embeddings/plugin.go`):
   ```go
   var (
       ttsRegistry = make(map[TTSProviderType]func(TTSConfig) (TTSProvider, error))
       sttRegistry = make(map[STTProviderType]func(STTConfig) (STTProvider, error))
   )

   func RegisterTTSProvider(t TTSProviderType, constructor func(TTSConfig) (TTSProvider, error)) {
       ttsRegistry[t] = constructor
   }

   func NewTTSProvider(config TTSConfig) (TTSProvider, error) {
       constructor, ok := ttsRegistry[config.Type]
       if !ok {
           return nil, fmt.Errorf("unknown TTS provider: %s", config.Type)
       }
       return constructor(config)
   }
   ```

### Phase 2: Provider Implementations

5. **OpenAI TTS** (`lib/audio/tts/openai.go`):
   ```go
   func init() {
       RegisterTTSProvider(OpenAI, NewOpenAITTS)
   }

   type OpenAITTS struct {
       client *openai.Client
       model  string
       voice  string
   }

   func NewOpenAITTS(config TTSConfig) (TTSProvider, error) {
       c, err := config.AsOpenAIConfig()
       if err != nil {
           return nil, err
       }
       apiKey := getConfigOrEnv(c.ApiKey, "OPENAI_API_KEY")
       return &OpenAITTS{
           client: openai.NewClient(apiKey),
           model:  c.Model,  // tts-1 or tts-1-hd
           voice:  c.Voice,  // alloy, echo, fable, onyx, nova, shimmer
       }, nil
   }

   func (o *OpenAITTS) Synthesize(ctx context.Context, req SynthesizeRequest) (*SynthesizeResponse, error) {
       resp, err := o.client.CreateSpeech(ctx, openai.CreateSpeechRequest{
           Model:          o.model,
           Input:          req.Text,
           Voice:          openai.SpeechVoice(req.Voice),
           ResponseFormat: mapFormat(req.Format),
           Speed:          req.Speed,
       })
       // Handle inline vs S3 output...
   }

   func (o *OpenAITTS) SynthesizeStream(ctx context.Context, req SynthesizeRequest) (<-chan AudioChunk, error) {
       // OpenAI supports streaming via response body
       ch := make(chan AudioChunk, 100)
       go func() {
           defer close(ch)
           resp, err := o.client.CreateSpeechStream(ctx, ...)
           reader := resp.Body
           buf := make([]byte, 4096)
           for {
               n, err := reader.Read(buf)
               if n > 0 {
                   ch <- AudioChunk{Data: buf[:n]}
               }
               if err == io.EOF {
                   break
               }
           }
       }()
       return ch, nil
   }
   ```

6. **OpenAI STT (Whisper)** (`lib/audio/stt/openai.go`):
   ```go
   func (o *OpenAISTT) Transcribe(ctx context.Context, req TranscribeRequest) (*TranscribeResponse, error) {
       audioData, err := o.resolveAudio(ctx, req) // Handle inline/S3/URL

       resp, err := o.client.CreateTranscription(ctx, openai.AudioRequest{
           Model:    "whisper-1",
           Reader:   bytes.NewReader(audioData),
           FilePath: "audio." + string(req.Format),
           Language: req.Language,
           Format:   openai.AudioResponseFormatVerboseJSON, // For timestamps
       })

       return &TranscribeResponse{
           Text:     resp.Text,
           Language: resp.Language,
           Duration: time.Duration(resp.Duration * float64(time.Second)),
           Segments: mapSegments(resp.Segments),
       }, nil
   }
   ```

7. **ElevenLabs TTS** (`lib/audio/tts/elevenlabs.go`):
   ```go
   type ElevenLabsTTS struct {
       client    *http.Client
       apiKey    string
       voiceID   string
       modelID   string
       baseURL   string
   }

   func (e *ElevenLabsTTS) Synthesize(ctx context.Context, req SynthesizeRequest) (*SynthesizeResponse, error) {
       body := map[string]any{
           "text":     req.Text,
           "model_id": e.modelID,
           "voice_settings": map[string]any{
               "stability":        0.5,
               "similarity_boost": 0.75,
           },
       }

       httpReq, _ := http.NewRequestWithContext(ctx, "POST",
           fmt.Sprintf("%s/v1/text-to-speech/%s", e.baseURL, e.voiceID),
           jsonBody(body))
       httpReq.Header.Set("xi-api-key", e.apiKey)
       httpReq.Header.Set("Accept", req.Format.MIMEType())

       // ...
   }

   func (e *ElevenLabsTTS) SynthesizeStream(ctx context.Context, req SynthesizeRequest) (<-chan AudioChunk, error) {
       // ElevenLabs supports websocket streaming
       // POST /v1/text-to-speech/{voice_id}/stream
   }
   ```

8. **Google Cloud TTS** (`lib/audio/tts/google.go`):
   ```go
   func (g *GoogleTTS) Synthesize(ctx context.Context, req SynthesizeRequest) (*SynthesizeResponse, error) {
       client, _ := texttospeech.NewClient(ctx, g.clientOpts...)

       input := &texttospeechpb.SynthesisInput{}
       if g.supportsSSML && strings.HasPrefix(req.Text, "<speak>") {
           input.InputSource = &texttospeechpb.SynthesisInput_Ssml{Ssml: req.Text}
       } else {
           input.InputSource = &texttospeechpb.SynthesisInput_Text{Text: req.Text}
       }

       resp, _ := client.SynthesizeSpeech(ctx, &texttospeechpb.SynthesizeSpeechRequest{
           Input: input,
           Voice: &texttospeechpb.VoiceSelectionParams{
               LanguageCode: g.languageCode,
               Name:         req.Voice,
           },
           AudioConfig: &texttospeechpb.AudioConfig{
               AudioEncoding: mapEncoding(req.Format),
               SpeakingRate:  req.Speed,
           },
       })

       return &SynthesizeResponse{Audio: resp.AudioContent, ...}, nil
   }
   ```

9. **Google Cloud STT** (`lib/audio/stt/google.go`):
   ```go
   func (g *GoogleSTT) TranscribeStream(ctx context.Context, audioStream <-chan []byte) (<-chan TranscriptChunk, error) {
       client, _ := speech.NewClient(ctx, g.clientOpts...)
       stream, _ := client.StreamingRecognize(ctx)

       // Send config first
       stream.Send(&speechpb.StreamingRecognizeRequest{
           StreamingRequest: &speechpb.StreamingRecognizeRequest_StreamingConfig{
               StreamingConfig: &speechpb.StreamingRecognitionConfig{
                   Config: &speechpb.RecognitionConfig{
                       Encoding:                   speechpb.RecognitionConfig_LINEAR16,
                       SampleRateHertz:            16000,
                       LanguageCode:               g.languageCode,
                       EnableAutomaticPunctuation: true,
                       EnableWordTimeOffsets:      true,
                       DiarizationConfig: &speechpb.SpeakerDiarizationConfig{
                           EnableSpeakerDiarization: true,
                           MinSpeakerCount:          2,
                           MaxSpeakerCount:          6,
                       },
                   },
                   InterimResults: true,
               },
           },
       })

       // Goroutine to send audio chunks
       go func() {
           for chunk := range audioStream {
               stream.Send(&speechpb.StreamingRecognizeRequest{
                   StreamingRequest: &speechpb.StreamingRecognizeRequest_AudioContent{
                       AudioContent: chunk,
                   },
               })
           }
           stream.CloseSend()
       }()

       // Return channel that receives transcripts
       ch := make(chan TranscriptChunk, 100)
       go func() {
           defer close(ch)
           for {
               resp, err := stream.Recv()
               if err == io.EOF {
                   break
               }
               for _, result := range resp.Results {
                   ch <- TranscriptChunk{
                       Text:    result.Alternatives[0].Transcript,
                       IsFinal: result.IsFinal,
                   }
               }
           }
       }()
       return ch, nil
   }
   ```

### Phase 3: S3 Integration

10. **S3 helpers** in `lib/audio/s3.go`:
    ```go
    func DownloadAudio(ctx context.Context, s3info *S3Info) ([]byte, AudioFormat, error) {
        cfg, _ := config.LoadDefaultConfig(ctx,
            config.WithRegion(s3info.Region),
            config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
                s3info.Credentials.AccessKeyID,
                s3info.Credentials.SecretAccessKey,
                s3info.Credentials.SessionToken,
            )),
        )

        if s3info.Endpoint != "" {
            cfg.BaseEndpoint = aws.String(s3info.Endpoint)
        }

        client := s3.NewFromConfig(cfg)
        resp, _ := client.GetObject(ctx, &s3.GetObjectInput{
            Bucket: aws.String(s3info.Bucket),
            Key:    aws.String(s3info.Key),
        })

        data, _ := io.ReadAll(resp.Body)
        format := detectFormat(s3info.Key, resp.ContentType)
        return data, format, nil
    }

    func UploadAudio(ctx context.Context, s3info *S3Info, data []byte, format AudioFormat) error {
        // Similar setup...
        _, err := client.PutObject(ctx, &s3.PutObjectInput{
            Bucket:      aws.String(s3info.Bucket),
            Key:         aws.String(s3info.Key),
            Body:        bytes.NewReader(data),
            ContentType: aws.String(format.MIMEType()),
        })
        return err
    }
    ```

### Phase 4: OpenAPI Configuration

11. **TTS OpenAPI schema** (`lib/audio/tts/openapi.yaml`):
    ```yaml
    openapi: 3.0.3
    info:
      title: Antfly TTS Configuration
      version: 1.0.0

    components:
      schemas:
        TTSProviderType:
          type: string
          enum: [openai, google, elevenlabs, termite]

        TTSConfig:
          type: object
          required: [type]
          properties:
            type:
              $ref: '#/components/schemas/TTSProviderType'
            openai:
              $ref: '#/components/schemas/OpenAITTSConfig'
            google:
              $ref: '#/components/schemas/GoogleTTSConfig'
            elevenlabs:
              $ref: '#/components/schemas/ElevenLabsTTSConfig'

        OpenAITTSConfig:
          type: object
          properties:
            model:
              type: string
              enum: [tts-1, tts-1-hd]
              default: tts-1
              description: tts-1 is faster, tts-1-hd has higher quality
            api_key:
              type: string
              description: Falls back to OPENAI_API_KEY env var
            voice:
              type: string
              enum: [alloy, echo, fable, onyx, nova, shimmer]
              default: alloy
            base_url:
              type: string
              description: For OpenAI-compatible APIs

        GoogleTTSConfig:
          type: object
          properties:
            project_id:
              type: string
              description: Falls back to GOOGLE_CLOUD_PROJECT env var
            location:
              type: string
              default: us-central1
            credentials_path:
              type: string
              description: Path to service account JSON
            language_code:
              type: string
              default: en-US
            voice_name:
              type: string
              description: e.g., en-US-Neural2-A

        ElevenLabsTTSConfig:
          type: object
          required: [voice_id]
          properties:
            api_key:
              type: string
              description: Falls back to ELEVENLABS_API_KEY env var
            voice_id:
              type: string
              description: ElevenLabs voice ID
            model_id:
              type: string
              default: eleven_turbo_v2_5
              enum: [eleven_monolingual_v1, eleven_multilingual_v2, eleven_turbo_v2_5]
            stability:
              type: number
              minimum: 0
              maximum: 1
              default: 0.5
            similarity_boost:
              type: number
              minimum: 0
              maximum: 1
              default: 0.75
    ```

12. **STT OpenAPI schema** (`lib/audio/stt/openapi.yaml`):
    ```yaml
    openapi: 3.0.3
    info:
      title: Antfly STT Configuration
      version: 1.0.0

    components:
      schemas:
        STTProviderType:
          type: string
          enum: [openai, google, termite]

        STTConfig:
          type: object
          required: [type]
          properties:
            type:
              $ref: '#/components/schemas/STTProviderType'
            openai:
              $ref: '#/components/schemas/OpenAISTTConfig'
            google:
              $ref: '#/components/schemas/GoogleSTTConfig'

        OpenAISTTConfig:
          type: object
          properties:
            model:
              type: string
              default: whisper-1
            api_key:
              type: string
              description: Falls back to OPENAI_API_KEY env var
            base_url:
              type: string

        GoogleSTTConfig:
          type: object
          properties:
            project_id:
              type: string
            location:
              type: string
              default: us-central1
            credentials_path:
              type: string
            language_code:
              type: string
              default: en-US
            enable_diarization:
              type: boolean
              default: false
            min_speaker_count:
              type: integer
              default: 2
            max_speaker_count:
              type: integer
              default: 6
    ```

### Phase 5: Integration Points

13. **Template helpers** (extend `lib/template/audiohelpers.go`):
    ```go
    // {{tts text="Hello world" voice="nova" format="mp3"}}
    func ttsHelper(text string, options map[string]any) (string, error) {
        provider := getDefaultTTSProvider()
        resp, _ := provider.Synthesize(ctx, tts.SynthesizeRequest{
            Text:   text,
            Voice:  options["voice"].(string),
            Format: audio.AudioFormat(options["format"].(string)),
            Inline: true,
        })
        // Return as data URI
        return fmt.Sprintf("data:%s;base64,%s",
            resp.Format.MIMEType(),
            base64.StdEncoding.EncodeToString(resp.Audio)), nil
    }

    // {{stt audio=audioDataURI}} or {{stt s3="bucket/key"}}
    func sttHelper(options map[string]any) (string, error) {
        provider := getDefaultSTTProvider()
        req := stt.TranscribeRequest{}

        if audioURI, ok := options["audio"].(string); ok {
            req.Audio = parseDataURI(audioURI)
        } else if s3Path, ok := options["s3"].(string); ok {
            req.S3Input = parseS3Path(s3Path)
        }

        resp, _ := provider.Transcribe(ctx, req)
        return resp.Text, nil
    }
    ```

14. **REST API endpoints** (add to `src/metadata/api.yaml`):
    ```yaml
    /v1/audio/tts:
      post:
        operationId: synthesizeSpeech
        requestBody:
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TTSRequest'
        responses:
          200:
            description: Audio response
            content:
              audio/mpeg: {}
              audio/wav: {}
              application/json:
                schema:
                  $ref: '#/components/schemas/TTSResponse'

    /v1/audio/stt:
      post:
        operationId: transcribeSpeech
        requestBody:
          content:
            multipart/form-data:
              schema:
                type: object
                properties:
                  file:
                    type: string
                    format: binary
                  language:
                    type: string
                  timestamps:
                    type: boolean
            application/json:
              schema:
                $ref: '#/components/schemas/STTRequest'
        responses:
          200:
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/STTResponse'
    ```

## Provider Support Matrix

| Feature | OpenAI | Google Cloud | ElevenLabs |
|---------|--------|--------------|------------|
| **TTS** | tts-1, tts-1-hd | Neural2, WaveNet, Standard | 30+ premium voices |
| **STT** | whisper-1 | Speech-to-Text v2 | N/A |
| **TTS Streaming** | Yes | Yes | Yes (WebSocket) |
| **STT Streaming** | No | Yes | N/A |
| **SSML Support** | No | Yes | Yes |
| **Diarization** | No | Yes | N/A |
| **Word Timestamps** | Yes (verbose) | Yes | N/A |
| **Max TTS Length** | 4096 chars | 5000 chars | 5000 chars |
| **Max STT Duration** | 25 MB file | 480 min | N/A |

## Critical Files to Create/Modify

### New Files
- `lib/audio/common.go` - Shared types
- `lib/audio/s3.go` - S3 upload/download helpers
- `lib/audio/tts/openapi.yaml` - TTS config schema
- `lib/audio/tts/cfg.yaml` - oapi-codegen config
- `lib/audio/tts/plugin.go` - Registry + interface
- `lib/audio/tts/openai.go` - OpenAI implementation
- `lib/audio/tts/google.go` - Google Cloud implementation
- `lib/audio/tts/elevenlabs.go` - ElevenLabs implementation
- `lib/audio/stt/openapi.yaml` - STT config schema
- `lib/audio/stt/cfg.yaml` - oapi-codegen config
- `lib/audio/stt/plugin.go` - Registry + interface
- `lib/audio/stt/openai.go` - OpenAI Whisper implementation
- `lib/audio/stt/google.go` - Google Cloud implementation
- `lib/template/audiohelpers.go` - Template helpers

### Modified Files
- `src/metadata/api.yaml` - Add `/v1/audio/tts` and `/v1/audio/stt` endpoints
- `src/metadata/api.go` - Implement audio handlers
- `go.mod` - Add dependencies (ElevenLabs SDK if available)
- `Makefile` - Add `generate` targets for new OpenAPI schemas

## Dependencies

```go
// Existing (already in go.mod)
cloud.google.com/go/texttospeech
cloud.google.com/go/speech
github.com/sashabaranov/go-openai

// New
// ElevenLabs has no official Go SDK - use HTTP client
```

## Testing Plan

1. **Unit tests** for each provider with mocked HTTP responses
2. **Integration tests** (optional, require API keys):
   ```bash
   OPENAI_API_KEY=... go test ./lib/audio/tts -run TestOpenAI -integration
   ELEVENLABS_API_KEY=... go test ./lib/audio/tts -run TestElevenLabs -integration
   ```
3. **S3 tests** using MinIO in CI
4. **Streaming tests** - verify chunk delivery timing
5. **E2E tests** - full API endpoint tests

## Future Enhancements

1. **Termite integration** - Local Whisper for STT (ONNX model)
2. **Caching** - Cache synthesized audio by content hash
3. **Index enricher** - Auto-transcribe audio fields during indexing
4. **Voice cloning** - ElevenLabs voice cloning API
5. **Real-time translation** - STT -> Translate -> TTS pipeline

## References

- [OpenAI TTS API](https://platform.openai.com/docs/guides/text-to-speech)
- [OpenAI Whisper API](https://platform.openai.com/docs/guides/speech-to-text)
- [Google Cloud Text-to-Speech](https://cloud.google.com/text-to-speech/docs)
- [Google Cloud Speech-to-Text](https://cloud.google.com/speech-to-text/docs)
- [ElevenLabs API](https://elevenlabs.io/docs/api-reference)
- [ElevenLabs Streaming](https://elevenlabs.io/docs/api-reference/streaming)
