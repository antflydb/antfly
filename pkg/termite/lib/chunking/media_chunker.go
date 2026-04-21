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
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"go.uber.org/zap"
)

// MediaChunker splits binary media content into chunks.
type MediaChunker interface {
	ChunkMedia(ctx context.Context, data []byte, mimeType string, opts chunking.ChunkOptions) ([]chunking.Chunk, error)
}

// FixedMediaChunker dispatches media chunking to the appropriate handler
// based on MIME type. Algorithmic only (no ML models required).
type FixedMediaChunker struct {
	audio *AudioChunker
	gif   *GIFChunker
}

// NewFixedMediaChunker creates a new media chunker that dispatches by MIME type.
func NewFixedMediaChunker() *FixedMediaChunker {
	return &FixedMediaChunker{
		audio: &AudioChunker{},
		gif:   &GIFChunker{},
	}
}

// CloseableMediaChunker extends MediaChunker with lifecycle management.
type CloseableMediaChunker interface {
	MediaChunker
	io.Closer
}

// normalizeAudioMIME classifies a MIME string as "wav", "mp3", or "" (unsupported).
func normalizeAudioMIME(mimeType string) string {
	m := strings.ToLower(strings.TrimSpace(mimeType))
	switch m {
	case "audio/wav", "audio/x-wav", "audio/wave":
		return "wav"
	case "audio/mpeg", "audio/mp3":
		return "mp3"
	}
	return ""
}

// ChunkMedia dispatches to the appropriate chunker based on MIME type.
func (m *FixedMediaChunker) ChunkMedia(ctx context.Context, data []byte, mimeType string, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty media data")
	}

	switch normalizeAudioMIME(mimeType) {
	case "wav":
		return m.audio.ChunkAudio(ctx, data, opts)
	case "mp3":
		return m.audio.ChunkMP3(ctx, data, opts)
	}

	// Non-audio types
	mime := strings.ToLower(strings.TrimSpace(mimeType))
	switch mime {
	case "image/gif":
		return m.gif.ChunkGIF(ctx, data, opts)
	default:
		return nil, fmt.Errorf("unsupported media type for chunking: %s", mimeType)
	}
}

// MediaChunkerConfig configures model-based media chunker creation.
type MediaChunkerConfig struct {
	// ModelPath is the directory containing the model files
	ModelPath string
	// Capabilities from the model manifest (e.g., ["audio"])
	Capabilities []string
	// ModelBackends restricts which inference backends to use
	ModelBackends []string
	// SessionOpts are additional session options (e.g., input constants, dynamic axes)
	SessionOpts []backends.SessionOption
	// Logger for logging
	Logger *zap.Logger
}

// NewMediaChunkerFromModel creates a model-based media chunker from a model directory.
// The concrete chunker type is determined by the model's capabilities:
//   - "audio" capability → VADAudioChunker (Silero VAD)
//
// Returns the chunker, the backend used, and any error.
func NewMediaChunkerFromModel(
	cfg MediaChunkerConfig,
	sessionManager *backends.SessionManager,
) (CloseableMediaChunker, backends.BackendType, error) {
	if cfg.ModelPath == "" {
		return nil, "", fmt.Errorf("model path is required")
	}
	if sessionManager == nil {
		return nil, "", fmt.Errorf("session manager is required for model-based media chunking")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	modelFile := filepath.Join(cfg.ModelPath, "model.onnx")
	if _, err := os.Stat(modelFile); os.IsNotExist(err) {
		return nil, "", fmt.Errorf("model file not found: %s", modelFile)
	}

	// Apply smart defaults when manifest doesn't provide session options.
	// This handles models downloaded directly from HuggingFace without a manifest.
	sessionOpts := cfg.SessionOpts
	if len(sessionOpts) == 0 && slices.Contains(cfg.Capabilities, string(modelregistry.CapabilityAudio)) {
		sessionOpts = defaultAudioSessionOpts()
		logger.Info("Using default VAD session options (no manifest session_options)")
	}

	factory, backendType, err := sessionManager.GetSessionFactoryForModel(cfg.ModelBackends)
	if err != nil {
		return nil, "", fmt.Errorf("no session factory available: %w", err)
	}

	session, err := factory.CreateSession(modelFile, sessionOpts...)
	if err != nil {
		return nil, "", fmt.Errorf("creating session: %w", err)
	}

	// Dispatch by capability to create the appropriate chunker type
	if slices.Contains(cfg.Capabilities, string(modelregistry.CapabilityAudio)) {
		chunker := NewVADAudioChunker(session, DefaultVADConfig())
		logger.Info("Created audio media chunker",
			zap.String("backend", string(backendType)),
			zap.String("path", cfg.ModelPath))
		return chunker, backendType, nil
	}

	// No matching capability — close session and return error
	_ = session.Close()
	return nil, "", fmt.Errorf("no media chunker implementation for capabilities: %v", cfg.Capabilities)
}

// defaultAudioSessionOpts returns session options suitable for Silero VAD models.
// These are applied when the model manifest doesn't specify session_options,
// e.g., when a user downloads the model directly from HuggingFace.
func defaultAudioSessionOpts() []backends.SessionOption {
	return []backends.SessionOption{
		backends.WithInputConstants(map[string]any{"sr": int64(vadSampleRate)}),
		backends.WithDynamicAxes([]backends.DynamicAxisOverride{
			{InputName: "input", Axis: 0, ParamName: "batch"},
			{InputName: "input", Axis: 1, ParamName: "frame_size"},
		}),
	}
}
