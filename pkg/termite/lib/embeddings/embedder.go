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

package embeddings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif" // Register GIF decoder
	stdjpeg "image/jpeg"
	_ "image/png" // Register PNG decoder
	"strings"

	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/embeddings"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	flexjpeg "github.com/kovidgoyal/imaging/jpeg"
	"go.uber.org/zap"
)

// Ensure PooledEmbedder implements the Embedder interface
var _ embeddings.Embedder = (*PooledEmbedder)(nil)

// DefaultEmbeddingBatchSize is the default batch size for embedding inference.
// The ONNX Runtime and GoMLX backends both handle batching fine.
// CoreML runs through GoMLX/go-coreml (not the ONNX Runtime CoreML EP bridge),
// so the old batch_size=1 workaround no longer applies.
const DefaultEmbeddingBatchSize = 32

// pipelineSet holds the pipelines for one pool slot.
// Any combination of pipelines can be present: text-only models have only text,
// CLIP models have text+visual, CLAP models have text+audio, and CLIPCLAP models
// have all three.
type pipelineSet struct {
	text   *pipelines.EmbeddingPipeline
	visual *pipelines.EmbeddingPipeline
	audio  *pipelines.EmbeddingPipeline
}

// close releases all pipelines in the set.
func (ps *pipelineSet) close() error {
	var errs []error
	if ps.text != nil {
		if err := ps.text.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing text pipeline: %w", err))
		}
	}
	if ps.visual != nil {
		if err := ps.visual.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing visual pipeline: %w", err))
		}
	}
	if ps.audio != nil {
		if err := ps.audio.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing audio pipeline: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing pipelines: %v", errs)
	}
	return nil
}

// PooledEmbedder manages multiple pipeline sets for concurrent embedding generation.
// Handles text-only, multimodal (CLIP), audio (CLAP), and unified (CLIPCLAP) models
// through a single type. Content is automatically routed to the appropriate pipeline
// based on input modality.
type PooledEmbedder struct {
	pool        *pool.LazyPool[pipelineSet]
	logger      *zap.Logger
	caps        embeddings.EmbedderCapabilities
	batchSize   int
	backendType backends.BackendType
}

// PooledEmbedderConfig holds configuration for creating a PooledEmbedder.
type PooledEmbedderConfig struct {
	// ModelPath is the path to the model directory
	ModelPath string

	// PoolSize determines how many concurrent requests can be processed (0 = auto-detect from CPU count)
	PoolSize int

	// BatchSize is the inference batch size (0 = use default)
	BatchSize int

	// Normalize enables L2 normalization of embeddings
	Normalize bool

	// Quantized enables quantized model loading (e.g., *_quantized.onnx files)
	Quantized bool

	// Pooling specifies the pooling strategy ("mean", "cls", "max")
	Pooling backends.PoolingStrategy

	// ModelBackends specifies which backends this model supports (nil = all backends)
	ModelBackends []string

	// Logger for logging (nil = no logging)
	Logger *zap.Logger
}

// NewPooledEmbedder creates a new pipeline-based pooled embedder.
// Automatically detects and loads all available pipelines (text, visual, audio)
// from the model directory. For text-only models, only the text pipeline is loaded.
// For multimodal models, all available pipelines are loaded per pool slot.
func NewPooledEmbedder(
	cfg PooledEmbedderConfig,
	sessionManager *backends.SessionManager,
) (*PooledEmbedder, backends.BackendType, error) {
	if cfg.ModelPath == "" {
		return nil, "", fmt.Errorf("model path is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// Default pool size
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	// Default batch size
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultEmbeddingBatchSize
	}

	// Default pooling strategy
	pooling := cfg.Pooling
	if pooling == "" {
		pooling = backends.PoolingMean
	}

	// Build loader options
	opts := []pipelines.EmbeddingLoaderOption{
		pipelines.WithEmbeddingNormalization(cfg.Normalize),
		pipelines.WithPoolingStrategy(pooling),
	}
	if cfg.Quantized {
		opts = append(opts, pipelines.WithQuantized(true))
	}

	logger.Info("Initializing pooled embedder",
		zap.String("modelPath", cfg.ModelPath),
		zap.Int("poolSize", poolSize),
		zap.Int("batchSize", batchSize),
		zap.Bool("quantized", cfg.Quantized))

	// Track the backend type from the first pipeline set
	var backendUsed backends.BackendType

	factory := func() (pipelineSet, error) {
		textPipeline, visualPipeline, audioPipeline, bt, err := pipelines.LoadEmbeddingPipelines(
			cfg.ModelPath,
			sessionManager,
			cfg.ModelBackends,
			opts...,
		)
		if err != nil {
			return pipelineSet{}, fmt.Errorf("creating pipeline set: %w", err)
		}
		if textPipeline == nil && visualPipeline == nil && audioPipeline == nil {
			return pipelineSet{}, fmt.Errorf("model at %s does not have any encoder", cfg.ModelPath)
		}
		backendUsed = bt
		return pipelineSet{
			text:   textPipeline,
			visual: visualPipeline,
			audio:  audioPipeline,
		}, nil
	}

	p, first, err := pool.New(pool.Config[pipelineSet]{
		Size:    poolSize,
		Factory: factory,
		Close:   func(ps pipelineSet) error { return ps.close() },
		Logger:  logger,
	})
	if err != nil {
		return nil, "", fmt.Errorf("creating embedder pool: %w", err)
	}

	// Build capabilities from the first pipeline set
	caps := buildCapabilities(first.text, first.visual, first.audio)

	logger.Info("Successfully created pooled embedder",
		zap.Int("poolSize", poolSize),
		zap.String("backend", string(backendUsed)),
		zap.Bool("hasVisual", first.visual != nil),
		zap.Bool("hasAudio", first.audio != nil))

	return &PooledEmbedder{
		pool:        p,
		logger:      logger,
		caps:        caps,
		batchSize:   batchSize,
		backendType: backendUsed,
	}, backendUsed, nil
}

// buildCapabilities constructs EmbedderCapabilities based on available pipelines.
func buildCapabilities(text, visual, audio *pipelines.EmbeddingPipeline) embeddings.EmbedderCapabilities {
	caps := embeddings.EmbedderCapabilities{
		SupportedMIMETypes: []embeddings.MIMETypeSupport{},
	}

	if text != nil {
		caps.SupportedMIMETypes = append(caps.SupportedMIMETypes,
			embeddings.MIMETypeSupport{MIMEType: "text/plain"})
	}

	if visual != nil {
		caps.SupportedMIMETypes = append(caps.SupportedMIMETypes,
			embeddings.MIMETypeSupport{MIMEType: "image/jpeg"},
			embeddings.MIMETypeSupport{MIMEType: "image/png"},
			embeddings.MIMETypeSupport{MIMEType: "image/*"})
	}

	if audio != nil {
		caps.SupportedMIMETypes = append(caps.SupportedMIMETypes,
			embeddings.MIMETypeSupport{MIMEType: "audio/wav"},
			embeddings.MIMETypeSupport{MIMEType: "audio/wave"},
			embeddings.MIMETypeSupport{MIMEType: "audio/x-wav"},
			embeddings.MIMETypeSupport{MIMEType: "audio/*"})
	}

	return caps
}

// Capabilities returns the capabilities of this embedder.
func (p *PooledEmbedder) Capabilities() embeddings.EmbedderCapabilities {
	return p.caps
}

// BackendType returns the backend type used by this embedder.
func (p *PooledEmbedder) BackendType() backends.BackendType {
	return p.backendType
}

// isMultimodal returns true if this embedder has visual or audio pipelines.
func (p *PooledEmbedder) isMultimodal() bool {
	first := p.pool.First()
	return first.visual != nil || first.audio != nil
}

// Embed generates embeddings for the given content.
// Thread-safe: uses pool to limit concurrent pipeline access.
// For text-only models, extracts text and processes in batches.
// For multimodal models, routes each input to the appropriate pipeline
// (text, visual, or audio) based on content type.
func (p *PooledEmbedder) Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error) {
	if len(contents) == 0 {
		return [][]float32{}, nil
	}

	// Acquire a pool slot (blocks if all pipelines busy)
	ps, idx, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring pipeline slot: %w", err)
	}
	defer p.pool.Release()

	// Use multimodal path if visual or audio pipelines are available
	if p.isMultimodal() {
		return p.embedMultimodal(ctx, contents, ps, idx)
	}

	return p.embedTextOnly(ctx, contents, ps.text, idx)
}

// embedTextOnly handles the text-only fast path with batching.
func (p *PooledEmbedder) embedTextOnly(ctx context.Context, contents [][]ai.ContentPart, textPipeline *pipelines.EmbeddingPipeline, pipelineIdx int) ([][]float32, error) {
	// Extract text from content parts
	texts := embeddings.ExtractText(contents)

	// Process in batches
	result := make([][]float32, 0, len(texts))
	batchSize := p.batchSize

	numBatches := (len(texts) + batchSize - 1) / batchSize
	p.logger.Debug("Processing text embeddings in batches",
		zap.Int("pipelineIndex", pipelineIdx),
		zap.Int("numTexts", len(texts)),
		zap.Int("batchSize", batchSize),
		zap.Int("numBatches", numBatches))

	for batchStart := 0; batchStart < len(texts); batchStart += batchSize {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		batchEnd := min(batchStart+batchSize, len(texts))
		batch := texts[batchStart:batchEnd]

		batchEmbeddings, err := textPipeline.Embed(ctx, batch)
		if err != nil {
			p.logger.Error("Pipeline inference failed",
				zap.Int("pipelineIndex", pipelineIdx),
				zap.Int("batchStart", batchStart),
				zap.Int("batchSize", len(batch)),
				zap.Error(err))
			return nil, fmt.Errorf("running embedding inference (batch %d-%d): %w", batchStart, batchEnd, err)
		}

		for i, embedding := range batchEmbeddings {
			if len(embedding) == 0 {
				p.logger.Error("Empty embedding returned",
					zap.Int("pipelineIndex", pipelineIdx),
					zap.Int("index", batchStart+i))
				return nil, fmt.Errorf("empty embedding at index %d", batchStart+i)
			}
		}

		result = append(result, batchEmbeddings...)
	}

	p.logger.Debug("Text embedding generation complete",
		zap.Int("pipelineIndex", pipelineIdx),
		zap.Int("numEmbeddings", len(result)))

	return result, nil
}

// embedMultimodal handles mixed-modality content by routing to appropriate pipelines.
func (p *PooledEmbedder) embedMultimodal(ctx context.Context, contents [][]ai.ContentPart, ps pipelineSet, pipelineIdx int) ([][]float32, error) {
	results := make([][]float32, len(contents))

	// Batch inputs by modality for efficiency
	var textIndices []int
	var textInputs []string
	var imageIndices []int
	var imageInputs []image.Image
	var audioIndices []int
	var audioInputs [][]byte

	for i, parts := range contents {
		text, img, audio, err := extractContent(parts)
		if err != nil {
			return nil, fmt.Errorf("extracting content at index %d: %w", i, err)
		}

		if text != "" {
			textIndices = append(textIndices, i)
			textInputs = append(textInputs, text)
		} else if img != nil {
			imageIndices = append(imageIndices, i)
			imageInputs = append(imageInputs, img)
		} else if audio != nil {
			audioIndices = append(audioIndices, i)
			audioInputs = append(audioInputs, audio)
		} else {
			return nil, fmt.Errorf("no text, image, or audio content found at index %d", i)
		}
	}

	// Process text inputs one at a time (multimodal text models may only support batch_size=1)
	if len(textInputs) > 0 {
		if ps.text == nil {
			return nil, fmt.Errorf("text embedding requested but no text encoder available")
		}
		for i, text := range textInputs {
			embedding, err := ps.text.EmbedOne(ctx, text)
			if err != nil {
				return nil, fmt.Errorf("embedding text %d: %w", i, err)
			}
			results[textIndices[i]] = embedding
		}
	}

	// Process image batch
	if len(imageInputs) > 0 {
		if ps.visual == nil {
			return nil, fmt.Errorf("image embedding requested but no visual encoder available")
		}
		imageEmbeddings, err := ps.visual.EmbedImages(ctx, imageInputs)
		if err != nil {
			return nil, fmt.Errorf("embedding images: %w", err)
		}
		for i, idx := range imageIndices {
			results[idx] = imageEmbeddings[i]
		}
	}

	// Process audio inputs
	if len(audioInputs) > 0 {
		if ps.audio == nil {
			return nil, fmt.Errorf("audio embedding requested but no audio encoder available")
		}
		audioEmbeddings, err := ps.audio.EmbedAudio(ctx, audioInputs)
		if err != nil {
			return nil, fmt.Errorf("embedding audio: %w", err)
		}
		for i, idx := range audioIndices {
			results[idx] = audioEmbeddings[i]
		}
	}

	p.logger.Debug("Multimodal embedding generation complete",
		zap.Int("pipelineIndex", pipelineIdx),
		zap.Int("textCount", len(textInputs)),
		zap.Int("imageCount", len(imageInputs)),
		zap.Int("audioCount", len(audioInputs)))

	return results, nil
}

// extractContent extracts text, image, or audio from content parts.
// Returns (text, image, audioData, error). Only one will be non-empty/non-nil.
func extractContent(parts []ai.ContentPart) (string, image.Image, []byte, error) {
	for _, part := range parts {
		switch c := part.(type) {
		case ai.TextContent:
			if c.Text != "" {
				return c.Text, nil, nil, nil
			}
		case ai.BinaryContent:
			if isImageMIME(c.MIMEType) {
				img, err := decodeImage(c.Data)
				if err != nil {
					return "", nil, nil, fmt.Errorf("decoding image: %w", err)
				}
				return "", img, nil, nil
			}
			if isAudioMIME(c.MIMEType) {
				audioCopy := make([]byte, len(c.Data))
				copy(audioCopy, c.Data)
				return "", nil, audioCopy, nil
			}
		case ai.ImageURLContent:
			if c.URL != "" {
				return c.URL, nil, nil, nil
			}
		}
	}
	return "", nil, nil, nil
}

// decodeImage decodes image bytes using Go's standard image decoders,
// falling back to a more tolerant JPEG decoder for images with
// non-standard chroma subsampling ratios that the stdlib rejects.
//
// TODO(go1.27): Remove flexjpeg fallback once Go 1.27 lands the stdlib
// fix for non-standard JPEG subsampling (https://go.dev/issue/2362).
func decodeImage(data []byte) (image.Image, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err == nil {
		return img, nil
	}

	// Only fall back for JPEG subsampling errors
	var unsupported stdjpeg.UnsupportedError
	if !errors.As(err, &unsupported) {
		return nil, err
	}

	img, fallbackErr := flexjpeg.Decode(bytes.NewReader(data))
	if fallbackErr != nil {
		return nil, fmt.Errorf("stdlib: %w; flex decoder: %w", err, fallbackErr)
	}
	return img, nil
}

// isImageMIME checks if the MIME type is an image type.
func isImageMIME(mimeType string) bool {
	return strings.HasPrefix(mimeType, "image/")
}

// isAudioMIME checks if the MIME type is an audio type.
func isAudioMIME(mimeType string) bool {
	return strings.HasPrefix(mimeType, "audio/")
}

// Close releases resources.
func (p *PooledEmbedder) Close() error {
	return p.pool.Close()
}
