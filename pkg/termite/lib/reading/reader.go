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

package reading

import (
	"context"
	"fmt"
	"image"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"github.com/antflydb/antfly/pkg/termite/lib/pool"
	"go.uber.org/zap"
)

// OutputParser transforms raw model text output into a structured Result.
// Each model family provides its own parser (e.g., DonutOutputParser, FlorenceOutputParser).
type OutputParser func(text, prompt string) Result

// DefaultOutputParser returns the trimmed text as-is with no structured extraction.
func DefaultOutputParser(text, prompt string) Result {
	return Result{Text: strings.TrimSpace(text)}
}

// RecognizedRegion represents a text region with recognized text and bounding box.
// Populated by multi-stage OCR models (Surya, PaddleOCR).
type RecognizedRegion struct {
	// Text is the recognized text within this region.
	Text string

	// BBox is the bounding box [x1, y1, x2, y2] in pixel coordinates.
	BBox [4]float64

	// Confidence is the recognition confidence score.
	Confidence float64

	// Label is the semantic label (e.g., "text", "title", "table"), set by layout analysis.
	Label string
}

// Result contains the output from reading an image.
type Result struct {
	// Text is the raw extracted text from the image
	Text string

	// Fields contains structured field values extracted by document understanding models.
	// Fields are flattened with dot notation for nested structures (e.g., "menu.nm", "menu.price").
	// This is populated by models like Donut that output structured data.
	Fields map[string]string

	// Regions contains individual text regions with bounding boxes and recognized text.
	// Populated by multi-stage OCR models (Surya, PaddleOCR).
	Regions []RecognizedRegion
}

// Reader provides OCR and document understanding for images.
// It wraps Vision2Seq models (TrOCR, Donut, Florence-2, Moondream) to extract text from images.
type Reader interface {
	// Read extracts text from the given images.
	// The optional prompt parameter allows specifying a task prompt for document understanding models:
	//   - TrOCR: prompt is ignored (pure OCR)
	//   - Donut CORD: "<s_cord-v2>" for receipt parsing
	//   - Donut DocVQA: "<s_docvqa><s_question>...</s_question><s_answer>" for visual QA
	//   - Florence-2: "<OCR>" for text extraction, "<CAPTION>" for captioning
	//   - Moondream: natural language prompt (e.g., "Describe this image in detail")
	//
	// maxTokens limits the generated output length (0 uses model default).
	//
	// Returns one Result per input image. For Moondream, Result.Fields contains
	// structured output (mood, tags, possible_source) extracted from the JSON response.
	Read(ctx context.Context, images []image.Image, prompt string, maxTokens int) ([]Result, error)

	// Close releases model resources.
	Close() error
}

// Ensure PooledReader implements the Reader interface
var _ Reader = (*PooledReader)(nil)

// PooledReader manages multiple Vision2Seq pipelines for concurrent OCR/document reading.
// Each request acquires a pipeline slot via the pool, enabling true parallelism.
type PooledReader struct {
	pool         *pool.LazyPool[*pipelines.Vision2SeqPipeline]
	logger       *zap.Logger
	outputParser OutputParser
	modelPath    string
}

// PooledReaderConfig holds configuration for creating a PooledReader.
type PooledReaderConfig struct {
	// ModelPath is the path to the Vision2Seq model.
	ModelPath string

	// PoolSize is the number of concurrent pipelines (0 = default of 1).
	PoolSize int

	// GenerationConfig holds text generation parameters. If nil, uses defaults.
	GenerationConfig *backends.GenerationConfig

	// ImageConfig holds image preprocessing parameters. If nil, uses model's default.
	ImageConfig *backends.ImageConfig

	// Logger for logging. If nil, uses a no-op logger.
	Logger *zap.Logger
}

// NewPooledReader creates a new pooled reader from the given configuration.
// sessionManager is used to load the vision2seq model.
func NewPooledReader(
	cfg *PooledReaderConfig,
	sessionManager *backends.SessionManager,
	modelBackends []string,
) (*PooledReader, backends.BackendType, error) {
	if cfg == nil {
		return nil, "", fmt.Errorf("config is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	// Detect output parser from path
	outputParser := detectOutputParser(cfg.ModelPath)
	logger.Info("Detected reader output parser",
		zap.String("path", cfg.ModelPath))

	// Build pipeline options
	var opts []pipelines.Vision2SeqPipelineOption
	if cfg.ImageConfig != nil {
		opts = append(opts, pipelines.WithVision2SeqImageConfig(cfg.ImageConfig))
	}
	if cfg.GenerationConfig != nil {
		opts = append(opts, pipelines.WithVision2SeqGenerationConfig(cfg.GenerationConfig))
	}

	// Create lazy pool of pipelines; capture backend type from the factory.
	var backendType backends.BackendType
	p, _, err := pool.New(pool.Config[*pipelines.Vision2SeqPipeline]{
		Size: poolSize,
		Factory: func() (*pipelines.Vision2SeqPipeline, error) {
			pipeline, bt, err := pipelines.LoadVision2SeqPipeline(
				cfg.ModelPath,
				sessionManager,
				modelBackends,
				opts...,
			)
			if err == nil {
				backendType = bt
			}
			return pipeline, err
		},
		Close: func(p *pipelines.Vision2SeqPipeline) error {
			if p != nil {
				return p.Close()
			}
			return nil
		},
		Logger: logger,
	})
	if err != nil {
		return nil, "", fmt.Errorf("creating pipeline pool: %w", err)
	}

	reader := &PooledReader{
		pool:         p,
		logger:       logger,
		outputParser: outputParser,
		modelPath:    cfg.ModelPath,
	}

	logger.Info("Created pooled reader",
		zap.Int("poolSize", poolSize),
		zap.String("backend", string(backendType)))

	return reader, backendType, nil
}

// detectOutputParser selects the appropriate output parser based on the model path.
func detectOutputParser(modelPath string) OutputParser {
	pathLower := strings.ToLower(modelPath)
	switch {
	case strings.Contains(pathLower, "donut"):
		return DonutOutputParser
	case strings.Contains(pathLower, "florence"):
		return FlorenceOutputParser
	case strings.Contains(pathLower, "moondream"):
		return MoondreamOutputParser
	default:
		return DefaultOutputParser
	}
}

// Read extracts text from the given images using the Vision2Seq model.
func (r *PooledReader) Read(ctx context.Context, images []image.Image, prompt string, maxTokens int) ([]Result, error) {
	if len(images) == 0 {
		return nil, fmt.Errorf("no images provided")
	}

	// Acquire a pipeline from the pool
	pipeline, _, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring pipeline from pool: %w", err)
	}
	defer r.pool.Release()

	// Temporarily override max tokens if specified
	originalMaxTokens := pipeline.GenerationConfig.MaxNewTokens
	if maxTokens > 0 {
		pipeline.GenerationConfig.MaxNewTokens = maxTokens
	}
	defer func() {
		pipeline.GenerationConfig.MaxNewTokens = originalMaxTokens
	}()

	// Process each image
	results := make([]Result, len(images))
	for i, img := range images {
		var output *pipelines.Vision2SeqResult
		var err error

		if prompt != "" {
			output, err = pipeline.RunWithPrompt(ctx, img, prompt)
		} else {
			output, err = pipeline.Run(ctx, img)
		}

		if err != nil {
			return nil, fmt.Errorf("running Vision2Seq inference on image %d: %w", i, err)
		}

		results[i] = r.parseOutput(output.Text, prompt)
	}

	r.logger.Debug("Read completed",
		zap.Int("numImages", len(images)),
		zap.Int("numResults", len(results)),
		zap.String("prompt", truncateString(prompt, 50)))

	return results, nil
}

// parseOutput parses the raw model output using the injected output parser.
func (r *PooledReader) parseOutput(text string, prompt string) Result {
	return r.outputParser(text, prompt)
}

// Close releases all pipeline resources.
func (r *PooledReader) Close() error {
	r.logger.Info("Closing pooled reader")
	return r.pool.Close()
}

// truncateString truncates a string to maxLen, adding "..." if truncated.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
