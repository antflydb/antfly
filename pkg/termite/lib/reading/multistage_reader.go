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

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/pipelines"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// Ensure MultiStageReader implements the Reader interface
var _ Reader = (*MultiStageReader)(nil)

// MultiStageReader wraps a MultiStageOCRPipeline with the Reader interface.
type MultiStageReader struct {
	pipeline *pipelines.MultiStageOCRPipeline
	sem      *semaphore.Weighted
	logger   *zap.Logger
}

// MultiStageReaderConfig holds configuration for creating a MultiStageReader.
type MultiStageReaderConfig struct {
	// ModelPath is the path to the multi-stage OCR model directory.
	ModelPath string

	// Logger for logging. If nil, uses a no-op logger.
	Logger *zap.Logger
}

// NewMultiStageReader creates a new multi-stage reader from the given configuration.
func NewMultiStageReader(
	cfg *MultiStageReaderConfig,
	sessionManager *backends.SessionManager,
	modelBackends []string,
) (*MultiStageReader, backends.BackendType, error) {
	if cfg == nil {
		return nil, "", fmt.Errorf("config is required")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	logger.Info("Creating multi-stage reader",
		zap.String("path", cfg.ModelPath))

	// Load the multi-stage pipeline
	pipeline, backendType, err := pipelines.LoadMultiStageOCRPipeline(
		cfg.ModelPath,
		sessionManager,
		modelBackends,
	)
	if err != nil {
		return nil, "", fmt.Errorf("loading multi-stage pipeline: %w", err)
	}

	reader := &MultiStageReader{
		pipeline: pipeline,
		sem:      semaphore.NewWeighted(1), // single-threaded for now
		logger:   logger,
	}

	logger.Info("Created multi-stage reader",
		zap.String("backend", string(backendType)))

	return reader, backendType, nil
}

// Read extracts text from the given images using the multi-stage OCR pipeline.
func (r *MultiStageReader) Read(ctx context.Context, images []image.Image, prompt string, maxTokens int) ([]Result, error) {
	if len(images) == 0 {
		return nil, fmt.Errorf("no images provided")
	}

	// Acquire pipeline slot
	if err := r.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("acquiring pipeline slot: %w", err)
	}
	defer r.sem.Release(1)

	results := make([]Result, len(images))
	for i, img := range images {
		ocrResult, err := r.pipeline.Run(ctx, img)
		if err != nil {
			return nil, fmt.Errorf("running multi-stage OCR on image %d: %w", i, err)
		}

		// Convert pipeline result to reading.Result
		result := Result{
			Text: ocrResult.FullText,
		}

		// Convert recognized regions
		if len(ocrResult.Regions) > 0 {
			result.Regions = make([]RecognizedRegion, len(ocrResult.Regions))
			for j, region := range ocrResult.Regions {
				result.Regions[j] = RecognizedRegion{
					Text:       region.Text,
					BBox:       region.BBox,
					Confidence: region.RecConfidence,
				}
			}

			// Add layout labels if available
			if len(ocrResult.Layout) > 0 {
				for j := range result.Regions {
					label := findMatchingLayoutLabel(result.Regions[j].BBox, ocrResult.Layout)
					if label != "" {
						result.Regions[j].Label = label
					}
				}
			}
		}

		results[i] = result
	}

	r.logger.Debug("Multi-stage read completed",
		zap.Int("numImages", len(images)),
		zap.Int("numResults", len(results)))

	return results, nil
}

// findMatchingLayoutLabel finds the layout label that best overlaps with a given bounding box.
func findMatchingLayoutLabel(bbox [4]float64, layout []pipelines.LayoutRegion) string {
	bestOverlap := 0.0
	bestLabel := ""

	for _, lr := range layout {
		overlap := computeOverlap(bbox, lr.BBox)
		if overlap > bestOverlap {
			bestOverlap = overlap
			bestLabel = lr.Label
		}
	}

	if bestOverlap > 0.3 { // Minimum 30% overlap
		return bestLabel
	}
	return ""
}

// computeOverlap computes the intersection-over-union between two bounding boxes.
func computeOverlap(a, b [4]float64) float64 {
	x1 := max(a[0], b[0])
	y1 := max(a[1], b[1])
	x2 := min(a[2], b[2])
	y2 := min(a[3], b[3])

	if x2 <= x1 || y2 <= y1 {
		return 0
	}

	intersection := (x2 - x1) * (y2 - y1)
	areaA := (a[2] - a[0]) * (a[3] - a[1])
	areaB := (b[2] - b[0]) * (b[3] - b[1])
	union := areaA + areaB - intersection

	if union <= 0 {
		return 0
	}
	return intersection / union
}

// Close releases all pipeline resources.
func (r *MultiStageReader) Close() error {
	r.logger.Info("Closing multi-stage reader")
	return r.pipeline.Close()
}
