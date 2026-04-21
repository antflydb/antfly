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

package pipelines

import (
	"image"
	"math"
)

// DBPostProcessor implements Differentiable Binarization (DB) post-processing
// for PaddleOCR's DBNet detection output.
type DBPostProcessor struct {
	// Threshold is the binarization threshold for the probability map.
	Threshold float32
	// BoxThreshold is the minimum mean probability within a detected box.
	BoxThreshold float32
	// UnclipRatio expands detected bounding boxes by this ratio.
	UnclipRatio float64
	// MinBoxArea is the minimum bounding box area to keep.
	MinBoxArea int
	// MaxCandidates is the maximum number of candidate boxes to process.
	MaxCandidates int
}

// NewDBPostProcessor creates a DB post-processor with the given parameters.
func NewDBPostProcessor(threshold, boxThreshold float32, unclipRatio float64, minBoxArea int) *DBPostProcessor {
	return &DBPostProcessor{
		Threshold:     threshold,
		BoxThreshold:  boxThreshold,
		UnclipRatio:   unclipRatio,
		MinBoxArea:    minBoxArea,
		MaxCandidates: 1000,
	}
}

// Process converts a DB probability map to text regions.
func (p *DBPostProcessor) Process(output []float32, width, height int, originalBounds image.Rectangle) []TextRegion {
	if len(output) < width*height {
		return nil
	}

	// Threshold to binary mask
	mask := make([]bool, width*height)
	for i := 0; i < width*height; i++ {
		mask[i] = output[i] > p.Threshold
	}

	// Find connected components (reuse the same infrastructure as heatmap)
	components := FindConnectedComponents(mask, width, height, p.MinBoxArea)

	// Scale to original image coordinates
	scaleX := float64(originalBounds.Dx()) / float64(width)
	scaleY := float64(originalBounds.Dy()) / float64(height)

	var regions []TextRegion
	for i, comp := range components {
		if i >= p.MaxCandidates {
			break
		}

		// Compute mean probability within the component box
		meanProb := p.computeBoxScore(output, comp, width)
		if meanProb < float64(p.BoxThreshold) {
			continue
		}

		// Expand bounding box by unclip ratio
		bbox := p.unclipBox(comp, scaleX, scaleY, originalBounds)

		regions = append(regions, TextRegion{
			BBox:       bbox,
			Confidence: meanProb,
		})
	}

	return regions
}

// computeBoxScore computes the mean probability within a component's bounding box.
func (p *DBPostProcessor) computeBoxScore(probMap []float32, comp ComponentRect, width int) float64 {
	var sum float64
	var count int

	for y := comp.MinY; y <= comp.MaxY; y++ {
		for x := comp.MinX; x <= comp.MaxX; x++ {
			idx := y*width + x
			if idx < len(probMap) {
				sum += float64(probMap[idx])
				count++
			}
		}
	}

	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// unclipBox expands a bounding box by the unclip ratio and scales to original coordinates.
func (p *DBPostProcessor) unclipBox(comp ComponentRect, scaleX, scaleY float64, bounds image.Rectangle) [4]float64 {
	boxW := float64(comp.MaxX-comp.MinX+1) * scaleX
	boxH := float64(comp.MaxY-comp.MinY+1) * scaleY

	// Compute expansion distance based on perimeter
	perimeter := 2 * (boxW + boxH)
	area := boxW * boxH
	distance := area * p.UnclipRatio / perimeter

	x1 := float64(comp.MinX)*scaleX - distance
	y1 := float64(comp.MinY)*scaleY - distance
	x2 := float64(comp.MaxX+1)*scaleX + distance
	y2 := float64(comp.MaxY+1)*scaleY + distance

	// Clamp to image bounds
	x1 = math.Max(0, x1)
	y1 = math.Max(0, y1)
	x2 = math.Min(float64(bounds.Dx()), x2)
	y2 = math.Min(float64(bounds.Dy()), y2)

	return [4]float64{x1, y1, x2, y2}
}
