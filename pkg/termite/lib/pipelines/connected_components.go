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

import "image"

// ComponentRect represents the bounding rectangle of a connected component.
type ComponentRect struct {
	MinX, MinY, MaxX, MaxY int
	Area                   int
}

// FindConnectedComponents performs connected component labeling on a binary mask
// using union-find. Returns bounding rectangles for components exceeding minArea.
func FindConnectedComponents(mask []bool, width, height, minArea int) []ComponentRect {
	if len(mask) != width*height {
		return nil
	}

	// Union-find data structure
	parent := make([]int, width*height)
	rank := make([]int, width*height)
	for i := range parent {
		parent[i] = i
	}

	find := func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]] // path compression
			x = parent[x]
		}
		return x
	}

	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra == rb {
			return
		}
		if rank[ra] < rank[rb] {
			ra, rb = rb, ra
		}
		parent[rb] = ra
		if rank[ra] == rank[rb] {
			rank[ra]++
		}
	}

	// First pass: connect adjacent foreground pixels
	for y := range height {
		for x := range width {
			idx := y*width + x
			if !mask[idx] {
				continue
			}

			// Check left neighbor
			if x > 0 && mask[idx-1] {
				union(idx, idx-1)
			}
			// Check top neighbor
			if y > 0 && mask[idx-width] {
				union(idx, idx-width)
			}
		}
	}

	// Second pass: collect component bounding boxes
	components := make(map[int]*ComponentRect)
	for y := range height {
		for x := range width {
			idx := y*width + x
			if !mask[idx] {
				continue
			}

			root := find(idx)
			comp, ok := components[root]
			if !ok {
				comp = &ComponentRect{
					MinX: x, MinY: y,
					MaxX: x, MaxY: y,
					Area: 0,
				}
				components[root] = comp
			}

			if x < comp.MinX {
				comp.MinX = x
			}
			if y < comp.MinY {
				comp.MinY = y
			}
			if x > comp.MaxX {
				comp.MaxX = x
			}
			if y > comp.MaxY {
				comp.MaxY = y
			}
			comp.Area++
		}
	}

	// Filter by minimum area
	result := make([]ComponentRect, 0, len(components))
	for _, comp := range components {
		if comp.Area >= minArea {
			result = append(result, *comp)
		}
	}

	return result
}

// HeatmapPostProcessor converts heatmap detection output to text regions
// using connected component labeling. Used by Surya detection.
type HeatmapPostProcessor struct {
	// Threshold is the probability threshold for binarization.
	Threshold float32
	// MinArea is the minimum component area to keep.
	MinArea int
}

// NewHeatmapPostProcessor creates a heatmap post-processor with the given parameters.
func NewHeatmapPostProcessor(threshold float32, minArea int) *HeatmapPostProcessor {
	return &HeatmapPostProcessor{
		Threshold: threshold,
		MinArea:   minArea,
	}
}

// Process converts a heatmap output to text regions.
func (p *HeatmapPostProcessor) Process(output []float32, width, height int, originalBounds image.Rectangle) []TextRegion {
	if len(output) < width*height {
		return nil
	}

	// Threshold to binary mask
	mask := make([]bool, width*height)
	for i := 0; i < width*height; i++ {
		mask[i] = output[i] > p.Threshold
	}

	// Find connected components
	components := FindConnectedComponents(mask, width, height, p.MinArea)

	// Scale to original image coordinates
	scaleX := float64(originalBounds.Dx()) / float64(width)
	scaleY := float64(originalBounds.Dy()) / float64(height)

	regions := make([]TextRegion, len(components))
	for i, comp := range components {
		regions[i] = TextRegion{
			BBox: [4]float64{
				float64(comp.MinX) * scaleX,
				float64(comp.MinY) * scaleY,
				float64(comp.MaxX+1) * scaleX,
				float64(comp.MaxY+1) * scaleY,
			},
			Confidence: 1.0,
		}
	}

	return regions
}
