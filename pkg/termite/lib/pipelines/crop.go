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
	"sort"
)

// CropBBox extracts a rectangular region from an image using a bounding box.
// bbox is [x1, y1, x2, y2] in pixel coordinates.
func CropBBox(img image.Image, bbox [4]float64) image.Image {
	x := int(bbox[0])
	y := int(bbox[1])
	w := int(bbox[2]-bbox[0]) + 1
	h := int(bbox[3]-bbox[1]) + 1

	if w <= 0 || h <= 0 {
		return image.NewRGBA(image.Rect(0, 0, 1, 1))
	}

	return cropImage(img, x, y, w, h)
}

// ResizeKeepAspect resizes an image to the target height while maintaining aspect ratio,
// capping at maxWidth. Used for recognition input preprocessing.
func ResizeKeepAspect(img image.Image, targetH, maxW int) image.Image {
	bounds := img.Bounds()
	srcW := bounds.Dx()
	srcH := bounds.Dy()

	if srcH == 0 || srcW == 0 {
		return img
	}

	// Compute target width maintaining aspect ratio
	targetW := min(int(float64(srcW)*float64(targetH)/float64(srcH)), maxW)
	if targetW <= 0 {
		targetW = 1
	}

	return resize(img, targetW, targetH)
}

// SortRegionsByReadingOrder sorts text regions in top-to-bottom, left-to-right order.
// Uses the Y-center as primary sort key with a tolerance band, then X position.
func SortRegionsByReadingOrder(regions []TextRegion) {
	if len(regions) <= 1 {
		return
	}

	// Compute average line height for tolerance band
	avgHeight := 0.0
	for _, r := range regions {
		avgHeight += r.BBox[3] - r.BBox[1]
	}
	avgHeight /= float64(len(regions))
	tolerance := avgHeight * 0.5 // Regions within half a line height are on the same line

	sort.Slice(regions, func(i, j int) bool {
		yi := (regions[i].BBox[1] + regions[i].BBox[3]) / 2
		yj := (regions[j].BBox[1] + regions[j].BBox[3]) / 2

		// If Y centers are within tolerance, sort by X
		if abs(yi-yj) < tolerance {
			return regions[i].BBox[0] < regions[j].BBox[0]
		}

		return yi < yj
	})
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
