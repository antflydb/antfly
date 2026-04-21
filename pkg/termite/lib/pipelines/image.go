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
	"bytes"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"  // Register GIF decoder
	_ "image/jpeg" // Register JPEG decoder
	_ "image/png"  // Register PNG decoder
	"io"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	_ "golang.org/x/image/bmp"  // Register BMP decoder
	_ "golang.org/x/image/tiff" // Register TIFF decoder
	_ "golang.org/x/image/webp" // Register WebP decoder
)

// ImageProcessor handles image preprocessing for vision backends.
type ImageProcessor struct {
	Config *backends.ImageConfig
}

// NewImageProcessor creates an ImageProcessor with the given configuration.
func NewImageProcessor(config *backends.ImageConfig) *ImageProcessor {
	if config == nil {
		config = backends.DefaultImageConfig()
	}
	return &ImageProcessor{Config: config}
}

// ProcessBytes preprocesses an image from bytes.
// Returns pixel values in NCHW format [channels, height, width] as a flat slice.
func (p *ImageProcessor) ProcessBytes(data []byte) ([]float32, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decoding image: %w", err)
	}
	return p.Process(img)
}

// ProcessReader preprocesses an image from a reader.
// Returns pixel values in NCHW format [channels, height, width] as a flat slice.
func (p *ImageProcessor) ProcessReader(r io.Reader) ([]float32, error) {
	img, _, err := image.Decode(r)
	if err != nil {
		return nil, fmt.Errorf("decoding image: %w", err)
	}
	return p.Process(img)
}

// Process preprocesses an image.
// Returns pixel values in NCHW format [channels, height, width] as a flat slice.
func (p *ImageProcessor) Process(img image.Image) ([]float32, error) {
	// Center crop if configured
	if p.Config.DoCenterCrop && p.Config.CropSize > 0 {
		img = centerCrop(img, p.Config.CropSize)
	}

	// Resize to target dimensions
	img = resize(img, p.Config.Width, p.Config.Height)

	// Convert to normalized float tensor in NCHW format
	return p.toTensor(img), nil
}

// ProcessBatch preprocesses multiple images.
// Returns pixel values in NCHW format [batch, channels, height, width] as a flat slice.
func (p *ImageProcessor) ProcessBatch(images []image.Image) ([]float32, error) {
	if len(images) == 0 {
		return nil, nil
	}

	c, h, w := p.Config.Channels, p.Config.Height, p.Config.Width
	result := make([]float32, len(images)*c*h*w)

	for i, img := range images {
		pixels, err := p.Process(img)
		if err != nil {
			return nil, fmt.Errorf("processing image %d: %w", i, err)
		}
		copy(result[i*c*h*w:], pixels)
	}

	return result, nil
}

// toTensor converts an image to a normalized float tensor in NCHW format.
func (p *ImageProcessor) toTensor(img image.Image) []float32 {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	channels := p.Config.Channels

	// Allocate in NCHW format: [channels, height, width]
	pixels := make([]float32, channels*height*width)

	// Extract pixels and normalize
	for y := range height {
		for x := range width {
			c := img.At(bounds.Min.X+x, bounds.Min.Y+y)
			r, g, b, _ := c.RGBA()

			// Convert from 0-65535 to 0-255, then apply rescale factor
			rf := float32(r>>8) * p.Config.RescaleFactor
			gf := float32(g>>8) * p.Config.RescaleFactor
			bf := float32(b>>8) * p.Config.RescaleFactor

			// Normalize with mean and std
			rf = (rf - p.Config.Mean[0]) / p.Config.Std[0]
			gf = (gf - p.Config.Mean[1]) / p.Config.Std[1]
			bf = (bf - p.Config.Mean[2]) / p.Config.Std[2]

			// Store in NCHW format
			pixels[0*height*width+y*width+x] = rf // R channel
			pixels[1*height*width+y*width+x] = gf // G channel
			pixels[2*height*width+y*width+x] = bf // B channel
		}
	}

	return pixels
}

// centerCrop performs center cropping on an image.
func centerCrop(img image.Image, size int) image.Image {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	// Determine crop dimensions
	cropWidth := size
	cropHeight := size
	if width < cropWidth {
		cropWidth = width
	}
	if height < cropHeight {
		cropHeight = height
	}

	// Calculate crop position (centered)
	left := (width - cropWidth) / 2
	top := (height - cropHeight) / 2

	return cropImage(img, left, top, cropWidth, cropHeight)
}

// cropImage extracts a rectangular region from an image.
func cropImage(img image.Image, x, y, width, height int) image.Image {
	bounds := img.Bounds()
	cropped := image.NewRGBA(image.Rect(0, 0, width, height))

	for dy := range height {
		for dx := range width {
			srcX := bounds.Min.X + x + dx
			srcY := bounds.Min.Y + y + dy
			if srcX < bounds.Max.X && srcY < bounds.Max.Y {
				cropped.Set(dx, dy, img.At(srcX, srcY))
			}
		}
	}

	return cropped
}

// resize performs bilinear interpolation to resize an image.
func resize(img image.Image, targetWidth, targetHeight int) image.Image {
	bounds := img.Bounds()
	srcWidth := bounds.Dx()
	srcHeight := bounds.Dy()

	if srcWidth == targetWidth && srcHeight == targetHeight {
		return img
	}

	resized := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))

	xRatio := float64(srcWidth) / float64(targetWidth)
	yRatio := float64(srcHeight) / float64(targetHeight)

	for y := range targetHeight {
		for x := range targetWidth {
			// Source coordinates (floating point)
			srcX := float64(x) * xRatio
			srcY := float64(y) * yRatio

			// Bilinear interpolation
			c := bilinearInterpolate(img, srcX, srcY, bounds)
			resized.Set(x, y, c)
		}
	}

	return resized
}

// bilinearInterpolate performs bilinear interpolation at floating-point coordinates.
func bilinearInterpolate(img image.Image, x, y float64, bounds image.Rectangle) color.Color {
	x0 := int(x)
	y0 := int(y)
	x1 := x0 + 1
	y1 := y0 + 1

	// Clamp to bounds
	if x0 < bounds.Min.X {
		x0 = bounds.Min.X
	}
	if y0 < bounds.Min.Y {
		y0 = bounds.Min.Y
	}
	if x1 >= bounds.Max.X {
		x1 = bounds.Max.X - 1
	}
	if y1 >= bounds.Max.Y {
		y1 = bounds.Max.Y - 1
	}

	// Get the four corner colors
	c00 := img.At(x0, y0)
	c01 := img.At(x0, y1)
	c10 := img.At(x1, y0)
	c11 := img.At(x1, y1)

	// Calculate interpolation weights
	xWeight := x - float64(x0)
	yWeight := y - float64(y0)

	// Interpolate each channel
	r00, g00, b00, a00 := c00.RGBA()
	r01, g01, b01, a01 := c01.RGBA()
	r10, g10, b10, a10 := c10.RGBA()
	r11, g11, b11, a11 := c11.RGBA()

	r := interpolate(r00, r01, r10, r11, xWeight, yWeight)
	g := interpolate(g00, g01, g10, g11, xWeight, yWeight)
	b := interpolate(b00, b01, b10, b11, xWeight, yWeight)
	a := interpolate(a00, a01, a10, a11, xWeight, yWeight)

	return color.RGBA64{
		R: uint16(r),
		G: uint16(g),
		B: uint16(b),
		A: uint16(a),
	}
}

// interpolate performs bilinear interpolation on a single value.
func interpolate(v00, v01, v10, v11 uint32, xWeight, yWeight float64) float64 {
	// Interpolate along x
	top := float64(v00)*(1-xWeight) + float64(v10)*xWeight
	bottom := float64(v01)*(1-xWeight) + float64(v11)*xWeight

	// Interpolate along y
	return top*(1-yWeight) + bottom*yWeight
}

// PadImage pads an image to the target dimensions with a specified color.
func PadImage(img image.Image, targetWidth, targetHeight int, padColor color.Color) image.Image {
	bounds := img.Bounds()
	srcWidth := bounds.Dx()
	srcHeight := bounds.Dy()

	if srcWidth >= targetWidth && srcHeight >= targetHeight {
		return img
	}

	// Create padded image
	padded := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))

	// Fill with pad color
	for y := range targetHeight {
		for x := range targetWidth {
			padded.Set(x, y, padColor)
		}
	}

	// Copy original image (centered)
	offsetX := (targetWidth - srcWidth) / 2
	offsetY := (targetHeight - srcHeight) / 2

	for y := range srcHeight {
		for x := range srcWidth {
			padded.Set(offsetX+x, offsetY+y, img.At(bounds.Min.X+x, bounds.Min.Y+y))
		}
	}

	return padded
}

// CropMargin removes uniform color margins from an image.
// tolerance specifies how much color variation is allowed in the margin.
func CropMargin(img image.Image, tolerance uint32) image.Image {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	// Get corner color as reference for margin
	refColor := img.At(bounds.Min.X, bounds.Min.Y)
	refR, refG, refB, _ := refColor.RGBA()

	// Find boundaries
	left := 0
	right := width - 1
	top := 0
	bottom := height - 1

	// Find left boundary
	for x := range width {
		hasContent := false
		for y := range height {
			if !isMarginColor(img.At(bounds.Min.X+x, bounds.Min.Y+y), refR, refG, refB, tolerance) {
				hasContent = true
				break
			}
		}
		if hasContent {
			left = x
			break
		}
	}

	// Find right boundary
	for x := width - 1; x >= left; x-- {
		hasContent := false
		for y := range height {
			if !isMarginColor(img.At(bounds.Min.X+x, bounds.Min.Y+y), refR, refG, refB, tolerance) {
				hasContent = true
				break
			}
		}
		if hasContent {
			right = x
			break
		}
	}

	// Find top boundary
	for y := range height {
		hasContent := false
		for x := left; x <= right; x++ {
			if !isMarginColor(img.At(bounds.Min.X+x, bounds.Min.Y+y), refR, refG, refB, tolerance) {
				hasContent = true
				break
			}
		}
		if hasContent {
			top = y
			break
		}
	}

	// Find bottom boundary
	for y := height - 1; y >= top; y-- {
		hasContent := false
		for x := left; x <= right; x++ {
			if !isMarginColor(img.At(bounds.Min.X+x, bounds.Min.Y+y), refR, refG, refB, tolerance) {
				hasContent = true
				break
			}
		}
		if hasContent {
			bottom = y
			break
		}
	}

	// Return cropped image
	return cropImage(img, left, top, right-left+1, bottom-top+1)
}

// isMarginColor checks if a color matches the margin reference within tolerance.
func isMarginColor(c color.Color, refR, refG, refB, tolerance uint32) bool {
	r, g, b, _ := c.RGBA()
	return absDiff(r, refR) <= tolerance &&
		absDiff(g, refG) <= tolerance &&
		absDiff(b, refB) <= tolerance
}

// absDiff returns the absolute difference between two uint32 values.
func absDiff(a, b uint32) uint32 {
	if a > b {
		return a - b
	}
	return b - a
}
