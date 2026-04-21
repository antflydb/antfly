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
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/ai"
)

func TestDecodeImage_JPEG(t *testing.T) {
	data := encodeTestJPEG(t, 8, 8)

	img, err := decodeImage(data)
	if err != nil {
		t.Fatalf("decodeImage(jpeg) error: %v", err)
	}
	bounds := img.Bounds()
	if bounds.Dx() != 8 || bounds.Dy() != 8 {
		t.Errorf("got %dx%d, want 8x8", bounds.Dx(), bounds.Dy())
	}
}

func TestDecodeImage_PNG(t *testing.T) {
	data := encodeTestPNG(t, 4, 4)

	img, err := decodeImage(data)
	if err != nil {
		t.Fatalf("decodeImage(png) error: %v", err)
	}
	bounds := img.Bounds()
	if bounds.Dx() != 4 || bounds.Dy() != 4 {
		t.Errorf("got %dx%d, want 4x4", bounds.Dx(), bounds.Dy())
	}
}

func TestDecodeImage_GIF(t *testing.T) {
	data := encodeTestGIF(t, 4, 4)

	img, err := decodeImage(data)
	if err != nil {
		t.Fatalf("decodeImage(gif) error: %v", err)
	}
	bounds := img.Bounds()
	if bounds.Dx() != 4 || bounds.Dy() != 4 {
		t.Errorf("got %dx%d, want 4x4", bounds.Dx(), bounds.Dy())
	}
}

func TestDecodeImage_InvalidData(t *testing.T) {
	_, err := decodeImage([]byte("not an image"))
	if err == nil {
		t.Fatal("expected error for invalid data, got nil")
	}
}

// TestDecodeImage_JPEG_NonStandardSubsampling verifies that JPEGs with
// non-standard chroma subsampling (h=3 or v=3) are decoded via the
// flexjpeg fallback. The stdlib rejects these; our patched fork handles
// them through its flex decode path.
func TestDecodeImage_JPEG_NonStandardSubsampling(t *testing.T) {
	data := encodeTestJPEG(t, 24, 24)

	// Patch the SOF0 marker to set luma sampling factor to h=3,v=1.
	// SOF0 = 0xFF 0xC0, followed by length(2), precision(1), height(2),
	// width(2), nComponents(1), then per component: id(1), hv(1), tq(1).
	// The luma component is first; its hv byte is at offset 7 within the
	// SOF0 payload (after the 2-byte marker).
	patched := patchJPEGSubsampling(t, data, 0x31) // h=3, v=1

	// stdlib should reject this
	_, _, err := image.Decode(bytes.NewReader(patched))
	if err == nil {
		t.Skip("stdlib accepted h=3 subsampling (Go 1.27+?), fallback test not needed")
	}

	// Our decodeImage should succeed via the flexjpeg fallback
	img, err := decodeImage(patched)
	if err != nil {
		t.Fatalf("decodeImage failed for h=3 subsampling: %v", err)
	}
	bounds := img.Bounds()
	if bounds.Dx() != 24 || bounds.Dy() != 24 {
		t.Errorf("got %dx%d, want 24x24", bounds.Dx(), bounds.Dy())
	}
}

// patchJPEGSubsampling finds the SOF0 marker in a JPEG and replaces the
// luma component's sampling factor byte with the given value.
func patchJPEGSubsampling(t *testing.T, data []byte, lumaHV byte) []byte {
	t.Helper()
	patched := make([]byte, len(data))
	copy(patched, data)

	// Scan for SOF0 marker (0xFF 0xC0)
	for i := 0; i < len(patched)-1; i++ {
		if patched[i] == 0xFF && patched[i+1] == 0xC0 {
			// SOF0 payload starts at i+2 (length field)
			// Component data starts at i+2+2+1+2+2+1 = i+10
			// First component's hv byte is at i+10+1 = i+11
			if i+11 >= len(patched) {
				t.Fatal("SOF0 marker too close to end of data")
			}
			patched[i+11] = lumaHV
			return patched
		}
	}
	t.Fatal("SOF0 marker not found in JPEG data")
	return nil
}

func TestExtractContent_GIF(t *testing.T) {
	data := encodeTestGIF(t, 4, 4)

	parts := []ai.ContentPart{
		ai.BinaryContent{MIMEType: "image/gif", Data: data},
	}

	text, img, audio, err := extractContent(parts)
	if err != nil {
		t.Fatalf("extractContent(gif) error: %v", err)
	}
	if text != "" {
		t.Error("expected empty text")
	}
	if img == nil {
		t.Fatal("expected non-nil image")
	}
	if audio != nil {
		t.Error("expected nil audio")
	}
	bounds := img.Bounds()
	if bounds.Dx() != 4 || bounds.Dy() != 4 {
		t.Errorf("got %dx%d, want 4x4", bounds.Dx(), bounds.Dy())
	}
}

func TestExtractContent_TextPreferred(t *testing.T) {
	parts := []ai.ContentPart{
		ai.TextContent{Text: "hello world"},
	}

	text, img, audio, err := extractContent(parts)
	if err != nil {
		t.Fatalf("extractContent error: %v", err)
	}
	if text != "hello world" {
		t.Errorf("got text %q, want %q", text, "hello world")
	}
	if img != nil {
		t.Error("expected nil image")
	}
	if audio != nil {
		t.Error("expected nil audio")
	}
}

func testImage(w, h int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			img.Set(x, y, color.RGBA{R: uint8(x * 32), G: uint8(y * 32), B: 128, A: 255})
		}
	}
	return img
}

func encodeTestJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, testImage(w, h), nil); err != nil {
		t.Fatalf("encoding test JPEG: %v", err)
	}
	return buf.Bytes()
}

func encodeTestPNG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, testImage(w, h)); err != nil {
		t.Fatalf("encoding test PNG: %v", err)
	}
	return buf.Bytes()
}

func encodeTestGIF(t *testing.T, w, h int) []byte {
	t.Helper()
	img := testImage(w, h)
	palettedImg := image.NewPaletted(img.Bounds(), color.Palette{
		color.Black, color.White,
		color.RGBA{R: 255, A: 255}, color.RGBA{G: 255, A: 255},
		color.RGBA{B: 255, A: 255}, color.RGBA{R: 128, G: 128, B: 128, A: 255},
	})
	for y := range h {
		for x := range w {
			palettedImg.Set(x, y, img.At(x, y))
		}
	}
	var buf bytes.Buffer
	if err := gif.Encode(&buf, palettedImg, nil); err != nil {
		t.Fatalf("encoding test GIF: %v", err)
	}
	return buf.Bytes()
}
