// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package chunking

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/gif"
	"math"
	"strings"
	"testing"

	libafchunking "github.com/antflydb/antfly/pkg/libaf/chunking"
	termaudio "github.com/antflydb/termite/pkg/termite/lib/audio"
)

func TestAntflyProvider_NewChunkerConfig(t *testing.T) {
	targetTokens := 500
	overlapTokens := 50
	maxChunks := 10

	config := AntflyChunkerConfig{
		MaxChunks: maxChunks,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  targetTokens,
			OverlapTokens: overlapTokens,
		},
	}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	if chunkerConfig.Provider != ChunkerProviderAntfly {
		t.Errorf("Provider = %v, want %v", chunkerConfig.Provider, ChunkerProviderAntfly)
	}
}

func TestAntflyProvider_GetProviderConfig(t *testing.T) {
	targetTokens := 500
	overlapTokens := 50
	maxChunks := 10

	antflyConfig := AntflyChunkerConfig{
		MaxChunks: maxChunks,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  targetTokens,
			OverlapTokens: overlapTokens,
		},
	}

	chunkerConfig, err := NewChunkerConfig(antflyConfig)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	got, err := GetProviderConfig(*chunkerConfig)
	if err != nil {
		t.Fatalf("GetProviderConfig() failed: %v", err)
	}

	extracted, ok := got.(AntflyChunkerConfig)
	if !ok {
		t.Fatalf("GetProviderConfig() returned wrong type: %T", got)
	}

	if extracted.Text.TargetTokens != 500 {
		t.Errorf("TargetTokens = %v, want 500", extracted.Text.TargetTokens)
	}
	if extracted.Text.OverlapTokens != 50 {
		t.Errorf("OverlapTokens = %v, want 50", extracted.Text.OverlapTokens)
	}
	if extracted.MaxChunks != 10 {
		t.Errorf("MaxChunks = %v, want 10", extracted.MaxChunks)
	}
}

func TestAntflyProvider_RoundTrip(t *testing.T) {
	targetTokens := 1000
	maxChunks := 100
	separator := "\n\n"

	original := AntflyChunkerConfig{
		MaxChunks: maxChunks,
		Text: libafchunking.TextChunkOptions{
			TargetTokens: targetTokens,
			Separator:    separator,
		},
	}

	unified, err := NewChunkerConfig(original)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	extracted, err := GetProviderConfig(*unified)
	if err != nil {
		t.Fatalf("GetProviderConfig() failed: %v", err)
	}

	antfly, ok := extracted.(AntflyChunkerConfig)
	if !ok {
		t.Fatalf("GetProviderConfig() returned wrong type: %T", extracted)
	}

	if antfly.Text.TargetTokens != original.Text.TargetTokens {
		t.Errorf("TargetTokens mismatch: got %v, want %v", antfly.Text.TargetTokens, original.Text.TargetTokens)
	}
	if antfly.MaxChunks != original.MaxChunks {
		t.Errorf("MaxChunks mismatch: got %v, want %v", antfly.MaxChunks, original.MaxChunks)
	}
	if antfly.Text.Separator != original.Text.Separator {
		t.Errorf("Separator mismatch: got %v, want %v", antfly.Text.Separator, original.Text.Separator)
	}
}

func TestAntflyProvider_Chunk(t *testing.T) {
	targetTokens := 100
	overlapTokens := 10
	maxChunks := 10

	config := AntflyChunkerConfig{
		MaxChunks: maxChunks,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  targetTokens,
			OverlapTokens: overlapTokens,
		},
	}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		t.Fatalf("NewChunker() failed: %v", err)
	}
	defer chunker.Close()

	testText := `This is the first paragraph with some content.
It spans multiple lines and contains important information.

This is the second paragraph with more content.
It also spans multiple lines and contains different information.

This is the third paragraph with even more content.
It continues to span multiple lines with additional details.`

	chunks, err := chunker.Chunk(context.Background(), testText)
	if err != nil {
		t.Fatalf("Chunk() failed: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatal("Expected at least one chunk, got zero")
	}

	// Verify chunks have sequential IDs
	for i, chunk := range chunks {
		if chunk.Id != uint32(i) {
			t.Errorf("Chunk %d has ID %d, want %d", i, chunk.Id, i)
		}
		if chunk.GetText() == "" {
			t.Errorf("Chunk %d has empty text", i)
		}
		tc, err := chunk.AsTextContent()
		if err != nil {
			t.Errorf("Chunk %d is not text content: %v", i, err)
			continue
		}
		if tc.StartChar >= tc.EndChar {
			t.Errorf("Chunk %d has invalid char range: [%d, %d)", i, tc.StartChar, tc.EndChar)
		}
	}
}

func TestAntflyProvider_EmptyText(t *testing.T) {
	config := AntflyChunkerConfig{}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		t.Fatalf("NewChunker() failed: %v", err)
	}
	defer chunker.Close()

	chunks, err := chunker.Chunk(context.Background(), "")
	if err != nil {
		t.Fatalf("Chunk() failed: %v", err)
	}

	if chunks != nil {
		t.Errorf("Expected nil chunks for empty text, got %v", chunks)
	}
}

func TestAntflyProvider_Defaults(t *testing.T) {
	// Test that defaults are properly applied when not specified
	config := AntflyChunkerConfig{}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		t.Fatalf("NewChunker() failed: %v", err)
	}
	defer chunker.Close()

	// Should use default values and not error
	testText := "This is a simple test with default configuration."
	chunks, err := chunker.Chunk(context.Background(), testText)
	if err != nil {
		t.Fatalf("Chunk() failed with defaults: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatal("Expected at least one chunk with defaults, got zero")
	}
}

func TestAntflyProvider_ChunkMedia_WAV(t *testing.T) {
	config := AntflyChunkerConfig{
		MaxChunks: 10,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  500,
			OverlapTokens: 50,
		},
	}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		t.Fatalf("NewChunker() failed: %v", err)
	}
	defer chunker.Close()

	// Generate test WAV data: 1 second of 440Hz sine wave at 16kHz
	samples := make([]float32, 16000)
	for i := range samples {
		samples[i] = float32(math.Sin(2.0 * math.Pi * 440.0 * float64(i) / 16000.0))
	}
	wavData, err := termaudio.EncodeWAV(samples, termaudio.Format{
		SampleRate:    16000,
		BitsPerSample: 16,
		NumChannels:   1,
	})
	if err != nil {
		t.Fatalf("EncodeWAV() failed: %v", err)
	}

	chunks, err := chunker.ChunkMedia(context.Background(), wavData, "audio/wav")
	if err != nil {
		t.Fatalf("ChunkMedia() failed: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatal("Expected at least one chunk, got zero")
	}

	for i, chunk := range chunks {
		if chunk.Id != uint32(i) {
			t.Errorf("Chunk %d has ID %d, want %d", i, chunk.Id, i)
		}
		if chunk.MimeType != "audio/wav" {
			t.Errorf("Chunk %d has mime_type %q, want %q", i, chunk.MimeType, "audio/wav")
		}
		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Errorf("Chunk %d is not binary content: %v", i, err)
			continue
		}
		if len(bc.Data) == 0 {
			t.Errorf("Chunk %d has empty binary data", i)
		}
	}
}

func TestAntflyProvider_ChunkMedia_GIF(t *testing.T) {
	config := AntflyChunkerConfig{
		MaxChunks: 10,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  500,
			OverlapTokens: 50,
		},
	}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		t.Fatalf("NewChunker() failed: %v", err)
	}
	defer chunker.Close()

	// Generate a minimal test GIF with 2 frames
	g := &gif.GIF{
		Config: image.Config{Width: 2, Height: 2},
	}
	img1 := image.NewPaletted(image.Rect(0, 0, 2, 2), color.Palette{color.White, color.Black})
	img2 := image.NewPaletted(image.Rect(0, 0, 2, 2), color.Palette{color.White, color.Black})
	img2.SetColorIndex(0, 0, 1) // Make second frame different
	g.Image = append(g.Image, img1, img2)
	g.Delay = append(g.Delay, 10, 10)
	g.Disposal = append(g.Disposal, gif.DisposalNone, gif.DisposalNone)

	var buf bytes.Buffer
	if err := gif.EncodeAll(&buf, g); err != nil {
		t.Fatalf("gif.EncodeAll() failed: %v", err)
	}
	gifData := buf.Bytes()

	chunks, err := chunker.ChunkMedia(context.Background(), gifData, "image/gif")
	if err != nil {
		t.Fatalf("ChunkMedia() failed: %v", err)
	}

	if len(chunks) != 2 {
		t.Fatalf("Expected 2 chunks (one per GIF frame), got %d", len(chunks))
	}

	for i, chunk := range chunks {
		if chunk.Id != uint32(i) {
			t.Errorf("Chunk %d has ID %d, want %d", i, chunk.Id, i)
		}
		// GIF frames are converted to PNG
		if chunk.MimeType != "image/png" {
			t.Errorf("Chunk %d has mime_type %q, want %q", i, chunk.MimeType, "image/png")
		}
		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Errorf("Chunk %d is not binary content: %v", i, err)
			continue
		}
		if len(bc.Data) == 0 {
			t.Errorf("Chunk %d has empty binary data", i)
		}
	}
}

func TestAntflyProvider_ChunkMedia_EmptyData(t *testing.T) {
	config := AntflyChunkerConfig{
		MaxChunks: 10,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  500,
			OverlapTokens: 50,
		},
	}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		t.Fatalf("NewChunker() failed: %v", err)
	}
	defer chunker.Close()

	_, err = chunker.ChunkMedia(context.Background(), []byte{}, "audio/wav")
	if err == nil {
		t.Fatal("Expected error for empty data, got nil")
	}
}

func TestAntflyProvider_ChunkMedia_UnsupportedType(t *testing.T) {
	config := AntflyChunkerConfig{
		MaxChunks: 10,
		Text: libafchunking.TextChunkOptions{
			TargetTokens:  500,
			OverlapTokens: 50,
		},
	}

	chunkerConfig, err := NewChunkerConfig(config)
	if err != nil {
		t.Fatalf("NewChunkerConfig() failed: %v", err)
	}

	chunker, err := NewChunker(*chunkerConfig)
	if err != nil {
		t.Fatalf("NewChunker() failed: %v", err)
	}
	defer chunker.Close()

	_, err = chunker.ChunkMedia(context.Background(), []byte("data"), "video/mp4")
	if err == nil {
		t.Fatal("Expected error for unsupported media type, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("Error should contain 'unsupported', got: %v", err)
	}
}
