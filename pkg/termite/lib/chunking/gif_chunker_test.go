package chunking

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
)

// generateTestGIF creates an animated GIF with the specified number of frames and dimensions.
// Each frame has a distinct color to make frames distinguishable.
func generateTestGIF(t *testing.T, numFrames int, width, height int) []byte {
	t.Helper()
	g := &gif.GIF{}
	for i := range numFrames {
		img := image.NewPaletted(image.Rect(0, 0, width, height), color.Palette{
			color.RGBA{uint8(i * 50), 0, 0, 255},
			color.RGBA{0, uint8(i * 50), 0, 255},
		})
		// Fill with first color
		for y := range height {
			for x := range width {
				img.SetColorIndex(x, y, 0)
			}
		}
		g.Image = append(g.Image, img)
		g.Delay = append(g.Delay, 10) // 100ms delay
		g.Disposal = append(g.Disposal, gif.DisposalNone)
	}
	g.Config = image.Config{Width: width, Height: height}

	var buf bytes.Buffer
	if err := gif.EncodeAll(&buf, g); err != nil {
		t.Fatalf("encoding test GIF: %v", err)
	}
	return buf.Bytes()
}

func TestGIFChunker_MultiFrame(t *testing.T) {
	gifData := generateTestGIF(t, 5, 32, 32)
	chunker := &GIFChunker{}

	chunks, err := chunker.ChunkGIF(context.Background(), gifData, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("ChunkGIF returned error: %v", err)
	}

	if len(chunks) != 5 {
		t.Fatalf("expected 5 chunks, got %d", len(chunks))
	}

	for i, chunk := range chunks {
		if chunk.MimeType != "image/png" {
			t.Errorf("chunk %d: expected mime_type %q, got %q", i, "image/png", chunk.MimeType)
		}
		if chunk.Id != uint32(i) {
			t.Errorf("chunk %d: expected id %d, got %d", i, i, chunk.Id)
		}

		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Fatalf("chunk %d: AsBinaryContent error: %v", i, err)
		}

		if bc.FrameIndex != i {
			t.Errorf("chunk %d: expected frame_index %d, got %d", i, i, bc.FrameIndex)
		}
		if bc.FrameDelayMs != 100 {
			t.Errorf("chunk %d: expected frame_delay_ms 100, got %d", i, bc.FrameDelayMs)
		}
		if len(bc.Data) == 0 {
			t.Errorf("chunk %d: expected non-empty data", i)
		}
	}
}

func TestGIFChunker_SingleFrame(t *testing.T) {
	gifData := generateTestGIF(t, 1, 16, 16)
	chunker := &GIFChunker{}

	chunks, err := chunker.ChunkGIF(context.Background(), gifData, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("ChunkGIF returned error: %v", err)
	}

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	if chunks[0].MimeType != "image/png" {
		t.Errorf("expected mime_type %q, got %q", "image/png", chunks[0].MimeType)
	}

	bc, err := chunks[0].AsBinaryContent()
	if err != nil {
		t.Fatalf("AsBinaryContent error: %v", err)
	}
	if bc.FrameIndex != 0 {
		t.Errorf("expected frame_index 0, got %d", bc.FrameIndex)
	}
}

func TestGIFChunker_MaxChunks(t *testing.T) {
	gifData := generateTestGIF(t, 10, 16, 16)
	chunker := &GIFChunker{}

	chunks, err := chunker.ChunkGIF(context.Background(), gifData, chunking.ChunkOptions{
		MaxChunks: 3,
	})
	if err != nil {
		t.Fatalf("ChunkGIF returned error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks (max_chunks=3), got %d", len(chunks))
	}

	// Verify the chunks are the first 3 frames
	for i, chunk := range chunks {
		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Fatalf("chunk %d: AsBinaryContent error: %v", i, err)
		}
		if bc.FrameIndex != i {
			t.Errorf("chunk %d: expected frame_index %d, got %d", i, i, bc.FrameIndex)
		}
	}
}

func TestGIFChunker_RoundtripPNG(t *testing.T) {
	width, height := 24, 24
	gifData := generateTestGIF(t, 3, width, height)
	chunker := &GIFChunker{}

	chunks, err := chunker.ChunkGIF(context.Background(), gifData, chunking.ChunkOptions{})
	if err != nil {
		t.Fatalf("ChunkGIF returned error: %v", err)
	}

	for i, chunk := range chunks {
		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Fatalf("chunk %d: AsBinaryContent error: %v", i, err)
		}

		img, err := png.Decode(bytes.NewReader(bc.Data))
		if err != nil {
			t.Fatalf("chunk %d: png.Decode error: %v", i, err)
		}

		bounds := img.Bounds()
		gotWidth := bounds.Max.X - bounds.Min.X
		gotHeight := bounds.Max.Y - bounds.Min.Y

		if gotWidth != width {
			t.Errorf("chunk %d: expected PNG width %d, got %d", i, width, gotWidth)
		}
		if gotHeight != height {
			t.Errorf("chunk %d: expected PNG height %d, got %d", i, height, gotHeight)
		}
	}
}

func TestGIFChunker_EmptyInput(t *testing.T) {
	chunker := &GIFChunker{}

	_, err := chunker.ChunkGIF(context.Background(), []byte{}, chunking.ChunkOptions{})
	if err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
}

func TestGIFChunker_InvalidGIF(t *testing.T) {
	chunker := &GIFChunker{}

	_, err := chunker.ChunkGIF(context.Background(), []byte("not a gif at all"), chunking.ChunkOptions{})
	if err == nil {
		t.Fatal("expected error for invalid GIF data, got nil")
	}
}
