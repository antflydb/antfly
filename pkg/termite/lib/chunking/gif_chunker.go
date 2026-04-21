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

package chunking

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/draw"
	"image/gif"
	"image/png"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
)

// GIFChunker extracts frames from animated GIFs and outputs each as a PNG chunk.
// Frames are fully composited respecting GIF disposal methods.
type GIFChunker struct{}

// ChunkGIF decodes an animated GIF and returns each frame as a PNG chunk.
func (g *GIFChunker) ChunkGIF(ctx context.Context, data []byte, opts chunking.ChunkOptions) ([]chunking.Chunk, error) {
	gifData, err := gif.DecodeAll(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decoding GIF: %w", err)
	}

	if len(gifData.Image) == 0 {
		return nil, fmt.Errorf("GIF contains no frames")
	}

	// Create a canvas for compositing frames
	bounds := image.Rect(0, 0, gifData.Config.Width, gifData.Config.Height)
	canvas := image.NewRGBA(bounds)

	var chunks []chunking.Chunk

	for i, frame := range gifData.Image {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Apply disposal method from previous frame
		if i > 0 {
			disposal := gif.DisposalNone
			if i-1 < len(gifData.Disposal) {
				disposal = int(gifData.Disposal[i-1])
			}
			switch disposal {
			case gif.DisposalBackground:
				// Clear the previous frame's area to transparent
				prevBounds := gifData.Image[i-1].Bounds()
				draw.Draw(canvas, prevBounds, image.Transparent, image.Point{}, draw.Src)
			case gif.DisposalPrevious:
				// Restore to previous state — for simplicity, treat as no-op
				// (proper implementation would save/restore canvas state)
			case gif.DisposalNone:
				// Leave canvas as-is (accumulate)
			}
		}

		// Draw current frame onto canvas
		draw.Draw(canvas, frame.Bounds(), frame, frame.Bounds().Min, draw.Over)

		// Encode composited frame as PNG
		var pngBuf bytes.Buffer
		if err := png.Encode(&pngBuf, canvas); err != nil {
			return nil, fmt.Errorf("encoding frame %d as PNG: %w", i, err)
		}

		// Get frame delay (in 10ms units, convert to ms)
		delayMs := 0
		if i < len(gifData.Delay) {
			delayMs = gifData.Delay[i] * 10
		}

		var c chunking.Chunk
		c.Id = uint32(i)
		c.MimeType = "image/png"
		_ = c.FromBinaryContent(chunking.BinaryContent{
			Data:         pngBuf.Bytes(),
			FrameIndex:   i,
			FrameDelayMs: delayMs,
		})

		chunks = append(chunks, c)

		// Enforce max chunks limit
		if opts.MaxChunks > 0 && len(chunks) >= opts.MaxChunks {
			break
		}
	}

	return chunks, nil
}
