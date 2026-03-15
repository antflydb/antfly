package reading

import (
	"context"

	"github.com/antflydb/antfly/pkg/libaf/ai"
)

// Reader extracts text from images and single-page PDFs using OCR models.
type Reader interface {
	// Read extracts text from one or more pages. Each page should be a
	// single image (PNG, JPEG) or a single-page PDF as ai.BinaryContent.
	// Returns one string per input page.
	Read(ctx context.Context, pages []ai.BinaryContent, opts *ReadOptions) ([]string, error)

	// Close releases any resources held by the reader (sessions, connections, etc.)
	Close() error
}

// ReadOptions configures a Read call.
type ReadOptions struct {
	// Prompt is a custom extraction prompt (empty = default OCR).
	Prompt string

	// MaxTokens is the max output tokens per page (0 = provider default).
	MaxTokens int
}

// ReadPages is a convenience function that wraps raw page bytes as BinaryContent.
func ReadPages(ctx context.Context, r Reader, pages [][]byte, mimeType string, opts *ReadOptions) ([]string, error) {
	if len(pages) == 0 {
		return []string{}, nil
	}
	contents := make([]ai.BinaryContent, len(pages))
	for i, p := range pages {
		contents[i] = ai.BinaryContent{MIMEType: mimeType, Data: p}
	}
	return r.Read(ctx, contents, opts)
}
