// Copyright 2026 Antfly, Inc.
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
)

// Reader extracts text from images and single-page PDFs using OCR models.
type Reader interface {
	// Read extracts text from one or more pages. Each page should be a
	// single image (PNG, JPEG) or a single-page PDF as BinaryContent.
	// Returns one string per input page.
	Read(ctx context.Context, pages []BinaryContent, opts *ReadOptions) ([]string, error)

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
	contents := make([]BinaryContent, len(pages))
	for i, p := range pages {
		contents[i] = BinaryContent{MIMEType: mimeType, Data: p}
	}
	return r.Read(ctx, contents, opts)
}
