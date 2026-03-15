package embeddings

import (
	"context"
	"strings"

	"github.com/antflydb/antfly/pkg/libaf/ai"
)

// Embedder is the core interface for generating embeddings.
type Embedder interface {
	// Capabilities returns what this embedder supports (MIME types, dimensions, etc.)
	Capabilities() EmbedderCapabilities

	// Embed generates embeddings for content.
	// Each []ContentPart represents one document (can be text, image, mixed, etc.)
	// Returns one embedding vector per input document.
	Embed(ctx context.Context, contents [][]ai.ContentPart) ([][]float32, error)
}

// EmbedText is a convenience function for text-only embedding.
func EmbedText(ctx context.Context, e Embedder, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	contents := make([][]ai.ContentPart, len(texts))
	for i, text := range texts {
		contents[i] = []ai.ContentPart{ai.TextContent{Text: text}}
	}
	return e.Embed(ctx, contents)
}

// MIMETypeSupport describes support for a specific MIME type with optional constraints.
type MIMETypeSupport struct {
	// MIMEType is the MIME type (e.g., "text/plain", "image/png", "image/*")
	MIMEType string `json:"mime_type"`

	// MaxSizeBytes is the maximum file size in bytes (0 = unlimited/unknown)
	MaxSizeBytes int64 `json:"max_size_bytes,omitempty"`

	// MaxWidth is the maximum width for images/video (0 = unlimited/unknown)
	MaxWidth int `json:"max_width,omitempty"`

	// MaxHeight is the maximum height for images/video (0 = unlimited/unknown)
	MaxHeight int `json:"max_height,omitempty"`

	// MaxDurationSec is the maximum duration for audio/video in seconds (0 = unlimited/unknown)
	MaxDurationSec float64 `json:"max_duration_sec,omitempty"`
}

// EmbedderCapabilities describes what an embedder supports.
type EmbedderCapabilities struct {
	// SupportedMIMETypes lists all MIME types this embedder can process.
	// Text embedders should include "text/plain".
	SupportedMIMETypes []MIMETypeSupport `json:"supported_mime_types,omitempty"`

	// Dimensions lists available output dimensions (empty = fixed/unknown)
	Dimensions []int `json:"dimensions,omitempty"`

	// DefaultDimension is the default output dimension (0 = unknown)
	DefaultDimension int `json:"default_dimension,omitempty"`

	// MaxBatchSize is the maximum items per request (0 = unlimited/unknown)
	MaxBatchSize int `json:"max_batch_size,omitempty"`

	// SupportsFusion indicates if mixed content (text+image) can be
	// fused into a single embedding vector
	SupportsFusion bool `json:"supports_fusion,omitempty"`

	// SupportsURLs indicates if the embedder can fetch content from URLs directly
	SupportsURLs bool `json:"supports_urls,omitempty"`
}

// SupportsMIMEType checks if a specific MIME type is supported.
// Supports exact matches and wildcard patterns like "image/*".
func (c EmbedderCapabilities) SupportsMIMEType(mimeType string) bool {
	for _, s := range c.SupportedMIMETypes {
		if s.MIMEType == mimeType {
			return true
		}
		// Handle wildcards like "image/*"
		if strings.HasSuffix(s.MIMEType, "/*") {
			prefix := strings.TrimSuffix(s.MIMEType, "*")
			if strings.HasPrefix(mimeType, prefix) {
				return true
			}
		}
	}
	return false
}

// GetMIMETypeSupport returns the support details for a specific MIME type, if supported.
func (c EmbedderCapabilities) GetMIMETypeSupport(mimeType string) (MIMETypeSupport, bool) {
	for _, s := range c.SupportedMIMETypes {
		if s.MIMEType == mimeType {
			return s, true
		}
		// Handle wildcards like "image/*"
		if strings.HasSuffix(s.MIMEType, "/*") {
			prefix := strings.TrimSuffix(s.MIMEType, "*")
			if strings.HasPrefix(mimeType, prefix) {
				return s, true
			}
		}
	}
	return MIMETypeSupport{}, false
}

// SupportsModality checks if the embedder supports a broad modality category.
// prefix should be like "image/", "audio/", "video/", or "text/".
func (c EmbedderCapabilities) SupportsModality(prefix string) bool {
	for _, s := range c.SupportedMIMETypes {
		if strings.HasPrefix(s.MIMEType, prefix) {
			return true
		}
	}
	return false
}

// IsTextOnly returns true if the embedder only supports text.
func (c EmbedderCapabilities) IsTextOnly() bool {
	for _, s := range c.SupportedMIMETypes {
		if !strings.HasPrefix(s.MIMEType, "text/") {
			return false
		}
	}
	return true
}

// IsMultimodal returns true if the embedder supports non-text content.
func (c EmbedderCapabilities) IsMultimodal() bool {
	return !c.IsTextOnly()
}

// TextOnlyCapabilities returns a basic text-only capability set.
func TextOnlyCapabilities() EmbedderCapabilities {
	return EmbedderCapabilities{
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
		},
	}
}

// ExtractText extracts text from ContentPart slices for text-only embedders.
// It prefers TextContent but falls back to ImageURLContent URL as text if no text found.
func ExtractText(contents [][]ai.ContentPart) []string {
	texts := make([]string, len(contents))
	for i, parts := range contents {
		for _, part := range parts {
			if tc, ok := part.(ai.TextContent); ok {
				texts[i] = tc.Text
				break
			}
		}
		// If no text content found, fall back to URL content as text
		if texts[i] == "" {
			for _, part := range parts {
				if uc, ok := part.(ai.ImageURLContent); ok {
					texts[i] = uc.URL
					break
				}
			}
		}
	}
	return texts
}
