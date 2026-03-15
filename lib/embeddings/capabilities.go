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

package embeddings

import (
	"strings"
)

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

	// SupportsSparse indicates if the embedder can generate sparse (SPLADE) vectors
	SupportsSparse bool `json:"supports_sparse,omitempty"`
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

// KnownModelCapabilities maps model names to their known capabilities.
// This serves as a registry for models we have pre-defined capability information for.
var KnownModelCapabilities = map[string]EmbedderCapabilities{
	// Google Vertex AI models
	"text-embedding-004": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{256, 512, 768},
		DefaultDimension:   768,
	},
	"text-embedding-005": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{256, 512, 768},
		DefaultDimension:   768,
	},
	"textembedding-gecko@003": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"textembedding-gecko@002": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"textembedding-gecko@001": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"textembedding-gecko-multilingual@001": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"text-multilingual-embedding-002": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"multimodalembedding": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "image/png", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/jpeg", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/gif", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/bmp", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/webp", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "video/mp4", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/quicktime", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/x-msvideo", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/x-flv", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/webm", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/mpeg", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/3gpp", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
		},
		Dimensions:       []int{128, 256, 512, 1408},
		DefaultDimension: 1408,
		SupportsFusion:   true,
	},
	"multimodalembedding@001": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "image/png", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/jpeg", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/gif", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/bmp", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/webp", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "video/mp4", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/quicktime", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/x-msvideo", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/x-flv", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/webm", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/mpeg", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
			{MIMEType: "video/3gpp", MaxDurationSec: 60, MaxSizeBytes: 50 * 1024 * 1024},
		},
		Dimensions:       []int{128, 256, 512, 1408},
		DefaultDimension: 1408,
		SupportsFusion:   true,
	},

	// Google Gemini API models
	"gemini-embedding-001": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768, 1536, 3072},
		DefaultDimension:   768,
	},
	"embedding-001": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"text-embedding-004-gemini": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},

	// OpenAI models
	"text-embedding-3-small": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{512, 1536},
		DefaultDimension:   1536,
		MaxBatchSize:       2048,
	},
	"text-embedding-3-large": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{256, 1024, 3072},
		DefaultDimension:   3072,
		MaxBatchSize:       2048,
	},
	"text-embedding-ada-002": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1536},
		DefaultDimension:   1536,
		MaxBatchSize:       2048,
	},

	// AWS Bedrock models
	"amazon.titan-embed-text-v1": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1536},
		DefaultDimension:   1536,
		MaxBatchSize:       100,
	},
	"amazon.titan-embed-text-v2:0": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{256, 512, 1024},
		DefaultDimension:   1024,
		MaxBatchSize:       100,
	},
	"amazon.titan-embed-image-v1": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/png", MaxSizeBytes: 5 * 1024 * 1024, MaxWidth: 2048, MaxHeight: 2048},
			{MIMEType: "image/jpeg", MaxSizeBytes: 5 * 1024 * 1024, MaxWidth: 2048, MaxHeight: 2048},
			{MIMEType: "image/gif", MaxSizeBytes: 5 * 1024 * 1024, MaxWidth: 2048, MaxHeight: 2048},
			{MIMEType: "image/webp", MaxSizeBytes: 5 * 1024 * 1024, MaxWidth: 2048, MaxHeight: 2048},
		},
		Dimensions:       []int{256, 384, 1024},
		DefaultDimension: 1024,
		SupportsFusion:   true,
	},
	"cohere.embed-english-v3": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1024},
		DefaultDimension:   1024,
		MaxBatchSize:       96,
	},
	"cohere.embed-multilingual-v3": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1024},
		DefaultDimension:   1024,
		MaxBatchSize:       96,
	},

	// Antfly clipclap (multimodal: text + image + audio, CLIP-compatible shared space)
	"clipclap": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/png", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/jpeg", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/gif", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/webp", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "audio/wav"},
			{MIMEType: "audio/wave"},
			{MIMEType: "audio/x-wav"},
		},
		Dimensions:       []int{512},
		DefaultDimension: 512,
		SupportsFusion:   false,
	},

	// OpenAI CLIP models (multimodal)
	"clip-vit-base-patch32": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/png", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/jpeg", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/gif", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/webp", MaxSizeBytes: 20 * 1024 * 1024},
		},
		Dimensions:       []int{512},
		DefaultDimension: 512,
		SupportsFusion:   false, // CLIP creates separate embeddings in shared space
	},
	"clip-vit-base-patch16": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/png", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/jpeg", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/gif", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/webp", MaxSizeBytes: 20 * 1024 * 1024},
		},
		Dimensions:       []int{512},
		DefaultDimension: 512,
		SupportsFusion:   false,
	},
	"clip-vit-large-patch14": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/png", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/jpeg", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/gif", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/webp", MaxSizeBytes: 20 * 1024 * 1024},
		},
		Dimensions:       []int{768},
		DefaultDimension: 768,
		SupportsFusion:   false,
	},
	"clip-vit-large-patch14-336": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/png", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/jpeg", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/gif", MaxSizeBytes: 20 * 1024 * 1024},
			{MIMEType: "image/webp", MaxSizeBytes: 20 * 1024 * 1024},
		},
		Dimensions:       []int{768},
		DefaultDimension: 768,
		SupportsFusion:   false,
	},

	// CLAP audio embedding models
	"clap-htsat-unfused": {
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "audio/wav", MaxDurationSec: 10},
			{MIMEType: "audio/wave", MaxDurationSec: 10},
			{MIMEType: "audio/x-wav", MaxDurationSec: 10},
			{MIMEType: "audio/*", MaxDurationSec: 10},
		},
		Dimensions:       []int{512},
		DefaultDimension: 512,
		SupportsFusion:   false,
	},

	// Popular Ollama/local models
	"all-minilm": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{384},
		DefaultDimension:   384,
	},
	"all-minilm:latest": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{384},
		DefaultDimension:   384,
	},
	"nomic-embed-text": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"nomic-embed-text:latest": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{768},
		DefaultDimension:   768,
	},
	"mxbai-embed-large": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1024},
		DefaultDimension:   1024,
	},
	"mxbai-embed-large:latest": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1024},
		DefaultDimension:   1024,
	},
	"bge-m3": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1024},
		DefaultDimension:   1024,
	},
	"bge-m3:latest": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1024},
		DefaultDimension:   1024,
	},
	"snowflake-arctic-embed": {
		SupportedMIMETypes: []MIMETypeSupport{{MIMEType: "text/plain"}},
		Dimensions:         []int{1024},
		DefaultDimension:   1024,
	},
}

// ResolveCapabilities determines capabilities for a model.
// Priority order:
//  1. User-provided config capabilities (if non-nil and has MIME types)
//  2. Known model registry lookup
//  3. Default text-only capabilities
func ResolveCapabilities(model string, configCaps *EmbedderCapabilities) EmbedderCapabilities {
	// 1. User-provided config takes highest priority
	if configCaps != nil && len(configCaps.SupportedMIMETypes) > 0 {
		return *configCaps
	}

	// 2. Check our known models registry
	if caps, ok := KnownModelCapabilities[model]; ok {
		return caps
	}

	// 3. Try stripping provider prefix (e.g., "openai/clip-vit-base-patch32" -> "clip-vit-base-patch32")
	if _, after, ok := strings.Cut(model, "/"); ok {
		baseModel := after
		if caps, ok := KnownModelCapabilities[baseModel]; ok {
			return caps
		}
	}

	// 4. Try stripping version suffix (e.g., "model:latest" -> "model")
	if idx := strings.LastIndex(model, ":"); idx > 0 {
		baseModel := model[:idx]
		if caps, ok := KnownModelCapabilities[baseModel]; ok {
			return caps
		}
	}

	// 5. Try stripping @version suffix (e.g., "model@001" -> "model")
	if idx := strings.LastIndex(model, "@"); idx > 0 {
		baseModel := model[:idx]
		if caps, ok := KnownModelCapabilities[baseModel]; ok {
			return caps
		}
	}

	// 6. Fall back to text-only defaults
	return TextOnlyCapabilities()
}
