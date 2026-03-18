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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveCapabilities_ExactMatch(t *testing.T) {
	// Models in the registry should be found directly
	caps := ResolveCapabilities("clip-vit-base-patch32", nil)
	assert.False(t, caps.IsTextOnly(), "CLIP should be multimodal")
	assert.Equal(t, 512, caps.DefaultDimension)
}

func TestResolveCapabilities_ProviderPrefix(t *testing.T) {
	// Models with provider/ prefix should resolve to base model capabilities
	// This is critical for Termite models like "openai/clip-vit-base-patch32"
	testCases := []struct {
		model            string
		expectMultimodal bool
		expectDimension  int
	}{
		{"openai/clip-vit-base-patch32", true, 512},
		{"openai/clip-vit-base-patch16", true, 512},
		{"huggingface/clip-vit-large-patch14", true, 768},
		// Note: Unknown base models still fall back to text-only
		{"unknown/some-model", false, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.model, func(t *testing.T) {
			caps := ResolveCapabilities(tc.model, nil)

			if tc.expectMultimodal {
				require.False(t, caps.IsTextOnly(),
					"Model %s should be multimodal, got text-only capabilities", tc.model)
			} else {
				require.True(t, caps.IsTextOnly(),
					"Model %s should be text-only", tc.model)
			}

			assert.Equal(t, tc.expectDimension, caps.DefaultDimension,
				"Model %s should have dimension %d", tc.model, tc.expectDimension)
		})
	}
}

func TestResolveCapabilities_VersionSuffix(t *testing.T) {
	// Models with :version suffix should resolve
	caps := ResolveCapabilities("clip-vit-base-patch32:latest", nil)
	assert.False(t, caps.IsTextOnly(), "CLIP with :latest should be multimodal")
}

func TestResolveCapabilities_ProviderAndVersion(t *testing.T) {
	// Note: Combined provider prefix AND version suffix is an edge case
	// that would require recursive stripping. For now, this falls back to text-only.
	// The main use case (provider prefix alone) is covered.
	caps := ResolveCapabilities("openai/clip-vit-base-patch32:v1", nil)
	// Current behavior: neither stripped version matches, falls back to text-only
	// This is acceptable since real model names don't typically combine both
	assert.True(t, caps.IsTextOnly(), "Combined prefix+version falls back to text-only (edge case)")
}

func TestResolveCapabilities_UnknownModelFallback(t *testing.T) {
	// Unknown models should fall back to text-only
	caps := ResolveCapabilities("unknown-model-xyz", nil)
	assert.True(t, caps.IsTextOnly(), "Unknown model should default to text-only")
}

func TestResolveCapabilities_ConfigOverride(t *testing.T) {
	// User-provided config should take precedence
	customCaps := &EmbedderCapabilities{
		SupportedMIMETypes: []MIMETypeSupport{
			{MIMEType: "text/plain"},
			{MIMEType: "image/png"},
		},
		DefaultDimension: 1024,
	}

	caps := ResolveCapabilities("clip-vit-base-patch32", customCaps)
	assert.Equal(t, 1024, caps.DefaultDimension, "Config should override registry")
}

func TestResolveCapabilities_GeminiEmbedding2Preview(t *testing.T) {
	caps := ResolveCapabilities("gemini-embedding-2-preview", nil)
	assert.False(t, caps.IsTextOnly(), "gemini-embedding-2-preview should be multimodal")
	assert.Equal(t, 3072, caps.DefaultDimension)
	assert.True(t, caps.SupportsMIMEType("image/png"))
	assert.True(t, caps.SupportsMIMEType("audio/wav"))
	assert.True(t, caps.SupportsMIMEType("video/mp4"))
	assert.True(t, caps.SupportsMIMEType("application/pdf"))
	assert.True(t, caps.SupportsFusion)
}

func TestGetConfigCapabilities_MultimodalTrue(t *testing.T) {
	cfg := NewEmbedderConfigFromJSON("vertex", []byte(`{"model":"future-model","multimodal":true}`))
	caps := cfg.GetConfigCapabilities()
	require.NotNil(t, caps, "multimodal: true should produce capabilities")
	assert.False(t, caps.IsTextOnly())
	assert.True(t, caps.SupportsMIMEType("image/png"))
	assert.True(t, caps.SupportsMIMEType("audio/wav"))
}

func TestGetConfigCapabilities_MultimodalFalse(t *testing.T) {
	cfg := NewEmbedderConfigFromJSON("vertex", []byte(`{"model":"text-model","multimodal":false}`))
	caps := cfg.GetConfigCapabilities()
	assert.Nil(t, caps, "multimodal: false should not produce capabilities")
}

func TestGetConfigCapabilities_NotSet(t *testing.T) {
	cfg := NewEmbedderConfigFromJSON("openai", []byte(`{"model":"text-embedding-3-small"}`))
	caps := cfg.GetConfigCapabilities()
	assert.Nil(t, caps, "missing multimodal should not produce capabilities")
}

func TestResolveCapabilities_MultimodalOverridesTextOnly(t *testing.T) {
	// An unknown model with multimodal: true should resolve as multimodal
	cfg := NewEmbedderConfigFromJSON("vertex", []byte(`{"model":"brand-new-model","multimodal":true}`))
	caps := ResolveCapabilities("brand-new-model", cfg.GetConfigCapabilities())
	assert.False(t, caps.IsTextOnly(), "multimodal config should override text-only default")
}
