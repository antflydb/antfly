// Copyright 2026 Antfly, Inc.
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
	"context"
	encjson "encoding/json"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTitanMultimodalBody_TextOnly(t *testing.T) {
	body, err := buildTitanMultimodalBody([]ai.ContentPart{
		ai.TextContent{Text: "a portrait painting"},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, encjson.Unmarshal(body, &got))
	assert.Equal(t, "a portrait painting", got["inputText"])
	_, hasImage := got["inputImage"]
	assert.False(t, hasImage, "text-only input must not produce inputImage")
}

func TestBuildTitanMultimodalBody_ImageOnly(t *testing.T) {
	// PNG magic prefix bytes, base64 → "iVBORw=="
	img := []byte{0x89, 0x50, 0x4E, 0x47}
	body, err := buildTitanMultimodalBody([]ai.ContentPart{
		ai.BinaryContent{MIMEType: "image/png", Data: img},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, encjson.Unmarshal(body, &got))
	assert.Equal(t, "iVBORw==", got["inputImage"])
	_, hasText := got["inputText"]
	assert.False(t, hasText, "image-only input must not produce inputText")
}

func TestBuildTitanMultimodalBody_Fused(t *testing.T) {
	img := []byte{0x89, 0x50, 0x4E, 0x47}
	body, err := buildTitanMultimodalBody([]ai.ContentPart{
		ai.TextContent{Text: "a red square"},
		ai.BinaryContent{MIMEType: "image/png", Data: img},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, encjson.Unmarshal(body, &got))
	assert.Equal(t, "a red square", got["inputText"])
	assert.Equal(t, "iVBORw==", got["inputImage"])
}

func TestBuildTitanMultimodalBody_EmptyErrors(t *testing.T) {
	_, err := buildTitanMultimodalBody([]ai.ContentPart{
		ai.TextContent{Text: ""},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one")

	_, err = buildTitanMultimodalBody([]ai.ContentPart{
		ai.BinaryContent{MIMEType: "image/png", Data: nil},
	})
	require.Error(t, err)

	_, err = buildTitanMultimodalBody([]ai.ContentPart{})
	require.Error(t, err)
}

// newBedrockTitanMultimodalEmbedder returns a live Bedrock Titan Multimodal G1
// embedder, or skips the test if AWS credentials / region are not available.
// Mirrors newGeminiEmbedder's skip-when-unconfigured pattern.
func newBedrockTitanMultimodalEmbedder(t *testing.T) Embedder {
	t.Helper()
	if os.Getenv("AWS_REGION") == "" && os.Getenv("AWS_DEFAULT_REGION") == "" {
		t.Skip("AWS_REGION not set")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	cfg := EmbedderConfig{Provider: EmbedderProviderBedrock}
	err := cfg.FromBedrockEmbedderConfig(BedrockEmbedderConfig{
		Model:  titanMultimodalModel,
		Region: &region,
	})
	require.NoError(t, err)
	emb, err := NewBedrockImpl(cfg)
	require.NoError(t, err)
	return emb
}

func TestBedrockTitan_Image(t *testing.T) {
	emb := newBedrockTitanMultimodalEmbedder(t)
	ctx := context.Background()

	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.BinaryContent{MIMEType: "image/png", Data: testImagePNG(t)}},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0], 1024, "Titan G1 default output is 1024 dimensions")
}

func TestBedrockTitan_FusedTextAndImage(t *testing.T) {
	emb := newBedrockTitanMultimodalEmbedder(t)
	ctx := context.Background()

	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{
			ai.TextContent{Text: "A red square"},
			ai.BinaryContent{MIMEType: "image/png", Data: testImagePNG(t)},
		},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0], 1024)
}
