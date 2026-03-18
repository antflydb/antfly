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
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"math"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const geminiEmbedding2Model = "gemini-embedding-2-preview"

// testImagePNG returns a small 8x8 red PNG for multimodal tests.
func testImagePNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := range 8 {
		for x := range 8 {
			img.Set(x, y, color.RGBA{R: 255, A: 255})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

func newGeminiEmbedder(t *testing.T) Embedder {
	t.Helper()
	if os.Getenv("GEMINI_API_KEY") == "" {
		t.Skip("GEMINI_API_KEY not set")
	}
	cfg := EmbedderConfig{Provider: EmbedderProviderGemini}
	err := cfg.FromGoogleEmbedderConfig(GoogleEmbedderConfig{Model: geminiEmbedding2Model})
	require.NoError(t, err)
	emb, err := NewGenaiGoogleImpl(cfg)
	require.NoError(t, err)
	return emb
}

// newGeminiVertexEmbedder creates a Gemini embedder using the Vertex AI backend.
// This uses the genai SDK with BackendVertexAI, which is the correct path for
// gemini-embedding-2-preview on Vertex (not the legacy Prediction API).
func newGeminiVertexEmbedder(t *testing.T) Embedder {
	t.Helper()
	project := os.Getenv("VERTEXAI_PROJECT")
	if project == "" {
		project = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	if project == "" {
		t.Skip("VERTEXAI_PROJECT or GOOGLE_CLOUD_PROJECT not set")
	}
	// Set env vars for the genai SDK to use Vertex AI backend
	t.Setenv("GOOGLE_GENAI_USE_VERTEXAI", "1")
	t.Setenv("VERTEXAI_PROJECT", project)
	if os.Getenv("VERTEXAI_LOCATION") == "" {
		t.Setenv("VERTEXAI_LOCATION", "us-central1")
	}

	cfg := EmbedderConfig{Provider: EmbedderProviderGemini}
	err := cfg.FromGoogleEmbedderConfig(GoogleEmbedderConfig{Model: geminiEmbedding2Model})
	require.NoError(t, err)
	emb, err := NewGenaiGoogleImpl(cfg)
	require.NoError(t, err)
	return emb
}

// cosineSimilarity computes cosine similarity between two vectors.
func cosineSimilarity(a, b []float32) float64 {
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func TestGeminiEmbedding2_TextViaGeminiProvider(t *testing.T) {
	emb := newGeminiEmbedder(t)
	ctx := context.Background()

	// Verify capabilities
	caps := emb.Capabilities()
	assert.False(t, caps.IsTextOnly(), "gemini-embedding-2-preview should be multimodal")
	assert.Equal(t, 3072, caps.DefaultDimension)

	// Embed two semantically different texts
	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.TextContent{Text: "The cat sat on the mat"}},
		{ai.TextContent{Text: "Quantum mechanics describes subatomic particles"}},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Len(t, results[0], 3072, "expected 3072 dimensions")
	assert.Len(t, results[1], 3072, "expected 3072 dimensions")

	// Semantically different texts should have lower similarity
	sim := cosineSimilarity(results[0], results[1])
	t.Logf("cosine similarity (different texts): %.4f", sim)
	assert.Less(t, sim, 0.9, "very different texts should not be too similar")
}

func TestGeminiEmbedding2_ImageViaGeminiProvider(t *testing.T) {
	emb := newGeminiEmbedder(t)
	ctx := context.Background()

	imgData := testImagePNG(t)

	// Embed an image
	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.BinaryContent{MIMEType: "image/png", Data: imgData}},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0], 3072, "expected 3072 dimensions")
}

func TestGeminiEmbedding2_FusedTextAndImageViaGeminiProvider(t *testing.T) {
	emb := newGeminiEmbedder(t)
	ctx := context.Background()

	imgData := testImagePNG(t)

	// Embed mixed text+image (fused into single embedding)
	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{
			ai.TextContent{Text: "A red square"},
			ai.BinaryContent{MIMEType: "image/png", Data: imgData},
		},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0], 3072, "expected 3072 dimensions")
}

func TestGeminiEmbedding2_TextViaVertexBackend(t *testing.T) {
	emb := newGeminiVertexEmbedder(t)
	ctx := context.Background()

	caps := emb.Capabilities()
	assert.False(t, caps.IsTextOnly(), "gemini-embedding-2-preview should be multimodal")

	// Vertex backend may require one-at-a-time embedding for this model
	r1, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.TextContent{Text: "The cat sat on the mat"}},
	})
	if err != nil {
		t.Skipf("Vertex backend not available: %v", err)
	}
	require.Len(t, r1, 1)
	assert.Len(t, r1[0], 3072, "expected 3072 dimensions")

	r2, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.TextContent{Text: "Quantum mechanics describes subatomic particles"}},
	})
	require.NoError(t, err)
	require.Len(t, r2, 1)
	assert.Len(t, r2[0], 3072, "expected 3072 dimensions")

	sim := cosineSimilarity(r1[0], r2[0])
	t.Logf("cosine similarity (different texts): %.4f", sim)
	assert.Less(t, sim, 0.9, "very different texts should not be too similar")
}

func TestGeminiEmbedding2_ImageViaVertexBackend(t *testing.T) {
	emb := newGeminiVertexEmbedder(t)
	ctx := context.Background()

	imgData := testImagePNG(t)

	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.BinaryContent{MIMEType: "image/png", Data: imgData}},
	})
	if err != nil {
		t.Skipf("Vertex backend not available: %v", err)
	}
	require.Len(t, results, 1)
	assert.Len(t, results[0], 3072, "expected 3072 dimensions")
}

func TestGeminiEmbedding2_MultimodalConfigOverride(t *testing.T) {
	// Pure logic test — no API calls, no credentials needed
	cfg := NewEmbedderConfigFromJSON("gemini", []byte(`{
		"model": "gemini-embedding-2-preview",
		"multimodal": true
	}`))

	caps := ResolveCapabilities("gemini-embedding-2-preview", cfg.GetConfigCapabilities())
	assert.False(t, caps.IsTextOnly(), "multimodal override should make it multimodal")
	assert.True(t, caps.SupportsMIMEType("image/png"))
}
