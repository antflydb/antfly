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
	"encoding/base64"
	encjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/scraping"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTitanMultimodalBody_TextOnly(t *testing.T) {
	ctx := context.Background()
	body, err := buildTitanMultimodalBody(ctx, []ai.ContentPart{
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
	ctx := context.Background()
	// PNG magic prefix bytes, base64 → "iVBORw=="
	img := []byte{0x89, 0x50, 0x4E, 0x47}
	body, err := buildTitanMultimodalBody(ctx, []ai.ContentPart{
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
	ctx := context.Background()
	img := []byte{0x89, 0x50, 0x4E, 0x47}
	body, err := buildTitanMultimodalBody(ctx, []ai.ContentPart{
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
	ctx := context.Background()
	_, err := buildTitanMultimodalBody(ctx, []ai.ContentPart{
		ai.TextContent{Text: ""},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one")

	_, err = buildTitanMultimodalBody(ctx, []ai.ContentPart{
		ai.BinaryContent{MIMEType: "image/png", Data: nil},
	})
	require.Error(t, err)

	_, err = buildTitanMultimodalBody(ctx, []ai.ContentPart{})
	require.Error(t, err)
}

func TestBuildTitanMultimodalBody_ImageURL_DataURI(t *testing.T) {
	ctx := context.Background()
	// Same 4-byte PNG prefix as the BinaryContent test, encoded as a data URI.
	dataURI := "data:image/png;base64,iVBORw=="
	body, err := buildTitanMultimodalBody(ctx, []ai.ContentPart{
		ai.ImageURLContent{URL: dataURI},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, encjson.Unmarshal(body, &got))
	assert.Equal(t, "iVBORw==", got["inputImage"])
}

func TestBuildTitanMultimodalBody_MultipleImages_Error(t *testing.T) {
	ctx := context.Background()
	img := []byte{0x89, 0x50, 0x4E, 0x47}
	_, err := buildTitanMultimodalBody(ctx, []ai.ContentPart{
		ai.BinaryContent{MIMEType: "image/png", Data: img},
		ai.BinaryContent{MIMEType: "image/png", Data: img},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one image")
}

func TestBuildTitanMultimodalBody_ImageURL_Empty_Ignored(t *testing.T) {
	ctx := context.Background()
	body, err := buildTitanMultimodalBody(ctx, []ai.ContentPart{
		ai.ImageURLContent{URL: ""},
		ai.TextContent{Text: "hello"},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, encjson.Unmarshal(body, &got))
	assert.Equal(t, "hello", got["inputText"])
	_, hasImage := got["inputImage"]
	assert.False(t, hasImage, "empty image URL must be ignored, not produce inputImage")
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

// TestBedrockTitan_ImageViaURL exercises the ImageURLContent → DownloadContent
// → base64 path end-to-end against live Bedrock. Uses a data: URI so the test
// has no external network dependency (data: is resolved locally by lib/scraping).
func TestBedrockTitan_ImageViaURL(t *testing.T) {
	emb := newBedrockTitanMultimodalEmbedder(t)
	ctx := context.Background()

	dataURI := "data:image/png;base64," +
		base64.StdEncoding.EncodeToString(testImagePNG(t))

	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.ImageURLContent{URL: dataURI}},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0], 1024)
}

// TestBedrockTitan_ImageViaHTTPURL exercises the full HTTP code path in
// lib/scraping (vs. the data: fast-path in ImageViaURL) — real socket, real
// Content-Type, real bytes streamed — without an external network dependency.
// Uses httptest.NewServer per the e2e/remote_content_test.go precedent, and
// temporarily relaxes BlockPrivateIps so loopback is reachable.
func TestBedrockTitan_ImageViaHTTPURL(t *testing.T) {
	emb := newBedrockTitanMultimodalEmbedder(t)
	ctx := context.Background()

	pngBytes := testImagePNG(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	}))
	t.Cleanup(srv.Close)

	// httptest binds to 127.0.0.1; the default security config blocks loopback.
	// Save/restore so this test doesn't leak into others.
	prev := scraping.GetDefaultSecurityConfig()
	scraping.SetDefaultSecurityConfig(&scraping.ContentSecurityConfig{
		MaxDownloadSizeBytes:   10 * 1024 * 1024,
		DownloadTimeoutSeconds: 30,
		MaxImageDimension:      2048,
	})
	t.Cleanup(func() { scraping.SetDefaultSecurityConfig(prev) })

	results, err := emb.Embed(ctx, [][]ai.ContentPart{
		{ai.ImageURLContent{URL: srv.URL + "/test.png"}},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Len(t, results[0], 1024)
}
