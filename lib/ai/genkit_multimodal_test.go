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

package ai

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSummarizeDocsWithSchemaAwareLinkProcessing tests the new schema-aware link processing
func TestSummarizeDocsWithSchemaAwareLinkProcessing(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	// Create a mock HTTP server that serves different content types
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/image.png":
			// Serve a small 1x1 PNG image
			pngData, _ := base64.StdEncoding.DecodeString(
				"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
			)
			w.Header().Set("Content-Type", "image/png")
			if _, err := w.Write(pngData); err != nil {
				t.Fatalf("Failed to write PNG data: %v", err)
			}
		case "/article.html":
			// Serve a simple HTML article
			w.Header().Set("Content-Type", "text/html")
			if _, err := w.Write(
				[]byte(
					`<html><body><article><h1>Test Article</h1><p>This is a test article about AI.</p></article></body></html>`,
				),
			); err != nil {
				t.Fatalf("Failed to write HTML data: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	t.Run("processes link fields marked in schema", func(t *testing.T) {
		// Create document with link fields
		docs := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"_type":       "article",
					"title":       "AI Research",
					"image_url":   server.URL + "/image.png",
					"article_url": server.URL + "/article.html",
					"author":      "John Doe",
				},
			},
		}

		// Render and summarize
		rendered := renderDocuments(docs)
		summaries, err := summarizer.SummarizeRenderedDocs(context.Background(), rendered)
		require.NoError(t, err)
		require.Len(t, summaries, 1)

		// Verify we got a summary
		assert.NotEmpty(t, summaries[0])
		assert.NotEqual(t, "No content to summarize", summaries[0])
	})

	// Note: Template-based field filtering test removed
	// ProcessDocumentLinks now processes all link fields (referencedFields parameter available for optimization)

	t.Run("works without schema (backward compatibility)", func(t *testing.T) {
		// Without schema, should work with basic text content
		docs := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "AI Research",
					"content": "This is a research paper about AI.",
				},
			},
		}

		rendered := renderDocuments(docs)
		summaries, err := summarizer.SummarizeRenderedDocs(context.Background(), rendered)
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.NotEmpty(t, summaries[0])
	})

	// Duplicate test removed

	t.Run("uses custom prompt option", func(t *testing.T) {
		docs := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "AI Research",
					"content": "This is about machine learning.",
				},
			},
		}

		customPrompt := `{{this}}

Summarize the above in exactly 5 words.`

		rendered := renderDocuments(docs)
		summaries, err := summarizer.SummarizeRenderedDocs(context.Background(), rendered,
			WithGeneratePrompt(customPrompt))
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.NotEmpty(t, summaries[0])
	})

	t.Run("processes nested link fields", func(t *testing.T) {
		// Create document with nested link
		docs := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"_type": "post",
					"title": "Blog Post",
					"metadata": map[string]any{
						"thumbnail": server.URL + "/image.png",
					},
				},
			},
		}

		rendered := renderDocuments(docs)
		summaries, err := summarizer.SummarizeRenderedDocs(context.Background(), rendered)
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.NotEmpty(t, summaries[0])
	})

	t.Run("handles missing document type gracefully", func(t *testing.T) {
		// Document without _type field
		docs := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Test",
					"content": "Content without type",
				},
			},
		}

		rendered := renderDocuments(docs)
		summaries, err := summarizer.SummarizeRenderedDocs(context.Background(), rendered)
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.NotEmpty(t, summaries[0])
	})

	t.Run("handles download failures gracefully", func(t *testing.T) {
		docs := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"_type":       "article",
					"title":       "Test",
					"broken_link": server.URL + "/does-not-exist",
				},
			},
		}

		rendered := renderDocuments(docs)
		summaries, err := summarizer.SummarizeRenderedDocs(context.Background(), rendered)
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		// Should still have some summary from the title
		assert.NotEmpty(t, summaries[0])
	})
}

// TestTemplateFieldExtraction tests that custom prompts work correctly
func TestTemplateFieldExtraction(t *testing.T) {
	t.Run("uses custom prompt", func(t *testing.T) {
		docs := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"_type":  "doc",
					"field1": "value1",
					"field2": "value2",
				},
			},
		}

		// Skip if in CI
		if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
			t.Skip("Skipping GenKit tests in CI environment")
		}

		summarizer := newSummarizer(t)

		// Render documents manually
		rendered := renderDocuments(docs)

		// Use custom prompt with {{this}} placeholder
		customPrompt := `{{this}}

Extract and display the value of field1 from the content above.`

		summaries, err := summarizer.SummarizeRenderedDocs(context.Background(), rendered,
			WithGeneratePrompt(customPrompt))
		require.NoError(t, err)
		require.Len(t, summaries, 1)
	})
}
