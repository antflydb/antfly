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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/template"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/ollama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSummarizer(t *testing.T) *GenKitModelImpl {
	url := "http://localhost:11434"
	modelName := "gemma3:4b"
	ollamaPlugin := &ollama.Ollama{ServerAddress: url}
	g := genkit.Init(t.Context(),
		genkit.WithPlugins(ollamaPlugin),
	)
	model := ollamaPlugin.DefineModel(
		g,
		ollama.ModelDefinition{
			Name: modelName,
			Type: "chat",
		},
		&ai.ModelOptions{
			Label: modelName,
			Supports: &ai.ModelSupports{
				Multiturn:  true,
				SystemRole: true,
				Media:      true,
				Tools:      false,
			},
			Versions: []string{},
		},
	)
	summarizer := NewGenKitSummarizer(g, model)
	return summarizer
}

// renderDocuments is a helper to convert schema.Document to rendered strings for testing
func renderDocuments(docs []schema.Document) []string {
	// Template that handles both text fields and images
	tmpl := `{{#each this}}
	{{#unless (eq @key "_embeddings")}}
	{{#if (eq @key "image") }}
	{{media url=this}}{{else}}
	{{@key}}: {{this}}
    {{/if}}
    {{/unless}}
    {{/each}}`

	rendered := make([]string, len(docs))
	for i, doc := range docs {
		result, err := template.Render(tmpl, doc.Fields)
		// fmt.Println(result, err)
		if err != nil {
			// Fallback to simple string representation if template fails
			result = fmt.Sprintf("%v", doc.Fields)
		}
		rendered[i] = result
	}
	return rendered
}

func TestGenKitSummarizerImpl_Summarize(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)
	t.Run("text only summarization", func(t *testing.T) {
		content := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"text": "Artificial intelligence (AI) is intelligence demonstrated by machines, as opposed to natural intelligence displayed by animals including humans. Leading AI textbooks define the field as the study of intelligent agents.",
				},
			},
		}

		rendered := renderDocuments(content)
		summaries, err := summarizer.SummarizeRenderedDocs(t.Context(), rendered)
		require.NoError(t, err, "Failed to generate text summary")
		require.Len(t, summaries, 1, "Expected one summary")
		assert.NotEmpty(t, summaries[0], "Summary should not be empty")
		assert.Contains(t, summaries[0], "intelligence", "Summary should contain key concepts")
	})

	t.Run("image summarization with flower.jpg", func(t *testing.T) {
		// Read the flower.jpg file
		flowerPath := filepath.Join("testdata", "flower.jpg")
		imageData, err := os.ReadFile(flowerPath)
		require.NoError(t, err, "Failed to read flower.jpg")

		encodedData := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(imageData)
		content := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"image": encodedData,
				},
			},
		}

		rendered := renderDocuments(content)
		summaries, err := summarizer.SummarizeRenderedDocs(t.Context(), rendered)
		require.NoError(t, err, "Failed to generate image summary")
		require.Len(t, summaries, 1, "Expected one summary")
		assert.NotEmpty(t, summaries[0], "Summary should not be empty")
	})

	t.Run("mixed content summarization", func(t *testing.T) {
		// Read the flower.jpg file
		flowerPath := filepath.Join("testdata", "flower.jpg")
		imageData, err := os.ReadFile(flowerPath)
		require.NoError(t, err, "Failed to read flower.jpg")

		encodedData := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(imageData)
		content := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"description": "This is a beautiful flower captured in a garden setting.",
					"image":       encodedData,
				},
			},
		}

		rendered := renderDocuments(content)
		summaries, err := summarizer.SummarizeRenderedDocs(t.Context(), rendered)
		require.NoError(t, err, "Failed to generate mixed content summary")
		require.Len(t, summaries, 1, "Expected one summary")
		assert.NotEmpty(t, summaries[0], "Summary should not be empty")
	})

	t.Run("empty content", func(t *testing.T) {
		content := []schema.Document{}

		rendered := renderDocuments(content)
		summaries, err := summarizer.SummarizeRenderedDocs(t.Context(), rendered)
		require.NoError(t, err, "Should handle empty content gracefully")
		assert.Empty(t, summaries, "Should return empty summaries for empty content")
	})

	t.Run("multiple content parts", func(t *testing.T) {
		content := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"text": "First piece of content about technology.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"text": "Second piece of content about nature.",
				},
			},
		}

		rendered := renderDocuments(content)
		summaries, err := summarizer.SummarizeRenderedDocs(t.Context(), rendered)
		require.NoError(t, err, "Failed to generate multiple summaries")
		require.Len(t, summaries, 2, "Expected two summaries")
		assert.NotEmpty(t, summaries[0], "First summary should not be empty")
		assert.NotEmpty(t, summaries[1], "Second summary should not be empty")
	})
}

func TestGenKitSummarizerImpl_WithSystemPrompt(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)
	t.Run("custom system prompt", func(t *testing.T) {
		content := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"text": "Artificial intelligence is transforming technology.",
				},
			},
		}

		customPrompt := "You are a technical writer. Provide a formal, detailed summary."
		rendered := renderDocuments(content)
		summaries, err := summarizer.SummarizeRenderedDocs(
			t.Context(),
			rendered,
			WithGenerateSystemPrompt(customPrompt),
		)
		require.NoError(t, err, "Failed to generate summary with custom system prompt")
		require.Len(t, summaries, 1, "Expected one summary")
		assert.NotEmpty(t, summaries[0], "Summary should not be empty")
		t.Logf("Summary with custom prompt: %s", summaries[0])
	})

	t.Run("default system prompt when not specified", func(t *testing.T) {
		content := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"text": "Artificial intelligence is transforming technology.",
				},
			},
		}

		rendered := renderDocuments(content)
		summaries, err := summarizer.SummarizeRenderedDocs(t.Context(), rendered)
		require.NoError(t, err, "Failed to generate summary with default system prompt")
		require.Len(t, summaries, 1, "Expected one summary")
		assert.NotEmpty(t, summaries[0], "Summary should not be empty")
		t.Logf("Summary with default prompt: %s", summaries[0])
	})

	// Note: Streaming test removed - DocumentSummarizer doesn't support streaming
}

func TestGenKitSummarizerImpl_SummarizeWithCitations(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)
	t.Run("basic citation summarization", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"Title": "Introduction to AI",
					"Body":  "Artificial intelligence is the simulation of human intelligence by machines. It includes learning, reasoning, and self-correction.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"Title": "Machine Learning Basics",
					"Body":  "Machine learning is a subset of AI that enables systems to learn from data without being explicitly programmed.",
				},
			},
		}

		output, _, err := summarizer.RAG(t.Context(), documents)
		require.NoError(t, err, "Failed to generate summary with inline references")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Summary: %s", output)

		// Verify inline references are present using regex: [doc1] or [resource_id doc1] or [doc_id doc1] or [resource_id doc1, doc2]
		inlineRefRegex := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]+\]`)
		matches := inlineRefRegex.FindAllString(output, -1)
		assert.NotEmpty(
			t,
			matches,
			"Summary should contain inline resource references like [doc1] or [resource_id doc1]",
		)

		// Check if at least one of our documents is referenced
		hasDoc1 := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]*doc1[^\]]*\]`).MatchString(output)
		hasDoc2 := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]*doc2[^\]]*\]`).MatchString(output)
		assert.True(t, hasDoc1 || hasDoc2, "At least one document should be referenced inline")
	})

	t.Run("empty documents", func(t *testing.T) {
		documents := []schema.Document{}

		output, _, err := summarizer.RAG(t.Context(), documents)
		require.NoError(t, err, "Should handle empty documents gracefully")
		// LLM generates a response indicating no resources are available
		require.NotEmpty(t, output)
	})

	t.Run("single document", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Climate Change",
					"content": "Climate change refers to long-term shifts in global temperatures and weather patterns. Human activities have been the main driver since the 1800s.",
					"author":  "Dr. Jane Smith",
				},
			},
		}

		output, _, err := summarizer.RAG(t.Context(), documents)
		require.NoError(t, err, "Failed to generate summary for single document")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Summary: %s", output)

		// Should reference the only document available
		hasDoc1 := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]*doc1[^\]]*\]`).MatchString(output)
		assert.True(t, hasDoc1, "Should reference doc1 inline")
	})

	t.Run("multiple documents with different topics", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"topic":   "Space Exploration",
					"content": "NASA's Artemis program aims to return humans to the Moon by 2025. This will be the first crewed lunar landing since Apollo 17 in 1972.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"topic":   "Renewable Energy",
					"content": "Solar panel efficiency has increased dramatically over the past decade. Modern panels can convert over 22% of sunlight into electricity.",
				},
			},
			{
				ID: "doc3",
				Fields: map[string]any{
					"topic":   "Medical Research",
					"content": "mRNA vaccine technology has revolutionized vaccine development. These vaccines can be developed and manufactured much faster than traditional vaccines.",
				},
			},
		}

		output, _, err := summarizer.RAG(t.Context(), documents)
		require.NoError(t, err, "Failed to generate summary for multiple topics")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Summary: %s", output)

		// Verify inline references are present
		inlineRefRegex := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]+\]`)
		matches := inlineRefRegex.FindAllString(output, -1)
		assert.NotEmpty(t, matches, "Summary should contain inline resource references")
		t.Logf("Found %d inline references", len(matches))
	})

	t.Run("custom system prompt", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Python is a high-level programming language known for its simplicity and readability.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"content": "JavaScript is primarily used for web development and runs in web browsers.",
				},
			},
		}

		customPrompt := "You are a technical educator. Provide a clear, beginner-friendly summary with citations. Include relevant quotes that support each key point."
		output, _, err := summarizer.RAG(t.Context(), documents, WithSystemPrompt(customPrompt))
		require.NoError(t, err, "Failed to generate summary with custom system prompt")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Summary with custom prompt: %s", output)
	})

	t.Run("documents with minimal fields", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "simple1",
				Fields: map[string]any{
					"text": "The sky is blue.",
				},
			},
			{
				ID: "simple2",
				Fields: map[string]any{
					"text": "Water is wet.",
				},
			},
		}

		output, _, err := summarizer.RAG(t.Context(), documents)
		require.NoError(t, err, "Failed to generate summary for minimal documents")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Summary: %s", output)
	})

	t.Run("custom document renderer", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "article1",
				Fields: map[string]any{
					"title":  "Breaking News",
					"author": "John Doe",
					"body":   "A major scientific discovery was announced today.",
				},
			},
			{
				ID: "article2",
				Fields: map[string]any{
					"title":  "Weather Update",
					"author": "Jane Smith",
					"body":   "Sunny skies expected for the weekend.",
				},
			},
		}

		// Custom template for rendering articles (using Handlebars syntax)
		// The template has access to document fields via this.fields
		template := `Title: {{this.fields.title}}
Author: {{this.fields.author}}

{{this.fields.body}}`

		output, _, err := summarizer.RAG(t.Context(), documents, WithDocumentRenderer(template))
		require.NoError(t, err, "Failed to generate summary with custom renderer")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Summary with custom renderer: %s", output)

		// Verify we got inline references for at least one article
		hasArticle1 := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]*article1[^\]]*\]`).
			MatchString(output)
		hasArticle2 := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]*article2[^\]]*\]`).
			MatchString(output)
		assert.True(
			t,
			hasArticle1 || hasArticle2,
			"At least one article should be referenced inline",
		)
	})

	t.Run("with semantic query", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Introduction to Neural Networks",
					"content": "Neural networks are computing systems inspired by biological neural networks. They consist of interconnected nodes that process information using a connectionist approach to computation.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"title":   "Deep Learning Applications",
					"content": "Deep learning has revolutionized computer vision, enabling applications like facial recognition, autonomous vehicles, and medical image analysis.",
				},
			},
			{
				ID: "doc3",
				Fields: map[string]any{
					"title":   "Natural Language Processing",
					"content": "NLP enables computers to understand, interpret, and generate human language. Modern approaches use transformer architectures and attention mechanisms.",
				},
			},
		}

		query := "computer vision and image recognition"
		output, _, err := summarizer.RAG(t.Context(), documents, WithSemanticQuery(query))
		require.NoError(t, err, "Failed to generate summary with semantic query")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Query: %s", query)
		t.Logf("Summary: %s", output)

		// Verify inline references are present
		inlineRefRegex := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]+\]`)
		matches := inlineRefRegex.FindAllString(output, -1)
		assert.NotEmpty(t, matches, "Summary should contain inline resource references")

		// The summary should ideally focus on computer vision (doc2) given the query
		// But we'll just verify basic functionality works
	})

	t.Run("with empty semantic query", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Quantum computing leverages quantum mechanics to process information.",
				},
			},
		}

		// Empty query should work just like no query
		output, _, err := summarizer.RAG(t.Context(), documents, WithSemanticQuery(""))
		require.NoError(t, err, "Failed to generate summary with empty semantic query")
		assert.NotEmpty(t, output, "Summary should not be empty")

		t.Logf("Summary with empty query: %s", output)
	})

	t.Run("streaming with structured results", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Distributed Systems",
					"content": "Distributed systems are collections of independent computers that appear to users as a single coherent system. They provide fault tolerance and scalability.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"title":   "Consensus Algorithms",
					"content": "Raft and Paxos are consensus algorithms that allow distributed systems to agree on values despite failures. Raft is designed to be more understandable than Paxos.",
				},
			},
		}

		var streamedChunks []string
		output, _, err := summarizer.RAG(
			t.Context(),
			documents,
			WithStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "generation" {
					streamedChunks = append(streamedChunks, data.(string))
				}
				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate streaming summary")
		assert.NotEmpty(t, output, "Summary should not be empty")
		assert.NotEmpty(t, streamedChunks, "Should have received streaming chunks")

		// CRITICAL: Verify streaming is working
		// With markdown streaming, we get chunks as the LLM generates text
		// We verify that we received at least some streaming chunks (proves streaming works)
		assert.NotEmpty(t, streamedChunks, "Should receive streaming chunks")

		t.Logf("Final summary: %s", output)
		t.Logf("Received %d streaming summary chunks", len(streamedChunks))

		// Verify that streaming chunks combine to the full summary (with inline references)
		var combinedSummary string
		var combinedSummarySb526 strings.Builder
		for _, chunk := range streamedChunks {
			combinedSummarySb526.WriteString(chunk)
		}
		combinedSummary += combinedSummarySb526.String()
		assert.NotEmpty(t, combinedSummary, "Should have received summary text chunks")
		assert.Equal(
			t,
			output,
			combinedSummary,
			"Streamed chunks should combine to the full summary",
		)

		// Verify inline references are present in the summary
		inlineRefRegex := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]+\]`)
		matches := inlineRefRegex.FindAllString(output, -1)
		assert.NotEmpty(t, matches, "Summary should contain inline resource references")
	})
}

// TestGenKitSummarizerImpl_SummarizeWithSemanticQuery removed - DocumentSummarizer doesn't support semantic queries
// Semantic queries are tested in Answer() function tests instead

func TestSplitValidUTF8(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedValid  string
		expectedRemain string
	}{
		{
			name:           "all valid ASCII",
			input:          "Hello World",
			expectedValid:  "Hello World",
			expectedRemain: "",
		},
		{
			name:           "all valid UTF-8 with emoji",
			input:          "Hello 👋 World 🌍",
			expectedValid:  "Hello 👋 World 🌍",
			expectedRemain: "",
		},
		{
			name:           "empty string",
			input:          "",
			expectedValid:  "",
			expectedRemain: "",
		},
		{
			name:           "valid with incomplete emoji at end",
			input:          "Hello\xF0\x9F",
			expectedValid:  "Hello",
			expectedRemain: "\xF0\x9F",
		},
		{
			name:           "valid with single incomplete byte",
			input:          "Hello\xF0",
			expectedValid:  "Hello",
			expectedRemain: "\xF0",
		},
		{
			name:           "valid multibyte characters",
			input:          "Café résumé naïve",
			expectedValid:  "Café résumé naïve",
			expectedRemain: "",
		},
		{
			name:           "Chinese characters",
			input:          "你好世界",
			expectedValid:  "你好世界",
			expectedRemain: "",
		},
		{
			name:           "mixed with incomplete at end",
			input:          "Hello 世界\xE4",
			expectedValid:  "Hello 世界",
			expectedRemain: "\xE4",
		},
		{
			name:           "only invalid bytes",
			input:          "\xF0\x9F",
			expectedValid:  "",
			expectedRemain: "\xF0\x9F",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			valid, remain := splitValidUTF8(tc.input)
			assert.Equal(t, tc.expectedValid, valid, "Valid portion mismatch")
			assert.Equal(t, tc.expectedRemain, remain, "Remaining portion mismatch")
		})
	}
}

func TestGenKitSummarizerImpl_StreamingWithUTF8(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)
	t.Run("streaming with emojis and special characters", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Tell me about popular emojis like 👋 🌍 🚀 and how they're used in communication. Include various special characters like café, résumé, and naïve.",
				},
			},
		}

		var streamedChunks []string
		output, _, err := summarizer.RAG(
			context.Background(),
			documents,
			WithStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "generation" {
					chunk := data.(string)
					streamedChunks = append(streamedChunks, chunk)

					// CRITICAL: Verify no UTF-8 replacement characters in chunks
					assert.NotContains(
						t,
						chunk,
						"\ufffd",
						"Chunk should not contain UTF-8 replacement character (�)",
					)

					// Verify each chunk is valid UTF-8
					assert.True(
						t,
						utf8.ValidString(chunk),
						"Each streamed chunk should be valid UTF-8",
					)

					// t.Logf("Chunk %d (len=%d): %q", len(streamedChunks), len(chunk), chunk)
				}
				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate streaming summary with UTF-8 content")
		assert.NotEmpty(t, output, "Summary should not be empty")
		assert.NotEmpty(t, streamedChunks, "Should have received streaming chunks")

		// Verify final output is valid UTF-8
		assert.True(t, utf8.ValidString(output), "Final summary should be valid UTF-8")
		assert.NotContains(
			t,
			output,
			"\ufffd",
			"Final summary should not contain UTF-8 replacement character",
		)

		// Verify chunks combine to final output
		var combined string
		var combinedSb657 strings.Builder
		for _, chunk := range streamedChunks {
			combinedSb657.WriteString(chunk)
		}
		combined += combinedSb657.String()
		assert.Equal(t, output, combined, "Streamed chunks should combine to final summary")

		t.Logf("Final summary: %s", output)
		t.Logf("Received %d chunks", len(streamedChunks))
	})

	t.Run("streaming with various Unicode blocks", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Examples of text: English, Français, Español, Русский, العربية, 中文, 日本語, 한국어, עברית, हिन्दी.",
				},
			},
		}

		var streamedChunks []string
		output, _, err := summarizer.RAG(
			context.Background(),
			documents,
			WithStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "generation" {
					chunk := data.(string)
					streamedChunks = append(streamedChunks, chunk)

					// Verify each chunk is valid UTF-8
					assert.True(
						t,
						utf8.ValidString(chunk),
						"Each streamed chunk should be valid UTF-8",
					)
					assert.NotContains(
						t,
						chunk,
						"\ufffd",
						"Chunk should not contain replacement character",
					)
				}
				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate summary with multilingual content")
		require.NotEmpty(t, output)

		// Verify all chunks are valid UTF-8
		for i, chunk := range streamedChunks {
			assert.True(t, utf8.ValidString(chunk), "Chunk %d should be valid UTF-8", i)
		}

		t.Logf("Processed %d chunks with multilingual content", len(streamedChunks))
	})
}

func TestGenKitSummarizerImpl_HandlebarsTemplateInjection(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("default template renders all fields", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Test Article",
					"author":  "John Doe",
					"content": "This is test content.",
					"year":    2024,
				},
			},
		}

		output, _, err := summarizer.RAG(t.Context(), documents)
		require.NoError(t, err, "Failed to generate summary with default template")
		assert.NotEmpty(t, output, "Summary should not be empty")
		t.Logf("Summary with default template: %s", output)
	})

	t.Run("custom handlebars template with conditionals", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "article1",
				Fields: map[string]any{
					"title":    "Breaking News",
					"author":   "Jane Smith",
					"content":  "Major discovery announced.",
					"featured": true,
				},
			},
			{
				ID: "article2",
				Fields: map[string]any{
					"title":    "Regular Update",
					"author":   "Bob Jones",
					"content":  "Daily news summary.",
					"featured": false,
				},
			},
		}

		// Template with Handlebars conditionals
		template := `**{{this.fields.title}}** by {{this.fields.author}}
{{#if this.fields.featured}}⭐ FEATURED ARTICLE{{/if}}

{{this.fields.content}}`

		output, _, err := summarizer.RAG(t.Context(), documents, WithDocumentRenderer(template))
		require.NoError(t, err, "Failed to generate summary with conditional template")
		assert.NotEmpty(t, output, "Summary should not be empty")
		t.Logf("Summary with conditional template: %s", output)

		// Verify inline references exist
		hasArticle1 := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]*article1[^\]]*\]`).
			MatchString(output)
		hasArticle2 := regexp.MustCompile(`\[(?:(?:resource_id|doc_id)\s+)?[^\]]*article2[^\]]*\]`).
			MatchString(output)
		assert.True(t, hasArticle1 || hasArticle2, "At least one article should be referenced")
	})

	t.Run("custom template with loops", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "project1",
				Fields: map[string]any{
					"name": "Project Alpha",
					"tags": []string{"backend", "api", "golang"},
				},
			},
		}

		// Template that iterates over tags array
		template := `# {{this.fields.name}}

Tags: {{#each this.fields.tags}}{{this}}, {{/each}}`

		output, _, err := summarizer.RAG(t.Context(), documents, WithDocumentRenderer(template))
		require.NoError(t, err, "Failed to generate summary with loop template")
		assert.NotEmpty(t, output, "Summary should not be empty")
		t.Logf("Summary with loop template: %s", output)
	})

	t.Run("custom template with missing fields", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "incomplete1",
				Fields: map[string]any{
					"title": "Only Title",
					// author is missing
				},
			},
		}

		// Template references a field that doesn't exist
		template := `Title: {{this.fields.title}}
Author: {{this.fields.author}}
Description: {{this.fields.description}}`

		output, _, err := summarizer.RAG(t.Context(), documents, WithDocumentRenderer(template))
		require.NoError(t, err, "Should handle missing fields gracefully")
		assert.NotEmpty(t, output, "Summary should not be empty")
		t.Logf("Summary with missing fields: %s", output)
	})

	t.Run("custom template with nested data", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "complex1",
				Fields: map[string]any{
					"title": "Complex Document",
					"metadata": map[string]any{
						"created": "2024-01-01",
						"updated": "2024-01-15",
					},
				},
			},
		}

		// Template accessing nested data
		template := `# {{this.fields.title}}

Created: {{this.fields.metadata.created}}
Updated: {{this.fields.metadata.updated}}`

		output, _, err := summarizer.RAG(t.Context(), documents, WithDocumentRenderer(template))
		require.NoError(t, err, "Failed to generate summary with nested data template")
		assert.NotEmpty(t, output, "Summary should not be empty")
		t.Logf("Summary with nested data: %s", output)
	})

	t.Run("custom template with semantic query", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "tech1",
				Fields: map[string]any{
					"title":    "AI Advances",
					"category": "Technology",
					"summary":  "Recent breakthroughs in AI research.",
				},
			},
			{
				ID: "tech2",
				Fields: map[string]any{
					"title":    "Quantum Computing",
					"category": "Technology",
					"summary":  "Progress in quantum algorithms.",
				},
			},
		}

		template := `[{{this.fields.category}}] {{this.fields.title}}
{{this.fields.summary}}`

		query := "What are the latest technology developments?"
		output, _, err := summarizer.RAG(t.Context(), documents,
			WithDocumentRenderer(template),
			WithSemanticQuery(query))
		require.NoError(t, err, "Failed to generate summary with template and query")
		assert.NotEmpty(t, output, "Summary should not be empty")
		t.Logf("Query: %s", query)
		t.Logf("Summary with template and query: %s", output)
	})
}
