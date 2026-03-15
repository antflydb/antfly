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
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/antflydb/antfly/lib/schema"
	genkitai "github.com/firebase/genkit/go/ai"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGenKitSummarizerImpl_Answer_WithReasoning tests that reasoning from classification
// can be passed to the answer step via WithReasoning()
func TestGenKitSummarizerImpl_Answer_WithReasoning(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("answer with classification reasoning passed in", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Climate Change Overview",
					"content": "Climate change refers to long-term shifts in global temperatures and weather patterns. Human activities have been the main driver since the 1800s, primarily due to burning fossil fuels.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"title":   "Renewable Energy",
					"content": "Solar and wind energy are rapidly growing renewable sources. They help reduce carbon emissions and combat climate change.",
				},
			},
		}

		query := "What is climate change and how can we address it?"
		// Simulate reasoning from classification step
		classificationReasoning := "The user is asking a two-part question about climate change: what it is and how to address it. This requires both definitional information and solution-oriented content."

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithReasoning(classificationReasoning),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer with reasoning context")
		require.NotNil(t, result)

		// Answer should be present (reasoning is only shown to LLM, not returned in result)
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		t.Logf("Answer: %s", result.Generation)
	})

	t.Run("answer without reasoning", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Artificial intelligence is transforming technology.",
				},
			},
		}

		query := "What is AI?"
		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer without reasoning")
		require.NotNil(t, result)

		// Answer should still be present
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		t.Logf("Answer: %s", result.Generation)
	})
}

func TestGenKitSummarizerImpl_Answer_WithFollowup(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("answer with followup questions enabled", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Introduction to Machine Learning",
					"content": "Machine learning is a subset of AI that enables systems to learn from data. It includes supervised learning, unsupervised learning, and reinforcement learning.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"title":   "Neural Networks",
					"content": "Neural networks are computing systems inspired by biological neural networks. They are fundamental to deep learning.",
				},
			},
		}

		query := "What is machine learning?"
		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(true),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer with followup questions")
		require.NotNil(t, result)

		// Verify followup questions are present
		assert.NotEmpty(t, result.FollowupQuestions, "Followup questions should not be empty when enabled")
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		// Typically expect 2-3 follow-up questions
		assert.NotEmpty(t, result.FollowupQuestions, "Should have at least one follow-up question")
		assert.LessOrEqual(t, len(result.FollowupQuestions), 5, "Should have reasonable number of follow-up questions")

		t.Logf("Answer: %s", result.Generation)
		t.Logf("Follow-up questions (%d):", len(result.FollowupQuestions))
		for i, question := range result.FollowupQuestions {
			t.Logf("  %d. %s", i+1, question)
			assert.NotEmpty(t, question, "Each follow-up question should not be empty")
		}
	})

	t.Run("answer without followup questions", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Quantum computing uses quantum mechanics for computation.",
				},
			},
		}

		query := "What is quantum computing?"
		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(false),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer without followup")
		require.NotNil(t, result)

		// Verify followup questions are empty when disabled
		assert.Empty(t, result.FollowupQuestions, "Followup questions should be empty when disabled")
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		t.Logf("Answer: %s", result.Generation)
	})
}

func TestGenKitSummarizerImpl_Answer_WithReasoningAndFollowup(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("answer with classification reasoning and followup enabled", func(t *testing.T) {
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
					"content": "Raft and Paxos are consensus algorithms that allow distributed systems to agree on values despite failures.",
				},
			},
		}

		query := "How do distributed systems maintain consistency?"
		classificationReasoning := "The user is asking about consistency mechanisms in distributed systems, which requires understanding consensus algorithms and fault tolerance."

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithReasoning(classificationReasoning),
			WithFollowup(true),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer with reasoning context and followup")
		require.NotNil(t, result)

		// Verify components are present
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")
		assert.NotEmpty(t, result.FollowupQuestions, "Followup questions should not be empty when enabled")

		t.Logf("Answer: %s", result.Generation)
		t.Logf("Follow-up questions (%d):", len(result.FollowupQuestions))
		for i, question := range result.FollowupQuestions {
			t.Logf("  %d. %s", i+1, question)
		}

		assert.NotEmpty(t, result.FollowupQuestions, "Should have at least one follow-up question")
	})
}

func TestGenKitSummarizerImpl_Answer_StreamingWithReasoning(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("streaming answer with classification reasoning context", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Space Exploration",
					"content": "NASA's Artemis program aims to return humans to the Moon. This will enable future Mars missions.",
				},
			},
		}

		query := "What is the Artemis program?"
		classificationReasoning := "The user is asking for factual information about a specific NASA program."

		var generationChunks []string

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithReasoning(classificationReasoning),
			WithFollowup(false),
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "generation" {
					chunk := data.(string)
					generationChunks = append(generationChunks, chunk)
					t.Logf("Answer chunk: %q", chunk)
				}
				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate streaming answer")
		require.NotNil(t, result)

		// Verify answer chunks were streamed
		assert.NotEmpty(t, generationChunks, "Should have received answer chunks")

		// Verify final result contains streamed content
		var streamedAnswer string
		var streamedAnswerSb strings.Builder
		for _, chunk := range generationChunks {
			streamedAnswerSb.WriteString(chunk)
		}
		streamedAnswer = streamedAnswerSb.String()

		assert.NotEmpty(t, streamedAnswer, "Should have streamed some answer content")

		t.Logf("Received %d answer chunks", len(generationChunks))
		t.Logf("Final answer: %s", result.Generation)
	})
}

func TestGenKitSummarizerImpl_Answer_StreamingWithFollowup(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("streaming answer with followup questions", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Electric Vehicles",
					"content": "Electric vehicles are becoming more popular due to environmental concerns and improving battery technology. They produce zero direct emissions.",
				},
			},
		}

		query := "Why are electric vehicles popular?"

		var generationChunks []string

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(true),
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				switch eventType {
				case "generation":
					chunk := data.(string)
					generationChunks = append(generationChunks, chunk)
				case "followup":
					t.Logf("Followup section detected")
				}
				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate streaming answer with followup")
		require.NotNil(t, result)

		// Verify answer chunks were streamed
		assert.NotEmpty(t, generationChunks, "Should have received answer chunks")

		// Verify followup questions are in final result
		assert.NotEmpty(t, result.FollowupQuestions, "Should have followup questions")

		t.Logf("Received %d answer chunks", len(generationChunks))
		t.Logf("Final answer: %s", result.Generation)
		t.Logf("Follow-up questions (%d):", len(result.FollowupQuestions))
		for i, question := range result.FollowupQuestions {
			t.Logf("  %d. %s", i+1, question)
		}
	})
}

func TestMarkdownSectionParsing(t *testing.T) {
	// Test the internal markdown parsing logic
	t.Run("parse complete response with all sections", func(t *testing.T) {
		response := `## Generation
This is the main answer to the question.
It includes detailed information [resource_id doc1].

## Follow-up Questions
- What about this aspect?
- How does it relate to that?
- Can you provide more details?`

		result := parseStructuredResponse(response, false, false, true)

		assert.NotEmpty(t, result.Generation, "Should parse answer section")
		assert.Contains(t, result.Generation, "main answer")

		assert.Len(t, result.FollowupQuestions, 3, "Should parse 3 follow-up questions")
		assert.Contains(t, result.FollowupQuestions[0], "What about")
		assert.Contains(t, result.FollowupQuestions[1], "How does")
		assert.Contains(t, result.FollowupQuestions[2], "Can you")
	})

	t.Run("parse response with only answer section", func(t *testing.T) {
		response := `## Generation
This is just the answer without followup.`

		result := parseStructuredResponse(response, false, false, false)

		assert.NotEmpty(t, result.Generation, "Should parse answer section")
		assert.Contains(t, result.Generation, "just the answer")
		assert.Empty(t, result.FollowupQuestions, "Follow-up questions should be empty")
	})

	t.Run("parse followup questions with different formats", func(t *testing.T) {
		response := `## Generation
Some answer.

## Follow-up Questions
- Bullet point question
* Another bullet format
1. Numbered question
2. Second numbered question`

		result := parseStructuredResponse(response, false, false, true)

		assert.NotEmpty(t, result.FollowupQuestions, "Should parse follow-up questions")
		assert.GreaterOrEqual(t, len(result.FollowupQuestions), 4, "Should parse all question formats")
	})

	t.Run("parse response without section headers", func(t *testing.T) {
		response := `This is a plain text response without any markdown sections.`

		result := parseStructuredResponse(response, false, false, false)

		// Should use entire response as answer
		assert.Equal(t, response, result.Generation, "Should use entire response as answer")
		assert.Empty(t, result.FollowupQuestions, "Follow-up questions should be empty")
	})
}

func TestFindMarkdownHeader(t *testing.T) {
	testCases := []struct {
		name       string
		text       string
		headerName string
		expected   int
	}{
		{
			name:       "header at beginning",
			text:       "## Reasoning\nSome text",
			headerName: "Reasoning",
			expected:   0,
		},
		{
			name:       "header in middle",
			text:       "Some text\n## Generation\nMore text",
			headerName: "Generation",
			expected:   10,
		},
		{
			name:       "header not found",
			text:       "Some text without header",
			headerName: "Reasoning",
			expected:   -1,
		},
		{
			name:       "header with different case (should not match)",
			text:       "## reasoning\nSome text",
			headerName: "Reasoning",
			expected:   -1,
		},
		{
			name:       "header in middle of line (should not match)",
			text:       "text ## Generation\nMore text",
			headerName: "Generation",
			expected:   -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := findMarkdownHeader(tc.text, tc.headerName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseFollowupQuestions(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name: "bullet points with dash",
			input: `- Question one?
- Question two?
- Question three?`,
			expected: []string{"Question one?", "Question two?", "Question three?"},
		},
		{
			name: "bullet points with asterisk",
			input: `* First question
* Second question`,
			expected: []string{"First question", "Second question"},
		},
		{
			name: "numbered list",
			input: `1. First question
2. Second question
3. Third question`,
			expected: []string{"First question", "Second question", "Third question"},
		},
		{
			name: "mixed formats",
			input: `- Bullet question
1. Numbered question
* Asterisk question`,
			expected: []string{"Bullet question", "Numbered question", "Asterisk question"},
		},
		{
			name:     "empty text",
			input:    "",
			expected: nil,
		},
		{
			name: "with empty lines",
			input: `- Question one

- Question two

- Question three`,
			expected: []string{"Question one", "Question two", "Question three"},
		},
		{
			name: "with double quotes",
			input: `- "Question with quotes"
- "Another quoted question"`,
			expected: []string{"Question with quotes", "Another quoted question"},
		},
		{
			name: "with double layer quotes",
			input: `- ""Double quoted question""
- ""Another double quoted""`,
			expected: []string{"Double quoted question", "Another double quoted"},
		},
		{
			name: "with single quotes",
			input: `- 'Single quoted question'
- 'Another single quoted'`,
			expected: []string{"Single quoted question", "Another single quoted"},
		},
		{
			name: "mixed quotes and no quotes",
			input: `- "Quoted question"
- Normal question
- ""Double quoted""`,
			expected: []string{"Quoted question", "Normal question", "Double quoted"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseFollowupQuestions(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFindMarkdownHeaderInCombined(t *testing.T) {
	testCases := []struct {
		name              string
		accumulated       string
		buffer            string
		headerName        string
		expectedFound     bool
		expectedIdxInBuf  int
		expectedBefore    string
		expectedRemaining string
	}{
		{
			name:              "header in current buffer only",
			accumulated:       "Some confidence",
			buffer:            " data\n## Generation\nAnswer text",
			headerName:        "Generation",
			expectedFound:     true,
			expectedIdxInBuf:  0,
			expectedBefore:    " data\n",
			expectedRemaining: "## Generation\nAnswer text",
		},
		{
			name:              "header spanning across buffers",
			accumulated:       "Some confidence data\n#",
			buffer:            "# Generation\nGeneration text",
			headerName:        "Generation",
			expectedFound:     true,
			expectedIdxInBuf:  0,
			expectedBefore:    "",
			expectedRemaining: "## Generation\nGeneration text",
		},
		{
			name:              "header entirely in accumulated buffer",
			accumulated:       "Some text\n## Generation",
			buffer:            "\nAnswer text",
			headerName:        "Generation",
			expectedFound:     true,
			expectedIdxInBuf:  0,
			expectedBefore:    "",
			expectedRemaining: "## Generation\nAnswer text",
		},
		{
			name:              "header not found",
			accumulated:       "Some confidence data",
			buffer:            " more data",
			headerName:        "Generation",
			expectedFound:     false,
			expectedIdxInBuf:  0,
			expectedBefore:    "",
			expectedRemaining: "",
		},
		{
			name:              "empty buffers",
			accumulated:       "",
			buffer:            "",
			headerName:        "Generation",
			expectedFound:     false,
			expectedIdxInBuf:  0,
			expectedBefore:    "",
			expectedRemaining: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			loc, found := findMarkdownHeaderInCombined(tc.accumulated, tc.buffer, tc.headerName)
			assert.Equal(t, tc.expectedFound, found)
			if found {
				assert.Equal(t, tc.expectedIdxInBuf, loc.idxInBuffer)
				assert.Equal(t, tc.expectedBefore, loc.beforeHeader)
				assert.Equal(t, tc.expectedRemaining, loc.remaining)
			}
		})
	}
}

func TestSkipHeaderLine(t *testing.T) {
	testCases := []struct {
		name              string
		buffer            string
		expectedOk        bool
		expectedRemaining string
	}{
		{
			name:              "complete header with newline",
			buffer:            "## Generation\nAnswer text",
			expectedOk:        true,
			expectedRemaining: "Answer text",
		},
		{
			name:              "header without newline (incomplete)",
			buffer:            "## Generation",
			expectedOk:        false,
			expectedRemaining: "",
		},
		{
			name:              "header with text on same line",
			buffer:            "## Reasoning: some text\nMore text",
			expectedOk:        true,
			expectedRemaining: "More text",
		},
		{
			name:              "empty buffer",
			buffer:            "",
			expectedOk:        false,
			expectedRemaining: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			remaining, ok := skipHeaderLine(tc.buffer)
			assert.Equal(t, tc.expectedOk, ok)
			if ok {
				assert.Equal(t, tc.expectedRemaining, remaining)
			}
		})
	}
}

func TestSplitAtHeader(t *testing.T) {
	testCases := []struct {
		name           string
		buffer         string
		headerName     string
		expectedFound  bool
		expectedBefore string
		expectedAfter  string
	}{
		{
			name:           "header in middle",
			buffer:         "Some text\n## Generation\nAnswer text",
			headerName:     "Generation",
			expectedFound:  true,
			expectedBefore: "Some text\n",
			expectedAfter:  "## Generation\nAnswer text",
		},
		{
			name:           "header at beginning",
			buffer:         "## Generation\nAnswer text",
			headerName:     "Generation",
			expectedFound:  true,
			expectedBefore: "",
			expectedAfter:  "## Generation\nAnswer text",
		},
		{
			name:           "header not found",
			buffer:         "Some text without header",
			headerName:     "Generation",
			expectedFound:  false,
			expectedBefore: "",
			expectedAfter:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before, after, found := splitAtHeader(tc.buffer, tc.headerName)
			assert.Equal(t, tc.expectedFound, found)
			if found {
				assert.Equal(t, tc.expectedBefore, before)
				assert.Equal(t, tc.expectedAfter, after)
			}
		})
	}
}

func TestMarkdownSectionParserHeaderStripping(t *testing.T) {
	ctx := context.Background()

	t.Run("answer header split across chunks should be stripped", func(t *testing.T) {
		// This test reproduces the bug where "## Generation" header arrives without
		// a newline in the first chunk, causing the header to leak into streamed content
		var streamedContent []string
		callback := func(ctx context.Context, eventType string, data any) error {
			if eventType == "generation" {
				streamedContent = append(streamedContent, data.(string))
			}
			return nil
		}

		parser := newMarkdownSectionParser(callback, false, false, false)

		// Simulate chunks arriving where the header line is split:
		// First chunk: "## Generation" (no newline yet)
		// Second chunk: "\nThis is the actual answer content that should be streamed."
		chunks := []string{
			"## Generation",
			"\nThis is the actual answer content that should be streamed.",
		}

		for _, chunk := range chunks {
			err := parser.parseChunk(ctx, "generation", chunk)
			require.NoError(t, err)
		}

		// Flush remaining content
		parser.flush(ctx)

		// Combine all streamed content
		combined := strings.Join(streamedContent, "")

		// The header "## Generation" should NOT appear in the streamed content
		assert.NotContains(t, combined, "## Generation",
			"The ## Generation header should be stripped from streamed content")

		// The actual content SHOULD be present
		assert.Contains(t, combined, "This is the actual answer content",
			"The actual answer content should be streamed")
	})

	t.Run("answer header in single chunk should be stripped", func(t *testing.T) {
		var streamedContent []string
		callback := func(ctx context.Context, eventType string, data any) error {
			if eventType == "generation" {
				streamedContent = append(streamedContent, data.(string))
			}
			return nil
		}

		parser := newMarkdownSectionParser(callback, false, false, false)

		// Single chunk with complete header line
		chunk := "## Generation\nThis is the answer content."
		err := parser.parseChunk(ctx, "generation", chunk)
		require.NoError(t, err)

		parser.flush(ctx)

		combined := strings.Join(streamedContent, "")

		assert.NotContains(t, combined, "## Generation",
			"The ## Generation header should be stripped from streamed content")
		assert.Contains(t, combined, "This is the answer content",
			"The actual answer content should be streamed")
	})

	t.Run("header split character by character should be stripped", func(t *testing.T) {
		var streamedContent []string
		callback := func(ctx context.Context, eventType string, data any) error {
			if eventType == "generation" {
				streamedContent = append(streamedContent, data.(string))
			}
			return nil
		}

		parser := newMarkdownSectionParser(callback, false, false, false)

		// Simulate extreme case: header arrives character by character
		fullContent := "## Generation\nThe answer is here."
		for _, char := range fullContent {
			err := parser.parseChunk(ctx, "generation", string(char))
			require.NoError(t, err)
		}

		parser.flush(ctx)

		combined := strings.Join(streamedContent, "")

		assert.NotContains(t, combined, "## Generation",
			"The ## Generation header should be stripped even when arriving char by char")
		assert.Contains(t, combined, "The answer is here",
			"The actual answer content should be streamed")
	})

	t.Run("content without any header should still be emitted as answer", func(t *testing.T) {
		// This tests the case where the LLM doesn't output ## Generation header at all
		var streamedContent []string
		callback := func(ctx context.Context, eventType string, data any) error {
			if eventType == "generation" {
				streamedContent = append(streamedContent, data.(string))
			}
			return nil
		}

		parser := newMarkdownSectionParser(callback, false, false, false)

		// LLM outputs content without any markdown section header
		chunks := []string{
			"This is ",
			"the answer ",
			"without any header.",
		}

		for _, chunk := range chunks {
			err := parser.parseChunk(ctx, "generation", chunk)
			require.NoError(t, err)
		}

		parser.flush(ctx)

		combined := strings.Join(streamedContent, "")

		// The content should still be emitted as an answer
		assert.Contains(t, combined, "This is the answer without any header",
			"Content without header should be emitted as answer on flush")
	})
}

func TestStreamContentWithLookahead(t *testing.T) {
	testCases := []struct {
		name           string
		buffer         string
		lookaheadSize  int
		minStreamSize  int
		expectedShould bool
	}{
		{
			name:           "buffer too small to stream (< minStreamSize)",
			buffer:         "This is a long text that should be streamed because it exceeds the minimum size",
			lookaheadSize:  50,
			minStreamSize:  100,
			expectedShould: false, // buffer length is 81, which is < minStreamSize 100
		},
		{
			name:           "buffer way too small",
			buffer:         "Short text",
			lookaheadSize:  50,
			minStreamSize:  100,
			expectedShould: false,
		},
		{
			name:           "buffer exactly at threshold (should not stream)",
			buffer:         "This text is exactly one hundred characters long padding padding padding padding padding paddingx",
			lookaheadSize:  50,
			minStreamSize:  100,
			expectedShould: false,
		},
		{
			name:           "buffer just over threshold (should stream)",
			buffer:         "This text is one hundred and one characters long padding padding padding padding padding padding xxxx",
			lookaheadSize:  50,
			minStreamSize:  100,
			expectedShould: true,
		},
		{
			name:           "buffer much larger (should stream)",
			buffer:         "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco.",
			lookaheadSize:  50,
			minStreamSize:  100,
			expectedShould: true,
		},
		{
			name:           "empty buffer",
			buffer:         "",
			lookaheadSize:  50,
			minStreamSize:  100,
			expectedShould: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			streamable, remaining, should := streamContentWithLookahead(tc.buffer, tc.lookaheadSize, tc.minStreamSize)
			assert.Equal(t, tc.expectedShould, should)

			if should {
				// When streaming, should have non-empty streamable content
				assert.NotEmpty(t, streamable, "streamable content should not be empty when should=true")
				// Remaining should have approximately lookaheadSize bytes (may vary due to UTF-8 splitting)
				assert.LessOrEqual(t, len(remaining), tc.lookaheadSize+10, "remaining should be approximately lookaheadSize")
				// Combined length should equal original
				assert.Equal(t, len(tc.buffer), len(streamable)+len(remaining), "streamable + remaining should equal original buffer length")
			} else {
				// When not streaming, remaining should be the entire buffer
				assert.Empty(t, streamable, "streamable should be empty when should=false")
				assert.Equal(t, tc.buffer, remaining, "remaining should equal buffer when not streaming")
			}
		})
	}
}

func TestGenerateQueryResponse(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("generate query response with reasoning and followup", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "AI in Healthcare",
					"content": "Artificial intelligence is revolutionizing healthcare through improved diagnostics and personalized treatment plans.",
				},
			},
		}

		query := "How is AI used in healthcare?"

		// First classify the query with reasoning
		classification, err := summarizer.ClassifyImproveAndTransformQuery(
			context.Background(),
			query,
			WithClassificationReasoning(true),
		)
		require.NoError(t, err)
		require.NotNil(t, classification)
		// Note: Reasoning is optional and depends on LLM output, so we don't assert it's non-empty

		// Generate the full response, passing classification reasoning to answer
		result, err := summarizer.GenerateQueryResponse(
			context.Background(),
			query,
			documents,
			classification,
			WithReasoning(classification.Reasoning),
			WithFollowup(true),
		)
		require.NoError(t, err, "Failed to generate query response")
		require.NotNil(t, result)

		// Verify all components
		assert.NotEmpty(t, result.Classification, "Classification should not be empty")
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")
		assert.NotEmpty(t, result.FollowupQuestions, "Followup questions should not be empty")

		t.Logf("Classification: %s", result.Classification)
		t.Logf("Classification Reasoning: %s", classification.Reasoning)
		t.Logf("Answer: %s", result.Generation)
		t.Logf("Follow-up questions (%d):", len(result.FollowupQuestions))
		for i, question := range result.FollowupQuestions {
			t.Logf("  %d. %s", i+1, question)
		}
	})
}

func TestAnswer_FollowupQuestionStreamingEvents(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("followup questions are emitted as individual streaming events", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Quantum Computing",
					"content": "Quantum computing leverages quantum mechanics for computation. It uses qubits instead of classical bits.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"title":   "Quantum Algorithms",
					"content": "Quantum algorithms like Shor's algorithm can factor large numbers exponentially faster than classical algorithms.",
				},
			},
		}

		query := "What is quantum computing?"

		// Track all streaming events in order
		type streamEvent struct {
			eventType string
			data      string
		}
		var events []streamEvent
		var followupEventCount int

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(true),
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				dataStr := data.(string)
				events = append(events, streamEvent{
					eventType: eventType,
					data:      dataStr,
				})

				if eventType == "followup" {
					followupEventCount++
					t.Logf("Followup question event #%d: %s", followupEventCount, dataStr)
				}

				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate answer with followup streaming")
		require.NotNil(t, result)

		// Verify followup questions were streamed as events
		assert.Positive(t, followupEventCount, "Should have received followup_question events")
		assert.NotEmpty(t, result.FollowupQuestions, "Final result should contain followup questions")

		// Verify that the number of events matches the number in the result
		assert.Equal(t, len(result.FollowupQuestions), followupEventCount,
			"Number of followup_question events should match final result count")

		// Verify followup events come after answer events
		var lastAnswerIndex int
		var firstFollowupIndex = len(events)
		for i, event := range events {
			if event.eventType == "generation" {
				lastAnswerIndex = i
			}
			if event.eventType == "followup" && i < firstFollowupIndex {
				firstFollowupIndex = i
			}
		}

		if followupEventCount > 0 {
			assert.Less(t, lastAnswerIndex, firstFollowupIndex,
				"Followup question events should come after answer events")
		}

		t.Logf("Total events: %d", len(events))
		t.Logf("Followup question events: %d", followupEventCount)
		t.Logf("Last answer event at index: %d", lastAnswerIndex)
		t.Logf("First followup event at index: %d", firstFollowupIndex)
	})

	t.Run("followup questions match between events and final result", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Machine learning is a subset of AI that enables systems to learn from data.",
				},
			},
		}

		query := "What is machine learning?"

		var streamedQuestions []string

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(true),
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "followup" {
					streamedQuestions = append(streamedQuestions, data.(string))
				}
				return nil
			}),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify streamed questions match final result
		if len(result.FollowupQuestions) > 0 {
			assert.Equal(t, result.FollowupQuestions, streamedQuestions,
				"Streamed followup questions should match final result")
		}

		t.Logf("Streamed questions (%d): %v", len(streamedQuestions), streamedQuestions)
		t.Logf("Final result questions (%d): %v", len(result.FollowupQuestions), result.FollowupQuestions)
	})

	t.Run("no followup events when followup disabled", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Blockchain is a distributed ledger technology.",
				},
			},
		}

		query := "What is blockchain?"

		var followupEventCount int

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(false), // Explicitly disable
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "followup" {
					followupEventCount++
				}
				return nil
			}),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify no followup events were emitted
		assert.Equal(t, 0, followupEventCount, "Should not receive followup_question events when disabled")
		assert.Empty(t, result.FollowupQuestions, "Final result should not contain followup questions")
	})

	t.Run("each followup question is a separate event", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Neural Networks",
					"content": "Neural networks are computing systems inspired by biological neural networks. They consist of layers of interconnected nodes.",
				},
			},
		}

		query := "What are neural networks?"

		var followupEvents []string

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(true),
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "followup" {
					question := data.(string)
					followupEvents = append(followupEvents, question)
					// Verify each event is a single question, not a list
					assert.NotEmpty(t, question, "Each followup event should contain a question")
					assert.NotContains(t, question, "\n-", "Each event should be a single question, not a list")
					assert.NotContains(t, question, "\n*", "Each event should be a single question, not a list")
				}
				return nil
			}),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify we got individual questions
		if len(followupEvents) > 1 {
			t.Logf("Verified %d separate followup question events", len(followupEvents))
			for i, q := range followupEvents {
				t.Logf("  Event %d: %s", i+1, q)
			}
		}
	})
}

func TestStructuredPromptTemplate_WithUserContext(t *testing.T) {
	t.Run("template has conditional sections using Handlebars", func(t *testing.T) {
		// Verify template uses Handlebars conditionals
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if reasoning}}", "Template should have conditional reasoning input section")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if followup_context}}", "Template should have conditional followup section")
		assert.Contains(t, StructuredGenerationPromptTemplate, "=== QUERY ANALYSIS ===", "Template should have query analysis section for reasoning")
		assert.Contains(t, StructuredGenerationPromptTemplate, "## Generation", "Template should have answer header")
		assert.Contains(t, StructuredGenerationPromptTemplate, "## Follow-up Questions", "Template should have followup header")
	})

	t.Run("template uses variables for customization", func(t *testing.T) {
		// Verify template uses variables for context
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{reasoning}}", "Template should reference reasoning variable")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{generation_context}}", "Template should reference generation_context variable")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{followup_context}}", "Template should reference followup_context variable")
	})

	t.Run("template has required structural elements", func(t *testing.T) {
		// Verify essential prompt structure
		assert.Contains(t, StructuredGenerationPromptTemplate, "VALID RESOURCE IDs", "Template should have resource ID instructions")
		assert.Contains(t, StructuredGenerationPromptTemplate, "[resource_id <ID>]", "Template should specify resource_id citation format")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#each documents}}", "Template should iterate over documents")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if semantic_search}}", "Template should conditionally handle semantic search")
	})
}

func TestGenKitSummarizerImpl_Answer_WithConfidence(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("answer with confidence enabled", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Photosynthesis",
					"content": "Photosynthesis is the process by which plants convert light energy into chemical energy. It occurs in chloroplasts and produces oxygen as a byproduct.",
				},
			},
		}

		query := "What is photosynthesis?"
		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithConfidence(true),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer with confidence")
		require.NotNil(t, result)

		// Verify confidence scores are present and valid
		assert.Greater(t, result.GenerationConfidence, float32(0.0), "Answer confidence should be greater than 0")
		assert.LessOrEqual(t, result.GenerationConfidence, float32(1.0), "Answer confidence should be <= 1.0")
		assert.Greater(t, result.ContextRelevance, float32(0.0), "Context relevance should be greater than 0")
		assert.LessOrEqual(t, result.ContextRelevance, float32(1.0), "Context relevance should be <= 1.0")
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		t.Logf("Answer Confidence: %.2f", result.GenerationConfidence)
		t.Logf("Context Relevance: %.2f", result.ContextRelevance)
		t.Logf("Answer: %s", result.Generation)
	})

	t.Run("answer without confidence", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Blockchain is a distributed ledger technology.",
				},
			},
		}

		query := "What is blockchain?"
		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithConfidence(false),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer without confidence")
		require.NotNil(t, result)

		// Verify confidence scores are default/zero when disabled
		assert.Equal(t, float32(0.0), result.GenerationConfidence, "Answer confidence should be 0 when disabled")
		assert.Equal(t, float32(0.0), result.ContextRelevance, "Context relevance should be 0 when disabled")
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		t.Logf("Answer: %s", result.Generation)
	})

	t.Run("answer with confidence and reasoning", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "DNA Structure",
					"content": "DNA is a double helix structure composed of nucleotides. It contains genetic information for all living organisms.",
				},
			},
		}

		query := "What is DNA?"

		// Reasoning is now part of classification, not answer
		// To test with reasoning context, we'd need to get it from classification first
		classificationReasoning := "The user is asking about DNA, which is a fundamental biological molecule."

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithConfidence(true),
			WithReasoning(classificationReasoning),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer with confidence and reasoning")
		require.NotNil(t, result)

		// Verify all components (reasoning is no longer in result)
		assert.Greater(t, result.GenerationConfidence, float32(0.0), "Should have answer confidence")
		assert.Greater(t, result.ContextRelevance, float32(0.0), "Should have context relevance")
		assert.NotEmpty(t, result.Generation, "Should have answer")

		t.Logf("Answer Confidence: %.2f", result.GenerationConfidence)
		t.Logf("Context Relevance: %.2f", result.ContextRelevance)
		t.Logf("Classification Reasoning (passed to answer): %s", classificationReasoning)
		t.Logf("Answer: %s", result.Generation)
	})
}

func TestGenKitSummarizerImpl_Answer_StreamingWithConfidence(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("streaming answer with confidence", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Relativity",
					"content": "Einstein's theory of relativity revolutionized physics. It describes the relationship between space and time.",
				},
			},
		}

		query := "What is relativity?"

		var confidenceEvents []ConfidenceResult
		var generationChunks []string

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithConfidence(true),
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				switch eventType {
				case "confidence":
					confidence := data.(ConfidenceResult)
					confidenceEvents = append(confidenceEvents, confidence)
					t.Logf("Confidence event - Answer: %.2f, Context: %.2f",
						confidence.GenerationConfidence, confidence.ContextRelevance)
				case "generation":
					chunk := data.(string)
					generationChunks = append(generationChunks, chunk)
				}
				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate streaming answer with confidence")
		require.NotNil(t, result)

		// Verify confidence event was emitted
		assert.Len(t, confidenceEvents, 1, "Should have received exactly one confidence event")
		if len(confidenceEvents) > 0 {
			conf := confidenceEvents[0]
			assert.Greater(t, conf.GenerationConfidence, float32(0.0), "Streamed answer confidence should be > 0")
			assert.LessOrEqual(t, conf.GenerationConfidence, float32(1.0), "Streamed answer confidence should be <= 1.0")
			assert.Greater(t, conf.ContextRelevance, float32(0.0), "Streamed context relevance should be > 0")
			assert.LessOrEqual(t, conf.ContextRelevance, float32(1.0), "Streamed context relevance should be <= 1.0")
		}

		// Verify answer chunks were streamed
		assert.NotEmpty(t, generationChunks, "Should have received answer chunks")

		// Verify final result has confidence scores
		assert.Greater(t, result.GenerationConfidence, float32(0.0), "Final answer confidence should be > 0")
		assert.Greater(t, result.ContextRelevance, float32(0.0), "Final context relevance should be > 0")
		assert.NotEmpty(t, result.Generation, "Final answer should not be empty")

		// Verify streamed confidence matches final result
		if len(confidenceEvents) > 0 {
			assert.Equal(t, confidenceEvents[0].GenerationConfidence, result.GenerationConfidence,
				"Streamed answer confidence should match final result")
			assert.Equal(t, confidenceEvents[0].ContextRelevance, result.ContextRelevance,
				"Streamed context relevance should match final result")
		}

		t.Logf("Received %d answer chunks", len(generationChunks))
		t.Logf("Final Answer Confidence: %.2f", result.GenerationConfidence)
		t.Logf("Final Context Relevance: %.2f", result.ContextRelevance)
		t.Logf("Final Answer: %s", result.Generation)
	})

	t.Run("streaming answer with confidence and reasoning context", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Gravity",
					"content": "Gravity is a fundamental force that attracts objects with mass toward each other. On Earth, it gives weight to physical objects.",
				},
			},
		}

		query := "What is gravity?"

		type streamEvent struct {
			eventType string
			data      any
		}
		var events []streamEvent

		// Reasoning is now part of classification, not answer streaming
		// So we pass it as context but don't expect it to be streamed
		classificationReasoning := "The user is asking about a fundamental physics concept."

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithConfidence(true),
			WithReasoning(classificationReasoning),
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				events = append(events, streamEvent{
					eventType: eventType,
					data:      data,
				})
				t.Logf("Event: %s", eventType)
				return nil
			}),
		)
		require.NoError(t, err, "Failed to generate streaming answer with confidence")
		require.NotNil(t, result)

		// Count event types (no more reasoning events from answer step)
		var confidenceCount, answerCount int
		var firstConfidenceIdx, firstAnswerIdx = -1, -1
		for i, event := range events {
			switch event.eventType {
			case "confidence":
				confidenceCount++
				if firstConfidenceIdx == -1 {
					firstConfidenceIdx = i
				}
			case "generation":
				answerCount++
				if firstAnswerIdx == -1 {
					firstAnswerIdx = i
				}
			}
		}

		// Verify event counts
		assert.Equal(t, 1, confidenceCount, "Should have exactly one confidence event")
		assert.Positive(t, answerCount, "Should have answer events")

		// Verify event order: confidence -> answer
		if confidenceCount > 0 && answerCount > 0 {
			assert.Less(t, firstConfidenceIdx, firstAnswerIdx,
				"Confidence event should come before answer events")
		}

		// Verify final result (reasoning no longer in result)
		assert.Greater(t, result.GenerationConfidence, float32(0.0), "Should have answer confidence")
		assert.Greater(t, result.ContextRelevance, float32(0.0), "Should have context relevance")
		assert.NotEmpty(t, result.Generation, "Should have answer")

		t.Logf("Total events: %d (confidence: %d, answer: %d)",
			len(events), confidenceCount, answerCount)
		t.Logf("Event order - Confidence: %d, Answer: %d",
			firstConfidenceIdx, firstAnswerIdx)
		t.Logf("Classification Reasoning (passed to answer): %s", classificationReasoning)
	})

	t.Run("no confidence events when confidence disabled", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"content": "Photosynthesis converts light into energy.",
				},
			},
		}

		query := "What is photosynthesis?"

		var confidenceEventCount int

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithConfidence(false), // Explicitly disable
			WithGenerationSemanticQuery(query),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "confidence" {
					confidenceEventCount++
				}
				return nil
			}),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify no confidence events were emitted
		assert.Equal(t, 0, confidenceEventCount, "Should not receive confidence events when disabled")
		assert.Equal(t, float32(0.0), result.GenerationConfidence, "Final result should not have confidence scores")
		assert.Equal(t, float32(0.0), result.ContextRelevance, "Final result should not have context relevance")
	})
}

func TestAnswerWithUserContext(t *testing.T) {
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("answer with custom answer context", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "AI Safety",
					"content": "AI safety focuses on ensuring artificial intelligence systems behave as intended and don't cause harm.",
				},
			},
		}

		query := "What is AI safety?"
		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithGenerationContext("Keep the answer to exactly one sentence"),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Answer: %s", result.Generation)

		// Verify answer is present
		assert.NotEmpty(t, result.Generation, "Should have answer")
	})

	t.Run("answer with custom followup context", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Quantum Computing",
					"content": "Quantum computers use quantum bits (qubits) to perform calculations. They can solve certain problems much faster than classical computers.",
				},
			},
		}

		query := "What are quantum computers?"
		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithFollowup(true),
			WithFollowupContext("Generate exactly 2 follow-up questions about practical applications"),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Answer: %s", result.Generation)
		t.Logf("Follow-up questions (%d): %v", len(result.FollowupQuestions), result.FollowupQuestions)

		assert.NotEmpty(t, result.Generation, "Should have answer")
		// Note: LLM may not always follow the exact number requested, but should generate some questions
		assert.NotEmpty(t, result.FollowupQuestions, "Should have follow-up questions")
	})

	t.Run("answer with all custom contexts", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Machine Learning",
					"content": "Machine learning is a subset of AI that enables systems to learn from data without explicit programming.",
				},
			},
		}

		query := "What is machine learning?"
		classificationReasoning := "The user is asking for a basic definition of machine learning, which is appropriate for beginners."

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithReasoning(classificationReasoning),
			WithFollowup(true),
			WithGenerationContext("Give a concise, beginner-friendly explanation"),
			WithFollowupContext("Generate 2 questions about learning more"),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Answer: %s", result.Generation)
		t.Logf("Follow-up questions: %v", result.FollowupQuestions)

		assert.NotEmpty(t, result.Generation, "Should have answer")
		assert.NotEmpty(t, result.FollowupQuestions, "Should have follow-up questions")
	})
}

// TestGenKitSummarizerImpl_ClassifyImproveAndTransformQuery tests classification with reasoning
func TestGenKitSummarizerImpl_ClassifyImproveAndTransformQuery(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("classification without reasoning", func(t *testing.T) {
		query := "What is Redis and how does it work?"

		result, err := summarizer.ClassifyImproveAndTransformQuery(
			context.Background(),
			query,
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify basic classification fields
		assert.NotEmpty(t, result.RouteType, "Should have route type")
		assert.NotEmpty(t, result.Strategy, "Should have strategy")
		assert.NotEmpty(t, result.ImprovedQuery, "Should have improved query")
		assert.NotEmpty(t, result.SemanticQuery, "Should have semantic query")
		assert.Empty(t, result.Reasoning, "Should not have reasoning when disabled")

		t.Logf("Route Type: %s", result.RouteType)
		t.Logf("Strategy: %s", result.Strategy)
		t.Logf("Improved Query: %s", result.ImprovedQuery)
	})

	t.Run("classification with reasoning", func(t *testing.T) {
		query := "Compare JWT and OAuth for authentication"

		result, err := summarizer.ClassifyImproveAndTransformQuery(
			context.Background(),
			query,
			WithClassificationReasoning(true),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify required classification fields
		assert.NotEmpty(t, result.RouteType, "Should have route type")
		assert.NotEmpty(t, result.Strategy, "Should have strategy")
		assert.NotEmpty(t, result.ImprovedQuery, "Should have improved query")
		assert.NotEmpty(t, result.SemanticQuery, "Should have semantic query")

		// Note: Reasoning field exists and will be populated if LLM provides it,
		// but we don't assert it's non-empty since LLM output is non-deterministic
		// The important thing is the field exists and can be populated

		t.Logf("Route Type: %s", result.RouteType)
		t.Logf("Strategy: %s", result.Strategy)
		t.Logf("Reasoning: %s", result.Reasoning)
		t.Logf("Improved Query: %s", result.ImprovedQuery)
	})

	t.Run("classification with reasoning streaming", func(t *testing.T) {
		query := "Why do microservices add complexity?"

		var reasoningChunks []string

		result, err := summarizer.ClassifyImproveAndTransformQuery(
			context.Background(),
			query,
			WithClassificationReasoning(true),
			WithGenerationStreaming(func(ctx context.Context, eventType string, data any) error {
				if eventType == "reasoning" {
					chunk := data.(string)
					reasoningChunks = append(reasoningChunks, chunk)
					t.Logf("Reasoning chunk: %q", chunk)
				}
				return nil
			}),
		)
		require.NoError(t, err)
		require.NotNil(t, result)

		// If reasoning was provided by LLM, verify it was streamed properly
		// Note: LLM output is non-deterministic, so we can't guarantee reasoning will be provided
		if result.Reasoning != "" {
			assert.NotEmpty(t, reasoningChunks, "If reasoning exists, it should have been streamed")
			t.Logf("Reasoning chunks received: %d", len(reasoningChunks))
		}

		// Verify streamed content is part of final reasoning
		var streamedReasoning strings.Builder
		for _, chunk := range reasoningChunks {
			streamedReasoning.WriteString(chunk)
		}

		t.Logf("Received %d reasoning chunks", len(reasoningChunks))
		t.Logf("Streamed reasoning: %s", streamedReasoning.String())
		t.Logf("Final reasoning: %s", result.Reasoning)
		t.Logf("Strategy: %s", result.Strategy)
	})
}

func TestAddClassificationContextToTemplateVars(t *testing.T) {
	t.Run("nil result does not panic", func(t *testing.T) {
		templateVars := make(map[string]any)
		addClassificationContextToTemplateVars(templateVars, nil)
		assert.Empty(t, templateVars, "Should not add any vars for nil result")
	})

	t.Run("adds entire classification result to template vars", func(t *testing.T) {
		templateVars := make(map[string]any)
		result := &ClassificationTransformationResult{
			Strategy: QueryStrategyDecompose,
			SubQuestions: []string{
				"What is the capital of France?",
				"What is its population?",
				"What is its history?",
			},
			StepBackQuery: "What is the general theory of relativity?",
			ImprovedQuery: "What are the benefits of exercise for mental health?",
			SemanticMode:  SemanticQueryModeHypothetical,
		}
		addClassificationContextToTemplateVars(templateVars, result)

		storedResult, ok := templateVars["classification_result"].(*ClassificationTransformationResult)
		require.True(t, ok, "classification_result should be *ClassificationTransformationResult")
		assert.Equal(t, result, storedResult, "Should store the entire result object")
		assert.Equal(t, QueryStrategyDecompose, storedResult.Strategy)
		assert.Len(t, storedResult.SubQuestions, 3)
		assert.Equal(t, "What is the capital of France?", storedResult.SubQuestions[0])
	})
}

func TestStructuredPromptTemplate_WithClassificationContext(t *testing.T) {
	t.Run("template has conditional sections for classification context", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if classification_result.sub_questions}}", "Template should have conditional sub_questions section")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if classification_result.step_back_query}}", "Template should have conditional step_back_query section")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if classification_result.improved_query}}", "Template should have conditional improved_query section")
	})

	t.Run("template has descriptive section headers", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "=== QUERY DECOMPOSITION ===", "Template should have decomposition section header")
		assert.Contains(t, StructuredGenerationPromptTemplate, "=== BACKGROUND CONTEXT ===", "Template should have background context section header")
		assert.Contains(t, StructuredGenerationPromptTemplate, "=== CLARIFIED QUERY ===", "Template should have clarified query section header")
	})

	t.Run("template iterates over sub_questions", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#each classification_result.sub_questions}}", "Template should iterate over sub_questions")
	})

	t.Run("template references step_back_query variable", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{classification_result.step_back_query}}", "Template should reference step_back_query variable")
	})

	t.Run("template references improved_query variable", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{classification_result.improved_query}}", "Template should reference improved_query variable")
	})

	t.Run("template instructs LLM to address all sub-questions", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "IMPORTANT: Your answer should address ALL of these sub-questions", "Template should instruct to address all sub-questions")
	})
}

func TestGenKitSummarizerImpl_Answer_WithClassificationResult(t *testing.T) {
	// Skip if in CI or if Ollama is not available
	if os.Getenv("CI") == "true" || os.Getenv("SKIP_GENKIT_TESTS") == "true" {
		t.Skip("Skipping GenKit tests in CI environment")
	}

	summarizer := newSummarizer(t)

	t.Run("answer with decompose strategy context", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "Paris Overview",
					"content": "Paris is the capital of France. It has a population of about 2.1 million in the city proper and over 12 million in the metropolitan area.",
				},
			},
			{
				ID: "doc2",
				Fields: map[string]any{
					"title":   "Paris History",
					"content": "Paris has been an important city for over 2,000 years. It was founded by the Romans as Lutetia. The city became the capital of France in the 10th century.",
				},
			},
		}

		query := "Tell me about Paris - its role as capital, population, and history"
		classificationResult := &ClassificationTransformationResult{
			Strategy: QueryStrategyDecompose,
			SubQuestions: []string{
				"What is Paris's role as capital of France?",
				"What is the population of Paris?",
				"What is the history of Paris?",
			},
			ImprovedQuery: "Provide information about Paris including its role as the capital of France, its population statistics, and its historical background.",
			SemanticMode:  SemanticQueryModeRewrite,
		}

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithClassificationResult(classificationResult),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer with classification result")
		require.NotNil(t, result)
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		t.Logf("Answer: %s", result.Generation)
	})

	t.Run("answer with step_back strategy context", func(t *testing.T) {
		documents := []schema.Document{
			{
				ID: "doc1",
				Fields: map[string]any{
					"title":   "E=mc²",
					"content": "Einstein's famous equation E=mc² shows that energy and mass are interchangeable. This is a consequence of special relativity, which Einstein published in 1905.",
				},
			},
		}

		query := "What does E=mc² mean?"
		classificationResult := &ClassificationTransformationResult{
			Strategy:      QueryStrategyStepBack,
			StepBackQuery: "What is Einstein's theory of special relativity?",
			ImprovedQuery: "Explain the meaning and significance of Einstein's equation E=mc²",
			SemanticMode:  SemanticQueryModeRewrite,
		}

		result, _, err := summarizer.Generate(
			context.Background(),
			query,
			documents,
			WithClassificationResult(classificationResult),
			WithGenerationSemanticQuery(query),
		)
		require.NoError(t, err, "Failed to generate answer with step_back context")
		require.NotNil(t, result)
		assert.NotEmpty(t, result.Generation, "Answer should not be empty")

		t.Logf("Answer: %s", result.Generation)
	})
}

func TestStripModelEndTokens(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no end token",
			input:    "This is a normal answer.",
			expected: "This is a normal answer.",
		},
		{
			name:     "Gemini end_of_turn token",
			input:    "This is the answer.</end_of_turn>",
			expected: "This is the answer.",
		},
		{
			name:     "Gemini alternate end_of_turn token",
			input:    "This is the answer.<end_of_turn>",
			expected: "This is the answer.",
		},
		{
			name:     "GPT endoftext token",
			input:    "This is the answer.<|endoftext|>",
			expected: "This is the answer.",
		},
		{
			name:     "ChatML im_end token",
			input:    "This is the answer.<|im_end|>",
			expected: "This is the answer.",
		},
		{
			name:     "Llama 3 eot_id token",
			input:    "This is the answer.<|eot_id|>",
			expected: "This is the answer.",
		},
		{
			name:     "end token with trailing whitespace",
			input:    "This is the answer.</end_of_turn>  \n",
			expected: "This is the answer.",
		},
		{
			name:     "end token with leading whitespace before token",
			input:    "This is the answer.  </end_of_turn>",
			expected: "This is the answer.",
		},
		{
			name:     "token in middle should not be stripped",
			input:    "The </end_of_turn> token appeared mid-sentence.",
			expected: "The </end_of_turn> token appeared mid-sentence.",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only whitespace",
			input:    "   \n\t  ",
			expected: "",
		},
		{
			name:     "only end token",
			input:    "</end_of_turn>",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := stripModelEndTokens(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseStructuredResponse_StripsEndTokens(t *testing.T) {
	t.Run("strips end token from answer section", func(t *testing.T) {
		response := `## Generation
This is the answer content.</end_of_turn>`

		result := parseStructuredResponse(response, false, false, false)
		assert.Equal(t, "This is the answer content.", result.Generation)
		assert.NotContains(t, result.Generation, "</end_of_turn>")
	})

	t.Run("strips end token from response without sections", func(t *testing.T) {
		response := "Plain text response without headers.<|endoftext|>"

		result := parseStructuredResponse(response, false, false, false)
		assert.Equal(t, "Plain text response without headers.", result.Generation)
		assert.NotContains(t, result.Generation, "<|endoftext|>")
	})
}

func TestAddResourceSubQuestionMapping(t *testing.T) {
	t.Run("empty inputs produce no mapping", func(t *testing.T) {
		templateVars := make(map[string]any)
		addResourceSubQuestionMapping(templateVars, nil, nil)
		_, exists := templateVars["resource_sub_question_mapping"]
		assert.False(t, exists, "Should not add mapping for nil inputs")

		addResourceSubQuestionMapping(templateVars, map[string][]int{}, []string{})
		_, exists = templateVars["resource_sub_question_mapping"]
		assert.False(t, exists, "Should not add mapping for empty inputs")
	})

	t.Run("creates mapping with correct sub-question names", func(t *testing.T) {
		templateVars := make(map[string]any)
		resourceSubQMap := map[string][]int{
			"doc1": {0, 1},
			"doc2": {1, 2},
			"doc3": {0},
		}
		subQuestions := []string{
			"What is the capital?",
			"What is the population?",
			"What is the history?",
		}

		addResourceSubQuestionMapping(templateVars, resourceSubQMap, subQuestions)

		mapping, ok := templateVars["resource_sub_question_mapping"]
		require.True(t, ok, "Mapping should be added to template vars")

		// Verify it's a slice with correct length using reflection
		mappingSlice, ok := mapping.([]ResourceMapping)
		require.True(t, ok, "Mapping should be []ResourceMapping type")
		assert.Len(t, mappingSlice, 3, "Should have 3 resource mappings")
	})

	t.Run("handles out of range sub-question indices", func(t *testing.T) {
		templateVars := make(map[string]any)
		resourceSubQMap := map[string][]int{
			"doc1": {0, 5, 10}, // 5 and 10 are out of range
		}
		subQuestions := []string{"Question 1", "Question 2"}

		addResourceSubQuestionMapping(templateVars, resourceSubQMap, subQuestions)

		mapping, ok := templateVars["resource_sub_question_mapping"]
		require.True(t, ok, "Mapping should be added")

		// Verify only valid indices are included
		mappingSlice := mapping.([]ResourceMapping)
		require.Len(t, mappingSlice, 1)
		assert.Equal(t, "doc1", mappingSlice[0].ResourceID)
		assert.Len(t, mappingSlice[0].SubQuestions, 1, "Only valid index should be included")
		assert.Equal(t, "Question 1", mappingSlice[0].SubQuestions[0])
	})
}

func TestStructuredPromptTemplate_WithResourceSubQuestionMapping(t *testing.T) {
	t.Run("template has conditional section for resource-subquestion mapping", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if resource_sub_question_mapping}}", "Template should have conditional resource_sub_question_mapping section")
		assert.Contains(t, StructuredGenerationPromptTemplate, "=== RESOURCE RELEVANCE MAP ===", "Template should have resource relevance map header")
	})

	t.Run("template iterates over mapping", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#each resource_sub_question_mapping}}", "Template should iterate over resource_sub_question_mapping")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{this.resource_id}}", "Template should reference resource_id")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#each this.sub_questions}}", "Template should iterate over sub_questions")
	})
}

func TestStreamContentWithLookahead_UTF8Handling(t *testing.T) {
	// This test verifies the fix for the bug where multi-byte UTF-8 characters
	// (like curly quotes ' U+2019) were corrupted when split across streaming chunks.
	// The bug occurred because the split point calculation was byte-based, and when
	// it fell in the middle of a multi-byte character, the remaining buffer was
	// incorrectly constructed by concatenating incomplete byte sequences.

	t.Run("preserves multi-byte UTF-8 characters when split point lands mid-character", func(t *testing.T) {
		// The curly apostrophe ' (U+2019) is 3 bytes: E2 80 99
		// We construct a string where the split point will land in the middle of this character
		text := "Since it\u2019s a test" // Contains curly apostrophe (U+2019)

		// Verify our test string actually contains multi-byte characters
		assert.Greater(t, len(text), len([]rune(text)), "Test string should contain multi-byte characters")

		// Use a lookahead and min size that would cause a split in the middle of the apostrophe
		// The string is 18 bytes, the apostrophe starts at byte 8 (E2 80 99)
		// With lookaheadSize=10 and minStreamSize=5, splitPoint would be around byte 8
		lookaheadSize := 10
		minStreamSize := 5

		streamable, remaining, shouldStream := streamContentWithLookahead(text, lookaheadSize, minStreamSize)

		if shouldStream {
			// Verify both parts are valid UTF-8
			assert.True(t, isValidUTF8(streamable), "Streamable content should be valid UTF-8, got: %q", streamable)
			assert.True(t, isValidUTF8(remaining), "Remaining content should be valid UTF-8, got: %q", remaining)

			// Verify concatenation produces original string
			combined := streamable + remaining
			assert.Equal(t, text, combined, "Combined streamable + remaining should equal original")

			// Verify no duplicate or missing bytes
			assert.Equal(t, len(text), len(streamable)+len(remaining),
				"Total byte length should be preserved")
		}
	})

	t.Run("handles various multi-byte UTF-8 characters", func(t *testing.T) {
		testCases := []struct {
			name string
			text string
		}{
			{"curly apostrophe", "Since it\u2019s working correctly"},            // U+2019 RIGHT SINGLE QUOTATION MARK
			{"em dash", "This is a test\u2014with an em dash"},                   // U+2014 EM DASH
			{"ellipsis", "Wait for it\u2026 here it comes"},                      // U+2026 HORIZONTAL ELLIPSIS
			{"emoji", "Hello \U0001F44B world"},                                  // U+1F44B WAVING HAND SIGN (4 bytes)
			{"mixed unicode", "Caf\u00e9 r\u00e9sum\u00e9 na\u00efve \u00fcber"}, // accented chars
			{"chinese characters", "Hello \u4f60\u597d world \u4e16\u754c"},      // 你好 世界
			{"combining characters", "cafe\u0301 with combining accent"},         // e + combining acute
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Try various split points
				for lookahead := 5; lookahead < len(tc.text) && lookahead < 50; lookahead += 3 {
					for minSize := 3; minSize < len(tc.text)-lookahead && minSize < 20; minSize += 5 {
						streamable, remaining, shouldStream := streamContentWithLookahead(tc.text, lookahead, minSize)

						if shouldStream {
							// Verify both parts are valid UTF-8
							assert.True(t, isValidUTF8(streamable),
								"lookahead=%d, minSize=%d: streamable should be valid UTF-8, got: %q",
								lookahead, minSize, streamable)
							assert.True(t, isValidUTF8(remaining),
								"lookahead=%d, minSize=%d: remaining should be valid UTF-8, got: %q",
								lookahead, minSize, remaining)

							// Verify no data loss
							combined := streamable + remaining
							assert.Equal(t, tc.text, combined,
								"lookahead=%d, minSize=%d: combined should equal original",
								lookahead, minSize)
						}
					}
				}
			})
		}
	})

	t.Run("streaming parser preserves UTF-8 in answer chunks", func(t *testing.T) {
		ctx := context.Background()
		var streamedChunks []string

		callback := func(ctx context.Context, eventType string, data any) error {
			if eventType == "generation" {
				chunk := data.(string)
				streamedChunks = append(streamedChunks, chunk)
			}
			return nil
		}

		parser := newMarkdownSectionParser(callback, false, false, false)

		// Simulate streaming a response with multi-byte characters
		// The content is long enough to trigger streaming with lookahead
		content := "## Generation\n" +
			"This is a comprehensive answer that explains why it\u2019s important to handle UTF-8 correctly. " +
			"When streaming responses, we must ensure that multi-byte characters like curly quotes (\u2018\u2019), " +
			"em dashes (\u2014), and emoji (\U0001F44D) are not corrupted when split across chunks. " +
			"This is especially important for international users who may see characters like \u4f60\u597d or caf\u00e9."

		// Feed the content in chunks to simulate streaming
		chunkSize := 30
		for i := 0; i < len(content); i += chunkSize {
			end := min(i+chunkSize, len(content))
			err := parser.parseChunk(ctx, "generation", content[i:end])
			require.NoError(t, err)
		}
		parser.flush(ctx)

		// Verify all streamed chunks are valid UTF-8
		for i, chunk := range streamedChunks {
			assert.True(t, isValidUTF8(chunk),
				"Chunk %d should be valid UTF-8, got: %q", i, chunk)
		}

		// Verify combined chunks don't have corrupted characters
		combined := strings.Join(streamedChunks, "")
		assert.True(t, isValidUTF8(combined), "Combined output should be valid UTF-8")
		assert.NotContains(t, combined, "\ufffd", "Should not contain replacement character (corrupted UTF-8)")
	})
}

// isValidUTF8 checks if a string is valid UTF-8 and contains no replacement characters
func isValidUTF8(s string) bool {
	if !strings.Contains(s, "\ufffd") {
		// Check for any invalid UTF-8 sequences
		for _, r := range s {
			if r == 0xFFFD {
				return false
			}
		}
		return true
	}
	return false
}

func TestWithResourceSubQuestionMap(t *testing.T) {
	t.Run("option applies mapping to answer options", func(t *testing.T) {
		opts := &generationOptions{}
		mapping := map[string][]int{
			"doc1": {0, 1},
			"doc2": {2},
		}

		opt := WithResourceSubQuestionMap(mapping)
		opt.applyGenerationOption(opts)

		assert.Equal(t, mapping, opts.resourceSubQuestionMap)
	})

	t.Run("nil mapping is applied", func(t *testing.T) {
		opts := &generationOptions{}
		opt := WithResourceSubQuestionMap(nil)
		opt.applyGenerationOption(opts)

		assert.Nil(t, opts.resourceSubQuestionMap)
	})
}

func TestWithAgentKnowledge(t *testing.T) {
	t.Run("option applies agent knowledge to answer options", func(t *testing.T) {
		opts := &generationOptions{}
		knowledge := "This is domain-specific knowledge about the data."

		opt := WithAgentKnowledge(knowledge)
		opt.applyGenerationOption(opts)

		assert.Equal(t, knowledge, opts.agentKnowledge)
	})

	t.Run("empty string is applied", func(t *testing.T) {
		opts := &generationOptions{}
		opt := WithAgentKnowledge("")
		opt.applyGenerationOption(opts)

		assert.Empty(t, opts.agentKnowledge)
	})
}

func TestStructuredPromptTemplate_WithAgentKnowledge(t *testing.T) {
	t.Run("template has conditional section for agent knowledge", func(t *testing.T) {
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{#if agent_knowledge}}", "Template should have conditional agent_knowledge section")
		assert.Contains(t, StructuredGenerationPromptTemplate, "=== BACKGROUND KNOWLEDGE ===", "Template should have background knowledge header")
		assert.Contains(t, StructuredGenerationPromptTemplate, "{{agent_knowledge}}", "Template should reference agent_knowledge variable")
	})
}

func TestClassificationPromptTemplate_WithAgentKnowledge(t *testing.T) {
	t.Run("classification prompt has conditional section for agent knowledge", func(t *testing.T) {
		assert.Contains(t, ClassificationTransformationUserPrompt, "{{#if agent_knowledge}}", "Classification prompt should have conditional agent_knowledge section")
		assert.Contains(t, ClassificationTransformationUserPrompt, "## BACKGROUND KNOWLEDGE", "Classification prompt should have background knowledge header")
		assert.Contains(t, ClassificationTransformationUserPrompt, "{{agent_knowledge}}", "Classification prompt should reference agent_knowledge variable")
	})
}

// TestClassificationStreamingParser_ReasoningNotTruncated tests that the streaming parser
// doesn't lose content at the beginning of the reasoning section when streaming incrementally.
// This reproduces a bug where buffer manipulation caused the first few words to be lost.
func TestClassificationStreamingParser_ReasoningNotTruncated(t *testing.T) {
	ctx := context.Background()

	// Track all streamed reasoning content
	var streamedReasoning []string
	callback := func(ctx context.Context, eventType string, data any) error {
		if eventType == "reasoning" {
			t.Logf("Streamed reasoning chunk: %q", data.(string))
			streamedReasoning = append(streamedReasoning, data.(string))
		}
		return nil
	}

	parser := newClassificationStreamingParser(callback)

	// Simulate streaming chunks that will trigger the bug
	// We need enough content AFTER "## Reasoning" to exceed streamLookaheadSize (50 bytes)
	// First chunk: "## Reasoning\nBased on the documents [id doc_61697890cb8eefad]" = 66 chars after header
	// This should trigger streaming since afterHeader length > 50
	chunks := []*genkitai.ModelResponseChunk{
		{
			Content: []*genkitai.Part{
				genkitai.NewTextPart("## Reasoning\nBased on the documents [id doc_61697890cb8eefad] and"),
			},
		},
		{
			Content: []*genkitai.Part{
				genkitai.NewTextPart(" other relevant context, this query requires a comprehensive"),
			},
		},
		{
			Content: []*genkitai.Part{
				genkitai.NewTextPart(" analysis of the topic with multiple sources.\n\n## Classification Result"),
			},
		},
	}

	// Process all chunks
	for i, chunk := range chunks {
		t.Logf("Processing chunk %d", i)
		err := parser.parseChunk(ctx, chunk)
		require.NoError(t, err)
		t.Logf("Buffer after chunk %d: %q", i, parser.buffer)
	}

	// Flush any remaining content
	parser.flush(ctx)

	// Reconstruct the full streamed reasoning
	fullStreamedReasoning := strings.Join(streamedReasoning, "")
	t.Logf("Full streamed reasoning: %q", fullStreamedReasoning)
	t.Logf("Number of stream callbacks: %d", len(streamedReasoning))
	for i, chunk := range streamedReasoning {
		t.Logf("  Chunk %d: %q", i, chunk)
	}

	// The expected reasoning should contain the complete text without truncation
	expectedReasoning := "Based on the documents [id doc_61697890cb8eefad] and other relevant context, this query requires a comprehensive analysis of the topic with multiple sources."

	// This assertion will FAIL with the buggy code because "Based on the " gets lost
	assert.Equal(t, expectedReasoning, strings.TrimSpace(fullStreamedReasoning),
		"Streamed reasoning should not be truncated at the beginning")

	// Additional check: ensure the beginning is not lost
	assert.True(t, strings.HasPrefix(strings.TrimSpace(fullStreamedReasoning), "Based on"),
		"Reasoning should start with 'Based on' but got: %q", fullStreamedReasoning)

	// Verify no duplicate content in streams (each character should appear exactly once)
	// If there's buffer corruption but no data loss, we'd see duplicates
	for i := 0; i < len(streamedReasoning)-1; i++ {
		current := streamedReasoning[i]
		next := streamedReasoning[i+1]
		// Check that current doesn't end with the beginning of next (no overlap/duplication)
		if len(current) > 0 && len(next) > 0 {
			// This is a basic check - in correct streaming, each chunk should be distinct
			assert.NotEqual(t, current, next, "Streamed chunks should not be identical")
		}
	}
}

// TestClassificationStreamingParser_RegressionIssue tests the specific bug reported
// where "Based on the documents [id doc_61697890cb8eefad]" was truncated to "id doc_61697890cb8eefad]"
func TestClassificationStreamingParser_RegressionIssue(t *testing.T) {
	ctx := context.Background()

	var streamedReasoning []string
	callback := func(ctx context.Context, eventType string, data any) error {
		if eventType == "reasoning" {
			streamedReasoning = append(streamedReasoning, data.(string))
		}
		return nil
	}

	parser := newClassificationStreamingParser(callback)

	// Simulate the exact scenario: reasoning that contains "Based on the documents [id doc_XXX]"
	// This is split across multiple chunks to trigger incremental streaming
	chunks := []*genkitai.ModelResponseChunk{
		{
			Content: []*genkitai.Part{
				// First chunk includes header + beginning of reasoning (>50 chars after header)
				genkitai.NewTextPart("## Reasoning\nBased on the documents [id doc_61697890cb8eefad], the query"),
			},
		},
		{
			Content: []*genkitai.Part{
				genkitai.NewTextPart(" is asking about climate change impacts.\n\n## Classification Result\n"),
			},
		},
	}

	for _, chunk := range chunks {
		err := parser.parseChunk(ctx, chunk)
		require.NoError(t, err)
	}

	parser.flush(ctx)

	fullStreamedReasoning := strings.Join(streamedReasoning, "")

	// The bug would cause "Based on the documents [" to be lost, showing only "id doc_61697890cb8eefad]..."
	assert.Contains(t, fullStreamedReasoning, "Based on the documents [id doc_61697890cb8eefad]",
		"Should contain the full document reference without truncation")
	assert.True(t, strings.HasPrefix(strings.TrimSpace(fullStreamedReasoning), "Based on"),
		"Should start with 'Based on', got: %q", fullStreamedReasoning)
}

// TestClassificationStreamingParser_UTF8Safety tests that the classification streaming parser
// does not split multi-byte UTF-8 characters across chunk boundaries, which would produce
// U+FFFD replacement characters in the streamed output.
func TestClassificationStreamingParser_UTF8Safety(t *testing.T) {
	ctx := context.Background()

	var streamedReasoning []string
	callback := func(ctx context.Context, eventType string, data any) error {
		if eventType == "reasoning" {
			chunk := data.(string)
			// Every streamed chunk must be valid UTF-8
			assert.True(t, utf8.ValidString(chunk),
				"Streamed reasoning chunk is not valid UTF-8: %q", chunk)
			// No replacement characters should appear
			assert.NotContains(t, chunk, "\ufffd",
				"Streamed reasoning chunk contains replacement character: %q", chunk)
			streamedReasoning = append(streamedReasoning, chunk)
		}
		return nil
	}

	parser := newClassificationStreamingParser(callback)

	// Build reasoning content with multi-byte characters placed so they'll land near
	// the streamLookaheadSize (50 byte) boundary when sliced.
	// Use a mix of emoji (4-byte), CJK (3-byte), and accented chars (2-byte).
	// We need >50 bytes after the header to trigger incremental streaming.
	reasoning := "The user\u2019s query about \u201csearch algorithms\u201d requires step back. " +
		"\u00abStep back\u00bb is a strategy for complex queries \U0001F50D that need broader context. " +
		"\u4E16\u754C means world in Chinese. This needs comprehensive analysis across sources."

	// Feed in small chunks to maximize the chance of hitting a multi-byte boundary
	fullText := "## Reasoning\n" + reasoning + "\n\n## Classification Result\n{}"
	for i := 0; i < len(fullText); i += 7 {
		end := min(i+7, len(fullText))
		chunk := &genkitai.ModelResponseChunk{
			Content: []*genkitai.Part{
				genkitai.NewTextPart(fullText[i:end]),
			},
		}
		err := parser.parseChunk(ctx, chunk)
		require.NoError(t, err)
	}

	parser.flush(ctx)

	fullStreamed := strings.Join(streamedReasoning, "")
	assert.Equal(t, strings.TrimSpace(reasoning), strings.TrimSpace(fullStreamed),
		"Full streamed reasoning should match original without corruption")
}
