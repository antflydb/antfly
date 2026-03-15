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
	"testing"

	"github.com/google/dotprompt/go/dotprompt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderedPromptToContentParts_TextOnly(t *testing.T) {
	rendered := dotprompt.RenderedPrompt{
		Messages: []dotprompt.Message{
			{
				Content: []dotprompt.Part{
					&dotprompt.TextPart{
						Text: "Hello, world!",
					},
				},
			},
		},
	}

	parts, err := RenderedPromptToContentParts(rendered)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	require.Len(t, parts[0], 1)

	textContent, ok := parts[0][0].(TextContent)
	require.True(t, ok)
	assert.Equal(t, "Hello, world!", textContent.Text)
}

func TestRenderedPromptToContentParts_WithImageURL(t *testing.T) {
	rendered := dotprompt.RenderedPrompt{
		Messages: []dotprompt.Message{
			{
				Content: []dotprompt.Part{
					&dotprompt.TextPart{
						Text: "Check out this image:",
					},
					&dotprompt.MediaPart{
						Media: dotprompt.Media{
							URL:         "https://example.com/image.jpg",
							ContentType: "image/jpeg",
						},
					},
				},
			},
		},
	}

	parts, err := RenderedPromptToContentParts(rendered)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	require.Len(t, parts[0], 2)

	textContent, ok := parts[0][0].(TextContent)
	require.True(t, ok)
	assert.Equal(t, "Check out this image:", textContent.Text)

	imageContent, ok := parts[0][1].(ImageURLContent)
	require.True(t, ok)
	assert.Equal(t, "https://example.com/image.jpg", imageContent.URL)
}

func TestRenderedPromptToContentParts_WithDataURI(t *testing.T) {
	// Small 1x1 red PNG
	dataURI := "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg=="

	rendered := dotprompt.RenderedPrompt{
		Messages: []dotprompt.Message{
			{
				Content: []dotprompt.Part{
					&dotprompt.MediaPart{
						Media: dotprompt.Media{
							URL: dataURI,
						},
					},
				},
			},
		},
	}

	parts, err := RenderedPromptToContentParts(rendered)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	require.Len(t, parts[0], 1)

	binaryContent, ok := parts[0][0].(BinaryContent)
	require.True(t, ok)
	assert.Equal(t, "image/png", binaryContent.MIMEType)
	assert.NotEmpty(t, binaryContent.Data)
}

func TestRenderedPromptToContentParts_MultipleMessages(t *testing.T) {
	rendered := dotprompt.RenderedPrompt{
		Messages: []dotprompt.Message{
			{
				Content: []dotprompt.Part{
					&dotprompt.TextPart{
						Text: "First message",
					},
				},
			},
			{
				Content: []dotprompt.Part{
					&dotprompt.TextPart{
						Text: "Second message",
					},
				},
			},
		},
	}

	parts, err := RenderedPromptToContentParts(rendered)
	require.NoError(t, err)
	require.Len(t, parts, 2)

	textContent1, ok := parts[0][0].(TextContent)
	require.True(t, ok)
	assert.Equal(t, "First message", textContent1.Text)

	textContent2, ok := parts[1][0].(TextContent)
	require.True(t, ok)
	assert.Equal(t, "Second message", textContent2.Text)
}

func TestRenderedPromptToContentParts_IgnoresUnsupportedParts(t *testing.T) {
	rendered := dotprompt.RenderedPrompt{
		Messages: []dotprompt.Message{
			{
				Content: []dotprompt.Part{
					&dotprompt.TextPart{
						Text: "Text content",
					},
					&dotprompt.DataPart{
						Data: map[string]any{"key": "value"},
					},
					&dotprompt.ToolRequestPart{},
				},
			},
		},
	}

	parts, err := RenderedPromptToContentParts(rendered)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	// Only the text part should be included
	require.Len(t, parts[0], 1)

	textContent, ok := parts[0][0].(TextContent)
	require.True(t, ok)
	assert.Equal(t, "Text content", textContent.Text)
}

func TestRenderedPromptToContentParts_InvalidDataURI(t *testing.T) {
	rendered := dotprompt.RenderedPrompt{
		Messages: []dotprompt.Message{
			{
				Content: []dotprompt.Part{
					&dotprompt.MediaPart{
						Media: dotprompt.Media{
							URL: "data:invalid",
						},
					},
				},
			},
		},
	}

	_, err := RenderedPromptToContentParts(rendered)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing data URI")
}
