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

package template

import (
	"context"
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/google/dotprompt/go/dotprompt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentToParts_SimpleTextOnly(t *testing.T) {
	// Simple document with text fields only
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title":   "Hello World",
			"content": "This is a test document",
			"author":  "Test Author",
		},
	}

	template := `Title: {{title}}
Content: {{content}}
Author: {{author}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	// Should have at least one text part
	hasText := false
	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			if textPart, ok := part.(*dotprompt.TextPart); ok {
				hasText = true
				assert.Contains(t, textPart.Text, "Hello World")
				assert.Contains(t, textPart.Text, "This is a test document")
				assert.Contains(t, textPart.Text, "Test Author")
			}
		}
	}
	assert.True(t, hasText, "Should have at least one text part")
}

func TestDocumentToParts_WithConditionals(t *testing.T) {
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title":       "Test",
			"hasMetadata": true,
			"metadata":    "Some metadata",
		},
	}

	template := `Title: {{title}}
{{#if hasMetadata}}
Metadata: {{metadata}}
{{/if}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	// Check that conditional was evaluated
	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			if textPart, ok := part.(*dotprompt.TextPart); ok {
				assert.Contains(t, textPart.Text, "Metadata: Some metadata")
			}
		}
	}
}

func TestDocumentToParts_WithLoop(t *testing.T) {
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title": "My List",
			"items": []string{"Item 1", "Item 2", "Item 3"},
		},
	}

	template := `Title: {{title}}
Items:
{{#each items}}
- {{this}}
{{/each}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	// Check that loop was evaluated
	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			if textPart, ok := part.(*dotprompt.TextPart); ok {
				assert.Contains(t, textPart.Text, "Item 1")
				assert.Contains(t, textPart.Text, "Item 2")
				assert.Contains(t, textPart.Text, "Item 3")
			}
		}
	}
}

func TestDocumentToParts_WithNestedFields(t *testing.T) {
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"user": map[string]any{
				"name":  "John Doe",
				"email": "john@example.com",
			},
		},
	}

	template := `Name: {{user.name}}
Email: {{user.email}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	// Check nested field access
	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			if textPart, ok := part.(*dotprompt.TextPart); ok {
				assert.Contains(t, textPart.Text, "John Doe")
				assert.Contains(t, textPart.Text, "john@example.com")
			}
		}
	}
}

func TestDocumentToParts_FiltersInternalFields(t *testing.T) {
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title":       "Public Title",
			"_embeddings": []float64{0.1, 0.2, 0.3}, // Should be filtered out
		},
	}

	template := `Title: {{title}}
Embeddings: {{_embeddings}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	// _embeddings should not appear in output
	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			if textPart, ok := part.(*dotprompt.TextPart); ok {
				assert.Contains(t, textPart.Text, "Public Title")
				// The _embeddings field should be filtered, so template will show empty
				assert.NotContains(t, textPart.Text, "0.1")
			}
		}
	}
}

func TestDocumentToParts_EmptyDocument(t *testing.T) {
	doc := schema.Document{
		ID:     "doc1",
		Fields: map[string]any{},
	}

	template := `This is a static template`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	// Should still render the static content
	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			if textPart, ok := part.(*dotprompt.TextPart); ok {
				assert.Contains(t, textPart.Text, "static template")
			}
		}
	}
}

func TestDocumentToParts_InvalidTemplate(t *testing.T) {
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title": "Test",
		},
	}

	// Invalid Handlebars syntax
	template := `{{#if unclosed`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	_, err := DocumentToParts(ctx, dp, doc, template)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "compiling template")
}

func TestDocumentToParts_WithMediaDirective(t *testing.T) {
	// Document with a data URI (already processed)
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title":    "Image Document",
			"photoUrl": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
		},
	}

	// Use dotprompt's {{media}} helper for the data URI
	template := `Title: {{title}}
{{#if photoUrl}}
{{media url=photoUrl}}
{{/if}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	// Should have both text and media parts
	hasText := false
	hasMedia := false

	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			switch p := part.(type) {
			case *dotprompt.TextPart:
				hasText = true
				assert.Contains(t, p.Text, "Image Document")
			case *dotprompt.MediaPart:
				hasMedia = true
				// Media part should have the data URI
				assert.NotEmpty(t, p.Media)
			}
		}
	}

	assert.True(t, hasText, "Should have text part")
	assert.True(t, hasMedia, "Should have media part")
}

func TestDocumentToParts_ComplexDocument(t *testing.T) {
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title": "Product Review",
			"product": map[string]any{
				"name":  "Widget Pro",
				"price": 99.99,
			},
			"tags":   []string{"electronics", "featured", "new"},
			"rating": 4.5,
			"review": "This is an excellent product with great features.",
		},
	}

	template := `Product Review: {{title}}

Product: {{product.name}} - ${{product.price}}

Tags:
{{#each tags}}
- {{this}}
{{/each}}

Rating: {{rating}}/5

Review: {{review}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	require.NotEmpty(t, rendered.Messages)

	for _, msg := range rendered.Messages {
		for _, part := range msg.Content {
			if textPart, ok := part.(*dotprompt.TextPart); ok {
				// Verify all fields are rendered
				assert.Contains(t, textPart.Text, "Product Review")
				assert.Contains(t, textPart.Text, "Widget Pro")
				assert.Contains(t, textPart.Text, "99.99")
				assert.Contains(t, textPart.Text, "electronics")
				assert.Contains(t, textPart.Text, "featured")
				assert.Contains(t, textPart.Text, "new")
				assert.Contains(t, textPart.Text, "4.5")
				assert.Contains(t, textPart.Text, "excellent product")
			}
		}
	}
}

func TestDocumentToParts_MultipleMessages(t *testing.T) {
	// Test that parts from all messages are returned
	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"content": "Test content",
		},
	}

	// Simple template
	template := `{{content}}`

	ctx := context.Background()
	dp := dotprompt.NewDotprompt(nil)
	rendered, err := DocumentToParts(ctx, dp, doc, template)

	require.NoError(t, err)
	assert.NotEmpty(t, rendered.Messages, "Should return messages from rendered prompt")
}
