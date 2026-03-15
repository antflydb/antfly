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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRender(t *testing.T) {
	tests := []struct {
		name      string
		template  string
		context   map[string]any
		want      string
		wantError bool
	}{
		{
			name:     "simple string substitution",
			template: "Hello, {{name}}!",
			context:  map[string]any{"name": "World"},
			want:     "Hello, World!",
		},
		{
			name:     "multiple substitutions",
			template: "{{greeting}}, {{name}}! Today is {{day}}.",
			context: map[string]any{
				"greeting": "Hello",
				"name":     "Alice",
				"day":      "Monday",
			},
			want: "Hello, Alice! Today is Monday.",
		},
		{
			name:     "with conditional",
			template: "{{#if premium}}Premium user: {{name}}{{else}}Standard user: {{name}}{{/if}}",
			context: map[string]any{
				"name":    "Bob",
				"premium": true,
			},
			want: "Premium user: Bob",
		},
		{
			name:     "with each loop",
			template: "Items: {{#each items}}{{this}}, {{/each}}",
			context: map[string]any{
				"items": []string{"apple", "banana", "cherry"},
			},
			want: "Items: apple, banana, cherry, ",
		},
		{
			name:     "nested map access",
			template: "User: {{user.name}} ({{user.email}})",
			context: map[string]any{
				"user": map[string]any{
					"name":  "John Doe",
					"email": "john@example.com",
				},
			},
			want: "User: John Doe (john@example.com)",
		},
		{
			name:     "with integer values",
			template: "You have {{count}} new messages",
			context:  map[string]any{"count": 5},
			want:     "You have 5 new messages",
		},
		{
			name:     "empty context",
			template: "Static text without variables",
			context:  map[string]any{},
			want:     "Static text without variables",
		},
		{
			name:     "missing variable",
			template: "Hello, {{name}}!",
			context:  map[string]any{},
			want:     "Hello, !",
		},
		{
			name:      "invalid template syntax",
			template:  "Hello, {{name}",
			context:   map[string]any{"name": "World"},
			wantError: true,
		},
		{
			name: "multiline template",
			template: `Dear {{name}},

Thank you for your order #{{orderID}}.
Your total is: ${{total}}

Best regards,
The Team`,
			context: map[string]any{
				"name":    "Customer",
				"orderID": "12345",
				"total":   "99.99",
			},
			want: `Dear Customer,

Thank you for your order #12345.
Your total is: $99.99

Best regards,
The Team`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Render(tt.template, tt.context)

			if tt.wantError {
				require.Error(t, err, "Render() expected error but got none")
				return
			}

			require.NoError(t, err, "Render() unexpected error")
			assert.Equal(t, tt.want, got, "Render() mismatch")
		})
	}
}

func TestRenderTemplate_ErrCases(t *testing.T) {
	tests := []struct {
		name     string
		template string
		context  map[string]any
	}{
		{
			name:     "unclosed action",
			template: "{{#if condition}} no end",
			context:  map[string]any{"condition": true},
		},
		{
			name:     "invalid syntax - missing closing brace",
			template: "{{name}",
			context:  map[string]any{"name": "test"},
		},
		{
			name:     "mismatched block helpers",
			template: "{{#if test}}content{{/unless}}",
			context:  map[string]any{"test": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Render(tt.template, tt.context)
			require.Error(
				t,
				err,
				"Render() expected error for case %q but got none",
				tt.name,
			)
			// Verify error message contains useful information
			assert.Contains(
				t,
				err.Error(),
				"failed to",
				"Expected error message to contain 'failed to'",
			)
		})
	}
}

func TestRenderTemplate_ComplexTypes(t *testing.T) {
	// Test with various complex types that might be used in real scenarios
	type User struct {
		Name  string
		Email string
		Age   int
	}

	context := map[string]any{
		"users": []User{
			{Name: "Alice", Email: "alice@example.com", Age: 30},
			{Name: "Bob", Email: "bob@example.com", Age: 25},
		},
		"config": map[string]any{
			"debug":   true,
			"version": "1.2.3",
		},
	}

	template := `Users:
{{#each users}}- {{Name}} ({{Email}}, age: {{Age}})
{{/each}}Debug mode: {{config.debug}}
Version: {{config.version}}`

	want := `Users:
- Alice (alice@example.com, age: 30)
- Bob (bob@example.com, age: 25)
Debug mode: true
Version: 1.2.3`

	got, err := Render(template, context)
	require.NoError(t, err, "Render() unexpected error")
	assert.Equal(t, want, got, "Render() mismatch")
}

func TestRenderTemplate_CacheHit(t *testing.T) {
	// Clear cache to start fresh
	ClearTemplateCache()

	template := "Hello, {{name}}!"
	context1 := map[string]any{"name": "World"}
	context2 := map[string]any{"name": "Universe"}

	// First render - should cache the template
	result1, err := Render(template, context1)
	require.NoError(t, err, "First render failed")
	assert.Equal(t, "Hello, World!", result1, "First render mismatch")

	// Second render with same template but different context - should use cached template
	result2, err := Render(template, context2)
	require.NoError(t, err, "Second render failed")
	assert.Equal(t, "Hello, Universe!", result2, "Second render mismatch")
}

func TestSetTemplateCacheTTL(t *testing.T) {
	// Set a short TTL
	SetTemplateCacheTTL(100 * time.Millisecond)

	template := "TTL test: {{value}}"
	context := map[string]any{"value": "test"}

	// First render
	_, err := Render(template, context)
	require.NoError(t, err, "Render failed")

	// Wait for TTL to expire
	time.Sleep(200 * time.Millisecond)

	// Render again - should re-parse the template
	_, err = Render(template, context)
	require.NoError(t, err, "Render after TTL failed")

	// Reset to default TTL
	SetTemplateCacheTTL(5 * time.Minute)
}

func TestRenderNoCache(t *testing.T) {
	template := "No cache: {{value}}"
	context := map[string]any{"value": "test"}

	result, err := RenderNoCache(template, context)
	require.NoError(t, err, "RenderTemplateNoCache failed")
	assert.Equal(t, "No cache: test", result, "RenderTemplateNoCache mismatch")
}

func TestClearTemplateCache(t *testing.T) {
	// Ensure cache is populated
	template := "Clear test: {{value}}"
	context := map[string]any{"value": "test"}

	_, err := Render(template, context)
	require.NoError(t, err, "Initial render failed")

	// Clear the cache
	ClearTemplateCache()

	// Render again - should work fine even after clearing
	_, err = Render(template, context)
	require.NoError(t, err, "Render after clear failed")
}

func BenchmarkRender(b *testing.B) {
	template := "Hello {{name}}, you have {{count}} messages from {{sender}}."
	context := map[string]any{
		"name":   "User",
		"count":  42,
		"sender": "System",
	}

	for b.Loop() {
		_, err := Render(template, context)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRenderNoCache(b *testing.B) {
	template := "Hello {{name}}, you have {{count}} messages from {{sender}}."
	context := map[string]any{
		"name":   "User",
		"count":  42,
		"sender": "System",
	}

	for b.Loop() {
		_, err := RenderNoCache(template, context)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRenderTemplate_CacheHit(b *testing.B) {
	// Pre-populate cache
	template := "Cached: {{name}}, {{count}}, {{sender}}"
	warmupContext := map[string]any{
		"name":   "Warmup",
		"count":  0,
		"sender": "System",
	}
	_, _ = Render(template, warmupContext)

	context := map[string]any{
		"name":   "User",
		"count":  42,
		"sender": "System",
	}

	for b.Loop() {
		_, err := Render(template, context)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestRenderHandlebars(t *testing.T) {
	tests := []struct {
		name      string
		template  string
		context   map[string]any
		want      string
		wantError bool
	}{
		{
			name:     "simple string substitution",
			template: "Hello, {{name}}!",
			context:  map[string]any{"name": "World"},
			want:     "Hello, World!",
		},
		{
			name:     "multiple substitutions",
			template: "{{greeting}}, {{name}}! Today is {{day}}.",
			context: map[string]any{
				"greeting": "Hello",
				"name":     "Alice",
				"day":      "Monday",
			},
			want: "Hello, Alice! Today is Monday.",
		},
		{
			name:     "with conditional - true",
			template: "{{#if premium}}Premium user: {{name}}{{else}}Standard user: {{name}}{{/if}}",
			context: map[string]any{
				"name":    "Bob",
				"premium": true,
			},
			want: "Premium user: Bob",
		},
		{
			name:     "with conditional - false",
			template: "{{#if premium}}Premium user: {{name}}{{else}}Standard user: {{name}}{{/if}}",
			context: map[string]any{
				"name":    "Bob",
				"premium": false,
			},
			want: "Standard user: Bob",
		},
		{
			name:     "with each loop",
			template: "Items: {{#each items}}{{this}}, {{/each}}",
			context: map[string]any{
				"items": []string{"apple", "banana", "cherry"},
			},
			want: "Items: apple, banana, cherry, ",
		},
		{
			name:     "nested map access",
			template: "User: {{user.name}} ({{user.email}})",
			context: map[string]any{
				"user": map[string]any{
					"name":  "John Doe",
					"email": "john@example.com",
				},
			},
			want: "User: John Doe (john@example.com)",
		},
		{
			name:     "with integer values",
			template: "You have {{count}} new messages",
			context:  map[string]any{"count": 5},
			want:     "You have 5 new messages",
		},
		{
			name:     "empty context",
			template: "Static text without variables",
			context:  map[string]any{},
			want:     "Static text without variables",
		},
		{
			name:     "missing variable",
			template: "Hello, {{name}}!",
			context:  map[string]any{},
			want:     "Hello, !",
		},
		{
			name:      "invalid template syntax - unclosed block",
			template:  "Hello, {{#if name}}",
			context:   map[string]any{"name": "World"},
			wantError: true,
		},
		{
			name: "multiline template",
			template: `Dear {{name}},

Thank you for your order #{{orderID}}.
Your total is: ${{total}}

Best regards,
The Team`,
			context: map[string]any{
				"name":    "Customer",
				"orderID": "12345",
				"total":   "99.99",
			},
			want: `Dear Customer,

Thank you for your order #12345.
Your total is: $99.99

Best regards,
The Team`,
		},
		{
			name:     "with unless helper",
			template: "{{#unless logged_in}}Please log in{{/unless}}",
			context: map[string]any{
				"logged_in": false,
			},
			want: "Please log in",
		},
		{
			name:     "with each and @index",
			template: "{{#each items}}{{@index}}: {{this}} {{/each}}",
			context: map[string]any{
				"items": []string{"first", "second", "third"},
			},
			want: "0: first 1: second 2: third ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RenderHandlebars(tt.template, tt.context)

			if tt.wantError {
				require.Error(t, err, "RenderHandlebars() expected error but got none")
				return
			}

			require.NoError(t, err, "RenderHandlebars() unexpected error")
			assert.Equal(t, tt.want, got, "RenderHandlebars() mismatch")
		})
	}
}

func TestRenderHandlebarsTemplate_ErrCases(t *testing.T) {
	tests := []struct {
		name     string
		template string
		context  map[string]any
	}{
		{
			name:     "unclosed block helper",
			template: "{{#if condition}} no end",
			context:  map[string]any{"condition": true},
		},
		{
			name:     "invalid syntax - missing closing brace",
			template: "{{name}",
			context:  map[string]any{"name": "test"},
		},
		{
			name:     "mismatched block helpers",
			template: "{{#if test}}content{{/unless}}",
			context:  map[string]any{"test": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RenderHandlebars(tt.template, tt.context)
			require.Error(
				t,
				err,
				"RenderHandlebars() expected error for case %q but got none",
				tt.name,
			)
			// Verify error message contains useful information
			assert.Contains(
				t,
				err.Error(),
				"failed to",
				"Expected error message to contain 'failed to'",
			)
		})
	}
}

func TestRenderHandlebarsTemplate_ComplexTypes(t *testing.T) {
	// Test with various complex types that might be used in real scenarios
	type User struct {
		Name  string
		Email string
		Age   int
	}

	context := map[string]any{
		"users": []User{
			{Name: "Alice", Email: "alice@example.com", Age: 30},
			{Name: "Bob", Email: "bob@example.com", Age: 25},
		},
		"config": map[string]any{
			"debug":   true,
			"version": "1.2.3",
		},
	}

	template := `Users:
{{#each users}}- {{Name}} ({{Email}}, age: {{Age}})
{{/each}}Debug mode: {{config.debug}}
Version: {{config.version}}`

	want := `Users:
- Alice (alice@example.com, age: 30)
- Bob (bob@example.com, age: 25)
Debug mode: true
Version: 1.2.3`

	got, err := RenderHandlebars(template, context)
	require.NoError(t, err, "RenderHandlebars() unexpected error")
	assert.Equal(t, want, got, "RenderHandlebars() mismatch")
}

func TestRenderHandlebarsTemplate_CacheHit(t *testing.T) {
	// Clear cache to start fresh
	ClearTemplateCache()

	template := "Hello, {{name}}!"
	context1 := map[string]any{"name": "World"}
	context2 := map[string]any{"name": "Universe"}

	// First render - should cache the template
	result1, err := RenderHandlebars(template, context1)
	require.NoError(t, err, "First render failed")
	assert.Equal(t, "Hello, World!", result1, "First render mismatch")

	// Second render with same template but different context - should use cached template
	result2, err := RenderHandlebars(template, context2)
	require.NoError(t, err, "Second render failed")
	assert.Equal(t, "Hello, Universe!", result2, "Second render mismatch")
}

func TestRenderHandlebarsNoCache(t *testing.T) {
	template := "No cache: {{value}}"
	context := map[string]any{"value": "test"}

	result, err := RenderHandlebarsNoCache(template, context)
	require.NoError(t, err, "RenderHandlebarsTemplateNoCache failed")
	assert.Equal(t, "No cache: test", result, "RenderHandlebarsTemplateNoCache mismatch")
}

func TestClearTemplateCache_Handlebars(t *testing.T) {
	// Ensure cache is populated
	template := "Clear test: {{value}}"
	context := map[string]any{"value": "test"}

	_, err := RenderHandlebars(template, context)
	require.NoError(t, err, "Initial render failed")

	// Clear the cache
	ClearTemplateCache()

	// Render again - should work fine even after clearing
	_, err = RenderHandlebars(template, context)
	require.NoError(t, err, "Render after clear failed")
}

func TestSetTemplateCacheTTL_Handlebars(t *testing.T) {
	// Set a short TTL
	SetTemplateCacheTTL(100 * time.Millisecond)

	template := "TTL test: {{value}}"
	context := map[string]any{"value": "test"}

	// First render
	_, err := RenderHandlebars(template, context)
	require.NoError(t, err, "Render failed")

	// Wait for TTL to expire
	time.Sleep(200 * time.Millisecond)

	// Render again - should re-parse the template
	_, err = RenderHandlebars(template, context)
	require.NoError(t, err, "Render after TTL failed")

	// Reset to default TTL
	SetTemplateCacheTTL(5 * time.Minute)
}

func BenchmarkRenderHandlebars(b *testing.B) {
	template := "Hello {{name}}, you have {{count}} messages from {{sender}}."
	context := map[string]any{
		"name":   "User",
		"count":  42,
		"sender": "System",
	}

	for b.Loop() {
		_, err := RenderHandlebars(template, context)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRenderHandlebarsNoCache(b *testing.B) {
	template := "Hello {{name}}, you have {{count}} messages from {{sender}}."
	context := map[string]any{
		"name":   "User",
		"count":  42,
		"sender": "System",
	}

	for b.Loop() {
		_, err := RenderHandlebarsNoCache(template, context)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRenderHandlebarsTemplate_CacheHit(b *testing.B) {
	// Pre-populate cache
	template := "Cached: {{name}}, {{count}}, {{sender}}"
	warmupContext := map[string]any{
		"name":   "Warmup",
		"count":  0,
		"sender": "System",
	}
	_, _ = RenderHandlebars(template, warmupContext)

	context := map[string]any{
		"name":   "User",
		"count":  42,
		"sender": "System",
	}

	for b.Loop() {
		_, err := RenderHandlebars(template, context)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestScrubHtml_Render(t *testing.T) {
	tests := []struct {
		name     string
		template string
		context  map[string]any
		want     string
	}{
		{
			name:     "simple HTML with script and style tags",
			template: `Product Text: {{scrubHtml description}}`,
			context: map[string]any{
				"description": `
					<h2>Awesome T-Shirt</h2>
					<p>This is the <strong>best t-shirt ever</strong>. Buy it now!</p>
					<style>.blue { color: blue; }</style>
					<ul>
						<li>100% Cotton</li>
						<li>Made in USA</li>
					</ul>
					<script>console.log("you don't want this text");</script>
				`,
			},
			want: "Product Text: Awesome T-Shirt\n\n\t\t\t\t\tThis is the best t-shirt ever. Buy it now!\n\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t100% Cotton\n\n\t\t\t\t\t\tMade in USA",
		},
		{
			name:     "HTML with only text",
			template: `Clean: {{scrubHtml text}}`,
			context: map[string]any{
				"text": `<p>Just some plain text</p>`,
			},
			want: "Clean: Just some plain text",
		},
		{
			name:     "HTML with nested script tags",
			template: `{{scrubHtml html}}`,
			context: map[string]any{
				"html": `
					<div>
						<p>Visible content</p>
						<script type="text/javascript">
							var x = 10;
							console.log("hidden");
						</script>
						<p>More visible content</p>
					</div>
				`,
			},
			want: "Visible content\n\n\t\t\t\t\t\t\n\t\t\t\t\t\tMore visible content",
		},
		{
			name:     "empty HTML",
			template: `Result: {{scrubHtml html}}`,
			context: map[string]any{
				"html": ``,
			},
			want: "Result: ",
		},
		{
			name:     "plain text without HTML tags",
			template: `{{scrubHtml text}}`,
			context: map[string]any{
				"text": `This is plain text without any HTML tags`,
			},
			want: "This is plain text without any HTML tags",
		},
		{
			name:     "HTML with inline styles",
			template: `{{scrubHtml html}}`,
			context: map[string]any{
				"html": `<p style="color: red;">Red text</p><p>Normal text</p>`,
			},
			want: "Red text\nNormal text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Render(tt.template, tt.context)
			require.NoError(t, err, "Render() unexpected error")
			assert.Equal(t, tt.want, got, "Render() mismatch")
		})
	}
}

func TestEncodeToon(t *testing.T) {
	tests := []struct {
		name     string
		template string
		context  map[string]any
		want     string
	}{
		{
			name:     "simple object with default options",
			template: `{{encodeToon fields}}`,
			context: map[string]any{
				"fields": map[string]any{
					"name":  "Alice",
					"email": "alice@example.com",
					"age":   30,
				},
			},
			want: "age: 30\nemail: alice@example.com\nname: Alice",
		},
		{
			name:     "with lengthMarker disabled",
			template: `{{encodeToon items lengthMarker=false}}`,
			context: map[string]any{
				"items": []string{"apple", "banana", "cherry"},
			},
			want: "[3]: apple,banana,cherry",
		},
		{
			name:     "with custom indent",
			template: `{{encodeToon data indent=4}}`,
			context: map[string]any{
				"data": map[string]any{
					"user": map[string]any{
						"name": "Bob",
						"age":  25,
					},
				},
			},
			want: "user:\n    age: 25\n    name: Bob",
		},
		{
			name:     "array with lengthMarker enabled",
			template: `{{encodeToon numbers}}`,
			context: map[string]any{
				"numbers": []int{1, 2, 3, 4, 5},
			},
			want: "[#5]: 1,2,3,4,5",
		},
		{
			name:     "nested structure",
			template: `{{encodeToon doc}}`,
			context: map[string]any{
				"doc": map[string]any{
					"title":  "RAG Document",
					"author": "System",
					"tags":   []string{"ai", "search", "rag"},
				},
			},
			want: "author: System\ntags[#3]: ai,search,rag\ntitle: RAG Document",
		},
		{
			name:     "with delimiter option",
			template: `{{encodeToon data delimiter="\t"}}`,
			context: map[string]any{
				"data": []map[string]any{
					{"name": "Alice", "age": 30},
					{"name": "Bob", "age": 25},
				},
			},
			want: "[#2\\t]{age\\tname}:\n  30\\tAlice\n  25\\tBob",
		},
		{
			name:     "multiple options combined",
			template: `{{encodeToon fields lengthMarker=false indent=4}}`,
			context: map[string]any{
				"fields": map[string]any{
					"title": "Example",
					"data":  []int{1, 2, 3},
				},
			},
			want: "data[3]: 1,2,3\ntitle: Example",
		},
		{
			name:     "empty object",
			template: `{{encodeToon empty}}`,
			context: map[string]any{
				"empty": map[string]any{},
			},
			want: "",
		},
		{
			name:     "primitives",
			template: `{{encodeToon value}}`,
			context: map[string]any{
				"value": "simple string",
			},
			want: "simple string",
		},
		{
			name:     "boolean and null values",
			template: `{{encodeToon config}}`,
			context: map[string]any{
				"config": map[string]any{
					"enabled": true,
					"debug":   false,
					"cache":   nil,
				},
			},
			want: "cache: null\ndebug: false\nenabled: true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Render(tt.template, tt.context)
			require.NoError(t, err, "Render() unexpected error")
			assert.Equal(t, tt.want, got, "Render() mismatch")
		})
	}
}

func TestEncodeToon_DocumentRendering(t *testing.T) {
	// Test realistic RAG document rendering scenarios
	tests := []struct {
		name     string
		template string
		context  map[string]any
		contains []string // Check if output contains these substrings
	}{
		{
			name: "RAG document with all fields",
			template: `Document {{id}}:
{{encodeToon fields}}`,
			context: map[string]any{
				"id": "doc_123",
				"fields": map[string]any{
					"title":       "Introduction to Vector Search",
					"content":     "Vector search is a technique...",
					"author":      "Jane Doe",
					"published":   "2024-01-15",
					"tags":        []string{"vector", "search", "ai"},
					"page_count":  42,
					"is_featured": true,
				},
			},
			contains: []string{
				"Document doc_123:",
				"title: Introduction to Vector Search",
				"author: Jane Doe",
				"tags[#3]:",
				"vector",
				"search",
				"ai",
			},
		},
		{
			name: "multiple documents in loop",
			template: `{{#each documents}}Document {{this.id}}:
{{encodeToon this.fields}}
---
{{/each}}`,
			context: map[string]any{
				"documents": []map[string]any{
					{
						"id": "doc_1",
						"fields": map[string]any{
							"title": "First Doc",
						},
					},
					{
						"id": "doc_2",
						"fields": map[string]any{
							"title": "Second Doc",
						},
					},
				},
			},
			contains: []string{
				"Document doc_1:",
				"title: First Doc",
				"Document doc_2:",
				"title: Second Doc",
				"---",
			},
		},
		{
			name:     "compact rendering with no lengthMarker",
			template: `{{encodeToon fields lengthMarker=false indent=0}}`,
			context: map[string]any{
				"fields": map[string]any{
					"name": "Compact",
					"type": "example",
				},
			},
			contains: []string{
				"name: Compact",
				"type: example",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Render(tt.template, tt.context)
			require.NoError(t, err, "Render() unexpected error")

			for _, substr := range tt.contains {
				assert.Contains(t, got, substr, "Expected output to contain substring")
			}
		})
	}
}

func TestScrubHtml_Handlebars(t *testing.T) {
	tests := []struct {
		name     string
		template string
		context  map[string]any
		want     string
	}{
		{
			name:     "simple HTML with script and style tags",
			template: `Product Text: {{scrubHtml description}}`,
			context: map[string]any{
				"description": `
					<h2>Awesome T-Shirt</h2>
					<p>This is the <strong>best t-shirt ever</strong>. Buy it now!</p>
					<style>.blue { color: blue; }</style>
					<ul>
						<li>100% Cotton</li>
						<li>Made in USA</li>
					</ul>
					<script>console.log("you don't want this text");</script>
				`,
			},
			want: "Product Text: Awesome T-Shirt\n\n\t\t\t\t\tThis is the best t-shirt ever. Buy it now!\n\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t100% Cotton\n\n\t\t\t\t\t\tMade in USA",
		},
		{
			name:     "HTML with only text",
			template: `Clean: {{scrubHtml text}}`,
			context: map[string]any{
				"text": `<p>Just some plain text</p>`,
			},
			want: "Clean: Just some plain text",
		},
		{
			name:     "HTML with nested script tags",
			template: `{{scrubHtml html}}`,
			context: map[string]any{
				"html": `
					<div>
						<p>Visible content</p>
						<script type="text/javascript">
							var x = 10;
							console.log("hidden");
						</script>
						<p>More visible content</p>
					</div>
				`,
			},
			want: "Visible content\n\n\t\t\t\t\t\t\n\t\t\t\t\t\tMore visible content",
		},
		{
			name:     "empty HTML",
			template: `Result: {{scrubHtml html}}`,
			context: map[string]any{
				"html": ``,
			},
			want: "Result: ",
		},
		{
			name:     "plain text without HTML tags",
			template: `{{scrubHtml text}}`,
			context: map[string]any{
				"text": `This is plain text without any HTML tags`,
			},
			want: "This is plain text without any HTML tags",
		},
		{
			name:     "HTML with inline styles",
			template: `{{scrubHtml html}}`,
			context: map[string]any{
				"html": `<p style="color: red;">Red text</p><p>Normal text</p>`,
			},
			want: "Red text\nNormal text",
		},
		{
			name:     "complex e-commerce product description",
			template: `{{scrubHtml productHtml}}`,
			context: map[string]any{
				"productHtml": `
					<div class="product-description">
						<h1>Premium Cotton T-Shirt</h1>
						<script>trackProductView('shirt-001');</script>
						<p>Experience ultimate comfort with our <strong>premium cotton t-shirt</strong>.</p>
						<style>
							.product-description { padding: 20px; }
							.highlight { color: #007bff; }
						</style>
						<h2>Features:</h2>
						<ul>
							<li>100% organic cotton</li>
							<li>Breathable fabric</li>
							<li>Machine washable</li>
						</ul>
						<script async src="analytics.js"></script>
					</div>
				`,
			},
			want: "Premium Cotton T-Shirt\n\n\t\t\t\t\t\t\n\t\t\t\t\t\tExperience ultimate comfort with our premium cotton t-shirt.\n\n\t\t\t\t\t\t\n\t\t\t\t\t\tFeatures:\n\n\t\t\t\t\t\t\n\t\t\t\t\t\t\t100% organic cotton\n\n\t\t\t\t\t\t\tBreathable fabric\n\n\t\t\t\t\t\t\tMachine washable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RenderHandlebars(tt.template, tt.context)
			require.NoError(t, err, "RenderHandlebars() unexpected error")
			assert.Equal(t, tt.want, got, "RenderHandlebars() mismatch")
		})
	}
}
