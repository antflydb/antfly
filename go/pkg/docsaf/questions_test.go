package docsaf

import (
	"strings"
	"testing"
)

func TestQuestionsExtractor_ExtractFromFrontmatter_StringArray(t *testing.T) {
	extractor := &QuestionsExtractor{}

	frontmatter := map[string]any{
		"title": "Test Document",
		"questions": []any{
			"How do I install this?",
			"What are the requirements?",
			"Where can I find help?",
		},
	}

	questions := extractor.extractFromFrontmatter("test.md", "https://example.com/test.md", frontmatter)

	if len(questions) != 3 {
		t.Fatalf("Expected 3 questions, got %d", len(questions))
	}

	// Check first question
	if questions[0].Text != "How do I install this?" {
		t.Errorf("Expected first question text 'How do I install this?', got %q", questions[0].Text)
	}
	if questions[0].SourceType != "frontmatter" {
		t.Errorf("Expected source type 'frontmatter', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "Test Document" {
		t.Errorf("Expected context 'Test Document', got %q", questions[0].Context)
	}
	if questions[0].SourcePath != "test.md" {
		t.Errorf("Expected source path 'test.md', got %q", questions[0].SourcePath)
	}
	if questions[0].SourceURL != "https://example.com/test.md" {
		t.Errorf("Expected source URL 'https://example.com/test.md', got %q", questions[0].SourceURL)
	}
}

func TestQuestionsExtractor_ExtractFromFrontmatter_ObjectArray(t *testing.T) {
	extractor := &QuestionsExtractor{}

	frontmatter := map[string]any{
		"title": "API Docs",
		"questions": []any{
			map[string]any{
				"text":     "How do I authenticate?",
				"category": "auth",
				"priority": "high",
			},
			map[string]any{
				"text":     "What are the rate limits?",
				"category": "limits",
			},
		},
	}

	questions := extractor.extractFromFrontmatter("api.mdx", "", frontmatter)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	// Check first question with metadata
	if questions[0].Text != "How do I authenticate?" {
		t.Errorf("Expected first question text 'How do I authenticate?', got %q", questions[0].Text)
	}
	if questions[0].Metadata["category"] != "auth" {
		t.Errorf("Expected category 'auth', got %v", questions[0].Metadata["category"])
	}
	if questions[0].Metadata["priority"] != "high" {
		t.Errorf("Expected priority 'high', got %v", questions[0].Metadata["priority"])
	}

	// Second question
	if questions[1].Text != "What are the rate limits?" {
		t.Errorf("Expected second question text 'What are the rate limits?', got %q", questions[1].Text)
	}
}

func TestQuestionsExtractor_ExtractFromFrontmatter_NoQuestions(t *testing.T) {
	extractor := &QuestionsExtractor{}

	frontmatter := map[string]any{
		"title":  "No Questions Doc",
		"author": "Test Author",
	}

	questions := extractor.extractFromFrontmatter("test.md", "", frontmatter)

	if len(questions) != 0 {
		t.Errorf("Expected 0 questions when no 'questions' field, got %d", len(questions))
	}
}

func TestQuestionsExtractor_ExtractFromQuestionsComponents(t *testing.T) {
	extractor := &QuestionsExtractor{}

	content := []byte(`# Getting Started

<Questions>
- How do I install Antfly?
- Where can I download the CLI?
- What are the system requirements?
</Questions>

Some content here.

<Questions>
* Can I use this in production?
* How do I upgrade?
</Questions>
`)

	questions := extractor.extractFromQuestionsComponents("guide.mdx", "https://example.com/guide", content)

	if len(questions) != 5 {
		t.Fatalf("Expected 5 questions, got %d", len(questions))
	}

	// Check questions extracted from first component
	if questions[0].Text != "How do I install Antfly?" {
		t.Errorf("Expected 'How do I install Antfly?', got %q", questions[0].Text)
	}
	if questions[1].Text != "Where can I download the CLI?" {
		t.Errorf("Expected 'Where can I download the CLI?', got %q", questions[1].Text)
	}
	if questions[2].Text != "What are the system requirements?" {
		t.Errorf("Expected 'What are the system requirements?', got %q", questions[2].Text)
	}

	// Check questions from second component (using *)
	if questions[3].Text != "Can I use this in production?" {
		t.Errorf("Expected 'Can I use this in production?', got %q", questions[3].Text)
	}
	if questions[4].Text != "How do I upgrade?" {
		t.Errorf("Expected 'How do I upgrade?', got %q", questions[4].Text)
	}

	// Verify source type
	for _, q := range questions {
		if q.SourceType != "mdx_component" {
			t.Errorf("Expected source type 'mdx_component', got %q", q.SourceType)
		}
	}
}

func TestQuestionsExtractor_ExtractFromQuestionsComponents_Empty(t *testing.T) {
	extractor := &QuestionsExtractor{}

	content := []byte(`# No Questions Here

Just regular markdown content.
`)

	questions := extractor.extractFromQuestionsComponents("test.md", "", content)

	if len(questions) != 0 {
		t.Errorf("Expected 0 questions when no <Questions> component, got %d", len(questions))
	}
}

func TestQuestionsExtractor_ExtractFromMDXContent_Combined(t *testing.T) {
	extractor := &QuestionsExtractor{}

	frontmatter := map[string]any{
		"title": "Complete Guide",
		"questions": []any{
			"What is this guide about?",
		},
	}

	content := []byte(`Some intro text.

<Questions>
- How do I get started?
- Where is the documentation?
</Questions>
`)

	questions := extractor.ExtractFromMDXContent("guide.mdx", "https://example.com/guide", content, frontmatter)

	if len(questions) != 3 {
		t.Fatalf("Expected 3 questions (1 from frontmatter + 2 from component), got %d", len(questions))
	}

	// First should be from frontmatter
	if questions[0].SourceType != "frontmatter" {
		t.Errorf("Expected first question from 'frontmatter', got %q", questions[0].SourceType)
	}

	// Rest should be from mdx_component
	if questions[1].SourceType != "mdx_component" {
		t.Errorf("Expected second question from 'mdx_component', got %q", questions[1].SourceType)
	}
}

func TestQuestionsExtractor_ExtractFromOpenAPI_StringArray(t *testing.T) {
	extractor := &QuestionsExtractor{}

	extensions := map[string]any{
		"x-docsaf-questions": []any{
			"How do I call this endpoint?",
			"What authentication is required?",
		},
	}

	questions := extractor.ExtractFromOpenAPI(
		"api.yaml", "https://example.com/api.yaml",
		"openapi_operation", "getUsers",
		extensions,
	)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	if questions[0].Text != "How do I call this endpoint?" {
		t.Errorf("Expected 'How do I call this endpoint?', got %q", questions[0].Text)
	}
	if questions[0].SourceType != "openapi_operation" {
		t.Errorf("Expected source type 'openapi_operation', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "getUsers" {
		t.Errorf("Expected context 'getUsers', got %q", questions[0].Context)
	}
}

func TestQuestionsExtractor_ExtractFromOpenAPI_ObjectArray(t *testing.T) {
	extractor := &QuestionsExtractor{}

	extensions := map[string]any{
		"x-docsaf-questions": []any{
			map[string]any{
				"text":     "What fields are required?",
				"category": "validation",
			},
			map[string]any{
				"text": "How do I format dates?",
			},
		},
	}

	questions := extractor.ExtractFromOpenAPI(
		"api.yaml", "",
		"openapi_schema", "UserInput",
		extensions,
	)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	if questions[0].Text != "What fields are required?" {
		t.Errorf("Expected 'What fields are required?', got %q", questions[0].Text)
	}
	if questions[0].Metadata["category"] != "validation" {
		t.Errorf("Expected metadata category 'validation', got %v", questions[0].Metadata["category"])
	}
}

func TestQuestionsExtractor_ExtractFromOpenAPI_NoExtension(t *testing.T) {
	extractor := &QuestionsExtractor{}

	extensions := map[string]any{
		"x-other-extension": "some value",
	}

	questions := extractor.ExtractFromOpenAPI("api.yaml", "", "openapi_info", "API", extensions)

	if len(questions) != 0 {
		t.Errorf("Expected 0 questions when no x-docsaf-questions, got %d", len(questions))
	}
}

func TestMarkdownProcessor_ExtractQuestions(t *testing.T) {
	mp := &MarkdownProcessor{}

	content := []byte(`---
title: Installation Guide
questions:
  - How do I install on Windows?
  - How do I install on macOS?
---

# Getting Started

<Questions>
- What are the prerequisites?
- Where do I get support?
</Questions>

Follow these steps to install...
`)

	questions := mp.ExtractQuestions("install.mdx", "https://docs.example.com/install", content)

	if len(questions) != 4 {
		t.Fatalf("Expected 4 questions, got %d", len(questions))
	}

	// Check frontmatter questions
	frontmatterCount := 0
	componentCount := 0
	for _, q := range questions {
		switch q.SourceType {
		case "frontmatter":
			frontmatterCount++
		case "mdx_component":
			componentCount++
		}
	}

	if frontmatterCount != 2 {
		t.Errorf("Expected 2 frontmatter questions, got %d", frontmatterCount)
	}
	if componentCount != 2 {
		t.Errorf("Expected 2 component questions, got %d", componentCount)
	}
}

func TestMarkdownProcessor_ExtractQuestions_NoQuestions(t *testing.T) {
	mp := &MarkdownProcessor{}

	content := []byte(`---
title: Simple Doc
---

# Introduction

Just regular content without questions.
`)

	questions := mp.ExtractQuestions("simple.md", "", content)

	if len(questions) != 0 {
		t.Errorf("Expected 0 questions, got %d", len(questions))
	}
}

func TestQuestion_ToDocument(t *testing.T) {
	q := Question{
		ID:         "q_12345",
		Text:       "How do I install?",
		SourcePath: "guide.md",
		SourceURL:  "https://example.com/guide",
		SourceType: "frontmatter",
		Context:    "Installation Guide",
		Metadata: map[string]any{
			"category": "installation",
		},
	}

	doc := q.ToDocument()

	if doc["id"] != "q_12345" {
		t.Errorf("Expected id 'q_12345', got %v", doc["id"])
	}
	if doc["text"] != "How do I install?" {
		t.Errorf("Expected text 'How do I install?', got %v", doc["text"])
	}
	if doc["source_path"] != "guide.md" {
		t.Errorf("Expected source_path 'guide.md', got %v", doc["source_path"])
	}
	if doc["source_url"] != "https://example.com/guide" {
		t.Errorf("Expected source_url 'https://example.com/guide', got %v", doc["source_url"])
	}
	if doc["source_type"] != "frontmatter" {
		t.Errorf("Expected source_type 'frontmatter', got %v", doc["source_type"])
	}
	if doc["context"] != "Installation Guide" {
		t.Errorf("Expected context 'Installation Guide', got %v", doc["context"])
	}
	if doc["_type"] != "question" {
		t.Errorf("Expected _type 'question', got %v", doc["_type"])
	}

	metadata, ok := doc["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("Expected metadata to be map[string]any")
	}
	if metadata["category"] != "installation" {
		t.Errorf("Expected metadata category 'installation', got %v", metadata["category"])
	}
}

func TestQuestion_ToDocument_MinimalFields(t *testing.T) {
	q := Question{
		ID:         "q_minimal",
		Text:       "Simple question?",
		SourcePath: "test.md",
		SourceType: "mdx_component",
	}

	doc := q.ToDocument()

	// These should be present
	if doc["id"] != "q_minimal" {
		t.Errorf("Expected id 'q_minimal', got %v", doc["id"])
	}
	if doc["_type"] != "question" {
		t.Errorf("Expected _type 'question', got %v", doc["_type"])
	}

	// These should not be present when empty
	if _, exists := doc["source_url"]; exists {
		t.Errorf("source_url should not be present when empty")
	}
	if _, exists := doc["context"]; exists {
		t.Errorf("context should not be present when empty")
	}
	if _, exists := doc["metadata"]; exists {
		t.Errorf("metadata should not be present when empty")
	}
}

func TestQuestionID_Uniqueness(t *testing.T) {
	extractor := &QuestionsExtractor{}

	frontmatter := map[string]any{
		"questions": []any{
			"Question A",
			"Question B",
		},
	}

	questions := extractor.extractFromFrontmatter("test.md", "", frontmatter)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	// IDs should be different for different questions
	if questions[0].ID == questions[1].ID {
		t.Errorf("Question IDs should be unique, both have %q", questions[0].ID)
	}

	// Same question in same file should get same ID
	questions2 := extractor.extractFromFrontmatter("test.md", "", frontmatter)
	if questions[0].ID != questions2[0].ID {
		t.Errorf("Same question should get same ID: %q vs %q", questions[0].ID, questions2[0].ID)
	}
}

func TestQuestionsExtractor_WhitespaceHandling(t *testing.T) {
	extractor := &QuestionsExtractor{}

	content := []byte(`<Questions>
-   How do I install?
- What are the   requirements?
</Questions>`)

	questions := extractor.extractFromQuestionsComponents("test.mdx", "", content)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	// Whitespace should be trimmed
	if questions[0].Text != "How do I install?" {
		t.Errorf("Expected trimmed text 'How do I install?', got %q", questions[0].Text)
	}
	if !strings.Contains(questions[1].Text, "requirements?") {
		t.Errorf("Expected text containing 'requirements?', got %q", questions[1].Text)
	}
}
