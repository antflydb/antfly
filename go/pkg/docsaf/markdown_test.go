package docsaf

import (
	"strings"
	"testing"
)

func TestMarkdownProcessor_CanProcess(t *testing.T) {
	mp := &MarkdownProcessor{}

	tests := []struct {
		name        string
		contentType string
		path        string
		want        bool
	}{
		{"md file", "", "test.md", true},
		{"mdx file", "", "test.mdx", true},
		{"MD uppercase", "", "test.MD", true},
		{"MDX uppercase", "", "test.MDX", true},
		{"markdown content type", "text/markdown", "test", true},
		{"x-markdown content type", "text/x-markdown", "test", true},
		{"html file", "", "test.html", false},
		{"text file", "", "test.txt", false},
		{"no extension", "", "test", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mp.CanProcess(tt.contentType, tt.path); got != tt.want {
				t.Errorf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestMarkdownProcessor_Process_WithHeadings(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1} // Split on every heading

	mdContent := []byte(`# Introduction

This is the introduction section.

## Getting Started

Here's how to get started.

## Advanced Topics

Advanced content here.

### Subsection

Subsection content.
`)

	sections, err := mp.Process("test.md", "", "https://example.com", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 4 {
		t.Errorf("Expected 4 sections, got %d", len(sections))
	}

	// Check first section
	if sections[0].Title != "Introduction" {
		t.Errorf("First section title = %q, want %q", sections[0].Title, "Introduction")
	}
	if sections[0].Type != "markdown_section" {
		t.Errorf("First section type = %q, want %q", sections[0].Type, "markdown_section")
	}
	if sections[0].URL != "https://example.com/test#introduction" {
		t.Errorf("First section URL = %q, want %q", sections[0].URL, "https://example.com/test#introduction")
	}

	// Check metadata
	if level, ok := sections[0].Metadata["heading_level"].(int); !ok || level != 1 {
		t.Errorf("First section heading_level = %v, want 1", sections[0].Metadata["heading_level"])
	}

	// Check second section
	if sections[1].Title != "Getting Started" {
		t.Errorf("Second section title = %q, want %q", sections[1].Title, "Getting Started")
	}
}

func TestMarkdownProcessor_Process_WithFrontmatter(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	mdContent := []byte(`---
title: My Document Title
author: Test Author
---

# Introduction

This is the introduction.
`)

	sections, err := mp.Process("test.md", "", "https://example.com", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("Expected 1 section, got %d", len(sections))
	}

	// First section should use frontmatter title
	if sections[0].Title != "My Document Title" {
		t.Errorf("Section title = %q, want %q", sections[0].Title, "My Document Title")
	}

	// Check frontmatter in metadata
	fm, ok := sections[0].Metadata["frontmatter"].(map[string]any)
	if !ok {
		t.Fatalf("Expected frontmatter in metadata")
	}
	if fm["author"] != "Test Author" {
		t.Errorf("Frontmatter author = %v, want %q", fm["author"], "Test Author")
	}
}

func TestMarkdownProcessor_Process_NoHeadings(t *testing.T) {
	mp := &MarkdownProcessor{}

	mdContent := []byte(`This is a simple document without headings.

Just some content.
`)

	sections, err := mp.Process("simple.md", "", "https://example.com", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 1 {
		t.Errorf("Expected 1 section, got %d", len(sections))
	}

	section := sections[0]
	if section.Title != "simple.md" {
		t.Errorf("Section title = %q, want %q", section.Title, "simple.md")
	}
	if section.URL != "https://example.com/simple" {
		t.Errorf("Section URL = %q, want %q", section.URL, "https://example.com/simple")
	}
	if _, ok := section.Metadata["no_headings"].(bool); !ok {
		t.Errorf("Expected no_headings metadata to be set")
	}
}

func TestMarkdownProcessor_Process_MDX(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	mdxContent := []byte(`# Component

<CustomComponent prop="value" />

Some content with JSX.
`)

	sections, err := mp.Process("test.mdx", "", "", mdxContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("Expected 1 section, got %d", len(sections))
	}

	if sections[0].Type != "mdx_section" {
		t.Errorf("Section type = %q, want %q", sections[0].Type, "mdx_section")
	}
	if isMDX, ok := sections[0].Metadata["is_mdx"].(bool); !ok || !isMDX {
		t.Errorf("Expected is_mdx metadata to be true")
	}
}

func TestMarkdownProcessor_Process_EmptyBaseURL(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	mdContent := []byte(`# Test

Content
`)

	sections, err := mp.Process("test.md", "", "", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if sections[0].URL != "" {
		t.Errorf("URL should be empty when baseURL not provided, got %q", sections[0].URL)
	}
}

func TestMarkdownProcessor_Process_SourceURL(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	mdContent := []byte(`# Test

Content
`)

	sections, err := mp.Process("test.md", "https://github.com/org/repo/blob/main/test.md", "", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	sourceURL, ok := sections[0].Metadata["source_url"].(string)
	if !ok || sourceURL != "https://github.com/org/repo/blob/main/test.md" {
		t.Errorf("source_url = %v, want %q", sourceURL, "https://github.com/org/repo/blob/main/test.md")
	}
}

func TestMarkdownProcessor_MergeSmallSections(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 500} // Default threshold

	// Create markdown with many small sections that should be merged
	mdContent := []byte(`# Introduction

Short intro.

## Part 1

Brief part 1.

## Part 2

Brief part 2.

## Part 3

Brief part 3.
`)

	sections, err := mp.Process("test.md", "", "", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// With 500 token minimum, these small sections should be merged
	if len(sections) != 1 {
		t.Errorf("Expected 1 merged section, got %d", len(sections))
	}

	// Content should contain all parts
	content := sections[0].Content
	if !strings.Contains(content, "Short intro") {
		t.Errorf("Merged content missing 'Short intro'")
	}
	if !strings.Contains(content, "Brief part 1") {
		t.Errorf("Merged content missing 'Brief part 1'")
	}
	if !strings.Contains(content, "Brief part 3") {
		t.Errorf("Merged content missing 'Brief part 3'")
	}
}

func TestMarkdownProcessor_NoMergeWhenLargeEnough(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1} // Split on every heading

	mdContent := []byte(`# Section 1

Content for section 1.

## Section 2

Content for section 2.
`)

	sections, err := mp.Process("test.md", "", "", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// With MinTokensPerSection=1, should not merge
	if len(sections) != 2 {
		t.Errorf("Expected 2 sections (no merging with threshold 1), got %d", len(sections))
	}
}

func TestMarkdownProcessor_MergePreservesFirstTitle(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 500}

	mdContent := []byte(`# Main Title

Introduction.

## Subsection

More content.
`)

	sections, err := mp.Process("test.md", "", "", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Merged section should preserve the first (main) title
	if sections[0].Title != "Main Title" {
		t.Errorf("Merged section title = %q, want %q", sections[0].Title, "Main Title")
	}
}

func TestEstimateTokens(t *testing.T) {
	tests := []struct {
		text      string
		minTokens int
		maxTokens int
	}{
		{"", 0, 0},
		{"hello", 1, 2},
		{"hello world", 2, 4},
		{"This is a longer sentence with more words.", 10, 15},
	}

	for _, tt := range tests {
		tokens := estimateTokens(tt.text)
		if tokens < tt.minTokens || tokens > tt.maxTokens {
			t.Errorf("estimateTokens(%q) = %d, expected between %d and %d",
				tt.text, tokens, tt.minTokens, tt.maxTokens)
		}
	}
}

func TestMergeSmallerSections(t *testing.T) {
	// Test with explicit section merging
	sections := []DocumentSection{
		{ID: "1", Title: "A", Content: "Short content A"},
		{ID: "2", Title: "B", Content: "Short content B"},
		{ID: "3", Title: "C", Content: "Short content C"},
	}

	// With high threshold, all should merge into one
	merged := mergeSmallerSections(sections, 1000)
	if len(merged) != 1 {
		t.Errorf("Expected 1 merged section, got %d", len(merged))
	}

	// First section's metadata should be preserved
	if merged[0].Title != "A" {
		t.Errorf("Merged section title = %q, want %q", merged[0].Title, "A")
	}

	// Content should contain all sections
	if !strings.Contains(merged[0].Content, "Short content A") ||
		!strings.Contains(merged[0].Content, "Short content B") ||
		!strings.Contains(merged[0].Content, "Short content C") {
		t.Errorf("Merged content missing expected text: %q", merged[0].Content)
	}
}

func TestMergeSmallerSections_SingleSection(t *testing.T) {
	sections := []DocumentSection{
		{ID: "1", Title: "Only", Content: "Only content"},
	}

	merged := mergeSmallerSections(sections, 1000)
	if len(merged) != 1 {
		t.Errorf("Expected 1 section, got %d", len(merged))
	}
	if merged[0].Title != "Only" {
		t.Errorf("Section title = %q, want %q", merged[0].Title, "Only")
	}
}

func TestMergeSmallerSections_EmptySlice(t *testing.T) {
	sections := []DocumentSection{}
	merged := mergeSmallerSections(sections, 1000)
	if len(merged) != 0 {
		t.Errorf("Expected 0 sections, got %d", len(merged))
	}
}

func TestCalculateCodeRatio(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantMin float64
		wantMax float64
	}{
		{
			name:    "no code blocks",
			content: "This is just plain text without any code.",
			wantMin: 0.0,
			wantMax: 0.01,
		},
		{
			name:    "pure code block",
			content: "```python\nprint('hello')\n```",
			wantMin: 0.99,
			wantMax: 1.0,
		},
		{
			name:    "mixed content",
			content: "Some explanation here.\n\n```go\nfunc main() {}\n```\n\nMore text after.",
			wantMin: 0.3,
			wantMax: 0.6,
		},
		{
			name:    "empty content",
			content: "",
			wantMin: 0.0,
			wantMax: 0.01,
		},
		{
			name:    "multiple code blocks",
			content: "Text\n```a\ncode1\n```\nText\n```b\ncode2\n```",
			wantMin: 0.4,
			wantMax: 0.75,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ratio := calculateCodeRatio(tt.content)
			if ratio < tt.wantMin || ratio > tt.wantMax {
				t.Errorf("calculateCodeRatio() = %v, want between %v and %v", ratio, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestMergeSmallerSections_PureCodeMergesWithPrevious(t *testing.T) {
	// Create sections where the second section is pure code
	sections := []DocumentSection{
		{
			ID:       "1",
			Title:    "Explanation",
			Content:  "This explains how to use the function. It provides context and details about the API.",
			Metadata: map[string]any{"heading_level": 2},
		},
		{
			ID:       "2",
			Title:    "Example",
			Content:  "```python\ndef hello():\n    print('Hello, World!')\n\nhello()\n```",
			Metadata: map[string]any{"heading_level": 3},
		},
		{
			ID:       "3",
			Title:    "Next Topic",
			Content:  "This is another topic with enough content to stand alone as its own section with prose.",
			Metadata: map[string]any{"heading_level": 2},
		},
	}

	// With threshold of 1 (split on every heading), the pure-code section should still merge
	merged := mergeSmallerSections(sections, 1)

	// The pure-code section should have been merged with the previous one
	if len(merged) != 2 {
		t.Errorf("Expected 2 merged sections (code merged with explanation), got %d", len(merged))
		for i, s := range merged {
			t.Logf("Section %d: %q (code_ratio: %v)", i, s.Title, s.Metadata["code_ratio"])
		}
		return
	}

	// First merged section should contain both explanation and code
	if !strings.Contains(merged[0].Content, "explains how to use") {
		t.Errorf("First section should contain explanation text")
	}
	if !strings.Contains(merged[0].Content, "```python") {
		t.Errorf("First section should contain the code block")
	}

	// Should have has_code_blocks metadata
	if merged[0].Metadata["has_code_blocks"] != true {
		t.Errorf("First section should have has_code_blocks=true metadata")
	}

	// Second section should be the next topic
	if merged[1].Title != "Next Topic" {
		t.Errorf("Second section title = %q, want %q", merged[1].Title, "Next Topic")
	}
}

func TestMergeSmallerSections_FirstSectionPureCode(t *testing.T) {
	// Edge case: first section is pure code (no previous to merge with)
	sections := []DocumentSection{
		{
			ID:       "1",
			Title:    "Quick Start",
			Content:  "```bash\nnpm install mypackage\n```",
			Metadata: map[string]any{"heading_level": 2},
		},
		{
			ID:       "2",
			Title:    "Details",
			Content:  "After installation, you can use the package. Here are more details about configuration.",
			Metadata: map[string]any{"heading_level": 2},
		},
	}

	merged := mergeSmallerSections(sections, 1)

	// Since first section is pure code with no previous, it should merge forward
	// into the next section's accumulation
	if len(merged) != 1 {
		t.Errorf("Expected 1 merged section, got %d", len(merged))
	}

	if !strings.Contains(merged[0].Content, "npm install") {
		t.Errorf("Merged section should contain the code")
	}
	if !strings.Contains(merged[0].Content, "After installation") {
		t.Errorf("Merged section should contain the prose")
	}
}

func TestMarkdownProcessor_CodeBlockExtraction(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	mdContent := []byte("# Example\n\nHere's some code:\n\n```python\nprint('hello')\n```\n\nAnd more text.")

	sections, err := mp.Process("test.md", "", "https://example.com", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("Expected 1 section, got %d", len(sections))
	}

	// Should contain the code block with language tag preserved
	if !strings.Contains(sections[0].Content, "```python") {
		t.Errorf("Section should contain code block with language tag, got: %q", sections[0].Content)
	}
	if !strings.Contains(sections[0].Content, "print('hello')") {
		t.Errorf("Section should contain code content")
	}
}

func TestMarkdownProcessor_PureCodeSectionMergedWithProse(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	// Document with explanation followed by pure-code section
	mdContent := []byte(`# Getting Started

This guide explains how to set up the project. You'll need to follow these steps carefully.

## Installation

` + "```bash\npip install mypackage\nmypackage init\nmypackage run\n```" + `

## Configuration

After installation, configure the settings in your config file.
`)

	sections, err := mp.Process("test.md", "", "https://example.com", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// The pure-code "Installation" section should merge with "Getting Started"
	// So we should have 2 sections: (Getting Started + Installation) and Configuration
	if len(sections) != 2 {
		t.Errorf("Expected 2 sections (code merged with intro), got %d", len(sections))
		for i, s := range sections {
			ratio := calculateCodeRatio(s.Content)
			t.Logf("Section %d: %q (code_ratio: %.2f, len: %d)", i, s.Title, ratio, len(s.Content))
		}
	}

	// First section should have the code block merged in
	foundCodeInFirst := strings.Contains(sections[0].Content, "pip install")
	if !foundCodeInFirst && len(sections) >= 1 {
		t.Logf("First section content: %s", sections[0].Content)
	}
}

func TestGenerateSlug(t *testing.T) {
	tests := []struct {
		heading string
		want    string
	}{
		{"Simple", "simple"},
		{"Two Words", "two-words"},
		{"Multiple   Spaces", "multiple--spaces"},
		{"Special!@#Characters", "specialcharacters"},
		{"Numbers123", "numbers123"},
		{"already-dashed", "already-dashed"},
		{"Under_scored", "under-scored"},
		{"", ""},
	}

	for _, tt := range tests {
		if got := generateSlug(tt.heading); got != tt.want {
			t.Errorf("generateSlug(%q) = %q, want %q", tt.heading, got, tt.want)
		}
	}
}

func TestTransformURLPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"test.md", "test"},
		{"test.mdx", "test"},
		{"path/to/file.md", "path/to/file"},
		{"noextension", "noextension"},
		{"file.html", "file.html"},
	}

	for _, tt := range tests {
		if got := transformURLPath(tt.input); got != tt.want {
			t.Errorf("transformURLPath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMarkdownProcessor_HeadingNotDuplicatedInContent(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	mdContent := []byte(`# Overview

This is the overview content.

## Getting Started

Getting started content here.
`)

	sections, err := mp.Process("test.md", "", "", mdContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Verify headings are not duplicated in content
	for _, section := range sections {
		// The title should NOT appear at the start of content (no "OverviewOverview" bug)
		if strings.HasPrefix(section.Content, section.Title) {
			t.Errorf("Section %q has title duplicated at start of content: %q",
				section.Title, section.Content[:min(50, len(section.Content))])
		}
		// Content should not contain the raw heading markdown
		if strings.Contains(section.Content, "# "+section.Title) {
			t.Errorf("Section %q content contains heading markdown: %q",
				section.Title, section.Content)
		}
	}
}

func TestExtractFrontmatter(t *testing.T) {
	tests := []struct {
		name            string
		content         string
		wantFrontmatter bool
		wantTitle       string
	}{
		{
			name: "with frontmatter",
			content: `---
title: Test Title
---

Content here`,
			wantFrontmatter: true,
			wantTitle:       "Test Title",
		},
		{
			name:            "without frontmatter",
			content:         "Just content without frontmatter",
			wantFrontmatter: false,
		},
		{
			name:            "incomplete frontmatter",
			content:         "---\ntitle: Incomplete",
			wantFrontmatter: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm, _ := extractFrontmatter([]byte(tt.content))
			if tt.wantFrontmatter {
				if fm == nil {
					t.Errorf("Expected frontmatter, got nil")
				} else if fm["title"] != tt.wantTitle {
					t.Errorf("Frontmatter title = %v, want %q", fm["title"], tt.wantTitle)
				}
			} else {
				if fm != nil {
					t.Errorf("Expected no frontmatter, got %v", fm)
				}
			}
		})
	}
}

func TestMarkdownProcessor_Process_MDXComponentContent(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	mdxContent := []byte(`# Getting Started

<Questions>
- How do I install Antfly?
- Where can I download the CLI?
</Questions>

Some content here.
`)

	sections, err := mp.Process("test.mdx", "", "", mdxContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("Expected 1 section, got %d", len(sections))
	}

	// Verify MDX component content is included
	if !strings.Contains(sections[0].Content, "How do I install Antfly?") {
		t.Errorf("Expected content to include MDX component text, got: %q", sections[0].Content)
	}
	if !strings.Contains(sections[0].Content, "Where can I download the CLI?") {
		t.Errorf("Expected content to include MDX component text, got: %q", sections[0].Content)
	}
	// Also verify regular content is still included
	if !strings.Contains(sections[0].Content, "Some content here") {
		t.Errorf("Expected content to include regular text, got: %q", sections[0].Content)
	}
}

func TestMarkdownProcessor_Process_MDXComponentBeforeHeading(t *testing.T) {
	mp := &MarkdownProcessor{MinTokensPerSection: 1}

	// MDX content with Questions component BEFORE the first heading (after frontmatter)
	mdxContent := []byte(`---
title: My Guide
---

<Questions>
- What is Antfly?
- How do I get started?
</Questions>

Some intro text here.

# Getting Started

This is the getting started section.
`)

	sections, err := mp.Process("test.mdx", "", "https://example.com", mdxContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Preamble is now merged into the first section
	if len(sections) != 1 {
		t.Fatalf("Expected 1 section (preamble merged into first heading), got %d", len(sections))
	}

	// First section should have preamble content merged with heading content
	section := sections[0]

	// Title should be from frontmatter
	if section.Title != "My Guide" {
		t.Errorf("Section title = %q, want %q", section.Title, "My Guide")
	}

	// Should contain preamble content
	if !strings.Contains(section.Content, "What is Antfly?") {
		t.Errorf("Section should contain preamble MDX component text, got: %q", section.Content)
	}
	if !strings.Contains(section.Content, "How do I get started?") {
		t.Errorf("Section should contain preamble MDX component text, got: %q", section.Content)
	}
	if !strings.Contains(section.Content, "Some intro text here") {
		t.Errorf("Section should contain preamble intro text, got: %q", section.Content)
	}

	// Should also contain heading section content
	if !strings.Contains(section.Content, "getting started section") {
		t.Errorf("Section should contain heading content, got: %q", section.Content)
	}

	// Check has_preamble metadata
	if section.Metadata["has_preamble"] != true {
		t.Errorf("Section metadata should have has_preamble=true")
	}

	// URL should have the heading anchor (from the first heading)
	if section.URL != "https://example.com/test#getting-started" {
		t.Errorf("Section URL = %q, want %q", section.URL, "https://example.com/test#getting-started")
	}

	// Section path should be from the heading
	if len(section.SectionPath) != 1 || section.SectionPath[0] != "Getting Started" {
		t.Errorf("SectionPath = %v, want [Getting Started]", section.SectionPath)
	}
}
