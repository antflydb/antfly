package docsaf

import (
	"testing"
)

func TestHTMLProcessor_CanProcess(t *testing.T) {
	hp := &HTMLProcessor{}

	tests := []struct {
		name        string
		contentType string
		path        string
		want        bool
	}{
		{"html file", "", "test.html", true},
		{"htm file", "", "test.htm", true},
		{"HTML uppercase", "", "test.HTML", true},
		{"html content type", "text/html", "test", true},
		{"html content type with charset", "text/html; charset=utf-8", "test", true},
		{"markdown file", "", "test.md", false},
		{"text file", "", "test.txt", false},
		{"no extension", "", "test", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hp.CanProcess(tt.contentType, tt.path); got != tt.want {
				t.Errorf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestHTMLProcessor_Process_WithHeadings(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head>
	<title>Test Document</title>
	<meta name="description" content="A test document">
	<meta property="og:title" content="OG Test">
</head>
<body>
	<h1>Introduction</h1>
	<p>This is the introduction section.</p>
	<p>It has multiple paragraphs.</p>

	<h2>Getting Started</h2>
	<p>Here's how to get started.</p>

	<h2>Advanced Topics</h2>
	<p>Advanced content here.</p>

	<h3>Subsection</h3>
	<p>Subsection content.</p>
</body>
</html>`)

	sections, err := hp.Process("test.html", "", "https://example.com", htmlContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Should have 4 sections (h1, h2, h2, h3)
	if len(sections) != 4 {
		t.Errorf("Expected 4 sections, got %d", len(sections))
	}

	// Check first section
	if sections[0].Title != "Test Document" {
		t.Errorf("First section title = %q, want %q", sections[0].Title, "Test Document")
	}
	if sections[0].Type != "html_section" {
		t.Errorf("First section type = %q, want %q", sections[0].Type, "html_section")
	}
	if !contains(sections[0].Content, "introduction section") {
		t.Errorf("First section content missing expected text")
	}
	if sections[0].URL != "https://example.com/test#introduction" {
		t.Errorf("First section URL = %q, want %q", sections[0].URL, "https://example.com/test#introduction")
	}

	// Check metadata
	if level, ok := sections[0].Metadata["heading_level"].(int); !ok || level != 1 {
		t.Errorf("First section heading_level = %v, want 1", sections[0].Metadata["heading_level"])
	}
	if title, ok := sections[0].Metadata["title"].(string); !ok || title != "Test Document" {
		t.Errorf("Metadata title = %q, want %q", title, "Test Document")
	}

	// Check second section
	if sections[1].Title != "Getting Started" {
		t.Errorf("Second section title = %q, want %q", sections[1].Title, "Getting Started")
	}
}

func TestHTMLProcessor_Process_NoHeadings(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head>
	<title>Simple Page</title>
</head>
<body>
	<p>This is a simple page without headings.</p>
	<p>Just some content.</p>
</body>
</html>`)

	sections, err := hp.Process("simple.html", "", "https://example.com", htmlContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 1 {
		t.Errorf("Expected 1 section, got %d", len(sections))
	}

	section := sections[0]
	if section.Title != "Simple Page" {
		t.Errorf("Section title = %q, want %q", section.Title, "Simple Page")
	}
	if section.Type != "html_document" {
		t.Errorf("Section type = %q, want %q", section.Type, "html_document")
	}
	if section.URL != "https://example.com/simple" {
		t.Errorf("Section URL = %q, want %q", section.URL, "https://example.com/simple")
	}
}

func TestHTMLProcessor_Process_DuplicateHeadings(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<body>
	<h1>Introduction</h1>
	<p>First intro.</p>

	<h2>Introduction</h2>
	<p>Second intro.</p>

	<h2>Introduction</h2>
	<p>Third intro.</p>
</body>
</html>`)

	sections, err := hp.Process("duplicate.html", "", "https://example.com", htmlContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) != 3 {
		t.Fatalf("Expected 3 sections, got %d", len(sections))
	}

	expectedURLs := []string{
		"https://example.com/duplicate#introduction",
		"https://example.com/duplicate#introduction-1",
		"https://example.com/duplicate#introduction-2",
	}

	for i, expected := range expectedURLs {
		if sections[i].URL != expected {
			t.Errorf("Section %d URL = %q, want %q", i, sections[i].URL, expected)
		}
	}
}

func TestHTMLProcessor_Process_WithExistingIDs(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<body>
	<h1 id="custom-intro">Introduction</h1>
	<p>Content here.</p>

	<h2 id="setup">Getting Started</h2>
	<p>Setup content.</p>
</body>
</html>`)

	sections, err := hp.Process("with-ids.html", "", "https://example.com", htmlContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if sections[0].URL != "https://example.com/with-ids#custom-intro" {
		t.Errorf("First section URL = %q, want %q", sections[0].URL, "https://example.com/with-ids#custom-intro")
	}
	if sections[1].URL != "https://example.com/with-ids#setup" {
		t.Errorf("Second section URL = %q, want %q", sections[1].URL, "https://example.com/with-ids#setup")
	}
}

func TestHTMLProcessor_Process_SkipsScriptAndStyle(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<body>
	<h1>Content</h1>
	<p>Visible text.</p>
	<script>console.log('this should not appear');</script>
	<style>body { color: red; }</style>
	<noscript>Please enable JavaScript.</noscript>
	<p>More visible text.</p>
</body>
</html>`)

	sections, err := hp.Process("with-script.html", "", "", htmlContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	content := sections[0].Content
	if contains(content, "console.log") {
		t.Errorf("Content should not contain script content")
	}
	if contains(content, "color: red") {
		t.Errorf("Content should not contain style content")
	}
	if !contains(content, "Visible text") {
		t.Errorf("Content should contain visible text")
	}
}

func TestHTMLProcessor_Process_EmptyBaseURL(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<h1>Test</h1><p>Content</p>`)

	sections, err := hp.Process("test.html", "", "", htmlContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if sections[0].URL != "" {
		t.Errorf("URL should be empty when baseURL not provided, got %q", sections[0].URL)
	}
}

func TestSlugCounter_Unique(t *testing.T) {
	sc := newSlugCounter()

	if got := sc.unique("test"); got != "test" {
		t.Errorf("unique('test') = %q, want 'test'", got)
	}

	if got := sc.unique("test"); got != "test-1" {
		t.Errorf("unique('test') = %q, want 'test-1'", got)
	}

	if got := sc.unique("test"); got != "test-2" {
		t.Errorf("unique('test') = %q, want 'test-2'", got)
	}

	if got := sc.unique("other"); got != "other" {
		t.Errorf("unique('other') = %q, want 'other'", got)
	}
}

func TestTransformHTMLPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"test.html", "test"},
		{"test.htm", "test"},
		{"path/to/file.html", "path/to/file"},
		{"noextension", "noextension"},
		{"file.md", "file.md"},
	}

	for _, tt := range tests {
		if got := transformHTMLPath(tt.input); got != tt.want {
			t.Errorf("transformHTMLPath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestHTMLProcessor_ExtractQuestions_DataAttribute(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head>
	<title>FAQ Page</title>
</head>
<body>
	<div data-docsaf-questions='["How do I sign up?", "What payment methods are accepted?"]'>
		<h1>Getting Started</h1>
		<p>Content here.</p>
	</div>
</body>
</html>`)

	questions := hp.ExtractQuestions("faq.html", "https://example.com/faq", htmlContent)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	if questions[0].Text != "How do I sign up?" {
		t.Errorf("Expected 'How do I sign up?', got %q", questions[0].Text)
	}
	if questions[0].SourceType != "html_data_attribute" {
		t.Errorf("Expected source type 'html_data_attribute', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "FAQ Page" {
		t.Errorf("Expected context 'FAQ Page', got %q", questions[0].Context)
	}
	if questions[0].SourcePath != "faq.html" {
		t.Errorf("Expected source path 'faq.html', got %q", questions[0].SourcePath)
	}

	if questions[1].Text != "What payment methods are accepted?" {
		t.Errorf("Expected 'What payment methods are accepted?', got %q", questions[1].Text)
	}
}

func TestHTMLProcessor_ExtractQuestions_DataAttributeWithObjects(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Test</title></head>
<body>
	<div data-docsaf-questions='[{"text": "How do I authenticate?", "category": "auth"}, {"text": "What are rate limits?"}]'>
		<h1>API Docs</h1>
	</div>
</body>
</html>`)

	questions := hp.ExtractQuestions("api.html", "", htmlContent)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	if questions[0].Text != "How do I authenticate?" {
		t.Errorf("Expected 'How do I authenticate?', got %q", questions[0].Text)
	}
	if questions[0].Metadata["category"] != "auth" {
		t.Errorf("Expected metadata category 'auth', got %v", questions[0].Metadata["category"])
	}

	if questions[1].Text != "What are rate limits?" {
		t.Errorf("Expected 'What are rate limits?', got %q", questions[1].Text)
	}
}

func TestHTMLProcessor_ExtractQuestions_ClassContainer(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Help Center</title></head>
<body>
	<h1>Frequently Asked Questions</h1>
	<ul class="docsaf-questions">
		<li>How do I reset my password?</li>
		<li>Where can I find my API key?</li>
		<li>How do I contact support?</li>
	</ul>
</body>
</html>`)

	questions := hp.ExtractQuestions("help.html", "https://help.example.com/faq", htmlContent)

	if len(questions) != 3 {
		t.Fatalf("Expected 3 questions, got %d", len(questions))
	}

	if questions[0].Text != "How do I reset my password?" {
		t.Errorf("Expected 'How do I reset my password?', got %q", questions[0].Text)
	}
	if questions[0].SourceType != "html_class" {
		t.Errorf("Expected source type 'html_class', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "Help Center" {
		t.Errorf("Expected context 'Help Center', got %q", questions[0].Context)
	}

	if questions[1].Text != "Where can I find my API key?" {
		t.Errorf("Expected 'Where can I find my API key?', got %q", questions[1].Text)
	}
	if questions[2].Text != "How do I contact support?" {
		t.Errorf("Expected 'How do I contact support?', got %q", questions[2].Text)
	}
}

func TestHTMLProcessor_ExtractQuestions_MultipleSources(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Docs</title></head>
<body>
	<div data-docsaf-questions='["Question from data attribute"]'>
		<h1>Section 1</h1>
	</div>
	<ul class="docsaf-questions">
		<li>Question from class</li>
	</ul>
</body>
</html>`)

	questions := hp.ExtractQuestions("docs.html", "", htmlContent)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions (1 data + 1 class), got %d", len(questions))
	}

	// Check that both source types are present
	sourceTypes := make(map[string]int)
	for _, q := range questions {
		sourceTypes[q.SourceType]++
	}

	if sourceTypes["html_data_attribute"] != 1 {
		t.Errorf("Expected 1 question from data attribute, got %d", sourceTypes["html_data_attribute"])
	}
	if sourceTypes["html_class"] != 1 {
		t.Errorf("Expected 1 question from class, got %d", sourceTypes["html_class"])
	}
}

func TestHTMLProcessor_ExtractQuestions_NoQuestions(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Regular Page</title></head>
<body>
	<h1>No Questions Here</h1>
	<p>Just regular content.</p>
</body>
</html>`)

	questions := hp.ExtractQuestions("regular.html", "", htmlContent)

	if len(questions) != 0 {
		t.Errorf("Expected 0 questions, got %d", len(questions))
	}
}

func TestHTMLProcessor_ExtractQuestions_InvalidJSON(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<body>
	<div data-docsaf-questions='not valid json'>
		<h1>Test</h1>
	</div>
</body>
</html>`)

	questions := hp.ExtractQuestions("invalid.html", "", htmlContent)

	// Should return empty slice for invalid JSON, not error
	if len(questions) != 0 {
		t.Errorf("Expected 0 questions for invalid JSON, got %d", len(questions))
	}
}

func TestHTMLProcessor_ExtractQuestions_EmptyListItems(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<body>
	<ul class="docsaf-questions">
		<li>Valid question</li>
		<li>   </li>
		<li></li>
		<li>Another valid question</li>
	</ul>
</body>
</html>`)

	questions := hp.ExtractQuestions("test.html", "", htmlContent)

	// Empty/whitespace-only items should be skipped
	if len(questions) != 2 {
		t.Errorf("Expected 2 questions (skipping empty items), got %d", len(questions))
	}
}

func TestHTMLProcessor_ExtractQuestions_NoTitle(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<body>
	<div data-docsaf-questions='["Question here"]'>
		<p>Content</p>
	</div>
</body>
</html>`)

	questions := hp.ExtractQuestions("notitle.html", "", htmlContent)

	if len(questions) != 1 {
		t.Fatalf("Expected 1 question, got %d", len(questions))
	}

	// Context should be empty when no title
	if questions[0].Context != "" {
		t.Errorf("Expected empty context when no title, got %q", questions[0].Context)
	}
}

func TestHTMLProcessor_ExtractQuestions_SectionPath(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Docs</title></head>
<body>
	<h1>Getting Started</h1>
	<p>Intro content</p>

	<h2>Installation</h2>
	<ul class="docsaf-questions">
		<li>How do I install?</li>
		<li>What are the requirements?</li>
	</ul>

	<h2>Configuration</h2>
	<h3>Database Setup</h3>
	<div data-docsaf-questions='["How do I configure the database?"]'>
		<p>Database content</p>
	</div>
</body>
</html>`)

	questions := hp.ExtractQuestions("docs.html", "", htmlContent)

	if len(questions) != 3 {
		t.Fatalf("Expected 3 questions, got %d", len(questions))
	}

	// First two questions should be under "Getting Started > Installation"
	expectedPath1 := []string{"Getting Started", "Installation"}
	if !slicesEqual(questions[0].SectionPath, expectedPath1) {
		t.Errorf("Expected section path %v, got %v", expectedPath1, questions[0].SectionPath)
	}
	if !slicesEqual(questions[1].SectionPath, expectedPath1) {
		t.Errorf("Expected section path %v, got %v", expectedPath1, questions[1].SectionPath)
	}

	// Third question should be under "Getting Started > Configuration > Database Setup"
	expectedPath2 := []string{"Getting Started", "Configuration", "Database Setup"}
	if !slicesEqual(questions[2].SectionPath, expectedPath2) {
		t.Errorf("Expected section path %v, got %v", expectedPath2, questions[2].SectionPath)
	}
}

func TestHTMLProcessor_ExtractQuestions_NoHeadings(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Simple Page</title></head>
<body>
	<div data-docsaf-questions='["Question without section"]'>
		<p>Content</p>
	</div>
</body>
</html>`)

	questions := hp.ExtractQuestions("simple.html", "", htmlContent)

	if len(questions) != 1 {
		t.Fatalf("Expected 1 question, got %d", len(questions))
	}

	// Section path should be empty when there are no preceding headings
	if len(questions[0].SectionPath) != 0 {
		t.Errorf("Expected empty section path, got %v", questions[0].SectionPath)
	}
}

func TestHTMLProcessor_ExtractQuestions_SectionPathReset(t *testing.T) {
	hp := &HTMLProcessor{}

	// Test that section path resets when a higher-level heading appears
	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Docs</title></head>
<body>
	<h1>Chapter 1</h1>
	<h2>Section 1.1</h2>
	<h3>Subsection 1.1.1</h3>
	<ul class="docsaf-questions">
		<li>Question in 1.1.1</li>
	</ul>

	<h1>Chapter 2</h1>
	<ul class="docsaf-questions">
		<li>Question in Chapter 2</li>
	</ul>
</body>
</html>`)

	questions := hp.ExtractQuestions("docs.html", "", htmlContent)

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions, got %d", len(questions))
	}

	// First question should have full path
	expectedPath1 := []string{"Chapter 1", "Section 1.1", "Subsection 1.1.1"}
	if !slicesEqual(questions[0].SectionPath, expectedPath1) {
		t.Errorf("Expected section path %v, got %v", expectedPath1, questions[0].SectionPath)
	}

	// Second question should have reset path (only h1)
	expectedPath2 := []string{"Chapter 2"}
	if !slicesEqual(questions[1].SectionPath, expectedPath2) {
		t.Errorf("Expected section path %v, got %v", expectedPath2, questions[1].SectionPath)
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHTMLProcessor_Process_QuestionsInSections(t *testing.T) {
	hp := &HTMLProcessor{}

	htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>API Documentation</title></head>
<body>
	<h1>Getting Started</h1>
	<p>Welcome to the API.</p>

	<h2>Authentication</h2>
	<ul class="docsaf-questions">
		<li>How do I authenticate?</li>
		<li>What auth methods are supported?</li>
	</ul>

	<h2>Endpoints</h2>
	<h3>User API</h3>
	<div data-docsaf-questions='["How do I create a user?"]'>
		<p>User endpoint documentation.</p>
	</div>
</body>
</html>`)

	sections, err := hp.Process("api.html", "", "https://example.com", htmlContent)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Should have 4 sections: Getting Started, Authentication, Endpoints, User API
	if len(sections) != 4 {
		t.Fatalf("Expected 4 sections, got %d", len(sections))
	}

	// Check that Authentication section has 2 questions
	authSection := sections[1] // Authentication is the second section
	if authSection.Title != "Authentication" {
		t.Errorf("Expected 'Authentication', got %q", authSection.Title)
	}
	if len(authSection.Questions) != 2 {
		t.Errorf("Expected 2 questions in Authentication section, got %d", len(authSection.Questions))
	}
	if authSection.Questions[0] != "How do I authenticate?" {
		t.Errorf("Expected first question 'How do I authenticate?', got %q", authSection.Questions[0])
	}

	// Check that User API section has 1 question
	userSection := sections[3] // User API is the fourth section
	if userSection.Title != "User API" {
		t.Errorf("Expected 'User API', got %q", userSection.Title)
	}
	if len(userSection.Questions) != 1 {
		t.Errorf("Expected 1 question in User API section, got %d", len(userSection.Questions))
	}
	if userSection.Questions[0] != "How do I create a user?" {
		t.Errorf("Expected question 'How do I create a user?', got %q", userSection.Questions[0])
	}

	// Check that Getting Started section has no questions
	if len(sections[0].Questions) != 0 {
		t.Errorf("Expected 0 questions in Getting Started section, got %d", len(sections[0].Questions))
	}
}
