package docsaf

import (
	"os"
	"testing"
)

func TestPDFProcessor_CanProcess(t *testing.T) {
	pp := &PDFProcessor{}

	tests := []struct {
		name        string
		contentType string
		path        string
		want        bool
	}{
		{"pdf file", "", "test.pdf", true},
		{"PDF uppercase", "", "test.PDF", true},
		{"pdf content type", "application/pdf", "test", true},
		{"markdown file", "", "test.md", false},
		{"html file", "", "test.html", false},
		{"text file", "", "test.txt", false},
		{"no extension", "", "test", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pp.CanProcess(tt.contentType, tt.path); got != tt.want {
				t.Errorf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestPDFProcessor_Process_Basic(t *testing.T) {
	testPDF := "testdata/pdf/sample.pdf"
	content, err := os.ReadFile(testPDF)
	if err != nil {
		t.Skip("Test PDF not found, skipping test")
	}

	pp := &PDFProcessor{}

	sections, err := pp.Process("sample.pdf", "", "https://example.com", content)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) == 0 {
		t.Fatal("Expected at least one section")
	}

	section := sections[0]
	if section.Type != "pdf_page" {
		t.Errorf("Section type = %q, want %q", section.Type, "pdf_page")
	}
	if section.FilePath != "sample.pdf" {
		t.Errorf("Section FilePath = %q, want %q", section.FilePath, "sample.pdf")
	}
	if section.Content == "" {
		t.Error("Section content should not be empty")
	}
	if section.URL == "" {
		t.Error("Section URL should not be empty when baseURL provided")
	}

	if _, ok := section.Metadata["page_number"]; !ok {
		t.Error("Section should have page_number metadata")
	}
	if _, ok := section.Metadata["total_pages"]; !ok {
		t.Error("Section should have total_pages metadata")
	}
}

func TestPDFProcessor_Process_EmptyBaseURL(t *testing.T) {
	testPDF := "testdata/pdf/sample.pdf"
	content, err := os.ReadFile(testPDF)
	if err != nil {
		t.Skip("Test PDF not found, skipping test")
	}

	pp := &PDFProcessor{}
	sections, err := pp.Process("sample.pdf", "", "", content)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) == 0 {
		t.Fatal("Expected at least one section")
	}

	if sections[0].URL != "" {
		t.Errorf("URL should be empty when baseURL not provided, got %q", sections[0].URL)
	}
}

func TestPDFProcessor_Process_TitleFormat(t *testing.T) {
	testPDF := "testdata/pdf/sample.pdf"
	content, err := os.ReadFile(testPDF)
	if err != nil {
		t.Skip("Test PDF not found, skipping test")
	}

	pp := &PDFProcessor{}
	sections, err := pp.Process("sample.pdf", "", "https://example.com", content)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) == 0 {
		t.Fatal("Expected at least one section")
	}

	for i, section := range sections {
		if !containsPageNumber(section.Title) {
			t.Errorf("Section %d title %q should contain 'Page N' format", i, section.Title)
		}
	}
}

func TestPDFProcessor_URLGeneration(t *testing.T) {
	testPDF := "testdata/pdf/sample.pdf"
	content, err := os.ReadFile(testPDF)
	if err != nil {
		t.Skip("Test PDF not found, skipping test")
	}

	pp := &PDFProcessor{}
	sections, err := pp.Process("sample.pdf", "", "https://docs.example.com", content)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) == 0 {
		t.Fatal("Expected at least one section")
	}

	expectedPrefix := "https://docs.example.com/sample#page-"
	if !hasPrefix(sections[0].URL, expectedPrefix) {
		t.Errorf("URL = %q, want prefix %q", sections[0].URL, expectedPrefix)
	}

	seenURLs := make(map[string]bool)
	for _, section := range sections {
		if seenURLs[section.URL] {
			t.Errorf("Duplicate URL found: %q", section.URL)
		}
		seenURLs[section.URL] = true
	}
}

func TestPDFProcessor_PageNumbers(t *testing.T) {
	testPDF := "testdata/pdf/sample.pdf"
	content, err := os.ReadFile(testPDF)
	if err != nil {
		t.Skip("Test PDF not found, skipping test")
	}

	pp := &PDFProcessor{}
	sections, err := pp.Process("sample.pdf", "", "", content)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(sections) == 0 {
		t.Fatal("Expected at least one section")
	}

	for i, section := range sections {
		pageNum, ok := section.Metadata["page_number"].(int)
		if !ok {
			t.Fatalf("Section %d: page_number not an int", i)
		}

		if pageNum < 1 {
			t.Errorf("Section %d: page_number = %d, want >= 1", i, pageNum)
		}

		totalPages, ok := section.Metadata["total_pages"].(int)
		if !ok {
			t.Fatalf("Section %d: total_pages not an int", i)
		}
		if totalPages < 1 {
			t.Errorf("Section %d: total_pages = %d, want >= 1", i, totalPages)
		}

		if pageNum > totalPages {
			t.Errorf("Section %d: page_number %d > total_pages %d", i, pageNum, totalPages)
		}
	}
}

func TestPDFProcessor_ErrHandling(t *testing.T) {
	pp := &PDFProcessor{}

	// Test with invalid PDF content
	_, err := pp.Process("test.pdf", "", "", []byte("not a pdf"))
	if err == nil {
		t.Error("Expected error for invalid PDF content")
	}
}

func TestTransformPDFPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"test.pdf", "test"},
		{"path/to/file.pdf", "path/to/file"},
		{"document.PDF", "document.PDF"},
		{"noextension", "noextension"},
		{"file.txt", "file.txt"},
	}

	for _, tt := range tests {
		if got := transformPDFPath(tt.input); got != tt.want {
			t.Errorf("transformPDFPath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func containsPageNumber(s string) bool {
	return len(s) > 5 && (hasSubstring(s, "Page 1") || hasSubstring(s, "Page 2") ||
		hasSubstring(s, "Page 3") || hasSubstring(s, "Page 4") ||
		hasSubstring(s, "Page 5") || hasSubstring(s, "Page 6") ||
		hasSubstring(s, "Page 7") || hasSubstring(s, "Page 8") ||
		hasSubstring(s, "Page 9") || hasSubstring(s, "Page 0"))
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func hasSubstring(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
