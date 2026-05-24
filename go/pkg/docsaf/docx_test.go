package docsaf

import (
	"archive/zip"
	"bytes"
	"testing"
)

func buildTestDocx(t *testing.T, documentXML string, coreXML string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(documentXML)); err != nil {
		t.Fatal(err)
	}

	if coreXML != "" {
		w, err = zw.Create("docProps/core.xml")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(coreXML)); err != nil {
			t.Fatal(err)
		}
	}

	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

const testDocxWithHeadings = `<?xml version="1.0" encoding="UTF-8"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p>
      <w:pPr><w:pStyle w:val="Heading1"/></w:pPr>
      <w:r><w:t>Introduction</w:t></w:r>
    </w:p>
    <w:p>
      <w:r><w:t>This is the introduction paragraph.</w:t></w:r>
    </w:p>
    <w:p>
      <w:pPr><w:pStyle w:val="Heading2"/></w:pPr>
      <w:r><w:t>Background</w:t></w:r>
    </w:p>
    <w:p>
      <w:r><w:t>Some background information.</w:t></w:r>
    </w:p>
    <w:p>
      <w:pPr><w:pStyle w:val="Heading1"/></w:pPr>
      <w:r><w:t>Methods</w:t></w:r>
    </w:p>
    <w:p>
      <w:r><w:t>Description of methods used.</w:t></w:r>
    </w:p>
  </w:body>
</w:document>`

const testDocxNoHeadings = `<?xml version="1.0" encoding="UTF-8"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p>
      <w:r><w:t>First paragraph of the document.</w:t></w:r>
    </w:p>
    <w:p>
      <w:r><w:t>Second paragraph with more content.</w:t></w:r>
    </w:p>
  </w:body>
</w:document>`

const testCoreXML = `<?xml version="1.0" encoding="UTF-8"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
                   xmlns:dc="http://purl.org/dc/elements/1.1/"
                   xmlns:dcterms="http://purl.org/dc/terms/">
  <dc:title>Test Document</dc:title>
  <dc:creator>Test Author</dc:creator>
  <dc:subject>Testing</dc:subject>
  <cp:keywords>test, docx</cp:keywords>
  <dcterms:created>2025-01-15T10:30:00Z</dcterms:created>
  <dcterms:modified>2025-02-28T14:00:00Z</dcterms:modified>
</cp:coreProperties>`

func TestDocxProcessor_CanProcess(t *testing.T) {
	dp := &DocxProcessor{}

	tests := []struct {
		name        string
		contentType string
		path        string
		want        bool
	}{
		{"DOCX by MIME", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "doc.docx", true},
		{"DOCX by extension", "", "report.docx", true},
		{"DOCX uppercase extension", "", "report.DOCX", true},
		{"DOC old format", "", "report.doc", false},
		{"PDF", "application/pdf", "doc.pdf", false},
		{"Markdown", "text/markdown", "doc.md", false},
		{"Empty", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dp.CanProcess(tt.contentType, tt.path)
			if got != tt.want {
				t.Errorf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestDocxProcessor_Process_WithHeadings(t *testing.T) {
	dp := &DocxProcessor{}
	content := buildTestDocx(t, testDocxWithHeadings, "")

	sections, err := dp.Process("docs/report.docx", "", "https://example.com", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 3 {
		t.Fatalf("got %d sections, want 3", len(sections))
	}

	// Section 1: Introduction
	if sections[0].Title != "Introduction" {
		t.Errorf("sections[0].Title = %q, want %q", sections[0].Title, "Introduction")
	}
	if sections[0].Content != "This is the introduction paragraph." {
		t.Errorf("sections[0].Content = %q", sections[0].Content)
	}
	if sections[0].Type != "docx_section" {
		t.Errorf("sections[0].Type = %q, want %q", sections[0].Type, "docx_section")
	}
	if sections[0].Metadata["heading_level"] != 1 {
		t.Errorf("sections[0] heading_level = %v, want 1", sections[0].Metadata["heading_level"])
	}

	// Section 2: Background (nested under Introduction)
	if sections[1].Title != "Background" {
		t.Errorf("sections[1].Title = %q, want %q", sections[1].Title, "Background")
	}
	if len(sections[1].SectionPath) != 2 {
		t.Errorf("sections[1].SectionPath len = %d, want 2", len(sections[1].SectionPath))
	} else {
		if sections[1].SectionPath[0] != "Introduction" || sections[1].SectionPath[1] != "Background" {
			t.Errorf("sections[1].SectionPath = %v", sections[1].SectionPath)
		}
	}

	// Section 3: Methods (resets hierarchy)
	if sections[2].Title != "Methods" {
		t.Errorf("sections[2].Title = %q, want %q", sections[2].Title, "Methods")
	}
	if len(sections[2].SectionPath) != 1 {
		t.Errorf("sections[2].SectionPath len = %d, want 1", len(sections[2].SectionPath))
	}
}

func TestDocxProcessor_Process_NoHeadings(t *testing.T) {
	dp := &DocxProcessor{}
	content := buildTestDocx(t, testDocxNoHeadings, "")

	sections, err := dp.Process("notes.docx", "", "https://example.com", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}

	if sections[0].Title != "notes.docx" {
		t.Errorf("Title = %q, want filename", sections[0].Title)
	}
	if sections[0].Metadata["no_headings"] != true {
		t.Errorf("no_headings = %v, want true", sections[0].Metadata["no_headings"])
	}
	if !bytes.Contains([]byte(sections[0].Content), []byte("First paragraph")) {
		t.Errorf("Content missing first paragraph: %q", sections[0].Content)
	}
}

func TestDocxProcessor_Process_Metadata(t *testing.T) {
	dp := &DocxProcessor{}
	content := buildTestDocx(t, testDocxNoHeadings, testCoreXML)

	sections, err := dp.Process("report.docx", "https://example.com/report.docx", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}

	s := sections[0]
	if s.Title != "Test Document" {
		t.Errorf("Title = %q, want %q", s.Title, "Test Document")
	}
	if s.Metadata["author"] != "Test Author" {
		t.Errorf("author = %v, want %q", s.Metadata["author"], "Test Author")
	}
	if s.Metadata["subject"] != "Testing" {
		t.Errorf("subject = %v, want %q", s.Metadata["subject"], "Testing")
	}
	if s.Metadata["source_url"] != "https://example.com/report.docx" {
		t.Errorf("source_url = %v", s.Metadata["source_url"])
	}
}

func TestDocxProcessor_Process_URLGeneration(t *testing.T) {
	dp := &DocxProcessor{}
	content := buildTestDocx(t, testDocxWithHeadings, "")

	sections, err := dp.Process("docs/report.docx", "", "https://example.com", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) < 1 {
		t.Fatal("expected at least 1 section")
	}

	// URL should strip .docx extension and include slug anchor
	if !bytes.Contains([]byte(sections[0].URL), []byte("docs/report#")) {
		t.Errorf("URL = %q, want path without .docx extension", sections[0].URL)
	}
}

func TestDocxProcessor_Process_EmptyBaseURL(t *testing.T) {
	dp := &DocxProcessor{}
	content := buildTestDocx(t, testDocxWithHeadings, "")

	sections, err := dp.Process("report.docx", "", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	for i, s := range sections {
		if s.URL != "" {
			t.Errorf("sections[%d].URL = %q, want empty", i, s.URL)
		}
	}
}

func TestDocxProcessor_Process_InvalidContent(t *testing.T) {
	dp := &DocxProcessor{}
	_, err := dp.Process("bad.docx", "", "", []byte("not a zip file"))
	if err == nil {
		t.Error("Process() expected error for invalid content")
	}
}

func TestDocxProcessor_Process_MultipleRuns(t *testing.T) {
	dp := &DocxProcessor{}

	// Test that multiple <w:r> runs in a paragraph are concatenated
	docXML := `<?xml version="1.0" encoding="UTF-8"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p>
      <w:r><w:t>Hello </w:t></w:r>
      <w:r><w:t>world</w:t></w:r>
    </w:p>
  </w:body>
</w:document>`

	content := buildTestDocx(t, docXML, "")
	sections, err := dp.Process("test.docx", "", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}
	if sections[0].Content != "Hello world" {
		t.Errorf("Content = %q, want %q", sections[0].Content, "Hello world")
	}
}

func TestParseHeadingLevel(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"Heading1", 1},
		{"Heading2", 2},
		{"heading1", 1},
		{"heading 3", 3},
		{"Heading9", 9},
		{"Normal", 0},
		{"Title", 0},
		{"heading", 0},
		{"Heading10", 0},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseHeadingLevel(tt.input)
			if got != tt.want {
				t.Errorf("parseHeadingLevel(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestTransformDocxPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"report.docx", "report"},
		{"docs/file.docx", "docs/file"},
		{"noext", "noext"},
	}

	for _, tt := range tests {
		got := transformDocxPath(tt.input)
		if got != tt.want {
			t.Errorf("transformDocxPath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
