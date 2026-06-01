package docsaf

import (
	"archive/zip"
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func buildTestPptx(t *testing.T, slides map[int]string, notes map[int]string, coreXML string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	for num, xml := range slides {
		w, err := zw.Create(fmt.Sprintf("ppt/slides/slide%d.xml", num))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(xml)); err != nil {
			t.Fatal(err)
		}
	}

	for num, xml := range notes {
		w, err := zw.Create(fmt.Sprintf("ppt/notesSlides/notesSlide%d.xml", num))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(xml)); err != nil {
			t.Fatal(err)
		}
	}

	if coreXML != "" {
		w, err := zw.Create("docProps/core.xml")
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

func makeSlideXML(texts ...string) string {
	var shapes strings.Builder
	for _, text := range texts {
		fmt.Fprintf(&shapes, `
      <p:sp>
        <p:txBody>
          <a:p><a:r><a:t>%s</a:t></a:r></a:p>
        </p:txBody>
      </p:sp>`, text)
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"
       xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">
  <p:cSld>
    <p:spTree>%s
    </p:spTree>
  </p:cSld>
</p:sld>`, shapes.String())
}

func makeNotesXML(text string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<p:notes xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"
         xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">
  <p:cSld>
    <p:spTree>
      <p:sp>
        <p:txBody>
          <a:p><a:r><a:t>%s</a:t></a:r></a:p>
        </p:txBody>
      </p:sp>
    </p:spTree>
  </p:cSld>
</p:notes>`, text)
}

func TestPptxProcessor_CanProcess(t *testing.T) {
	pp := &PptxProcessor{}

	tests := []struct {
		name        string
		contentType string
		path        string
		want        bool
	}{
		{"PPTX by MIME", "application/vnd.openxmlformats-officedocument.presentationml.presentation", "deck.pptx", true},
		{"PPTX by extension", "", "slides.pptx", true},
		{"PPTX uppercase", "", "slides.PPTX", true},
		{"PPT old format", "", "slides.ppt", false},
		{"PDF", "application/pdf", "doc.pdf", false},
		{"DOCX", "", "doc.docx", false},
		{"Empty", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pp.CanProcess(tt.contentType, tt.path)
			if got != tt.want {
				t.Errorf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestPptxProcessor_Process_BasicSlides(t *testing.T) {
	pp := &PptxProcessor{}

	slides := map[int]string{
		1: makeSlideXML("Welcome", "Introduction to the topic"),
		2: makeSlideXML("Key Findings"),
		3: makeSlideXML("Conclusion", "Thank you"),
	}

	content := buildTestPptx(t, slides, nil, "")

	sections, err := pp.Process("deck.pptx", "", "https://example.com", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 3 {
		t.Fatalf("got %d sections, want 3", len(sections))
	}

	// Check types
	for i, s := range sections {
		if s.Type != "pptx_slide" {
			t.Errorf("sections[%d].Type = %q, want %q", i, s.Type, "pptx_slide")
		}
	}

	// Check slide 1
	if sections[0].Metadata["slide_number"] != 1 {
		t.Errorf("slide_number = %v, want 1", sections[0].Metadata["slide_number"])
	}
	if sections[0].Metadata["total_slides"] != 3 {
		t.Errorf("total_slides = %v, want 3", sections[0].Metadata["total_slides"])
	}

	// Check content contains text from shapes
	if !bytes.Contains([]byte(sections[0].Content), []byte("Welcome")) {
		t.Errorf("slide 1 missing 'Welcome': %q", sections[0].Content)
	}
	if !bytes.Contains([]byte(sections[0].Content), []byte("Introduction to the topic")) {
		t.Errorf("slide 1 missing 'Introduction to the topic': %q", sections[0].Content)
	}
}

func TestPptxProcessor_Process_SlideOrder(t *testing.T) {
	pp := &PptxProcessor{}

	// Add slides in reverse order in the map
	slides := map[int]string{
		3: makeSlideXML("Third"),
		1: makeSlideXML("First"),
		2: makeSlideXML("Second"),
	}

	content := buildTestPptx(t, slides, nil, "")
	sections, err := pp.Process("deck.pptx", "", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 3 {
		t.Fatalf("got %d sections, want 3", len(sections))
	}

	// Verify order
	expected := []string{"First", "Second", "Third"}
	for i, want := range expected {
		if !bytes.Contains([]byte(sections[i].Content), []byte(want)) {
			t.Errorf("sections[%d] should contain %q, got %q", i, want, sections[i].Content)
		}
	}
}

func TestPptxProcessor_Process_WithNotes(t *testing.T) {
	pp := &PptxProcessor{}

	slides := map[int]string{
		1: makeSlideXML("Slide Content"),
	}
	notes := map[int]string{
		1: makeNotesXML("Remember to mention the key point"),
	}

	content := buildTestPptx(t, slides, notes, "")
	sections, err := pp.Process("deck.pptx", "", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}

	if !bytes.Contains([]byte(sections[0].Content), []byte("Speaker Notes:")) {
		t.Errorf("Content missing notes separator: %q", sections[0].Content)
	}
	if !bytes.Contains([]byte(sections[0].Content), []byte("Remember to mention the key point")) {
		t.Errorf("Content missing notes text: %q", sections[0].Content)
	}
}

func TestPptxProcessor_Process_NotesDisabled(t *testing.T) {
	includeNotes := false
	pp := &PptxProcessor{IncludeNotes: &includeNotes}

	slides := map[int]string{
		1: makeSlideXML("Slide Content"),
	}
	notes := map[int]string{
		1: makeNotesXML("These notes should not appear"),
	}

	content := buildTestPptx(t, slides, notes, "")
	sections, err := pp.Process("deck.pptx", "", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}

	if bytes.Contains([]byte(sections[0].Content), []byte("Speaker Notes:")) {
		t.Errorf("Content should not contain notes when disabled: %q", sections[0].Content)
	}
}

func TestPptxProcessor_Process_Metadata(t *testing.T) {
	pp := &PptxProcessor{}

	slides := map[int]string{
		1: makeSlideXML("Content"),
	}

	content := buildTestPptx(t, slides, nil, testCoreXML)
	sections, err := pp.Process("deck.pptx", "https://example.com/deck.pptx", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}

	s := sections[0]
	if s.Title != "Test Document - Slide 1" {
		t.Errorf("Title = %q, want %q", s.Title, "Test Document - Slide 1")
	}
	if s.Metadata["author"] != "Test Author" {
		t.Errorf("author = %v, want %q", s.Metadata["author"], "Test Author")
	}
	if s.Metadata["source_url"] != "https://example.com/deck.pptx" {
		t.Errorf("source_url = %v", s.Metadata["source_url"])
	}
}

func TestPptxProcessor_Process_URLGeneration(t *testing.T) {
	pp := &PptxProcessor{}

	slides := map[int]string{
		1: makeSlideXML("Slide 1"),
		2: makeSlideXML("Slide 2"),
	}

	content := buildTestPptx(t, slides, nil, "")
	sections, err := pp.Process("decks/talk.pptx", "", "https://example.com", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if len(sections) != 2 {
		t.Fatalf("got %d sections, want 2", len(sections))
	}

	if sections[0].URL != "https://example.com/decks/talk#slide-1" {
		t.Errorf("sections[0].URL = %q", sections[0].URL)
	}
	if sections[1].URL != "https://example.com/decks/talk#slide-2" {
		t.Errorf("sections[1].URL = %q", sections[1].URL)
	}
}

func TestPptxProcessor_Process_EmptyBaseURL(t *testing.T) {
	pp := &PptxProcessor{}

	slides := map[int]string{
		1: makeSlideXML("Content"),
	}

	content := buildTestPptx(t, slides, nil, "")
	sections, err := pp.Process("deck.pptx", "", "", content)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	for i, s := range sections {
		if s.URL != "" {
			t.Errorf("sections[%d].URL = %q, want empty", i, s.URL)
		}
	}
}

func TestPptxProcessor_Process_InvalidContent(t *testing.T) {
	pp := &PptxProcessor{}
	_, err := pp.Process("bad.pptx", "", "", []byte("not a zip file"))
	if err == nil {
		t.Error("Process() expected error for invalid content")
	}
}

func TestPptxProcessor_Process_NoSlides(t *testing.T) {
	pp := &PptxProcessor{}

	// Valid ZIP but no slide files
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("ppt/presentation.xml")
	w.Write([]byte(`<?xml version="1.0"?><Presentation/>`))
	zw.Close()

	sections, err := pp.Process("empty.pptx", "", "", buf.Bytes())
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if sections != nil {
		t.Errorf("got %d sections, want nil", len(sections))
	}
}

func TestTransformPptxPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"talk.pptx", "talk"},
		{"decks/presentation.pptx", "decks/presentation"},
		{"noext", "noext"},
	}

	for _, tt := range tests {
		got := transformPptxPath(tt.input)
		if got != tt.want {
			t.Errorf("transformPptxPath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
