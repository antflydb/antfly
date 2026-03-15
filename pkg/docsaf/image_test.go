package docsaf

import (
	"context"
	"errors"
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/ai"
	"github.com/antflydb/antfly/pkg/libaf/reading"
)

type mockReader struct {
	results []string
	err     error
}

func (m *mockReader) Read(_ context.Context, _ []ai.BinaryContent, _ *reading.ReadOptions) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.results, nil
}

func (m *mockReader) Close() error { return nil }

func TestImageProcessor_CanProcess(t *testing.T) {
	withReader := &ImageProcessor{Reader: &mockReader{}}
	noReader := &ImageProcessor{}

	tests := []struct {
		name        string
		proc        *ImageProcessor
		contentType string
		path        string
		want        bool
	}{
		{"PNG by MIME", withReader, "image/png", "photo.png", true},
		{"JPEG by MIME", withReader, "image/jpeg", "photo.jpg", true},
		{"TIFF by MIME", withReader, "image/tiff", "scan.tiff", true},
		{"WebP by MIME", withReader, "image/webp", "photo.webp", true},
		{"BMP by MIME", withReader, "image/bmp", "photo.bmp", true},
		{"GIF by MIME", withReader, "image/gif", "photo.gif", true},
		{"PNG by extension", withReader, "", "photo.png", true},
		{"JPG by extension", withReader, "", "photo.jpg", true},
		{"JPEG by extension", withReader, "", "photo.jpeg", true},
		{"TIF by extension", withReader, "", "scan.tif", true},
		{"Not an image", withReader, "text/plain", "file.txt", false},
		{"PDF not handled", withReader, "application/pdf", "doc.pdf", false},
		{"No reader - PNG", noReader, "image/png", "photo.png", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.proc.CanProcess(tt.contentType, tt.path)
			if got != tt.want {
				t.Errorf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestImageProcessor_Process(t *testing.T) {
	proc := &ImageProcessor{
		Reader: &mockReader{results: []string{"Extracted document text from scanned image"}},
	}

	sections, err := proc.Process("scans/page1.png", "https://example.com/page1.png", "https://example.com", []byte{0x89, 0x50})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(sections) != 1 {
		t.Fatalf("Process() got %d sections, want 1", len(sections))
	}

	s := sections[0]
	if s.Type != "image" {
		t.Errorf("Type = %q, want %q", s.Type, "image")
	}
	if s.Title != "page1" {
		t.Errorf("Title = %q, want %q", s.Title, "page1")
	}
	if s.Content != "Extracted document text from scanned image" {
		t.Errorf("Content = %q, want extracted text", s.Content)
	}
	if s.URL != "https://example.com/page1.png" {
		t.Errorf("URL = %q, want source URL", s.URL)
	}
	if s.Metadata["extraction_method"] != "ocr" {
		t.Errorf("extraction_method = %v, want %q", s.Metadata["extraction_method"], "ocr")
	}
	if s.Metadata["mime_type"] != "image/png" {
		t.Errorf("mime_type = %v, want %q", s.Metadata["mime_type"], "image/png")
	}
}

func TestImageProcessor_Process_EmptyResult(t *testing.T) {
	proc := &ImageProcessor{
		Reader: &mockReader{results: []string{""}},
	}

	sections, err := proc.Process("blank.png", "", "https://example.com", []byte{0x89})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if sections != nil {
		t.Errorf("Process() = %v, want nil for empty OCR result", sections)
	}
}

func TestImageProcessor_Process_ReaderError(t *testing.T) {
	proc := &ImageProcessor{
		Reader: &mockReader{err: errors.New("OCR model unavailable")},
	}

	_, err := proc.Process("scan.png", "", "https://example.com", []byte{0x89})
	if err == nil {
		t.Fatal("Process() expected error")
	}
}

func TestImageProcessor_Process_NoReader(t *testing.T) {
	proc := &ImageProcessor{}

	_, err := proc.Process("scan.png", "", "https://example.com", []byte{0x89})
	if err == nil {
		t.Fatal("Process() expected error with no Reader")
	}
}

func TestImageProcessor_Process_FallbackURL(t *testing.T) {
	proc := &ImageProcessor{
		Reader: &mockReader{results: []string{"some text"}},
	}

	sections, err := proc.Process("images/scan.jpg", "", "https://example.com", []byte{0xFF})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}
	if sections[0].URL != "https://example.com/images/scan.jpg" {
		t.Errorf("URL = %q, want constructed URL from baseURL + path", sections[0].URL)
	}
	if sections[0].Metadata["mime_type"] != "image/jpeg" {
		t.Errorf("mime_type = %v, want image/jpeg for .jpg", sections[0].Metadata["mime_type"])
	}
}
