package docsaf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestSourceDocumentFromContentItemInline(t *testing.T) {
	item := ContentItem{
		Path:        "guide/intro.md",
		Content:     []byte("# Intro\nhello"),
		ContentType: "text/markdown",
		Metadata: map[string]any{
			"source_type": "filesystem",
			"file_size":   int64(13),
			"mod_time":    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
	}

	doc, err := SourceDocumentFromContentItem(item, SourceDocumentOptions{
		InlineContent: true,
		IDPrefix:      "docs",
	})
	if err != nil {
		t.Fatalf("SourceDocumentFromContentItem: %v", err)
	}

	sum := sha256.Sum256(item.Content)
	if doc.ID != "docs/guide/intro.md" {
		t.Fatalf("ID = %q", doc.ID)
	}
	if doc.URL == "" || doc.URL[:len("data:text/markdown;base64,")] != "data:text/markdown;base64," {
		t.Fatalf("unexpected URL %q", doc.URL)
	}
	if doc.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 = %q", doc.SHA256)
	}
	if doc.SourceKind != "filesystem" {
		t.Fatalf("SourceKind = %q", doc.SourceKind)
	}
	if doc.SizeBytes != 13 {
		t.Fatalf("SizeBytes = %d", doc.SizeBytes)
	}

	record := doc.ToDocument()
	if record["content"] != nil {
		t.Fatalf("source rows must not contain extracted content: %#v", record)
	}
	if record["doc_type"] != "source_document" {
		t.Fatalf("doc_type = %#v", record["doc_type"])
	}
	if _, exists := record["_type"]; exists {
		t.Fatalf("source rows must not use reserved _type: %#v", record)
	}
}

func TestSourceDocumentFromContentItemCanonicalMetadata(t *testing.T) {
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	item := ContentItem{
		Path:        "drive/report.pdf",
		SourceURL:   "https://drive.google.com/file/d/file-id/view",
		Content:     []byte("%PDF"),
		ContentType: "application/pdf",
		Metadata: map[string]any{
			"source_type": "google_drive",
			"file_size":   int64(42),
			"mod_time":    modTime,
			"etag":        "checksum",
			"version":     "revision",
		},
	}

	doc, err := SourceDocumentFromContentItem(item, SourceDocumentOptions{})
	if err != nil {
		t.Fatalf("SourceDocumentFromContentItem: %v", err)
	}
	if doc.SizeBytes != 42 {
		t.Fatalf("SizeBytes = %d, want 42", doc.SizeBytes)
	}
	if !doc.ModifiedAt.Equal(modTime) {
		t.Fatalf("ModifiedAt = %v, want %v", doc.ModifiedAt, modTime)
	}
	if doc.ETag != "checksum" {
		t.Fatalf("ETag = %q, want checksum", doc.ETag)
	}
	if doc.Version != "revision" {
		t.Fatalf("Version = %q, want revision", doc.Version)
	}
}

func TestBuildSourceDocumentsRequiresURL(t *testing.T) {
	source := singleItemSource{item: ContentItem{
		Path:    "a.txt",
		Content: []byte("alpha"),
	}}

	_, err := BuildSourceDocuments(context.Background(), source, SourceDocumentOptions{})
	if err == nil {
		t.Fatal("expected missing source URL error")
	}
}

func TestSourceDocumentInlineContentLimit(t *testing.T) {
	_, err := SourceDocumentFromContentItem(ContentItem{
		Path:    "large.pdf",
		Content: []byte(strings.Repeat("x", 8)),
	}, SourceDocumentOptions{
		InlineContent:  true,
		MaxInlineBytes: 4,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeding inline content limit 4 bytes") {
		t.Fatalf("SourceDocumentFromContentItem error = %v, want inline limit error", err)
	}
}

type singleItemSource struct {
	item ContentItem
}

func (s singleItemSource) Type() string    { return "test" }
func (s singleItemSource) BaseURL() string { return "" }
func (s singleItemSource) Traverse(context.Context) (<-chan ContentItem, <-chan error) {
	items := make(chan ContentItem, 1)
	errs := make(chan error, 1)
	items <- s.item
	close(items)
	close(errs)
	return items, errs
}
