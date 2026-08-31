package docsaf

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"mime"
	"path/filepath"
	"strings"
	"time"
)

const (
	// DefaultDocumentUnitsArtifact is the canonical document-unit artifact name
	// used by Antfly's derived document hierarchy.
	DefaultDocumentUnitsArtifact = "document_units_v1"

	// DefaultDocumentChunksArtifact is the canonical unit-derived chunk artifact
	// name used by docsaf-created table configurations.
	DefaultDocumentChunksArtifact = "document_chunks_v1"
)

// SourceDocument is the row docsaf writes to Antfly. The source row owns the
// original file identity and stable metadata; Antfly derives units, chunks,
// embeddings, full-text entries, and graph artifacts from this row.
type SourceDocument struct {
	ID         string
	URL        string
	Filename   string
	MIMEType   string
	ETag       string
	SHA256     string
	Version    string
	SourcePath string
	SourceKind string
	SizeBytes  int64
	ModifiedAt time.Time
	Metadata   map[string]any
}

// ToDocument converts a source document to the storage shape expected by
// Antfly's document_extraction artifact producer.
func (d SourceDocument) ToDocument() map[string]any {
	doc := map[string]any{
		"id":          d.ID,
		"url":         d.URL,
		"filename":    d.Filename,
		"mime_type":   d.MIMEType,
		"sha256":      d.SHA256,
		"source_path": d.SourcePath,
		"source_kind": d.SourceKind,
		"doc_type":    "source_document",
	}
	if d.ETag != "" {
		doc["etag"] = d.ETag
	}
	if d.Version != "" {
		doc["version"] = d.Version
	}
	if d.SizeBytes > 0 {
		doc["size_bytes"] = d.SizeBytes
	}
	if !d.ModifiedAt.IsZero() {
		doc["modified_at"] = d.ModifiedAt.UTC().Format(time.RFC3339Nano)
	}
	if len(d.Metadata) > 0 {
		doc["metadata"] = maps.Clone(d.Metadata)
	}
	return doc
}

// SourceDocumentOptions configures source-row projection from ContentItems.
type SourceDocumentOptions struct {
	// InlineContent emits data: URLs that contain the source bytes. This is
	// useful for local examples and tests; production sync should prefer
	// externally reachable URLs such as HTTPS or S3.
	InlineContent bool

	// BaseURL is used to construct source URLs for items that do not already
	// carry SourceURL. FilesystemSource populates only Path, so callers can point
	// this at an object store, static file server, or local Antfly-accessible URL
	// prefix.
	BaseURL string

	// MaxInlineBytes rejects InlineContent rows larger than this many source
	// bytes. Non-positive values disable the guard. Production sync should prefer
	// BaseURL over InlineContent for large files.
	MaxInlineBytes int64

	// IDPrefix optionally namespaces source document IDs.
	IDPrefix string
}

// BuildSourceDocuments traverses a ContentSource and returns source rows without
// doing local section extraction. Extraction belongs to Antfly's derived artifact
// producer so manifests, generations, stale-child cleanup, chunking, and indexing
// stay parent-owned and convergent.
func BuildSourceDocuments(ctx context.Context, source ContentSource, opts SourceDocumentOptions) ([]SourceDocument, error) {
	items, errs := source.Traverse(ctx)
	docs := make([]SourceDocument, 0)

	for item := range items {
		doc, err := SourceDocumentFromContentItem(item, opts)
		if err != nil {
			return docs, err
		}
		docs = append(docs, doc)
	}

	for err := range errs {
		if err != nil {
			return docs, err
		}
	}
	return docs, nil
}

// SourceDocumentFromContentItem projects one traversed content item into a
// source row for document_extraction.
func SourceDocumentFromContentItem(item ContentItem, opts SourceDocumentOptions) (SourceDocument, error) {
	sourcePath := filepath.ToSlash(item.Path)
	id := sourcePath
	if opts.IDPrefix != "" {
		id = strings.TrimSuffix(opts.IDPrefix, "/") + "/" + strings.TrimPrefix(id, "/")
	}

	sum := sha256.Sum256(item.Content)
	sha := hex.EncodeToString(sum[:])
	contentType := item.ContentType
	if contentType == "" {
		contentType = DetectContentType(item.Path, item.Content)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	sourceURL := item.SourceURL
	if sourceURL == "" && opts.BaseURL != "" {
		sourceURL = strings.TrimRight(opts.BaseURL, "/") + "/" + strings.TrimLeft(sourcePath, "/")
	}
	if opts.InlineContent {
		if opts.MaxInlineBytes > 0 && int64(len(item.Content)) > opts.MaxInlineBytes {
			return SourceDocument{}, fmt.Errorf("source document %q is %d bytes, exceeding inline content limit %d bytes; use BaseURL for large files", item.Path, len(item.Content), opts.MaxInlineBytes)
		}
		sourceURL = "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(item.Content)
	}
	if sourceURL == "" {
		return SourceDocument{}, fmt.Errorf("source document %q has no source URL; set BaseURL or InlineContent", item.Path)
	}

	doc := SourceDocument{
		ID:         id,
		URL:        sourceURL,
		Filename:   filepath.Base(item.Path),
		MIMEType:   contentType,
		SHA256:     sha,
		SourcePath: sourcePath,
		SourceKind: metadataString(item.Metadata, "source_type"),
		Metadata:   maps.Clone(item.Metadata),
	}
	if doc.SourceKind == "" {
		doc.SourceKind = "content_source"
	}
	doc.SizeBytes = metadataInt64(item.Metadata, "file_size")
	doc.ETag = metadataString(item.Metadata, "etag")
	doc.Version = metadataString(item.Metadata, "version")
	doc.ModifiedAt = metadataTime(item.Metadata, "mod_time")

	if doc.Filename == "." || doc.Filename == "/" || doc.Filename == "" {
		doc.Filename = filenameFromURL(sourceURL)
	}
	if doc.MIMEType == "application/octet-stream" {
		if extType := mime.TypeByExtension(strings.ToLower(filepath.Ext(doc.Filename))); extType != "" {
			doc.MIMEType = extType
		}
	}
	return doc, nil
}

// SourceDocumentRecords converts source documents to an object-only
// linear-merge record map.
func SourceDocumentRecords(docs []SourceDocument) map[string]map[string]any {
	records := make(map[string]map[string]any, len(docs))
	for _, doc := range docs {
		records[doc.ID] = doc.ToDocument()
	}
	return records
}

func metadataString(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	switch v := metadata[key].(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return ""
	}
}

func metadataInt64(metadata map[string]any, key string) int64 {
	if metadata == nil {
		return 0
	}
	switch v := metadata[key].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case uint64:
		if v <= uint64(^uint64(0)>>1) {
			return int64(v)
		}
	case float64:
		if v >= 0 {
			return int64(v)
		}
	}
	return 0
}

func metadataTime(metadata map[string]any, key string) time.Time {
	if metadata == nil {
		return time.Time{}
	}
	switch v := metadata[key].(type) {
	case time.Time:
		return v
	case string:
		t, _ := time.Parse(time.RFC3339Nano, v)
		return t
	default:
		return time.Time{}
	}
}

func filenameFromURL(raw string) string {
	withoutQuery := strings.Split(raw, "?")[0]
	withoutFragment := strings.Split(withoutQuery, "#")[0]
	name := filepath.Base(withoutFragment)
	if name == "." || name == "/" {
		return ""
	}
	return name
}
