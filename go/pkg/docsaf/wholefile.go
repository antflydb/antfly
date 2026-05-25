package docsaf

import (
	"path/filepath"
	"strings"
)

// WholeFileProcessor processes content by returning it as a single section
// without any chunking. This is useful when you want Antfly's internal
// chunking (e.g., Termite) to handle document segmentation.
type WholeFileProcessor struct{}

// CanProcess returns true for common text-based file types.
func (wfp *WholeFileProcessor) CanProcess(contentType, path string) bool {
	// Check common text MIME types
	if strings.HasPrefix(contentType, "text/") ||
		strings.Contains(contentType, "application/json") ||
		strings.Contains(contentType, "application/yaml") ||
		strings.Contains(contentType, "application/x-yaml") {
		return true
	}

	// Fall back to extension
	lower := strings.ToLower(path)
	supportedExtensions := []string{
		".md", ".mdx", ".txt", ".yaml", ".yml",
		".json", ".rst", ".adoc", ".html", ".htm",
	}
	for _, ext := range supportedExtensions {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}

// Process returns the entire content as a single DocumentSection.
func (wfp *WholeFileProcessor) Process(path, sourceURL, baseURL string, content []byte) ([]DocumentSection, error) {
	title := filepath.Base(path)

	url := ""
	if baseURL != "" {
		cleanPath := strings.TrimSuffix(path, filepath.Ext(path))
		url = baseURL + "/" + cleanPath
	}

	ext := strings.TrimPrefix(filepath.Ext(path), ".")
	metadata := map[string]any{
		"file_extension": ext,
		"whole_file":     true,
	}
	if sourceURL != "" {
		metadata["source_url"] = sourceURL
	}

	section := DocumentSection{
		ID:       generateID(path, "whole"),
		FilePath: path,
		Title:    title,
		Content:  string(content),
		Type:     "file",
		URL:      url,
		Metadata: metadata,
	}

	return []DocumentSection{section}, nil
}
