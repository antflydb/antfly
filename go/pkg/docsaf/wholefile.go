// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docsaf

import (
	"path/filepath"
	"strings"
)

// WholeFileProcessor processes content by returning it as a single section
// without any chunking. This is useful when you want Antfly inference
// to handle document segmentation.
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
