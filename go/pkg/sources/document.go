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

package sources

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

type Permissions struct {
	Public  bool     `json:"public"`
	Readers []string `json:"readers"`
}
type Document struct {
	Version     int            `json:"version"`
	URI         string         `json:"uri"`
	Path        string         `json:"path"`
	Content     string         `json:"content"`
	Revision    string         `json:"revision"`
	IndexedAt   int64          `json:"indexed_at"`
	Permissions Permissions    `json:"permissions"`
	Metadata    map[string]any `json:"metadata"`
}
type NormalizationError struct{ Code, Detail string }

func (e *NormalizationError) Error() string { return e.Detail }
func Normalize(sourceID string, item Item, data []byte, permissions Permissions, now time.Time) (Document, error) {
	fail := func(code, detail string) (Document, error) { return Document{}, &NormalizationError{code, detail} }
	if item.Revision == "" || len(item.Revision) > 4096 || !utf8.ValidString(item.Revision) || strings.ContainsRune(item.Revision, 0) {
		return fail("source_revision_missing", "Source revision is required.")
	}
	if len(data) > maxImportSourceObjectBytes || !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return fail("source_text_extraction_required", "Source must have bounded canonical UTF-8 text; binary documents require revision-bound text extraction before publication.")
	}
	uri := configString(item.Metadata, "canonical_url")
	// Pinned inventories may contain a URL emitted before S3 key escaping was
	// fixed. Repair only canonical metadata; retain the selected key/revision.
	if item.SourceKind == ProviderS3 && item.Path != "" {
		if bucket := configString(item.Metadata, "bucket"); bucket != "" {
			uri = canonicalS3URI(bucket, item.Path)
		}
	}
	if uri == "" {
		uri = item.URL
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme == "" || parsed.User != nil || len(uri) > 16384 || !utf8.ValidString(uri) || strings.EqualFold(parsed.Scheme, "data") || strings.ContainsAny(uri, "\x00\r\n") {
		return fail("source_uri_invalid", "Canonical source URI is missing or invalid.")
	}
	path := item.Path
	if path == "" {
		path = item.Filename
	}
	if path == "" || !utf8.ValidString(path) || len(path) > 16384 || strings.ContainsRune(path, 0) {
		return fail("source_path_invalid", "Source path is missing or invalid.")
	}
	if sourceID == "" || item.ID == "" {
		return Document{}, fmt.Errorf("source and item identities are required")
	}
	for _, reader := range permissions.Readers {
		if reader == "" || !utf8.ValidString(reader) || strings.ContainsAny(reader, "\x00\r\n") {
			return Document{}, fmt.Errorf("invalid reader identity")
		}
	}
	if permissions.Readers == nil {
		permissions.Readers = []string{}
	}
	metadata := cloneSourceMetadata(item.Metadata)
	digest := sha256.Sum256(data)
	metadata["sha256"] = hex.EncodeToString(digest[:])
	metadata["provider"] = item.SourceKind
	metadata["source_id"] = sourceID
	metadata["identity"] = item.ID
	return Document{Version: 1, URI: parsed.String(), Path: path, Content: string(data), Revision: item.Revision, IndexedAt: now.Unix(), Permissions: permissions, Metadata: metadata}, nil
}
