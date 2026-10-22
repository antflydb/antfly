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
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

// SourceAccess is normalized access metadata, supplied by a trusted connector.
// It is not a replacement for the data plane's authenticated row-filter policy.
type SourceAccess struct {
	Public  bool     `json:"public"`
	Readers []string `json:"readers"`
}
type EvidenceAsset struct {
	Field      string `json:"field"`
	MIMEType   string `json:"mime_type"`
	SHA256     string `json:"sha256"`
	Recipe     string `json:"recipe"`
	ByteLength int64  `json:"byte_length"`
}
type EvidenceLocation struct {
	Page  uint32 `json:"page,omitempty"`
	Slide uint32 `json:"slide,omitempty"`
}
type EvidenceUnit struct {
	ID       string           `json:"id"`
	Kind     string           `json:"kind"`
	Location EvidenceLocation `json:"location"`
	Text     string           `json:"text,omitempty"`
	Label    string           `json:"label,omitempty"`
	Asset    EvidenceAsset    `json:"asset"`
}
type SourceEvidenceOptions struct {
	// Field defaults to source; grep table configuration selects this logical field.
	Field     string
	SourceURI string
	Content   string
	IndexedAt time.Time
	Access    *SourceAccess
	Units     []EvidenceUnit
}

// ToEvidenceRecord augments the existing parent-owned source row with version 2
// search evidence. Access metadata must be explicit: successful provider consent
// does not imply public access. Callers publish units and payloads
// atomically with the source revision. Bytes live in explicit same-parent fields;
// this helper neither downloads media nor renders documents.
func (d SourceDocument) ToEvidenceRecord(opts SourceEvidenceOptions) (map[string]any, error) {
	if opts.Access == nil {
		return nil, fmt.Errorf("source access policy required")
	}
	if opts.IndexedAt.IsZero() {
		return nil, fmt.Errorf("index timestamp required")
	}
	uri := opts.SourceURI
	if uri == "" {
		uri = d.SourceURI
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Scheme == "" || parsed.User != nil || strings.EqualFold(parsed.Scheme, "data") || len(uri) > 16384 || !utf8.ValidString(uri) || strings.ContainsRune(uri, 0) {
		return nil, fmt.Errorf("canonical source URI required")
	}
	if d.ID == "" || d.SourcePath == "" || !utf8.ValidString(d.SourcePath) || strings.ContainsRune(d.SourcePath, 0) || len(d.SourcePath) > 16384 || !lowerDigest(d.SHA256) {
		return nil, fmt.Errorf("valid source identity and SHA-256 required")
	}
	if !utf8.ValidString(opts.Content) {
		return nil, fmt.Errorf("source content must be UTF-8")
	}
	field := opts.Field
	if field == "" {
		field = "source"
	}
	if !evidenceField(field) || strings.Contains(field, ".") {
		return nil, fmt.Errorf("source contract field must be one ordinary field")
	}
	if len(opts.Units) > 4096 {
		return nil, fmt.Errorf("too many evidence units")
	}
	ids := make(map[string]struct{}, len(opts.Units))
	for _, unit := range opts.Units {
		if err := unit.validate(); err != nil {
			return nil, err
		}
		if _, ok := ids[unit.ID]; ok {
			return nil, fmt.Errorf("duplicate evidence unit %q", unit.ID)
		}
		ids[unit.ID] = struct{}{}
	}
	access := *opts.Access
	access.Readers = append([]string{}, opts.Access.Readers...)
	for _, reader := range access.Readers {
		if reader == "" || !utf8.ValidString(reader) || strings.ContainsRune(reader, 0) {
			return nil, fmt.Errorf("invalid source reader")
		}
	}
	revision := d.SHA256
	if d.Version != "" {
		revision = d.Version + ":" + revision
	}
	if len(revision) > 4096 || !utf8.ValidString(revision) || strings.ContainsRune(revision, 0) {
		return nil, fmt.Errorf("invalid source revision")
	}
	record := d.ToDocument()
	if _, exists := record[field]; exists {
		return nil, fmt.Errorf("source contract field conflicts with parent field")
	}
	record[field] = map[string]any{"version": 2, "uri": uri, "path": d.SourcePath, "content": opts.Content, "revision": revision, "indexed_at": opts.IndexedAt.Unix(), "permissions": access, "evidence": append([]EvidenceUnit{}, opts.Units...)}
	return record, nil
}
func (u EvidenceUnit) validate() error {
	if u.ID == "" || len(u.ID) > 256 || !utf8.ValidString(u.ID) || strings.ContainsRune(u.ID, 0) || len(u.Text) > 128*1024 || !utf8.ValidString(u.Text) || len(u.Label) > 1024 || !utf8.ValidString(u.Label) || strings.ContainsRune(u.Label, 0) {
		return fmt.Errorf("invalid evidence identity/text")
	}
	if !evidenceField(u.Asset.Field) || !lowerDigest(u.Asset.SHA256) || u.Asset.Recipe == "" || len(u.Asset.Recipe) > 1024 || !utf8.ValidString(u.Asset.Recipe) || strings.ContainsRune(u.Asset.Recipe, 0) || u.Asset.ByteLength <= 0 {
		return fmt.Errorf("invalid evidence asset")
	}
	switch u.Asset.MIMEType {
	case "image/png", "image/jpeg", "image/webp", "image/gif":
	default:
		return fmt.Errorf("unsupported evidence MIME type")
	}
	switch u.Kind {
	case "image":
		if u.Location.Page != 0 || u.Location.Slide != 0 {
			return fmt.Errorf("image location must be whole-image")
		}
	case "page":
		if u.Location.Page == 0 || u.Location.Slide != 0 {
			return fmt.Errorf("page location required")
		}
	case "slide":
		if u.Location.Slide == 0 || u.Location.Page != 0 {
			return fmt.Errorf("slide location required")
		}
	default:
		return fmt.Errorf("unsupported evidence kind")
	}
	return nil
}
func lowerDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func evidenceField(s string) bool {
	if len(s) == 0 || len(s) > 1024 {
		return false
	}
	for _, part := range strings.Split(s, ".") {
		if part == "" || part[0] == '_' {
			return false
		}
		for _, c := range part {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
				return false
			}
		}
	}
	return true
}
