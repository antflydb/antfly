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
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Permissions struct {
	Public  bool     `json:"public"`
	Readers []string `json:"readers"`
}
type Document struct {
	MIMEType    string          `json:"mime_type,omitempty"`
	Version     int             `json:"version"`
	Kind        string          `json:"kind,omitempty"`
	Media       *MediaReference `json:"media,omitempty"`
	URI         string          `json:"uri"`
	Path        string          `json:"path"`
	Content     string          `json:"content"`
	Revision    string          `json:"revision"`
	IndexedAt   int64           `json:"indexed_at"`
	Permissions Permissions     `json:"permissions"`
	Metadata    map[string]any  `json:"metadata"`
}
type NormalizationError struct{ Code, Detail string }

func (e *NormalizationError) Error() string { return e.Detail }

// relativeKeyFor resolves an item's key relative to its source's root. A
// connector that reports one (local, S3, GCS: see their RelativeKey doc
// comment) uses it as-is. Otherwise it falls back to the item's native ID --
// item.ID with the connector's own "scheme://job/<sourceID>/" ownership
// prefix stripped -- which is exactly right for providers with no path-like
// key of their own (Drive, GitHub, and other collaboration surfaces): their
// identity already is their native ID, and moving such an item across
// providers is never a meaningful operation to begin with.
func relativeKeyFor(sourceID string, item Item) string {
	if item.RelativeKeySet || item.RelativeKey != "" {
		return item.RelativeKey
	}
	if _, rest, ok := strings.Cut(item.ID, "://job/"+sourceID+"/"); ok {
		return rest
	}
	return item.ID
}
func Normalize(sourceID string, item Item, data []byte, permissions Permissions, now time.Time) (Document, error) {
	fail := func(code, detail string) (Document, error) { return Document{}, &NormalizationError{code, detail} }
	if item.Revision == "" || len(item.Revision) > 4096 || !utf8.ValidString(item.Revision) || strings.ContainsRune(item.Revision, 0) {
		return fail("source_revision_missing", "Source revision is required.")
	}
	if len(data) > maxImportSourceObjectBytes || !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return fail("source_text_extraction_required", "Inline content must be bounded UTF-8; publish binary originals through native media acquisition.")
	}
	digest := sha256.Sum256(data)
	digestHex := hex.EncodeToString(digest[:])
	uri := configString(item.Metadata, "canonical_url")
	if item.SourceKind == ProviderGCS && configString(item.Metadata, "projection_uri") != "" {
		original, err := url.Parse(configString(item.Metadata, "original_pdf_uri"))
		page, pageErr := strconv.Atoi(configString(item.Metadata, "original_page"))
		if err != nil || original.User != nil || original.Scheme != "gs" || original.Port() != "" ||
			original.Hostname() == "" || len(original.Path) < 2 || !validPageCitationQuery(original) || original.Fragment != "" || original.Opaque != "" ||
			pageErr != nil || page <= 0 || configString(item.Metadata, "extraction_revision") == "" ||
			configString(item.Metadata, "content_sha256") != digestHex {
			return fail("source_page_provenance_invalid", "Page provenance is missing or does not match the projected text.")
		}
		original.Fragment = "page=" + strconv.Itoa(page)
		uri = original.String()
	}
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
	// Enumeration position is diagnostic bookkeeping, not document identity.
	delete(metadata, "object_index")
	metadata["sha256"] = digestHex
	metadata["provider"] = item.SourceKind
	metadata["source_id"] = sourceID
	metadata["identity"] = item.ID
	metadata["relative_key"] = relativeKeyFor(sourceID, item)
	return Document{Version: SourceRecordVersion, Kind: "text", MIMEType: "text/plain", URI: parsed.String(), Path: path, Content: string(data), Revision: item.Revision, IndexedAt: now.Unix(), Permissions: permissions, Metadata: metadata}, nil
}

// MediaReference is the model-aware chunk source contract. Version is a quoted
// strong ETag for the immutable bytes served at URI. Service uses the verified
// original-byte SHA-256; custom fetchers may use another strong representation tag.
type MediaReference struct {
	// ContentSHA256 is acquisition evidence, stored in the document metadata,
	// not sent as an extension to the inference source-reference wire contract.
	ContentSHA256 string `json:"-"`
	Type          string `json:"type"`
	URI           string `json:"uri"`
	MIMEType      string `json:"mime_type"`
	Version       string `json:"version"`
	VersionKind   string `json:"version_kind"`
	Length        int64  `json:"length"`
}

// NormalizeMedia publishes an explicit media row without claiming that provider
// metadata is a digest of bytes the normalizer has not read. Fetchers must enforce
// the pinned source revision before returning any bytes.
func NormalizeMedia(sourceID string, item Item, ref MediaReference, permissions Permissions, now time.Time) (Document, error) {
	if item.Size != ref.Length {
		return Document{}, fmt.Errorf("media length differs from pinned inventory")
	}
	doc, err := Normalize(sourceID, item, nil, permissions, now)
	if err != nil {
		return Document{}, err
	}
	doc.Version, doc.Kind, doc.Media = SourceRecordVersion, "media", &ref
	if !validContentSHA(ref.ContentSHA256) {
		return Document{}, fmt.Errorf("verified original-byte SHA-256 is required")
	}
	doc.Metadata["sha256"] = ref.ContentSHA256
	if err := validateDocumentContent(doc); err != nil {
		return Document{}, err
	}
	return doc, nil
}

func validateDocumentContent(doc Document) error {
	if (doc.Version == 1 && doc.Kind == "" || doc.Version == SourceRecordVersion && doc.Kind == "text") && doc.Media == nil {
		digest := sha256.Sum256([]byte(doc.Content))
		if !utf8.ValidString(doc.Content) || strings.ContainsRune(doc.Content, 0) || configString(doc.Metadata, "sha256") != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("source content digest does not match normalized text")
		}
		return nil
	}
	hasContentDigest := validContentSHA(configString(doc.Metadata, "sha256"))
	if (doc.Version != 2 && doc.Version != SourceRecordVersion) || doc.Kind != "media" || doc.Media == nil || doc.Content != "" || !hasContentDigest {
		return fmt.Errorf("invalid canonical media document")
	}
	ref := doc.Media
	u, err := url.Parse(ref.URI)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http" && u.Scheme != "s3") || len(ref.URI) > 16384 || strings.ContainsAny(ref.URI, "\x00\r\n") {
		return fmt.Errorf("invalid media URI")
	}
	if ref.Type != "source" || ref.Length < 1 || ref.MIMEType == "" || strings.ContainsAny(ref.MIMEType, "\x00\r\n") || len(ref.MIMEType) > 255 || ref.VersionKind != "strong_etag" || len(ref.Version) < 3 || len(ref.Version) > 4096 || ref.Version[0] != '"' || ref.Version[len(ref.Version)-1] != '"' || strings.ContainsAny(ref.Version[1:len(ref.Version)-1], "\"\x00\r\n") {
		return fmt.Errorf("invalid pinned media reference")
	}
	return nil
}

// A GCS generation pins the original PDF without carrying signed URL credentials.
func validPageCitationQuery(uri *url.URL) bool {
	values, err := url.ParseQuery(uri.RawQuery)
	generation := values["generation"]
	if err != nil || len(values) != 1 || len(generation) != 1 {
		return false
	}
	number, err := strconv.ParseInt(generation[0], 10, 64)
	return err == nil && number > 0
}
