// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// Manifest is a versioned set of source declarations; absent sources are untouched.
type Manifest struct {
	Version int              `json:"version"`
	Sources []ManifestSource `json:"sources"`
}

// ManifestSource manages required fields and only the optional fields it supplies.
type ManifestSource struct {
	Key                  string                            `json:"key"`
	Name                 string                            `json:"name,omitempty"`
	Connection           string                            `json:"connection"`
	Table                string                            `json:"table"`
	Config               map[string]any                    `json:"config"`
	Publication          management.SourcePublication      `json:"publication,omitempty"`
	Permissions          *management.Permissions           `json:"permissions,omitempty"`
	PermissionTtlSeconds *int                              `json:"permission_ttl_seconds,omitempty"`
	SearchVersion        *int                              `json:"search_version,omitempty"`
	Streams              *[]management.SearchStream        `json:"streams,omitempty"`
	Artifacts            *[]management.ArtifactBinding     `json:"artifacts,omitempty"`
	Processing           *management.ProcessingDeclaration `json:"processing,omitempty"`
	Identity             *management.SourceIdentity        `json:"identity,omitempty"`
	Knowledge            *management.KnowledgeDeclaration  `json:"knowledge,omitempty"`
	Extensions           map[string]json.RawMessage        `json:"extensions,omitempty"`
}

var manifestKey = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$`)

// DecodeManifest reads one strict JSON document of at most 1 MiB.
func DecodeManifest(r io.Reader) (Manifest, error) {
	const maxSize = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(r, maxSize+1))
	if err != nil {
		return Manifest{}, manifestInvalid("manifest", err.Error())
	}
	if len(raw) > maxSize {
		return Manifest{}, manifestInvalid("manifest", "manifest exceeds 1 MiB")
	}
	// Decode entries separately so even nested decoding errors identify a source.
	var document struct {
		Version int               `json:"version"`
		Sources []json.RawMessage `json:"sources"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return Manifest{}, manifestInvalid("manifest", err.Error())
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Manifest{}, manifestInvalid("manifest", "expected exactly one JSON value")
	}
	if document.Version != 1 {
		return Manifest{}, manifestInvalid("version", fmt.Sprintf("unsupported manifest version %d", document.Version))
	}
	m := Manifest{Version: document.Version, Sources: make([]ManifestSource, 0, len(document.Sources))}
	seen := make(map[string]bool, len(document.Sources))
	for i, raw := range document.Sources {
		var key struct {
			Key string `json:"key"`
		}
		// Best-effort extraction gives unknown-field errors the same path as validation.
		_ = json.Unmarshal(raw, &key)
		path := manifestSourcePath(i, key.Key)
		var source ManifestSource
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&source); err != nil {
			return Manifest{}, manifestInvalid(path, err.Error())
		}
		if err := validateManifestSource(source, seen); err != "" {
			return Manifest{}, manifestInvalid(path, err)
		}
		seen[source.Key] = true
		m.Sources = append(m.Sources, source)
	}
	return m, nil
}

func manifestSourcePath(i int, key string) string {
	return fmt.Sprintf("sources[%d] (%s)", i, key)
}

func manifestInvalid(path, message string) *Error {
	return &Error{Code: CodeInvalid, Message: path + ": " + message}
}

func validateManifestSource(s ManifestSource, seen map[string]bool) string {
	switch {
	case !manifestKey.MatchString(s.Key):
		return "key must be 1..64 lowercase letters, digits, dots, underscores or hyphens, starting and ending with a letter or digit"
	case seen[s.Key]:
		return fmt.Sprintf("duplicate key %q", s.Key)
	case s.Connection == "":
		return "connection is required"
	case s.Table == "":
		return "table is required"
	case s.Config == nil:
		return "config is required"
	}
	for _, key := range []string{
		"permissions", "permission_ttl_seconds", "publication", "streams",
		"search_version", "artifacts", "processing", "search_status", "knowledge",
	} {
		if _, ok := s.Config[key]; ok {
			return fmt.Sprintf("config must not contain reserved key %q", key)
		}
	}
	if s.Publication != "" && !s.Publication.Valid() {
		return fmt.Sprintf("invalid publication %q", s.Publication)
	}
	if s.PermissionTtlSeconds != nil && (*s.PermissionTtlSeconds < 60 || *s.PermissionTtlSeconds > 86400) {
		return "permission_ttl_seconds must be between 60 and 86400"
	}
	if s.SearchVersion != nil && *s.SearchVersion != 1 && *s.SearchVersion != 2 {
		return "search_version must be 1 or 2"
	}
	return ""
}
