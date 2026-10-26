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

// Package sources acquires and normalizes provider content independently of a
// scheduler, credential store, Cloud account, or database deployment.
package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

const (
	ProviderGitHub             = "github"
	ProviderGoogleDrive        = "google_drive"
	ProviderGCS                = "gcs"
	ProviderS3                 = "s3"
	ProviderSlack              = "slack"
	ProviderDiscord            = "discord"
	ProviderGoogleWorkspace    = "google_workspace"
	ImportModeSource           = "source"
	maxImportSourceObjects     = 10000
	maxImportSourceObjectBytes = 32 << 20
	sourceReadBytes            = "bytes"
)

var ErrInvalidInput = errors.New("invalid source input")
var ErrInventoryIncomplete = errors.New("source inventory incomplete; start a new full run")

type SourceConfig = json.RawMessage
type Request struct {
	SourceNamespace string
	Config          SourceConfig
	Mode            string
	Preview         bool
	RetrySourceKeys []string
}
type Item struct {
	// ContentRef is a private acquisition checkpoint, never document metadata.
	ContentRef                                              string `json:",omitempty"`
	ID, Path, Filename, Revision, URL, SourceKind, ReadMode string
	Size                                                    int64
	Metadata                                                map[string]any
	ObjectIndex                                             int
}
type DiagnosticSource struct{ Key, SourceKind, Revision string }
type Diagnostic struct {
	Level, Code, Detail string
	Source              *DiagnosticSource
}
type Inventory struct {
	ProviderState  map[string]string `json:",omitempty"`
	SourceRevision string
	Authoritative  bool
	Objects        []Item
	Diagnostics    []Diagnostic
	BatchObjects   int
	BatchBytes     int64
}
type Input struct {
	Item
	Bytes     []byte
	ReadError error
}
type Connector interface {
	ValidateInventory(context.Context, Request, Inventory) error
	Enumerate(context.Context, Request) (Inventory, error)
	Read(context.Context, Request, Item) ([]byte, error)
	CheckAccess(context.Context, Request, Item) error
	ValidateAbsentSources(context.Context, Request, []string) error
}
type RevisionChangedError struct{ Expected, Actual string }

func (e *RevisionChangedError) Error() string {
	return fmt.Sprintf("source revision changed while reading: enumerated %q, read %q", e.Expected, e.Actual)
}
func decodeSourceConfig[T any](raw SourceConfig) (T, error) {
	var out T
	if len(raw) == 0 || string(raw) == "null" {
		return out, fmt.Errorf("%w: source config required", ErrInvalidInput)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("%w: source config: %v", ErrInvalidInput, err)
	}
	return out, nil
}
func exactSourceKeySet(keys []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, key := range keys {
		out[key] = struct{}{}
	}
	return out
}
func sourceObjectRetryKey(o Item) string {
	if o.SourceKind == ProviderGoogleDrive {
		return o.ID
	}
	return o.Path
}
func configString(m map[string]any, key string) string { v, _ := m[key].(string); return v }
func cloneSourceMetadata(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
func sourceObjectAllowed(key string, suffixes, patterns, exclude []string) bool {
	base := filepath.Base(key)
	suffixOK := len(suffixes) == 0
	for _, suffix := range suffixes {
		if strings.HasSuffix(strings.ToLower(base), strings.ToLower(strings.TrimSpace(suffix))) {
			suffixOK = true
		}
	}
	match := func(patterns []string, defaultValue bool) bool {
		has := false
		for _, p := range patterns {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			has = true
			for _, s := range []string{key, base} {
				if p == s {
					return true
				}
				if ok, _ := filepath.Match(p, s); ok {
					return true
				}
				if wildcardMatch(p, s) {
					return true
				}
			}
		}
		return !has && defaultValue
	}
	return suffixOK && match(patterns, true) && !match(exclude, false)
}
func wildcardMatch(pattern, value string) bool {
	p, v, star, mark := 0, 0, -1, 0
	for v < len(value) {
		if p < len(pattern) && (pattern[p] == '?' || pattern[p] == value[v]) {
			p++
			v++
		} else if p < len(pattern) && pattern[p] == '*' {
			star = p
			p++
			mark = v
		} else if star >= 0 {
			p = star + 1
			mark++
			v = mark
		} else {
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}
func readBounded(r io.Reader) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r, maxImportSourceObjectBytes+1))
	if e != nil {
		return nil, e
	}
	if len(b) > maxImportSourceObjectBytes {
		return nil, fmt.Errorf("%w: source exceeds byte limit", ErrInvalidInput)
	}
	return b, nil
}

func inputFromObject(o Item) Input { return Input{Item: o} }
