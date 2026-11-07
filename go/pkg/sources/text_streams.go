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
	"fmt"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// TextStream binds a native relation to textual content. Artifact names and
// index names are application configuration, never protocol conventions.
type TextStream = management.TextStream

type DocumentFields struct {
	URI         string `json:"uri"`
	Path        string `json:"path"`
	Revision    string `json:"revision"`
	IndexedAt   string `json:"indexed_at"`
	Permissions string `json:"permissions"`
	Metadata    string `json:"metadata"`
}

func CanonicalDocumentFields() *DocumentFields {
	return &DocumentFields{URI: "source.uri", Path: "source.path", Revision: "source.revision", IndexedAt: "source.indexed_at", Permissions: "permissions", Metadata: "metadata"}
}
func DefaultTextStreams() []TextStream {
	return []TextStream{{Name: "source", Field: "content.text", Purpose: "source", Default: true, Boundary: "record"}}
}
func ValidateTextStreams(streams []TextStream) error {
	if len(streams) > 64 {
		return fmt.Errorf("at most 64 text streams are supported")
	}
	names := map[string]bool{}
	for _, stream := range streams {
		if stream.Name == "" || names[stream.Name] || stream.Field == "" || strings.ContainsAny(stream.Name+stream.Field+stream.Artifact+stream.Index+stream.Purpose+stream.LocationField, "\x00\r\n") {
			return fmt.Errorf("invalid or duplicate text stream")
		}
		names[stream.Name] = true
		if stream.Artifact == "" && stream.Index != "" {
			return fmt.Errorf("inline source streams use ordinary source scans; index requires an artifact")
		}
		if stream.Artifact != "" && stream.Index == "" {
			return fmt.Errorf("artifact text streams require a native full-text index binding")
		}
		if stream.Boundary != "" && stream.Boundary != "record" && stream.Boundary != "unit" {
			return fmt.Errorf("unsupported text matching boundary")
		}
	}
	return nil
}

func (fields DocumentFields) Validate() error {
	for _, field := range []string{fields.URI, fields.Path, fields.Revision, fields.IndexedAt, fields.Permissions, fields.Metadata} {
		if field == "" || strings.ContainsAny(field, "\x00\r\n") {
			return fmt.Errorf("invalid source field mapping")
		}
	}
	return nil
}
