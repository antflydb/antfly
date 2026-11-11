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

package commands

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const minimalManifestSource = `{"key":"engineering-docs","connection":"docs-connection","table":"docs","config":{}}`

func TestDecodeManifestMinimal(t *testing.T) {
	m, err := DecodeManifest(strings.NewReader(`{"version":1,"sources":[` + minimalManifestSource + `]}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 1 || len(m.Sources) != 1 {
		t.Fatalf("manifest = %#v, want version 1 and one source", m)
	}
	s := m.Sources[0]
	if s.Key != "engineering-docs" || s.Connection != "docs-connection" || s.Table != "docs" || s.Config == nil || len(s.Config) != 0 {
		t.Fatalf("minimal source = %#v", s)
	}
	if s.Permissions != nil || s.PermissionTtlSeconds != nil || s.SearchVersion != nil || s.Streams != nil || s.Artifacts != nil || s.Processing != nil || s.Identity != nil || s.Knowledge != nil {
		t.Fatalf("omitted fields must remain unmanaged: %#v", s)
	}
}

func TestDecodeManifestInvalid(t *testing.T) {
	wrap := func(source string) string { return `{"version":1,"sources":[` + source + `]}` }
	for _, tc := range []struct {
		name, input, message string
	}{
		{"unknown top-level field", `{"version":1,"sources":[],"surprise":true}`, `unknown field "surprise"`},
		{"unknown stream field", wrap(strings.TrimSuffix(minimalManifestSource, "}") + `,"streams":[{"name":"text","surprise":true}]}`), `unknown field "surprise"`},
		{"unsupported version", `{"version":2,"sources":[]}`, "unsupported manifest version 2"},
		{"duplicate key", `{"version":1,"sources":[` + minimalManifestSource + `,` + minimalManifestSource + `]}`, "engineering-docs"},
		{"bad key", wrap(strings.Replace(minimalManifestSource, "engineering-docs", "Engineering Docs", 1)), "key"},
		{"empty key", wrap(strings.Replace(minimalManifestSource, "engineering-docs", "", 1)), "key"},
		{"key trailing punctuation", wrap(strings.Replace(minimalManifestSource, "engineering-docs", "docs-", 1)), "key"},
		{"key too long", wrap(strings.Replace(minimalManifestSource, "engineering-docs", strings.Repeat("a", 65), 1)), "key"},
		{"missing connection", wrap(strings.Replace(minimalManifestSource, `"docs-connection"`, `""`, 1)), "connection"},
		{"missing table", wrap(strings.Replace(minimalManifestSource, `"table":"docs"`, `"table":""`, 1)), "table"},
		{"nil config", wrap(strings.Replace(minimalManifestSource, `"config":{}`, `"config":null`, 1)), "config"},
		{"missing config", wrap(strings.Replace(minimalManifestSource, `,"config":{}`, "", 1)), "config"},
		{"invalid publication", wrap(strings.TrimSuffix(minimalManifestSource, "}") + `,"publication":"unknown"}`), "publication"},
		{"ttl below minimum", wrap(strings.TrimSuffix(minimalManifestSource, "}") + `,"permission_ttl_seconds":30}`), "permission_ttl_seconds"},
		{"ttl above maximum", wrap(strings.TrimSuffix(minimalManifestSource, "}") + `,"permission_ttl_seconds":86401}`), "permission_ttl_seconds"},
		{"invalid search version", wrap(strings.TrimSuffix(minimalManifestSource, "}") + `,"search_version":3}`), "search_version"},
		{"oversized", `{"version":1,"sources":[]}` + strings.Repeat(" ", 1<<20), ""},
		{"two JSON values", wrap(minimalManifestSource) + ` {"version":1,"sources":[]}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeManifest(strings.NewReader(tc.input))
			failure := assertCode(t, err, CodeInvalid)
			if !strings.Contains(failure.Message, tc.message) {
				t.Fatalf("message = %q, want substring %q", failure.Message, tc.message)
			}
		})
	}
}

func TestDecodeManifestReservedConfigKeys(t *testing.T) {
	for _, key := range []string{"permissions", "permission_ttl_seconds", "publication", "streams", "search_version", "artifacts", "processing", "search_status", "knowledge"} {
		t.Run(key, func(t *testing.T) {
			source := strings.Replace(minimalManifestSource, `"config":{}`, fmt.Sprintf(`"config":{%q:null}`, key), 1)
			_, err := DecodeManifest(strings.NewReader(`{"version":1,"sources":[` + source + `]}`))
			failure := assertCode(t, err, CodeInvalid)
			if !strings.HasPrefix(failure.Message, "sources[0] (engineering-docs): ") || !strings.Contains(failure.Message, key) {
				t.Fatalf("reserved config diagnostic = %q", failure.Message)
			}
		})
	}
}

func TestDecodeManifestPathPrefix(t *testing.T) {
	bad := strings.Replace(minimalManifestSource, `"table":"docs"`, `"table":""`, 1)
	first := strings.Replace(minimalManifestSource, "engineering-docs", "first", 1)
	_, err := DecodeManifest(strings.NewReader(`{"version":1,"sources":[` + first + `,` + bad + `]}`))
	failure := assertCode(t, err, CodeInvalid)
	if !strings.HasPrefix(failure.Message, "sources[1] (engineering-docs): ") {
		t.Fatalf("message = %q, want indexed source/key prefix", failure.Message)
	}
}

func TestDecodeManifestOptionalPresenceAndBoundaries(t *testing.T) {
	for _, ttl := range []int{60, 86400} {
		for _, version := range []int{1, 2} {
			t.Run(fmt.Sprintf("ttl=%d/version=%d", ttl, version), func(t *testing.T) {
				source := strings.TrimSuffix(minimalManifestSource, "}") + fmt.Sprintf(`,"permission_ttl_seconds":%d,"search_version":%d,"streams":[],"artifacts":[]}`, ttl, version)
				m, err := DecodeManifest(strings.NewReader(`{"version":1,"sources":[` + source + `]}`))
				if err != nil {
					t.Fatal(err)
				}
				if len(m.Sources) != 1 {
					t.Fatalf("sources = %#v", m.Sources)
				}
				s := m.Sources[0]
				if s.PermissionTtlSeconds == nil || *s.PermissionTtlSeconds != ttl || s.SearchVersion == nil || *s.SearchVersion != version || s.Streams == nil || len(*s.Streams) != 0 || s.Artifacts == nil || len(*s.Artifacts) != 0 {
					t.Fatalf("present optional fields lost: %#v", s)
				}
			})
		}
	}
}

func TestManifestPreservesLargeConfigIntegers(t *testing.T) {
	m, err := DecodeManifest(strings.NewReader(`{"version":1,"sources":[{"key":"docs","connection":"conn","table":"docs","config":{"account_id":9007199254740993,"nested":[9223372036854775807]}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m.Sources[0].Config)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "9007199254740993") || !strings.Contains(string(raw), "9223372036854775807") {
		t.Fatalf("rounded manifest integers: %s", raw)
	}
}
