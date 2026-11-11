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

package management

import (
	"regexp"
	"testing"
)

func revisionTestSource() Source {
	return Source{
		Id: "source", Name: "Documents", Connection: "connection", Table: "docs",
		Config:      map[string]any{"path": "documents", "options": map[string]any{"recursive": true}},
		Permissions: Permissions{Readers: []string{"reader"}}, PermissionTtlSeconds: 60,
		Publication: "native", Identity: SourceIdentity{Namespace: "namespace"}, SearchVersion: 2,
		Streams:    []SearchStream{{Name: "text", Field: "body"}},
		Artifacts:  []ArtifactBinding{{Name: "original", Artifact: "original", ContentType: "text/plain"}},
		Processing: &ProcessingDeclaration{Indexes: []NativeIndexDeclaration{{Name: "text", Config: map[string]any{"type": "full_text"}}}},
	}
}

func TestSourceRevisionDeterministicAndFormatted(t *testing.T) {
	source := revisionTestSource()
	revision := SourceRevision(source)
	if !regexp.MustCompile(`^r1-[0-9a-f]{64}$`).MatchString(revision) {
		t.Errorf("revision = %q, want r1- followed by 64 lowercase hex characters", revision)
	}
	for range 10 {
		if got := SourceRevision(revisionTestSource()); got != revision {
			t.Fatalf("same declaration revision = %q, want %q", got, revision)
		}
	}
}

func TestSourceRevisionIgnoresObservations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Source)
	}{
		{"State", func(s *Source) { s.State = "running" }},
		{"SearchStatus", func(s *Source) { s.SearchStatus = &SearchStatus{Version: 2, ObservedAt: 123, Consistency: "ready"} }},
		{"Rekeys", func(s *Source) { s.Rekeys = true }},
		{"Revision", func(s *Source) { s.Revision = "forged" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := revisionTestSource()
			want := SourceRevision(source)
			tc.change(&source)
			if got := SourceRevision(source); got != want {
				t.Fatalf("observation changed revision: %q != %q", got, want)
			}
		})
	}
}

func TestSourceRevisionCoversDeclarationFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Source)
	}{
		{"Id", func(s *Source) { s.Id = "another-source" }},
		{"Name", func(s *Source) { s.Name = "Other documents" }},
		{"Connection", func(s *Source) { s.Connection = "another-connection" }},
		{"Table", func(s *Source) { s.Table = "other_docs" }},
		{"Config", func(s *Source) { s.Config["path"] = "other-documents" }},
		{"Permissions.Readers", func(s *Source) { s.Permissions.Readers[0] = "another-reader" }},
		{"Permissions.Public", func(s *Source) { s.Permissions.Public = true }},
		{"PermissionTtlSeconds", func(s *Source) { s.PermissionTtlSeconds = 120 }},
		{"Publication", func(s *Source) { s.Publication = "media" }},
		{"Identity", func(s *Source) { s.Identity.Namespace = "another-namespace" }},
		{"SearchVersion", func(s *Source) { s.SearchVersion = 1 }},
		{"Streams", func(s *Source) { s.Streams[0].Field = "title" }},
		{"Artifacts", func(s *Source) { s.Artifacts[0].ContentType = "application/pdf" }},
		{"Processing", func(s *Source) { s.Processing.Indexes[0].Config["type"] = "graph" }},
		{"Knowledge", func(s *Source) { s.Knowledge = &KnowledgeDeclaration{Audience: "team", Version: 1} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := revisionTestSource()
			before := SourceRevision(source)
			tc.change(&source)
			if got := SourceRevision(source); got == before {
				t.Fatalf("changing %s did not change revision %q", tc.name, got)
			}
		})
	}
}

func TestSourceRevisionIgnoresMapInsertionOrder(t *testing.T) {
	first, second := revisionTestSource(), revisionTestSource()
	first.Config = make(map[string]any)
	first.Config["a"] = 1
	first.Config["z"] = map[string]any{"a": true, "z": false}
	second.Config = make(map[string]any)
	nested := make(map[string]any)
	nested["z"] = false
	nested["a"] = true
	second.Config["z"] = nested
	second.Config["a"] = 1
	if a, b := SourceRevision(first), SourceRevision(second); a != b {
		t.Fatalf("map insertion order changed revision: %q != %q", a, b)
	}
}
