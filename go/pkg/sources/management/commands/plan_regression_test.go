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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

func TestManifestEmptyCollectionsConvergeOverHTTP(t *testing.T) {
	for _, create := range []bool{true, false} {
		t.Run(map[bool]string{true: "create", false: "clear"}[create], func(t *testing.T) {
			api := newManifestAPI(t)
			entry := manifestEntry("docs")
			version := 2
			entry.SearchVersion = &version
			streams := []management.SearchStream{}
			artifacts := []management.ArtifactBinding{}
			entry.Streams, entry.Artifacts = &streams, &artifacts
			if !create {
				current := manifestCurrent("docs")
				current.SearchVersion = 2
				current.Streams = []management.SearchStream{{Name: "text", Field: "body", Retrieval: &management.RetrievalBindings{Exact: &management.ExactRetrieval{}}}}
				current.Artifacts = []management.ArtifactBinding{{Name: "original", Artifact: "original", ContentType: "text/plain"}}
				api.sources[current.Id] = current
			}
			server := httptest.NewServer(management.Handler(api))
			defer server.Close()
			client, err := management.NewClient(server.URL, "test-token")
			if err != nil {
				t.Fatal(err)
			}
			env := Env{API: client, Out: io.Discard}
			results, err := ApplyManifest(t.Context(), env, manifestFile(entry), PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := "updated"
			if create {
				want = "created"
			}
			requireApplyOutcomes(t, results, []string{"docs"}, []string{want})
			writes := len(api.writes)
			results, err = ApplyManifest(t.Context(), env, manifestFile(entry), PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			requireApplyOutcomes(t, results, []string{"docs"}, []string{"unchanged"})
			if len(api.writes) != writes {
				t.Fatal("unchanged manifest wrote again")
			}
		})
	}
}

func TestManifestOmittedNamePreservesExistingDisplayName(t *testing.T) {
	current := manifestCurrent("docs")
	current.Name = "Engineering docs"
	api := newManifestAPI(t, current)
	entry := manifestEntry("docs")
	entry.Name = ""
	results, err := ApplyManifest(t.Context(), Env{API: api, Out: io.Discard}, manifestFile(entry), PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireApplyOutcomes(t, results, []string{"docs"}, []string{"unchanged"})
	if len(api.writes) != 0 || api.sources["docs"].Name != current.Name {
		t.Fatal("omitted name changed existing display name")
	}
}

func TestManifestValidatesAllEffectiveDeclarationsBeforeWrites(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*management.Source)
	}{
		{"stream name", func(s *management.Source) { s.Streams = []management.SearchStream{{}} }},
		{"stream boundary", func(s *management.Source) {
			s.Streams = []management.SearchStream{{Name: "text", Field: "body", Boundary: "invalid"}}
		}},
		{"artifact", func(s *management.Source) {
			s.SearchVersion = 2
			s.Artifacts = []management.ArtifactBinding{{Name: "original"}}
		}},
		{"processing", func(s *management.Source) {
			s.Processing = &management.ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: "text", Config: map[string]any{}}}}
		}},
		{"knowledge", func(s *management.Source) { s.Knowledge = &management.KnowledgeDeclaration{Version: 99} }},
		{"unbound graph processing", func(s *management.Source) {
			s.Processing = &management.ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: "graph", Config: map[string]any{"type": "graph"}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Invalid preserved declarations must be checked too, not just
			// optional fields explicitly provided by this manifest.
			current := manifestCurrent("invalid")
			tc.change(&current)
			api := newManifestAPI(t, current)
			var out bytes.Buffer
			_, err := ApplyManifest(t.Context(), Env{API: api, Out: &out}, manifestFile(manifestEntry("new"), manifestEntry("invalid")), PlanOptions{})
			failure := assertCode(t, err, CodeInvalid)
			if !strings.Contains(failure.Message, "sources[1] (invalid)") || len(api.writes) != 0 || out.Len() != 0 {
				t.Fatalf("error=%v writes=%v output=%s", err, api.writes, &out)
			}
		})
	}
}

func TestApplyRetainsFailureCategories(t *testing.T) {
	for _, tc := range []struct {
		name         string
		err          error
		code         string
		exit, status int
		server       string
	}{
		{"invalid", management.Failure(400, "invalid_source"), CodeInvalid, 2, 400, "invalid_source"},
		{"unauthorized", management.Failure(401, "expired"), CodeUnauthorized, 3, 401, "expired"},
		{"forbidden", management.Failure(403, "denied"), CodeForbidden, 3, 403, "denied"},
		{"not found", management.Failure(404, "source_not_found"), CodeNotFound, 4, 404, "source_not_found"},
		{"conflict", management.Failure(412, "source_revision_mismatch"), CodeConflict, 5, 412, "source_revision_mismatch"},
		{"unsupported", management.Failure(501, "unsupported"), CodeUnsupported, 6, 501, "unsupported"},
		{"timeout", context.DeadlineExceeded, CodeTimeout, 7, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newManifestAPI(t)
			api.putFailures["docs"] = tc.err
			var out bytes.Buffer
			results, err := ApplyManifest(t.Context(), Env{API: api, Out: &out, JSON: true}, manifestFile(manifestEntry("docs")), PlanOptions{})
			failure := assertCode(t, err, tc.code)
			if failure.ExitCode() != tc.exit || failure.HTTPStatus != tc.status || failure.ServerCode != tc.server {
				t.Fatalf("aggregate: %#v", failure)
			}
			var wire struct {
				Results []ApplyResult `json:"results"`
			}
			if err := json.Unmarshal(out.Bytes(), &wire); err != nil {
				t.Fatal(err)
			}
			got := wire.Results[0]
			if len(results) != 1 || got.ErrorCode != tc.code || got.HTTPStatus != tc.status || got.ServerCode != tc.server {
				t.Fatalf("per-source error: %#v", got)
			}
		})
	}
}

func TestApplyMixedFailureCategoriesAreOrderIndependent(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		api := newManifestAPI(t)
		api.putFailures["first"] = management.Failure(403, "denied")
		api.putFailures["second"] = management.Failure(412, "source_revision_mismatch")
		entries := []ManifestSource{manifestEntry("first"), manifestEntry("second")}
		if reverse {
			entries[0], entries[1] = entries[1], entries[0]
		}
		results, err := ApplyManifest(t.Context(), Env{API: api, Out: io.Discard}, manifestFile(entries...), PlanOptions{})
		failure := assertCode(t, err, CodeFailed)
		if failure.ExitCode() != 1 || failure.HTTPStatus != 0 || failure.ServerCode != "" || len(results) != 2 {
			t.Fatalf("mixed aggregate: %#v, results: %#v", failure, results)
		}
		for _, result := range results {
			if result.ErrorCode == "" || result.HTTPStatus == 0 || result.ServerCode == "" {
				t.Fatalf("lost details: %#v", result)
			}
		}
	}
}
