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

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolverCurationAcceptedResponseAndEscapedKeys(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/db/v1/tables/notes/resolvers/concepts/redirects/concept%2Falpha" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"resolver":"concepts","changed":true,"revision":3,"replay_sequence":3,"replay_sequence_kind":"resolver_revision","applied":false}`))
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.UnmergeResolverKey(context.Background(), "notes", "concepts", "concept/alpha", nil, UnmergeResolverKeyJSONRequestBody{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Revision != 3 || result.Applied || calls != 1 {
		t.Fatalf("accepted replay must be returned once: %+v, calls=%d", result, calls)
	}
}

func TestResolverRevisionFencePresence(t *testing.T) {
	zero := uint64(0)
	for _, tc := range []struct {
		name    string
		body    any
		present bool
	}{
		{"merge unset", oapi.ResolverMergeRequest{From: "a", To: "b"}, false},
		{"merge zero", oapi.ResolverMergeRequest{From: "a", To: "b", ExpectedRevision: &zero}, true},
		{"unmerge zero", oapi.ResolverUnmergeRequest{ExpectedRevision: &zero}, true},
		{"suppress zero", oapi.ResolverSuppressionRequest{ExpectedRevision: &zero}, true},
		{"namespace zero", oapi.ResolverNamespaceRequest{ExpectedRevision: &zero}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err = json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			fence, present := fields["expected_revision"]
			if present != tc.present || present && string(fence) != "0" {
				t.Fatalf("unexpected fence: %s", raw)
			}
		})
	}
}

func TestResolverFirstEditRevisionFenceRejectsConcurrentRetry(t *testing.T) {
	for _, namespaced := range []bool{false, true} {
		t.Run(fmt.Sprint(namespaced), func(t *testing.T) {
			revision := uint64(0)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					ExpectedRevision *uint64 `json:"expected_revision"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				if body.ExpectedRevision == nil {
					t.Error("lost explicit zero revision fence")
					w.WriteHeader(400)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if *body.ExpectedRevision != revision {
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"error":"ResolverCurationConflict"}`))
					return
				}
				revision++
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{"resolver":"concepts","changed":true,"revision":1,"replay_sequence":1,"applied":false}`))
			}))
			defer server.Close()
			client, err := NewAntflyClient(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			zero := uint64(0)
			body := oapi.ResolverMergeRequest{From: "a", To: "b", ExpectedRevision: &zero}
			call := func() error {
				if namespaced {
					_, err := client.NamespaceMergeResolverKeys(context.Background(), "default", "public", "notes", "concepts", body)
					return err
				}
				_, err := client.MergeResolverKeys(context.Background(), "notes", "concepts", body)
				return err
			}
			if err := call(); err != nil {
				t.Fatal(err)
			}
			if err := call(); err == nil {
				t.Fatal("concurrent retry bypassed the first-edit revision fence")
			}
			if revision != 1 {
				t.Fatalf("unexpected accepted revision %d", revision)
			}
		})
	}
}

func TestCurationProposalAcceptReturnsPendingReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.EscapedPath() != "/db/v1/databases/app/namespaces/private/tables/entities/curation/proposals/proposal%2Fone:accept" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["actor"] != "curator" {
			t.Errorf("lost attribution: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"proposal":{"id":"proposal/one","action":"suppress","key":"concept/noise","origin":{"origin":"manual"},"state":"accepted"},"complete":true,"applied":false,"results":[]}`))
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.NamespaceAcceptCurationProposal(context.Background(), "app", "private", "entities", "proposal/one", NamespaceAcceptCurationProposalJSONRequestBody{Actor: "curator"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied || !result.Complete || result.Proposal.Id != "proposal/one" {
		t.Fatalf("lost pending result: %+v", result)
	}
}

func TestCurationProposalPreservesExplicitZeroAndEmptyOptions(t *testing.T) {
	cases := []struct {
		name, input, field string
		value              any
	}{
		{"request zero confidence", `{"action":"suppress","key":"concept/noise","confidence":0}`, "confidence", new(CurationProposalRequest)},
		{"response zero confidence", `{"id":"id","action":"suppress","key":"concept/noise","state":"open","origin":{},"confidence":0}`, "confidence", new(CurationProposal)},
		{"empty options override", `{"options":{}}`, "options", new(CurationProposalDecisionRequest)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(tc.input), tc.value); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			if _, exists := fields[tc.field]; !exists {
				t.Fatalf("lost explicit %s in %s", tc.field, encoded)
			}
		})
	}
}

func TestEntityResolverProgressPagingAndNamespace(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		prefix := "/db/v1/tables/notes"
		if calls == 2 {
			prefix = "/db/v1/databases/app/namespaces/private/tables/notes"
		}
		if r.URL.EscapedPath() != prefix+"/entities/concept%2Falpha/progress" || r.URL.Query().Get("limit") != "7" || r.URL.Query().Get("cursor") != "opaque-next" {
			t.Errorf("unexpected progress request %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entity_table":"notes","key":"concept/alpha","catalog_revision":3,"complete":false,"next_cursor":"page-two","results":[]}`))
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	page, err := client.ListEntityResolverProgress(context.Background(), "notes", "concept/alpha", &ListEntityResolverProgressParams{Limit: 7, Cursor: "opaque-next"})
	if err != nil || page.Complete || page.NextCursor != "page-two" {
		t.Fatalf("invalid page: %+v %v", page, err)
	}
	_, err = client.NamespaceListEntityResolverProgress(context.Background(), "app", "private", "notes", "concept/alpha", &NamespaceListEntityResolverProgressParams{Limit: 7, Cursor: "opaque-next"})
	if err != nil {
		t.Fatal(err)
	}
}
