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
	"errors"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCypherExactIntegersAndReceiptInspectionNeverReplay(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			if !strings.HasSuffix(r.URL.Path, "/0123456789abcdef0123456789abcdef/status") {
				t.Errorf("unexpected receipt path %s", r.URL.Path)
			}
			_, _ = io.WriteString(w, `{"transaction_id":"0123456789abcdef0123456789abcdef","disposition":"committed"}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"value":"9223372036854775807"`) {
			t.Errorf("integer lost: %s", body)
		}
		_, _ = io.WriteString(w, `{"wire_version":1,"columns":["n"],"rows":[[{"type":"integer","value":"9223372036854775807"}]],"plan":[],"stats":{},"graph_incarnation":"1","graph_revision":"1","complete":false,"commit":{"transaction_id":"0123456789abcdef0123456789abcdef","status":"unknown"}}`)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ExecuteGraphQuery(context.Background(), "knowledge", GraphQueryRequest{Query: "RETURN $n", TypedParameters: GraphValue{Type: oapi.GraphWireValueTypeMap, Value: map[string]GraphValue{"n": {Type: oapi.GraphWireValueTypeInteger, Value: "9223372036854775807"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0][0].Value != "9223372036854775807" || result.Commit.Status != "unknown" {
		t.Fatalf("lossy result %#v", result)
	}
	if _, err = client.GraphWriteStatus(context.Background(), result.Commit.TransactionId); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("unexpected replay: %d calls", calls)
	}
}
func TestCypherErrorsPreserveCommittedDeliveryOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"code":"OutOfMemory","message":"Encoding failed","write_outcome":"committed"}`)
	}))
	defer server.Close()
	client, _ := NewAntflyClient(server.URL, server.Client())
	_, err := client.ExecuteGraphQuery(context.Background(), "knowledge", GraphQueryRequest{Query: "CREATE (n:Document) RETURN n"})
	var diagnostic *CypherExecutionError
	if !errors.As(err, &diagnostic) || diagnostic.WriteOutcome != "committed" {
		t.Fatalf("lost write outcome: %v", err)
	}
}

func TestGraphCatalogMutationsPreservePendingVisibility(t *testing.T) {
	for _, operation := range []string{"create", "update", "drop"} {
		t.Run(operation, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(w, `{"status":"committed_visibility_pending"}`)
			}))
			defer server.Close()
			client, _ := NewAntflyClient(server.URL, server.Client())
			var outcome *oapi.CommittedMutationOutcome
			var err error
			switch operation {
			case "drop":
				outcome, err = client.DropGraph(context.Background(), "g")
			default:
				var result *GraphCatalogMutationResult
				if operation == "create" {
					result, err = client.CreateGraph(context.Background(), "g", GraphDefinition{})
				} else {
					result, err = client.UpdateGraph(context.Background(), "g", "1", GraphDefinition{})
				}
				if err == nil {
					if result.Entry != nil {
						t.Fatal("pending result fabricated a catalog entry")
					}
					outcome = result.Outcome
				}
			}
			if err != nil || outcome == nil || outcome.Status != oapi.CommittedMutationOutcomeStatusCommittedVisibilityPending {
				t.Fatalf("lost pending outcome: %+v, %v", outcome, err)
			}
			if calls != 1 {
				t.Fatalf("mutation replayed: %d calls", calls)
			}
		})
	}
}

func TestGraphCatalogMutationVisibleResultsAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		valid  bool
	}{
		{201, `{"name":"g","graph_id":"1","revision":"1","definition":{"labels":[]}}`, true},
		{202, `{"status":"unknown"}`, false},
		{201, `{"status":"committed_visibility_pending"}`, false},
		{409, `{"code":"CatalogGenerationChanged","message":"stale revision"}`, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			client, _ := NewAntflyClient(server.URL, server.Client())
			result, err := client.CreateGraph(context.Background(), "g", GraphDefinition{})
			if tc.valid {
				if err != nil || result.Entry == nil || result.Entry.Name != "g" || result.Outcome != nil {
					t.Fatalf("invalid visible result: %+v, %v", result, err)
				}
			} else if err == nil {
				t.Fatalf("accepted invalid result: %+v", result)
			}
		})
	}
}

func TestGraphCatalogUpdateRetainsRevisionAndDoesNotReplay(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPut || !strings.Contains(string(body), `"expected_revision":"9223372036854775807"`) {
			t.Errorf("lost graph revision: %s %s", r.Method, body)
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"code":"CatalogGenerationChanged","message":"stale revision"}`)
	}))
	defer server.Close()
	client, _ := NewAntflyClient(server.URL, server.Client())
	_, err := client.UpdateGraph(context.Background(), "g", "9223372036854775807", GraphDefinition{})
	var diagnostic *CypherExecutionError
	if !errors.As(err, &diagnostic) || diagnostic.StatusCode != 409 {
		t.Fatalf("lost conflict: %v", err)
	}
	for _, invalid := range []string{"", "0", "01", "1.0", "-1"} {
		if _, err := client.UpdateGraph(context.Background(), "g", invalid, GraphDefinition{}); err == nil {
			t.Errorf("accepted invalid revision %q", invalid)
		}
	}
	if calls != 1 {
		t.Fatalf("mutation replayed: %d calls", calls)
	}
}

func TestCypherAndCatalogMutationsDoNotFollowRedirects(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls, redirects := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Location", "/replayed")
				w.WriteHeader(status)
			}))
			defer server.Close()
			borrowed := server.Client()
			borrowed.CheckRedirect = func(*http.Request, []*http.Request) error { redirects++; return nil }
			client, err := NewAntflyClient(server.URL, borrowed)
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range []func() error{
				func() error {
					_, err := client.ExecuteGraphQuery(context.Background(), "g", GraphQueryRequest{Query: "CREATE(n:D)"})
					return err
				},
				func() error { _, err := client.CreateGraph(context.Background(), "g", GraphDefinition{}); return err },
				func() error {
					_, err := client.UpdateGraph(context.Background(), "g", "1", GraphDefinition{})
					return err
				},
				func() error { _, err := client.DropGraph(context.Background(), "g"); return err },
			} {
				before := calls
				if err := operation(); err == nil || calls != before+1 || redirects != 0 {
					t.Fatalf("redirect policy: err=%v calls=%d redirects=%d", err, calls-before, redirects)
				}
			}
			_ = borrowed.CheckRedirect(nil, nil)
			if redirects != 1 {
				t.Fatal("mutated caller's redirect policy")
			}
		})
	}
}
