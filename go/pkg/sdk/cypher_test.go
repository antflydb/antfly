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
