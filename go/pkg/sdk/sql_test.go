// Copyright 2026 Antfly, Inc.
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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExecuteSQLPreservesBoundValuesAndResultOrdinals(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/db/v1/sql" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `9223372036854775807`) {
			t.Errorf("integer parameter lost precision: %s", body)
		}
		_, _ = io.WriteString(w, `{"columns":[{"name":"id","type":"integer"},{"name":"id","type":"json"}],"rows":[["9223372036854775807",{"id":9223372036854775807}]],"rows_affected":0,"command_tag":"SELECT 1"}`)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ExecuteSQL(context.Background(), SQLRequest{Statement: "SELECT $1", Parameters: []json.RawMessage{json.RawMessage(`9223372036854775807`)}})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Rows[0][0]) != `"9223372036854775807"` || string(result.Rows[0][1]) != `{"id":9223372036854775807}` {
		t.Fatalf("lost precision or result ordering: %#v", result.Rows)
	}
}

func TestExecuteSQLRejectsWrongRowWidth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"columns":[],"rows":[[1]],"rows_affected":0,"command_tag":"SELECT 1"}`)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ExecuteSQL(context.Background(), SQLRequest{Statement: "SELECT 1"}); err == nil {
		t.Fatal("accepted malformed SQL result")
	}
}

func TestExecuteSQLKeepsReconciliationReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"code":"40003","message":"do not replay","retryable":false,"transaction_id":"0123456789abcdef0123456789abcdef"}`)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ExecuteSQL(context.Background(), SQLRequest{Statement: "DELETE FROM docs"})
	var diagnostic *SQLExecutionError
	if !errors.As(err, &diagnostic) || diagnostic.Diagnostic.TransactionId != "0123456789abcdef0123456789abcdef" || diagnostic.Diagnostic.Retryable {
		t.Fatalf("lost reconciliation receipt: %#v", err)
	}
}

func TestExecuteSQLKeepsCommittedRepairReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"columns":[],"rows":[],"rows_affected":1,"command_tag":"DELETE 1","mutation_outcome":"committed_repair_required","transaction_id":"0123456789abcdef0123456789abcdef"}`)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ExecuteSQL(context.Background(), SQLRequest{Statement: "DELETE FROM docs WHERE _id = 'a'"})
	if err != nil || result.TransactionId != "0123456789abcdef0123456789abcdef" || result.MutationOutcome != "committed_repair_required" {
		t.Fatalf("lost committed repair receipt: %#v, %v", result, err)
	}
}
