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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPreparedSQLParameterContractsRetainElementIdentity(t *testing.T) {
	var response SQLPreparedResponse
	if err := json.Unmarshal([]byte(`{"prepared_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expires_at_ms":123,"owner_node_id":"9007199254740993","parameter_types":["array","integer"],"parameter_descriptors":[{"type":"array","element_type":"int64","nullable":true},{"type":"integer","element_type":"int32","nullable":true}],"columns":[]}`), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.ParameterDescriptors) != 2 {
		t.Fatalf("unexpected parameter count: %d", len(response.ParameterDescriptors))
	}
	var array SQLParameterDescriptor = response.ParameterDescriptors[0]
	if array.Type != "array" || array.ElementType != "int64" || !array.Nullable {
		t.Fatalf("lost array contract: %+v", array)
	}
	if response.ParameterDescriptors[1].ElementType != "int32" {
		t.Fatal("lost primitive width")
	}
}

func TestArrayColumnSchemaPreservesExplicitIdentityAndEnvelopeConstraints(t *testing.T) {
	for _, kind := range []SQLArrayElementType{"int64", "numeric"} {
		column := SQLArrayColumnSchema{Type: "sql_array", XAntflySqlType: kind, Nullable: true}
		column.Set("properties", map[string]any{"values": map[string]any{"minItems": 2}})
		encoded, err := json.Marshal(column)
		if err != nil {
			t.Fatal(err)
		}
		var decoded SQLArrayColumnSchema
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Type != "sql_array" || decoded.XAntflySqlType != kind || !decoded.Nullable {
			t.Fatalf("lost array column identity: %+v", decoded)
		}
		if _, found := decoded.Get("properties"); !found {
			t.Fatal("lost envelope constraints")
		}
	}
}

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

func TestExecuteSQLNumericPreservesPrecisionScaleNullsAndSpecials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"columns":[{"name":"n","type":"number","element_type":"numeric"}],"rows":[["9007199254740993.1200"],["0.0000"],[null],["NaN"],["Infinity"],["-Infinity"]],"rows_affected":0,"command_tag":"SELECT 6"}`)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ExecuteSQL(context.Background(), SQLRequest{Statement: "SELECT n FROM amounts"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Columns) != 1 || result.Columns[0].ElementType != "numeric" || len(result.Rows) != 6 {
		t.Fatalf("lost exact numeric contract: %#v", result)
	}
	for i, expected := range []string{`"9007199254740993.1200"`, `"0.0000"`, `null`, `"NaN"`, `"Infinity"`, `"-Infinity"`} {
		if len(result.Rows[i]) != 1 || string(result.Rows[i][0]) != expected {
			t.Fatalf("row %d lost numeric value: %#v", i, result.Rows[i])
		}
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

func TestPreparedSQLLifecycleAndTransportPolicy(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	calls := 0
	status := http.StatusOK
	sawConnectionHeader := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if status != http.StatusOK {
			w.Header().Set("Location", "/replayed")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"code":"40003","message":"do not replay","retryable":false,"transaction_id":"`+id+`"}`)
			return
		}
		switch r.URL.Path {
		case "/db/v1/sql/prepared":
			_, _ = io.WriteString(w, `{"prepared_id":"`+id+`","owner_node_id":"9007199254740993","expires_at_ms":123,"columns":[],"parameter_types":[]}`)
		case "/db/v1/sql/prepared/" + id + "/execute":
			_, _ = io.WriteString(w, `{"columns":[{"name":"v","type":"integer"}],"rows":[["9007199254740993"]],"rows_affected":0,"command_tag":"SELECT 1"}`)
		case "/db/v1/sql/prepared/" + id:
			if r.Method != http.MethodDelete {
				t.Errorf("close method: %s", r.Method)
			}
			if r.Header.Get("X-Antfly-SQL-Connection-Id") == id {
				sawConnectionHeader = true
			}
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := client.PrepareSQL(context.Background(), SQLPrepareRequest{Statement: "SELECT $1::BIGINT"})
	if err != nil || prepared.OwnerNodeId != "9007199254740993" {
		t.Fatalf("prepare: %#v %v", prepared, err)
	}
	result, err := client.ExecutePreparedSQL(context.Background(), id, SQLPreparedExecutionRequest{Parameters: []json.RawMessage{json.RawMessage(`9007199254740993`)}})
	if err != nil || string(result.Rows[0][0]) != `"9007199254740993"` {
		t.Fatalf("execute: %#v %v", result, err)
	}
	if err := client.ClosePreparedSQL(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := client.ClosePreparedSQL(context.Background(), id, id); err != nil {
		t.Fatal(err)
	}
	if !sawConnectionHeader {
		t.Fatal("connection-bound close did not forward the connection ID")
	}
	operations := []func() error{
		func() error {
			_, err := client.PrepareSQL(context.Background(), SQLPrepareRequest{Statement: "SELECT 1"})
			return err
		},
		func() error {
			_, err := client.ExecutePreparedSQL(context.Background(), id, SQLPreparedExecutionRequest{})
			return err
		},
		func() error { return client.ClosePreparedSQL(context.Background(), id) },
	}
	for _, code := range []int{307, 503} {
		status = code
		for _, operation := range operations {
			before := calls
			err := operation()
			var diagnostic *SQLExecutionError
			if !errors.As(err, &diagnostic) || calls != before+1 {
				t.Fatalf("SQL policy: %v, calls %d", err, calls-before)
			}
		}
	}
}
