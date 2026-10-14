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

//go:build cgo && libantfly

package embedded

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func TestCypherAtomicWritesAndReceipts(t *testing.T) {
	db, err := CreateHosted(filepath.Join(t.TempDir(), "graph.aflite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.SQLJSON([]byte(`{"statement":"CREATE TABLE docs (id TEXT)"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GraphCatalogJSON([]byte(`{"action":"put","name":"knowledge","definition":{"labels":[{"name":"Document","table":"docs"}]}}`)); err != nil {
		t.Fatal(err)
	}
	id := "aabbccddeeff00112233445566778899"
	request := []byte(`{"query":"CREATE (n:Document {id:'created'}) RETURN n.id","write_request_id":"` + id + `"}`)
	if _, err := db.CypherJSON("knowledge", request); err != nil {
		t.Fatal(err)
	}
	_, err = db.CypherJSON("knowledge", request)
	var diagnostic *GraphError
	if !errors.As(err, &diagnostic) || diagnostic.Code != "CypherWriteRequestAlreadyUsed" || diagnostic.TransactionID != id {
		t.Fatalf("duplicate diagnostic: %v", err)
	}
	if _, err := db.CypherJSON("knowledge", []byte(`{"query":"CREATE (n:Document {id:'aborted'}) RETURN 1/0"}`)); !errors.As(err, &diagnostic) || diagnostic.Code != "CypherDivisionByZero" {
		t.Fatalf("rollback diagnostic: %v", err)
	}
	body, err := db.CypherJSON("knowledge", []byte(`{"query":"MATCH (n:Document {id:'aborted'}) RETURN n"}`))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Rows []json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(body, &result); err != nil || len(result.Rows) != 0 {
		t.Fatalf("rollback rows: %s (%v)", body, err)
	}
	body, err = db.GraphWriteStatusJSON(id)
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		Disposition string `json:"disposition"`
		Durable     bool   `json:"durable"`
	}
	if err := json.Unmarshal(body, &status); err != nil || status.Disposition != "committed" || !status.Durable {
		t.Fatalf("receipt: %s (%v)", body, err)
	}
}
