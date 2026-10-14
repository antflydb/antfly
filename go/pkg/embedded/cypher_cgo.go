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

package embedded

/*
#include "antfly.h"
#if defined(__APPLE__)
#define ANTFLY_GRAPH_OPTIONAL __attribute__((weak_import))
#else
#define ANTFLY_GRAPH_OPTIONAL __attribute__((weak))
#endif
extern antfly_error_code antfly_db_graph_catalog_json(antfly_db *, antfly_slice, antfly_buffer *) ANTFLY_GRAPH_OPTIONAL;
extern antfly_error_code antfly_db_cypher_json(antfly_db *, antfly_slice, antfly_slice, antfly_buffer *) ANTFLY_GRAPH_OPTIONAL;
extern antfly_error_code antfly_db_graph_write_status_json(antfly_db *, antfly_slice, antfly_buffer *) ANTFLY_GRAPH_OPTIONAL;
static antfly_error_code antfly_go_graph_catalog_json(antfly_db *db, antfly_slice request, antfly_buffer *out) {
    return antfly_db_graph_catalog_json ? antfly_db_graph_catalog_json(db, request, out) : ANTFLY_UNSUPPORTED;
}
static antfly_error_code antfly_go_cypher_json(antfly_db *db, antfly_slice graph, antfly_slice request, antfly_buffer *out) {
    return antfly_db_cypher_json ? antfly_db_cypher_json(db, graph, request, out) : ANTFLY_UNSUPPORTED;
}
static antfly_error_code antfly_go_graph_write_status_json(antfly_db *db, antfly_slice id, antfly_buffer *out) {
    return antfly_db_graph_write_status_json ? antfly_db_graph_write_status_json(db, id, out) : ANTFLY_UNSUPPORTED;
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
)

// GraphError retains the native Cypher diagnostic and write delivery outcome.
type GraphError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Span    *struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"span,omitempty"`
	WriteOutcome  string `json:"write_outcome,omitempty"`
	TransactionID string `json:"transaction_id,omitempty"`
	Cause         error  `json:"-"`
}

func (e *GraphError) Error() string { return fmt.Sprintf("antfly graph %s: %s", e.Code, e.Message) }
func (e *GraphError) Unwrap() error { return e.Cause }
func graphError(body []byte, err error) error {
	if err == nil {
		return nil
	}
	var diagnostic GraphError
	if json.Unmarshal(body, &diagnostic) == nil && diagnostic.Code != "" {
		diagnostic.Cause = err
		return &diagnostic
	}
	return err
}

// GraphCatalogJSON gets/lists/puts/drops graphs. Updates require expected_revision.
func (db *DB) GraphCatalogJSON(request []byte) ([]byte, error) {
	handle, release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(db)
	input, freeInput := makeCStringSlice(request)
	defer freeInput()
	var out C.antfly_buffer
	code := C.antfly_go_graph_catalog_json((*C.antfly_db)(handle), input, &out)
	body := takeBuffer(out)
	return body, graphError(body, check(code))
}

// CypherJSON uses the hosted query contract and lossless tagged result values.
func (db *DB) CypherJSON(graph string, request []byte) ([]byte, error) {
	handle, release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(db)
	name, freeName := makeCStringSlice([]byte(graph))
	defer freeName()
	input, freeInput := makeCStringSlice(request)
	defer freeInput()
	var out C.antfly_buffer
	code := C.antfly_go_cypher_json((*C.antfly_db)(handle), name, input, &out)
	body := takeBuffer(out)
	return body, graphError(body, check(code))
}

// GraphWriteStatusJSON resolves a receipt without replaying the graph statement.
func (db *DB) GraphWriteStatusJSON(transactionID string) ([]byte, error) {
	handle, release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(db)
	id, freeID := makeCStringSlice([]byte(transactionID))
	defer freeID()
	var out C.antfly_buffer
	code := C.antfly_go_graph_write_status_json((*C.antfly_db)(handle), id, &out)
	body := takeBuffer(out)
	return body, graphError(body, check(code))
}
