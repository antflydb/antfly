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
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
	"unsafe"
)

// SQLError includes the engine's SQLSTATE diagnostic.
type SQLError struct {
	TransactionID string `json:"transaction_id,omitempty"`
	Code          string `json:"code"`
	Message       string `json:"message"`
	Hint          string `json:"hint"`
	Retryable     bool   `json:"retryable"`
}

func (e *SQLError) Error() string    { return fmt.Sprintf("antfly SQL %s: %s", e.Code, e.Message) }
func (e *SQLError) SQLState() string { return e.Code }
func sqlError(body []byte, err error) error {
	if err == nil {
		return nil
	}
	var envelope struct {
		Error         *SQLError `json:"error"`
		TransactionID string    `json:"transaction_id"`
	}
	if json.Unmarshal(body, &envelope) == nil && envelope.Error != nil {
		envelope.Error.TransactionID = envelope.TransactionID
		return envelope.Error
	}
	return err
}

func (db *DB) CreateTableJSON(name string, schema []byte) error {
	handle, release, err := db.acquire()
	if err != nil {
		return err
	}
	defer release()
	defer runtime.KeepAlive(db)
	n, freeName := makeCStringSlice([]byte(name))
	defer freeName()
	s, freeSchema := makeCStringSlice(schema)
	defer freeSchema()
	return check(C.antfly_db_create_table_json((*C.antfly_db)(handle), n, s))
}
func (db *DB) DropTable(name string) error {
	handle, release, err := db.acquire()
	if err != nil {
		return err
	}
	defer release()
	defer runtime.KeepAlive(db)
	n, freeName := makeCStringSlice([]byte(name))
	defer freeName()
	return check(C.antfly_db_drop_table((*C.antfly_db)(handle), n))
}
func (db *DB) ListTablesJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_list_tables_json((*C.antfly_db)(handle), out)
	})
}

// SQLSession is a connection-local SQL transaction owner. Close rolls back
// staged writes and closes its cursors. A database may own multiple sessions.
type SQLSession struct {
	db *DB
	ID uint64
}

func (db *DB) NewSQLSession() (*SQLSession, error) {
	handle, release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(db)
	var id C.uint64_t
	if err := check(C.antfly_db_sql_session_open((*C.antfly_db)(handle), &id)); err != nil {
		return nil, err
	}
	return &SQLSession{db: db, ID: uint64(id)}, nil
}
func (s *SQLSession) Close() error {
	handle, release, err := s.db.acquire()
	if err != nil {
		return err
	}
	defer release()
	defer runtime.KeepAlive(s.db)
	return check(C.antfly_db_sql_session_close((*C.antfly_db)(handle), C.uint64_t(s.ID)))
}
func (s *SQLSession) SQLJSON(request []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(request, &fields); err != nil {
		return nil, err
	}
	fields["session_id"], _ = json.Marshal(s.ID)
	body, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	result, err := s.db.SQLJSON(body)
	return result, sqlError(result, err)
}

// SQLCursor streams native result pages without imposing a SQL row limit.
type SQLCursor struct {
	db *DB
	ID uint64
}

func (db *DB) OpenSQLCursorJSON(request []byte) (*SQLCursor, error) {
	handle, release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(db)
	input, freeInput := makeCStringSlice(request)
	defer freeInput()
	var out C.antfly_buffer
	var id C.uint64_t
	code := C.antfly_db_sql_open_cursor_json((*C.antfly_db)(handle), input, &id, &out)
	body := takeBuffer(out)
	if err := sqlError(body, check(code)); err != nil {
		return nil, err
	}
	return &SQLCursor{db: db, ID: uint64(id)}, nil
}
func (c *SQLCursor) FetchJSON(rows uint32) ([]byte, error) {
	handle, release, err := c.db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(c.db)
	var out C.antfly_buffer
	code := C.antfly_db_sql_fetch_cursor_json((*C.antfly_db)(handle), C.uint64_t(c.ID), C.uint32_t(rows), &out)
	body := takeBuffer(out)
	return body, sqlError(body, check(code))
}
func (c *SQLCursor) Close() error {
	handle, release, err := c.db.acquire()
	if err != nil {
		return err
	}
	defer release()
	defer runtime.KeepAlive(c.db)
	return check(C.antfly_db_sql_close_cursor((*C.antfly_db)(handle), C.uint64_t(c.ID)))
}

// OpenTable returns a handle scoped to a catalog table. Existing document,
// schema, index, enrichment, and search methods operate on that table.
// Close the table before DropTable. Database Close invalidates table handles.
func (db *DB) OpenTable(name string) (*DB, error) {
	handle, release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(db)
	n, freeName := makeCStringSlice([]byte(name))
	defer freeName()
	var table *C.antfly_db
	if err := check(C.antfly_db_open_table((*C.antfly_db)(handle), n, &table)); err != nil {
		return nil, err
	}
	child := newDB(unsafe.Pointer(table))
	child.owner = db
	return child, nil
}
