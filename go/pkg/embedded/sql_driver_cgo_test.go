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

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestSQLConformance(t *testing.T) {
	raw, err := os.ReadFile("../../../zig/pkg/antfly-embedded/capi-conformance/sql/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Statement  string            `json:"statement"`
		Parameters []json.RawMessage `json:"parameters"`
		Rows       [][]any           `json:"rows"`
		State      string            `json:"sqlstate"`
	}
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sql.aflite")
	seedSQLSearchFixture(t, path)
	db, err := sql.Open("antfly", "file:"+path+"?no_sync=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, c := range cases {
		args := make([]any, len(c.Parameters))
		for i, p := range c.Parameters {
			d := json.NewDecoder(bytes.NewReader(p))
			d.UseNumber()
			var v any
			if err = d.Decode(&v); err != nil {
				t.Fatal(err)
			}
			switch value := v.(type) {
			case json.Number:
				n, e := value.Int64()
				if e != nil {
					f, floatErr := value.Float64()
					if floatErr != nil {
						t.Fatal(floatErr)
					}
					args[i] = f
				} else {
					args[i] = n
				}
			case map[string]any, []any:
				args[i] = p
			default:
				args[i] = v
			}
		}
		if c.State != "" {
			_, err = db.Query(c.Statement, args...)
			var e *SQLError
			if !errors.As(err, &e) || e.Code != c.State {
				t.Fatalf("state %s: %v", c.State, err)
			}
			continue
		}
		if c.Rows == nil {
			if _, err = db.Exec(c.Statement, args...); err != nil {
				t.Fatalf("%s: %v", c.Statement, err)
			}
			continue
		}
		rows, e := db.Query(c.Statement, args...)
		if e != nil {
			t.Fatal(e)
		}
		cols, _ := rows.Columns()
		var actual [][]any
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if e = rows.Scan(ptrs...); e != nil {
				t.Fatal(e)
			}
			for i, v := range values {
				switch value := v.(type) {
				case int64:
					values[i] = strconv.FormatInt(value, 10)
				case []byte:
					if e = json.Unmarshal(value, &values[i]); e != nil {
						t.Fatal(e)
					}
				}
			}
			actual = append(actual, values)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !reflect.DeepEqual(actual, c.Rows) {
			t.Fatalf("got %#v want %#v", actual, c.Rows)
		}
	}
}

func seedSQLSearchFixture(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile("../../../zig/pkg/antfly-embedded/capi-conformance/sql/search-fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Table   string
		Schema  json.RawMessage
		History json.RawMessage
		Indexes []json.RawMessage
		Batch   json.RawMessage
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	db, err := CreateWithOptions(path, OpenOptions{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTableJSON(fixture.Table, fixture.Schema); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTableJSON("history_items", fixture.History); err != nil {
		t.Fatal(err)
	}
	table, err := db.OpenTable(fixture.Table)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	for _, index := range fixture.Indexes {
		if err := table.AddIndexJSON(index); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.BatchJSON(fixture.Batch); err != nil {
		t.Fatal(err)
	}
	if err := table.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLSessionsAndStreaming(t *testing.T) {
	db, err := sql.Open("antfly", "file:"+filepath.Join(t.TempDir(), "sessions.aflite")+"?no_sync=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	if _, err = db.Exec("CREATE TABLE numbers (n BIGINT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if _, err = tx.Exec("INSERT INTO numbers (_id,n) VALUES ($1,$2)", fmt.Sprintf("row:%04d", i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.Query("SELECT n FROM numbers")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("uncommitted rows visible")
	}
	rows.Close()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err = db.Query("SELECT n FROM numbers ORDER BY _id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var v int64
		if err = rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		if v != int64(n) {
			t.Fatalf("row %d: %d", n, v)
		}
		n++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 300 {
		t.Fatalf("streamed %d rows", n)
	}
}

func TestSQLPoolRecoversAfterFailedCommit(t *testing.T) {
	db, err := sql.Open("antfly", "file:"+filepath.Join(t.TempDir(), "failed.aflite")+"?no_sync=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("CREATE TABLE numbers (n BIGINT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("SELECT n FROM missing_table"); err == nil {
		t.Fatal("missing table succeeded")
	}
	if err = tx.Commit(); err == nil {
		t.Fatal("aborted transaction committed")
	}
	if _, err = db.Exec("INSERT INTO numbers (n) VALUES (1)"); err != nil {
		t.Fatalf("pooled connection retained failed transaction: %v", err)
	}
}

func TestSQLSyntaxErrorAbortsTransaction(t *testing.T) {
	db, err := sql.Open("antfly", "file:"+filepath.Join(t.TempDir(), "syntax.aflite")+"?no_sync=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("CREATE TABLE numbers (n BIGINT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(context.Background(), "INSERT INTO numbers (_id,n) VALUES ('discarded',1)"); err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(context.Background(), "INSERT INTO")
	var diagnostic *SQLError
	if !errors.As(err, &diagnostic) || diagnostic.Code != "42601" {
		t.Fatalf("syntax error: %v", err)
	}
	err = tx.Commit()
	if !errors.As(err, &diagnostic) || diagnostic.Code != "25P02" {
		t.Fatalf("aborted commit: %v", err)
	}
	rows, err := db.Query("SELECT n FROM numbers")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("syntax-error transaction published its insert")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLPoolUsesIndependentNativeConnections(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pool.aflite")
	db, err := sql.Open("antfly", "file:"+path+"?no_sync=1&busy_timeout_ms=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	first, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.Raw(func(raw any) error {
		return second.Raw(func(other any) error {
			if raw.(*sqlConnection).db == other.(*sqlConnection).db {
				t.Fatal("pooled connections share a native handle")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ExecContext(ctx, "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := first.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO items (id,name) VALUES (1, 'pending')"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := second.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("uncommitted rows visible: %d", count)
	}
	if _, err := second.ExecContext(ctx, "INSERT INTO items (id,name) VALUES (2, 'other')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := second.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("committed rows = %d, want 2", count)
	}
}

func TestSQLLargePreparedValuesAndTransaction(t *testing.T) {
	db, err := sql.Open("antfly", "file:"+filepath.Join(t.TempDir(), "large.aflite")+"?no_sync=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE entries (id TEXT, body TEXT)"); err != nil {
		t.Fatal(err)
	}
	payload := string(bytes.Repeat([]byte("é"), 1_100_000))
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO entries (_id,id,body) VALUES ($1,$1,$2)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, id := range []string{"a", "b"} {
		if _, err = stmt.Exec(id, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var actual string
	if err = db.QueryRow("SELECT body FROM entries WHERE id=$1", "a").Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != payload {
		t.Fatalf("large prepared value changed: got %d bytes, want %d", len(actual), len(payload))
	}
}
