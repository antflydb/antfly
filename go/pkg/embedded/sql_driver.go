// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//go:build cgo

package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func init() { sql.Register("antfly", sqlDriver{}) }

type sqlDriver struct{}

func (sqlDriver) Open(dsn string) (driver.Conn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "file" || u.Host != "" || u.Path == "" || u.Opaque != "" {
		return nil, fmt.Errorf("antfly: expected file:/path/database.aflite DSN")
	}
	path, err := filepath.Abs(u.Path)
	if err != nil {
		return nil, err
	}
	if canonical, resolveErr := filepath.EvalSymlinks(path); resolveErr == nil {
		path = canonical
	} else if parent, resolveErr := filepath.EvalSymlinks(filepath.Dir(path)); resolveErr == nil {
		path = filepath.Join(parent, filepath.Base(path))
	}
	for name := range u.Query() {
		if name != "no_sync" && name != "busy_timeout_ms" {
			return nil, fmt.Errorf("antfly: unknown DSN option %q", name)
		}
	}
	noSync := u.Query().Get("no_sync") == "1"
	if value := u.Query().Get("no_sync"); value != "" && value != "0" && value != "1" {
		return nil, fmt.Errorf("antfly: no_sync must be 0 or 1")
	}
	timeout := 5 * time.Second
	if value := u.Query().Get("busy_timeout_ms"); value != "" {
		milliseconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || milliseconds > uint64((1<<63-1)/int64(time.Millisecond)) {
			return nil, fmt.Errorf("antfly: invalid busy_timeout_ms")
		}
		timeout = time.Duration(milliseconds) * time.Millisecond
	}
	options := OpenOptions{NoSync: noSync, BusyTimeout: timeout}
	db, err := OpenWithOptions(path, options)
	if errors.Is(err, NotFound) {
		db, err = CreateWithOptions(path, options)
		if err != nil {
			if _, statErr := os.Stat(path); statErr == nil {
				db, err = OpenWithOptions(path, options)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	session, err := db.NewSQLSession()
	if err != nil {
		db.Close()
		return nil, err
	}
	return &sqlConnection{db: db, session: session}, nil
}

type sqlConnection struct {
	db          *DB
	session     *SQLSession
	closed      bool
	poisoned    bool
	transaction bool
}

func (c *sqlConnection) IsValid() bool { return !c.closed && !c.poisoned }

func (c *sqlConnection) ResetSession(ctx context.Context) error {
	if !c.IsValid() {
		return driver.ErrBadConn
	}
	_, err := c.ExecContext(ctx, "ROLLBACK", nil)
	if err != nil {
		c.poisoned = true
		return driver.ErrBadConn
	}
	c.transaction = false
	return nil
}

func (c *sqlConnection) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	err := c.session.Close()
	closeErr := c.db.Close()
	if err == nil {
		err = closeErr
	}
	return err
}
func (c *sqlConnection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *sqlConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &sqlStatement{connection: c, query: query}, nil
}
func (c *sqlConnection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *sqlConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if options.Isolation != driver.IsolationLevel(sql.LevelDefault) && options.Isolation != driver.IsolationLevel(sql.LevelReadCommitted) {
		return nil, fmt.Errorf("antfly: only READ COMMITTED isolation is supported")
	}
	query := "BEGIN ISOLATION LEVEL READ COMMITTED"
	if options.ReadOnly {
		query += " READ ONLY"
	}
	if _, err := c.ExecContext(ctx, query, nil); err != nil {
		return nil, err
	}
	c.transaction = true
	return &sqlTransaction{connection: c}, nil
}
func (c *sqlConnection) CheckNamedValue(value *driver.NamedValue) error {
	if value.Name != "" {
		return fmt.Errorf("antfly: use positional $n parameters")
	}
	switch v := value.Value.(type) {
	case json.RawMessage:
		if !json.Valid(v) {
			return fmt.Errorf("antfly: invalid JSON parameter")
		}
		return nil
	case nil, int64, float64, bool, string:
		return nil
	case []byte:
		if !utf8.Valid(v) {
			return fmt.Errorf("antfly: byte parameters must contain UTF-8 text")
		}
		value.Value = string(v)
		return nil
	case time.Time:
		value.Value = v.UTC().Format(time.RFC3339Nano)
		return nil
	default:
		return driver.ErrSkip
	}
}
func (c *sqlConnection) request(query string, args []driver.NamedValue) ([]byte, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	parameters := make([]any, len(args))
	for i, arg := range args {
		if arg.Name != "" || arg.Ordinal != i+1 {
			return nil, fmt.Errorf("antfly: use positional $n parameters")
		}
		parameters[i] = arg.Value
	}
	return json.Marshal(struct {
		Statement  string `json:"statement"`
		Parameters []any  `json:"parameters"`
		SessionID  uint64 `json:"session_id"`
		Limit      int    `json:"limit,omitempty"`
	}{query, parameters, c.session.ID, 4096})
}

type sqlColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type sqlOutput struct {
	Columns  []sqlColumn         `json:"columns"`
	Rows     [][]json.RawMessage `json:"rows"`
	Nulls    [][]bool            `json:"sql_nulls"`
	Affected int64               `json:"rows_affected"`
}

func decodeSQL(body []byte) (sqlOutput, error) {
	var result sqlOutput
	err := json.Unmarshal(body, &result)
	return result, err
}
func (c *sqlConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := c.request(query, args)
	if err != nil {
		return nil, err
	}
	response, err := c.session.SQLJSON(body)
	if err != nil {
		return nil, err
	}
	result, err := decodeSQL(response)
	if err != nil {
		return nil, err
	}
	return sqlResult(result.Affected), nil
}
func (c *sqlConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := c.request(query, args)
	if err != nil {
		return nil, err
	}
	// Cursor requests do not have a result limit: each fetch is one page.
	var request map[string]json.RawMessage
	_ = json.Unmarshal(body, &request)
	delete(request, "limit")
	cursorBody, _ := json.Marshal(request)
	cursor, err := c.db.OpenSQLCursorJSON(cursorBody)
	if err != nil {
		var diagnostic *SQLError
		if !errors.As(err, &diagnostic) || diagnostic.Code != "0A000" {
			return nil, err
		}
		response, err := c.session.SQLJSON(body)
		if err != nil {
			return nil, err
		}
		output, err := decodeSQL(response)
		if err != nil {
			return nil, err
		}
		return &sqlRows{ctx: ctx, output: output, exhausted: true}, nil
	}
	rows := &sqlRows{ctx: ctx, cursor: cursor}
	if err := rows.fetch(); err != nil {
		cursor.Close()
		return nil, err
	}
	return rows, nil
}

type sqlStatement struct {
	connection *sqlConnection
	query      string
}

func (*sqlStatement) Close() error  { return nil }
func (*sqlStatement) NumInput() int { return -1 }
func namedValues(args []driver.Value) []driver.NamedValue {
	result := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		result[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return result
}
func (s *sqlStatement) Exec(args []driver.Value) (driver.Result, error) {
	return s.connection.ExecContext(context.Background(), s.query, namedValues(args))
}
func (s *sqlStatement) Query(args []driver.Value) (driver.Rows, error) {
	return s.connection.QueryContext(context.Background(), s.query, namedValues(args))
}
func (s *sqlStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.connection.ExecContext(ctx, s.query, args)
}
func (s *sqlStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.connection.QueryContext(ctx, s.query, args)
}

type sqlTransaction struct {
	connection *sqlConnection
	done       bool
}

func (t *sqlTransaction) finish(command string) error {
	if t.done {
		return sql.ErrTxDone
	}
	t.done = true
	t.connection.transaction = false
	_, err := t.connection.ExecContext(context.Background(), command, nil)
	if err != nil {
		var diagnostic *SQLError
		if errors.As(err, &diagnostic) && diagnostic.Code == "40003" {
			t.connection.poisoned = true
		} else if _, rollbackErr := t.connection.ExecContext(context.Background(), "ROLLBACK", nil); rollbackErr != nil {
			t.connection.poisoned = true
		}
	}
	return err
}
func (t *sqlTransaction) Commit() error   { return t.finish("COMMIT") }
func (t *sqlTransaction) Rollback() error { return t.finish("ROLLBACK") }

type sqlResult int64

func (r sqlResult) RowsAffected() (int64, error) { return int64(r), nil }
func (sqlResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("antfly: use INSERT RETURNING _id")
}

type sqlRows struct {
	ctx       context.Context
	cursor    *SQLCursor
	output    sqlOutput
	offset    int
	exhausted bool
	closed    bool
}

func (r *sqlRows) Columns() []string {
	names := make([]string, len(r.output.Columns))
	for i, column := range r.output.Columns {
		names[i] = column.Name
	}
	return names
}
func (r *sqlRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.cursor != nil {
		return r.cursor.Close()
	}
	return nil
}
func (r *sqlRows) ColumnTypeDatabaseTypeName(i int) string {
	return strings.ToUpper(r.output.Columns[i].Type)
}
func (r *sqlRows) fetch() error {
	body, err := r.cursor.FetchJSON(128)
	if err != nil {
		return err
	}
	var page struct {
		Result    sqlOutput `json:"result"`
		Exhausted bool      `json:"exhausted"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return err
	}
	r.output = page.Result
	r.exhausted = page.Exhausted
	r.offset = 0
	return nil
}
func (r *sqlRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		r.Close()
		return err
	}
	for r.offset == len(r.output.Rows) {
		if r.exhausted {
			r.Close()
			return io.EOF
		}
		if err := r.fetch(); err != nil {
			r.Close()
			return err
		}
	}
	index := r.offset
	row := r.output.Rows[index]
	r.offset++
	for i, raw := range row {
		null := len(r.output.Nulls) > index && len(r.output.Nulls[index]) > i && r.output.Nulls[index][i]
		if null {
			dest[i] = nil
			continue
		}
		switch r.output.Columns[i].Type {
		case "integer":
			var value string
			if len(raw) > 0 && raw[0] == '"' {
				if err := json.Unmarshal(raw, &value); err != nil {
					return err
				}
			} else {
				value = string(raw)
			}
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			dest[i] = v
		case "number":
			var value float64
			if err := json.Unmarshal(raw, &value); err != nil {
				return err
			}
			dest[i] = value
		case "boolean":
			var value bool
			if err := json.Unmarshal(raw, &value); err != nil {
				return err
			}
			dest[i] = value
		case "json":
			dest[i] = append([]byte(nil), raw...)
		default:
			if string(raw) == "null" {
				dest[i] = nil
				continue
			}
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return err
			}
			dest[i] = value
		}
	}
	return nil
}
