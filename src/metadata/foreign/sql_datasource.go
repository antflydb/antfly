// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package foreign

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Dialect abstracts database-specific SQL differences.
type Dialect interface {
	DriverName() string
	QuoteIdentifier(name string) string
	Placeholder(n int) string // 1-indexed: Placeholder(1) -> "$1" for postgres
	DiscoverColumns(ctx context.Context, db *sql.DB, table string) ([]ForeignColumn, error)
	TableStatistics(ctx context.Context, db *sql.DB, table string) (rowCount int64, sizeBytes int64, err error)
	MapError(err error) (httpStatus int, message string)
}

// SQLDataSource implements DataSource for SQL databases using database/sql.
type SQLDataSource struct {
	db      *sql.DB
	dialect Dialect

	colsMu    sync.RWMutex
	colsCache map[string][]ForeignColumn // keyed by table name
}

// NewSQLDataSource creates a new SQLDataSource with the given dialect and DSN.
func NewSQLDataSource(dialect Dialect, dsn string) (*SQLDataSource, error) {
	db, err := sql.Open(dialect.DriverName(), dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return &SQLDataSource{
		db:        db,
		dialect:   dialect,
		colsCache: make(map[string][]ForeignColumn),
	}, nil
}

// cachedColumns returns the column list for a table, caching the result of DiscoverColumns.
func (s *SQLDataSource) cachedColumns(ctx context.Context, table string) ([]ForeignColumn, error) {
	s.colsMu.RLock()
	if cols, ok := s.colsCache[table]; ok {
		s.colsMu.RUnlock()
		return cols, nil
	}
	s.colsMu.RUnlock()

	cols, err := s.dialect.DiscoverColumns(ctx, s.db, table)
	if err != nil {
		return nil, err
	}

	s.colsMu.Lock()
	if _, ok := s.colsCache[table]; !ok {
		s.colsCache[table] = cols
	}
	s.colsMu.Unlock()
	return cols, nil
}

// Query executes a query against the SQL database.
func (s *SQLDataSource) Query(ctx context.Context, params *QueryParams) (*QueryResult, error) {
	columns := params.Columns
	if len(columns) == 0 {
		var err error
		columns, err = s.cachedColumns(ctx, params.Table)
		if err != nil {
			return nil, fmt.Errorf("discovering columns: %w", err)
		}
	}

	// Translate filter_query to SQL WHERE clause
	where, args, err := TranslateFilter(params.FilterQuery, s.dialect.Placeholder, columns)
	if err != nil {
		return nil, fmt.Errorf("translating filter: %w", err)
	}

	// Build SELECT
	selectCols := "*"
	if len(params.Fields) > 0 {
		quoted := make([]string, len(params.Fields))
		for i, f := range params.Fields {
			if !isKnownColumn(f, columns) {
				return nil, fmt.Errorf("unknown field: %q", f)
			}
			quoted[i] = s.dialect.QuoteIdentifier(f)
		}
		selectCols = strings.Join(quoted, ", ")
	}

	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(selectCols)
	query.WriteString(" FROM ")
	query.WriteString(s.dialect.QuoteIdentifier(params.Table))
	if where != "" {
		query.WriteString(" WHERE ")
		query.WriteString(where)
	}

	// ORDER BY
	if len(params.OrderBy) > 0 {
		var orderParts []string
		for _, sf := range params.OrderBy {
			if !isKnownColumn(sf.Field, columns) {
				return nil, fmt.Errorf("unknown sort field: %q", sf.Field)
			}
			dir := "ASC"
			if sf.Desc != nil && *sf.Desc {
				dir = "DESC"
			}
			orderParts = append(orderParts, s.dialect.QuoteIdentifier(sf.Field)+" "+dir)
		}
		query.WriteString(" ORDER BY ")
		query.WriteString(strings.Join(orderParts, ", "))
	}

	if params.Limit > 0 {
		fmt.Fprintf(&query, " LIMIT %d", params.Limit)
	}
	if params.Offset > 0 {
		fmt.Fprintf(&query, " OFFSET %d", params.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		status, msg := s.dialect.MapError(err)
		return nil, &QueryError{Status: status, Message: msg, Err: err}
	}
	defer func() { _ = rows.Close() }()

	results, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scanning rows: %w", err)
	}

	return &QueryResult{
		Rows:  results,
		Total: len(results),
	}, nil
}

// Statistics returns estimated row count and size for the given table.
func (s *SQLDataSource) Statistics(ctx context.Context, table string) (int64, int64, error) {
	return s.dialect.TableStatistics(ctx, s.db, table)
}

// Close closes the underlying database connection pool.
func (s *SQLDataSource) Close() {
	_ = s.db.Close()
}

// DB returns the underlying *sql.DB for advanced use.
func (s *SQLDataSource) DB() *sql.DB { return s.db }

// QueryError wraps a SQL query error with an HTTP status code.
type QueryError struct {
	Status  int
	Message string
	Err     error
}

func (e *QueryError) Error() string { return e.Message }
func (e *QueryError) Unwrap() error { return e.Err }

func isKnownColumn(name string, columns []ForeignColumn) bool {
	if len(columns) == 0 {
		return true // no column validation
	}
	for _, c := range columns {
		if c.Name == name {
			return true
		}
	}
	return false
}

// scanRows converts sql.Rows into []map[string]any with type normalization.
func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colCount := len(colNames)
	values := make([]any, colCount)
	ptrs := make([]any, colCount)
	for i := range values {
		ptrs[i] = &values[i]
	}

	var results []map[string]any
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, colCount)
		for i, col := range colNames {
			row[col] = convertValue(values[i])
		}
		results = append(results, row)
		// Reset values for next row
		for i := range values {
			values[i] = nil
		}
	}
	return results, rows.Err()
}

// convertValue normalizes database driver types to JSON-friendly Go types.
func convertValue(v any) any {
	switch val := v.(type) {
	case []byte:
		// pgx returns []byte for JSON/JSONB columns. Try to unmarshal.
		var obj any
		if err := json.Unmarshal(val, &obj); err == nil {
			return obj
		}
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	case [16]byte:
		// UUID type (e.g. from pgx)
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			val[0:4], val[4:6], val[6:8], val[8:10], val[10:16])
	default:
		return v
	}
}

// simpleAggFuncs maps aggregation type names to their SQL function.
var simpleAggFuncs = map[string]string{
	"count": "COUNT",
	"sum":   "SUM",
	"avg":   "AVG",
	"min":   "MIN",
	"max":   "MAX",
}

// Aggregate executes aggregation queries against the SQL database.
// Simple scalar aggregations (count/sum/avg/min/max) are batched into a single
// SELECT to avoid per-aggregation round-trips.
func (s *SQLDataSource) Aggregate(ctx context.Context, params *AggregateParams) (*AggregateResult, error) {
	columns := params.Columns
	if len(columns) == 0 {
		var err error
		columns, err = s.cachedColumns(ctx, params.Table)
		if err != nil {
			return nil, fmt.Errorf("discovering columns: %w", err)
		}
	}

	where, args, err := TranslateFilter(params.FilterQuery, s.dialect.Placeholder, columns)
	if err != nil {
		return nil, fmt.Errorf("translating filter: %w", err)
	}

	// Validate all fields upfront and split into simple vs complex aggregations.
	type simpleAgg struct {
		name string // user-defined aggregation name
		fn   string // SQL function (COUNT, SUM, etc.)
		expr string // quoted column or "*"
	}

	var simpleAggs []simpleAgg
	results := make(map[string]any, len(params.Aggregations))

	for name, agg := range params.Aggregations {
		if agg.Field != "" && !isKnownColumn(agg.Field, columns) {
			return nil, fmt.Errorf("unknown aggregation field: %q", agg.Field)
		}

		if fn, ok := simpleAggFuncs[agg.Type]; ok {
			expr := "*"
			if agg.Field != "" {
				expr = s.dialect.QuoteIdentifier(agg.Field)
			}
			simpleAggs = append(simpleAggs, simpleAgg{name: name, fn: fn, expr: expr})
			continue
		}

		// Complex aggregation types that need their own queries.
		var aggResult any
		switch agg.Type {
		case "stats":
			aggResult, err = s.runStatsAgg(ctx, agg.Field, params.Table, where, args)
		case "terms":
			aggResult, err = s.runTermsAgg(ctx, agg.Field, params.Table, where, args, agg.Size)
		default:
			return nil, fmt.Errorf("unsupported aggregation type for foreign tables: %q", agg.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("aggregation %q failed: %w", name, err)
		}
		results[name] = aggResult
	}

	// Batch all simple aggregations into one SELECT.
	if len(simpleAggs) > 0 {
		var q strings.Builder
		q.WriteString("SELECT ")
		for i, sa := range simpleAggs {
			if i > 0 {
				q.WriteString(", ")
			}
			fmt.Fprintf(&q, "%s(%s)", sa.fn, sa.expr)
		}
		fmt.Fprintf(&q, " FROM %s", s.dialect.QuoteIdentifier(params.Table))
		if where != "" {
			q.WriteString(" WHERE ")
			q.WriteString(where)
		}

		values := make([]any, len(simpleAggs))
		ptrs := make([]any, len(simpleAggs))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := s.db.QueryRowContext(ctx, q.String(), args...).Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("batch aggregation query failed: %w", err)
		}
		for i, sa := range simpleAggs {
			results[sa.name] = convertValue(values[i])
		}
	}

	return &AggregateResult{Results: results}, nil
}

func (s *SQLDataSource) runStatsAgg(ctx context.Context, field, table, where string, args []any) (map[string]any, error) {
	col := s.dialect.QuoteIdentifier(field)
	var q strings.Builder
	fmt.Fprintf(&q, "SELECT COUNT(%s), MIN(%s), MAX(%s), AVG(%s), SUM(%s) FROM %s",
		col, col, col, col, col, s.dialect.QuoteIdentifier(table))
	if where != "" {
		q.WriteString(" WHERE ")
		q.WriteString(where)
	}

	var count any
	var min, max, avg, sum any
	err := s.db.QueryRowContext(ctx, q.String(), args...).Scan(&count, &min, &max, &avg, &sum)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"count": convertValue(count),
		"min":   convertValue(min),
		"max":   convertValue(max),
		"avg":   convertValue(avg),
		"sum":   convertValue(sum),
	}, nil
}

// maxTermsBuckets caps the number of terms buckets to prevent excessive memory usage.
const maxTermsBuckets = 1000

func (s *SQLDataSource) runTermsAgg(ctx context.Context, field, table, where string, args []any, size int) ([]map[string]any, error) {
	if size <= 0 {
		size = 10
	}
	if size > maxTermsBuckets {
		size = maxTermsBuckets
	}
	col := s.dialect.QuoteIdentifier(field)
	var q strings.Builder
	fmt.Fprintf(&q, "SELECT %s, COUNT(*) AS doc_count FROM %s", col, s.dialect.QuoteIdentifier(table))
	if where != "" {
		q.WriteString(" WHERE ")
		q.WriteString(where)
	}
	fmt.Fprintf(&q, " GROUP BY %s ORDER BY doc_count DESC LIMIT %d", col, size)

	rows, err := s.db.QueryContext(ctx, q.String(), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var buckets []map[string]any
	for rows.Next() {
		var key any
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		buckets = append(buckets, map[string]any{
			"key":       convertValue(key),
			"doc_count": count,
		})
	}
	return buckets, rows.Err()
}
