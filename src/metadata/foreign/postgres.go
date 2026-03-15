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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // register pgx as database/sql driver
)

func init() {
	RegisterFactory("postgres", func(dsn string) (DataSource, error) {
		return NewSQLDataSource(&PostgresDialect{}, dsn)
	})
}

// PostgresDialect implements Dialect for PostgreSQL via pgx.
type PostgresDialect struct{}

func (d *PostgresDialect) DriverName() string { return "pgx" }

func (d *PostgresDialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (d *PostgresDialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

func (d *PostgresDialect) DiscoverColumns(ctx context.Context, db *sql.DB, table string) ([]ForeignColumn, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, fmt.Errorf("querying information_schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var cols []ForeignColumn
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return nil, err
		}
		cols = append(cols, ForeignColumn{
			Name:     name,
			Type:     dataType,
			Nullable: nullable == "YES",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %q not found or has no columns", table)
	}
	return cols, nil
}

func (d *PostgresDialect) TableStatistics(ctx context.Context, db *sql.DB, table string) (int64, int64, error) {
	var rowCount int64
	var sizeBytes int64

	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(n_live_tup, 0),
		       COALESCE(pg_total_relation_size(relid), 0)
		FROM pg_stat_user_tables
		WHERE relname = $1`, table).Scan(&rowCount, &sizeBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("querying pg_stat_user_tables: %w", err)
	}
	return rowCount, sizeBytes, nil
}

func (d *PostgresDialect) MapError(err error) (int, string) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42P01": // undefined_table
			return http.StatusNotFound, fmt.Sprintf("postgres table not found: %s", pgErr.Message)
		case "42703": // undefined_column
			return http.StatusBadRequest, fmt.Sprintf("unknown column: %s", pgErr.Message)
		case "28P01", "28000": // invalid_password, invalid_authorization_specification
			return http.StatusInternalServerError, "database authentication failed"
		case "08001", "08006": // sqlclient_unable_to_establish_sqlconnection, connection_failure
			return http.StatusServiceUnavailable, "database connection failed"
		case "42601": // syntax_error
			return http.StatusBadRequest, fmt.Sprintf("SQL syntax error: %s", pgErr.Message)
		}
	}
	return http.StatusInternalServerError, fmt.Sprintf("database error: %v", err)
}
