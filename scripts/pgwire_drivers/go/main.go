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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"os"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("ANTFLY_PGWIRE_URL"))
	check(err)
	defer pool.Close()
	var n int64
	check(pool.QueryRow(ctx, "SELECT 1").Scan(&n))
	if n != 1 {
		panic(n)
	}
	check(pool.QueryRow(ctx, "SELECT CAST($1 AS BIGINT)", int64(42)).Scan(&n))
	if n != 42 {
		panic(n)
	}
	for name, want := range map[string]string{"DateStyle": "ISO, MDY", "TimeZone": "UTC", "extra_float_digits": "3"} {
		var got string
		check(pool.QueryRow(ctx, "SHOW "+name).Scan(&got))
		if got != want {
			panic(got)
		}
	}
	cached, err := pool.Acquire(ctx)
	check(err)
	_, err = cached.Exec(ctx, "CREATE TABLE cached_pgx (id TEXT PRIMARY KEY, n BIGINT)")
	check(err)
	_, err = cached.Exec(ctx, "INSERT INTO cached_pgx (id, n) VALUES ('a', 1)")
	check(err)
	query := "SELECT n FROM cached_pgx WHERE id = $1"
	check(cached.QueryRow(ctx, query, "a").Scan(&n))
	for _, ddl := range []string{"ALTER TABLE cached_pgx ADD COLUMN note TEXT", "CREATE INDEX cached_pgx_n ON cached_pgx(n)"} {
		_, err = pool.Exec(ctx, ddl)
		check(err)
		check(cached.QueryRow(ctx, query, "a").Scan(&n))
		if n != 1 {
			panic(n)
		}
	}
	cached.Release()
	tx, err := pool.Begin(ctx)
	check(err)
	check(tx.QueryRow(ctx, "SELECT 1").Scan(&n))
	check(tx.Commit(ctx))
	db, err := sql.Open("pgx", os.Getenv("ANTFLY_PGWIRE_URL"))
	check(err)
	defer db.Close()
	check(db.PingContext(ctx))
	check(db.QueryRowContext(ctx, "SELECT 1").Scan(&n))
	if n != 1 {
		panic(n)
	}
	stmt, err := db.PrepareContext(ctx, "SELECT CAST($1 AS BIGINT)")
	check(err)
	defer stmt.Close()
	check(stmt.QueryRowContext(ctx, int64(42)).Scan(&n))
	if n != 42 {
		panic(n)
	}
	ddlconn, err := db.Conn(ctx)
	check(err)
	defer ddlconn.Close()
	_, err = ddlconn.ExecContext(ctx, "CREATE TABLE cached_stdlib (id TEXT PRIMARY KEY, n BIGINT)")
	check(err)
	_, err = ddlconn.ExecContext(ctx, "INSERT INTO cached_stdlib (id, n) VALUES ('a', 1)")
	check(err)
	migrationStmt, err := db.PrepareContext(ctx, "SELECT n FROM cached_stdlib WHERE id = $1")
	check(err)
	defer migrationStmt.Close()
	check(migrationStmt.QueryRowContext(ctx, "a").Scan(&n))
	for _, ddl := range []string{"ALTER TABLE cached_stdlib ADD COLUMN note TEXT", "CREATE INDEX cached_stdlib_n ON cached_stdlib(n)"} {
		_, err = ddlconn.ExecContext(ctx, ddl)
		check(err)
		check(migrationStmt.QueryRowContext(ctx, "a").Scan(&n))
		if n != 1 {
			panic(n)
		}
	}
	sqltx, err := db.BeginTx(ctx, nil)
	check(err)
	check(sqltx.QueryRowContext(ctx, "SELECT 1").Scan(&n))
	check(sqltx.Commit())
	fmt.Println("pgx pool and database/sql: PASS")
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
