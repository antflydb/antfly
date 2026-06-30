package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	if len(os.Args) != 3 {
		exitf("usage: pgx_smoke <host> <port>")
	}
	host := os.Args[1]
	port := os.Args[2]

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%s user=antfly sslmode=disable", host, port))
	if err != nil {
		exitf("parse config: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		exitf("connect: %v", err)
	}
	defer conn.Close(context.Background())

	table := fmt.Sprintf("pgx_default_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id text PRIMARY KEY, status text);", table)); err != nil {
		exitf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, status) VALUES ($1, $2);", table), "row:pgx", "ready"); err != nil {
		exitf("insert: %v", err)
	}

	var status string
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT status FROM %s WHERE id = $1;", table), "row:pgx").Scan(&status); err != nil {
		exitf("select: %v", err)
	}
	if status != "ready" {
		exitf("unexpected selected status: %q", status)
	}

	var version string
	if err := conn.QueryRow(ctx, "SELECT version();").Scan(&version); err != nil {
		exitf("select version: %v", err)
	}
	if version == "" {
		exitf("empty version")
	}

	var serverVersion string
	if err := conn.QueryRow(ctx, "SHOW server_version;").Scan(&serverVersion); err != nil {
		exitf("show server_version: %v", err)
	}
	if serverVersion != "16.0-antfly" {
		exitf("unexpected server_version: %q", serverVersion)
	}

	var searchPath string
	if err := conn.QueryRow(ctx, "SHOW search_path;").Scan(&searchPath); err != nil {
		exitf("show search_path: %v", err)
	}
	if searchPath != "public" {
		exitf("unexpected search_path: %q", searchPath)
	}

	if _, err := conn.Exec(ctx, "BEGIN;"); err != nil {
		exitf("begin: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT id FROM missing_pgwire_table;"); err == nil {
		exitf("missing table query unexpectedly succeeded")
	} else {
		expectPgErrorCode("missing table", err, "42P01")
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s_blocked (id text PRIMARY KEY);", table)); err == nil {
		exitf("aborted transaction statement unexpectedly succeeded")
	} else {
		expectPgErrorCode("aborted transaction", err, "25P02")
	}
	if _, err := conn.Exec(ctx, "ROLLBACK;"); err != nil {
		exitf("rollback: %v", err)
	}

	fmt.Println("pgx default extended query smoke passed")
}

func expectPgErrorCode(label string, err error, code string) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		exitf("%s returned non-pg error: %T %v", label, err, err)
	}
	if pgErr.Code != code {
		exitf("%s returned SQLSTATE %s, want %s: %s", label, pgErr.Code, code, pgErr.Message)
	}
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
