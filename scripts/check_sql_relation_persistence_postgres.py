#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Isolated PostgreSQL oracle for relation persistence lifetime contracts.

Oracle evidence alone does not close native parity cases. No existing server,
network listener, or database is used. All SQL runs on an owned local socket.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
import os
import re
import subprocess


def check():
    binary = Path(
        os.environ.get("ANTFLY_PG_BIN", "/opt/homebrew/opt/postgresql@18/bin")
    )
    with TemporaryDirectory(prefix="antfly-relation-pg-", dir="/tmp") as directory:
        root = Path(directory)
        data = root / "data"
        subprocess.run(
            [
                binary / "initdb",
                "-D",
                data,
                "--no-locale",
                "--encoding=UTF8",
                "--auth=trust",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        started = False
        try:
            subprocess.run(
                [
                    binary / "pg_ctl",
                    "-D",
                    data,
                    "-l",
                    root / "server.log",
                    "-o",
                    f"-k {root} -c listen_addresses='' -c max_prepared_transactions=8",
                    "-w",
                    "start",
                ],
                check=True,
                capture_output=True,
                timeout=30,
            )
            started = True
            sql = r"""
\set ON_ERROR_STOP on
SET statement_timeout='5s';
CREATE TABLE p(id int PRIMARY KEY);
CREATE UNLOGGED TABLE u(id int PRIMARY KEY);
CREATE TEMP TABLE t(id int PRIMARY KEY);
CREATE UNLOGGED TABLE up(id int REFERENCES p);
CREATE TEMP TABLE tt(id int REFERENCES t);
DO $$ BEGIN
  BEGIN EXECUTE 'CREATE TABLE pu(id int REFERENCES u)'; RAISE EXCEPTION 'accepted permanent FK to unlogged'; EXCEPTION WHEN invalid_table_definition THEN NULL; END;
  BEGIN EXECUTE 'CREATE TABLE pt(id int REFERENCES t)'; RAISE EXCEPTION 'accepted permanent FK to temporary'; EXCEPTION WHEN invalid_table_definition THEN NULL; END;
  BEGIN EXECUTE 'CREATE UNLOGGED TABLE ut(id int REFERENCES t)'; RAISE EXCEPTION 'accepted unlogged FK to temporary'; EXCEPTION WHEN invalid_table_definition THEN NULL; END;
  BEGIN EXECUTE 'CREATE TEMP TABLE tp(id int REFERENCES p)'; RAISE EXCEPTION 'accepted temporary FK to permanent'; EXCEPTION WHEN invalid_table_definition THEN NULL; END;
  BEGIN EXECUTE 'CREATE TEMP TABLE tu(id int REFERENCES u)'; RAISE EXCEPTION 'accepted temporary FK to unlogged'; EXCEPTION WHEN invalid_table_definition THEN NULL; END;
END $$;
CREATE TABLE shadow(id int);
INSERT INTO shadow VALUES(1);
CREATE TEMP TABLE shadow(id int);
INSERT INTO shadow VALUES(2);
DO $$ BEGIN IF (SELECT id FROM shadow) <> 2 OR (SELECT id FROM public.shadow) <> 1 THEN RAISE EXCEPTION 'temporary shadowing failed'; END IF; END $$;
BEGIN;
CREATE TEMP TABLE deleted(id int) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE dropped(id int) ON COMMIT DROP;
CREATE TEMP TABLE preserved(id int) ON COMMIT PRESERVE ROWS;
INSERT INTO deleted VALUES(1);
INSERT INTO dropped VALUES(1);
INSERT INTO preserved VALUES(1);
SAVEPOINT before_extra;
CREATE TEMP TABLE rollback_created(id int);
INSERT INTO preserved VALUES(2);
ROLLBACK TO before_extra;
DO $$ BEGIN IF to_regclass('rollback_created') IS NOT NULL OR (SELECT count(*) FROM preserved) <> 1 THEN RAISE EXCEPTION 'savepoint rollback failed'; END IF; END $$;
COMMIT;
DO $$ BEGIN IF to_regclass('dropped') IS NOT NULL OR (SELECT count(*) FROM deleted) <> 0 OR (SELECT count(*) FROM preserved) <> 1 THEN RAISE EXCEPTION 'on commit actions failed'; END IF; END $$;
BEGIN;
INSERT INTO preserved VALUES(3);
TRUNCATE preserved;
ROLLBACK;
DO $$ BEGIN IF (SELECT count(*) FROM preserved) <> 1 THEN RAISE EXCEPTION 'transactional truncate rollback failed'; END IF; END $$;
DISCARD TEMP;
DO $$ BEGIN IF to_regclass('preserved') IS NOT NULL OR (SELECT id FROM shadow) <> 1 THEN RAISE EXCEPTION 'discard lifetime failed'; END IF; END $$;
SELECT 'relation persistence oracle passed';
"""
            result = subprocess.run(
                [binary / "psql", "-X", "-h", root, "-d", "postgres", "-At"],
                input=sql,
                text=True,
                capture_output=True,
                timeout=30,
            )
            if result.returncode:
                raise RuntimeError(result.stdout + result.stderr)

            def query(text):
                answer = subprocess.run(
                    [
                        binary / "psql",
                        "-X",
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-h",
                        root,
                        "-d",
                        "postgres",
                        "-At",
                        "-c",
                        text,
                    ],
                    text=True,
                    capture_output=True,
                    timeout=15,
                )
                if answer.returncode:
                    raise RuntimeError(answer.stdout + answer.stderr)
                return answer.stdout.strip()

            # SQL ownership is tested with two live connections of the exact
            # same non-superuser role, not inferred from ordinary grants.
            query("CREATE ROLE persistence_owner LOGIN")
            holder = subprocess.Popen(
                [
                    binary / "psql",
                    "-X",
                    "-qAt",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-h",
                    root,
                    "-U",
                    "persistence_owner",
                    "-d",
                    "postgres",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            try:
                holder.stdin.write(
                    "SET statement_timeout='5s'; CREATE TEMP TABLE cross_session(id int); INSERT INTO cross_session VALUES(1); SELECT nspname FROM pg_namespace WHERE oid=pg_my_temp_schema();\n"
                )
                holder.stdin.flush()
                temporary_namespace = holder.stdout.readline().strip()
                if not re.fullmatch(r"pg_temp_[0-9]+", temporary_namespace):
                    raise AssertionError(
                        f"invalid holder namespace {temporary_namespace!r}"
                    )
                observed = {}
                for privilege_mode in ("ordinary", "explicit_namespace_usage"):
                    if privilege_mode == "explicit_namespace_usage":
                        query(
                            f"GRANT USAGE ON SCHEMA {temporary_namespace} TO persistence_owner"
                        )
                    states = {}
                    for operation, statement in {
                        "SELECT": f"SELECT * FROM {temporary_namespace}.cross_session",
                        "INSERT": f"INSERT INTO {temporary_namespace}.cross_session VALUES(2)",
                        "DROP": f"DROP TABLE {temporary_namespace}.cross_session",
                    }.items():
                        attempt = subprocess.run(
                            [
                                binary / "psql",
                                "-X",
                                "-v",
                                "ON_ERROR_STOP=1",
                                "-v",
                                "VERBOSITY=verbose",
                                "-h",
                                root,
                                "-U",
                                "persistence_owner",
                                "-d",
                                "postgres",
                                "-At",
                                "-c",
                                statement,
                            ],
                            text=True,
                            capture_output=True,
                            timeout=15,
                        )
                        match = re.search(r"ERROR:\s+([A-Z0-9]{5}):", attempt.stderr)
                        states[operation] = (
                            match.group(1)
                            if match
                            else "00000"
                            if attempt.returncode == 0
                            else attempt.stderr
                        )
                    observed[privilege_mode] = states
                print(
                    "same-role fully-qualified cross-session TEMP SQLSTATEs:", observed
                )
                if observed["ordinary"] != {
                    "SELECT": "42501",
                    "INSERT": "42501",
                    "DROP": "42501",
                }:
                    raise AssertionError(
                        f"unexpected ordinary TEMP ownership behavior: {observed}"
                    )
                if observed["explicit_namespace_usage"] != {
                    "SELECT": "0A000",
                    "INSERT": "0A000",
                    "DROP": "00000",
                }:
                    raise AssertionError(
                        f"unexpected granted TEMP ownership behavior: {observed}"
                    )
            finally:
                holder.communicate("\\q\n", timeout=15)

            two_phase = {}
            for label, before, transactional in (
                ("temp_select", "", "SELECT * FROM two_phase_temp;"),
                (
                    "temp_select_rollback_to",
                    "",
                    "SAVEPOINT before_temp; SELECT * FROM two_phase_temp; ROLLBACK TO before_temp;",
                ),
                (
                    "sql_prepare_inside",
                    "",
                    "PREPARE prepared_temp AS SELECT * FROM two_phase_temp;",
                ),
                (
                    "sql_prepare_outside_unused",
                    "PREPARE prepared_temp AS SELECT * FROM two_phase_temp;",
                    "SELECT 1;",
                ),
                (
                    "sql_prepare_outside_execute",
                    "PREPARE prepared_temp AS SELECT * FROM two_phase_temp;",
                    "EXECUTE prepared_temp;",
                ),
            ):
                gid = "oracle_" + label
                commands = f"CREATE TEMP TABLE two_phase_temp(n int);\nINSERT INTO two_phase_temp VALUES(1);\n{before}\nBEGIN;\n{transactional}\nPREPARE TRANSACTION '{gid}';\n"
                attempt = subprocess.run(
                    [
                        binary / "psql",
                        "-X",
                        "-qAt",
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-v",
                        "VERBOSITY=verbose",
                        "-h",
                        root,
                        "-d",
                        "postgres",
                    ],
                    input=commands,
                    text=True,
                    capture_output=True,
                    timeout=15,
                )
                match = re.search(r"ERROR:\s+([A-Z0-9]{5}):", attempt.stderr)
                two_phase[label] = (
                    match.group(1)
                    if match
                    else "00000"
                    if attempt.returncode == 0
                    else attempt.stderr
                )
                if (
                    query(
                        f"SELECT EXISTS(SELECT FROM pg_prepared_xacts WHERE gid='{gid}')"
                    )
                    == "t"
                ):
                    query(f"ROLLBACK PREPARED '{gid}'")
            print("TEMP two-phase SQLSTATEs:", two_phase)
            if two_phase != {
                "temp_select": "0A000",
                "temp_select_rollback_to": "0A000",
                "sql_prepare_inside": "0A000",
                "sql_prepare_outside_unused": "00000",
                "sql_prepare_outside_execute": "0A000",
            }:
                raise AssertionError(
                    f"unexpected temporary two-phase behavior: {two_phase}"
                )

            if query("SELECT to_regclass('t') IS NULL") != "t":
                raise AssertionError("TEMP survived connection close")
            query("INSERT INTO p VALUES(17); INSERT INTO u VALUES(17)")
            # Prepared UNLOGGED work survives a genuinely clean process stop;
            # its local heap image and decision identity remain independently
            # recoverable without putting source rows into a replicated log.
            query(
                "BEGIN; INSERT INTO u VALUES(18); PREPARE TRANSACTION 'oracle_unlogged_restart'"
            )
            subprocess.run(
                [binary / "pg_ctl", "-D", data, "-m", "fast", "-w", "stop"],
                check=True,
                capture_output=True,
                timeout=30,
            )
            started = False
            subprocess.run(
                [
                    binary / "pg_ctl",
                    "-D",
                    data,
                    "-l",
                    root / "server.log",
                    "-o",
                    f"-k {root} -c listen_addresses='' -c max_prepared_transactions=8",
                    "-w",
                    "start",
                ],
                check=True,
                capture_output=True,
                timeout=30,
            )
            started = True
            if (
                query("SELECT (SELECT count(*) FROM p),(SELECT count(*) FROM u)")
                != "1|1"
            ):
                raise AssertionError("clean restart did not preserve unlogged rows")
            if (
                query(
                    "SELECT count(*) FROM pg_prepared_xacts WHERE gid='oracle_unlogged_restart'"
                )
                != "1"
            ):
                raise AssertionError("clean restart lost unlogged prepared identity")
            query("COMMIT PREPARED 'oracle_unlogged_restart'")
            if query("SELECT count(*) FROM u") != "2":
                raise AssertionError(
                    "prepared unlogged effects lost after clean restart"
                )
            print(
                "UNLOGGED prepared transaction survives clean restart and commits its rows"
            )
            subprocess.run(
                [binary / "pg_ctl", "-D", data, "-m", "immediate", "-w", "stop"],
                check=True,
                capture_output=True,
                timeout=30,
            )
            started = False
            subprocess.run(
                [
                    binary / "pg_ctl",
                    "-D",
                    data,
                    "-l",
                    root / "server.log",
                    "-o",
                    f"-k {root} -c listen_addresses='' -c max_prepared_transactions=8",
                    "-w",
                    "start",
                ],
                check=True,
                capture_output=True,
                timeout=30,
            )
            started = True
            if (
                query("SELECT (SELECT count(*) FROM p),(SELECT count(*) FROM u)")
                != "1|0"
            ):
                raise AssertionError("crash recovery did not reset only unlogged rows")
            print(
                "relation persistence oracle passed: ownership, FK classes, commit/savepoint, clean/crash restart"
            )
        finally:
            if started or (data / "postmaster.pid").exists():
                subprocess.run(
                    [binary / "pg_ctl", "-D", data, "-m", "immediate", "-w", "stop"],
                    check=True,
                    capture_output=True,
                    timeout=30,
                )


if __name__ == "__main__":
    check()
