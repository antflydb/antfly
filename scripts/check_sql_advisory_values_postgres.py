#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PostgreSQL 18+ oracle for advisory scalar typing and void wire contracts.

Complements the lock lifetime/wait campaigns. Owns a disposable server and never
changes Antfly's original-case inventory or infers native/public activation.
"""

from generate_sql_postgres_reference import postgres


def check():
    import psycopg
    from psycopg import pq

    assertions = 0

    def equal(actual, expected):
        nonlocal assertions
        if actual != expected:
            raise AssertionError((actual, expected))
        assertions += 1

    def value(connection, sql, expected, oid):
        with connection.cursor() as cursor:
            cursor.execute(sql)
            equal(cursor.description[0].type_code, oid)
            equal(cursor.fetchone()[0], expected)

    def rejects(connection, sql, code):
        try:
            connection.execute(sql).fetchall()
        except psycopg.Error as error:
            equal(error.sqlstate, code)
        else:
            raise AssertionError((sql, "unexpected success"))

    with postgres() as connection:
        connection.execute("SET statement_timeout = '2s'")
        functions = (
            "pg_advisory_lock",
            "pg_advisory_lock_shared",
            "pg_try_advisory_lock",
            "pg_try_advisory_lock_shared",
            "pg_advisory_xact_lock",
            "pg_advisory_xact_lock_shared",
            "pg_try_advisory_xact_lock",
            "pg_try_advisory_xact_lock_shared",
            "pg_advisory_unlock",
            "pg_advisory_unlock_shared",
        )
        for function in functions:
            boolean = "try_" in function or "unlock" in function
            for arguments in ("-42", "-42, 7"):
                connection.execute("SELECT pg_advisory_unlock_all()")
                if "unlock" in function:
                    lock = (
                        "pg_advisory_lock_shared"
                        if function.endswith("_shared")
                        else "pg_advisory_lock"
                    )
                    connection.execute(f"SELECT {lock}({arguments})")
                value(
                    connection,
                    f"SELECT {function}({arguments})",
                    True if boolean else "",
                    16 if boolean else 2278,
                )
            for arguments in ("NULL::bigint", "NULL::integer, 7", "7, NULL::integer"):
                value(
                    connection,
                    f"SELECT {function}({arguments})",
                    None,
                    16 if boolean else 2278,
                )
        connection.execute("SELECT pg_advisory_unlock_all()")
        value(connection, "SELECT pg_advisory_unlock_all()", "", 2278)
        for sql, expected in (
            ("SELECT 'ignored'::void", ""),
            ("SELECT NULL::void", None),
            ("SELECT coalesce(NULL::void, 'ignored'::void)", ""),
            (
                "SELECT CASE WHEN false THEN pg_advisory_lock(881) ELSE NULL::void END",
                None,
            ),
        ):
            value(connection, sql, expected, 2278)
        value(
            connection,
            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()",
            0,
            20,
        )
        value(connection, "SELECT 'ignored'::void IS NULL", False, 16)
        value(connection, "SELECT NULL::void IS NULL", True, 16)
        value(connection, "SELECT 'ignored'::void::text", "", 25)
        value(connection, "SELECT 'ignored'::void || 'suffix'", "suffix", 25)
        value(connection, "SELECT concat('ignored'::void, 'suffix')", "suffix", 25)
        value(connection, "SELECT count('ignored'::void)", 1, 20)
        for sql, code in (
            ("SELECT 'x'::void = 'y'::void", "42883"),
            ("SELECT 'x'::void < 'y'::void", "42883"),
            ("SELECT 'x'::void || 'y'::void", "42883"),
            ("SELECT 'x'::void ORDER BY 1", "42883"),
            ("SELECT 'x'::void GROUP BY 1", "42883"),
            ("SELECT count(DISTINCT 'x'::void)", "42883"),
            ("SELECT min('x'::void)", "42883"),
            ("SELECT nullif('x'::void, NULL)", "42883"),
            ("SELECT greatest('x'::void)", "42883"),
            ("SELECT least('x'::void, 'y'::void)", "42883"),
            ("SELECT 'x'::void IN (NULL)", "42883"),
            ("SELECT row_number() OVER (PARTITION BY 'x'::void)", "42883"),
            ("SELECT row_number() OVER (ORDER BY 'x'::void)", "42883"),
            ("SELECT min('x'::void) OVER ()", "42883"),
            ("SELECT 'x'::void UNION SELECT 'x'::void", "42883"),
            ("SELECT ARRAY['x'::void]", "42704"),
            ("SELECT array_agg('x'::void)", "42704"),
            ("CREATE TABLE invalid_void(v void)", "42P16"),
            ("SELECT pg_advisory_lock(1::bigint, 2::bigint)", "42883"),
            ("SELECT pg_advisory_lock(1.5)", "42883"),
        ):
            rejects(connection, sql, code)
        for sql, count in (
            ("SELECT pg_advisory_lock(1) AS v UNION ALL SELECT pg_advisory_lock(2)", 2),
            (
                "WITH r AS MATERIALIZED (SELECT pg_advisory_lock(1) AS v) SELECT v FROM r",
                1,
            ),
            ("SELECT first_value(pg_advisory_lock(1)) OVER () AS v", 1),
            ("SELECT 'x'::void AS v FROM (VALUES (2), (1)) AS r(id) ORDER BY id", 2),
            (
                "WITH RECURSIVE r(n,v) AS (SELECT 1, 'x'::void UNION ALL SELECT n+1,v FROM r WHERE n<3) SELECT v FROM r ORDER BY n",
                3,
            ),
        ):
            with connection.cursor() as cursor:
                cursor.execute(sql)
                equal(cursor.description[0].type_code, 2278)
                equal(cursor.fetchall(), [("",)] * count)
        connection.execute("SELECT pg_advisory_unlock_all()")
        connection.execute("EXPLAIN SELECT pg_advisory_lock(882)").fetchall()
        value(
            connection,
            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()",
            0,
            20,
        )
        for format_, raw, expected in (
            (0, b"ignored", b""),
            (0, b"", b""),
            (1, b"", b""),
            (0, None, None),
            (1, None, None),
        ):
            result = connection.pgconn.exec_params(
                b"SELECT $1",
                [raw],
                param_types=[2278],
                param_formats=[format_],
                result_format=1,
            )
            equal(result.status, pq.ExecStatus.TUPLES_OK)
            equal(result.ftype(0), 2278)
            equal(result.get_value(0, 0), expected)
        for format_, raw, code in (
            (1, b"x", b"22P03"),
            (1, b"\xff", b"22P03"),
            (0, b"\xff", b"22021"),
        ):
            result = connection.pgconn.exec_params(
                b"SELECT $1", [raw], param_types=[2278], param_formats=[format_]
            )
            equal(result.status, pq.ExecStatus.FATAL_ERROR)
            equal(result.error_field(pq.DiagnosticField.SQLSTATE), code)
    print(f"PostgreSQL advisory scalar/void oracle: {assertions} assertions passed")


if __name__ == "__main__":
    check()
