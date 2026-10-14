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

"""Run real Python PostgreSQL clients against ANTFLY_PGWIRE_URL."""

import asyncio
import os

import asyncpg
import psycopg

from ddl import exercise_cached_plans, exercise_ddl, exercise_sql_cached_plans

url = os.environ["ANTFLY_PGWIRE_URL"]
with psycopg.connect(url, autocommit=True, prepare_threshold=None) as conn:
    exercise_sql_cached_plans(conn)
with psycopg.connect(url, autocommit=True) as conn:
    assert conn.execute("SELECT 1").fetchone() == (1,)
    assert conn.execute(
        "SELECT CAST(%s AS BIGINT)", (42,), prepare=True
    ).fetchone() == (42,)
    for name, value in [
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("extra_float_digits", "3"),
    ]:
        assert conn.execute("SHOW " + name).fetchone() == (value,)
    for sql in [
        "SET extra_float_digits = 3 -- comment",
        "SET DateStyle = ISO/* nested /* comment */ */, MDY",
        "SET TimeZone = UTC /* comment */; -- trailing",
    ]:
        conn.execute(sql)
        conn.execute(sql, prepare=True)
    with conn.transaction():
        assert conn.execute("SELECT 1").fetchone() == (1,)
    with psycopg.connect(url, autocommit=True) as migrator:
        exercise_cached_plans(conn, migrator)
    exercise_ddl(conn)
print("psycopg: PASS")


async def main():
    conn = await asyncpg.connect(url)
    try:
        assert await conn.fetchval("SELECT 1") == 1
        assert await conn.fetchval("SELECT CAST($1 AS BIGINT)", 42) == 42
        assert conn.get_settings().TimeZone == "UTC"
        await conn.execute(
            "CREATE TABLE cached_asyncpg (id TEXT PRIMARY KEY, n BIGINT)"
        )
        await conn.execute("INSERT INTO cached_asyncpg (id, n) VALUES ('a', 1)")
        stmt = await conn.prepare("SELECT n FROM cached_asyncpg WHERE id = $1")
        assert await stmt.fetchval("a") == 1
        await conn.execute("ALTER TABLE cached_asyncpg ADD COLUMN note TEXT")
        assert await stmt.fetchval("a") == 1
        await conn.execute("CREATE INDEX cached_asyncpg_n ON cached_asyncpg(n)")
        assert await stmt.fetchval("a") == 1
        async with conn.transaction():
            assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()
    for name, value in [
        ("DateStyle", "SQL"),
        ("TimeZone", "America/New_York"),
        ("search_path", "secret"),
    ]:
        try:
            conn = await asyncpg.connect(url, server_settings={name: value})
        except asyncpg.PostgresError as exc:
            assert exc.sqlstate == "0A000", exc
            assert name in str(exc), exc
        else:
            await conn.close()
            raise AssertionError("unsupported setting accepted: " + name)
    print("asyncpg: PASS")


asyncio.run(main())
