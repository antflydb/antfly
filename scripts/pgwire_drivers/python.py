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

url = os.environ["ANTFLY_PGWIRE_URL"]
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
print("psycopg: PASS")


async def main():
    conn = await asyncpg.connect(url)
    try:
        assert await conn.fetchval("SELECT 1") == 1
        assert await conn.fetchval("SELECT CAST($1 AS BIGINT)", 42) == 42
        assert conn.get_settings().TimeZone == "UTC"
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
