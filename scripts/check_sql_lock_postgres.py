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

"""Independent PostgreSQL 18+ lock conflict and lifetime oracle.

Owns an isolated temporary server using the existing oracle context. No user
server or persistent database is touched. Native/public case dispositions are
not inferred from this independent semantic check.
"""

from generate_sql_postgres_reference import postgres

TABLE_MODES = (
    "ACCESS SHARE",
    "ROW SHARE",
    "ROW EXCLUSIVE",
    "SHARE UPDATE EXCLUSIVE",
    "SHARE",
    "SHARE ROW EXCLUSIVE",
    "EXCLUSIVE",
    "ACCESS EXCLUSIVE",
)
TABLE_CONFLICTS = (0x80, 0xC0, 0xF0, 0xF8, 0xEC, 0xFC, 0xFE, 0xFF)
ROW_MODES = ("KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE")
ROW_CONFLICTS = (0x08, 0x0C, 0x0E, 0x0F)


def check():
    import psycopg

    assertions = 0

    def equal(actual, expected):
        nonlocal assertions
        if actual != expected:
            raise AssertionError((actual, expected))
        assertions += 1

    def blocked(connection, statement):
        try:
            with connection.transaction():
                connection.execute("SET LOCAL lock_timeout = '15ms'")
                connection.execute(statement).fetchall() if statement.startswith(
                    "SELECT"
                ) else connection.execute(statement)
        except psycopg.Error as error:
            if error.sqlstate != "55P03":
                raise
            return True
        return False

    def scalar(connection, statement):
        return connection.execute(statement).fetchone()[0]

    with postgres(max_prepared_transactions=4) as holder:
        with psycopg.connect(holder.info.dsn, autocommit=True) as contender:
            contender.execute("SET statement_timeout = '2s'")
            holder.execute(
                "CREATE TABLE lock_probe(id bigint PRIMARY KEY, k text UNIQUE, v text)"
            )
            holder.execute("INSERT INTO lock_probe VALUES(1,'one','initial')")
            for left, held in enumerate(TABLE_MODES):
                with holder.transaction():
                    holder.execute(f"LOCK TABLE lock_probe IN {held} MODE")
                    for right, requested in enumerate(TABLE_MODES):
                        equal(
                            blocked(
                                contender,
                                f"LOCK TABLE lock_probe IN {requested} MODE NOWAIT",
                            ),
                            bool(TABLE_CONFLICTS[left] & (1 << right)),
                        )
            for left, held in enumerate(ROW_MODES):
                with holder.transaction():
                    holder.execute(
                        f"SELECT id FROM lock_probe WHERE id=1 FOR {held}"
                    ).fetchall()
                    for right, requested in enumerate(ROW_MODES):
                        equal(
                            blocked(
                                contender,
                                f"SELECT id FROM lock_probe WHERE id=1 FOR {requested} NOWAIT",
                            ),
                            bool(ROW_CONFLICTS[left] & (1 << right)),
                        )
                    equal(scalar(contender, "SELECT count(*) FROM lock_probe"), 1)
            with holder.transaction():
                holder.execute(
                    "SELECT id FROM lock_probe WHERE id=1 FOR KEY SHARE"
                ).fetchall()
                equal(
                    blocked(contender, "UPDATE lock_probe SET v='updated' WHERE id=1"),
                    False,
                )
                equal(
                    blocked(
                        contender, "UPDATE lock_probe SET k='replacement' WHERE id=1"
                    ),
                    True,
                )
                equal(blocked(contender, "DELETE FROM lock_probe WHERE id=1"), True)
            with holder.transaction():
                holder.execute(
                    "SELECT id FROM lock_probe WHERE id=1 FOR SHARE"
                ).fetchall()
                equal(
                    blocked(contender, "UPDATE lock_probe SET v='another' WHERE id=1"),
                    True,
                )
            with holder.transaction():
                holder.execute("SAVEPOINT parent")
                holder.execute("SAVEPOINT child")
                holder.execute(
                    "SELECT id FROM lock_probe WHERE id=1 FOR UPDATE"
                ).fetchall()
                holder.execute("RELEASE SAVEPOINT child")
                equal(
                    blocked(
                        contender,
                        "SELECT id FROM lock_probe WHERE id=1 FOR UPDATE NOWAIT",
                    ),
                    True,
                )
                holder.execute("ROLLBACK TO SAVEPOINT parent")
                equal(
                    blocked(
                        contender,
                        "SELECT id FROM lock_probe WHERE id=1 FOR UPDATE NOWAIT",
                    ),
                    False,
                )
            with holder.transaction():
                holder.execute(
                    "SELECT id FROM lock_probe WHERE id=1 FOR KEY SHARE"
                ).fetchall()
                holder.execute("SAVEPOINT upgrade")
                holder.execute(
                    "SELECT id FROM lock_probe WHERE id=1 FOR UPDATE"
                ).fetchall()
                holder.execute("ROLLBACK TO SAVEPOINT upgrade")
                equal(
                    blocked(
                        contender,
                        "SELECT id FROM lock_probe WHERE id=1 FOR NO KEY UPDATE NOWAIT",
                    ),
                    False,
                )
                equal(
                    blocked(
                        contender,
                        "SELECT id FROM lock_probe WHERE id=1 FOR UPDATE NOWAIT",
                    ),
                    True,
                )
            # Reentrance counts session acquisitions, not pg_locks entries.
            lock = holder.execute("SELECT pg_advisory_lock(42)")
            equal(lock.description[0].type_code, 2278)
            equal(lock.fetchone()[0], "")
            holder.execute("SELECT pg_advisory_lock(42)").fetchall()
            equal(
                scalar(
                    holder,
                    "SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'",
                ),
                1,
            )
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
            equal(scalar(holder, "SELECT pg_advisory_unlock(42)"), True)
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
            equal(scalar(holder, "SELECT pg_advisory_unlock(42)"), True)
            equal(scalar(holder, "SELECT pg_advisory_unlock(42)"), False)
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), True)
            contender.execute("SELECT pg_advisory_unlock_all()")
            # The bigint and two-int key spaces remain distinct for identical bits.
            holder.execute("SELECT pg_advisory_lock(-42)")
            equal(scalar(contender, "SELECT pg_try_advisory_lock(-1,-42)"), True)
            equal(scalar(contender, "SELECT pg_try_advisory_lock(-42)"), False)
            contender.execute("SELECT pg_advisory_unlock_all()")
            holder.execute("SELECT pg_advisory_unlock_all()")
            holder.execute("BEGIN")
            holder.execute("SELECT pg_advisory_lock(42)")
            holder.execute("ROLLBACK")
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
            holder.execute("SELECT pg_advisory_unlock_all()")
            with holder.transaction():
                holder.execute("SELECT pg_advisory_xact_lock(42)")
                holder.execute("SELECT pg_advisory_lock(42)")
                equal(scalar(holder, "SELECT pg_advisory_unlock(42)"), True)
                equal(scalar(holder, "SELECT pg_advisory_unlock(42)"), False)
                holder.execute("SELECT pg_advisory_unlock_all()")
                equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), True)
            contender.execute("SELECT pg_advisory_unlock_all()")
            with holder.transaction():
                holder.execute("SELECT pg_advisory_xact_lock_shared(42)")
                holder.execute("SAVEPOINT child")
                holder.execute("SELECT pg_advisory_xact_lock(42)")
                equal(
                    scalar(contender, "SELECT pg_try_advisory_lock_shared(42)"), False
                )
                holder.execute("ROLLBACK TO SAVEPOINT child")
                equal(scalar(contender, "SELECT pg_try_advisory_lock_shared(42)"), True)
                contender.execute("SELECT pg_advisory_unlock_all()")
                equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
            with holder.transaction():
                holder.execute("SELECT pg_advisory_xact_lock(42)")
                holder.execute("SAVEPOINT repeated")
                holder.execute("SELECT pg_advisory_xact_lock(42)")
                holder.execute("ROLLBACK TO SAVEPOINT repeated")
                equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), True)
            contender.execute("SELECT pg_advisory_unlock_all()")
            # A prepared transaction becomes an independent lock owner. A
            # resource with mixed session/xact ownership cannot be prepared.
            for session_mode in ("pg_advisory_lock", "pg_advisory_lock_shared"):
                for txn_mode in (
                    "pg_advisory_xact_lock",
                    "pg_advisory_xact_lock_shared",
                ):
                    holder.execute("BEGIN")
                    holder.execute(f"SELECT {session_mode}(42)")
                    holder.execute(f"SELECT {txn_mode}(42)")
                    try:
                        holder.execute("PREPARE TRANSACTION 'mixed_lock'")
                    except psycopg.Error as error:
                        equal(error.sqlstate, "0A000")
                    else:
                        raise AssertionError(
                            "mixed session/transaction lock was prepared"
                        )
                    holder.execute("ROLLBACK")
                    equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
                    holder.execute("SELECT pg_advisory_unlock_all()")
                    equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), True)
                    contender.execute("SELECT pg_advisory_unlock_all()")
            holder.execute("BEGIN")
            holder.execute("SELECT pg_advisory_lock(43)")
            holder.execute("SELECT pg_advisory_xact_lock(42)")
            holder.execute("PREPARE TRANSACTION 'lock_handoff'")
            equal(scalar(holder, "SELECT pg_try_advisory_lock(42)"), False)
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), False)
            equal(scalar(contender, "SELECT pg_try_advisory_lock(43)"), False)
            holder.execute("ROLLBACK PREPARED 'lock_handoff'")
            equal(scalar(contender, "SELECT pg_try_advisory_lock(42)"), True)
            contender.execute("SELECT pg_advisory_unlock_all()")
            equal(scalar(contender, "SELECT pg_try_advisory_lock(43)"), False)
            holder.execute("SELECT pg_advisory_unlock_all()")
    print(
        f"Verified {assertions} PostgreSQL lock matrix, MVCC, key-update, reentrance and savepoint assertions"
    )


if __name__ == "__main__":
    check()
