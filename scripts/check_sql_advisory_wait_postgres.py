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

"""PostgreSQL oracle for advisory wait ordering, soft cycles and cancellation.

Uses an isolated disposable server and independent connections. Wait admission
is observed in pg_locks; assertions do not depend on a fixed scheduler sleep.
"""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack
from time import monotonic, sleep

from generate_sql_postgres_reference import postgres


def check():
    import psycopg

    assertions = 0

    def equal(actual, expected):
        nonlocal assertions
        assert actual == expected, (actual, expected)
        assertions += 1

    def query(connection, statement):
        try:
            return connection.execute(statement).fetchone()[0]
        except psycopg.Error as error:
            return error.sqlstate

    with postgres() as root, ExitStack() as stack:
        a, b, c, observer = [
            stack.enter_context(psycopg.connect(root.info.dsn, autocommit=True))
            for _ in range(4)
        ]
        for connection in (a, b, c):
            connection.execute("SET statement_timeout = '5s'")
            connection.execute("SET deadlock_timeout = '100ms'")

        def queued(connection):
            deadline = monotonic() + 3
            while monotonic() < deadline:
                count = observer.execute(
                    "SELECT count(*) FROM pg_locks "
                    "WHERE pid=%s AND locktype='advisory' AND NOT granted",
                    (connection.info.backend_pid,),
                ).fetchone()[0]
                if count:
                    equal(count, 1)
                    return
                sleep(0.005)
            raise AssertionError("backend did not enter its advisory wait")

        with ThreadPoolExecutor(max_workers=3) as executor:
            # A queued exclusive requester prevents a stream of new shared
            # try-locks from overtaking it, even after the holder releases.
            equal(query(a, "SELECT pg_advisory_lock(9001)"), "")
            pending_b = executor.submit(query, b, "SELECT pg_advisory_lock(9001)")
            queued(b)
            equal(query(c, "SELECT pg_try_advisory_lock_shared(9001)"), False)
            equal(query(a, "SELECT pg_advisory_unlock(9001)"), True)
            equal(pending_b.result(timeout=4), "")
            equal(query(c, "SELECT pg_try_advisory_lock_shared(9001)"), False)
            equal(query(b, "SELECT pg_advisory_unlock(9001)"), True)

            # Own held modes and upgrades cannot wait behind a backend which
            # is already blocked by this connection's shared ownership.
            equal(query(a, "SELECT pg_advisory_lock_shared(9001)"), "")
            pending_b = executor.submit(query, b, "SELECT pg_advisory_lock(9001)")
            queued(b)
            equal(query(a, "SELECT pg_advisory_lock_shared(9001)"), "")
            equal(query(a, "SELECT pg_advisory_lock(9001)"), "")
            equal(query(a, "SELECT pg_advisory_unlock(9001)"), True)
            queued(b)
            equal(query(a, "SELECT pg_advisory_unlock_shared(9001)"), True)
            queued(b)
            equal(query(a, "SELECT pg_advisory_unlock_shared(9001)"), True)
            equal(pending_b.result(timeout=4), "")
            query(b, "SELECT pg_advisory_unlock_all()")

            # A->C (held Y), C->B (queue X), B->A (held SH X) is a soft
            # cycle. Reordering C before B admits C's compatible SH request.
            query(a, "SELECT pg_advisory_lock_shared(9001)")
            query(c, "SELECT pg_advisory_lock(9002)")
            pending_b = executor.submit(query, b, "SELECT pg_advisory_lock(9001)")
            queued(b)
            pending_c = executor.submit(
                query, c, "SELECT pg_advisory_lock_shared(9001)"
            )
            queued(c)
            pending_a = executor.submit(query, a, "SELECT pg_advisory_lock(9002)")
            equal(pending_c.result(timeout=4), "")
            query(c, "SELECT pg_advisory_unlock_all()")
            equal(pending_a.result(timeout=4), "")
            query(a, "SELECT pg_advisory_unlock_all()")
            equal(pending_b.result(timeout=4), "")
            query(b, "SELECT pg_advisory_unlock_all()")

            # A hard cycle rejects one request. Session locks owned before
            # that statement remain held until the victim explicitly unlocks.
            query(a, "SELECT pg_advisory_lock(9001)")
            query(b, "SELECT pg_advisory_lock(9002)")
            pending_a = executor.submit(query, a, "SELECT pg_advisory_lock(9002)")
            queued(a)
            pending_b = executor.submit(query, b, "SELECT pg_advisory_lock(9001)")
            done, _ = wait(
                (pending_a, pending_b), timeout=4, return_when=FIRST_COMPLETED
            )
            equal(len(done), 1)
            failed = next(iter(done))
            equal(failed.result(), "40P01")
            victim = a if failed is pending_a else b
            winner = b if victim is a else a
            successful = pending_b if failed is pending_a else pending_a
            queued(winner)
            query(victim, "SELECT pg_advisory_unlock_all()")
            equal(successful.result(timeout=4), "")
            query(winner, "SELECT pg_advisory_unlock_all()")

            # Cancelling a queued command leaves no later ghost acquisition.
            query(a, "SELECT pg_advisory_lock(9001)")
            pending_b = executor.submit(query, b, "SELECT pg_advisory_lock(9001)")
            queued(b)
            b.cancel()
            equal(pending_b.result(timeout=4), "57014")
            query(a, "SELECT pg_advisory_unlock_all()")
            equal(query(c, "SELECT pg_try_advisory_lock(9001)"), True)
            query(c, "SELECT pg_advisory_unlock_all()")
            equal(query(b, "SELECT pg_advisory_unlock(9001)"), False)
    print(
        f"Verified {assertions} PostgreSQL advisory wait, reentrance, upgrade, soft/hard cycle and cancellation assertions"
    )


if __name__ == "__main__":
    check()
