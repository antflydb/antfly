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

"""Independent PostgreSQL notification lifetime oracle in a disposable database.

Executes the three unchanged original statements and supplemental concurrency,
transaction, savepoint, PREPARE, channel and payload contracts. This script alone
cannot qualify Antfly public execution or change original case dispositions.
"""

import json
from pathlib import Path

from generate_sql_postgres_reference import postgres


def check():
    import psycopg

    assertions = 0

    def equal(actual, expected):
        nonlocal assertions
        if actual != expected:
            raise AssertionError((actual, expected))
        assertions += 1

    def drain(connection):
        return [
            (message.channel, message.payload, message.pid)
            for message in connection.notifies(timeout=0.025)
        ]

    def notify(connection, channel, payload):
        connection.execute("SELECT pg_notify(%s,%s)", (channel, payload))

    def rejected(connection, statement, code):
        try:
            connection.execute(statement)
        except psycopg.Error as error:
            equal(error.sqlstate, code)
        else:
            raise AssertionError(f"accepted {statement}")

    inventory = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
        ).read_text()
    )
    originals = {row["id"]: row["sql"] for row in inventory["entries"]}
    with postgres(max_prepared_transactions=4) as listener:
        with psycopg.connect(listener.info.dsn, autocommit=True) as sender:
            listener_pid = listener.execute("SELECT pg_backend_pid()").fetchone()[0]
            sender_pid = sender.execute("SELECT pg_backend_pid()").fetchone()[0]
            listener.execute(originals["sql-0126"])
            listener.execute(originals["sql-0127"])
            equal(drain(listener), [("usage_events", "updated", listener_pid)])
            listener.execute(originals["sql-0128"])
            notify(sender, "usage_events", "absent")
            equal(drain(listener), [])

            listener.execute("BEGIN")
            listener.execute(originals["sql-0126"])
            notify(sender, "usage_events", "before-registration")
            listener.execute("COMMIT")
            equal(drain(listener), [])
            notify(sender, "usage_events", "registered")
            equal(drain(listener), [("usage_events", "registered", sender_pid)])

            listener.execute("BEGIN")
            listener.execute(originals["sql-0128"])
            notify(sender, "usage_events", "rollback-keeps-listener")
            equal(drain(listener), [])
            listener.execute("ROLLBACK")
            equal(
                drain(listener),
                [("usage_events", "rollback-keeps-listener", sender_pid)],
            )

            sender.execute("BEGIN")
            notify(sender, "usage_events", "discarded")
            sender.execute("ROLLBACK")
            equal(drain(listener), [])
            sender.execute("BEGIN")
            notify(sender, "usage_events", "first")
            sender.execute("SAVEPOINT child")
            notify(sender, "usage_events", "second")
            sender.execute("ROLLBACK TO child")
            notify(sender, "usage_events", "first")
            notify(sender, "usage_events", "third")
            sender.execute("RELEASE child")
            sender.execute("COMMIT")
            equal(
                drain(listener),
                [
                    ("usage_events", "first", sender_pid),
                    ("usage_events", "third", sender_pid),
                ],
            )
            notify(sender, "usage_events", "first")
            equal(drain(listener), [("usage_events", "first", sender_pid)])

            listener.execute("BEGIN")
            notify(sender, "usage_events", "deferred")
            equal(drain(listener), [])
            listener.execute("SELECT 1")
            equal(drain(listener), [])
            listener.execute("COMMIT")
            equal(drain(listener), [("usage_events", "deferred", sender_pid)])

            listener.execute("LISTEN usage_events")
            notify(sender, "usage_events", "registration-is-idempotent")
            equal(
                drain(listener),
                [("usage_events", "registration-is-idempotent", sender_pid)],
            )
            for statement in (
                "LISTEN usage_events",
                "UNLISTEN usage_events",
                "NOTIFY usage_events, 'prepare'",
            ):
                if statement.startswith("UNLISTEN"):
                    sender.execute("LISTEN usage_events")
                sender.execute("BEGIN")
                sender.execute(statement)
                rejected(sender, "PREPARE TRANSACTION 'notification_oracle'", "0A000")
                sender.execute("ROLLBACK")
            equal(drain(listener), [])

            sender.execute("BEGIN")
            sender.execute("SAVEPOINT removed")
            sender.execute("NOTIFY usage_events, 'removed'")
            sender.execute("ROLLBACK TO removed")
            sender.execute("PREPARE TRANSACTION 'empty_notification_oracle'")
            sender.execute("COMMIT PREPARED 'empty_notification_oracle'")
            equal(drain(listener), [])

            listener.execute("BEGIN READ ONLY")
            listener.execute("LISTEN read_only_events")
            listener.execute("NOTIFY read_only_events, 'allowed'")
            listener.execute("COMMIT")
            equal(drain(listener), [("read_only_events", "allowed", listener_pid)])
            listener.execute('LISTEN "Case Sensitive"')
            notify(sender, "Case Sensitive", "exact")
            equal(drain(listener), [("Case Sensitive", "exact", sender_pid)])
            notify(sender, "case sensitive", "wrong-case")
            equal(drain(listener), [])
            notify(sender, "usage_events", "x" * 7999)
            equal(drain(listener), [("usage_events", "x" * 7999, sender_pid)])
            rejected(
                sender, "SELECT pg_notify('usage_events', repeat('x',8000))", "22023"
            )
            listener.execute("NOTIFY usage_events")
            equal(drain(listener), [("usage_events", "", listener_pid)])

            listener.execute("BEGIN")
            listener.execute("UNLISTEN *")
            listener.execute("NOTIFY usage_events, 'unsubscribed-self'")
            listener.execute("COMMIT")
            equal(drain(listener), [])
            notify(sender, "usage_events", "unsubscribed-peer")
            equal(drain(listener), [])
            listener.execute("BEGIN")
            listener.execute("LISTEN usage_events")
            listener.execute("NOTIFY usage_events, 'subscribed-self'")
            listener.execute("COMMIT")
            equal(drain(listener), [("usage_events", "subscribed-self", listener_pid)])
    print(
        f"Verified {assertions} PostgreSQL notification ordering, lifetime, payload and PREPARE assertions"
    )


if __name__ == "__main__":
    check()
