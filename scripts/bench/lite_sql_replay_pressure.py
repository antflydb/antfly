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

"""Synthetic SQL commit followed by a replay-capacity admission probe.

Uses a prebuilt library, no models or private inputs. Key width and transaction
size are explicit workload dimensions; this is a local diagnosis, not a frozen
importer or final-host qualification. Never retries an ambiguous SQL commit or
batch result. Verification and explicit replay draining follow measurement.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
import hashlib
import json
from pathlib import Path
import resource
import time

from lite_contention import sdk, usage_delta


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identifier(table, number, width):
    prefix = f"table-{table}/row-{number:09d}/"
    return prefix + "k" * max(0, width - len(prefix))


def row_text(text, key):
    # Repeated vocabulary, but distinct source bytes: whole-input compilation
    # cache hits must not stand in for the cost of replaying real unique rows.
    return text + " record " + "/".join(key.split("/")[:2])


def snapshot(root, tables):
    return {
        "background_publications": root.pending_work_stats().get(
            "background_publications", {}
        ),
        "indexes": [
            table.pending_work_stats()["index_replay_debt"] for table in tables
        ],
    }


def verify_rows(root, name, keys, text, key_column="_id"):
    remaining = set(keys)
    with closing(root.sql_cursor(f'SELECT {key_column}, text FROM "{name}"')) as cursor:
        while True:
            page = cursor.fetch(512)
            for key, value in page["result"]["rows"]:
                if key not in remaining or value != row_text(text, key):
                    raise RuntimeError("primary key/payload mismatch or duplicate row")
                remaining.remove(key)
            if page["exhausted"]:
                break
            if not page["result"]["rows"]:
                raise RuntimeError("cursor did not make progress")
    if remaining:
        raise RuntimeError("acknowledged primary rows missing")


def run(args):
    library = Path(args.library).resolve()
    af = sdk(str(library))
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    database = directory / "database.aflite"
    output = directory / "result.json"
    report = {
        "library_sha256": digest(library),
        "benchmark_sha256": digest(__file__),
        "scope": "Local synthetic diagnostic; no inference, no automatic retries",
        "arguments": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "load_average": __import__("os").getloadavg(),
        "status": "incomplete",
    }

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n")

    unit = "configuration visibility admission maintenance acknowledgement transactions indexing generation publication "
    text = (unit * (args.text_bytes // len(unit) + 1))[: args.text_bytes]
    names = ["replay_left", "replay_right"]
    key_column = "row_key" if args.relational_key else "_id"
    expected = [
        [
            identifier(table, row, args.key_bytes)
            for row in range(args.rows * args.transactions)
        ]
        for table in range(len(names))
    ]
    try:
        with (
            af.create(
                database, busy_timeout=args.busy_timeout, no_sync=args.no_sync
            ) as root,
            ExitStack() as stack,
        ):
            for name in names:
                columns = (
                    "row_key TEXT PRIMARY KEY, text TEXT"
                    if args.relational_key
                    else "text TEXT"
                )
                root.sql(f'CREATE TABLE "{name}" ({columns})')
            tables = [stack.enter_context(root.open_table(name)) for name in names]
            for table in tables:
                if not args.no_text_index and not any(
                    index["kind"] == "full_text" for index in table.stats()["indexes"]
                ):
                    table.add_index({"name": "text", "type": "full_text"})
            probe_name = names[0]
            probe_table = tables[0]
            if args.relational_key:
                # SQL primary-key claims require the SQL coordinator. Probe
                # file-wide batch admission through an ordinary document table,
                # matching applications which use both APIs on one file.
                probe_name = "replay_admission_probe"
                root.sql(f'CREATE TABLE "{probe_name}" (text TEXT)')
                probe_table = stack.enter_context(root.open_table(probe_name))
            probe_expected = []
            report["before"] = snapshot(root, tables)
            before = resource.getrusage(resource.RUSAGE_SELF)
            report["sql_prepare_seconds"] = 0.0
            report["sql_commit_seconds"] = 0.0
            report["transactions"] = []
            report["sql_blocks"] = []
            for transaction in range(args.transactions):
                began = time.monotonic()
                with closing(root.sql_session()) as session:
                    session.execute("BEGIN")
                    for table_number, (name, keys) in enumerate(zip(names, expected)):
                        selected = keys[
                            transaction * args.rows : (transaction + 1) * args.rows
                        ]
                        for start in range(0, len(selected), args.block_rows):
                            block = selected[start : start + args.block_rows]
                            parameters = [
                                item
                                for key in block
                                for item in (key, row_text(text, key))
                            ]
                            statement = (
                                f'INSERT INTO "{name}" ({key_column}, text) VALUES '
                                + ",".join(
                                    f"(${2 * row + 1},${2 * row + 2})"
                                    for row in range(len(block))
                                )
                            )
                            if args.on_conflict:
                                statement += f" ON CONFLICT ({key_column}) DO UPDATE SET text = EXCLUDED.text"
                            block_started = time.monotonic()
                            session.execute(statement, parameters)
                            report["sql_blocks"].append(
                                {
                                    "transaction": transaction,
                                    "table": name,
                                    "preceding_rows_in_table": start,
                                    "transaction_prior_rows": table_number * args.rows
                                    + start,
                                    "rows": len(block),
                                    "seconds": time.monotonic() - block_started,
                                }
                            )
                    preparation = time.monotonic() - began
                    report["sql_prepare_seconds"] += preparation
                    save()
                    commit = time.monotonic()
                    try:
                        session.execute("COMMIT")
                    except Exception as error:
                        report["failed_commit"] = {
                            "ordinal": transaction,
                            "sqlstate": getattr(error, "sqlstate", None),
                            "commit_outcome": getattr(error, "commit_outcome", None),
                            "has_transaction_receipt": bool(
                                getattr(error, "transaction_id", None)
                            ),
                        }
                        raise
                    elapsed = time.monotonic() - commit
                    report["sql_commit_seconds"] += elapsed
                    report["transactions"].append(
                        {
                            "ordinal": transaction,
                            "prepare_seconds": preparation,
                            "commit_seconds": elapsed,
                        }
                    )
            report["after_commit"] = snapshot(root, tables)
            save()
            extra = [
                identifier(0, args.rows * args.transactions + row, args.key_bytes)
                for row in range(args.followup_rows)
            ]
            count_before = int(
                root.sql(f'SELECT COUNT(*) FROM "{probe_name}"')["rows"][0][0]
            )
            probe = time.monotonic()
            try:
                probe_table.batch_json(
                    {
                        "inserts": {
                            key: {"text": row_text(text, key)} for key in extra
                        },
                        "sync_level": "write",
                    }
                )
                report["admission"] = "success"
                if args.relational_key:
                    probe_expected.extend(extra)
                else:
                    expected[0].extend(extra)
            except af.BusyError:
                report["admission"] = "busy"
                count_after = int(
                    root.sql(f'SELECT COUNT(*) FROM "{probe_name}"')["rows"][0][0]
                )
                if count_after != count_before:
                    raise RuntimeError("retryable Busy changed primary row count")
                report["busy_precommit_count_check"] = True
            report["followup_seconds"] = time.monotonic() - probe
            report["measured_cpu"] = usage_delta(
                before, resource.getrusage(resource.RUSAGE_SELF)
            )
            report["after_probe"] = snapshot(root, tables)
            report["file_bytes_before_drain"] = database.stat().st_size
            save()
            drain = time.monotonic()
            root.run_until_idle()
            report["explicit_drain_seconds"] = time.monotonic() - drain
            report["after_drain"] = snapshot(root, tables)
            for debts in report["after_drain"]["indexes"]:
                for debt in debts:
                    if (
                        debt["catch_up_required"]
                        or debt["applied_sequence"] < debt["target_sequence"]
                        or debt["replay_document_not_visible_skipped"]
                    ):
                        raise RuntimeError(
                            "index did not catch up without skipped documents"
                        )
            for name, keys in zip(names, expected):
                verify_rows(root, name, keys, text, key_column)
            if args.relational_key:
                verify_rows(root, probe_name, probe_expected, text)
            report["integrity"] = root.check()
            if not report["integrity"]["valid"]:
                raise RuntimeError("integrity failed")
        with af.open_readonly(database) as root:
            for name, keys in zip(names, expected):
                verify_rows(root, name, keys, text, key_column)
            if args.relational_key:
                verify_rows(root, probe_name, probe_expected, text)
        report.update(
            status="passed",
            acknowledged_rows=sum(map(len, expected)) + len(probe_expected),
            reopen=True,
        )
    except Exception as error:
        report.update(
            status="failed", error_type=type(error).__name__, error=str(error)
        )
        failed_commit = report.get("failed_commit", {})
        if (
            failed_commit.get("commit_outcome") == "not_applied"
            or failed_commit.get("sqlstate") == "55P03"
        ):
            # Verify the refusal after session/handle teardown. An exit status
            # or absent receipt cannot establish that COMMIT changed no rows.
            successful = len(report["transactions"])
            check = report["failed_commit_primary_check"] = {
                "passed": False,
                "successful_commit_responses": successful,
                "rows_per_table": successful * args.rows,
            }
            with af.open_readonly(database) as root:
                for name, keys in zip(names, expected):
                    verify_rows(
                        root, name, keys[: successful * args.rows], text, key_column
                    )
                valid = root.check()["valid"]
                if not valid:
                    raise RuntimeError("integrity failed after refused COMMIT")
            check.update(passed=True, integrity_valid=valid)
        raise
    finally:
        save()
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "admission",
                    "sql_prepare_seconds",
                    "sql_commit_seconds",
                    "followup_seconds",
                    "explicit_drain_seconds",
                    "acknowledged_rows",
                )
            }
        )
    )
    return not args.require_no_busy or report["admission"] == "success"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument(
        "--rows",
        type=int,
        default=1024,
        help="Rows per table in each two-table transaction",
    )
    parser.add_argument("--transactions", type=int, default=8)
    parser.add_argument("--block-rows", type=int, default=128)
    parser.add_argument("--key-bytes", type=int, default=512)
    parser.add_argument("--text-bytes", type=int, default=4096)
    parser.add_argument("--followup-rows", type=int, default=128)
    parser.add_argument("--busy-timeout", type=float, default=5)
    parser.add_argument("--no-sync", action="store_true")
    parser.add_argument(
        "--on-conflict",
        action="store_true",
        help="Exercise incremental SQL conflict-owner resolution",
    )
    parser.add_argument(
        "--relational-key",
        action="store_true",
        help="Use a declared SQL primary key to exercise coordinated unique claims",
    )
    parser.add_argument(
        "--no-text-index",
        action="store_true",
        help="Isolate transaction staging from text replay",
    )
    parser.add_argument("--require-no-busy", action="store_true")
    args = parser.parse_args()
    if (
        min(
            args.rows,
            args.transactions,
            args.block_rows,
            args.key_bytes,
            args.text_bytes,
            args.followup_rows,
        )
        < 1
        or args.busy_timeout < 0
    ):
        parser.error("row/key/text sizes must be positive and timeout nonnegative")
    raise SystemExit(0 if run(args) else 1)
