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

"""Exercise allocator proofs on a fresh, multi-GB retained document graph.

The large fixture drops the default text index to isolate storage traversal.
Foreign writers exit without close. When the measured runtime publishes live
certificates, use --foreign-library with a legacy writer to keep these mutations
uncertified and require an actual graph walk. Use --certified-handoff to measure
checked live handoff instead and require zero repeated graph walks. Fresh paths
are created. Use the contract suite for deterministic proof-over-deadline and
cancellation coverage; this program reports actual observed proof duration.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import random
import resource
import subprocess
import sys
import time
from pathlib import Path

from lite_contention import measurement_stats, percentile, sdk


def emit(**row):
    print(json.dumps(row, sort_keys=True), flush=True)


def foreign(library, database, sequence):
    af = sdk(library)
    db = af.open(database, busy_timeout=5.0)
    attempts = []
    payload = {"inserts": {"foreign": {"sequence": sequence, "nonce": time.time_ns()}}}
    deadline = time.monotonic() + 120
    while True:
        began = time.monotonic()
        try:
            db.batch_json(payload)
        except af.BusyError:
            attempts.append(
                {"status": "busy", "elapsed_seconds": time.monotonic() - began}
            )
            if time.monotonic() >= deadline:
                emit(
                    kind="foreign_setup",
                    sequence=sequence,
                    attempts=attempts,
                    status="error",
                )
                raise
            time.sleep(0.05)
        else:
            attempts.append(
                {"status": "ok", "elapsed_seconds": time.monotonic() - began}
            )
            break
    emit(kind="foreign_setup", sequence=sequence, attempts=attempts, status="ok")
    # Deliberately simulate an unclean process exit after a successful commit.
    os._exit(0)


def run(args):
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=args.reuse_fixture)
    database = directory / "large.aflite"
    af = sdk(args.library)
    random_source = random.Random(1412)
    first_digest = None
    started = time.monotonic()
    opener = af.open if args.reuse_fixture else af.create
    with opener(database, busy_timeout=5.0) as db:
        if not args.reuse_fixture:
            assert db.delete_index("full_text_index_v0")
            for start in range(0, args.records, 8):
                documents = {}
                for index in range(start, min(start + 8, args.records)):
                    payload = base64.b64encode(random_source.randbytes(196608)).decode()
                    documents[f"seed-{index}"] = {"payload": payload}
                    if index == 0:
                        first_digest = hashlib.sha256(payload.encode()).hexdigest()
                db.batch_json({"inserts": documents})
                if start % 512 == 0:
                    emit(
                        kind="seed_progress",
                        records=start + len(documents),
                        file_bytes=database.stat().st_size,
                        elapsed_seconds=time.monotonic() - started,
                    )
            # Keep 15/16 of the graph live and leave a reusable pool for proofs.
            for start in range(0, args.records, 128):
                db.batch_json(
                    {
                        "deletes": [
                            f"seed-{i}"
                            for i in range(start, min(start + 128, args.records))
                            if i % 16 == 15
                        ]
                    }
                )
        if args.reuse_fixture:
            payload = base64.b64encode(random_source.randbytes(196608)).decode()
            first_digest = hashlib.sha256(payload.encode()).hexdigest()
            assert (
                hashlib.sha256(db.lookup("seed-0")["payload"].encode()).hexdigest()
                == first_digest
            )
        if "large_proof_numbers" not in db.list_tables():
            db.sql("CREATE TABLE large_proof_numbers (n BIGINT)")
        run_nonce = time.time_ns()
        sql_key = "measured-" + str(run_nonce)
        emit(
            kind="seeded",
            file_bytes=database.stat().st_size,
            retained_payload_bytes=(args.records - args.records // 16) * 262144,
            elapsed_seconds=time.monotonic() - started,
            stats=db.stats(),
        )
        operations = (
            (
                "batch",
                lambda: db.batch_json(
                    {"inserts": {"measured": {"value": 1, "nonce": run_nonce}}}
                ),
            ),
            (
                "sql",
                lambda: db.sql(
                    "INSERT INTO large_proof_numbers (_id,n) VALUES ($1,7)", [sql_key]
                ),
            ),
            ("read", lambda: db.lookup("seed-0")),
        )
        for sequence, (name, operation) in enumerate(operations):
            if args.warm_only or name not in args.operations:
                continue
            subprocess.run(
                [
                    sys.executable,
                    __file__,
                    "_foreign",
                    args.foreign_library or args.library,
                    str(database),
                    str(sequence),
                ],
                check=True,
                timeout=300,
            )
            before = measurement_stats(db)
            began = time.monotonic()
            value = operation()
            elapsed = time.monotonic() - began
            after = measurement_stats(db)
            if name == "read":
                assert (
                    hashlib.sha256(value["payload"].encode()).hexdigest()
                    == first_digest
                )
            if name == "sql":
                rows = db.sql(
                    "SELECT n FROM large_proof_numbers WHERE _id=$1", [sql_key]
                )["rows"]
                assert len(rows) == 1 and int(rows[0][0]) == 7
            assert db.lookup("foreign")["sequence"] == sequence
            proof_walks = (
                after["allocator_proof_walks"] - before["allocator_proof_walks"]
            )
            if name != "read":
                if args.certified_handoff and proof_walks != 0:
                    raise RuntimeError(f"{name} repeated a proof after checked handoff")
                if not args.certified_handoff and proof_walks <= 0:
                    raise RuntimeError(f"{name} did not exercise an allocator proof")
            emit(
                kind="operation",
                operation=name,
                sql_key=sql_key if name == "sql" else None,
                status="ok",
                busy_timeout_seconds=5,
                proof_required=name != "read" and not args.certified_handoff,
                certificate_expected=name != "read" and args.certified_handoff,
                elapsed_seconds=elapsed,
                proof_walks=after["allocator_proof_walks"]
                - before["allocator_proof_walks"],
                proof_ns=after["allocator_proof_ns"] - before["allocator_proof_ns"],
                proof_max_ns=after["allocator_proof_max_ns"],
                proof_failures=after["allocator_proof_failures"]
                - before["allocator_proof_failures"],
            )
        if args.warm_iterations:
            # Exclude opening, initial proofs, and foreign handoff from the
            # steady-writer cost. Both mutations warm their namespace first.
            db.batch_json(
                {"inserts": {"warm-measured": {"nonce": run_nonce, "sequence": -1}}}
            )
            db.sql(
                f"INSERT INTO large_proof_numbers (n, _id) VALUES (7, 'warm-{sql_key}-setup')"
            )
            for name in ("batch", "sql"):
                before = (
                    db.pending_work_stats().get("background_publications", {})
                    if args.warm_only
                    else measurement_stats(db)
                )
                usage_before = resource.getrusage(resource.RUSAGE_SELF)
                samples = []
                began = time.monotonic()
                for sequence in range(args.warm_iterations):
                    started_ns = time.monotonic_ns()
                    if name == "batch":
                        db.batch_json(
                            {
                                "inserts": {
                                    "warm-measured": {
                                        "nonce": run_nonce,
                                        "sequence": sequence,
                                    }
                                }
                            }
                        )
                    else:
                        db.sql(
                            f"INSERT INTO large_proof_numbers (n, _id) VALUES (7, 'warm-{sql_key}-{sequence}')"
                        )
                    samples.append(time.monotonic_ns() - started_ns)
                wall_seconds = time.monotonic() - began
                usage_after = resource.getrusage(resource.RUSAGE_SELF)
                after = (
                    db.pending_work_stats().get("background_publications", {})
                    if args.warm_only
                    else measurement_stats(db)
                )
                proof_walks = (
                    after["allocator_proof_walks"] - before["allocator_proof_walks"]
                    if "allocator_proof_walks" in before
                    and "allocator_proof_walks" in after
                    else None
                )
                if proof_walks is not None and proof_walks != 0:
                    raise RuntimeError(
                        f"warm {name} unexpectedly repeated an allocator proof"
                    )
                emit(
                    kind="warm_operation",
                    operation=name,
                    status="ok",
                    calls=args.warm_iterations,
                    wall_seconds=wall_seconds,
                    calls_per_second=args.warm_iterations / wall_seconds,
                    p50_ns=percentile(samples, 50),
                    p95_ns=percentile(samples, 95),
                    max_ns=max(samples),
                    cpu_seconds=(usage_after.ru_utime + usage_after.ru_stime)
                    - (usage_before.ru_utime + usage_before.ru_stime),
                    proof_walks=proof_walks,
                    timing_scope="single writer after batch and SQL warmup; no foreign publications",
                )
        integrity = db.check()
        if not isinstance(integrity, dict) or integrity.get("valid") is not True:
            raise RuntimeError(f"integrity failed: {integrity}")
        emit(kind="integrity", status="ok", result=integrity)
    with af.open(database, busy_timeout=5.0) as db:
        if not args.warm_only and "batch" in args.operations:
            measured = db.lookup("measured")
            assert measured["value"] == 1 and measured["nonce"] == run_nonce
        if not args.warm_only and "sql" in args.operations:
            rows = db.sql("SELECT n FROM large_proof_numbers WHERE _id=$1", [sql_key])[
                "rows"
            ]
            assert len(rows) == 1 and int(rows[0][0]) == 7
        if args.warm_iterations:
            measured = db.lookup("warm-measured")
            assert measured["nonce"] == run_nonce
            assert measured["sequence"] == args.warm_iterations - 1
            rows = db.sql(
                "SELECT n FROM large_proof_numbers WHERE _id=$1",
                [f"warm-{sql_key}-{args.warm_iterations - 1}"],
            )["rows"]
            assert len(rows) == 1 and int(rows[0][0]) == 7
        assert (
            hashlib.sha256(db.lookup("seed-0")["payload"].encode()).hexdigest()
            == first_digest
        )
    emit(
        kind="complete",
        status="ok",
        file_bytes=database.stat().st_size,
        elapsed_seconds=time.monotonic() - started,
    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_foreign":
        foreign(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--library", required=True)
        parser.add_argument(
            "--foreign-library",
            help="legacy setup writer that leaves an uncertified tip on unclean exit",
        )
        parser.add_argument(
            "--certified-handoff",
            action="store_true",
            help="use the measured runtime as setup writer and require no repeated proof",
        )
        parser.add_argument("--directory", required=True)
        parser.add_argument("--records", type=int, default=12288)
        parser.add_argument(
            "--warm-iterations",
            type=int,
            default=0,
            help="also measure steady batch and SQL writes after excluding initial proof work",
        )
        parser.add_argument(
            "--warm-only",
            action="store_true",
            help="skip foreign handoffs; allow legacy libraries without proof counters (reported as null)",
        )
        parser.add_argument(
            "--reuse-fixture",
            action="store_true",
            help="resume this synthetic fixture without reseeding",
        )
        parser.add_argument(
            "--operations",
            nargs="+",
            choices=("batch", "sql", "read"),
            default=["batch", "sql", "read"],
        )
        arguments = parser.parse_args()
        if arguments.certified_handoff and arguments.foreign_library:
            parser.error("--certified-handoff cannot use --foreign-library")
        if arguments.warm_only and not arguments.warm_iterations:
            parser.error("--warm-only requires positive --warm-iterations")
        if arguments.warm_iterations < 0:
            parser.error("--warm-iterations must be nonnegative")
        if arguments.records < 16:
            parser.error("records must be at least 16")
        run(arguments)
