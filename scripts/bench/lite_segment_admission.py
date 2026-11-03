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

"""Compare immutable-segment admission and replay with prebuilt libraries.

Run each case in a fresh process and directory; reuse an unchanged generated
seed for both libraries. For example:
  python3 scripts/bench/lite_segment_admission.py seed --library LIB \
      --directory .tools/admission-seed --rows 128 --fields 32 --compact-seed
  python3 scripts/bench/lite_segment_admission.py open --library LIB \
      --directory .tools/admission-open --source .tools/admission-seed/database.aflite \
      --rows 128 --fields 32

For replay, create a separate seed with --pending-batches 8, then select replay
with the same rows/fields/pending-batches arguments. Timed service precedes an
explicit drain and acknowledgment/search/integrity/reopen checks. Use a parent
process watchdog for potentially blocked native calls. This synthetic diagnostic
is not unchanged-importer or sustained-write qualification.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

from lite_contention import sdk


def digest(path):
    with Path(path).open("rb") as stream:
        hasher = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
        return hasher.hexdigest()


def builds():
    try:
        rows = subprocess.check_output(
            ["ps", "-axo", "stat,comm"], text=True, stderr=subprocess.DEVNULL
        ).splitlines()[1:]
    except (OSError, subprocess.CalledProcessError):
        return None
    return sum(
        len(v := row.split(None, 1)) == 2
        and "T" not in v[0]
        and Path(v[1]).name
        in {"zig", "maker", "cargo", "rustc", "clang", "clang++", "ninja"}
        for row in rows
    )


def observe_builds(report):
    current = builds()
    prior = report["peak_native_builds"]
    report["peak_native_builds"] = (
        max(prior, current) if current is not None and prior is not None else None
    )


def row(number, fields):
    return {
        name: f"seedcanary configuration visibility admission maintenance record{number} column{column}"
        for column in range(fields)
        for name in ["body" if column == 0 else f"column{column:02}"]
    }


def verify(db, count, fields):
    for number in range(count):
        actual = db.lookup(f"row/{number:08}")
        for key, value in row(number, fields).items():
            if actual.get(key) != value:
                raise RuntimeError("acknowledged generated row mismatch")
    if not db.check()["valid"]:
        raise RuntimeError("integrity failed")
    for column in range(fields):
        field = "body" if column == 0 else f"column{column:02}"
        found = json.dumps(
            db.search(
                {
                    "full_text_search": {
                        "match": {"field": field, "text": "seedcanary"}
                    },
                    "limit": 10,
                }
            )
        )
        if "row/" not in found:
            raise RuntimeError(f"search canary missing for {field}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["seed", "open", "replay"])
    p.add_argument("--library", type=Path, required=True)
    p.add_argument("--directory", type=Path, required=True)
    p.add_argument("--source", type=Path)
    p.add_argument("--rows", type=int, default=512)
    p.add_argument("--fields", type=int, default=32)
    p.add_argument("--pending-batches", type=int, default=0)
    p.add_argument("--opens", type=int, default=12)
    p.add_argument("--seconds", type=float, default=25)
    p.add_argument("--compact-seed", action="store_true")
    a = p.parse_args()
    assert 0 < a.rows <= 100_000 and 1 <= a.fields <= 128
    assert a.opens >= 3 and 0 <= a.pending_batches <= 100_000 and a.seconds > 0
    os.umask(0o077)
    a.directory.mkdir(exist_ok=False)
    database = a.directory / "database.aflite"

    report = {
        "scope": "Local synthetic admission/service diagnostic, not original-importer or sustained-write qualification",
        "mode": a.mode,
        "library_sha256": digest(a.library),
        "benchmark_sha256": digest(__file__),
        "rows": a.rows,
        "fields": a.fields,
        "load_before": os.getloadavg(),
        "peak_native_builds": builds(),
        "status": "incomplete",
    }

    def save():
        report["load_after"] = os.getloadavg()
        (a.directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")

    try:
        # Never load a mutable build output directly. A later replacement of
        # that path must not change which binary this process measures.
        library_directory = a.directory / "library"
        library_directory.mkdir()
        library = library_directory / a.library.name
        if sys.platform == "darwin":
            subprocess.run(["cp", "-c", str(a.library), str(library)], check=True)
        else:
            shutil.copyfile(a.library, library)
        if (
            digest(library) != report["library_sha256"]
            or digest(a.library) != report["library_sha256"]
        ):
            raise RuntimeError("library changed while freezing")
        af = sdk(str(library.resolve()))
        if a.mode == "seed":
            with af.create(database, profile=af.Profile.HOSTED, busy_timeout=5) as db:
                if not any(
                    index["kind"] == "full_text" for index in db.stats()["indexes"]
                ):
                    db.add_index({"name": "text", "type": "full_text"})
                for start in range(0, a.rows, 64):
                    db.batch_json(
                        {
                            "inserts": {
                                f"row/{number:08}": row(number, a.fields)
                                for number in range(start, min(start + 64, a.rows))
                            },
                            "sync_level": "write",
                        }
                    )
                db.run_until_idle()
                verify(db, a.rows, a.fields)
                if a.compact_seed:
                    report["seed_compaction"] = db.compact()
                    verify(db, a.rows, a.fields)
                report["warm_debt"] = db.pending_work_stats()["index_replay_debt"]
                for batch in range(a.pending_batches):
                    start = a.rows + batch * 256
                    db.batch_json(
                        {
                            "inserts": {
                                f"row/{number:08}": row(number, a.fields)
                                for number in range(start, start + 256)
                            },
                            "sync_level": "write",
                        }
                    )
                report["seed_debt"] = db.pending_work_stats()["index_replay_debt"]
            report["source_sha256"] = digest(database)
            report["expected_rows"] = a.rows + a.pending_batches * 256
            if a.pending_batches and not any(
                d["catch_up_required"] for d in report["seed_debt"]
            ):
                raise RuntimeError("seed unexpectedly drained pending replay")
        else:
            assert a.source is not None and a.source.is_file()
            report["source_sha256"] = digest(a.source)
            seed = json.loads((a.source.parent / "result.json").read_text())
            if (
                seed.get("status") != "diagnostic_completed"
                or seed.get("mode") != "seed"
                or seed.get("source_sha256") != report["source_sha256"]
                or seed.get("rows") != a.rows
                or seed.get("fields") != a.fields
                or seed.get("expected_rows") != a.rows + a.pending_batches * 256
            ):
                raise RuntimeError(
                    "source fixture parameters or immutable identity mismatch"
                )
            if sys.platform == "darwin":
                subprocess.run(["cp", "-c", str(a.source), str(database)], check=True)
            else:
                shutil.copyfile(a.source, database)
            if (
                digest(database) != report["source_sha256"]
                or digest(a.source) != report["source_sha256"]
            ):
                raise RuntimeError("source changed while cloning")
            if a.mode == "open":
                assert a.pending_batches == 0
                report["samples"] = []
                for iteration in range(a.opens + 2):
                    observe_builds(report)
                    began = time.monotonic()
                    db = af.open(database, profile=af.Profile.HOSTED, busy_timeout=5)
                    opened = time.monotonic()
                    db.close()
                    ended = time.monotonic()
                    if iteration >= 2:
                        report["samples"].append(
                            {
                                "open_seconds": opened - began,
                                "close_seconds": ended - opened,
                            }
                        )
                    save()
                report["open_median_seconds"] = statistics.median(
                    s["open_seconds"] for s in report["samples"]
                )
            else:
                report["samples"] = []
                began = time.monotonic()
                with af.open(database, busy_timeout=5) as db:
                    report["open_seconds"] = time.monotonic() - began
                    began = time.monotonic()
                    while time.monotonic() - began < a.seconds:
                        observe_builds(report)
                        report["samples"].append(
                            {
                                "seconds": time.monotonic() - began,
                                "state": db.pending_work_stats(),
                            }
                        )
                        save()
                        time.sleep(1)
                    report["measured_seconds"] = time.monotonic() - began
                    drain = time.monotonic()
                    db.run_until_idle()
                    report["explicit_drain_seconds"] = time.monotonic() - drain
                    verify(db, a.rows + a.pending_batches * 256, a.fields)
                    report["after_drain"] = db.pending_work_stats()["index_replay_debt"]
                    if any(
                        d["catch_up_required"]
                        or d["applied_sequence"] < d["target_sequence"]
                        or d["replay_document_not_visible_skipped"]
                        for d in report["after_drain"]
                    ):
                        raise RuntimeError("index catch-up failed")
            with af.open_readonly(database) as db:
                verify(db, a.rows + a.pending_batches * 256, a.fields)
            report["integrity_search_acknowledgement_reopen"] = True
        report["status"] = "diagnostic_completed"
        report["file_bytes"] = database.stat().st_size
    except BaseException as error:
        report.update(
            status="failed", error_type=type(error).__name__, error=str(error)
        )
        raise
    finally:
        save()
    print(
        json.dumps(
            {
                key: report[key]
                for key in [
                    "status",
                    "mode",
                    "library_sha256",
                    "peak_native_builds",
                    "file_bytes",
                ]
            }
            | {
                key: report[key]
                for key in ["open_median_seconds", "explicit_drain_seconds"]
                if key in report
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
