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

"""Matched durable document writes: SQLite WAL/FULL vs an immutable Antfly library.

One reusable file per engine. Each successful case verifies every latest value
and indexed token, records a receipt, then deletes the closed fixture. No data
or database snapshots are archived. Full-text acknowledgement is synchronous
on both engines; primary-only cases have no full-text index on either engine.
"""

from __future__ import annotations
import argparse, contextlib, hashlib, json, math, multiprocessing as mp, os
from pathlib import Path
import platform, queue, sqlite3, subprocess, sys, time, traceback

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/bench"))
from lite_contention import sdk

TIMEOUT = 5.0
UPSERT = "INSERT INTO documents(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload"


def payload(worker, sequence, text_bytes=1024):
    token = f"matchedworker{worker}sequence{sequence}"
    prefix = token + " "
    text = (prefix + "searchable churn alpha beta gamma delta " * 40)[:text_bytes]
    assert text.startswith(prefix)
    return {"text": text, "sequence": sequence}


def key(worker, sequence):
    return f"worker-{worker}-{sequence % 128}"


def encoded(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def sqlite_open(path):
    connection = sqlite3.connect(path, timeout=TIMEOUT, isolation_level=None)
    connection.execute("PRAGMA busy_timeout=5000")
    assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        "PRAGMA fullfsync=OFF"
    )  # Matches native Zig's fsync(), not F_FULLFSYNC.
    connection.execute("PRAGMA checkpoint_fullfsync=OFF")
    connection.execute("PRAGMA wal_autocheckpoint=1000")
    assert connection.execute("PRAGMA fullfsync").fetchone()[0] == 0
    assert connection.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 1000
    assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    return connection


def bootstrap(engine, path, indexed, library, workers):
    if engine == "sqlite":
        with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as db:
            assert db.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() == "wal"
            db.execute("PRAGMA synchronous=FULL")
            db.execute(
                "CREATE TABLE documents(id TEXT PRIMARY KEY, payload TEXT NOT NULL, text TEXT GENERATED ALWAYS AS (json_extract(payload,'$.text')) VIRTUAL)"
            )
            if indexed:
                db.executescript("""
CREATE VIRTUAL TABLE search USING fts5(text, content='documents', content_rowid='rowid', tokenize='unicode61');
CREATE TRIGGER document_insert AFTER INSERT ON documents BEGIN
 INSERT INTO search(rowid,text) VALUES(new.rowid,new.text);
END;
CREATE TRIGGER document_delete AFTER DELETE ON documents BEGIN
 INSERT INTO search(search,rowid,text) VALUES('delete',old.rowid,old.text);
END;
CREATE TRIGGER document_update AFTER UPDATE ON documents BEGIN
 INSERT INTO search(search,rowid,text) VALUES('delete',old.rowid,old.text);
 INSERT INTO search(rowid,text) VALUES(new.rowid,new.text);
END;
""")
            db.execute("BEGIN IMMEDIATE")
            try:
                for worker in range(workers):
                    for slot in range(128):
                        db.execute(
                            UPSERT, (key(worker, slot), encoded(payload(worker, -1)))
                        )
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    else:
        af = sdk(library)
        with af.create(path, no_sync=False, busy_timeout=TIMEOUT) as db:
            assert db.delete_index("full_text_index_v0")
            if indexed:
                db.add_index(
                    {
                        "name": "matched_text",
                        "type": "full_text",
                        "config_json": {"field": "text"},
                    }
                )
                assert {index["name"] for index in db.list_indexes()} == {
                    "matched_text"
                }
            for worker in range(workers):
                for start in range(0, 128, 32):
                    db.batch_json(
                        {
                            "inserts": {
                                key(worker, slot): payload(worker, -1)
                                for slot in range(start, start + 32)
                            },
                            "sync_level": "write",
                        }
                    )
            db.run_until_idle()


def worker_main(config, worker, ready, start, begin, stop, results):
    db = None
    result = {
        "worker": worker,
        "successful_calls": 0,
        "busy_calls": 0,
        "latencies_ns": [],
        "error": None,
    }
    try:
        if config["engine"] == "sqlite":
            db = sqlite_open(config["path"])

            def write(document_key, document, request):
                db.execute("BEGIN IMMEDIATE")
                try:
                    db.execute(UPSERT, (document_key, document))
                    db.execute("COMMIT")
                except BaseException:
                    if db.in_transaction:
                        db.execute("ROLLBACK")
                    raise

            busy = (sqlite3.OperationalError,)
        else:
            af = sdk(config["library"])
            db = af.open(config["path"], no_sync=False, busy_timeout=TIMEOUT)

            def write(document_key, document, request):
                db.batch_json(request)

            busy = (af.BusyError,)
        ready.put({"worker": worker, "ready": True})
        assert start.wait(120)
        while time.monotonic() < begin.value:
            time.sleep(0.001)
        result["started"] = time.monotonic()
        deadline = begin.value + config["seconds"]
        sequence = 0
        while not stop.is_set() and time.monotonic() < deadline:
            document_key = key(worker, sequence)
            value = payload(worker, sequence)
            document = encoded(value)
            request = (
                '{"inserts":{'
                + encoded(document_key)
                + ":"
                + document
                + '},"sync_level":'
                + encoded("full_index" if config["indexed"] else "write")
                + "}"
            )
            began = time.perf_counter_ns()
            try:
                write(document_key, document, request)
            except busy as error:
                if config["engine"] == "sqlite" and getattr(
                    error, "sqlite_errorcode", None
                ) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                result["busy_calls"] += 1
                # Absolute upserts are idempotent. The case still fails its
                # zero-Busy gate, including any post-commit visibility refusal.
                continue
            result["latencies_ns"].append(time.perf_counter_ns() - began)
            sequence += 1
        result["successful_calls"] = sequence
        result["ended"] = time.monotonic()
    except BaseException:
        result["error"] = traceback.format_exc()
        stop.set()
        ready.put({"worker": worker, "ready": False})
    finally:
        closing = time.monotonic()
        if db is not None:
            try:
                db.close()
            except BaseException:
                result["close_error"] = traceback.format_exc()
        result["close_seconds"] = time.monotonic() - closing
        results.put(result)


def verify(engine, path, indexed, library, worker_results):
    counts = {row["worker"]: row["successful_calls"] for row in worker_results}
    assert sorted(counts) == list(range(len(worker_results)))
    began = time.monotonic()
    expected_rows = len(counts) * 128
    checked = 0
    if engine == "sqlite":
        db = sqlite_open(path)
        with contextlib.closing(db):
            assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            if indexed:
                db.execute(
                    "INSERT INTO search(search,rank) VALUES('integrity-check',1)"
                )
            assert (
                db.execute("SELECT count(*) FROM documents").fetchone()[0]
                == expected_rows
            )

            def lookup(document_key):
                return json.loads(
                    db.execute(
                        "SELECT payload FROM documents WHERE id=?", (document_key,)
                    ).fetchone()[0]
                )

            def search_count(token):
                return db.execute(
                    "SELECT count(*) FROM search WHERE search MATCH ?", (token,)
                ).fetchone()[0]

            checked = verify_rows(counts, indexed, lookup, search_count)
            assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
    else:
        af = sdk(library)
        with af.open(path, no_sync=False, busy_timeout=TIMEOUT) as db:
            db.run_until_idle()
            assert db.check()["valid"]

            def search_count(token):
                result = db.search(
                    {
                        "full_text_search": {"match": {"field": "text", "text": token}},
                        "limit": 16,
                    }
                )
                return len(result["responses"][0]["hits"]["hits"])

            checked = verify_rows(counts, indexed, db.lookup, search_count)
    return {
        "every_latest_value_checked": True,
        "primary_rows_checked": checked,
        "expected_rows": expected_rows,
        "latest_and_overwritten_tokens_checked": indexed,
        "integrity_passed": True,
        "seconds": time.monotonic() - began,
    }


def verify_rows(counts, indexed, lookup, search_count):
    checked = 0
    for worker, count in counts.items():
        for slot in range(128):
            sequence = count - 1 - ((count - 1 - slot) % 128) if count > slot else -1
            document_key = key(worker, slot)
            assert lookup(document_key) == payload(worker, sequence), (
                worker,
                slot,
                sequence,
            )
            checked += 1
        if indexed and count:
            latest = f"matchedworker{worker}sequence{count - 1}"
            assert search_count(latest) == 1, (latest, "not visible")
            if count > 128:
                assert (
                    search_count(f"matchedworker{worker}sequence{count - 129}") == 0
                ), "overwritten token remains visible"
    return checked


def cleanup(path):
    paths = [p for p in path.parent.glob(path.name + "*") if p.is_file()]
    if not paths:
        return {"removed_bytes": 0}
    identities = {
        p: (p.stat().st_dev, p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns)
        for p in paths
    }
    check = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-Fpn", "--", *map(str, paths)],
        capture_output=True,
        text=True,
    )
    assert (
        check.returncode in (0, 1)
        and not check.stdout.strip()
        and not check.stderr.strip()
    ), "Fixture still open or cannot inspect"
    removed = 0
    for p in paths:
        assert not p.is_symlink()
        info = p.stat()
        assert identities[p] == (
            info.st_dev,
            info.st_ino,
            info.st_size,
            info.st_mtime_ns,
        ), "Fixture changed during cleanup"
        removed += p.stat().st_size
        p.unlink()
    return {"removed_bytes": removed}


def compiler_processes():
    output = subprocess.check_output(["ps", "-axo", "stat,comm"], text=True)
    return [
        line
        for line in output.splitlines()[1:]
        if len(parts := line.split(None, 1)) == 2
        and "T" not in parts[0]
        and (
            Path(parts[1]).name
            in {"zig", "maker", "cargo", "rustc", "clang", "clang++", "ninja"}
        )
    ]


def run_case(config):
    assert not config["path"].exists(), (
        "Reuse fixed fixture filename after successful verification and cleanup"
    )
    bootstrap(
        config["engine"],
        config["path"],
        config["indexed"],
        config["library"],
        config["workers"],
    )
    ctx = mp.get_context("spawn")
    ready, results = ctx.Queue(), ctx.Queue()
    start, stop, begin = ctx.Event(), ctx.Event(), ctx.Value("d", 0)
    processes = [
        ctx.Process(
            target=worker_main,
            args=(config, worker, ready, start, begin, stop, results),
        )
        for worker in range(config["workers"])
    ]
    rows, samples = [], []
    contamination = []
    try:
        for process in processes:
            process.start()
        for _ in processes:
            assert ready.get(timeout=120)["ready"], "Worker could not open database"
        begin.value = time.monotonic() + 0.1
        start.set()
        deadline = time.monotonic() + config["seconds"] + TIMEOUT + 120
        next_inspection = 0
        while len(rows) < len(processes):
            assert time.monotonic() < deadline, "Worker watchdog"
            try:
                rows.append(results.get(timeout=0.1))
            except queue.Empty:
                pass
            samples.append(
                sum(
                    p.stat().st_size
                    for p in config["path"].parent.glob(config["path"].name + "*")
                    if p.is_file()
                )
            )
            if not config["smoke"] and time.monotonic() >= next_inspection:
                contamination.extend(compiler_processes())
                next_inspection = time.monotonic() + 2
        for process in processes:
            process.join(timeout=30)
            assert process.exitcode == 0
    finally:
        stop.set()
        start.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
            if process.is_alive():
                process.kill()
                process.join()
    assert not contamination, "Compiler overlapped timed comparison"
    assert all(not row.get("error") and not row.get("close_error") for row in rows), (
        rows
    )
    verification = verify(
        config["engine"], config["path"], config["indexed"], config["library"], rows
    )
    latencies = sorted(value for row in rows for value in row.pop("latencies_ns"))
    elapsed = max(row["ended"] for row in rows) - min(row["started"] for row in rows)
    result = {
        "engine": config["engine"],
        "indexed": config["indexed"],
        "workers": config["workers"],
        "seconds_requested": config["seconds"],
        "elapsed_seconds": elapsed,
        "successful_calls": len(latencies),
        "successful_calls_per_second": len(latencies) / elapsed,
        "busy_calls": sum(row["busy_calls"] for row in rows),
        "per_worker": sorted(rows, key=lambda row: row["worker"]),
        "latency_p50_ns": latencies[math.ceil(len(latencies) * 0.50) - 1],
        "latency_p95_ns": latencies[math.ceil(len(latencies) * 0.95) - 1],
        "peak_file_bytes": max(samples),
        "final_file_bytes": sum(
            p.stat().st_size
            for p in config["path"].parent.glob(config["path"].name + "*")
            if p.is_file()
        ),
        "verification": verification,
        "load_average": os.getloadavg(),
        "smoke_only": config["smoke"],
    }
    result["all_workers_progressed"] = all(row["successful_calls"] > 0 for row in rows)
    result["zero_busy"] = result["busy_calls"] == 0
    result["qualification_passed"] = (
        result["all_workers_progressed"] and result["zero_busy"]
    )
    result["cleanup"] = cleanup(config["path"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument(
        "--engine", choices=["both", "sqlite", "antfly"], default="both"
    )
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--source-commit", default="unspecified")
    args = parser.parse_args()
    assert (
        args.seconds > 0
        and args.rounds > 0
        and all(worker > 0 for worker in args.workers)
    )
    args.directory.mkdir(parents=True, exist_ok=True)
    assert not args.output.exists()
    library = Path(args.library).resolve()
    library_sha = hashlib.file_digest(library.open("rb"), "sha256").hexdigest()
    script_sha = hashlib.file_digest(Path(__file__).open("rb"), "sha256").hexdigest()
    with sqlite3.connect(":memory:") as metadata_db:
        sqlite_source = metadata_db.execute("SELECT sqlite_source_id()").fetchone()[0]
        sqlite_options = [
            row[0] for row in metadata_db.execute("PRAGMA compile_options")
        ]
    report = {
        "native_source_commit": args.source_commit,
        "sqlite_source_id": sqlite_source,
        "sqlite_compile_options": sqlite_options,
        "cpu_count": os.cpu_count(),
        "status": "running",
        "qualified": False,
        "purpose": "Matched primary-only and synchronous full-text-visible durable churn; not comparable to primary-acknowledged asynchronous indexing rates.",
        "library_sha256": library_sha,
        "script_sha256": script_sha,
        "sqlite_version": sqlite3.sqlite_version,
        "python": sys.version,
        "host": platform.platform(),
        "sqlite_settings": {
            "journal_mode": "wal",
            "synchronous": "FULL",
            "fullfsync": False,
            "checkpoint_fullfsync": False,
            "busy_timeout_ms": 5000,
            "wal_autocheckpoint_pages": 1000,
        },
        "antfly_settings": {
            "no_sync": False,
            "busy_timeout_seconds": TIMEOUT,
            "primary_only_sync_level": "write",
            "indexed_sync_level": "full_index",
        },
        "indexed_field": "text",
        "tokenizer_scope": "Controlled ASCII alphanumeric corpus; no general language/tokenizer equivalence claimed",
        "payload_bytes_text": 1024,
        "documents_per_transaction": 1,
        "keys_per_worker": 128,
        "retention": "One fixed path per engine, removed after closed successful verification; receipts only.",
        "cases": [],
        "documentation": [
            "https://sqlite.org/pragma.html#pragma_synchronous",
            "https://sqlite.org/fts5.html#external_content_tables",
        ],
    }

    def save():
        args.output.write_text(json.dumps(report, indent=2) + "\n")

    save()
    try:
        for indexed in (False, True):
            for workers in args.workers:
                for ordinal in range(args.rounds):
                    engines = (
                        ["antfly", "sqlite"]
                        if ordinal % 2 == 0
                        else ["sqlite", "antfly"]
                    )
                    if args.engine != "both":
                        engines = [args.engine]
                    for engine in engines:
                        assert (
                            hashlib.file_digest(
                                library.open("rb"), "sha256"
                            ).hexdigest()
                            == library_sha
                        )
                        assert (
                            hashlib.file_digest(
                                Path(__file__).open("rb"), "sha256"
                            ).hexdigest()
                            == script_sha
                        )
                        report["current_case"] = {
                            "engine": engine,
                            "indexed": indexed,
                            "workers": workers,
                            "round": ordinal,
                        }
                        save()
                        if not args.smoke:
                            assert not compiler_processes(), (
                                "Compiler active before comparison"
                            )
                        suffix = "aflite" if engine == "antfly" else "db"
                        config = {
                            "engine": engine,
                            "path": args.directory / (engine + "." + suffix),
                            "indexed": indexed,
                            "library": str(library),
                            "workers": workers,
                            "seconds": args.seconds,
                            "smoke": args.smoke,
                        }
                        result = run_case(config)
                        result["round"] = ordinal
                        assert (
                            hashlib.file_digest(
                                library.open("rb"), "sha256"
                            ).hexdigest()
                            == library_sha
                        )
                        report["cases"].append(result)
                        save()
                        print(
                            json.dumps(
                                {
                                    k: v
                                    for k, v in result.items()
                                    if k not in ["per_worker", "verification"]
                                }
                            ),
                            flush=True,
                        )
        report.update(
            status="passed",
            qualified=not args.smoke
            and all(case["qualification_passed"] for case in report["cases"]),
        )
    except BaseException as error:
        report.update(status="failed", error=repr(error))
        save()
        raise
    save()


if __name__ == "__main__":
    main()
