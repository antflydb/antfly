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
import platform, queue, shutil, sqlite3, subprocess, sys, threading, time, traceback

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/bench"))
from lite_contention import sdk

TIMEOUT = 5.0
UPSERT = "INSERT INTO documents(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload"


# Counts durability syscalls in every image of a worker process, the same way
# for both engines. Preloaded into each worker process; see `sync_counter`.
SYNC_COUNTER_SOURCE = r"""
#include <fcntl.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <unistd.h>
static _Atomic unsigned long long antfly_bench_syncs;
unsigned long long antfly_bench_sync_count(void) { return atomic_load(&antfly_bench_syncs); }
static int is_sync_command(int cmd) {
#ifdef F_FULLFSYNC
    if (cmd == F_FULLFSYNC) return 1;
#endif
#ifdef F_BARRIERFSYNC
    if (cmd == F_BARRIERFSYNC) return 1;
#endif
    return 0;
}
#ifdef __APPLE__
extern int fdatasync(int);
static int counted_fsync(int fd) { atomic_fetch_add(&antfly_bench_syncs, 1); return fsync(fd); }
static int counted_fdatasync(int fd) { atomic_fetch_add(&antfly_bench_syncs, 1); return fdatasync(fd); }
static int counted_fcntl(int fd, int cmd, ...) {
    va_list args; va_start(args, cmd); long arg = va_arg(args, long); va_end(args);
    if (is_sync_command(cmd)) atomic_fetch_add(&antfly_bench_syncs, 1);
    return fcntl(fd, cmd, arg);
}
__attribute__((used, section("__DATA,__interpose"))) static struct { const void *replacement, *original; } interposers[] = {
    { (const void *)counted_fsync, (const void *)fsync },
    { (const void *)counted_fdatasync, (const void *)fdatasync },
    { (const void *)counted_fcntl, (const void *)fcntl },
};
#else
#define _GNU_SOURCE
#include <dlfcn.h>
int fsync(int fd) { static int (*real)(int); if (!real) real = dlsym(RTLD_NEXT, "fsync"); atomic_fetch_add(&antfly_bench_syncs, 1); return real(fd); }
int fdatasync(int fd) { static int (*real)(int); if (!real) real = dlsym(RTLD_NEXT, "fdatasync"); atomic_fetch_add(&antfly_bench_syncs, 1); return real(fd); }
int fcntl(int fd, int cmd, ...) {
    static int (*real)(int, int, ...); if (!real) real = dlsym(RTLD_NEXT, "fcntl");
    va_list args; va_start(args, cmd); long arg = va_arg(args, long); va_end(args);
    if (is_sync_command(cmd)) atomic_fetch_add(&antfly_bench_syncs, 1);
    return real(fd, cmd, arg);
}
#endif
"""
PRELOAD_VARIABLE = "DYLD_INSERT_LIBRARIES" if sys.platform == "darwin" else "LD_PRELOAD"


def build_sync_counter(directory):
    """Compiles the sync-counting shim, or returns None without a C compiler."""
    compiler = shutil.which("cc")
    if compiler is None:
        return None
    source = directory / "sync_counter.c"
    output = directory / (
        "sync_counter.dylib" if sys.platform == "darwin" else "sync_counter.so"
    )
    source.write_text(SYNC_COUNTER_SOURCE)
    command = [compiler, "-shared", "-fPIC", "-O2", "-o", str(output), str(source)]
    if sys.platform != "darwin":
        command.append("-ldl")
    subprocess.run(command, check=True)
    return output


def sync_counter(config):
    """The preloaded shim's process-wide counter, or None when not loaded."""
    if not config.get("sync_counter") or config["sync_counter"] not in os.environ.get(
        PRELOAD_VARIABLE, ""
    ):
        return None
    import ctypes

    function = ctypes.CDLL(config["sync_counter"]).antfly_bench_sync_count
    function.restype = ctypes.c_ulonglong
    return function


CALIBRATION_WRITES = 16


def calibration_main(engine, library, path, counter_path, results):
    """Counts the shim's syncs across sequential durable writes by one writer.
    Each must issue at least one durability barrier; fewer observed means
    this engine syncs through a path the shim cannot see (Linux io_uring
    submits FSYNC without a libc call)."""
    config = {"sync_counter": counter_path}
    syncs = sync_counter(config)
    if syncs is None:
        results.put(None)
        return
    if engine == "sqlite":
        bootstrap("sqlite", path, False, None, 1)
        with contextlib.closing(sqlite_open(path)) as db:
            before = syncs()
            for sequence in range(CALIBRATION_WRITES):
                db.execute("BEGIN IMMEDIATE")
                db.execute(UPSERT, (key(0, sequence), encoded(payload(0, sequence))))
                db.execute("COMMIT")
            results.put(syncs() - before)
    else:
        af = sdk(library)
        with af.create(path, no_sync=False, busy_timeout=TIMEOUT) as db:
            before = syncs()
            for sequence in range(CALIBRATION_WRITES):
                db.batch_json(
                    {
                        "inserts": {key(0, sequence): payload(0, sequence)},
                        "sync_level": "write",
                    }
                )
            results.put(syncs() - before)


def sync_counter_observes(engine, library, directory, counter):
    """Whether the preloaded counter sees this engine's durability barriers."""
    if counter is None:
        return False
    path = (
        directory
        / f"sync-calibration-{engine}.{'db' if engine == 'sqlite' else 'aflite'}"
    )
    for stale in directory.glob(path.name + "*"):
        stale.unlink()
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    process = ctx.Process(
        target=calibration_main,
        args=(engine, str(library), path, str(counter), results),
    )
    preload = os.environ.get(PRELOAD_VARIABLE)
    os.environ[PRELOAD_VARIABLE] = str(counter)
    try:
        process.start()
    finally:
        if preload is None:
            os.environ.pop(PRELOAD_VARIABLE, None)
        else:
            os.environ[PRELOAD_VARIABLE] = preload
    observed = results.get(timeout=300)
    process.join(timeout=60)
    for done in directory.glob(path.name + "*"):
        done.unlink()
    return observed is not None and observed >= CALIBRATION_WRITES


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


# Once every worker's timed window has ended and the timed state is verified,
# each full-text worker makes this many more writes and searches each on its
# own handle as soon as it is acknowledged: full_index must already make it
# visible, before any later write, drain or close could. Behind a barrier, so
# probes neither contend with timed writes nor enter their sync count.
VISIBILITY_PROBES = 8


def worker_main(
    config,
    worker,
    ready,
    start,
    begin,
    stop,
    results,
    probe_start,
    probes_done,
    release,
    closes,
):
    db = None
    result = {
        "worker": worker,
        "successful_calls": 0,
        "busy_calls": 0,
        "latencies_ns": [],
        "error": None,
    }
    syncs = sync_counter(config)
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

            def search_keys(token):
                return sorted(
                    row[0]
                    for row in db.execute(
                        "SELECT documents.id FROM search JOIN documents ON documents.rowid=search.rowid WHERE search MATCH ?",
                        (token,),
                    )
                )
        else:
            af = sdk(config["library"])
            db = af.open(config["path"], no_sync=False, busy_timeout=TIMEOUT)
            batch_path = config["batch_path"]

            if batch_path == "wire":

                def write(document_key, document, request):
                    db.batch_wire(request)
            else:

                def write(document_key, document, request):
                    db.batch_json(request)

            busy = (af.BusyError,)

            def search_keys(token):
                found = db.search(
                    {
                        "full_text_search": {"match": {"field": "text", "text": token}},
                        "limit": 16,
                    }
                )
                return sorted(
                    hit["_id"] for hit in found["responses"][0]["hits"]["hits"]
                )

        ready.put({"worker": worker, "ready": True})
        assert start.wait(120)
        while time.monotonic() < begin.value:
            time.sleep(0.001)
        result["started"] = time.monotonic()
        if syncs:
            result["syncs_started"] = syncs()
        deadline = begin.value + config["seconds"]
        sync_level = "full_index" if config["indexed"] else "write"

        def build(sequence):
            document_key = key(worker, sequence)
            document = encoded(payload(worker, sequence))
            if config["engine"] != "sqlite" and config["batch_path"] == "wire":
                # Built outside the timed section, matching the JSON request
                # string below: only the engine's batch-apply cost is timed,
                # not request encoding.
                request = af.encode_batch_wire(
                    [(document_key, document.encode("utf-8"))], sync_level
                )
            else:
                request = (
                    '{"inserts":{'
                    + encoded(document_key)
                    + ":"
                    + document
                    + '},"sync_level":'
                    + encoded(sync_level)
                    + "}"
                )
            return document_key, document, request

        sequence = 0
        while not stop.is_set() and time.monotonic() < deadline:
            document_key, document, request = build(sequence)
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
        result["timed_calls"] = sequence
        result["ended"] = time.monotonic()
        if syncs:
            result["syncs_ended"] = syncs()
        result["successful_calls"] = sequence

        def probe(sequence):
            done = {
                "worker": worker,
                "visibility_samples": 0,
                "visibility_failures": [],
                "probe_writes": 0,
                "error": None,
            }
            while done["probe_writes"] < VISIBILITY_PROBES and not stop.is_set():
                document_key, document, request = build(sequence)
                try:
                    write(document_key, document, request)
                except busy as error:
                    if config["engine"] == "sqlite" and getattr(
                        error, "sqlite_errorcode", None
                    ) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                        raise
                    continue  # Another worker's probe holds the writer.
                token = f"matchedworker{worker}sequence{sequence}"
                found = search_keys(token)
                done["visibility_samples"] += 1
                if found != [document_key]:
                    done["visibility_failures"].append({"token": token, "found": found})
                done["probe_writes"] += 1
                sequence += 1
            return done
    except BaseException:
        result["error"] = traceback.format_exc()
        stop.set()
        ready.put({"worker": worker, "ready": False})
    finally:
        # Verification runs while this handle stays open: closing first could
        # sync or drain a visibility failure away before it is checked.
        results.put(result)
        probed = {
            "worker": worker,
            "visibility_samples": 0,
            "visibility_failures": [],
            "probe_writes": 0,
            "error": None,
        }
        # Probes start once every timed write has ended and the timed state
        # has been verified.
        probe_start.wait(TIMEOUT + 600)
        if config["indexed"] and result["error"] is None and not stop.is_set():
            try:
                probed = probe(result["successful_calls"])
            except BaseException:
                probed["error"] = traceback.format_exc()
                stop.set()
        probes_done.put(probed)
        release.wait(TIMEOUT + 600)
        closing = time.monotonic()
        closed = {"worker": worker, "close_error": None}
        if db is not None:
            try:
                db.close()
            except BaseException:
                closed["close_error"] = traceback.format_exc()
        closed["close_seconds"] = time.monotonic() - closing
        closes.put(closed)


def thread_group_main(
    config,
    workers,
    ready,
    start,
    begin,
    stop,
    results,
    probe_start,
    probes_done,
    release,
    closes,
):
    """Runs every worker as a thread of one process, each with its own
    connection, so they share the engine's in-process group commit."""
    threads = [
        threading.Thread(
            target=worker_main,
            args=(
                config,
                worker,
                ready,
                start,
                begin,
                stop,
                results,
                probe_start,
                probes_done,
                release,
                closes,
            ),
        )
        for worker in workers
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def timed_syncs(config, rows):
    """Durability syscalls issued inside the timed window, or None."""
    if any("syncs_started" not in row or "syncs_ended" not in row for row in rows):
        return None
    if config["worker_model"] == "process":
        return sum(row["syncs_ended"] - row["syncs_started"] for row in rows)
    # One process-wide counter: from the first start to the last end.
    return max(row["syncs_ended"] for row in rows) - min(
        row["syncs_started"] for row in rows
    )


def verify(engine, path, indexed, library, worker_results, final=True):
    counts = {row["worker"]: row["successful_calls"] for row in worker_results}
    assert sorted(counts) == list(range(len(worker_results)))
    began = time.monotonic()
    expected_rows = len(counts) * 128
    checked = 0
    if engine == "sqlite":
        db = sqlite_open(path)
        with contextlib.closing(db):
            if final:
                assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            if final and indexed:
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

            def search_keys(token):
                return sorted(
                    row[0]
                    for row in db.execute(
                        "SELECT documents.id FROM search JOIN documents ON documents.rowid=search.rowid WHERE search MATCH ?",
                        (token,),
                    )
                )

            checked = verify_rows(counts, indexed, lookup, search_keys)
            if final:
                assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
    else:
        af = sdk(library)
        with af.open(path, no_sync=False, busy_timeout=TIMEOUT) as db:

            def search_keys(token):
                result = db.search(
                    {
                        "full_text_search": {"match": {"field": "text", "text": token}},
                        "limit": 16,
                    }
                )
                return sorted(
                    hit["_id"] for hit in result["responses"][0]["hits"]["hits"]
                )

            # full_index acknowledged every write as searchable: check before
            # any drain, which could repair a missed acknowledgement.
            checked = verify_rows(counts, indexed, db.lookup, search_keys)
            if final:
                db.run_until_idle()
                assert db.check()["valid"]
    return {
        "every_latest_value_checked": True,
        "primary_rows_checked": checked,
        "expected_rows": expected_rows,
        "live_tokens_checked": sum(min(count, 128) for count in counts.values())
        if indexed
        else 0,
        "overwritten_tokens_checked": sum(
            max(0, min(count, 256) - 128) for count in counts.values()
        )
        if indexed
        else 0,
        "integrity_passed": True,
        "seconds": time.monotonic() - began,
    }


def slot_sequence(count, slot):
    """Latest sequence written to `slot`, or -1 for the bootstrap value."""
    return count - 1 - ((count - 1 - slot) % 128) if count > slot else -1


def verify_rows(counts, indexed, lookup, search_keys):
    checked = 0
    for worker, count in counts.items():
        for slot in range(128):
            sequence = slot_sequence(count, slot)
            document_key = key(worker, slot)
            assert lookup(document_key) == payload(worker, sequence), (
                worker,
                slot,
                sequence,
            )
            checked += 1
    if not indexed:
        return checked
    # Every live document, by its own key, then the generation each slot
    # overwrote: a lost full-text delete leaves that token visible.
    for worker, count in counts.items():
        for slot in range(min(count, 128)):
            sequence = slot_sequence(count, slot)
            document_key = key(worker, slot)
            token = f"matchedworker{worker}sequence{sequence}"
            found = search_keys(token)
            assert found == [document_key], (token, document_key, found, "not visible")
            if sequence >= 128:
                stale = f"matchedworker{worker}sequence{sequence - 128}"
                found = search_keys(stale)
                assert found == [], (stale, found, "overwritten token remains visible")
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
    ), (
        "Fixture still open or cannot inspect",
        check.stdout,
        check.stderr,
        subprocess.run(
            [
                "ps",
                "-o",
                "pid,ppid,command",
                "-p",
                ",".join(
                    line[1:] for line in check.stdout.split() if line.startswith("p")
                )
                or "0",
            ],
            capture_output=True,
            text=True,
        ).stdout,
    )
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
    ready, results, probes_done, closes = (
        ctx.Queue(),
        ctx.Queue(),
        ctx.Queue(),
        ctx.Queue(),
    )
    probe_start = ctx.Event()
    start, stop, begin, release = (
        ctx.Event(),
        ctx.Event(),
        ctx.Value("d", 0),
        ctx.Event(),
    )
    if config["worker_model"] == "thread":
        processes = [
            ctx.Process(
                target=thread_group_main,
                args=(
                    config,
                    list(range(config["workers"])),
                    ready,
                    start,
                    begin,
                    stop,
                    results,
                    probe_start,
                    probes_done,
                    release,
                    closes,
                ),
            )
        ]
    else:
        processes = [
            ctx.Process(
                target=worker_main,
                args=(
                    config,
                    worker,
                    ready,
                    start,
                    begin,
                    stop,
                    results,
                    probe_start,
                    probes_done,
                    release,
                    closes,
                ),
            )
            for worker in range(config["workers"])
        ]
    preload = os.environ.get(PRELOAD_VARIABLE)
    rows, samples, closed = [], [], []
    contamination = []
    live_verification = None
    try:
        if config.get("sync_counter"):
            os.environ[PRELOAD_VARIABLE] = config["sync_counter"]
        try:
            for process in processes:
                process.start()
        finally:
            if preload is None:
                os.environ.pop(PRELOAD_VARIABLE, None)
            else:
                os.environ[PRELOAD_VARIABLE] = preload
        for _ in range(config["workers"]):
            assert ready.get(timeout=120)["ready"], "Worker could not open database"
        begin.value = time.monotonic() + 0.1
        start.set()
        deadline = time.monotonic() + config["seconds"] + TIMEOUT + 120
        next_inspection = 0
        while len(rows) < config["workers"]:
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
        assert all(not row.get("error") for row in rows), rows
        # Writers still hold their handles: nothing their close would sync or
        # drain has happened yet.
        live_verification = verify(
            config["engine"],
            config["path"],
            config["indexed"],
            config["library"],
            rows,
            final=False,
        )
        # Every timed write has ended and its state is verified: probes now
        # neither contend with timed writes nor overwrite unverified keys.
        probe_start.set()
        probed = []
        while len(probed) < config["workers"]:
            assert time.monotonic() < deadline + TIMEOUT + 60, "Worker probe watchdog"
            probed.append(probes_done.get(timeout=TIMEOUT + 60))
        assert all(not row["error"] for row in probed), probed
        for row in rows:
            done = next(p for p in probed if p["worker"] == row["worker"])
            row["visibility_samples"] = done["visibility_samples"]
            row["visibility_failures"] = done["visibility_failures"]
            row["successful_calls"] += done["probe_writes"]
        release.set()
        while len(closed) < config["workers"]:
            assert time.monotonic() < deadline + TIMEOUT + 60, "Worker close watchdog"
            closed.append(closes.get(timeout=TIMEOUT + 60))
        for process in processes:
            process.join(timeout=30)
            assert process.exitcode == 0
    finally:
        stop.set()
        start.set()
        probe_start.set()
        release.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
            if process.is_alive():
                process.kill()
                process.join()
    # On a dedicated quiet host this would be fatal contamination. On a
    # shared dev host with other agents' builds running continuously, a
    # hard assert here makes the comparison unrunnable rather than noisy.
    # Record it as a diagnostic (and in load_average, already sampled
    # below) instead of aborting; treat the within-run SQLite-vs-Lite
    # ratio, not absolute rates, as the signal when this is non-empty.
    compiler_contamination_samples = len(contamination)
    assert all(not row.get("close_error") for row in closed), closed
    for row in rows:
        row["close_seconds"] = next(
            c["close_seconds"] for c in closed if c["worker"] == row["worker"]
        )
    verification = verify(
        config["engine"], config["path"], config["indexed"], config["library"], rows
    )
    verification["checked_with_writers_open"] = live_verification is not None
    latencies = sorted(value for row in rows for value in row.pop("latencies_ns"))
    elapsed = max(row["ended"] for row in rows) - min(row["started"] for row in rows)
    syncs = timed_syncs(config, rows)
    result = {
        "engine": config["engine"],
        "batch_path": config.get("batch_path"),
        "worker_model": config["worker_model"],
        "timed_syncs": syncs,
        "syncs_per_write": syncs / len(latencies)
        if syncs is not None and latencies
        else None,
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
        "compiler_contamination_samples": compiler_contamination_samples,
    }
    result["all_workers_progressed"] = all(row["timed_calls"] > 0 for row in rows)
    result["zero_busy"] = result["busy_calls"] == 0
    result["visibility_samples"] = sum(row["visibility_samples"] for row in rows)
    result["visibility_failures"] = [
        f for row in rows for f in row["visibility_failures"]
    ]
    result["visibility_probed_every_worker"] = not config["indexed"] or all(
        row["visibility_samples"] >= VISIBILITY_PROBES for row in rows
    )
    result["qualification_passed"] = (
        result["all_workers_progressed"]
        and result["zero_busy"]
        and result["visibility_probed_every_worker"]
        and not result["visibility_failures"]
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
    parser.add_argument(
        "--batch-path",
        choices=["both", "json", "wire"],
        default="both",
        help="For the antfly engine: request encoding to measure. 'json' is "
        "antfly_db_batch_json (JSON object); 'wire' is antfly_db_batch_wire "
        "(the length-prefixed binary format); 'both' measures each as a "
        "separate case. No effect on the sqlite engine.",
    )
    parser.add_argument(
        "--worker-model",
        choices=["both", "process", "thread"],
        default="both",
        help="'process' runs each worker as its own OS process (cross-process "
        "lease contention); 'thread' runs every worker as a thread of one "
        "process, each with its own connection, which shares in-process group "
        "commit. 'both' measures each as a separate case.",
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
    counter = build_sync_counter(args.directory)
    library = Path(args.library).resolve()
    counter_observes = {
        engine: sync_counter_observes(engine, library, args.directory, counter)
        for engine in (["antfly", "sqlite"] if args.engine == "both" else [args.engine])
    }
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
        "sync_counter_observes": counter_observes,
        "sync_counter": "fsync, fdatasync and F_FULLFSYNC/F_BARRIERFSYNC calls in the worker processes during the timed window"
        if counter
        else None,
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
                        # Only the antfly engine has a batch_path choice
                        # (JSON vs the binary wire format); sqlite's write
                        # path doesn't change. "both" measures json and wire
                        # as two separate cases in the same round.
                        batch_paths = (
                            [None]
                            if engine == "sqlite"
                            else (
                                ["json", "wire"]
                                if args.batch_path == "both"
                                else [args.batch_path]
                            )
                        )
                        worker_models = (
                            ["process", "thread"]
                            if args.worker_model == "both"
                            else [args.worker_model]
                        )
                        for batch_path, worker_model in (
                            (b, m) for b in batch_paths for m in worker_models
                        ):
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
                                "batch_path": batch_path,
                                "worker_model": worker_model,
                                "indexed": indexed,
                                "workers": workers,
                                "round": ordinal,
                            }
                            save()
                            # See run_case's compiler_contamination_samples: on a
                            # shared dev host this is routinely nonzero from
                            # other agents' builds. Record it rather than
                            # blocking the run; load_average plus the
                            # within-round SQLite-vs-Lite ratio are the signal.
                            report["current_case"]["pre_run_compiler_processes"] = len(
                                compiler_processes()
                            )
                            suffix = "aflite" if engine == "antfly" else "db"
                            path_tag = f"-{batch_path}" if batch_path else ""
                            config = {
                                "engine": engine,
                                "batch_path": batch_path,
                                "worker_model": worker_model,
                                "sync_counter": str(counter)
                                if counter and counter_observes[engine]
                                else None,
                                "path": args.directory
                                / (engine + path_tag + "." + suffix),
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
