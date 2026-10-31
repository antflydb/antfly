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

"""Require automatic full-text visibility while peer writers remain active."""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import queue
import resource
import time
import traceback
from pathlib import Path

from lite_contention import sdk, seed


def worker(library, database, index, ready, start, stop, counts, results):
    announced = False
    try:
        af = sdk(library)
        with af.open(database, busy_timeout=5.0) as db:
            ready.put({"index": index, "status": "ok"})
            announced = True
            if not start.wait(90):
                raise RuntimeError("start barrier timed out")
            deadline = time.monotonic() + 120
            sequence = busy = successful = 0
            last_write = None
            while not stop.is_set() and time.monotonic() < deadline:
                key = f"live-worker-{index}-{sequence % 128}"
                try:
                    db.batch_json(
                        {"inserts": {key: {"text": "v" * 1024, "sequence": sequence}}}
                    )
                    successful += 1
                    last_write = {"key": key, "sequence": sequence}
                    with counts.get_lock():
                        counts[index] += 1
                except af.BusyError:
                    busy += 1
                sequence += 1
        results.put(
            {
                "index": index,
                "status": "ok",
                "successful_calls": successful,
                "busy_calls": busy,
                "last_write": last_write,
            }
        )
    except BaseException:  # noqa: BLE001 - always report child failures
        failure = {
            "index": index,
            "status": "error",
            "diagnostic": traceback.format_exc(),
        }
        if not announced:
            ready.put(failure)
        results.put(failure)


def snapshot(counts):
    with counts.get_lock():
        return list(counts)


def run(library: str, directory: Path, workers: int, timeout: float) -> None:
    af = sdk(library)
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / "database.aflite"
    seed(library, database, 512, False)
    context = mp.get_context("spawn")
    ready, results = context.Queue(), context.Queue()
    start, stop = context.Event(), context.Event()
    counts = context.Array("Q", workers, lock=True)
    processes = [
        context.Process(
            target=worker,
            args=(library, str(database), i, ready, start, stop, counts, results),
        )
        for i in range(workers)
    ]
    canaries, observations, completed = [], [], []
    # A read-only observer cannot manually advance the index it is checking.
    with (
        af.open(database, busy_timeout=5.0) as writer,
        af.open(database, mode=af.OpenMode.READONLY, busy_timeout=5.0) as observer,
    ):
        writer.run_until_idle()
        try:
            for process in processes:
                process.start()
            for _ in processes:
                item = ready.get(timeout=90)
                if item["status"] != "ok":
                    raise RuntimeError(item)
            start.set()
            for round_index in range(2):
                before = snapshot(counts)
                nonce = time.time_ns()
                key, term = f"live-canary-{nonce}", f"livecanary{nonce}"
                writer.batch_json({"inserts": {key: {"text": term, "nonce": nonce}}})
                target = writer.pending_work_stats()["derived_target_sequence"]
                assert target > 0
                canaries.append({"key": key, "term": term, "nonce": nonce})
                began = time.monotonic()
                deadline = began + timeout
                request = {
                    "full_text_search": {"match": {"field": "text", "text": term}},
                    "limit": 10,
                }
                visible = False
                applied = 0
                while time.monotonic() < deadline:
                    if not all(process.is_alive() for process in processes):
                        raise RuntimeError(
                            "a writer exited before the live visibility check"
                        )
                    try:
                        visible = key in json.dumps(observer.search(request))
                        debts = observer.pending_work_stats()["index_replay_debt"]
                        full_text = [
                            d for d in debts if d["index_name"] == "full_text_index_v0"
                        ]
                        if full_text:
                            applied = min(d["applied_sequence"] for d in full_text)
                    except af.BusyError:
                        pass
                    after = snapshot(counts)
                    if (
                        visible
                        and applied >= target
                        and all(a > b for a, b in zip(after, before, strict=True))
                    ):
                        break
                    time.sleep(0.02)
                else:
                    raise RuntimeError(
                        {
                            "reason": "live index or writer progress stalled",
                            "visible": visible,
                            "before": before,
                            "after": snapshot(counts),
                        }
                    )
                observations.append(
                    {
                        "round": round_index,
                        "visibility_seconds": time.monotonic() - began,
                        "target_sequence": target,
                        "applied_sequence": applied,
                        "per_worker_progress": [
                            a - b for a, b in zip(after, before, strict=True)
                        ],
                    }
                )
        finally:
            stop.set()
            start.set()
            for process in processes:
                if process.pid is None:
                    continue
                process.join(timeout=10)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            finish_deadline = time.monotonic() + 5
            while len(completed) < workers and time.monotonic() < finish_deadline:
                try:
                    completed.append(
                        results.get(
                            timeout=max(0.01, finish_deadline - time.monotonic())
                        )
                    )
                except queue.Empty:
                    break
            ready.close()
            results.close()
    assert len(completed) == workers and all(
        row["status"] == "ok" for row in completed
    ), completed
    assert all(process.exitcode == 0 for process in processes)
    completed.sort(key=lambda row: row["index"])
    with af.open(database, busy_timeout=5.0) as db:
        db.run_until_idle()
        for canary in canaries:
            request = {
                "full_text_search": {
                    "match": {"field": "text", "text": canary["term"]}
                },
                "limit": 10,
            }
            assert canary["key"] in json.dumps(db.search(request))
            assert db.lookup(canary["key"])["nonce"] == canary["nonce"]
        for row in completed:
            last = row["last_write"]
            assert (
                last is not None
                and db.lookup(last["key"])["sequence"] == last["sequence"]
            )
        integrity = db.check()
        assert isinstance(integrity, dict) and integrity.get("valid") is True, integrity
    print(
        json.dumps(
            {
                "kind": "indexed_during_load",
                "status": "ok",
                "workers": workers,
                "observations": observations,
                "worker_results": completed,
                "integrity": integrity,
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--workers", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.timeout <= 0 or any(n < 1 or n > 128 for n in args.workers):
        parser.error("timeout must be positive; workers must be between 1 and 128")
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    maximum = 10240 if hard == resource.RLIM_INFINITY else min(hard, 10240)
    resource.setrlimit(resource.RLIMIT_NOFILE, (max(soft, maximum), hard))
    for count in args.workers:
        run(args.library, args.directory / f"p{count}", count, args.timeout)
