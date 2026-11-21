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

"""Benchmark retained HTTP changefeeds on an isolated native Antfly fixture."""

import argparse
import json
import platform
import statistics
import time
from pathlib import Path

import requests

from conftest import AUTH_BOOTSTRAP_PASSWORD, StatefulAntflyServer


def allocated_bytes(path):
    return sum(
        entry.stat().st_blocks * 512 for entry in path.rglob("*") if entry.is_file()
    )


def call(client, method, url, **options):
    deadline = time.monotonic() + 45
    while True:
        response = client.request(method, url, timeout=15, **options)
        pending = response.status_code == 503 or (
            response.status_code == 409 and "changefeed_retry_required" in response.text
        )
        if not pending or time.monotonic() >= deadline:
            response.raise_for_status()
            return response
        time.sleep(0.1)


def deliver(client, feed, cursor, expected):
    started = time.monotonic()
    deadline = started + 45
    keys = set()
    bytes_read = 0
    while len(keys) < expected and time.monotonic() < deadline:
        reply = call(client, "GET", f"{feed}/events", params={"after": cursor})
        bytes_read += len(reply.content)
        next_cursor = reply.headers["Antfly-Changefeed-Cursor"]
        for event in reply.json()["events"]:
            key = event["key"]["value"]
            if key in keys:
                raise ValueError(f"duplicate event for {key!r}")
            keys.add(key)
        if next_cursor != cursor:
            call(client, "POST", f"{feed}/ack", json={"cursor": next_cursor})
            cursor = next_cursor
        else:
            time.sleep(0.1)
    if len(keys) != expected:
        raise ValueError(f"delivered {len(keys)} rows, expected {expected}")
    return cursor, bytes_read


def run(binary, rows, batch_size, fanout):
    server = StatefulAntflyServer(str(binary), "127.0.0.1", 0, auth_enabled=True)
    try:
        with requests.Session() as client:
            client.auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
            table = f"{server.api_url}/tables/changefeed_bench"
            feeds = f"{server.api_url}/changefeeds"
            call(client, "POST", table, json={"num_shards": 1})
            consumers = []
            for consumer in range(fanout):
                result = call(
                    client,
                    "POST",
                    feeds,
                    json={"tables": ["changefeed_bench"], "include": "rows"},
                    headers={"Idempotency-Key": f"benchmark-{consumer}"},
                ).json()
                consumers.append((f"{feeds}/{result['id']}", result["cursor"]))
            before = allocated_bytes(server.replica_root)
            write_seconds = 0.0
            lag_ms = []
            delivered_bytes = 0
            started = time.monotonic()
            for first in range(0, rows, batch_size):
                batch = {
                    f"row-{index:08d}": {"sequence": index, "data": "x" * 256}
                    for index in range(first, min(first + batch_size, rows))
                }
                write_start = time.monotonic()
                response = client.post(
                    f"{table}/batch", json={"inserts": batch}, timeout=30
                )
                response.raise_for_status()
                committed = time.monotonic()
                write_seconds += committed - write_start
                # Leave the last consumer unacknowledged until restart.
                for index, (feed, cursor) in enumerate(consumers[:-1]):
                    next_cursor, read = deliver(client, feed, cursor, len(batch))
                    lag_ms.append((time.monotonic() - committed) * 1000)
                    delivered_bytes += read
                    consumers[index] = (feed, next_cursor)
            elapsed = time.monotonic() - started
            retained = allocated_bytes(server.replica_root) - before
            restart_seconds = None
            catch_up_seconds = None
            if consumers:
                feed, cursor = consumers[-1]
                restart_start = time.monotonic()
                server.restart()
                restart_seconds = time.monotonic() - restart_start
                catch_up_start = time.monotonic()
                _, read = deliver(client, feed, cursor, rows)
                catch_up_seconds = time.monotonic() - catch_up_start
                delivered_bytes += read
                for feed, _ in consumers:
                    call(client, "DELETE", feed)
            return {
                "consumers": fanout,
                "active_consumers": max(0, fanout - 1),
                "write_rows_per_second": rows / write_seconds,
                "write_and_deliver_rows_per_second": rows / elapsed,
                "acknowledged_batch_lag_ms_median": statistics.median(lag_ms)
                if lag_ms
                else None,
                "acknowledged_batch_lag_ms_max": max(lag_ms, default=None),
                "replica_allocated_bytes_growth_with_stalled_consumer": retained,
                "response_bytes": delivered_bytes,
                "restart_seconds": restart_seconds,
                "stalled_consumer_catch_up_seconds": catch_up_seconds,
            }
    finally:
        server.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--rows", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--fanout", type=int, nargs="+", default=[0, 2, 4])
    args = parser.parse_args()
    if args.rows < 1 or args.batch_size < 1 or any(n < 0 for n in args.fanout):
        parser.error("row and batch counts must be positive and fanout nonnegative")
    report = {
        "platform": platform.platform(),
        "rows": args.rows,
        "batch_size": args.batch_size,
        "results": [
            run(args.binary.resolve(), args.rows, args.batch_size, fanout)
            for fanout in args.fanout
        ],
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
