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

"""Paired full-view consumption: decoded trees versus staged original frames.

One exact binary, warm pinned generation. Includes request, response decoding and
transaction publication; refresh excluded. Python CPU measures the consumer.
Traced peak is a separate untimed allocation probe, not process RSS.
"""

import argparse
import gc
import hashlib
import json
import os
import platform
import statistics
import tempfile
import time
import tracemalloc
from pathlib import Path
from lexical_reuse import signature
from registered import Client
from retained import Payloads, View


def deliver(client, handle, streaming):
    payloads = Payloads() if streaming else None
    _, rows = client.exchange(
        "search",
        revision=client.ready["revision"],
        query_handle=handle,
        delta=True,
        if_result_token="0" * 64,
        consume=payloads.consume if payloads else None,
    )
    view = View(handle)
    view.apply(
        rows, client.ready["revision"], client.ready["observed_at"], payloads=payloads
    )
    return view


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--files", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=32)
    parser.add_argument("--trials", type=int, default=9)
    args = parser.parse_args()
    if min(args.files, args.rounds, args.trials) < 1:
        parser.error("positive sizes required")
    binary = args.binary.resolve()
    report = {
        "measurement": __doc__.strip(),
        "host": platform.platform(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "files": args.files,
        "rounds": args.rounds,
        "trials": [],
    }
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_", "JG_", "GIT_CONFIG"))
    }
    with tempfile.TemporaryDirectory(prefix="grep-consumer-") as tmp:
        root = Path(tmp)
        (root / "src").mkdir()
        for n in range(args.files):
            (root / "src" / f"f{n:06}.txt").write_text("STOP original\n")
        env.update(HOME=tmp, XDG_CONFIG_HOME=tmp)
        client = Client(binary, root, "src", [], env)
        try:
            _, rows = client.exchange("register", args=["STOP", "--threads=2"])
            handle = rows[0]["query_handle"]
            expected = signature(deliver(client, handle, False).materialize())
            assert signature(deliver(client, handle, True).materialize()) == expected
            for trial in range(args.trials):
                result = {"load_average": os.getloadavg()}
                modes = (False, True) if trial % 2 else (True, False)
                for streaming in modes:
                    before = client.wire_bytes
                    cpu, started = time.process_time(), time.monotonic()
                    for _ in range(args.rounds):
                        view = deliver(client, handle, streaming)
                    elapsed, cpu = time.monotonic() - started, time.process_time() - cpu
                    assert signature(view.materialize()) == expected
                    result["streaming" if streaming else "decoded"] = {
                        "seconds_per_request": elapsed / args.rounds,
                        "client_cpu_seconds_per_request": cpu / args.rounds,
                        "bytes_per_request": (client.wire_bytes - before) / args.rounds,
                        "retained_payload_bytes": view.payload_bytes,
                    }
                report["trials"].append(result)
            report["allocation_probe"] = {}
            del view
            for streaming in (False, True):
                gc.collect()
                tracemalloc.start()
                view = deliver(client, handle, streaming)
                live, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                report["allocation_probe"]["streaming" if streaming else "decoded"] = {
                    "live_bytes": live,
                    "peak_bytes": peak,
                }
                assert signature(view.materialize()) == expected
                del view
        finally:
            client.close()
    report["median"] = {
        name: {
            key: statistics.median(t[name][key] for t in report["trials"])
            for key in report["trials"][0][name]
        }
        for name in ("decoded", "streaming")
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("median", "allocation_probe")}, indent=2))


if __name__ == "__main__":
    main()
