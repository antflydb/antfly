# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run a Windows Antfly Lite HTTP smoke test in an isolated CrossOver bottle.

The bottle must already exist. Logs and the database are retained in --out.
This checks Wine behavior, not native NTFS crash durability.
"""

import argparse
import concurrent.futures
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from durability_probe import verify, write


def windows_path(path: Path) -> str:
    return "Z:" + str(path.resolve()).replace("/", "\\")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--bottle", required=True)
    parser.add_argument("--bottle-path", required=True, type=Path)
    parser.add_argument("--durability-count", type=int, default=0)
    parser.add_argument(
        "--wine",
        default="/Applications/CrossOver.app/Contents/SharedSupport/CrossOver/bin/wine",
    )
    args = parser.parse_args()
    if args.durability_count < 0:
        parser.error("durability-count must be nonnegative")
    if not args.binary.is_file():
        parser.error(f"Windows executable does not exist: {args.binary}")
    args.out.mkdir(parents=True, exist_ok=False)
    env = {**os.environ, "CX_BOTTLE_PATH": str(args.bottle_path.resolve())}
    wine = [args.wine, "--bottle", args.bottle, "--no-update", "--no-gui"]
    binary = windows_path(args.binary)
    database = windows_path(args.out / "database with spaces.aflite")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"

    def request(method: str, path: str, payload=None):
        body = None if payload is None else json.dumps(payload).encode()
        req = urllib.request.Request(
            base + path,
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as response:
            return json.load(response)

    def search():
        result = request(
            "POST",
            "/db/v1/tables/windows_smoke/query",
            {
                "full_text_search": {"match": "crossover", "field": "body"},
                "limit": 10,
            },
        )
        ids = {hit["_id"] for hit in result["responses"][0]["hits"]["hits"]}
        if ids != {"alpha", "beta"}:
            raise AssertionError(result)
        return result

    command = [
        *wine,
        binary,
        "standalone",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--health",
        "false",
        "--storage-engine",
        "lite",
        "--storage-path",
        database,
        "--data-dir",
        windows_path(args.out / "runtime data"),
        "--fsync",
        "true",
    ]
    for run in range(2):
        with (args.out / f"server-{run}.log").open("wb") as log:
            server = subprocess.Popen(command, env=env, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 180
                while True:
                    try:
                        request("GET", "/readyz")
                        break
                    except (urllib.error.URLError, TimeoutError):
                        if server.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError(
                                f"Server failed to become ready; see {log.name}"
                            ) from None
                        time.sleep(0.5)
                if run == 0:
                    request("POST", "/db/v1/tables/windows_smoke", {})
                    request(
                        "POST",
                        "/db/v1/tables/windows_smoke/batch",
                        {
                            "inserts": {
                                "alpha": {"body": "crossover alpha"},
                                "beta": {"body": "crossover beta"},
                                "other": {"body": "unrelated document"},
                            },
                            "sync_level": "full_index",
                        },
                    )
                search()
                if args.durability_count:
                    ledger = args.out / "acknowledged.jsonl"
                    if run == 0:
                        write(base, ledger, args.durability_count, 4096)
                    else:
                        verify(base, ledger)
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    list(pool.map(lambda _: search(), range(32)))
                print(
                    f"run {run}: full-text and 32 concurrent queries passed", flush=True
                )
            finally:
                if server.poll() is None:
                    subprocess.run(
                        [*wine, "taskkill", "/F", "/IM", args.binary.name],
                        env=env,
                        check=True,
                        timeout=30,
                        stdout=subprocess.DEVNULL,
                    )
                server.wait(timeout=30)
    subprocess.run(
        [*wine, binary, "lite", "check", database], env=env, check=True, timeout=60
    )
    print(f"CrossOver smoke passed; logs and database: {args.out}")


if __name__ == "__main__":
    main()
