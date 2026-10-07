# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Record acknowledgments off the test VM, then verify them after an abrupt reset.

Run this controller on a separate machine, with an HTTP tunnel to an isolated
Antfly Windows server. It never resets machines itself. A process kill tests
recovery; an abrupt hypervisor reset additionally tests loss of the guest cache.
Neither establishes physical disk/controller power-loss behavior.
"""

import argparse
import hashlib
import json
import os
import time
import urllib.request
import uuid
from pathlib import Path


def request(url: str, method: str, path: str, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        url.rstrip("/") + path,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        return json.load(response)


def record(ledger, value: dict) -> None:
    ledger.write(json.dumps(value, sort_keys=True) + "\n")
    ledger.flush()
    os.fsync(ledger.fileno())


def write(url: str, ledger_path: Path, count: int, payload_bytes: int) -> None:
    table = "windows_durability_" + uuid.uuid4().hex
    # Exclusive creation prevents accidentally overwriting the recovery oracle.
    with ledger_path.open("x", encoding="utf-8") as ledger:
        record(ledger, {"format": 1, "table": table, "started": time.time()})
        request(url, "POST", f"/db/v1/tables/{table}", {})
        index = 0
        while count == 0 or index < count:
            key = f"ack{index:08d}"
            body = key + " " + "padding " * ((payload_bytes + 7) // 8)
            request(
                url,
                "POST",
                f"/db/v1/tables/{table}/batch",
                {"inserts": {key: {"body": body}}, "sync_level": "full_index"},
            )
            # Only successful full_index responses enter the oracle. An
            # interrupted request may commit and is deliberately not required.
            record(
                ledger,
                {"key": key, "sha256": hashlib.sha256(body.encode()).hexdigest()},
            )
            index += 1
            print(f"acknowledged {index}: {key}", flush=True)


def verify(url: str, ledger_path: Path) -> int:
    with ledger_path.open(encoding="utf-8") as ledger:
        header = json.loads(next(ledger))
        if header.get("format") != 1:
            raise ValueError("Unsupported acknowledgment ledger format")
        table = header["table"]
        count = 0
        for line in ledger:
            ack = json.loads(line)
            key = ack["key"]
            doc = request(url, "GET", f"/db/v1/tables/{table}/documents/{key}")
            if hashlib.sha256(doc["body"].encode()).hexdigest() != ack["sha256"]:
                raise AssertionError(f"Acknowledged document changed: {key}")
            result = request(
                url,
                "POST",
                f"/db/v1/tables/{table}/query",
                {"full_text_search": {"match": key, "field": "body"}, "limit": 10},
            )
            ids = {hit["_id"] for hit in result["responses"][0]["hits"]["hits"]}
            if ids != {key}:
                raise AssertionError(f"Acknowledged index entry missing: {key}: {ids}")
            count += 1
        if count == 0:
            raise ValueError("Ledger contains no acknowledged writes")
    print(f"Verified {count} acknowledged documents and full-text entries", flush=True)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("write", "verify"))
    parser.add_argument("--url", required=True)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--count", type=int, default=1000, help="0 writes until reset")
    parser.add_argument("--payload-bytes", type=int, default=4096)
    args = parser.parse_args()
    if args.count < 0 or args.payload_bytes < 0:
        parser.error("count and payload-bytes must be nonnegative")
    if args.phase == "write":
        write(args.url, args.ledger, args.count, args.payload_bytes)
    else:
        verify(args.url, args.ledger)


if __name__ == "__main__":
    main()
