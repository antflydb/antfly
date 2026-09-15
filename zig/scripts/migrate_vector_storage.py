#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Drive bounded online migration passes; the server owns all durable state.

The request body, including budgets, is the idempotency contract. Keep it
unchanged when retrying. Ctrl-C pauses the driver, not the durable migration.
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--table", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument(
        "--action",
        choices=("run", "start", "step", "publish", "cancel", "status"),
        default="run",
    )
    parser.add_argument("--batch-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--batch-rows", type=int, default=1024)
    parser.add_argument("--temporary-bytes", type=int, default=64 * 1024**3)
    parser.add_argument("--disk-reserve-bytes", type=int, default=1024**3)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    endpoint = (
        args.url.rstrip("/")
        + "/db/v1/tables/"
        + urllib.parse.quote(args.table, safe="")
        + "/storage-migration"
    )
    request = {
        "job_id": args.job,
        "mode": "online",
        "budget": {
            name.replace("-", "_"): getattr(args, name.replace("-", "_"))
            for name in (
                "batch-bytes",
                "batch-rows",
                "temporary-bytes",
                "disk-reserve-bytes",
            )
        },
    }
    headers = {"Content-Type": "application/json"}
    if token := os.environ.get("ANTFLY_API_KEY"):
        headers["Authorization"] = "Bearer " + token
    action = "start" if args.action == "run" else args.action
    previous = None
    while True:
        body = json.dumps({"action": action, "request": request}).encode()
        try:
            with urllib.request.urlopen(
                urllib.request.Request(endpoint, body, headers, method="POST"),
                timeout=args.timeout,
            ) as response:
                job = json.load(response)
        except urllib.error.HTTPError as error:
            raise SystemExit(
                f"HTTP {error.code}: {error.read().decode()}; retry with the same job and budgets"
            ) from error
        print(json.dumps(job, sort_keys=True), flush=True)
        if args.action != "run" or job["phase"] in ("complete", "cancelled"):
            return
        # Unchanged serving progress means normal index repair/replay owns the
        # next boundary; avoid turning a pending build into a polling hot loop.
        signature = (job["phase"], job["cursor"], job["scanned_rows"])
        if signature == previous:
            time.sleep(0.25)
        previous = signature
        action = "publish" if job["phase"] == "ready" else "step"


if __name__ == "__main__":
    main()
