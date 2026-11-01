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

"""Compare bounded reader requests using hash-verified private page fixtures.

Writes metrics and text hashes only; document contents are never included.
The first observation may include a model load. Subsequent observations are
warm candidates, not proof that a model stayed resident or the GPU was idle.
"""

import argparse
import base64
import concurrent.futures
import hashlib
import json
import pathlib
import re
import sys
import time
import urllib.error
import urllib.request


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--fixtures", type=pathlib.Path, required=True)
    p.add_argument("--pages", nargs="+", default=["doj-07-0-p1", "doj-12-1-p1"])
    p.add_argument("--rounds", type=int, default=2)
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--max-tokens", type=int, default=2048)
    p.add_argument("--require-native-batch", action="store_true")
    p.add_argument("--output", type=pathlib.Path, required=True)
    args = p.parse_args()
    if not 1 <= args.rounds <= 4 or not 1 <= args.concurrency <= 16:
        p.error("qualification is bounded to 1–4 rounds and 1–16 concurrent requests")
    if not 1 <= len(args.pages) <= 64:
        p.error("qualification is bounded to 1–64 pages per round")
    manifest = json.loads((args.fixtures / "manifest.json").read_text())
    expected = json.loads((args.fixtures / "expectations.json").read_text())["cases"]
    norm = lambda s: re.sub("[^a-z0-9]", "", s.casefold())
    payloads = []
    for page in args.pages:
        item = next(x for x in manifest if x["id"] == page)
        image = (args.fixtures / pathlib.Path(item["image"]).name).read_bytes()
        if hashlib.sha256(image).hexdigest() != item["image_sha256"]:
            raise ValueError("fixture hash mismatch: " + page)
        payloads.append(
            (
                page,
                item["image_sha256"],
                json.dumps(
                    {
                        "model": args.model,
                        "max_tokens": args.max_tokens,
                        "images": [
                            {
                                "url": "data:image/png;base64,"
                                + base64.b64encode(image).decode()
                            }
                        ],
                    }
                ).encode(),
            )
        )
    receipt = {
        "model": args.model,
        "max_tokens": args.max_tokens,
        "concurrency": args.concurrency,
        "observations": [],
    }

    def request(item):
        page, digest, body = item
        row = {"page": page, "image_sha256": digest}
        start = time.monotonic()
        try:
            req = urllib.request.Request(
                args.url, data=body, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=360) as response:
                row["http_status"] = response.status
                data = json.load(response)
            texts = [x.get("text", "") for x in data.get("data", [])]
            text = "\n".join(texts)
            row.update(
                items=len(texts),
                characters=len(text),
                text_sha256=hashlib.sha256(text.encode()).hexdigest(),
                tail_pass=all(norm(x) in norm(text) for x in expected[page]),
                usage=data.get("usage"),
                execution=data.get("execution"),
                statuses=[x.get("status") for x in data.get("data", [])],
            )
        except urllib.error.HTTPError as e:
            row["http_status"] = e.code
            try:
                error = json.load(e)
                row["error_code"] = error.get("error", error.get("code"))
            except (ValueError, AttributeError):
                row["error_code"] = "unparseable_error"
        row["seconds"] = round(time.monotonic() - start, 3)
        print(json.dumps(row), flush=True)
        return row

    for round_number in range(args.rounds):
        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.concurrency
        ) as pool:
            rows = list(pool.map(request, payloads))
        receipt["observations"].append(
            {
                "round": round_number,
                "seconds": round(time.monotonic() - start, 3),
                "requests": rows,
            }
        )
        args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    passed = all(
        row.get("http_status") == 200
        and row.get("items") == 1
        and row.get("tail_pass")
        and (
            not args.require_native_batch
            or (row.get("execution") or {}).get("native_items", 0) == 1
        )
        and not any(
            s in ("failed", "truncated", "unresolved") for s in row.get("statuses", [])
        )
        for observation in receipt["observations"]
        for row in observation["requests"]
    )
    receipt["passed"] = passed
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
