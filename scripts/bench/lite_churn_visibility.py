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

"""Verify replacement postings after a changing-words churn run catches up."""

import argparse
import hashlib
import json
import time
from pathlib import Path

from lite_contention import sdk


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with open(args.library, "rb") as source:
        library_sha256 = hashlib.file_digest(source, "sha256").hexdigest()
    rows = [
        row
        for line in args.report.read_text().splitlines()
        if (row := json.loads(line)).get("kind") == "native"
    ]
    if len(rows) != 1 or rows[0].get("status") != "ok":
        raise RuntimeError("expected one completed native churn run")
    row = rows[0]
    if row["library_sha256"] != library_sha256:
        raise RuntimeError("verification library differs from measured library")
    expected = row["expected_latest_writes"]
    if not expected or not all(write.get("text_token") for write in expected):
        raise RuntimeError("report must contain changing-words acknowledgments")
    af = sdk(args.library)
    began = time.monotonic()
    latest = old = 0
    with af.open_readonly(args.database) as db:
        for index in db.stats()["indexes"]:
            if index.get("replay_catch_up_required", False):
                raise RuntimeError("index must catch up before posting verification")

        def hits(token):
            result = db.search(
                {
                    "full_text_search": {"match": {"field": "text", "text": token}},
                    "limit": 10,
                }
            )
            return [hit["_id"] for hit in result["responses"][0]["hits"]["hits"]]

        for write in expected:
            if write["key"] not in hits(write["text_token"]):
                raise RuntimeError(f"latest posting absent: {write['key']}")
            latest += 1
            if write["sequence"] >= 128:
                previous = (
                    write["text_token"].rsplit("sequence", 1)[0]
                    + "sequence"
                    + str(write["sequence"] - 128)
                )
                if write["key"] in hits(previous):
                    raise RuntimeError(
                        f"replaced posting still visible: {write['key']}"
                    )
                old += 1
        if not db.check()["valid"]:
            raise RuntimeError("read-only integrity failed")
    result = {
        "library_sha256": library_sha256,
        "latest_token_queries_passed": latest,
        "old_token_removals_passed": old,
        "readonly_integrity": True,
        "seconds": time.monotonic() - began,
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
