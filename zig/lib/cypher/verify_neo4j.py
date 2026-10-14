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

"""Replay the pinned read campaign against an empty disposable Neo4j database."""

import argparse
import json
from pathlib import Path
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:17474")
    parser.add_argument(
        "--reuse-fixture",
        action="store_true",
        help="Run read checks against a previously seeded disposable fixture",
    )
    args = parser.parse_args()
    campaign = json.loads(
        Path(__file__).with_name("fixtures").joinpath("neo4j_read.json").read_text()
    )

    def run(statement):
        request = urllib.request.Request(
            args.url.rstrip("/") + "/db/neo4j/tx/commit",
            json.dumps({"statements": [{"statement": statement}]}).encode(),
            {"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    initial = run("MATCH (n) RETURN count(n)")
    expected_count = 4 if args.reuse_fixture else 0
    if initial["errors"] or initial["results"][0]["data"][0]["row"] != [expected_count]:
        raise RuntimeError("Reference campaign requires an empty disposable database")
    if not args.reuse_fixture:
        seed = run(campaign["setup"])
        if seed["errors"]:
            raise RuntimeError(seed["errors"])
    failures = []
    extensions = 0
    for case in campaign["cases"]:
        response = run(case["query"])
        if "reference_error" in case:
            extensions += 1
            okay = (
                response["errors"]
                and response["errors"][0]["code"] == case["reference_error"]
            )
        else:
            rows = (
                [entry["row"] for entry in response["results"][0]["data"]]
                if response["results"]
                else None
            )
            okay = not response["errors"] and rows == case["rows"]
        if not okay:
            failures.append(
                {"query": case["query"], "expected": case, "actual": response}
            )
    for failure in failures:
        print(json.dumps(failure, ensure_ascii=False))
    print(
        f"{len(campaign['cases']) - len(failures)}/{len(campaign['cases'])} cases qualified; {extensions} documented extensions"
    )
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
