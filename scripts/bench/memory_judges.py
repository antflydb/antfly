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

"""Upload the pinned Codex gold corpus to native memory evaluation datasets.

--prepare-only writes the reproducible upload without a server. --judge selects
an Antfly provider/model/revision JSON file; the service owns provider credentials.
--external-judge pins client-owned judge metadata without invoking a provider.
With no managed judge, an external run waits for predictions through the native tasks and
verdicts APIs. Labels never enter judge inputs. This does not manufacture scores
by replaying gold as predictions.
"""

import argparse
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "zig/pkg/antfly-embedded/src/memory/fixtures/codex-a2d59cfb01"


def corpus():
    cases = []
    digests = {}
    for name in ("concept_pairs", "concept_merge", "concept_relations", "supersession"):
        raw = (FIXTURE / f"{name}.jsonl").read_bytes()
        digests[name] = hashlib.sha256(raw).hexdigest()
        for ordinal, line in enumerate(raw.splitlines()):
            if not line.strip():
                continue
            row = json.loads(line)
            identity = f"{name}-{ordinal:04d}"
            if name.startswith("concept_") and name != "concept_relations":
                inputs = {
                    key: value
                    for key, value in row.items()
                    if key not in ("same", "source")
                }
                cases.append(
                    {
                        "id": identity,
                        "task": "identity",
                        "input": inputs,
                        "expected": "same" if row["same"] else "different",
                    }
                )
            elif name == "concept_relations":
                # Evaluate every proposed typed predicate, including the wrong
                # predicates on positive pairs. The proposed predicate is input;
                # the annotated correct predicate remains gold only.
                for predicate in ("is_a", "part_of", "alternative_to"):
                    inputs = {
                        key: value
                        for key, value in row.items()
                        if key not in ("relation", "source")
                    }
                    inputs["proposed_predicate"] = predicate
                    cases.append(
                        {
                            "id": f"{identity}-{predicate}",
                            "task": "relationship",
                            "input": inputs,
                            "expected": "related"
                            if row["relation"] == predicate
                            else "unrelated",
                        }
                    )
            else:
                # This corpus has a binary replacement label, not four-way
                # annotations. Never relabel all negatives as "independent".
                cases.append(
                    {
                        "id": identity,
                        "task": "supersession",
                        "input": {"earlier": row["earlier"], "later": row["later"]},
                        "expected_supersedes": row["replaces"],
                    }
                )
    return {"fixture": "codex-a2d59cfb01", "sha256": digests, "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--space", default=f"judge_eval_{time.time_ns()}")
    judges = parser.add_mutually_exclusive_group()
    judges.add_argument("--judge", type=Path)
    judges.add_argument("--external-judge", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--wait-seconds", type=float, default=900)
    args = parser.parse_args()
    evidence = corpus()
    if args.prepare_only:
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        return
    judge_path = args.judge or args.external_judge
    judge = json.loads(judge_path.read_text()) if judge_path else None
    if judge is not None and (
        set(judge) - {"provider", "model", "revision", "dimensions"}
        or not all(judge.get(key) for key in ("provider", "model", "revision"))
    ):
        raise ValueError(
            "judge file must contain provider, model and immutable revision only"
        )
    prefix = (
        args.url.rstrip("/")
        + "/memory/v1/spaces/"
        + urllib.parse.quote(args.space, safe="")
    )

    def call(method, suffix, body=None, key=None):
        headers = {"Content-Type": "application/json"}
        if token := os.environ.get("ANTFLY_AUTHORIZATION"):
            headers["Authorization"] = token
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(
            prefix + suffix,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
            method=method,
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)

    created = call("PUT", "", {}, "create")
    revision = created["revision"]
    # Bound both the number and byte size of each native admission batch.
    batch = []
    for case in evidence["cases"] + [None]:
        if batch and (
            case is None
            or len(batch) == 32
            or len(json.dumps(batch + [case]).encode()) > 128 * 1024
        ):
            receipt = call(
                "POST",
                "/evaluation-datasets",
                {"id": "codex", "expected_revision": revision, "cases": batch},
                f"dataset-{revision}",
            )
            revision = receipt["revision"]
            batch = []
        if case is not None:
            batch.append(case)
    start = {
        "id": "codex",
        "dataset": "codex",
        "expected_revision": revision,
        "mode": "managed" if args.judge else "external",
        "judge": judge,
    }
    receipt = call("POST", "/evaluations", start, "start")
    evidence.update(
        space=args.space,
        judge=judge,
        operation=receipt,
        status=call("GET", "/evaluations/codex"),
    )
    if args.judge:
        until = time.monotonic() + args.wait_seconds
        while (
            evidence["status"]["run"]["phase"] == "pending" and time.monotonic() < until
        ):
            # Explicit owner progress works on Lite-compatible and remote
            # deployments without depending on a particular poll interval.
            try:
                call(
                    "POST",
                    f"/operations/{receipt['id']}",
                    {},
                    f"advance-{time.time_ns()}",
                )
            except urllib.error.HTTPError as error:
                if error.code not in (409, 429, 503):
                    raise
                time.sleep(0.2)
            evidence["status"] = call("GET", "/evaluations/codex")
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    if args.judge and evidence["status"]["run"]["phase"] != "completed":
        raise RuntimeError("evaluation did not complete; saved its current status")


if __name__ == "__main__":
    main()
