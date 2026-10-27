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

"""Evaluate the pinned Codex discovery corpus against the native memory API.

The server owns the qualified embedder and credentials. --model is JSON containing
provider/model/revision/dimensions. The script creates an isolated new space and
writes the complete evidence-backed 84-concept corpus before owner maintenance.
No labels or synonyms are converted into aliases or embeddings.
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
FIXTURE = (
    ROOT
    / "zig/pkg/antfly-embedded/src/memory/fixtures/codex-a2d59cfb01/concept_merge.jsonl"
)


def measure(rows, pages, ids):
    modes = {mode: {} for mode in ("lexical", "ann", "union")}
    for name, page in pages.items():
        items = page["items"]
        modes["union"][name] = [hit["entity"]["entity"]["id"] for hit in items]
        modes["lexical"][name] = [
            hit["entity"]["entity"]["id"] for hit in items if hit["match_score"] > 0
        ]
        semantic = [hit for hit in items if hit["semantic_discovered"]]
        semantic.sort(
            key=lambda hit: (
                -max(hit.get("name_similarity") or 0, hit.get("view_similarity") or 0),
                hit["entity"]["entity"]["id"],
            )
        )
        modes["ann"][name] = [hit["entity"]["entity"]["id"] for hit in semantic]
    report = {}
    for mode, candidates in modes.items():

        def found(row, limit=None):
            return (
                ids[row["b"]] in candidates[row["a"]][:limit]
                or ids[row["a"]] in candidates[row["b"]][:limit]
            )

        positives = [row for row in rows if row["same"]]
        negatives = [row for row in rows if not row["same"]]
        report[mode] = {
            "synonym_pairs": len(positives),
            "recalled_pairs": sum(found(row) for row in positives),
            "recalled_pairs_top5": sum(found(row, 5) for row in positives),
            "labelled_non_synonyms": len(negatives),
            "admitted_non_synonyms": sum(found(row) for row in negatives),
            "average_candidates_per_seed": sum(map(len, candidates.values()))
            / len(candidates),
            "missed_synonyms": [
                [row["a"], row["b"]] for row in positives if not found(row)
            ],
        }
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--space", default=f"codex_eval_{time.time_ns()}")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wait-seconds", type=float, default=900)
    parser.add_argument("--name-threshold", type=float, default=0.80)
    parser.add_argument("--view-threshold", type=float, default=0.85)
    parser.add_argument("--neighbors", type=int, default=32)
    args = parser.parse_args()
    model = json.loads(args.model.read_text())
    rows = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
    descriptions = {}
    for row in rows:
        for side in ("a", "b"):
            descriptions.setdefault(row[side], row[f"{side}_description"])
    ids = {name: hashlib.sha256(name.encode()).hexdigest() for name in descriptions}
    base = (
        args.url.rstrip("/")
        + "/memory/v1/spaces/"
        + urllib.parse.quote(args.space, safe="")
    )
    headers = {"Content-Type": "application/json"}
    if token := os.environ.get("ANTFLY_AUTHORIZATION"):
        headers["Authorization"] = token
    counter = 0

    def call(suffix="", body=None, method="POST"):
        nonlocal counter
        counter += 1
        request = urllib.request.Request(
            base + suffix,
            None if body is None else json.dumps(body).encode(),
            headers={**headers, "Idempotency-Key": f"eval-{counter}"},
            method=method,
        )
        with urllib.request.urlopen(request, timeout=180) as response:
            return json.load(response)

    call(
        body={
            "max_output_items": 256,
            "max_input_bytes": 1048576,
            "models": {"embedder": model},
            "semantic_discovery": {
                "enabled": True,
                "name_threshold": args.name_threshold,
                "view_threshold": args.view_threshold,
                "neighbors": args.neighbors,
            },
        },
        method="PUT",
    )
    evidence = [
        {
            "id": ids[name],
            "source_id": "codex-a2d59cfb01",
            "source_version": "1",
            "text": name + "\n" + description,
        }
        for name, description in descriptions.items()
    ]
    entities = [
        {"id": ids[name], "name": name, "evidence": [{"id": ids[name]}]}
        for name in descriptions
    ]
    assertions = [
        {
            "id": "description-" + ids[name],
            "subject": ids[name],
            "predicate": "description",
            "value": name,
            "statement": description,
            "evidence": [{"id": ids[name]}],
        }
        for name, description in descriptions.items()
    ]
    call(
        "/observations",
        {"evidence": evidence, "entities": entities, "assertions": assertions},
    )
    started = time.monotonic()
    seed = next(iter(ids.values()))
    while True:
        call("/maintenance", {})
        page = call(f"/entities/{seed}/candidates", {"limit": 64})
        if page["semantic_discovery"] == "indexed":
            break
        if time.monotonic() - started > args.wait_seconds:
            raise TimeoutError("semantic generation did not reach indexed readiness")
    pages = {}
    for name, entity_id in ids.items():
        page = call(f"/entities/{entity_id}/candidates", {"limit": 64})
        if page["semantic_discovery"] != "indexed":
            raise RuntimeError(
                f"projection not ready for {name}: {page['semantic_discovery']}"
            )
        unique = {}
        for _ in range(256):
            if page["omitted"]:
                raise RuntimeError(
                    f"candidate window truncated for {name}; do not report partial recall"
                )
            for hit in page["items"]:
                identifier = hit["entity"]["entity"]["id"]
                if identifier in unique:
                    hit["semantic_discovered"] |= unique[identifier][
                        "semantic_discovered"
                    ]
                unique[identifier] = hit
            if not page["next_cursor"]:
                break
            page = call(
                f"/entities/{entity_id}/candidates",
                {"limit": 64, "cursor": page["next_cursor"]},
            )
        else:
            raise RuntimeError(
                f"candidate pagination exceeded evaluation bound for {name}"
            )
        page["items"] = sorted(
            unique.values(),
            key=lambda hit: (
                -hit["match_score"],
                -hit["description_score"],
                hit["entity"]["entity"]["id"],
            ),
        )
        pages[name] = page
    report = {
        "fixture": str(FIXTURE.relative_to(ROOT)),
        "fixture_sha256": hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
        "model": model,
        "entities": len(ids),
        "space": args.space,
        "thresholds": {"names": args.name_threshold, "views": args.view_threshold},
        "neighbors_per_projection": args.neighbors,
        "pair_admission": "either direction; top5 uses native ordering for lexical/union and semantic similarity ordering for ANN",
        "results": measure(rows, pages, ids),
        "pages": pages,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "pages"}, indent=2
        )
    )


if __name__ == "__main__":
    main()
