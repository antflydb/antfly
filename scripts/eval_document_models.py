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

"""Compare document readers against labelled PDF/PPTX/photo sources over HTTP.

Corpus JSON: {"cases":[{"id":"receipt", "path":"receipt.png",
"media_type":"image/png", "pages":[{"number":1,"text":"Total 12.50"}]}]}.
Paths are relative to the corpus. Optional page.tables uses native read table
cells. Optional normalize_whitespace is explicit and recorded. No model downloads,
prices or quality judgments are inferred. Credentials stay in an environment
variable; reports never contain request headers.
"""

from __future__ import annotations

import argparse
import base64
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request


def distance(left: str, right: str) -> int:
    """Exact Unicode-code-point Levenshtein distance using Myers bit vectors."""
    if len(left) > len(right):
        left, right = right, left
    if not left:
        return len(right)
    masks: dict[str, int] = {}
    for index, char in enumerate(left):
        masks[char] = masks.get(char, 0) | (1 << index)
    width = (1 << len(left)) - 1
    high = 1 << (len(left) - 1)
    positive, negative, score = width, 0, len(left)
    for char in right:
        matches = masks.get(char, 0)
        vertical = matches | negative
        horizontal = (((matches & positive) + positive) ^ positive) | matches
        plus = negative | ~(horizontal | positive)
        minus = positive & horizontal
        if plus & high:
            score += 1
        elif minus & high:
            score -= 1
        plus = (plus << 1) | 1
        minus <<= 1
        positive = (minus | ~(horizontal | plus)) & width
        negative = (plus & vertical) & width
    return score


def cells(tables: list[dict]) -> Counter:
    return Counter(
        (
            index,
            cell["row"],
            cell["column"],
            cell.get("row_span", 1),
            cell.get("column_span", 1),
            cell["text"],
        )
        for index, table in enumerate(tables)
        for cell in table["cells"]
    )


def score(case: dict, result: dict) -> dict:
    rows = result["data"]
    actual = {(row["source_id"], row["number"]): row for row in rows}
    expected = {(case["id"], page["number"]): page for page in case["pages"]}
    if len(actual) != len(rows):
        raise ValueError("duplicate source/page identity in reading")
    errors, characters = 0, 0
    table_expected, table_actual, table_correct = 0, 0, 0
    for identity, page in expected.items():
        observed = actual.get(identity, {})
        gold, text = page["text"], observed.get("text", "")
        if case.get("normalize_whitespace", False):
            gold, text = " ".join(gold.split()), " ".join(text.split())
        errors += distance(gold, text)
        characters += len(gold)
        if "tables" in page:
            wanted, got = cells(page["tables"]), cells(observed.get("tables", []))
            table_expected += wanted.total()
            table_actual += got.total()
            table_correct += (wanted & got).total()
    return {
        "coverage_exact": actual.keys() == expected.keys(),
        "missing_pages": len(expected.keys() - actual.keys()),
        "extra_pages": len(actual.keys() - expected.keys()),
        "character_errors": errors,
        "gold_characters": characters,
        "character_error_rate": errors / characters if characters else None,
        "unresolved_pages": sum(row.get("status") != "complete" for row in rows),
        "table_cells_expected": table_expected,
        "table_cells_actual": table_actual,
        "table_cells_correct": table_correct,
        "table_cell_precision": table_correct / table_actual if table_actual else None,
        "table_cell_recall": table_correct / table_expected if table_expected else None,
    }


def evaluate(corpus: Path, models: list[str], call) -> dict:
    content = corpus.read_bytes()
    cases = json.loads(content)["cases"]
    if not cases or len({case["id"] for case in cases}) != len(cases):
        raise ValueError("corpus needs unique non-empty case IDs")
    report = {
        "format_version": 1,
        "corpus_sha256": hashlib.sha256(content).hexdigest(),
        "distance": "exact Unicode code point Levenshtein",
        "runs": [],
    }
    for model in models:
        outcomes = []
        for case in cases:
            payload = (corpus.parent / case["path"]).read_bytes()
            source = {
                "id": case["id"],
                "media_type": case["media_type"],
                "url": f"data:{case['media_type']};base64,{base64.b64encode(payload).decode('ascii')}",
            }
            request = {"model": model, "documents": [source]}
            started = time.perf_counter()
            try:
                result = call(request)
            except Exception as error:
                outcomes.append(
                    {
                        "id": case["id"],
                        "error": str(error),
                        "elapsed_seconds": time.perf_counter() - started,
                    }
                )
                continue
            outcomes.append(
                {
                    "id": case["id"],
                    "source_sha256": hashlib.sha256(payload).hexdigest(),
                    "normalize_whitespace": case.get("normalize_whitespace", False),
                    "elapsed_seconds": time.perf_counter() - started,
                    "metrics": score(case, result),
                    "usage": result.get("usage"),
                    "document": result.get("document"),
                    "output": result,
                }
            )
        report["runs"].append({"model": model, "cases": outcomes})
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--model", required=True, action="append")
    parser.add_argument("--url", default="http://localhost:8080/ai/v1")
    parser.add_argument("--token-env", default="ANTFLY_INFERENCE_API_KEY")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    def call(body: dict) -> dict:
        headers = {"Content-Type": "application/json"}
        if token := os.environ.get(args.token_env):
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            args.url.rstrip("/") + "/read",
            data=json.dumps(body).encode(),
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=args.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"HTTP {error.code}") from None

    args.output.write_text(
        json.dumps(evaluate(args.corpus, args.model, call), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
