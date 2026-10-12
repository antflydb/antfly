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

"""Compare extraction-v2 configs on a labelled JSONL corpus using embedded inference.

Each corpus row: {"id":"unique", "text":"Ada in Paris", "entities":
[{"label":"person", "start":0, "end":3}]}. Offsets are Unicode code points,
end-exclusive. An empty entities list is an explicitly labelled negative example.
Config files are extraction-v2 requests without inputs. No model downloads occur.
"""

from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time


def spans(entities: list[dict], text: str) -> set[tuple[str, int, int]]:
    result = set()
    for entity in entities:
        label, start, end = entity["label"], entity["start"], entity["end"]
        if not isinstance(label, str) or not label:
            raise ValueError("entity labels must be non-empty strings")
        if (
            type(start) is not int
            or type(end) is not int
            or not 0 <= start < end <= len(text)
        ):
            raise ValueError(
                "entity offsets must be non-empty Unicode code point ranges"
            )
        if "text" in entity and entity["text"] != text[start:end]:
            raise ValueError("entity text does not match its annotated span")
        result.add((label, start, end))
    return result


def load_corpus(path: Path) -> list[dict]:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    seen = set()
    for row in rows:
        if not isinstance(row["id"], str) or not row["id"] or row["id"] in seen:
            raise ValueError("corpus IDs must be unique non-empty strings")
        if not isinstance(row["text"], str) or not isinstance(row["entities"], list):
            raise ValueError("every row needs text and an explicit entities list")
        seen.add(row["id"])
        spans(row["entities"], row["text"])
    if not rows:
        raise ValueError("corpus must contain labelled examples")
    return rows


def metrics(
    gold: list[set], predicted: list[set], labels: tuple[str, ...] | list[str] = ()
) -> dict:
    if len(gold) != len(predicted):
        raise ValueError("prediction count differs from corpus")
    counts = {label: Counter() for label in labels}
    for expected, actual in zip(gold, predicted, strict=True):
        labels = {span[0] for span in expected | actual}
        for label in labels:
            count = counts.setdefault(label, Counter())
            e = {s for s in expected if s[0] == label}
            a = {s for s in actual if s[0] == label}
            count.update(
                gold=len(e),
                mentions=len(a),
                tp=len(e & a),
                fp=len(a - e),
                fn=len(e - a),
            )

    def summarize(count):
        tp, fp, fn = count["tp"], count["fp"], count["fn"]
        return {
            **{k: count[k] for k in ("gold", "mentions", "tp", "fp", "fn")},
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None,
            "f1": 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else None,
        }

    total = sum(counts.values(), Counter())
    return {
        "per_label": {label: summarize(counts[label]) for label in sorted(counts)},
        "micro": summarize(total),
    }


def evaluate(inference, config: dict, corpus: list[dict]) -> dict:
    config = deepcopy(config)
    if config.get("schema_version") != 2 or "inputs" in config:
        raise ValueError("configs must be extraction-v2 requests without inputs")
    options = config.setdefault("options", {})
    options.update(
        include_spans=True, include_confidence=True, offset_unit="unicode_codepoints"
    )
    outputs, predicted = [], []
    started = time.perf_counter()
    for row in corpus:
        request = {**config, "inputs": [{"id": row["id"], "content": row["text"]}]}
        output = inference.extract(request)
        data = output["data"]
        if (
            len(data) != 1
            or data[0].get("id") != row["id"]
            or data[0].get("offset_unit") != "unicode_codepoints"
        ):
            raise ValueError(
                "inference response does not match the requested document/offsets"
            )
        predicted.append(spans(data[0].get("entities", []), row["text"]))
        outputs.append(output)
    return {
        "effective_config": config,
        "elapsed_seconds": time.perf_counter() - started,
        "metrics": metrics(
            [spans(row["entities"], row["text"]) for row in corpus],
            predicted,
            config["schema"].get("entities", []),
        ),
        "outputs": outputs,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        action="append",
        help="repeat for each config to compare",
    )
    parser.add_argument("--models-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    for budget in ("host", "backend", "process-memory", "combined", "scratch"):
        parser.add_argument(
            f"--{budget}-budget-mb",
            type=int,
            default=0,
            help="embedded inference budget; zero selects automatic",
        )
    args = parser.parse_args()
    corpus = load_corpus(args.corpus)
    from antfly_embedded import Inference

    report = {
        "format_version": 1,
        "matching": "exact label and Unicode-code-point span; duplicate spans deduplicated",
        "corpus_sha256": hashlib.sha256(args.corpus.read_bytes()).hexdigest(),
        "documents": len(corpus),
        "runs": [],
    }
    with Inference.open(
        models_dir=args.models_dir,
        host_budget_mb=args.host_budget_mb,
        backend_budget_mb=args.backend_budget_mb,
        process_memory_budget_mb=args.process_memory_budget_mb,
        combined_budget_mb=args.combined_budget_mb,
        scratch_budget_mb=args.scratch_budget_mb,
    ) as inference:
        report["installed_models"] = inference.list_models()
        for path in args.config:
            result = evaluate(inference, json.loads(path.read_text()), corpus)
            report["runs"].append(
                {
                    "config_path": str(path),
                    "config_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    **result,
                }
            )
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
