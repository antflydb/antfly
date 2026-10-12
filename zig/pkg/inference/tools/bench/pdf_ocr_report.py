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

"""Summarize OCR timing and normalized word coverage from corpus reports."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
from statistics import mean, median
import unicodedata


def words(text):
    text = unicodedata.normalize("NFKC", text).lower().replace("\u00ad", "")
    return re.findall(r"\w+", re.sub(r"-\s*\n\s*", "", text))


def quality(rows, references):
    result = []
    for row in rows:
        key = (row["document"], row["page"])
        ref = Counter(words(references[key]))
        output = Counter(words(row["text"]))
        overlap = sum((ref & output).values())
        precision = overlap / sum(output.values()) if output else 0
        recall = overlap / sum(ref.values()) if ref else 0
        result.append(
            {
                "document": key[0],
                "page": key[1],
                "reference_words": sum(ref.values()),
                "output_words": sum(output.values()),
                "precision": precision,
                "recall": recall,
                "f1": 2 * precision * recall / (precision + recall)
                if precision + recall
                else 0,
                "finish_reason": row.get("finish_reason"),
            }
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--gemma-dir", type=Path, required=True)
    parser.add_argument("--apple-accurate", type=Path, required=True)
    parser.add_argument("--apple-fast", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    corpus = json.loads(args.corpus.read_text())
    references = {
        (row["document"], row["page"]): Path(row["reference"]).read_text()
        for row in corpus["pages"]
    }
    cases = []
    baseline = None
    for path in sorted(
        args.gemma_dir.glob("gemma4-b*.json"),
        key=lambda p: int(p.stem.removeprefix("gemma4-b")),
    ):
        report = json.loads(path.read_text())
        texts = [row["text"] for row in report["results"][0]]
        if baseline is None:
            baseline = texts
        cases.append(
            {
                "engine": "gemma4",
                "batch": report["batch"],
                "samples_seconds": report["totals"],
                "seconds": median(report["totals"]),
                "pages_per_second": len(references) / median(report["totals"]),
                "quality": quality(report["results"][-1], references),
                "repeat_identical": all(
                    [row["text"] for row in run] == texts for run in report["results"]
                ),
                "matches_first_batch": texts == baseline,
            }
        )
    for level, path in [("accurate", args.apple_accurate), ("fast", args.apple_fast)]:
        report = json.loads(path.read_text())
        cases.append(
            {
                "engine": "apple_" + level,
                "batch": report["batch"],
                "samples_seconds": report["totals"],
                "seconds": median(report["totals"]),
                "pages_per_second": len(references) / median(report["totals"]),
                "quality": quality(report["results"], references),
            }
        )
    args.output.write_text(
        json.dumps(
            {
                "pages": len(references),
                "cases": cases,
                "quality_method": "Normalized multiset word F1 against PDF text layer; not ground-truth OCR accuracy",
            },
            indent=2,
        )
        + "\n"
    )
    for row in cases:
        print(
            f"{row['engine']} batch={row['batch']} {row['seconds']:.3f}s "
            f"{row['pages_per_second']:.3f} pages/s word F1={mean(q['f1'] for q in row['quality']):.3f}"
        )


if __name__ == "__main__":
    main()
