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

"""Compare complete OCR transcripts, work performed and elapsed time.

The first --case is the transcript control. Reference text is the PDF text layer,
so word coverage is a proxy, not a reading-order or manually labelled OCR score.
"""

import argparse
import json
from pathlib import Path
from statistics import mean, median

from pdf_ocr_report import quality


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--case", action="append", required=True, metavar="NAME=JSON")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    corpus = json.loads(args.corpus.read_text())
    identities = [(p["document"], p["page"]) for p in corpus["pages"]]
    references = {
        (p["document"], p["page"]): Path(p["reference"]).read_text()
        for p in corpus["pages"]
    }
    cases = []
    control = None
    for entry in args.case:
        name, path = entry.split("=", 1)
        report = json.loads(Path(path).read_text())
        passes = report.get("results", report.get("outputs"))
        if not passes or len(passes) != len(report["totals"]):
            raise ValueError(f"{name}: no complete timed passes")
        for rows in passes:
            if [(r["document"], r["page"]) for r in rows] != identities:
                raise ValueError(f"{name}: incomplete or reordered corpus")
        if control is None:
            control = passes[0]
        tokens = [sum(r["usage"]["completion_tokens"] for r in rows) for rows in passes]
        coverage = quality(passes[0], references)
        cases.append(
            {
                "case": name,
                "runtime": report.get("runtime", "antfly"),
                "batch": report["batch"],
                "seconds": report["totals"],
                "median_seconds": median(report["totals"]),
                "pages_per_second": len(identities) / median(report["totals"]),
                "completion_tokens": tokens,
                "whole_request_tokens_per_second": [
                    n / elapsed for n, elapsed in zip(tokens, report["totals"])
                ],
                "truncated_outputs": sum(
                    r["finish_reason"] == "length" for rows in passes for r in rows
                ),
                "control_transcripts_equal": all(
                    r["text"] == c["text"]
                    for rows in passes
                    for r, c in zip(rows, control)
                ),
                "repeats_equal": all(
                    r["text"] == c["text"]
                    for rows in passes[1:]
                    for r, c in zip(rows, passes[0])
                ),
                "mean_word_f1_proxy": mean(r["f1"] for r in coverage),
                "per_page_coverage": coverage,
                "speculation": [r.get("speculation") for r in passes[0]],
            }
        )
    args.output.write_text(
        json.dumps({"pages": len(identities), "cases": cases}, indent=2) + "\n"
    )
    for case in cases:
        print(
            case["case"],
            f"{case['median_seconds']:.3f}s",
            f"{case['pages_per_second']:.3f} pages/s",
            f"F1 proxy={case['mean_word_f1_proxy']:.3f}",
            f"control equal={case['control_transcripts_equal']}",
            f"truncated={case['truncated_outputs']}",
        )


if __name__ == "__main__":
    main()
