#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Freeze synthetic edge cases by executing the pinned upstream span decoder."""
import argparse
import json
from pathlib import Path
import sys

from capture_oracle import FIXTURES, verify_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    verify_source(args.source_root, manifest["upstream"]["source_sha256"])
    sys.path.insert(0, str(args.source_root.resolve()))
    import torch
    from gliner2.inference.candidate_decoder import finalize_spans
    from gliner2.inference.runtime import ExtractorRuntimeMixin

    overlap_cases = []
    for name, raw in [
        ("greedy_differs_from_weighted", [["long", .75, 0, 5], ["left", .5, 0, 2], ["right", .5, 3, 5]]),
        ("stable_equal_confidence", [["first", .5, 2, 8], ["second", .5, 2, 15], ["third", .5, 9, 15]]),
    ]:
        overlap_cases.append({"id": name, "candidates": raw,
            "omitted_overlap": finalize_spans(raw),
            "explicit_flat": finalize_spans(raw, overlap_policy="flat"),
            "explicit_allow": finalize_spans(raw, overlap_policy="allow")})

    class Probe(ExtractorRuntimeMixin):
        architecture = "span"

        def __init__(self, count):
            self.count = count

        def count_pred(self, embs):
            result = torch.zeros((1, 20), dtype=torch.float32)
            result[0, self.count] = 1
            return result

        def count_embed(self, embs, count):
            return torch.ones((count, embs.shape[0], 1), dtype=torch.float32)

    text = "😀 Élodie Élodie"
    span_rep = torch.full((3, 8, 1), -100, dtype=torch.float32)
    span_rep[:, 0, 0] = 0  # Exactly 0.5 sigmoid probability.
    gate_cases = []
    for count in [0, 1, 2, 19]:
        model = Probe(count)
        result = {}
        model._extract_span_result(result, "entities", "entities", torch.zeros((3, 1)),
            {"span_rep": span_rep}, ["(", "[P]", "entities", "(", "[E]", "person", "[E]", "alias", ")", ")"],
            ["😀", "Élodie", "Élodie"], 3, text, [0, 2, 9], [1, 8, 15], .5,
            {"entity_order": ["person", "alias"]}, {}, True, True)
        gate_cases.append({"id": "count_" + str(count), "predicted_count": count, "text": text,
            "threshold": .5, "span_logits": span_rep.squeeze(-1).tolist(),
            "start_mapping": [0, 2, 9], "end_mapping": [1, 8, 15],
            "expected": model.format_results(result, include_confidence=True)})
    output = {"format_version": 1, "upstream": manifest["upstream"],
              "coordinate_unit": "unicode_codepoints", "overlap_cases": overlap_cases,
              "count_threshold_unicode_cases": gate_cases}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
