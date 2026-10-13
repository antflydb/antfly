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

"""Independent per-window model evidence and documented document aggregation.

Window coordinates/ownership implement the published source-word contract;
model scores and local outputs come exclusively from pinned upstream GLiNER2.
"""

import argparse
import copy
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys

from family_oracle import UPSTREAM_COMMIT, verify_model
from source_pin import verify_source


def windows(model, text, width, overlap):
    words = list(model.processor.word_splitter(text, lower=False))
    ranges = [(word[1], word[2]) for word in words]
    result = []
    start = 0
    while start < len(ranges):
        end = min(len(ranges), start + width)
        left = 0 if start == 0 else ranges[start][0]
        right = len(text) if end == len(ranges) else ranges[end - 1][1]
        if end - start + int(text[right - 1] not in ".!?") > width:
            end -= 1
            right = ranges[end - 1][1]
        result.append(
            {"start": left, "end": right, "word_start": start, "word_end": end}
        )
        if end == len(ranges):
            break
        start = end - overlap
    previous = 0
    for i, window in enumerate(result):
        boundary = (
            len(ranges)
            if i + 1 == len(result)
            else (window["word_end"] + result[i + 1]["word_start"]) // 2
        )
        window["owned_words"] = boundary - previous
        previous = boundary
    return result


def select_nonoverlapping(values):
    # Exact weighted interval scheduling; corpus fixtures avoid score ties.
    values = sorted(
        values, key=lambda value: (value["source"]["end"], value["source"]["start"])
    )
    states = [(0.0, [])]
    for i, value in enumerate(values):
        before = i
        while before and values[before - 1]["source"]["end"] > value["source"]["start"]:
            before -= 1
        weight, selected = states[before]
        candidate = (weight + value["confidence"], selected + [value])
        previous = states[-1]
        states.append(
            max((candidate, previous), key=lambda state: (state[0], len(state[1])))
        )
    return sorted(
        states[-1][1],
        key=lambda value: (
            -value["confidence"],
            value["source"]["start"],
            value["source"]["end"],
        ),
    )


def relation_order(value):
    head, tail = value["head"]["source"], value["tail"]["source"]
    distance = max(head["start"] - tail["end"], tail["start"] - head["end"], 0)
    return (distance, -value["confidence"], head["start"], tail["start"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--variant", choices=["base", "multi_decide", "decide_1b"], required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source_sha256 = verify_source(args.upstream)
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    from gliner2 import AutoExtractor
    from oracle import build_extract_schema, load_manifest
    from benchmark_family_python import canonical_expected

    if args.variant == "base":
        pin = load_manifest()["models"]["base"]
        for name, expected in pin["files"].items():
            data = (args.model_dir / name).read_bytes()
            assert (
                len(data) == expected["size_bytes"]
                and hashlib.sha256(data).hexdigest() == expected["sha256"]
            )
    else:
        pin = verify_model(args.model_dir, args.variant)
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(True)
    model = AutoExtractor.from_pretrained(
        str(args.model_dir), map_location="cpu"
    ).eval()
    definitions = {
        "person": "A named human being",
        "organization": "A company or organization",
    }
    labels = ["positive", "negative"]
    spec = {
        "entities": definitions,
        "relations": ["works_for"],
        "classifications": [{"task": "sentiment", "labels": labels}],
    }
    native = {
        "entities": list(definitions),
        "entity_definitions": {
            name: {"description": description}
            for name, description in definitions.items()
        },
        "relations": [{"type": "works_for"}],
        "classifications": [{"name": "sentiment", "labels": labels}],
    }
    schema = build_extract_schema(spec)
    texts = [
        ("tiny", "a", 128),
        ("short", "Ada works for Antfly. The service is excellent.", 128),
        (
            "multi_window",
            ("Ada works for Antfly. The service is excellent. " * 32).rstrip(),
            128,
        ),
    ]
    # Measure the encoded ceiling with the complete mixed prompt, never by
    # guessing source words from the model's architectural context size.
    chosen = None
    for count in range(900, 1025):
        candidate = "word " * count + "."
        batch = model.processor.collate_fn_inference(
            [(candidate, schema.build())],
            max_len=2048,
            architecture="span" if args.variant == "decide_1b" else "boundary",
            error_policy="raise",
            build_targets=False,
            on_capacity_exceeded="raise",
        )
        if batch.input_ids.shape[-1] > 1024:
            break
        chosen = candidate
    assert chosen is not None
    texts.append(("token_boundary", chosen, 1024))
    cases = []
    for name, text, width in texts:
        planned = windows(model, text, width, 16)
        entity_values = {kind: {} for kind in definitions}
        relation_values = {}
        rows = []
        evidence = []
        for window in planned:
            local = text[window["start"] : window["end"]]
            logits = []
            original = model.classifier.forward

            def observed(*positional, **keyword):
                result = original(*positional, **keyword)
                logits.extend(result.detach().flatten().tolist())
                return result

            model.classifier.forward = observed
            with torch.inference_mode():
                output = model.extract(
                    local, schema, include_confidence=True, include_spans=True
                )
            model.classifier.forward = original
            assert len(logits) == len(labels), (name, logits)
            rows.append((window["owned_words"], logits))
            batch = model.processor.collate_fn_inference(
                [(local, schema.build())],
                max_len=2048,
                architecture="span" if args.variant == "decide_1b" else "boundary",
                error_policy="raise",
                build_targets=False,
                on_capacity_exceeded="raise",
            )
            expected = canonical_expected({}, native, output)
            evidence.append(
                dict(
                    window,
                    text=local,
                    input_ids=batch.input_ids.tolist()[0],
                    expected=expected,
                )
            )
            for group in expected["entities"]:
                for candidate in group["values"]:
                    value = copy.deepcopy(candidate)
                    value["source"]["start"] += window["start"]
                    value["source"]["end"] += window["start"]
                    key = (value["source"]["start"], value["source"]["end"])
                    old = entity_values[group["name"]].get(key)
                    if old is None or value["confidence"] > old["confidence"]:
                        entity_values[group["name"]][key] = value
            for candidate in expected["relations"]:
                value = copy.deepcopy(candidate)
                for endpoint in ("head", "tail"):
                    value[endpoint]["source"]["start"] += window["start"]
                    value[endpoint]["source"]["end"] += window["start"]
                key = (
                    value["name"],
                    value["head"]["text"].casefold(),
                    value["tail"]["text"].casefold(),
                )
                old = relation_values.get(key)
                if old is None or relation_order(value) < relation_order(old):
                    relation_values[key] = value
        denominator = sum(weight for weight, _ in rows)
        aggregated = torch.tensor(
            [
                sum(weight * scores[i] for weight, scores in rows) / denominator
                for i in range(len(labels))
            ],
            dtype=torch.float64,
        )
        probabilities = torch.softmax(aggregated, dim=-1).tolist()
        best = max(range(len(labels)), key=probabilities.__getitem__)
        expected = {
            "entities": [
                {"name": kind, "values": select_nonoverlapping(list(values.values()))}
                for kind, values in entity_values.items()
            ],
            "classifications": [
                {
                    "name": "sentiment",
                    "labels": [
                        {"label": labels[best], "confidence": probabilities[best]}
                    ],
                }
            ],
            "structures": [],
            "relations": sorted(
                relation_values.values(),
                key=lambda value: (
                    value["name"],
                    value["head"]["source"]["start"],
                    value["tail"]["source"]["start"],
                ),
            ),
        }
        cases.append(
            {
                "id": name,
                "text": text,
                "window_words": width,
                "native_schema_json": json.dumps(native),
                "windows": evidence,
                "classification_logits": aggregated.tolist(),
                "expected": expected,
            }
        )
        print(
            args.variant,
            name,
            len(text),
            len(planned),
            max(len(window["input_ids"]) for window in evidence),
            flush=True,
        )
    args.output.write_text(
        json.dumps(
            {
                "format_version": 1,
                "variant": args.variant,
                "model": pin,
                "upstream_commit": UPSTREAM_COMMIT,
                "source_sha256": source_sha256,
                "runtime": {
                    name: importlib.metadata.version(name)
                    for name in ("torch", "transformers", "tokenizers", "pydantic")
                },
                "cases": cases,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
