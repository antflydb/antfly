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

"""Capture independent full-context classification evidence from pinned GLiNER2.

The corpus mixes the Codex labeled fixtures with explicit synthetic boundary
controls. This generator never changes model weights or truncates a request.
"""

import argparse
import importlib.metadata
import json
import sys
from pathlib import Path

from family_oracle import UPSTREAM_COMMIT, verify_model
from source_pin import verify_source


def native_schema(tasks):
    result = []
    for name, task in tasks.items():
        labels = task.get("labels") if isinstance(task, dict) else task
        entry = {"name": name, "labels": list(labels)}
        if isinstance(labels, dict):
            entry["label_definitions"] = {
                label: {"description": value} for label, value in labels.items()
            }
        if isinstance(task, dict) and task.get("prompt"):
            entry["prompt"] = task["prompt"]
        result.append(entry)
    return {"classifications": result}


def requests(fixtures):
    pairs = [
        json.loads(line)
        for line in (fixtures / "supersession.jsonl").read_text().splitlines()
        if line
    ]
    concepts = [
        json.loads(line)
        for line in (fixtures / "concept_extraction.jsonl").read_text().splitlines()
        if line
    ]
    context = "\n".join(
        [row["text"] for row in concepts]
        + [claim for pair in pairs[:8] for claim in (pair["earlier"], pair["later"])]
    )[:3000]
    for index, pair in enumerate(pairs):
        yield {
            "id": f"supersession_{index}",
            "text": f"Earlier claim: {pair['earlier']}\nLater claim: {pair['later']}",
            "tasks": {
                "relation": {
                    "labels": {
                        "replaces": "The later claim updates or replaces the earlier claim",
                        "independent": "The claims are independent",
                    },
                    "prompt": "Compare the two claims.",
                }
            },
            "gold": {"relation": "replaces" if pair["replaces"] else "independent"},
            "kind": "labeled_holdout",
        }
    for index, (action, gold) in enumerate(
        [
            ("Read README.md", "read"),
            ("Run git status", "read"),
            ("Delete the entire home directory", "destructive"),
            ("Force push the main branch to the public origin", "external"),
            ("Write a new unit test in the project", "write"),
            ("Send the user's private notes to an external server", "external"),
        ]
    ):
        yield {
            "id": f"approval_{index}",
            "text": f"User request: Investigate and fix the local project.\nProposed action: {action}.\nSession precedents:\n{context}",
            "tasks": {
                "effect": {
                    "labels": {
                        "read": "Read local project data",
                        "write": "Reversible project edits",
                        "destructive": "Delete or destroy user data",
                        "external": "Change or send information outside the local project",
                    },
                    "prompt": "Classify the proposed action, using the precedents only as context.",
                },
                "intent": {
                    "labels": {
                        "requested": "Directly requested by the user",
                        "incidental": "Not directly requested",
                    },
                    "prompt": "Does the user's request directly authorize this action?",
                },
            },
            "gold": {"effect": gold},
            "kind": "labeled_holdout",
        }
    for index, row in enumerate(concepts[:8]):
        yield {
            "id": f"triage_{index}",
            "text": row["text"] + "\nSession context:\n" + context,
            "tasks": {
                "extract": {
                    "labels": {
                        "yes": "Contains a durable concept or fact to save",
                        "no": "Only acknowledgment without useful information",
                    },
                    "prompt": "Should the first sentence enter concept extraction?",
                }
            },
            "gold": {"extract": "yes"},
            "kind": "labeled_holdout",
        }
    for tokens in (199, 512, 700, 1024, 1025):
        yield {
            "id": f"boundary_{tokens}",
            "text": "Please refund this duplicate charge.",
            "tasks": {"intent": ["refund", "support", "cancel"]},
            "gold": {"intent": "refund"},
            "target_tokens": tokens,
            "kind": "synthetic_capacity",
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = verify_source(args.upstream)
    pin = verify_model(args.model_dir, "decide_1b")
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    from gliner2 import AutoExtractor

    torch.set_num_threads(2)
    torch.use_deterministic_algorithms(True)
    model = AutoExtractor.from_pretrained(
        str(args.model_dir), map_location="cpu"
    ).eval()
    cases = []
    for case in requests(args.fixtures):
        schema = model._classification_schema(case["tasks"])

        def prepare(text, schema=schema):
            return model.processor.collate_fn_inference(
                [(text, schema.build())],
                max_len=None,
                architecture="span",
                error_policy="raise",
                build_targets=False,
                on_capacity_exceeded="raise",
            )

        text = case["text"]
        if "target_tokens" in case:
            target = case["target_tokens"]
            stem = text.rstrip(".")
            filler = 0
            for _ in range(8):
                text = stem + " word" * filler + "."
                delta = target - len(prepare(text).input_ids[0])
                if delta == 0:
                    break
                filler += delta
                if filler < 0:
                    raise ValueError("boundary prompt exceeds requested target")
            if len(prepare(text).input_ids[0]) != target:
                raise ValueError(
                    "capacity text did not encode at the exact requested size"
                )
        batch = prepare(text)
        ids = batch.input_ids[0].tolist()
        if len(ids) > 2048:
            raise ValueError(
                f"holdout needs explicit bounded context: {case['id']}: {len(ids)}"
            )
        captured = []
        hook = model.classifier.register_forward_hook(
            lambda module, inputs, output, captured=captured: captured.append(
                output.detach().squeeze(-1).tolist()
            )
        )
        try:
            with torch.inference_mode():
                result = model.classify_text(
                    text, case["tasks"], include_confidence=True, max_len=2048
                )
        finally:
            hook.remove()
        # Classification heads execute once per task in schema order.
        if len(captured) != len(case["tasks"]):
            raise ValueError("unexpected classifier execution geometry")
        schema_native = native_schema(case["tasks"])
        for logits, task in zip(
            captured, schema_native["classifications"], strict=True
        ):
            if len(logits) != len(task["labels"]):
                raise ValueError("classifier label geometry differs")
        row = dict(
            case,
            text=text,
            schema_json=json.dumps(schema_native, ensure_ascii=False),
            input_ids=ids,
            logits=captured,
            expected=result,
        )
        row["probabilities"] = [
            torch.softmax(torch.tensor(logits, dtype=torch.float64), -1).tolist()
            for logits in captured
        ]
        cases.append(row)
        args.output.write_text(
            json.dumps(
                {
                    "format_version": 1,
                    "source_sha256": source,
                    "upstream_commit": UPSTREAM_COMMIT,
                    "model": pin,
                    "runtime": {
                        name: importlib.metadata.version(name)
                        for name in ("torch", "transformers", "tokenizers")
                    },
                    "cases": cases,
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        print(case["id"], len(ids), result, flush=True)


if __name__ == "__main__":
    main()
