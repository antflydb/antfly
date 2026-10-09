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

"""Capture four frozen short-request holdouts from the pinned CPU FP32 oracle."""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

import benchmark_family_python as bench

# Freeze texts, question/label insertion order and descriptions before tuning.
REQUESTS = [
    (
        "binary_short",
        "The package arrived damaged.",
        {
            "refund": {
                "type": "choice",
                "instructions": "Choose the action.",
                "criteria": {"refund": "Refund the order", "keep": "Keep the order"},
            }
        },
    ),
    (
        "described_four_labels",
        "My account is locked after several incorrect passwords. I still have access to my email.",
        {
            "action": {
                "type": "choice",
                "instructions": "Choose the action that resolves this support request.",
                "criteria": {
                    "reset": "Reset the account password",
                    "refund": "Refund a payment",
                    "cancel": "Cancel the subscription",
                    "ship": "Ship a replacement item",
                },
            }
        },
    ),
    (
        "score_and_noul",
        "A single internal dashboard is unavailable. Customer requests still succeed and the team has a workaround.",
        {
            "severity": {
                "type": "score",
                "instructions": "Rate the impact of this incident.",
                "criteria": ["No impact", "Small impact", "Major impact"],
            },
            "page": {
                "type": "noul",
                "instructions": "Is immediate on-call escalation necessary?",
            },
        },
    ),
    (
        "mixed_longer",
        "The deployment failed twice and customers cannot sign in. The previous release was healthy. "
        "The database remains intact, rollback has been tested, and the engineer is available. "
        "Support has received multiple reports from customers in different regions.",
        {
            "action": {
                "type": "choice",
                "instructions": "Choose the safest immediate action.",
                "criteria": {
                    "rollback": "Restore the previous release",
                    "retry": "Retry this release",
                    "wait": "Wait for more evidence",
                },
            },
            "severity": {
                "type": "score",
                "instructions": "Rate the incident impact.",
                "criteria": ["Low", "Moderate", "High", "Critical"],
            },
            "page": {
                "type": "noul",
                "instructions": "Should the on-call engineer respond now?",
            },
        },
    ),
]


def capture(model, identity):
    from gliner2.classification import (
        Classifier,
        ClassificationConfig,
        ClassificationSchema,
    )

    rows = []
    for case_id, text, questions in REQUESTS:
        tasks, native = {}, []
        for name, question in questions.items():
            kind = question["type"]
            labels = (
                question["criteria"]
                if kind == "choice"
                else {str(i): v for i, v in enumerate(question["criteria"])}
                if kind == "score"
                else {"false": "False", "true": "True"}
            )
            tasks[name] = dict(
                instruction=question["instructions"],
                labels=labels,
                min_labels=1,
                max_labels=1,
            )
            if kind == "score":
                tasks[name]["ordered"] = True
            native.append(
                dict(
                    name=name,
                    prompt=question["instructions"],
                    top_k=len(labels),
                    labels=list(labels),
                    label_definitions={
                        k: {"description": v} for k, v in labels.items()
                    },
                )
            )
        schema = {"tasks": tasks}
        classifier = Classifier(model)
        compiled = classifier.compile_schema(ClassificationSchema.from_dict(schema))
        encoded = bench.encoded_evidence(model, text, compiled, boundary=False)
        if not 1 <= len(encoded["input_ids"]) <= 198:
            raise RuntimeError(
                f"{case_id}: {len(encoded['input_ids'])} tokens outside serving ceiling"
            )
        config = ClassificationConfig(
            on_infeasible="raise", max_len=bench.MAX_WORDS, include_confidence=True
        )
        scores = classifier.score(text, compiled, config=config)
        decoded = classifier.decode(scores, compiled, config=config)
        probabilities = {
            name: {label: scores.probability(name, label) for label in task["labels"]}
            for name, task in tasks.items()
        }
        evidence = [
            dict(
                name=name,
                labels=list(task["labels"]),
                raw_logits=list(scores.tasks[name].values()),
            )
            for name, task in tasks.items()
        ]
        answers = {}
        for name, question in questions.items():
            probs = probabilities[name]
            answer = {"type": question["type"]}
            if question["type"] == "choice":
                answer.update(choice=max(probs, key=probs.get), probabilities=probs)
            elif question["type"] == "score":
                answer.update(
                    score=sum(i * p for i, p in enumerate(probs.values())),
                    probabilities=probs,
                    legend=tasks[name]["labels"],
                )
            else:
                answer["noul"] = probs["true"]
            answers[name] = answer
        request = dict(
            model="fastino/GLiNER2.5-Decide-1B", state=text, questions=questions
        )
        rows.append(
            dict(
                id=case_id,
                text=text,
                schema=schema,
                encoded=encoded,
                native_schema_json=json.dumps(
                    {"classifications": native}, separators=(",", ":")
                ),
                decide_request_json=json.dumps(request, separators=(",", ":")),
                native_classification=dict(
                    input_ids=encoded["input_ids"], tasks=evidence
                ),
                selected={name: list(decoded.selected(name)) for name in tasks},
                probabilities=probabilities,
                decide_expected=dict(model=request["model"], answers=answers),
            )
        )
    return dict(
        format_version=1,
        model=identity,
        requests=[],
        public_decide_requests=rows,
        scope="Frozen short holdouts; CPU FP32 pinned upstream oracle; not semantic accuracy qualification",
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("model-dir", "upstream", "runtime-dir", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    a = p.parse_args()
    if a.output.exists():
        raise RuntimeError("refusing to overwrite a frozen capture")
    bench.configure_environment()
    contract = bench.strict_json(bench.CONTRACT)
    source = bench.verify_source(a.upstream, contract)
    identity = bench.family.verify_model(
        "decide_1b", a.model_dir, contract_path=bench.CONTRACT, verify_model_sha256=True
    )
    runtime = bench.verify_runtime_dir(
        a.runtime_dir, bench.strict_json(bench.RUNTIME_CONTRACT_1B)
    )
    bench.activate(a.runtime_dir, a.upstream)
    import torch
    from gliner2 import AutoExtractor

    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(0)
    model = (
        AutoExtractor.from_pretrained(
            str(a.model_dir.resolve()),
            local_files_only=True,
            map_location="cpu",
            use_flashdeberta=False,
        )
        .float()
        .eval()
    )
    bench.verify_model_device(model, torch, "cpu")
    rope = bench.verify_rope(
        a.model_dir, bench.strict_json(bench.RUNTIME_CONTRACT_1B), model.encoder
    )
    with torch.inference_mode():
        result = capture(model, identity)
    result.update(source=source, runtime=runtime, rope=rope)
    bench.atomic_write(a.output, result)
    print(
        json.dumps(
            {
                r["id"]: len(r["encoded"]["input_ids"])
                for r in result["public_decide_requests"]
            }
        )
    )
    print(bench.sha256(a.output))


if __name__ == "__main__":
    main()
