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

"""Score pinned classification captures against MASSIVE's public gold intents.

Uses the same preselected 200 test examples per language as the parity suite.
Gold accuracy and implementation parity are separate measurements. No inference
is rerun, no examples are selected by outcome, and no release gate is granted.
"""

import argparse
import gzip
import hashlib
import json
from pathlib import Path

from check_family_holdout import compare_rows
from check_family_reference_holdout import pinned_bytes
from family_oracle import FIXTURES


def selected_labels(case, native, model):
    labels = case["tasks"]["intent"]
    if model == "decide_1b":
        results = []
        for row in native["decisions"]:
            if (
                len(row["tasks"]) != 1
                or row["tasks"][0]["task_index"] != 0
                or len(row["tasks"][0]["selections"]) != 1
            ):
                raise ValueError("expected one intent selection per document")
            results.append(labels[row["tasks"][0]["selections"][0]["label_index"]])
        return results
    outputs = native["outputs"] if len(case["texts"]) > 1 else [native["output"]]
    results = []
    for row in outputs:
        tasks = row["classifications"]
        if (
            len(tasks) != 1
            or tasks[0]["name"] != "intent"
            or len(tasks[0]["labels"]) != 1
        ):
            raise ValueError("expected one intent selection per document")
        results.append(tasks[0]["labels"][0]["label"])
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import pyarrow.parquet as pq

    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    baseline_bytes = pinned_bytes(args.report, manifest)
    baseline = json.loads(baseline_bytes)
    if (
        baseline["status"] != "complete"
        or baseline.get("task", "classification") != "classification"
        or baseline["scope"] != "public_test_classification_parity"
    ):
        raise ValueError("expected complete classification parity report")
    models = [
        key for key, pin in manifest["models"].items() if pin == baseline["model"]
    ]
    if len(models) != 1:
        raise ValueError("model pin mismatch")
    model = models[0]
    source = json.loads(pinned_bytes(FIXTURES / "holdout/source.json", manifest))
    if baseline["dataset"] != source:
        raise ValueError("dataset mismatch")
    capture = baseline["raw_capture"]
    data = gzip.decompress(pinned_bytes(args.report.parent / capture["path"], manifest))
    if hashlib.sha256(data).hexdigest() != capture["uncompressed_sha256"]:
        raise ValueError("capture hash mismatch")
    captured = {}
    for line in data.splitlines():
        row = json.loads(line)
        key = row["locale"], row["case"]
        if key in captured:
            raise ValueError("duplicate capture")
        captured[key] = row
    report = dict(
        qualification=False,
        scope="massive_public_test_gold_intent_accuracy",
        model=baseline["model"],
        precision=baseline["precision"],
        dataset=source,
        native_binary_sha256=baseline["native_binary_sha256"],
        reference=baseline["reference"],
        parity_report=str(args.report.resolve().relative_to(FIXTURES.resolve())),
        parity_report_sha256=hashlib.sha256(baseline_bytes).hexdigest(),
        raw_capture_sha256=capture["uncompressed_sha256"],
        note="Preselected public test subset; training-data independence is not established.",
        slices=[],
    )
    visited = set()
    for filename, pin in source["files"].items():
        locale = filename.split("/")[0]
        path = args.source_dir / (locale + ".parquet")
        raw = path.read_bytes()
        if (
            len(raw) != pin["size_bytes"]
            or hashlib.sha256(raw).hexdigest() != pin["sha256"]
        ):
            raise ValueError("gold dataset file hash mismatch")
        table = pq.read_table(path)
        labels = json.loads(table.schema.metadata[b"huggingface"])["info"]["features"][
            "intent"
        ]["names"]
        source_rows = table.to_pylist()
        gold = {str(row["id"]): row for row in source_rows}
        if len(gold) != len(source_rows):
            raise ValueError("duplicate source IDs")
        fixture = json.loads(
            pinned_bytes(FIXTURES / "holdout/cases" / (locale + ".json"), manifest)
        )
        rows = []
        for case in fixture["cases"]:
            key = locale, case["id"]
            if key in visited or case["tasks"] != {"intent": labels}:
                raise ValueError("case identity or intent vocabulary mismatch")
            visited.add(key)
            capture_row = captured[key]
            parity = compare_rows(
                case,
                capture_row["native"],
                capture_row["reference"],
                model,
                5e-4 if baseline["precision"] == "fp32" else 5e-3,
            )
            actual = selected_labels(case, capture_row["native"], model)
            for source_id, text, predicted, reference, match in zip(
                case["source_ids"],
                case["texts"],
                actual,
                capture_row["reference"]["outputs"],
                parity,
                strict=True,
            ):
                truth = gold[source_id]
                if (
                    truth["utt"] != text
                    or truth["partition"] != "test"
                    or truth["locale"] != locale
                ):
                    raise ValueError("gold source identity mismatch")
                expected = labels[truth["intent"]]
                oracle = reference["intent"]["label"]
                rows.append(
                    dict(
                        source_id=source_id,
                        gold=expected,
                        native=predicted,
                        fastino_fp32=oracle,
                        native_correct=predicted == expected,
                        fastino_correct=oracle == expected,
                        parity_passed=match["passed"],
                    )
                )
        if len(rows) != 200 or len({r["source_id"] for r in rows}) != 200:
            raise ValueError("expected 200 unique preselected documents")
        result = dict(locale=locale, documents=len(rows), rows=rows)
        for arm in ("native", "fastino"):
            result[arm + "_correct"] = sum(row[arm + "_correct"] for row in rows)
            result[arm + "_accuracy"] = result[arm + "_correct"] / len(rows)
        report["slices"].append(result)
    if visited != captured.keys():
        raise ValueError("capture differs from complete holdout")
    report["documents"] = sum(s["documents"] for s in report["slices"])
    for arm in ("native", "fastino"):
        report[arm + "_accuracy"] = (
            sum(s[arm + "_correct"] for s in report["slices"]) / report["documents"]
        )
    report["status"] = "complete"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        stream.write(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("documents", "native_accuracy", "fastino_accuracy")
            }
        )
    )


if __name__ == "__main__":
    main()
