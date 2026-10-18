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

"""Independently score an exported span-head probe with the pinned upstream.

The source runtime is unmodified. Verify every tensor against the pinned source,
permit changes only in the six declared trainable tensors, then compare decoded
spans and confidence against an optional native evaluation report. Never grant
runtime qualification from this diagnostic command.
"""

import argparse
import hashlib
import json
import mmap
import struct
import sys
from pathlib import Path

from family_oracle import verify_model
from source_pin import verify_source

PREFIXES = ("classifier.2", "count_pred.2", "span_rep.span_rep_layer.out_project.3")
ALLOWED = {
    prefix + "." + suffix for prefix in PREFIXES for suffix in ("weight", "bias")
}


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            value.update(block)
    return value.hexdigest()


def header(mapping):
    size = struct.unpack("<Q", mapping[:8])[0]
    if not 0 < size <= 4 << 20:
        raise ValueError("invalid tensor header")
    values = json.loads(mapping[8 : 8 + size])
    values.pop("__metadata__", None)
    return 8 + size, values


def verify_tensors(source, trained):
    changed = []
    with (
        source.open("rb") as left,
        trained.open("rb") as right,
        mmap.mmap(left.fileno(), 0, access=mmap.ACCESS_READ) as a,
        mmap.mmap(right.fileno(), 0, access=mmap.ACCESS_READ) as b,
    ):
        a_start, a_header = header(a)
        b_start, b_header = header(b)
        if a_header.keys() != b_header.keys():
            raise ValueError("tensor inventory changed")
        for name, first in a_header.items():
            last = b_header[name]
            if (
                first["shape"] != last["shape"]
                or first["dtype"] != last["dtype"]
                or first["dtype"] != "F32"
            ):
                raise ValueError(f"tensor contract changed: {name}")
            a_range, b_range = first["data_offsets"], last["data_offsets"]
            first_hash = hashlib.sha256(
                memoryview(a)[a_start + a_range[0] : a_start + a_range[1]]
            ).digest()
            last_hash = hashlib.sha256(
                memoryview(b)[b_start + b_range[0] : b_start + b_range[1]]
            ).digest()
            if first_hash != last_hash:
                if name not in ALLOWED:
                    raise ValueError(f"frozen tensor changed: {name}")
                changed.append(name)
    return {
        "changed_tensors": changed,
        "encoder_unchanged": True,
        "classifier_unchanged": not any(
            name.startswith("classifier.") for name in changed
        ),
    }


def coordinates(text, value):
    start, end = value["start"], value["end"]
    if not 0 <= start < end <= len(text):
        raise ValueError(
            f"upstream prediction exceeds original source: {start}..{end}/{len(text)}"
        )
    if text[start:end] != value["text"]:
        raise ValueError("upstream text and offsets differ")
    return len(text[:start].encode()), len(text[:end].encode())


def canonical(text, output):
    entities, relations = [], []
    for name, values in output.get("entities", {}).items():
        for value in values if isinstance(values, list) else [values]:
            start, end = coordinates(text, value)
            entities.append(
                {
                    "label": name,
                    "start": start,
                    "end": end,
                    "confidence": value["confidence"],
                }
            )
    for name, values in output.get("relation_extraction", {}).items():
        for value in values:
            hs, he = coordinates(text, value["head"])
            ts, te = coordinates(text, value["tail"])
            # Upstream supplies separate role confidences, not a pair score.
            # Native's explicit pair confidence is the weaker role score.
            relations.append(
                {
                    "type": name,
                    "head": {"start": hs, "end": he},
                    "tail": {"start": ts, "end": te},
                    "confidence": min(
                        value["head"]["confidence"], value["tail"]["confidence"]
                    ),
                }
            )
    return {"entities": entities, "relations": relations}


def key(value):
    if "label" in value:
        return value["label"], value["start"], value["end"]
    return (
        value["type"],
        value["head"]["start"],
        value["head"]["end"],
        value["tail"]["start"],
        value["tail"]["end"],
    )


def compare(actual, expected, tolerance):
    errors = []
    for field in ("entities", "relations"):
        first, last = sorted(actual[field], key=key), sorted(expected[field], key=key)
        if [key(v) for v in first] != [key(v) for v in last]:
            errors.append(field + " spans differ")
            continue
        if any(
            abs(a["confidence"] - b["confidence"]) > tolerance
            for a, b in zip(first, last, strict=True)
        ):
            errors.append(field + " confidences differ")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--source-model", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--rows", type=Path, required=True)
    parser.add_argument("--native-report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = verify_source(args.upstream)
    verify_model(args.source_model, "decide_1b")
    receipt = json.loads(
        (args.model_dir / "antfly_gliner_span_training.json").read_text()
    )
    weights = args.model_dir / "model.safetensors"
    weights_sha256 = digest(weights)
    if weights_sha256 != receipt["weights_sha256"]:
        raise ValueError("trained artifact differs from its receipt")
    frozen = verify_tensors(args.source_model / "model.safetensors", weights)
    native = json.loads(args.native_report.read_text()) if args.native_report else None
    if native and native["weights_sha256"] != weights_sha256:
        raise ValueError("native report describes another artifact")
    native_rows = {row["id"]: row for row in native["rows"]} if native else {}
    rows = [json.loads(line) for line in args.rows.read_text().splitlines() if line]
    if native and set(native_rows) != {row["id"] for row in rows}:
        raise ValueError("native corpus differs")
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    from gliner2 import AutoExtractor
    from oracle import build_extract_schema

    torch.set_num_threads(2)
    torch.use_deterministic_algorithms(True)
    model = AutoExtractor.from_pretrained(
        str(args.model_dir), map_location="cpu"
    ).eval()
    results, failures = [], []
    for row in rows:
        specification = dict(row["schema"])
        # Antenna rows use Antfly's typed relation objects. Upstream's public
        # schema accepts a name/config dictionary for the same ordered labels.
        if "relations" in specification:
            specification["relations"] = {
                relation["type"]: {k: v for k, v in relation.items() if k != "type"}
                for relation in specification["relations"]
            }
        schema = build_extract_schema(specification)
        batch = model.processor.collate_fn_inference(
            [(row["text"], schema.build())],
            max_len=None,
            architecture="span",
            error_policy="raise",
            build_targets=False,
            on_capacity_exceeded="raise",
        )
        ids = batch.input_ids[0].tolist()
        if len(ids) > 1024:
            raise ValueError("probe exceeds admitted single-window geometry")
        with torch.inference_mode():
            output = model.extract(
                row["text"],
                schema,
                threshold=0.5,
                include_confidence=True,
                include_spans=True,
                max_len=1024,
                overlap_policy="flat",
            )
        record = {"id": row["id"], "input_ids": ids, "raw_output": output}
        try:
            record.update(canonical(row["text"], output))
            if native:
                match = native_rows[row["id"]]
                if match["prompt_tokens"] != len(ids) or (
                    "input_ids" in match and match["input_ids"] != ids
                ):
                    raise ValueError("native token identity differs")
                errors = compare(match, record, 5e-4)
                if errors:
                    failures.append({"id": row["id"], "errors": errors})
        except ValueError as error:
            failures.append({"id": row["id"], "errors": [str(error)]})
        results.append(record)
        args.output.write_text(
            json.dumps(
                {
                    "scope": "independent upstream decoded span-head evaluation",
                    "qualification": False,
                    "source_tree_sha256": source,
                    "weights_sha256": weights_sha256,
                    "data_sha256": digest(args.rows),
                    "frozen": frozen,
                    "complete": len(results) == len(rows),
                    "failures": failures,
                    "rows": results,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n"
        )
        print(
            row["id"],
            "decoded",
            len(record.get("entities", [])),
            len(record.get("relations", [])),
            flush=True,
        )
    if failures:
        raise SystemExit(
            f"{len(failures)} decoded parity failures; see retained output"
        )


if __name__ == "__main__":
    main()
