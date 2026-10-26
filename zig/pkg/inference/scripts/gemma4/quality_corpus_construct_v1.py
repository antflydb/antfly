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

"""Deterministically construct source-backed Gemma4 quality task blueprints.

The constructor reads only a sealed local source manifest and its referenced
bytes. It does not read model outputs, scores, calibration data, or profiles.
Renderer-aware prompt packing remains a later, separately evidenced step.
"""
import argparse
import ast
import hashlib
import json
import re
from pathlib import Path

import quality_corpus_prepare_v2 as prepare

BUCKETS = prepare.BUCKETS
CASES_PER_BUCKET = 20
MAX_SOURCE_BYTES = 256 * 1024
TEXT_TYPES = {"text", "markdown", "code", "config"}
CODE_EXPRESSION = re.compile(rb"(?m)^[ \t]*(?:pub[ \t]+)?const[ \t]+([A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*([0-9() \t+*/%<>&|^-]{1,96})[ \t]*;?[ \t]*$")
SCALAR = re.compile(rb"(?m)^[ \t]*([A-Za-z_][A-Za-z0-9_.-]{1,63})[ \t]*[:=][ \t]*([A-Za-z0-9_./-]{1,128})[ \t]*,?[ \t]*$")


def require(value, message):
    if not value:
        raise ValueError(message)


def clean_lines(raw):
    result, cursor = [], 0
    for with_end in raw.splitlines(keepends=True):
        line = with_end.rstrip(b"\r\n")
        leading = len(line) - len(line.lstrip())
        trailing = len(line.rstrip())
        start, end = cursor + leading, cursor + trailing
        cursor += len(with_end)
        if end - start < 12 or end - start > 240 or b"\x00" in raw[start:end]:
            continue
        try:
            raw[start:end].decode("utf-8")
        except UnicodeDecodeError:
            continue
        if raw.count(raw[start:end]) == 1:
            result.append((start, end))
    return result


def evaluate_integer_expression(raw):
    expression = raw.decode("ascii").strip().rstrip(";").strip()
    tree = ast.parse(expression, mode="eval")
    allowed = (ast.Expression, ast.Constant, ast.UnaryOp, ast.BinOp, ast.UAdd, ast.USub,
               ast.Add, ast.Sub, ast.Mult, ast.FloorDiv, ast.Mod, ast.LShift, ast.RShift,
               ast.BitAnd, ast.BitOr, ast.BitXor)
    require(all(isinstance(node, allowed) and not isinstance(node, ast.Constant) or
                isinstance(node, ast.Constant) and type(node.value) is int for node in ast.walk(tree)),
            "unsupported code expression")
    value = eval(compile(tree, "<sealed-source-expression>", "eval"), {"__builtins__": {}}, {})
    require(type(value) is int and -(2**63) <= value < 2**63, "code expression result out of range")
    return value


def task_for(bucket, source, raw):
    lines = clean_lines(raw)
    if bucket == "code":
        match = CODE_EXPRESSION.search(raw)
        if match is None:
            return None
        start, end = match.span(2)
        try:
            value = str(evaluate_integer_expression(raw[start:end])).encode()
        except (SyntaxError, ValueError, ZeroDivisionError, OverflowError):
            return None
        question = (b"Evaluate the cited integer constant expression using ordinary integer operator "
                    b"semantics. Return only the base-10 result.")
        oracle = "bounded_integer_expression_v1"
    elif bucket == "structured_tool":
        match = SCALAR.search(raw)
        if match is None:
            return None
        key, value = match.group(1), match.group(2)
        start, end = match.span(2)
        question = b"Emit canonical compact JSON with keys path and value for the cited scalar."
        value = json.dumps({"path": key.decode("ascii"), "value": value.decode("ascii")},
                           sort_keys=True, separators=(",", ":")).encode()
        oracle = "canonical_scalar_json_v1"
    elif bucket == "multiturn":
        if len(lines) < 2:
            return None
        first, replacement = lines[0], lines[-1]
        start, end = replacement
        value = raw[start:end]
        question = None
        oracle = "last_write_wins_exact_v1"
    elif bucket == "document_qa":
        match = SCALAR.search(raw)
        if match is None:
            return None
        key, value = match.group(1), match.group(2)
        start, end = match.span(2)
        question = b"What exact scalar value is assigned to key " + key + b" at the cited source range?"
        oracle = "source_scalar_lookup_v1"
    else:
        if not lines:
            return None
        start, end = lines[0 if bucket == "exact_retrieval" else len(lines) // 2]
        value = raw[start:end]
        question = (b"Return only the exact cited source line." if bucket == "exact_retrieval" else
                    b"What exact text appears at the cited source evidence range? Return it verbatim.")
        oracle = "exact_source_span_v1"
    require(0 <= start < end <= len(raw), "invalid derived source span")
    result = {"source_byte_start": start, "source_byte_end": end,
            "evidence_sha256": hashlib.sha256(raw[start:end]).hexdigest(),
            "reference_utf8": value.decode("utf-8"),
            "reference_sha256": hashlib.sha256(value).hexdigest(), "oracle": oracle,
            "authorship": source["authorship"]}
    if question is not None:
        result["question_utf8"] = question.decode("utf-8")
    else:
        result["messages"] = [
            {"role": "user", "content": "Remember the earlier cited source value exactly."},
            {"role": "assistant", "content": "Stored."},
            {"role": "user", "content": "Replace it with the later cited source value and return only the current value."},
        ]
        result["earlier_source_byte_start"], result["earlier_source_byte_end"] = first
    return result


def construct(source_manifest_ref, root, model, selection_seed, cases_per_bucket=CASES_PER_BUCKET):
    prepare.digest(selection_seed, "construction seed")
    manifest, sources, manifest_sha = prepare.validate_source_manifest(model, source_manifest_ref, root)
    candidates = {bucket: [] for bucket in BUCKETS}
    for family, source in sources.items():
        _, raw = prepare.campaign_file(root, {"path": source["path"], "sha256": source["sha256"]},
                                       "construction source", MAX_SOURCE_BYTES)
        for bucket in BUCKETS:
            task = task_for(bucket, source, raw)
            if task is None:
                continue
            rank = hashlib.sha256(bytes.fromhex(selection_seed) + b"\x00" + bucket.encode() +
                                  b"\x00" + family.encode()).hexdigest()
            candidates[bucket].append((rank, family, source, task))
    cases = []
    for bucket in BUCKETS:
        rows = sorted(candidates[bucket])
        require(len(rows) >= cases_per_bucket, "insufficient deterministic source families for " + bucket)
        for index, (_, family, source, task) in enumerate(rows[:cases_per_bucket]):
            source_start = task.pop("source_byte_start")
            source_end = task.pop("source_byte_end")
            citation = f"Source {source['path']} bytes [{source_start},{source_end}). "
            if "question_utf8" in task:
                task["question_utf8"] = citation + task["question_utf8"]
            else:
                earlier_start = task["earlier_source_byte_start"]
                earlier_end = task["earlier_source_byte_end"]
                task["messages"][0]["content"] = (
                    f"Source {source['path']} bytes [{earlier_start},{earlier_end}). " +
                    task["messages"][0]["content"])
                task["messages"][2]["content"] = citation + task["messages"][2]["content"]
            cases.append({"id": f"{model}-{bucket}-{index:03d}", "bucket": bucket,
                          "primary_source_family": family,
                          "source": {"path": source["path"], "sha256": source["sha256"],
                                     "byte_start": source_start, "byte_end": source_end},
                          "intended_compactable_quartile": index % 4,
                          "distractor_order_seed": hashlib.sha256(
                              bytes.fromhex(selection_seed) + b"\x00distractors\x00" +
                              bucket.encode() + b"\x00" + str(index).encode()).hexdigest(),
                          **task})
    return {"schema_version": 1, "stage": "quality_source_task_blueprints_diagnostic",
            "completed": True, "model": model, "selection_seed": selection_seed,
            "source_manifest_sha256": manifest_sha, "source_snapshot_sha256": manifest["snapshot_sha256"],
            "cases_per_bucket": cases_per_bucket, "buckets": list(BUCKETS), "cases": cases,
            "model_outputs_observed": False, "acceptance_threshold": None,
            "diagnostic_only": True, "production_quality_coverage": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--model", choices=prepare.MODELS, required=True)
    parser.add_argument("--selection-seed", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cases-per-bucket", type=int, default=CASES_PER_BUCKET)
    args = parser.parse_args(argv)
    require(1 <= args.cases_per_bucket <= CASES_PER_BUCKET, "invalid cases per bucket")
    source_ref = {"path": str(Path(args.source_manifest).relative_to(Path(args.root))),
                  "sha256": hashlib.sha256(Path(args.source_manifest).read_bytes()).hexdigest()}
    result = construct(source_ref, Path(args.root), args.model, args.selection_seed, args.cases_per_bucket)
    output = Path(args.output)
    require(not output.exists() and output.parent.resolve() == Path(args.root).resolve(),
            "output must be a new file directly below campaign root")
    with output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
