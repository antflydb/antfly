#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare frozen and current GLiNER2 preprocessing in separate Python processes.

This audit observes tokenizer/processor drift without granting current upstream
any role in the frozen numerical oracle or changing downloaded checkpoint bytes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from capture_oracle import cases


def tree_digest(root):
    digest = hashlib.sha256()
    for path in sorted((root / "gliner2").rglob("*.py")):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def worker(source, model_dir, profile):
    sys.path.insert(0, str(source.resolve()))
    from gliner2 import Schema
    from gliner2.models.base import load_extractor_tokenizer
    from gliner2.processor import SchemaTransformer
    processor = SchemaTransformer(tokenizer=load_extractor_tokenizer(str(model_dir.resolve())), token_pooling="first")
    output = []
    for case in cases(profile):
        schema = Schema()
        if profile == "pii":
            schema.entities(case["schema"]["entities"])
        else:
            for task in case["schema"]["classifications"]:
                schema.classification(task["name"], task["labels"], multi_label=task.get("multi_label", False),
                                      cls_threshold=task.get("threshold", 0.5))
        batch = processor.collate_fn_inference([(case["text"], schema.build())], max_len=None, error_policy="raise")
        output.append({"id": case["id"], "input_ids": batch.input_ids[0].tolist(),
                       "schema_tokens": batch.schema_tokens_list[0], "text_tokens": batch.text_tokens[0],
                       "start_mapping": batch.start_mappings[0], "end_mapping": batch.end_mappings[0]})
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frozen-source", type=Path)
    parser.add_argument("--current-source", type=Path)
    parser.add_argument("--current-commit")
    parser.add_argument("--worker-source", type=Path)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--profile", choices=["pii", "gliguard"], required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.worker_source:
        print(json.dumps(worker(args.worker_source, args.model_dir, args.profile), ensure_ascii=False))
        return
    if not args.frozen_source or not args.current_source or not args.output or not args.current_commit:
        parser.error("audit requires frozen/current source, exact current commit, and output")
    captures = []
    for source in [args.frozen_source, args.current_source]:
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "HF_HUB_OFFLINE": "1"}
        result = subprocess.run([sys.executable, __file__, "--worker-source", str(source),
                                 "--model-dir", str(args.model_dir), "--profile", args.profile],
                                env=env, check=True, capture_output=True, text=True)
        captures.append(json.loads(result.stdout))
    differences = []
    for frozen, current in zip(*captures, strict=True):
        changed = [key for key in frozen if frozen[key] != current[key]]
        if changed:
            differences.append({"id": frozen["id"], "changed": changed, "frozen": frozen, "current": current})
    report = {"profile": args.profile, "frozen_commit": "3c913c7369301133d3b7699252074c4303ada50e",
              "current_commit": args.current_commit, "frozen_source_sha256": tree_digest(args.frozen_source),
              "current_source_sha256": tree_digest(args.current_source), "cases": len(captures[0]),
              "preprocessing_equal": not differences, "differences": differences}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
