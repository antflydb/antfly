#!/usr/bin/env python3
"""Generate or verify a pinned GLiNER2.5-Decide FP32 oracle fixture.

The script imports a caller-supplied, reviewed GLiNER2 source checkout. It
downloads the immutable model revision, verifies the reviewed weight bytes,
then records token IDs, [L] positions, raw logits, probabilities, and winners.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

REPO = "fastino/GLiNER2.5-Decide"
REVISION = "7ee5da4c2415e32259bcdc0b1a7367c32ce8d6f6"
WEIGHT_SHA256 = "40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997"
WEIGHT_SIZE = 1_945_828_140
L_TOKEN_ID = 128007

CASES = (
    {
        "name": "mixed_product_review",
        "text": "The camera is excellent, but the battery fails before lunch.",
        "tasks": (
            ("sentiment", ("positive", "negative", "neutral")),
            ("issue", ("camera", "battery", "performance", "none")),
        ),
    },
    {
        "name": "support_request",
        "text": "Please refund the duplicate charge. I do not need technical help.",
        "tasks": (
            ("intent", ("refund", "technical_support", "sales")),
            ("urgency", ("low", "medium", "high")),
        ),
    },
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_sha256(source_root: Path) -> str:
    digest = hashlib.sha256()
    for source in sorted((source_root / "gliner2").rglob("*.py")):
        digest.update(source.relative_to(source_root).as_posix().encode())
        digest.update(b"\0")
        digest.update(source.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def stable_float(value: float) -> float:
    return float(f"{value:.9g}")


def generate(source_root: Path, model_dir: Path | None = None) -> dict:
    sys.path.insert(0, str(source_root.resolve()))
    import torch
    from huggingface_hub import snapshot_download
    from gliner2 import AutoExtractor
    from gliner2.classification.compiler import compile_schema
    from gliner2.classification.schema import ClassificationSchema
    from gliner2.classification.scoring import ClassificationScorer

    snapshot = model_dir.resolve() if model_dir is not None else Path(
        snapshot_download(repo_id=REPO, revision=REVISION)
    )
    weight = snapshot / "model.safetensors"
    if weight.stat().st_size != WEIGHT_SIZE or sha256(weight) != WEIGHT_SHA256:
        raise RuntimeError("pinned GLiNER2.5-Decide weight identity mismatch")

    torch.manual_seed(0)
    torch.use_deterministic_algorithms(True)
    model = AutoExtractor.from_pretrained(str(snapshot), map_location="cpu")
    model.eval()
    scorer = ClassificationScorer(model, device="cpu", dtype=torch.float32).eval()
    outputs = []
    for case in CASES:
        schema = ClassificationSchema()
        for task, labels in case["tasks"]:
            schema.single(task, labels)
        compiled = compile_schema(schema)
        batch = model.processor.collate_fn_inference(
            [(case["text"], compiled.build())], max_len=512
        )
        ids = batch.input_ids[0].tolist()
        active = int(batch.attention_mask[0].sum().item())
        ids = ids[:active]
        marker_positions = [i for i, token_id in enumerate(ids) if token_id == L_TOKEN_ID]
        expected_markers = sum(len(labels) for _, labels in case["tasks"])
        if len(marker_positions) != expected_markers:
            raise RuntimeError(f"{case['name']}: expected {expected_markers} [L] markers")

        scores = scorer.score(case["text"], compiled, max_len=512)
        task_output = {}
        for task, labels in case["tasks"]:
            logits = {label: stable_float(scores.logit(task, label)) for label in labels}
            probabilities = {
                label: stable_float(scores.probability(task, label)) for label in labels
            }
            winner = max(labels, key=lambda label: (probabilities[label], -labels.index(label)))
            task_output[task] = {
                "labels": list(labels),
                "logits": logits,
                "probabilities": probabilities,
                "winner": winner,
            }
        outputs.append(
            {
                "name": case["name"],
                "text": case["text"],
                "input_ids": ids,
                "l_marker_positions": marker_positions,
                "schema_tokens": batch.schema_tokens_list[0],
                "tasks": task_output,
            }
        )
    return {
        "format": "antfly_gliner25_decide_oracle_v1",
        "model": {
            "repo": REPO,
            "revision": REVISION,
            "model_safetensors_size": WEIGHT_SIZE,
            "model_safetensors_sha256": WEIGHT_SHA256,
        },
        "runtime": {"device": "cpu", "dtype": "float32", "max_len": 512},
        "upstream_source_sha256": source_sha256(source_root),
        "cases": outputs,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, type=Path,
                        help="reviewed GLiNER2 source checkout containing gliner2/")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--model-dir", type=Path,
                        help="local copy of the pinned checkpoint; avoids a Hub download")
    parser.add_argument("--check", action="store_true",
                        help="compare generated canonical JSON with --output")
    args = parser.parse_args()
    actual = json.dumps(generate(args.source_root, args.model_dir), indent=2, sort_keys=True) + "\n"
    if args.check:
        expected = args.output.read_text(encoding="utf-8")
        if actual != expected:
            print(f"oracle fixture differs: {args.output}", file=sys.stderr)
            return 1
        print(f"oracle fixture matches: {args.output}")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(actual, encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
