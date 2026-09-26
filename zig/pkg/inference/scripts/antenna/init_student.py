#!/usr/bin/env python3
"""Initialize an Antenna student: a GLiNER2.5 boundary checkpoint on a pretrained
ModernBERT (or mmBERT) encoder with freshly initialized published heads.

Runs on the pinned GLiNER2.5 oracle (``scripts/gliner25/oracle.py``). The
encoder snapshot is downloaded first, at an exact revision, because the oracle
runtime is offline. The saved checkpoint is what the native training source
loads (``antfly-inference finetune train gliner25``); ``processor.json`` pins
the upstream token ids and routes for this tokenizer, in the format of
``testdata/gliner25/modernbert_tokenizer/processor.json``.

    PYTHONDONTWRITEBYTECODE=1 <oracle venv>/bin/python init_student.py \\
        --upstream <GLiNER2 checkout> --output <dir outside Git>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "gliner25"))

import oracle  # noqa: E402
import modernbert_reference as reference  # noqa: E402

ENCODER = "answerdotai/ModernBERT-base"
ENCODER_REVISION = "8949b909ec900327062f0ebf497f51aef5e6f0c8"
HEAD_SEED = 20260925


def download(repo: str, revision: str, destination: Path) -> Path:
    from huggingface_hub import snapshot_download

    return Path(snapshot_download(repo, revision=revision, local_dir=str(destination),
                                  allow_patterns=["config.json", "model.safetensors", "tokenizer.json",
                                                  "tokenizer_config.json", "special_tokens_map.json"]))


def build(args: argparse.Namespace) -> dict[str, Any]:
    encoder_dir = download(args.encoder, args.revision, args.output.parent / f".{args.output.name}-encoder")
    provenance, torch = oracle.prepare_runtime(args.upstream)
    from gliner2 import BoundaryExtractor, ExtractorConfig

    head = oracle.read_json(oracle.FIXTURES / "models" / "base" / "config.json")["boundary_head"]
    config = ExtractorConfig(model_name=str(encoder_dir), architecture="boundary", boundary_head=head,
                             token_pooling="first")
    torch.manual_seed(args.seed)
    model = BoundaryExtractor(config, use_flashdeberta=False).float().cpu().eval()
    if model.encoder.config.model_type != "modernbert":
        raise oracle.ContractError("the student encoder must be ModernBERT")
    # The saved name is provenance, not a path; ModernBERT is identified by
    # encoder_config/config.json.
    model.config.model_name = f"{args.encoder}@{args.revision}"
    heads_from = None
    if args.heads_from:
        # Trained boundary heads from a checkpoint of the same width (e.g.
        # gliner2.5-base on DeBERTa-v3-base, 768) replace the fresh heads;
        # only the encoder underneath changes.
        from safetensors.torch import load_file

        source = load_file(str(args.heads_from / "model.safetensors"))
        heads = {name: tensor for name, tensor in source.items() if not name.startswith("encoder.")}
        own = model.state_dict()
        for name, tensor in heads.items():
            if name not in own or own[name].shape != tensor.shape:
                raise oracle.ContractError(f"head tensor does not fit the student: {name}")
        missing = sorted(name for name in own if not name.startswith("encoder.") and name not in heads)
        if missing:
            raise oracle.ContractError(f"source lacks student head tensors: {missing[:5]}")
        model.load_state_dict(heads, strict=False)
        heads_from = {"path": str(args.heads_from), "tensors": len(heads),
                      "model_sha256": oracle.sha256_file(args.heads_from / "model.safetensors")}
    tokenizer = model.processor.tokenizer
    batch = model.processor.collate_fn_inference(
        [(case["text"], oracle.build_extract_schema(case["upstream_schema"]).build()) for case in reference.CASES],
        max_len=reference.MAX_WORDS, architecture="boundary", error_policy="raise", build_targets=False,
        on_capacity_exceeded="raise")
    routes = {"text": (batch.text_word_indices, batch.text_word_mask),
              "query": (batch.query_marker_indices, batch.query_marker_mask),
              "cls": (batch.cls_marker_indices, batch.cls_marker_mask)}
    with oracle.atomic_output_directory(args.output) as directory:
        model.save_pretrained(str(directory))
        oracle.write_json(directory / "processor.json", {
            "format_version": 1, "upstream_commit": oracle.UPSTREAM_COMMIT,
            "generator_sha256": oracle.sha256_file(Path(__file__)),
            "cases": [{key: case[key] for key in ("id", "text", "native_schema")} for case in reference.CASES],
            "tokenization": {word: tokenizer.tokenize(word) for word in ("john", "works", "apple", "café", "東京")},
            "input_ids": batch.input_ids.tolist(), "attention_mask": batch.attention_mask.tolist(),
            "routes": {kind: {"indices": indices.tolist(), "mask": mask.tolist()}
                       for kind, (indices, mask) in routes.items()},
        })
        oracle.write_json(directory / "student.json", {
            "format_version": 1, "encoder": args.encoder, "encoder_revision": args.revision,
            "head_seed": args.seed, "head_settings": "published gliner2.5-base boundary_head", "heads_from": heads_from,
            "vocab_size": model.encoder.config.vocab_size, "tokenizer_length": len(tokenizer),
            "parameters": sum(p.numel() for p in model.parameters()),
            "provenance": provenance, "generator_sha256": oracle.sha256_file(Path(__file__)),
        })
        oracle.verify_upstream_checkout(args.upstream)
    return {"status": "initialized", "output": str(args.output.resolve()),
            "vocab_size": model.encoder.config.vocab_size, "parameters": sum(p.numel() for p in model.parameters())}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--encoder", default=ENCODER)
    parser.add_argument("--revision", default=ENCODER_REVISION)
    parser.add_argument("--seed", type=int, default=HEAD_SEED)
    parser.add_argument("--heads-from", type=Path, help="checkpoint whose trained heads replace the fresh ones (same width)")
    print(json.dumps(build(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
