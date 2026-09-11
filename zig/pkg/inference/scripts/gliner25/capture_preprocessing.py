#!/usr/bin/env python3
"""Capture boundary processor routes with the pinned published tokenizer."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path
from typing import Any

import oracle


def fixture_schemas() -> tuple[Any, Any]:
    from gliner2 import AttributeGroup, Schema

    rich = Schema().entities({"person": "A named person", "organization": "An employer"})
    rich.entity_attributes({
        "sentiment": AttributeGroup(labels=["positive", "negative"], applies_to=["person"], qualify_labels=True),
        "status": AttributeGroup(labels=["active", "former"], multi_label=True, qualify_labels=True),
    })
    rich.structure("review").field("product", dtype="str", description="The reviewed product").field(
        "rating", dtype="str", choices=["great", "poor"], description="Overall product rating",
    )
    rich.classification("sentiment", {"positive": "A favorable review", "negative": "An unfavorable review"},
                        prompt="Choose the review sentiment", examples=[("It works well", "positive"), ("It broke", "negative")])
    rich.relations({"works_for": {"description": "A person is employed by an organization"}})
    plain = Schema().entities(["location", "person"])
    return rich, plain


def describe_batch(batch: Any) -> dict[str, Any]:
    scalar_names = (
        "mapped_indices", "schema_counts", "original_lengths", "structure_labels", "task_types",
        "text_tokens", "schema_tokens_list", "start_mappings", "end_mappings", "original_texts",
        "original_schemas", "text_word_counts", "schema_special_indices", "model_texts",
    )
    tensor_names = (
        "input_ids", "attention_mask", "text_word_indices", "text_word_mask",
        "query_marker_indices", "query_marker_mask", "query_group_index", "cls_marker_indices",
        "cls_marker_mask", "cls_group_index",
    )
    result = {name: getattr(batch, name) for name in scalar_names}
    result["query_layouts"] = [dataclasses.asdict(layout) for layout in batch.query_layouts]
    result["tensors"] = {
        name: {"shape": list(tensor.shape), "dtype": str(tensor.dtype).removeprefix("torch."),
               "values": tensor.reshape(-1).tolist()}
        for name in tensor_names if (tensor := getattr(batch, name)) is not None
    }
    return result


def capture(source: Path, model_dir: Path, destination: Path) -> dict[str, Any]:
    provenance, _ = oracle.prepare_runtime(source)
    bundle = oracle.verify_model_dir("small", model_dir)
    from gliner2.models.base import load_extractor_tokenizer
    from gliner2.processor import SchemaTransformer

    tokenizer = load_extractor_tokenizer(str(model_dir.resolve()))
    rich, plain = fixture_schemas()
    unicode_text = "İpek ΟΣ ς 東京 🙂 café é https://example.com/a @User user@example.com"
    specs = [
        ("rich_schema", "whitespace", [("John reviews a great iPhone at Apple", rich)]),
        ("heterogeneous_batch", "whitespace", [("John works at Apple", rich), ("Alice visits 東京", plain)]),
        ("unicode_whitespace", "whitespace", [(unicode_text, plain)]),
        ("unicode_char", "char", [(unicode_text, plain)]),
        ("empty_text", "whitespace", [("", plain)]),
    ]
    rows = []
    for name, splitter, samples in specs:
        processor = SchemaTransformer(tokenizer=tokenizer, token_pooling="first", word_splitter=splitter)
        raw = [(text, schema.build()) for text, schema in samples]
        for text, _ in raw:
            if len(list(processor.word_splitter(text))) > oracle.MAX_WORDS:
                raise oracle.ContractError("preprocessing fixture exceeds word budget")
        batch = processor.collate_fn_inference(raw, max_len=oracle.MAX_WORDS, architecture="boundary",
                                               build_targets=False, error_policy="raise", on_capacity_exceeded="raise")
        if batch.input_ids.shape[-1] > oracle.MAX_ENCODED_TOKENS or batch.query_marker_mask.shape[-1] > oracle.MAX_QUERIES:
            raise oracle.ContractError("preprocessing fixture exceeds token/query budget")
        rows.append({"id": name, "word_splitter": splitter,
                     "samples": [{"text": text, "schema": schema} for text, schema in raw],
                     "expected": describe_batch(batch)})
    oracle.verify_upstream_checkout(source)
    if oracle.verify_model_dir("small", model_dir) != bundle:
        raise oracle.ContractError("model bundle changed during preprocessing capture")
    report = {"format_version": 1, "scope": "preprocessing_reference", "provenance": provenance, "model": bundle,
              "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
              "real_model_qualified": False, "native_runtime_qualified": False, "training_qualified": False,
              "offset_unit": "unicode_codepoints", "cases": rows}
    with oracle.atomic_output_directory(destination) as output:
        # Map insertion order is semantic for schemas; preserve it in the raw
        # fixture so feeding it back into either processor retains declarations.
        (output / "preprocessing.json").write_text(json.dumps(report, ensure_ascii=False, allow_nan=False, indent=2) + "\n", encoding="utf-8")
    return {"status": "captured", "file": str(destination / "preprocessing.json"),
            "sha256": oracle.sha256_file(destination / "preprocessing.json")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(capture(args.upstream, args.model_dir, args.output), sort_keys=True))
        return 0
    except (oracle.ContractError, OSError, ImportError, RuntimeError, ValueError, TypeError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
