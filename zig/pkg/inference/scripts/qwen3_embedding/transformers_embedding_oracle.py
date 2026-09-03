#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Emit the deterministic Qwen3-Embedding-0.6B Transformers golden reference.

The oracle runs the pinned ``Qwen/Qwen3-Embedding-0.6B`` checkpoint exactly as
the model card prescribes: fp32 weights, eager attention, left padding,
last-token (EOS) pooling, then L2 normalization. It embeds a fixed prompt set
and records, per case, the exact token ids fed to the model (the current HF
tokenizer auto-appends the ``<|endoftext|>`` EOS via its TemplateProcessing
post-processor; the oracle verifies exactly one trailing EOS and never appends
one manually) plus embeddings at Matryoshka dims 1024, 256, and 32, where the
reduced dims are truncate-then-renormalize projections of the 1024 vector.
Each case runs as a batch of one so the reference numerics are independent of
cross-case padding.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import sys
from typing import Any


SCHEMA = "antfly.qwen3_embedding.transformers_oracle.v1"
MODEL_ID = "Qwen/Qwen3-Embedding-0.6B"
DEFAULT_REVISION = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
EXPECTED_ARCHITECTURE = "Qwen3ForCausalLM"
EOS_TOKEN_ID = 151_643
HIDDEN_SIZE = 1_024
NUM_HIDDEN_LAYERS = 28
DEFAULT_MAX_LENGTH = 8_192
OUTPUT_DIMS = (1_024, 256, 32)
MRL_MIN_DIM = 32
FLOAT_DECIMALS = 8
QUERY_TEMPLATE = "Instruct: {instruction}\nQuery:{text}"
DEFAULT_INSTRUCTION = (
    "Given a web search query, retrieve relevant passages that answer the query"
)
CUSTOM_INSTRUCTION = (
    "Given a question about world capitals, retrieve the passage that answers it"
)
SHARED_TEXT = "The Great Wall of China is visible from low Earth orbit."
LONG_SENTENCE = "Antfly stores documents in shards and serves hybrid search across them. "
LONG_REPEATS = 900

EXPECTED_VERSIONS = {
    "numpy": "2.4.2",
    "torch": "2.10.0",
    "transformers": "5.1.0",
}


class OracleError(RuntimeError):
    pass


def installed_versions() -> dict[str, str]:
    return {name: importlib.metadata.version(name) for name in EXPECTED_VERSIONS}


def verify_environment(allow_mismatch: bool = False) -> dict[str, str]:
    actual = installed_versions()
    mismatches = {
        name: {"expected": expected, "actual": actual[name]}
        for name, expected in EXPECTED_VERSIONS.items()
        if actual[name] != expected
    }
    if mismatches:
        if allow_mismatch:
            print(
                "WARNING: oracle dependency mismatch (accepted via --allow-env-mismatch): "
                + json.dumps(mismatches, sort_keys=True),
                file=sys.stderr,
            )
        else:
            raise OracleError(f"oracle dependency mismatch: {json.dumps(mismatches, sort_keys=True)}")
    return actual


def format_query(text: str, instruction: str) -> str:
    """Render the official query prompt. Note: no space after ``Query:``."""

    return QUERY_TEMPLATE.format(instruction=instruction, text=text)


def render_case_text(case: dict[str, Any]) -> str:
    role = case["role"]
    if role == "query":
        instruction = case["instruction"]
        if not isinstance(instruction, str) or not instruction:
            raise OracleError(f"query case {case['id']!r} lacks an instruction")
        return format_query(case["text"], instruction)
    if role == "document":
        if case["instruction"] is not None:
            raise OracleError(f"document case {case['id']!r} must not carry an instruction")
        return case["text"]
    raise OracleError(f"unknown oracle role: {role!r}")


def verify_single_trailing_eos(token_ids: list[int]) -> None:
    """The pinned tokenizer must append exactly one trailing EOS itself."""

    if not token_ids:
        raise OracleError("tokenizer produced an empty sequence")
    if token_ids[-1] != EOS_TOKEN_ID:
        raise OracleError(f"sequence does not end with EOS {EOS_TOKEN_ID}: tail={token_ids[-4:]}")
    if token_ids.count(EOS_TOKEN_ID) != 1:
        raise OracleError(
            f"expected exactly one EOS {EOS_TOKEN_ID}, found {token_ids.count(EOS_TOKEN_ID)}"
        )


def truncate_and_renormalize(vector: list[float], dimensions: int) -> list[float]:
    """Matryoshka reduction: keep the leading dims, then re-L2-normalize."""

    if not MRL_MIN_DIM <= dimensions <= len(vector):
        raise OracleError(
            f"MRL dimensions must be in [{MRL_MIN_DIM}, {len(vector)}], got {dimensions}"
        )
    prefix = [float(value) for value in vector[:dimensions]]
    norm = math.sqrt(sum(value * value for value in prefix))
    if not math.isfinite(norm) or norm <= 0.0:
        raise OracleError(f"cannot renormalize a degenerate {dimensions}-dim prefix (norm={norm})")
    return [value / norm for value in prefix]


def cosine(left: list[float], right: list[float]) -> float:
    if len(left) != len(right) or not left:
        raise OracleError(f"cosine requires equal non-empty vectors: {len(left)} vs {len(right)}")
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm <= 0.0 or right_norm <= 0.0:
        raise OracleError("cosine is undefined for zero vectors")
    return dot / (left_norm * right_norm)


def format_floats(values: list[float]) -> list[float]:
    """Fixed decimal formatting so re-runs emit byte-identical JSON."""

    return [float(f"{value:.{FLOAT_DECIMALS}f}") for value in values]


def prompt_cases() -> list[dict[str, Any]]:
    """The fixed, deterministic prompt set (sorted by case id)."""

    return [
        {
            "id": "doc_cjk",
            "role": "document",
            "instruction": None,
            "text": "北京是中华人民共和国的首都，也是全国的政治与文化中心。故宫和长城每年吸引数以百万计的游客。",
        },
        {
            "id": "doc_emoji",
            "role": "document",
            "instruction": None,
            "text": "Rocket launch day! 🚀 The crew tracked telemetry — 100% nominal ✨ (© Antfly, naïve café test).",
        },
        {
            "id": "doc_english",
            "role": "document",
            "instruction": None,
            "text": (
                "Photosynthesis is the process by which green plants capture sunlight "
                "and convert carbon dioxide and water into glucose and oxygen inside "
                "their chloroplasts."
            ),
        },
        {
            "id": "doc_long_truncation",
            "role": "document",
            "instruction": None,
            "text": LONG_SENTENCE * LONG_REPEATS,
            "expect_truncated": True,
        },
        {
            "id": "doc_shared_text",
            "role": "document",
            "instruction": None,
            "text": SHARED_TEXT,
        },
        {
            "id": "doc_single_token",
            "role": "document",
            "instruction": None,
            "text": "a",
            "expect_token_count": 2,
        },
        {
            "id": "doc_whitespace",
            "role": "document",
            "instruction": None,
            "text": "   ",
        },
        {
            "id": "query_custom",
            "role": "query",
            "instruction": CUSTOM_INSTRUCTION,
            "text": "中国的首都是哪个城市？",
        },
        {
            "id": "query_default",
            "role": "query",
            "instruction": DEFAULT_INSTRUCTION,
            "text": "How do plants turn sunlight into chemical energy?",
        },
        {
            "id": "query_shared_text",
            "role": "query",
            "instruction": DEFAULT_INSTRUCTION,
            "text": SHARED_TEXT,
        },
        {
            "id": "query_short",
            "role": "query",
            "instruction": DEFAULT_INSTRUCTION,
            "text": "rocket launch",
        },
    ]


def last_token_pool(last_hidden_states: Any, attention_mask: Any) -> Any:
    """Model-card reference pooling, reproduced verbatim."""

    import torch

    left_padding = attention_mask[:, -1].sum() == attention_mask.shape[0]
    if left_padding:
        return last_hidden_states[:, -1]
    sequence_lengths = attention_mask.sum(dim=1) - 1
    return last_hidden_states[
        torch.arange(last_hidden_states.shape[0], device=last_hidden_states.device),
        sequence_lengths,
    ]


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.model_dir is not None:
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    versions = verify_environment(allow_mismatch=args.allow_env_mismatch)

    import torch
    import torch.nn.functional as F
    from transformers import AutoConfig, AutoModel, AutoTokenizer

    torch.manual_seed(0)
    if args.device == "mps" and not torch.backends.mps.is_available():
        raise OracleError("MPS is not available")

    if args.model_dir is not None:
        source = str(args.model_dir.resolve(strict=True))
        load_kwargs: dict[str, Any] = {"local_files_only": True, "trust_remote_code": False}
    else:
        source = args.model_id
        load_kwargs = {"revision": args.revision, "trust_remote_code": False}

    tokenizer = AutoTokenizer.from_pretrained(source, padding_side="left", **load_kwargs)
    config = AutoConfig.from_pretrained(source, **load_kwargs)
    architectures = list(getattr(config, "architectures", None) or [])
    if architectures != [EXPECTED_ARCHITECTURE]:
        raise OracleError(f"unexpected architectures: {architectures}")
    if int(config.hidden_size) != HIDDEN_SIZE:
        raise OracleError(f"unexpected hidden size: {config.hidden_size}")
    if int(config.num_hidden_layers) != NUM_HIDDEN_LAYERS:
        raise OracleError(f"unexpected layer count: {config.num_hidden_layers}")
    # Note: tokenizer_config.json declares eos_token = <|im_end|> (151645),
    # but the TemplateProcessing post-processor appends <|endoftext|>
    # (151643) — which matches config.json's eos_token_id and is the token
    # last-token pooling reads. Validate the *encoded* trailing token, not
    # the tokenizer's nominal eos_token_id.
    verify_single_trailing_eos([int(token) for token in tokenizer("antfly")["input_ids"]])

    # transformers >= 5 renamed torch_dtype to dtype; support both so
    # --allow-env-mismatch runs on a 4.x environment still work.
    dtype_kwarg = (
        "dtype"
        if int(versions["transformers"].split(".", 1)[0]) >= 5
        else "torch_dtype"
    )
    model = AutoModel.from_pretrained(
        source,
        attn_implementation="eager",
        **{dtype_kwarg: torch.float32},
        **load_kwargs,
    )
    if type(model).__name__ != "Qwen3Model":
        raise OracleError(f"unexpected model class: {type(model).__name__}")
    model.to(args.device)
    model.eval()

    cases_payload: list[dict[str, Any]] = []
    unit_vectors: dict[str, list[float]] = {}
    for case in prompt_cases():
        rendered = render_case_text(case)
        encoded = tokenizer(
            rendered,
            padding=True,
            truncation=True,
            max_length=args.max_length,
            return_tensors="pt",
        )
        token_ids = [int(token) for token in encoded["input_ids"][0].tolist()]
        verify_single_trailing_eos(token_ids)
        full_token_count = len(tokenizer(rendered, truncation=False)["input_ids"])
        truncated = full_token_count > len(token_ids)
        expected_count = case.get("expect_token_count")
        if expected_count is not None and len(token_ids) != expected_count:
            raise OracleError(
                f"case {case['id']!r} expected {expected_count} token ids, got {len(token_ids)}"
            )
        if case.get("expect_truncated") and not truncated:
            raise OracleError(
                f"case {case['id']!r} must exceed max_length={args.max_length} "
                f"(got {full_token_count} tokens)"
            )

        # The server has no max_length request knob and serves the model's
        # full context (32k), so an over-length case can only be replayed
        # like-for-like by sending the text of the truncated sequence itself.
        # Decode it (minus the appended EOS) and require an exact re-encode
        # roundtrip so the replay covers the same token ids the golden
        # embedding was computed from. Queries are wrapped server-side and
        # cannot be replayed pre-rendered, so truncation cases must be
        # documents.
        served_text: str | None = None
        if truncated:
            if case["role"] != "document":
                raise OracleError(
                    f"case {case['id']!r}: truncated cases must be documents; "
                    "server-side query wrapping cannot reproduce a pre-rendered query"
                )
            served_text = tokenizer.decode(token_ids[:-1])
            replay_ids = tokenizer(
                served_text,
                truncation=True,
                max_length=args.max_length,
            )["input_ids"]
            if replay_ids != token_ids:
                raise OracleError(
                    f"case {case['id']!r}: truncated text does not roundtrip; "
                    f"replay has {len(replay_ids)} tokens vs {len(token_ids)}"
                )

        model_inputs = {name: value.to(args.device) for name, value in encoded.items()}
        with torch.inference_mode():
            hidden = model(**model_inputs).last_hidden_state
            pooled = last_token_pool(hidden, model_inputs["attention_mask"])
            normalized = F.normalize(pooled, p=2, dim=1)
        vector = [
            float(value)
            for value in normalized[0].to(device="cpu", dtype=torch.float32).tolist()
        ]
        if len(vector) != HIDDEN_SIZE:
            raise OracleError(f"unexpected embedding width: {len(vector)}")
        if not all(math.isfinite(value) for value in vector):
            raise OracleError(f"case {case['id']!r} produced non-finite embedding values")

        unit_vectors[case["id"]] = vector
        cases_payload.append(
            {
                "id": case["id"],
                "role": case["role"],
                "text": case["text"],
                "instruction": case["instruction"],
                "rendered_text": rendered,
                "served_text": served_text,
                "token_ids": token_ids,
                "token_count": len(token_ids),
                "full_token_count": full_token_count,
                "truncated": truncated,
                "embeddings": {
                    "1024": format_floats(vector),
                    "256": format_floats(truncate_and_renormalize(vector, 256)),
                    "32": format_floats(truncate_and_renormalize(vector, 32)),
                },
            }
        )

    query_shared = unit_vectors["query_shared_text"]
    document_shared = unit_vectors["doc_shared_text"]
    if query_shared == document_shared:
        raise OracleError("query and document embeddings of identical text must differ")

    return {
        "schema": SCHEMA,
        "model": {
            "id": args.model_id,
            "revision": args.revision,
            "source": source,
            "architecture": EXPECTED_ARCHITECTURE,
            "hidden_size": HIDDEN_SIZE,
            "num_hidden_layers": NUM_HIDDEN_LAYERS,
            "eos_token_id": EOS_TOKEN_ID,
        },
        "runtime": {
            "python": platform.python_version(),
            "packages": versions,
            "device": args.device,
            "dtype": "float32",
            "attn_implementation": "eager",
            "padding_side": "left",
            "batch_size": 1,
        },
        "contract": {
            "pooling": "last_token",
            "normalize": "l2",
            "output_dims": list(OUTPUT_DIMS),
            "matryoshka": "truncate_then_renormalize",
            "max_length": args.max_length,
            "query_template": QUERY_TEMPLATE,
            "document_template": "{text}",
            "default_instruction": DEFAULT_INSTRUCTION,
            "eos_handling": "tokenizer_appends_single_eos",
            "float_decimals": FLOAT_DECIMALS,
        },
        "cases": sorted(cases_payload, key=lambda entry: entry["id"]),
        "consistency": {
            "shared_text": {
                "query_case": "query_shared_text",
                "document_case": "doc_shared_text",
                "cosine": float(f"{cosine(query_shared, document_shared):.{FLOAT_DECIMALS}f}"),
                "identical": False,
            },
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--model-dir", type=Path)
    source.add_argument("--model-id", default=MODEL_ID)
    parser.add_argument("--revision", default=DEFAULT_REVISION)
    parser.add_argument("--max-length", type=int, default=DEFAULT_MAX_LENGTH)
    parser.add_argument("--device", choices=("cpu", "mps"), default="cpu")
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--allow-env-mismatch",
        action="store_true",
        help="Accept non-pinned numpy/torch/transformers versions (actual versions are still recorded in the payload). For local runs only; CI must use the pinned environment.",
    )
    args = parser.parse_args(argv)
    if args.model_dir is None and not args.model_id:
        parser.error("--model-id must be non-empty")
    if not 16 <= args.max_length <= DEFAULT_MAX_LENGTH:
        parser.error(f"--max-length must be in [16, {DEFAULT_MAX_LENGTH}]")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        payload = run(args)
        encoded = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
        if args.output:
            args.output.write_text(encoded, encoding="utf-8")
        else:
            sys.stdout.write(encoded)
        return 0
    except (OracleError, OSError, RuntimeError, ValueError) as exc:
        print(f"qwen3-embedding Transformers oracle failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
