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

"""Offline export of an encoder-LoRA research candidate for the Zig runtime.

Three artifacts leave Python: the encoder with its LoRA adapters merged into
the base projections (``merge-encoder``), the reader as a non-release
split-projection bundle (``export-reader``) and pipeline fixtures that record
every boundary of the research evaluator's own inference path for one list
of cases (``reference``). The copy head ships as-is. Nothing here downloads
models or promotes the candidate to serving; see
``docs/plans/memorilla-copy17-native.md``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

import torch
from safetensors import safe_open
from safetensors.torch import load_file, save_file

from encoder_lora import (
    COPY_HEAD_TEMPERATURE,
    DEFAULT_LORA_TARGETS,
    RELEASED_MEMORY_SLOTS,
    SCHEMA as CANDIDATE_SCHEMA,
    SYSTEM,
    candidate_artifacts,
    digest,
    load_split,
)
from exporter import (
    PER_TOKEN_DUAL_LAYER_POOLING,
    RELEASE_CONFIG,
    expected_shapes,
    load_bundle,
    read_json,
    sha256,
    split_projections,
    validate_tensors,
    write_bundle,
)

MERGE_SCHEMA = "antfly.memorilla.encoder-lora-merge.v1"
REFERENCE_SCHEMA = "antfly.memorilla.encoder-lora-pipeline-reference.v1"
CASES_SCHEMA = "antfly.memorilla.encoder-lora-reference-cases.v1"
MAX_NEW_TOKENS = 24
ENCODER_MAX_LENGTH = 128
MIN_COPY_GATE = 0.5
MEMORY_TOKEN = "<|memory|>"
# Managed research profile of a merged encoder directory. The Zig catalog's
# canonical encoder manifest (``memorilla_catalog.encoder_manifest``) with
# only the profile tag replaced, byte for byte; the Zig research profile
# compares ``model_manifest.json`` against this exact string.
RELEASE_ENCODER_PROFILE = "qwen3_encoder_f32_v1"
RESEARCH_ENCODER_PROFILE = "qwen3_encoder_lora_research_f32_v1"
ENCODER_MANIFEST = (
    '{"type":"embedder","pooling":"last","normalize":true,'
    '"embedding_style":"qwen3_embedding","embedding_trailing_eos_id":151643,'
    f'"memorilla_profile":"{RELEASE_ENCODER_PROFILE}",'
    '"embedding_profile":{"task_contract":"profiled","query":{"prefix":""},'
    '"document":{"prefix":""}}}\n'
)
ENCODER_MANIFEST_SHA256 = (
    "494f7e6793aef818a4f24c2fd37867f1ce50c50e69f79e8ca92036d221926735"
)
RESEARCH_ENCODER_MANIFEST = ENCODER_MANIFEST.replace(
    f'"memorilla_profile":"{RELEASE_ENCODER_PROFILE}"',
    f'"memorilla_profile":"{RESEARCH_ENCODER_PROFILE}"',
)
RESEARCH_ENCODER_MANIFEST_SHA256 = (
    "44ada04c47c6e91784346ba7bcf7ae80b0aa55467870fab2077d4885af5625ae"
)
# Sidecars that make the merged directory a managed research publication:
# the per-directory lock (mirrors the v1 ``files`` shape of
# ``base_models.json``), the canonical research manifest and the managed
# download receipt read by ``registry/managed_receipt.zig``.
RESEARCH_LOCK_SCHEMA = "antfly.memorilla.research-encoder-lock.v1"
RESEARCH_VARIANT = "memorilla-encoder-lora-research-f32-v1"
LOCK_NAME = "memorilla_lock.json"
MANIFEST_NAME = "model_manifest.json"
RECEIPT_NAME = ".antfly-download-complete.json"
RECEIPT_BLOCKERS = (".antfly-download-in-progress", ".antfly-download-plan.json")
SIDECAR_NAMES = (LOCK_NAME, MANIFEST_NAME, RECEIPT_NAME)
# Non-weight files the released encoder ships; the lock lists each of them.
ENCODER_SIDE_FILES = (
    "1_Pooling/config.json",
    "config.json",
    "config_sentence_transformers.json",
    "generation_config.json",
    "merges.txt",
    "model.safetensors.index.json",
    "modules.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "vocab.json",
)
BASE_MODELS_PATH = Path(__file__).with_name("base_models.json")
# Candidate files copied into the reader bundle and pinned in its manifest.
BUNDLE_EXTRA_FILES = ("candidate.json", "copy-head.safetensors", "protocol.json")
MERGED_DTYPES = {"float32": torch.float32, "bfloat16": torch.bfloat16}
ADAPTER_WEIGHT = re.compile(
    r"^(?:model\.)?(layers\.\d+\.self_attn\.(?:q|k|v|o)_proj)\.weight$"
)
DEFAULT_ASSETS = {
    "encoder": "models/qwen3-embedding-4b",
    "decoder": "models/qwen3-8b",
    "checkpoint": "checkpoints/stage3",
    "upstream_source": "upstream",
}


def save_json(path: Path, value) -> None:
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_candidate(directory: Path) -> tuple[dict, dict]:
    """Read and verify a research candidate directory -> (candidate, protocol).

    Every artifact hash in ``candidate.json`` must match its file, and the
    protocol must describe the configuration this exporter's reference
    pipeline implements (gated copy head over self-context tokens-dual
    features, one reader pass, no entity tags).
    """
    directory = Path(directory)
    candidate = read_json(directory / "candidate.json")
    protocol = read_json(directory / "protocol.json")
    if (
        candidate.get("schema") != CANDIDATE_SCHEMA
        or protocol.get("schema") != CANDIDATE_SCHEMA
    ):
        raise ValueError("not an encoder-LoRA research candidate")
    if candidate.get("rollout_qualified") or protocol.get("rollout_qualified"):
        raise ValueError("research candidates are never rollout qualified")
    for field, file in candidate_artifacts(protocol):
        if candidate.get(field) != digest(directory / file):
            raise ValueError(f"candidate artifact changed: {file}")
    required = {
        "memory_slots": RELEASED_MEMORY_SLOTS,
        "document_features": "tokens-dual",
        "copy_head": True,
        "document_space_prefix": True,
        "document_self_context": True,
        "encoder_special_rows": True,
        "document_pooled_context": False,
        "entity_tags": "none",
        "copy_restrict_by_type": False,
        "reader_loops": 1,
        "encoder_layer": None,
        "system_prompt": SYSTEM,
    }
    for key, value in required.items():
        if protocol.get(key) != value:
            raise ValueError(
                f"unsupported candidate protocol: {key}={protocol.get(key)!r}"
            )
    if candidate.get("memory_slots") != RELEASED_MEMORY_SLOTS:
        raise ValueError("candidate reader is not the released 16-slot geometry")
    if type(protocol.get("dual_layer")) is not int:
        raise ValueError("tokens-dual candidate lacks its dual layer")
    if list(protocol.get("target_modules", DEFAULT_LORA_TARGETS)) != list(
        DEFAULT_LORA_TARGETS
    ):
        raise ValueError("only q_proj/v_proj adapters are exportable")
    return candidate, protocol


def lora_scale(protocol: dict) -> float:
    rank, alpha = protocol["rank"], protocol["alpha"]
    if type(rank) is not int or rank < 1 or not isinstance(alpha, (int, float)):
        raise ValueError("invalid adapter rank/alpha")
    return float(alpha) / float(rank)


def candidate_hashes(directory: Path, protocol: dict) -> dict:
    directory = Path(directory)
    hashes = {"candidate_json": digest(directory / "candidate.json")}
    for field, file in candidate_artifacts(protocol):
        hashes[field.removesuffix("_sha256")] = digest(directory / file)
    return hashes


# ----------------------------------------------------------------------------
# managed research sidecars


def encoder_base(lock_path: Path = BASE_MODELS_PATH) -> dict:
    """``{"model", "revision"}`` of the released encoder from the lock mirror."""
    encoder = read_json(lock_path)["models"]["encoder"]
    base = {"model": encoder["model"], "revision": encoder["revision"]}
    if any(type(v) is not str or not v for v in base.values()):
        raise ValueError("base encoder lock lacks model/revision")
    return base


def regular_files(directory: Path) -> list[str]:
    """Sorted relative POSIX paths of every regular non-dot file under
    ``directory``; dot-entries (managed markers) are skipped at every level
    and symlinks are refused, mirroring ``managed_receipt.zig``."""
    directory = Path(directory)
    found = []
    for path in sorted(directory.rglob("*")):
        relative = path.relative_to(directory)
        if any(part.startswith(".") for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError(f"symlink in the merged encoder: {relative}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError(f"not a regular file: {relative}")
        name = relative.as_posix()
        if any(c in name for c in ("\\", ":", "\0")):
            raise ValueError(f"unsafe artifact path: {name}")
        found.append(name)
    return sorted(found)


def file_pin(directory: Path, name: str) -> dict:
    path = Path(directory) / name
    return {"path": name, "size_bytes": path.stat().st_size, "sha256": sha256(path)}


def write_sidecars(directory: Path, *, candidate_sha256: str, base: dict) -> dict:
    """Write ``memorilla_lock.json``, ``model_manifest.json`` and the managed
    receipt for a merged encoder directory that already holds its shards,
    index, side files and ``merge.json``. Existing sidecars are replaced;
    the result is byte-deterministic for unchanged content.

    The lock pins every regular file except the sidecars themselves; the
    receipt pins every regular file including the lock and the manifest, so
    the Zig loader's publication generation covers all of them.
    """
    directory = Path(directory)
    for blocker in RECEIPT_BLOCKERS:
        if (directory / blocker).exists():
            raise ValueError(f"managed publication in progress: {blocker}")
    for name in SIDECAR_NAMES:
        (directory / name).unlink(missing_ok=True)
    merge = read_json(directory / "merge.json")
    if merge.get("schema") != MERGE_SCHEMA:
        raise ValueError("merge.json is not an encoder-LoRA merge receipt")
    if merge.get("candidate_sha256", {}).get("candidate_json") != candidate_sha256:
        raise ValueError("candidate.json digest differs from merge.json")
    if not re.fullmatch(r"[0-9a-f]{64}", candidate_sha256):
        raise ValueError("candidate digest is not lowercase SHA-256 hex")
    files = regular_files(directory)
    shards = sorted(set(merge["shards"]))
    required = {"merge.json", "model.safetensors.index.json", "config.json", *shards}
    if not required <= set(files):
        raise ValueError(f"merged encoder lacks {sorted(required - set(files))}")
    for shard in shards:
        pin = merge["shards"][shard]
        if (directory / shard).stat().st_size != pin["size_bytes"] or sha256(
            directory / shard
        ) != pin["sha256"]:
            raise ValueError(f"shard differs from merge.json: {shard}")
    lock = {
        "schema": RESEARCH_LOCK_SCHEMA,
        "candidate_sha256": candidate_sha256,
        "base": {"model": base["model"], "revision": base["revision"]},
        "files": [file_pin(directory, name) for name in files],
    }
    (directory / LOCK_NAME).write_text(json.dumps(lock, indent=2) + "\n")
    (directory / MANIFEST_NAME).write_bytes(RESEARCH_ENCODER_MANIFEST.encode())
    if sha256(directory / MANIFEST_NAME) != RESEARCH_ENCODER_MANIFEST_SHA256:
        raise ValueError("research manifest constant drifted from its digest")
    receipt = {
        "version": 1,
        "source": {
            "owner": base["model"].split("/", 1)[0],
            "name": base["model"].split("/", 1)[1],
            "variant": RESEARCH_VARIANT,
            "requested_revision": base["revision"],
            "resolved_revision": base["revision"],
            "selected_format": RESEARCH_VARIANT,
            "merge": {
                "schema": MERGE_SCHEMA,
                "memorilla_profile": RESEARCH_ENCODER_PROFILE,
                "candidate_sha256": candidate_sha256,
                "adapter_sha256": merge["adapter_sha256"],
                "merge_json_sha256": sha256(directory / "merge.json"),
                "lock_sha256": sha256(directory / LOCK_NAME),
            },
        },
        "artifacts": [
            {
                "path": pin["path"],
                "size": pin["size_bytes"],
                "sha256": pin["sha256"],
            }
            for pin in (file_pin(directory, name) for name in regular_files(directory))
        ],
    }
    partial = directory / (RECEIPT_NAME + ".partial")
    partial.write_text(json.dumps(receipt, separators=(",", ":")) + "\n")
    partial.replace(directory / RECEIPT_NAME)
    return {"lock": lock, "receipt": receipt}


def refresh_sidecars(output_dir: Path, candidate_dir: Path | None = None) -> dict:
    """``--sidecars-only``: rewrite the lock, manifest and receipt of an
    existing merged directory from its shards and ``merge.json``."""
    output_dir = Path(output_dir)
    merge = read_json(output_dir / "merge.json")
    candidate_sha256 = merge.get("candidate_sha256", {}).get("candidate_json")
    if candidate_dir is not None:
        actual = digest(Path(candidate_dir) / "candidate.json")
        if actual != candidate_sha256:
            raise ValueError("candidate directory differs from the merged encoder")
    return write_sidecars(
        output_dir, candidate_sha256=candidate_sha256, base=encoder_base()
    )


def source_encoder_revision(encoder_dir: Path) -> str | None:
    """``resolved_revision`` of the source encoder's managed receipt, if any."""
    receipt_path = Path(encoder_dir) / RECEIPT_NAME
    if not receipt_path.is_file():
        return None
    source = read_json(receipt_path).get("source") or {}
    revision = source.get("resolved_revision")
    return revision if isinstance(revision, str) else None


# ----------------------------------------------------------------------------
# merge-encoder


def adapter_pairs(adapter: dict, scale: float) -> dict[str, torch.Tensor]:
    """``{module: scale * B @ A}`` in FP32 from ``module.A`` / ``module.B`` pairs."""
    deltas = {}
    modules = sorted({name.rsplit(".", 1)[0] for name in adapter})
    if set(adapter) != {f"{m}.{k}" for m in modules for k in ("A", "B")}:
        raise ValueError("adapter tensors are not complete A/B pairs")
    for module in modules:
        a, b = adapter[module + ".A"], adapter[module + ".B"]
        if (
            a.dtype != torch.float32
            or b.dtype != torch.float32
            or a.ndim != 2
            or b.ndim != 2
            or a.shape[0] != b.shape[1]
        ):
            raise ValueError(f"adapter geometry invalid: {module}")
        deltas[module] = (b @ a) * scale
    return deltas


def merge_encoder(
    candidate_dir: Path,
    encoder_dir: Path,
    output_dir: Path,
    *,
    merged_dtype: str = "float32",
) -> dict:
    """Write ``encoder_dir`` with the candidate's LoRA merged into its weights.

    Every ``layers.N.self_attn.{q,v}_proj.weight`` becomes
    ``W + (alpha / rank) * B @ A`` computed in FP32 and stored as
    ``merged_dtype`` (default FP32: the Zig runtime widens every weight to
    F32, so storing the merge in F32 keeps it exact; the source shards are
    BF16). All other tensors are copied byte for byte into the same shard,
    the index keeps its tensor-to-shard map, and every non-weight file is
    copied, except that dot-files (download receipts bound to the source
    shards) are skipped and ``model_manifest.json`` is replaced by the
    canonical research manifest. The source must be the released encoder
    (its manifest is the catalog's canonical one). After ``merge.json`` the
    directory gets ``memorilla_lock.json``, ``model_manifest.json`` and
    ``.antfly-download-complete.json`` (``write_sidecars``). Returns the
    ``merge.json`` receipt.
    """
    candidate_dir, encoder_dir, output_dir = (
        Path(candidate_dir),
        Path(encoder_dir),
        Path(output_dir),
    )
    if merged_dtype not in MERGED_DTYPES:
        raise ValueError("merged dtype must be float32 or bfloat16")
    if output_dir.exists():
        raise ValueError("output already exists; choose a new encoder directory")
    candidate, protocol = load_candidate(candidate_dir)
    scale = lora_scale(protocol)
    source_manifest = encoder_dir / MANIFEST_NAME
    if (
        not source_manifest.is_file()
        or source_manifest.read_bytes() != ENCODER_MANIFEST.encode()
    ):
        raise ValueError("source encoder manifest is not the catalog encoder manifest")
    base_lock = encoder_base()
    source_revision = source_encoder_revision(encoder_dir)
    if source_revision is not None and source_revision != base_lock["revision"]:
        raise ValueError("source encoder revision differs from the base lock")
    adapter_path = candidate_dir / "encoder-lora.safetensors"
    deltas = adapter_pairs(load_file(str(adapter_path), device="cpu"), scale)
    index_path = encoder_dir / "model.safetensors.index.json"
    index = read_json(index_path)
    weight_map = index["weight_map"]
    shards = sorted(set(weight_map.values()))
    by_shard = {
        shard: [name for name, s in weight_map.items() if s == shard]
        for shard in shards
    }
    source_bytes = sum((encoder_dir / shard).stat().st_size for shard in shards)
    extra = sum(
        d.numel() * (MERGED_DTYPES[merged_dtype].itemsize - 2) for d in deltas.values()
    )
    free = shutil.disk_usage(
        output_dir.parent if output_dir.parent.exists() else "."
    ).free
    if free < source_bytes + max(extra, 0) + (1 << 30):
        raise ValueError("insufficient free disk for the merged encoder")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    merged_names, per_tensor, copied_tensors = {}, {}, 0
    with tempfile.TemporaryDirectory(
        prefix=f".{output_dir.name}-", dir=output_dir.parent
    ) as temporary:
        staging = Path(temporary) / "encoder"
        staging.mkdir()
        for shard in shards:
            tensors = {}
            with safe_open(str(encoder_dir / shard), "pt", device="cpu") as handle:
                metadata = handle.metadata()
                if set(handle.keys()) != set(by_shard[shard]):
                    raise ValueError(f"shard inventory differs from the index: {shard}")
                for name in by_shard[shard]:
                    tensor = handle.get_tensor(name)
                    match = ADAPTER_WEIGHT.match(name)
                    module = match.group(1) if match else None
                    if module in deltas:
                        delta = deltas[module]
                        if tuple(delta.shape) != tuple(tensor.shape):
                            raise ValueError(f"adapter geometry differs: {module}")
                        base = tensor.float()
                        merged = (base + delta).to(MERGED_DTYPES[merged_dtype])
                        realized = merged.float() - base
                        per_tensor[name] = {
                            "shard": shard,
                            "module": module,
                            "shape": list(tensor.shape),
                            "source_dtype": str(tensor.dtype).removeprefix("torch."),
                            "merged_dtype": merged_dtype,
                            "max_abs_delta": float(delta.abs().max()),
                            "mean_abs_delta": float(delta.abs().mean()),
                            "max_abs_realized_delta": float(realized.abs().max()),
                            "max_abs_rounding": float((realized - delta).abs().max()),
                            "max_abs_weight": float(base.abs().max()),
                        }
                        merged_names[module] = name
                        tensors[name] = merged.contiguous()
                    else:
                        tensors[name] = tensor
                        copied_tensors += 1
            save_file(tensors, str(staging / shard), metadata=metadata)
            del tensors
        if set(merged_names) != set(deltas):
            missing = sorted(set(deltas) - set(merged_names))
            raise ValueError(f"adapter modules without a base weight: {missing}")
        # Verify from the written bytes: every merged tensor equals the FP32
        # merge recomputed from the source, rounded to the stored dtype.
        for module, name in merged_names.items():
            shard = weight_map[name]
            with (
                safe_open(str(staging / shard), "pt", device="cpu") as merged_handle,
                safe_open(
                    str(encoder_dir / shard), "pt", device="cpu"
                ) as source_handle,
            ):
                stored = merged_handle.get_tensor(name)
                expected = (source_handle.get_tensor(name).float() + deltas[module]).to(
                    MERGED_DTYPES[merged_dtype]
                )
                if stored.dtype != expected.dtype or not torch.equal(stored, expected):
                    raise ValueError(f"merged tensor verification failed: {name}")
                if merged_dtype == "float32" and not torch.allclose(
                    stored,
                    source_handle.get_tensor(name).float() + deltas[module],
                    rtol=0,
                    atol=1e-6,
                ):
                    raise ValueError(f"merged tensor drifts from W + scale*B@A: {name}")
        total_size = 0
        for shard in shards:
            with safe_open(str(staging / shard), "pt", device="cpu") as handle:
                for name in handle.keys():
                    view = handle.get_slice(name)
                    numel = 1
                    for dim in view.get_shape():
                        numel *= dim
                    total_size += (
                        numel * {"F32": 4, "BF16": 2, "F16": 2}[view.get_dtype()]
                    )
        new_index = dict(index)
        new_index["metadata"] = {**index.get("metadata", {}), "total_size": total_size}
        (staging / index_path.name).write_text(json.dumps(new_index, indent=2) + "\n")
        copied_files, skipped_files = [], []
        for entry in sorted(encoder_dir.iterdir()):
            if entry.name == index_path.name or entry.name in shards:
                continue
            if entry.name.startswith("."):
                # Dot-files are receipts about the source directory (the
                # managed-download marker binds the original shard bytes and
                # makes the Zig loader refuse a directory whose shards differ).
                skipped_files.append(entry.name)
                continue
            if entry.name.endswith(".safetensors"):
                raise ValueError(f"unindexed weight file in the encoder: {entry.name}")
            if entry.name in SIDECAR_NAMES:
                # Written by write_sidecars for this directory's own bytes.
                skipped_files.append(entry.name)
                continue
            if entry.is_dir():
                shutil.copytree(entry, staging / entry.name)
            else:
                shutil.copyfile(entry, staging / entry.name)
            copied_files.append(entry.name)
        receipt = {
            "schema": MERGE_SCHEMA,
            "candidate_directory": str(candidate_dir.resolve()),
            "candidate": candidate,
            "candidate_sha256": candidate_hashes(candidate_dir, protocol),
            "adapter_file": adapter_path.name,
            "adapter_sha256": sha256(adapter_path),
            "adapter_tensors": 2 * len(deltas),
            "rank": protocol["rank"],
            "alpha": protocol["alpha"],
            "scale": scale,
            "target_modules": list(
                protocol.get("target_modules", DEFAULT_LORA_TARGETS)
            ),
            "merge": "W + scale * (B @ A) in float32",
            "merged_dtype": merged_dtype,
            "source_encoder": str(encoder_dir.resolve()),
            "source_index_sha256": sha256(index_path),
            "source_shards": {
                shard: {
                    "sha256": sha256(encoder_dir / shard),
                    "size_bytes": (encoder_dir / shard).stat().st_size,
                }
                for shard in shards
            },
            "shards": {
                shard: {
                    "sha256": sha256(staging / shard),
                    "size_bytes": (staging / shard).stat().st_size,
                }
                for shard in shards
            },
            "total_size": total_size,
            "counts": {
                "tensors": len(weight_map),
                "merged": len(merged_names),
                "copied": copied_tensors,
            },
            "tensors": dict(sorted(per_tensor.items())),
            "max_abs_delta": max(t["max_abs_delta"] for t in per_tensor.values()),
            "copied_files": copied_files,
            "skipped_files": skipped_files,
            "sidecar_files": list(SIDECAR_NAMES),
            "model_manifest": {
                "memorilla_profile": RESEARCH_ENCODER_PROFILE,
                "source_sha256": ENCODER_MANIFEST_SHA256,
                "written_sha256": RESEARCH_ENCODER_MANIFEST_SHA256,
            },
            "base": base_lock,
            "exporter_sha256": sha256(Path(__file__)),
            "torch_version": torch.__version__,
        }
        save_json(staging / "merge.json", receipt)
        write_sidecars(
            staging,
            candidate_sha256=receipt["candidate_sha256"]["candidate_json"],
            base=base_lock,
        )
        staging.rename(output_dir)
    return receipt


# ----------------------------------------------------------------------------
# export-reader


def export_reader(
    candidate_dir: Path,
    output_dir: Path,
    *,
    config: dict | None = None,
    include_encoder_lora: bool = False,
    include_span_head: bool = False,
    include_span_gate_routes: bool = False,
) -> dict:
    """``reader.safetensors`` -> non-release split-projection bundle whose
    execution contract declares per-token dual-layer encoder features and
    which carries the candidate's copy head, ``candidate.json`` and
    ``protocol.json`` pinned in ``files`` (``BUNDLE_EXTRA_FILES``).

    ``include_encoder_lora`` also pins the candidate's ``encoder-lora.safetensors``
    so a server can apply the adapters at run time to the stock encoder instead
    of loading a second, merged encoder (the hybrid profile's one-encoder mode).
    Its digest must equal ``candidate.json``'s ``encoder_lora_sha256``.

    ``include_span_head`` pins a span-window candidate's
    ``span-head.safetensors`` (``qwen_span_copy.py``): the protocol must name
    ``record_kind: span-window`` and ``candidate.json``'s
    ``span_head_sha256`` must equal the file's digest.

    ``include_span_gate_routes`` (with ``include_span_head``) also pins the
    routed gate's ``span-gate-routes.safetensors`` (``qwen_span_copy.py
    gate-routes``): the protocol must name ``span_gate_routes`` and
    ``candidate.json``'s ``span_gate_routes_sha256`` must equal the file's
    digest."""
    candidate_dir = Path(candidate_dir)
    candidate, protocol = load_candidate(candidate_dir)
    if include_span_head:
        if protocol.get("record_kind") != "span-window":
            raise ValueError("span head export needs a span-window candidate")
        if candidate.get("span_head_sha256") != digest(
            candidate_dir / "span-head.safetensors"
        ):
            raise ValueError("candidate artifact changed: span-head.safetensors")
    if include_span_gate_routes:
        if not include_span_head or "span_gate_routes" not in protocol:
            raise ValueError("gate routes export needs a span head and span_gate_routes")
        if candidate.get("span_gate_routes_sha256") != digest(
            candidate_dir / "span-gate-routes.safetensors"
        ):
            raise ValueError("candidate artifact changed: span-gate-routes.safetensors")
    config = dict(RELEASE_CONFIG if config is None else config)
    reader_path = candidate_dir / "reader.safetensors"
    weights = load_file(str(reader_path), device="cpu")
    validate_tensors(weights, config, split=False, release=False)
    if set(weights) != set(expected_shapes(config, split=False)):
        raise ValueError("reader tensors differ from the packed upstream layout")
    converted = split_projections(weights)
    return write_bundle(
        converted,
        config,
        output_dir,
        release=False,
        source={
            "model": None,
            "revision": None,
            "checkpoint": None,
            "sha256": sha256(reader_path),
            "config_sha256": None,
            "candidate_directory": str(candidate_dir.resolve()),
            "candidate_schema": candidate["schema"],
            "candidate_sha256": digest(candidate_dir / "candidate.json"),
            "reader_file": reader_path.name,
            "protocol_sha256": digest(candidate_dir / "protocol.json"),
        },
        encoder_pooling=PER_TOKEN_DUAL_LAYER_POOLING,
        extra_files={
            name: candidate_dir / name
            for name in BUNDLE_EXTRA_FILES
            + (("encoder-lora.safetensors",) if include_encoder_lora else ())
            + (("span-head.safetensors",) if include_span_head else ())
            + (("span-gate-routes.safetensors",) if include_span_gate_routes else ())
        },
    )


# ----------------------------------------------------------------------------
# case files


def case_name(row: dict) -> str:
    name = str(row["id"]).replace(".", "_")
    if not name or "." in name:
        raise ValueError("case names must be nonempty and contain no '.'")
    return name


def case_entry(row: dict, cohort: str | None = None) -> dict:
    return {
        "name": case_name(row),
        "id": row["id"],
        "group": row.get("group"),
        "category": row.get("category"),
        "cohort": row.get("cohort", cohort),
        "documents": list(row["documents"]),
        "question": row["question"],
        "answer": row["answer"],
    }


def write_cases(path: Path, cases: list[dict], **provenance) -> dict:
    names = [c["name"] for c in cases]
    if len(set(names)) != len(names):
        raise ValueError("duplicate case names")
    value = {"schema": CASES_SCHEMA, **provenance, "cases": cases}
    save_json(path, value)
    return value


def read_cases(path: Path) -> list[dict]:
    value = json.loads(Path(path).read_text())
    rows = value["cases"] if isinstance(value, dict) else value
    cases = []
    for row in rows:
        case = dict(row)
        case.setdefault("name", case_name(case) if "id" in case else None)
        if case["name"] is None or "." in case["name"]:
            raise ValueError("every case needs a name without '.'")
        for key in ("documents", "question", "answer"):
            if key not in case:
                raise ValueError(f"case {case['name']} lacks {key}")
        cases.append(case)
    names = [c["name"] for c in cases]
    if not cases or len(set(names)) != len(names):
        raise ValueError("case file needs unique nonempty cases")
    return cases


def dev_cases(data_dir: Path) -> list[dict]:
    return [case_entry(row, "dev") for row in load_split(Path(data_dir), "dev")]


def fresh_cases(
    data_dir: Path, prior_dirs: list[Path], groups: int, seed: int
) -> tuple[int, list[dict], list[str]]:
    """The evaluator's post-freeze challenge for ``--fresh-groups`` /
    ``--fresh-seed``: disjoint from the training corpus and every prior
    corpus in the candidate's lineage."""
    from encoder_lora_eval import fresh_challenge

    data_dir = Path(data_dir)
    train = load_split(data_dir, "train")
    dev = load_split(data_dir, "dev")
    priors = []
    hashes = []
    for directory in prior_dirs:
        priors.extend(load_split(Path(directory), "train"))
        hashes.append(digest(Path(directory) / "manifest.json"))
    effective, rows = fresh_challenge(seed, groups, train + priors, dev)
    return effective, [case_entry(row) for row in rows], hashes


# ----------------------------------------------------------------------------
# reference fixtures


def wait_for_gpu(limit_mib: int = 0, poll_seconds: int = 10) -> int:
    """Block until no other compute process holds more than ``limit_mib``."""
    while True:
        used = sum(
            int(line.split(",")[1].strip().split()[0])
            for line in subprocess.check_output(
                [
                    "nvidia-smi",
                    "--query-compute-apps=pid,used_memory",
                    "--format=csv,noheader",
                ],
                text=True,
            ).splitlines()
            if line.strip()
        )
        if used <= limit_mib:
            return used
        print(json.dumps({"event": "gpu-busy", "cotenant_mib": used}), flush=True)
        time.sleep(poll_seconds)


def write_reference(
    output_dir: Path, header: dict, cases: list[dict], tensors: dict
) -> dict:
    """Publish ``reference.json`` + ``reference.safetensors`` atomically.

    ``cases`` are the per-case JSON records (``name`` first) and ``tensors``
    the ``<case>.<boundary>`` FP32 tensors; the receipt inventories them.
    """
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("output already exists; choose a new fixture directory")
    names = [c["name"] for c in cases]
    if len(set(names)) != len(names) or any("." in n for n in names):
        raise ValueError("fixture case names must be unique and contain no '.'")
    for key, tensor in tensors.items():
        case, _, boundary = key.partition(".")
        if case not in names or not boundary or tensor.dtype != torch.float32:
            raise ValueError(f"fixture tensor is not an FP32 case boundary: {key}")
        if not torch.isfinite(tensor).all().item():
            raise ValueError(f"nonfinite fixture tensor: {key}")
    receipt = {**header, "schema": REFERENCE_SCHEMA, "cases": cases}
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{output_dir.name}-", dir=output_dir.parent
    ) as temporary:
        staging = Path(temporary) / "fixtures"
        staging.mkdir()
        tensor_path = staging / "reference.safetensors"
        save_file(
            {k: v.contiguous().cpu() for k, v in sorted(tensors.items())},
            str(tensor_path),
            metadata={"format": REFERENCE_SCHEMA},
        )
        receipt["tensors"] = {
            "path": tensor_path.name,
            "sha256": sha256(tensor_path),
            "inventory": {
                name: {"shape": list(t.shape), "dtype": "float32"}
                for name, t in sorted(tensors.items())
            },
        }
        save_json(staging / "reference.json", receipt)
        staging.rename(output_dir)
    return receipt


def load_models(candidate_dir: Path, protocol: dict, paths: dict):
    """The evaluator's model stack: BF16 encoder with the candidate's adapters,
    BF16 decoder, FP32 reader and copy head, nearest-token snapper."""
    from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer

    from assets import verify_base_model
    from encoder_lora import (
        CopyHead,
        MemoryTokenSnapper,
        check_dual_layer,
        inject_lora,
        restore_adapters,
    )
    from reference import load_upstream
    from released_reference import checkpoint_proof, source_recipe

    torch.set_num_threads(4)
    torch.manual_seed(23)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    config, _ = checkpoint_proof(paths["checkpoint"])
    if config["num_memories"] != RELEASED_MEMORY_SLOTS:
        raise ValueError("released 16-slot reader checkpoint required")
    proofs = {
        "encoder": verify_base_model(paths["encoder"], "encoder"),
        "decoder": verify_base_model(paths["decoder"], "decoder"),
        "upstream": source_recipe(paths["upstream_source"]),
    }
    enc_tok = AutoTokenizer.from_pretrained(paths["encoder"], local_files_only=True)
    enc_tok.padding_side = "right"
    dec_tok = AutoTokenizer.from_pretrained(paths["decoder"], local_files_only=True)
    encoder = (
        AutoModel.from_pretrained(
            paths["encoder"],
            local_files_only=True,
            torch_dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .requires_grad_(False)
        .cuda()
        .eval()
    )
    adapters = inject_lora(
        encoder,
        protocol["rank"],
        protocol["alpha"],
        False,
        tuple(protocol.get("target_modules", DEFAULT_LORA_TARGETS)),
    )
    restore_adapters(
        adapters, load_file(str(Path(candidate_dir) / "encoder-lora.safetensors"))
    )
    check_dual_layer(protocol["dual_layer"], encoder.config.num_hidden_layers)
    decoder = (
        AutoModelForCausalLM.from_pretrained(
            paths["decoder"],
            local_files_only=True,
            torch_dtype=torch.bfloat16,
            attn_implementation="sdpa",
        )
        .requires_grad_(False)
        .cuda()
        .eval()
    )
    reader = (
        load_upstream(
            paths["upstream_source"],
            dict(config, num_memories=RELEASED_MEMORY_SLOTS),
            load_file(str(Path(candidate_dir) / "reader.safetensors")),
        )
        .float()
        .cuda()
        .requires_grad_(False)
        .eval()
    )
    copy_head = (
        CopyHead(4096, protocol.get("copy_head_temperature", COPY_HEAD_TEMPERATURE))
        .cuda()
        .requires_grad_(False)
        .eval()
    )
    copy_head.load_state_dict(
        load_file(str(Path(candidate_dir) / "copy-head.safetensors"))
    )
    snapper = MemoryTokenSnapper(decoder.get_input_embeddings())
    return {
        "enc_tok": enc_tok,
        "dec_tok": dec_tok,
        "encoder": encoder,
        "decoder": decoder,
        "reader": reader,
        "copy_head": copy_head,
        "snapper": snapper,
        "proofs": proofs,
        "reader_config": config,
    }


def run_case(models: dict, protocol: dict, case: dict, *, suffix: int, pad_id: int):
    """One case through the evaluator's copy17 path -> (record, tensors).

    Mirrors ``encoder_lora_eval.main`` under ``--snap-memory all
    --copy-contiguous-runs --copy-complete-words`` by calling the same
    helpers in the same order and precision context.
    """
    from decoder_reference import prepare_prompt
    from encoder_lora import (
        complete_copied_words,
        copy_with_contiguous_runs,
        document_features_dual,
        encoder_states_pair,
        looped_read,
        prefixed_documents,
        self_context_documents,
        self_context_states,
        token_ids_embeddings,
    )
    from gpu_quality_reference import generate

    enc_tok, dec_tok = models["enc_tok"], models["dec_tok"]
    encoder, decoder, reader = models["encoder"], models["decoder"], models["reader"]
    copy_head, snapper = models["copy_head"], models["snapper"]
    embedding = decoder.get_input_embeddings()
    with torch.inference_mode(), torch.autocast("cuda", dtype=torch.bfloat16):
        documents = prefixed_documents(case["documents"], True)
        doubled, counts, single_ids, fallbacks = self_context_documents(
            enc_tok, documents
        )
        texts = doubled + [case["question"]]
        copy_ids = torch.cat(single_ids)
        tokens = enc_tok(texts, padding=True, return_tensors="pt").to("cuda")
        if tokens.input_ids.shape[1] > ENCODER_MAX_LENGTH:
            raise ValueError("encoder bound exceeded")
        mask = tokens.attention_mask
        states = encoder_states_pair(encoder, tokens, protocol["dual_layer"])
        states, mask = self_context_states(states, mask, counts, suffix)
        final, layer = states
        docs, query = document_features_dual(final, layer, mask)
        memories = looped_read(
            reader,
            None,
            docs,
            torch.zeros((1, docs.shape[1]), dtype=torch.bool, device="cuda"),
            query,
            1,
        )[0]
        if tuple(memories.shape) != (RELEASED_MEMORY_SLOTS, 4096):
            raise ValueError("candidate changed memory geometry")
        sources = token_ids_embeddings(embedding, copy_ids)
        # Pre-softmax copy logits and gate for the fixture; the same FP32 head
        # call ``copy_with_contiguous_runs`` makes internally.
        _, logits, gate_logits = copy_head.forward_detailed(
            memories.unsqueeze(0), sources.unsqueeze(0)
        )
        copy_output, gate, moved, spans = copy_with_contiguous_runs(
            copy_head,
            memories,
            sources,
            None,
            min_gate=MIN_COPY_GATE,
            return_spans=True,
        )
        snapped, snap_ids, snap_top1 = snapper(copy_output)
        snapped_ids = snap_ids.tolist()
        inserted = 0
        if spans:
            completed, inserted = complete_copied_words(
                snapped_ids,
                spans,
                copy_ids.tolist(),
                dec_tok,
                pad_id,
                overflow="truncate",
            )
            final_ids = torch.tensor(completed, device=snap_ids.device)
            final_memories = embedding.weight[final_ids].detach().to(snapped.dtype)
        else:
            completed = list(snapped_ids)
            final_memories = snapped
        prompt_ids, embeds, info = prepare_prompt(
            dec_tok,
            embedding,
            final_memories,
            system_prompt=SYSTEM,
            question=case["question"],
            device="cuda",
        )
        output = generate(decoder, dec_tok, embeds, MAX_NEW_TOKENS)
    name = case["name"]
    record = {
        "name": name,
        **{k: case[k] for k in ("id", "group", "category", "cohort") if k in case},
        "documents": list(case["documents"]),
        "prefixed_documents": documents,
        "doubled_documents": doubled,
        "document_token_counts": counts,
        "self_context_fallbacks": fallbacks,
        "encoder_input_ids": tokens.input_ids.tolist(),
        "encoder_attention_mask": tokens.attention_mask.tolist(),
        "question": case["question"],
        "expected_answer": case["answer"],
        "copy_ids": copy_ids.tolist(),
        "gate": [float(g) for g in gate],
        "spans": [[int(b), int(e), int(s)] for b, e, s in spans],
        "copy_moved_slots": int(moved),
        "snapped_ids": [int(i) for i in snapped_ids],
        "snap_top1_cosines": [float(c) for c in snap_top1],
        "completed_ids": [int(i) for i in completed],
        "copy_completed_tokens": int(inserted),
        "snapped_text": "".join(dec_tok.decode([int(i)]) for i in completed),
        "memory_positions": info["memory_positions"],
        "prompt_ids": prompt_ids.tolist(),
        "output_ids": [int(i) for i in output["token_ids"]],
        "finish_reason": output["finish_reason"],
        "answer": output["answer"],
        "correct": output["answer"].strip() == case["answer"],
    }
    tensors = {
        f"{name}.docs": docs[0].detach().float().cpu(),
        f"{name}.query": query[0].detach().float().cpu(),
        f"{name}.memories": memories.detach().float().cpu(),
        f"{name}.copy_output": copy_output.detach().float().cpu(),
        f"{name}.gate": gate.detach().float().cpu(),
        f"{name}.logits": logits[0].detach().float().cpu(),
    }
    return record, tensors


def reference(
    candidate_dir: Path,
    cases_path: Path,
    output_dir: Path,
    paths: dict,
    *,
    max_cases: int | None = None,
) -> dict:
    """Run the research pipeline on a case file and publish the fixtures."""
    from encoder_lora import (
        assert_shared_token_ids,
        encoder_special_suffix,
        prefixed_documents,
    )
    from released_reference import EOS_IDS

    candidate_dir, output_dir = Path(candidate_dir), Path(output_dir)
    if output_dir.exists():
        raise ValueError("output already exists; choose a new fixture directory")
    candidate, protocol = load_candidate(candidate_dir)
    cases = read_cases(cases_path)
    if max_cases is not None:
        if max_cases < 1:
            raise ValueError("max cases must be positive")
        cases = cases[:max_cases]
    cotenant = wait_for_gpu()
    models = load_models(candidate_dir, protocol, paths)
    enc_tok, dec_tok = models["enc_tok"], models["dec_tok"]
    suffix = encoder_special_suffix(enc_tok)
    pad = dec_tok.encode(" ", add_special_tokens=False)
    if len(pad) != 1:
        raise ValueError("record pad must be a single token")
    pad_id = pad[0]
    assert_shared_token_ids(
        enc_tok, dec_tok, prefixed_documents(cases[0]["documents"], True), True
    )
    eos_id = enc_tok.convert_tokens_to_ids("<|endoftext|>")
    records, tensors = [], {}
    started = time.perf_counter()
    for index, case in enumerate(cases):
        record, case_tensors = run_case(
            models, protocol, case, suffix=suffix, pad_id=pad_id
        )
        records.append(record)
        tensors.update(case_tensors)
        print(
            json.dumps(
                {
                    "case": record["name"],
                    "index": index,
                    "answer": record["answer"],
                    "expected": record["expected_answer"],
                    "correct": record["correct"],
                    "spans": record["spans"],
                }
            ),
            flush=True,
        )
    memory_id = dec_tok.convert_tokens_to_ids(MEMORY_TOKEN)
    header = {
        "candidate_directory": str(candidate_dir.resolve()),
        "candidate": candidate,
        "candidate_sha256": digest(candidate_dir / "candidate.json"),
        "reader_sha256": candidate["reader_sha256"],
        "encoder_lora_sha256": candidate["encoder_lora_sha256"],
        "copy_head_sha256": candidate["copy_head_sha256"],
        "protocol_sha256": candidate["protocol_sha256"],
        "cases_file": str(Path(cases_path).resolve()),
        "cases_sha256": sha256(Path(cases_path)),
        "base_proofs": models["proofs"],
        "models": {k: str(Path(v).resolve()) for k, v in paths.items()},
        "reader_config": models["reader_config"],
        "dual_layer": protocol["dual_layer"],
        "lora": {
            "rank": protocol["rank"],
            "alpha": protocol["alpha"],
            "scale": lora_scale(protocol),
            "target_modules": list(
                protocol.get("target_modules", DEFAULT_LORA_TARGETS)
            ),
        },
        "copy_head_temperature": protocol.get(
            "copy_head_temperature", COPY_HEAD_TEMPERATURE
        ),
        "min_copy_gate": MIN_COPY_GATE,
        "memory_slots": RELEASED_MEMORY_SLOTS,
        "system_prompt": SYSTEM,
        "pad_id": pad_id,
        "eos_ids": list(EOS_IDS),
        "encoder_eos_id": int(eos_id),
        "encoder_special_suffix": suffix,
        "encoder_max_length": ENCODER_MAX_LENGTH,
        "memory_token": MEMORY_TOKEN,
        "memory_token_id": int(memory_id),
        "max_new_tokens": MAX_NEW_TOKENS,
        "decoding": {
            "snap_memory": "all",
            "copy_contiguous_runs": True,
            "copy_complete_words": True,
            "overflow": "truncate",
        },
        "execution": {
            "device": "cuda",
            "encoder_decoder_dtype": "bfloat16",
            "autocast": "bfloat16",
            "reader_weights": "float32 under bf16 autocast",
            "copy_head": "float32, autocast disabled",
            "snapper": "float32 cosine over the full decoder embedding table",
            "tf32": False,
            "gpu_cotenant_mib_at_start": cotenant,
        },
        "exporter_sha256": sha256(Path(__file__)),
        "torch_version": torch.__version__,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "correct": sum(r["correct"] for r in records),
        "case_count": len(records),
    }
    return write_reference(output_dir, header, records, tensors)


# ----------------------------------------------------------------------------
# command line


def model_paths(args) -> dict:
    paths = {}
    for key, relative in DEFAULT_ASSETS.items():
        value = getattr(args, key)
        if value is None:
            if args.assets is None:
                raise SystemExit(f"--{key.replace('_', '-')} or --assets is required")
            value = Path(args.assets) / relative
        paths[key] = Path(value)
    return paths


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    merge = sub.add_parser("merge-encoder")
    merge.add_argument("--candidate", type=Path)
    merge.add_argument("--encoder-f32", type=Path)
    merge.add_argument("--output", type=Path, required=True)
    merge.add_argument(
        "--merged-dtype", choices=sorted(MERGED_DTYPES), default="float32"
    )
    merge.add_argument(
        "--sidecars-only",
        action="store_true",
        help="rewrite memorilla_lock.json, model_manifest.json and the managed "
        "receipt of an existing merged directory without touching its shards",
    )
    reader = sub.add_parser("export-reader")
    reader.add_argument("--candidate", type=Path, required=True)
    reader.add_argument("--output", type=Path, required=True)
    reader.add_argument(
        "--include-encoder-lora",
        action="store_true",
        help="also pin encoder-lora.safetensors for run-time adapter application",
    )
    reader.add_argument(
        "--include-span-head",
        action="store_true",
        help="also pin a span-window candidate's span-head.safetensors",
    )
    reader.add_argument(
        "--include-span-gate-routes",
        action="store_true",
        help="also pin the routed gate's span-gate-routes.safetensors (with --include-span-head)",
    )
    dev = sub.add_parser("dev-cases")
    dev.add_argument("--data", type=Path, required=True)
    dev.add_argument("--output", type=Path, required=True)
    fresh = sub.add_parser("fresh-cases")
    fresh.add_argument("--data", type=Path, required=True)
    fresh.add_argument("--prior-training-data", type=Path, action="append", default=[])
    fresh.add_argument("--groups", type=int, required=True)
    fresh.add_argument("--seed", type=int, required=True)
    fresh.add_argument("--output", type=Path, required=True)
    ref = sub.add_parser("reference")
    ref.add_argument("--candidate", type=Path, required=True)
    ref.add_argument("--cases", type=Path, required=True)
    ref.add_argument("--output", type=Path, required=True)
    ref.add_argument("--max-cases", type=int)
    ref.add_argument(
        "--assets", type=Path, help="asset root with models/, checkpoints/, upstream/"
    )
    for key in DEFAULT_ASSETS:
        ref.add_argument("--" + key.replace("_", "-"), type=Path)
    args = parser.parse_args(argv)
    if args.command == "merge-encoder" and args.sidecars_only:
        written = refresh_sidecars(args.output, args.candidate)
        print(
            json.dumps(
                {
                    "output": str(args.output),
                    "candidate_sha256": written["lock"]["candidate_sha256"],
                    "lock_files": len(written["lock"]["files"]),
                    "receipt_artifacts": len(written["receipt"]["artifacts"]),
                }
            )
        )
    elif args.command == "merge-encoder":
        if args.candidate is None or args.encoder_f32 is None:
            raise SystemExit("--candidate and --encoder-f32 are required")
        receipt = merge_encoder(
            args.candidate,
            args.encoder_f32,
            args.output,
            merged_dtype=args.merged_dtype,
        )
        print(
            json.dumps(
                {
                    "output": str(args.output),
                    "counts": receipt["counts"],
                    "scale": receipt["scale"],
                    "max_abs_delta": receipt["max_abs_delta"],
                }
            )
        )
    elif args.command == "export-reader":
        manifest = export_reader(
            args.candidate,
            args.output,
            include_encoder_lora=args.include_encoder_lora,
            include_span_head=args.include_span_head,
            include_span_gate_routes=args.include_span_gate_routes,
        )
        _, _, loaded = load_bundle(args.output)
        if loaded != manifest:
            raise ValueError("exported reader bundle does not reload identically")
        print(
            json.dumps(
                {
                    "output": str(args.output),
                    "parameter_count": manifest["parameter_count"],
                    "encoder_pooling": manifest["execution"]["encoder_pooling"],
                }
            )
        )
    elif args.command == "dev-cases":
        cases = dev_cases(args.data)
        write_cases(
            args.output,
            cases,
            split="dev",
            data_directory=str(args.data.resolve()),
            dataset_sha256=digest(args.data / "manifest.json"),
        )
        print(json.dumps({"output": str(args.output), "cases": len(cases)}))
    elif args.command == "fresh-cases":
        effective, cases, hashes = fresh_cases(
            args.data, args.prior_training_data, args.groups, args.seed
        )
        write_cases(
            args.output,
            cases,
            split="postfreeze",
            seed=args.seed,
            effective_seed=effective,
            groups=args.groups,
            data_directory=str(args.data.resolve()),
            dataset_sha256=digest(args.data / "manifest.json"),
            prior_training_manifest_hashes=sorted(hashes),
        )
        print(
            json.dumps(
                {"output": str(args.output), "cases": len(cases), "seed": effective}
            )
        )
    else:
        receipt = reference(
            args.candidate,
            args.cases,
            args.output,
            model_paths(args),
            max_cases=args.max_cases,
        )
        print(
            json.dumps(
                {
                    "output": str(args.output),
                    "cases": receipt["case_count"],
                    "correct": receipt["correct"],
                }
            )
        )


if __name__ == "__main__":
    main()
