#!/usr/bin/env python3
"""Profile exact Gemma4 26B-A4B decode operations on Apple Metal.

The runner uses the real GGUF, resident mapped experts, and the explicitly
opt-in live Metal whole-model executor. The CLI still requests compiled
whole-model execution, but the A4B prepared-decode flag selects one executor
that owns both prefill and decode so KV state cannot cross an unsynchronized
provider boundary. On each invocation it isolates one selected MoE layer into
four Metal encoders so stage-boundary timestamp counters can attribute gate/up,
activation, down, and reduction without splitting every layer. The default
compiled lane remains the separate control when the flag is absent.

Logs may additionally contain a full-operation roofline ledger. The ledger is
self-describing and versioned so captures remain comparable when the runtime
grows new operations::

    metal_roofline_op: schema=antfly.metal_roofline_op.v1 regime=decode \
      frame=8 layer=0 layer_kind=local kv_position=29 op=attention_q \
      shape=1x2816x4096 gpu_ns=600 logical_bytes=6000 dispatches=1 \
      pipeline=termite_q4_0_linear_1x_reduce barrier_before=0 \
      barrier_after=0 barrier_scope=none barrier_reason=none
    metal_roofline_frame: schema=antfly.metal_roofline_frame.v1 \
      regime=decode frame=8 frame_gpu_ns=1000 unattributed_gpu_ns=400 \
      barrier_count=0

``logical_bytes`` is the encoded kernels' source-access estimate, not an
allocation, residency size, or hardware-counter byte count. Decimal effective GB/s is
``logical_bytes / gpu_ns``. Every v1 frame must reconcile exactly as
``sum(operation gpu_ns) + unattributed_gpu_ns == frame_gpu_ns``. Older logs
without these records retain the original v1 summary schema.

Runtime v1 records label their timing method explicitly: operation ``gpu_ns``
is whole-frame GPU time apportioned by compute-encoder counter ticks, not a
direct timestamp around an individual dispatch. Use it as a prioritization
ledger rather than a standalone kernel microbenchmark.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
from typing import Any


DETAIL_LINE = re.compile(r"^metal_stage_detail_ns:\s+(.+)$", re.MULTILINE)
ROOFLINE_OP_LINE = re.compile(r"^metal_roofline_op:\s+(.+)$", re.MULTILINE)
ROOFLINE_FRAME_LINE = re.compile(r"^metal_roofline_frame:\s+(.+)$", re.MULTILINE)
ROOFLINE_DROP_LINE = re.compile(r"^metal-roofline:\s+drop\s+(.+)$", re.MULTILINE)
TOKEN_IDS = re.compile(r"^token_ids:\s*(.*?)\s*$", re.MULTILINE)
DISPATCH = re.compile(
    r"^metal_dispatch_profile_pipeline:\s+rank=(\d+)\s+dispatches=(\d+)\s+"
    r"threadgroups=(\d+)\s+threads=(\d+)\s+grid_items=(\d+)\s+label=(.+)$",
    re.MULTILINE,
)
POLICY_PREFIXES = ("TERMITE_", "ANTFLY_GEMMA4_", "ANTFLY_INFERENCE_")
PROFILE_SCHEMA_V1 = "antfly.gemma4_a4b_metal_profile.v1"
PROFILE_SCHEMA_V2 = "antfly.gemma4_a4b_metal_profile.v2"
ROOFLINE_OP_SCHEMA = "antfly.metal_roofline_op.v1"
ROOFLINE_FRAME_SCHEMA = "antfly.metal_roofline_frame.v1"
ROOFLINE_LEDGER_SCHEMA = "antfly.gemma4_a4b_roofline_ledger.v1"
A4B_LAYER_COUNT = 30
A4B_GLOBAL_LAYERS = frozenset((5, 11, 17, 23, 29))
SHAPE = re.compile(r"^[1-9]\d*(?:x[1-9]\d*)+$")
IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:/+-]+$")


class ProfileError(RuntimeError):
    pass


def parse_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for item in text.split():
        key, separator, value = item.partition("=")
        if not separator or not key or key in fields:
            raise ProfileError(f"malformed detail field: {item!r}")
        fields[key] = value
    return fields


def parse_integer(
    fields: dict[str, str],
    key: str,
    path: Path,
    *,
    minimum: int = 0,
) -> int:
    try:
        value = int(fields[key])
    except (KeyError, ValueError) as exc:
        raise ProfileError(f"invalid {key} in {path}: {exc}") from exc
    if value < minimum:
        raise ProfileError(f"{key} must be at least {minimum} in {path}")
    return value


def parse_boolean(fields: dict[str, str], key: str, path: Path) -> bool:
    try:
        value = fields[key]
    except KeyError as exc:
        raise ProfileError(f"missing {key} in {path}") from exc
    if value not in ("0", "1"):
        raise ProfileError(f"{key} must be 0 or 1 in {path}")
    return value == "1"


def parse_identifier(fields: dict[str, str], key: str, path: Path) -> str:
    try:
        value = fields[key]
    except KeyError as exc:
        raise ProfileError(f"missing {key} in {path}") from exc
    if not IDENTIFIER.fullmatch(value):
        raise ProfileError(f"invalid {key}={value!r} in {path}")
    return value


def parse_roofline_operation(fields: dict[str, str], path: Path) -> dict[str, Any]:
    if fields.get("schema") != ROOFLINE_OP_SCHEMA:
        raise ProfileError(
            f"unsupported roofline operation schema {fields.get('schema')!r} in {path}"
        )
    if fields.get("regime") != "decode":
        raise ProfileError(f"roofline operation is not decode in {path}")

    frame = parse_integer(fields, "frame", path)
    try:
        layer = int(fields["layer"])
    except (KeyError, ValueError) as exc:
        raise ProfileError(f"invalid layer in roofline operation in {path}: {exc}") from exc
    layer_kind = parse_identifier(fields, "layer_kind", path)
    if layer == -1:
        if layer_kind != "model":
            raise ProfileError(f"layer=-1 requires layer_kind=model in {path}")
    elif 0 <= layer < A4B_LAYER_COUNT:
        expected_kind = "global" if layer in A4B_GLOBAL_LAYERS else "local"
        if layer_kind != expected_kind:
            raise ProfileError(
                f"layer {layer} requires layer_kind={expected_kind}, got {layer_kind} in {path}"
            )
    else:
        raise ProfileError(f"roofline layer must be -1 or 0..29 in {path}")

    shape = fields.get("shape")
    if shape is None or not SHAPE.fullmatch(shape):
        raise ProfileError(f"invalid roofline shape {shape!r} in {path}")
    shape_dimensions = [int(dimension) for dimension in shape.split("x")]
    gpu_ns = parse_integer(fields, "gpu_ns", path, minimum=1)
    logical_bytes = parse_integer(fields, "logical_bytes", path)
    dispatches = parse_integer(fields, "dispatches", path, minimum=1)
    barrier_before = parse_boolean(fields, "barrier_before", path)
    barrier_after = parse_boolean(fields, "barrier_after", path)
    barrier_scope = parse_identifier(fields, "barrier_scope", path)
    barrier_reason = parse_identifier(fields, "barrier_reason", path)
    has_barrier = barrier_before or barrier_after
    if has_barrier and (barrier_scope == "none" or barrier_reason == "none"):
        raise ProfileError(f"barrier scope and reason are required in {path}")
    if not has_barrier and (barrier_scope != "none" or barrier_reason != "none"):
        raise ProfileError(f"barrier metadata requires a barrier in {path}")

    known_fields = {
        "schema",
        "regime",
        "frame",
        "layer",
        "layer_kind",
        "kv_position",
        "op",
        "shape",
        "gpu_ns",
        "logical_bytes",
        "dispatches",
        "pipeline",
        "barrier_before",
        "barrier_after",
        "barrier_scope",
        "barrier_reason",
    }
    return {
        "schema": ROOFLINE_OP_SCHEMA,
        "regime": "decode",
        "frame": frame,
        "layer": layer,
        "layer_kind": layer_kind,
        "kv_position": parse_integer(fields, "kv_position", path),
        "op": parse_identifier(fields, "op", path),
        "shape": shape,
        "shape_dimensions": shape_dimensions,
        "gpu_ns": gpu_ns,
        "gpu_us": gpu_ns / 1_000.0,
        "logical_bytes": logical_bytes,
        "effective_gbps": logical_bytes / gpu_ns,
        "dispatches": dispatches,
        "pipeline": parse_identifier(fields, "pipeline", path),
        "barrier": {
            "before": barrier_before,
            "after": barrier_after,
            "scope": barrier_scope,
            "reason": barrier_reason,
        },
        "metadata": {
            key: value for key, value in sorted(fields.items()) if key not in known_fields
        },
    }


def parse_roofline_frame(fields: dict[str, str], path: Path) -> dict[str, Any]:
    if fields.get("schema") != ROOFLINE_FRAME_SCHEMA:
        raise ProfileError(
            f"unsupported roofline frame schema {fields.get('schema')!r} in {path}"
        )
    if fields.get("regime") != "decode":
        raise ProfileError(f"roofline frame is not decode in {path}")
    known_fields = {
        "schema",
        "regime",
        "frame",
        "frame_gpu_ns",
        "unattributed_gpu_ns",
        "barrier_count",
        "planned_barrier_count",
        "nonplanned_barrier_count",
        "timing",
        "logical_convention",
    }
    result = {
        "schema": ROOFLINE_FRAME_SCHEMA,
        "regime": "decode",
        "frame": parse_integer(fields, "frame", path),
        "frame_gpu_ns": parse_integer(fields, "frame_gpu_ns", path, minimum=1),
        "unattributed_gpu_ns": parse_integer(fields, "unattributed_gpu_ns", path),
        "barrier_count": parse_integer(fields, "barrier_count", path),
        "timing_method": fields.get("timing", "unspecified"),
        "logical_convention": fields.get("logical_convention", "unspecified"),
        "metadata": {
            key: value for key, value in sorted(fields.items()) if key not in known_fields
        },
    }
    if "planned_barrier_count" in fields:
        result["planned_barrier_count"] = parse_integer(
            fields, "planned_barrier_count", path
        )
    if "nonplanned_barrier_count" in fields:
        result["nonplanned_barrier_count"] = parse_integer(
            fields, "nonplanned_barrier_count", path
        )
    if (
        "planned_barrier_count" in result
        and "nonplanned_barrier_count" in result
        and result["planned_barrier_count"] + result["nonplanned_barrier_count"]
        != result["barrier_count"]
    ):
        raise ProfileError(f"roofline planner barrier counts do not reconcile in {path}")
    return result


def summarize_roofline_records(
    operations: list[dict[str, Any]],
    frames: list[dict[str, Any]],
    path: Path,
) -> dict[str, Any]:
    if not operations and not frames:
        return {}
    if not operations or not frames:
        raise ProfileError(f"roofline operation/frame records are incomplete in {path}")

    operations_by_frame: dict[int, list[dict[str, Any]]] = {}
    for operation in operations:
        operations_by_frame.setdefault(int(operation["frame"]), []).append(operation)
    frames_by_id: dict[int, dict[str, Any]] = {}
    for frame in frames:
        frame_id = int(frame["frame"])
        if frame_id in frames_by_id:
            raise ProfileError(f"duplicate roofline frame {frame_id} in {path}")
        frames_by_id[frame_id] = frame
    if set(operations_by_frame) != set(frames_by_id):
        raise ProfileError(f"roofline frame coverage mismatch in {path}")

    reconciliation: list[dict[str, Any]] = []
    barrier_reasons: dict[str, int] = {}
    barrier_scopes: dict[str, int] = {}
    barrier_total = 0
    barrier_before_total = 0
    barrier_after_total = 0
    timing_methods = sorted({str(frame["timing_method"]) for frame in frames})
    logical_conventions = sorted(
        {str(frame["logical_convention"]) for frame in frames}
    )
    for frame_id, frame in sorted(frames_by_id.items()):
        frame_operations = operations_by_frame[frame_id]
        operation_gpu_ns = sum(int(operation["gpu_ns"]) for operation in frame_operations)
        computed_barriers = sum(
            int(bool(operation["barrier"][side]))
            for operation in frame_operations
            for side in ("before", "after")
        )
        if computed_barriers != frame["barrier_count"]:
            raise ProfileError(
                f"roofline barrier count does not reconcile ({computed_barriers} != "
                f"{frame['barrier_count']}) for frame {frame_id} in {path}"
            )
        reconciled_ns = operation_gpu_ns + int(frame["unattributed_gpu_ns"])
        if reconciled_ns != frame["frame_gpu_ns"]:
            raise ProfileError(
                f"roofline timing does not reconcile ({reconciled_ns} != "
                f"{frame['frame_gpu_ns']}) for frame {frame_id} in {path}"
            )
        kv_positions = {int(operation["kv_position"]) for operation in frame_operations}
        if len(kv_positions) != 1:
            raise ProfileError(f"roofline frame {frame_id} mixes KV positions in {path}")
        barrier_total += computed_barriers
        for operation in frame_operations:
            barrier_before_total += int(operation["barrier"]["before"])
            barrier_after_total += int(operation["barrier"]["after"])
            if operation["barrier"]["before"] or operation["barrier"]["after"]:
                reason = str(operation["barrier"]["reason"])
                scope = str(operation["barrier"]["scope"])
                boundary_count = int(operation["barrier"]["before"]) + int(
                    operation["barrier"]["after"]
                )
                barrier_reasons[reason] = barrier_reasons.get(reason, 0) + boundary_count
                barrier_scopes[scope] = barrier_scopes.get(scope, 0) + boundary_count
        reconciliation.append(
            {
                "frame": frame_id,
                "kv_position": next(iter(kv_positions)),
                "frame_gpu_ns": int(frame["frame_gpu_ns"]),
                "frame_gpu_us": int(frame["frame_gpu_ns"]) / 1_000.0,
                "operation_gpu_ns": operation_gpu_ns,
                "operation_gpu_us": operation_gpu_ns / 1_000.0,
                "unattributed_gpu_ns": int(frame["unattributed_gpu_ns"]),
                "unattributed_gpu_us": int(frame["unattributed_gpu_ns"]) / 1_000.0,
                "delta_ns": 0,
                "reconciled": True,
                "barrier_count": computed_barriers,
                "planned_barrier_count": frame.get("planned_barrier_count"),
                "nonplanned_barrier_count": frame.get("nonplanned_barrier_count"),
                "timing_method": frame["timing_method"],
                "logical_convention": frame["logical_convention"],
            }
        )

    layers = sorted(
        {int(operation["layer"]) for operation in operations if int(operation["layer"]) >= 0}
    )
    local_layers = [layer for layer in layers if layer not in A4B_GLOBAL_LAYERS]
    global_layers = [layer for layer in layers if layer in A4B_GLOBAL_LAYERS]
    return {
        "schema": ROOFLINE_LEDGER_SCHEMA,
        "units": {
            "gpu_ns": "nanoseconds",
            "gpu_us": "microseconds",
            "logical_bytes": "bytes",
            "effective_gbps": "decimal GB/s; logical_bytes / gpu_ns",
        },
        "timing_contract": {
            "methods": timing_methods,
            "direct_per_operation_timestamps": False,
            "description": "whole-frame GPU time apportioned by per-encoder counter ticks",
        },
        "logical_byte_contract": {
            "conventions": logical_conventions,
            "description": "encoded source-access estimate, not allocation or residency size",
        },
        "operations": operations,
        "frames": reconciliation,
        "coverage": {
            "frame_ids": sorted(frames_by_id),
            "kv_positions": sorted(
                {int(operation["kv_position"]) for operation in operations}
            ),
            "layers": layers,
            "local_layers": local_layers,
            "global_layers": global_layers,
            "model_level_operations": sum(
                1 for operation in operations if int(operation["layer"]) == -1
            ),
            "operation_counts_by_layer_kind": {
                kind: sum(1 for operation in operations if operation["layer_kind"] == kind)
                for kind in ("local", "global", "model")
            },
            "complete_30_layer_coverage": layers == list(range(A4B_LAYER_COUNT)),
        },
        "barriers": {
            "count": barrier_total,
            "before_count": barrier_before_total,
            "after_count": barrier_after_total,
            "reasons": dict(sorted(barrier_reasons.items())),
            "scopes": dict(sorted(barrier_scopes.items())),
        },
        "reconciliation": {
            "all_frames_reconciled": True,
            "frame_count": len(reconciliation),
            "frame_gpu_ns": sum(frame["frame_gpu_ns"] for frame in reconciliation),
            "frame_gpu_us": sum(frame["frame_gpu_us"] for frame in reconciliation),
            "operation_gpu_ns": sum(frame["operation_gpu_ns"] for frame in reconciliation),
            "operation_gpu_us": sum(frame["operation_gpu_us"] for frame in reconciliation),
            "unattributed_gpu_ns": sum(
                frame["unattributed_gpu_ns"] for frame in reconciliation
            ),
            "unattributed_gpu_us": sum(
                frame["unattributed_gpu_us"] for frame in reconciliation
            ),
            "delta_ns": 0,
            "operation_accounted_fraction": sum(
                frame["operation_gpu_ns"] for frame in reconciliation
            )
            / sum(frame["frame_gpu_ns"] for frame in reconciliation),
        },
    }


def parse_log(
    path: Path,
    layer: int,
    specialized: bool,
    *,
    require_roofline: bool = False,
) -> dict[str, Any]:
    log = path.read_text(errors="replace")
    samples: list[dict[str, int | str]] = []
    numeric = (
        "layer",
        "frame_gpu",
        "attention",
        "ffn_unclassified",
        "ple",
        "tail",
        "embedding",
        "other",
        "moe_gate_up",
        "moe_activation",
        "moe_down",
        "moe_reduce",
    )
    for match in DETAIL_LINE.finditer(log):
        fields = parse_fields(match.group(1))
        if fields.get("regime") != "decode":
            continue
        try:
            sample: dict[str, int | str] = {"regime": "decode"}
            sample.update({key: int(fields[key]) for key in numeric})
        except (KeyError, ValueError) as exc:
            raise ProfileError(f"invalid detail record in {path}: {exc}") from exc
        if sample["layer"] != layer:
            raise ProfileError(f"detail layer mismatch in {path}")
        components = sum(int(sample[key]) for key in numeric[2:])
        if components != sample["frame_gpu"]:
            raise ProfileError(
                f"detail timing does not reconcile ({components} != "
                f"{sample['frame_gpu']}) in {path}"
            )
        for key in ("moe_gate_up", "moe_activation", "moe_down", "moe_reduce"):
            if int(sample[key]) <= 0:
                raise ProfileError(f"{key} was not isolated in {path}")
        samples.append(sample)
    if not samples:
        raise ProfileError(f"no complete decode detail records in {path}")

    roofline_operations = [
        parse_roofline_operation(parse_fields(match.group(1)), path)
        for match in ROOFLINE_OP_LINE.finditer(log)
    ]
    roofline_frames = [
        parse_roofline_frame(parse_fields(match.group(1)), path)
        for match in ROOFLINE_FRAME_LINE.finditer(log)
    ]
    roofline = summarize_roofline_records(roofline_operations, roofline_frames, path)
    if require_roofline and not roofline:
        drops = [match.group(1) for match in ROOFLINE_DROP_LINE.finditer(log)]
        detail = f"; drops: {' | '.join(drops)}" if drops else ""
        raise ProfileError(f"no complete roofline capture in {path}{detail}")
    if roofline and len(roofline["frames"]) == len(samples):
        for sample, roofline_frame in zip(samples, roofline["frames"], strict=True):
            if int(sample["frame_gpu"]) != int(roofline_frame["frame_gpu_ns"]):
                raise ProfileError(
                    "roofline frame GPU time does not match legacy stage detail "
                    f"({roofline_frame['frame_gpu_ns']} != {sample['frame_gpu']}) in {path}"
                )
        roofline["reconciliation"]["legacy_stage_detail_crosscheck"] = True
    elif roofline:
        roofline["reconciliation"]["legacy_stage_detail_crosscheck"] = False

    token_matches = list(TOKEN_IDS.finditer(log))
    if not token_matches:
        raise ProfileError(f"token_ids marker missing in {path}")
    token_text = token_matches[-1].group(1).strip()
    try:
        token_ids = [int(value) for value in token_text.split()]
    except ValueError as exc:
        raise ProfileError(f"invalid token_ids marker in {path}: {exc}") from exc
    token_sha256 = hashlib.sha256(token_text.encode()).hexdigest()

    dispatches: dict[str, int] = {}
    for match in DISPATCH.finditer(log):
        label = match.group(6).strip()
        dispatches[label] = max(dispatches.get(label, 0), int(match.group(2)))
    if not dispatches:
        raise ProfileError(f"dispatch profile missing in {path}")
    gate_label = "termite_q4_0_linear_id_a4b_gate_up"
    down_label = "termite_q4_0_linear_id_a4b_down"
    if specialized:
        if dispatches.get(gate_label, 0) < 30 or dispatches.get(down_label, 0) < 30:
            raise ProfileError(f"specialized A4B dispatches missing in {path}")
        if "metal_a4b_specialized_id: enabled=1" not in log:
            raise ProfileError(f"specialized A4B admission marker missing in {path}")
    elif dispatches.get(gate_label, 0) or dispatches.get(down_label, 0):
        raise ProfileError(f"rollback run used specialized A4B pipelines in {path}")

    forbidden = (
        "CompactMoeChunkExecutionFailed",
        "GENERATION_FAILED",
        "metal-runtime frame fallback",
        "mapped_moe_failure",
    )
    for marker in forbidden:
        if marker in log:
            raise ProfileError(f"forbidden marker {marker!r} in {path}")
    parsed = {
        "samples": samples,
        "token_count": len(token_ids),
        "token_ids_sha256": token_sha256,
        "dispatches": dict(sorted(dispatches.items())),
    }
    if roofline:
        parsed["roofline"] = roofline
    return parsed


def aggregate_roofline(samples: list[dict[str, Any]]) -> dict[str, Any]:
    operations: list[dict[str, Any]] = []
    frames: list[dict[str, Any]] = []
    aggregate_barrier_reasons: dict[str, int] = {}
    aggregate_barrier_scopes: dict[str, int] = {}
    layers: set[int] = set()
    kv_positions: set[int] = set()
    totals: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for sample in samples:
        roofline = sample["roofline"]
        label = str(sample.get("label", f"layer-{sample.get('layer', 'unknown')}"))
        for operation in roofline["operations"]:
            annotated = {**operation, "sample_label": label}
            operations.append(annotated)
            layer = int(operation["layer"])
            if layer >= 0:
                layers.add(layer)
            kv_positions.add(int(operation["kv_position"]))
            key = (
                str(operation["op"]),
                str(operation["shape"]),
                str(operation["layer_kind"]),
                str(operation["pipeline"]),
            )
            total = totals.setdefault(
                key,
                {
                    "op": key[0],
                    "shape": key[1],
                    "shape_dimensions": list(operation["shape_dimensions"]),
                    "layer_kind": key[2],
                    "pipeline": key[3],
                    "occurrences": 0,
                    "dispatches": 0,
                    "gpu_ns": 0,
                    "logical_bytes": 0,
                    "gpu_samples_ns": [],
                    "layers": set(),
                    "kv_positions": set(),
                },
            )
            total["occurrences"] += 1
            total["dispatches"] += int(operation["dispatches"])
            total["gpu_ns"] += int(operation["gpu_ns"])
            total["logical_bytes"] += int(operation["logical_bytes"])
            total["gpu_samples_ns"].append(int(operation["gpu_ns"]))
            total["layers"].add(int(operation["layer"]))
            total["kv_positions"].add(int(operation["kv_position"]))
        for frame in roofline["frames"]:
            frames.append({**frame, "sample_label": label})
        for reason, count in roofline["barriers"]["reasons"].items():
            aggregate_barrier_reasons[reason] = aggregate_barrier_reasons.get(reason, 0) + count
        for scope, count in roofline["barriers"]["scopes"].items():
            aggregate_barrier_scopes[scope] = aggregate_barrier_scopes.get(scope, 0) + count

    operation_totals: list[dict[str, Any]] = []
    for key in sorted(totals):
        total = totals[key]
        gpu_samples_ns = total.pop("gpu_samples_ns")
        total["layers"] = sorted(total["layers"])
        total["kv_positions"] = sorted(total["kv_positions"])
        total["gpu_us"] = total["gpu_ns"] / 1_000.0
        total["median_gpu_us"] = statistics.median(gpu_samples_ns) / 1_000.0
        total["effective_gbps"] = (
            total["logical_bytes"] / total["gpu_ns"] if total["gpu_ns"] else 0.0
        )
        operation_totals.append(total)

    frame_gpu_ns = sum(int(frame["frame_gpu_ns"]) for frame in frames)
    operation_gpu_ns = sum(int(frame["operation_gpu_ns"]) for frame in frames)
    unattributed_gpu_ns = sum(int(frame["unattributed_gpu_ns"]) for frame in frames)
    return {
        "schema": ROOFLINE_LEDGER_SCHEMA,
        "units": samples[0]["roofline"]["units"],
        "timing_contract": samples[0]["roofline"]["timing_contract"],
        "logical_byte_contract": samples[0]["roofline"]["logical_byte_contract"],
        "operations": operations,
        "operation_totals": operation_totals,
        "frames": frames,
        "coverage": {
            "sample_count": len(samples),
            "frame_count": len(frames),
            "kv_positions": sorted(kv_positions),
            "layers": sorted(layers),
            "local_layers": sorted(layer for layer in layers if layer not in A4B_GLOBAL_LAYERS),
            "global_layers": sorted(layer for layer in layers if layer in A4B_GLOBAL_LAYERS),
            "operation_counts_by_layer_kind": {
                kind: sum(1 for operation in operations if operation["layer_kind"] == kind)
                for kind in ("local", "global", "model")
            },
            "complete_30_layer_coverage": layers == set(range(A4B_LAYER_COUNT)),
        },
        "barriers": {
            "count": sum(int(frame["barrier_count"]) for frame in frames),
            "before_count": sum(
                int(operation["barrier"]["before"]) for operation in operations
            ),
            "after_count": sum(
                int(operation["barrier"]["after"]) for operation in operations
            ),
            "reasons": dict(sorted(aggregate_barrier_reasons.items())),
            "scopes": dict(sorted(aggregate_barrier_scopes.items())),
        },
        "reconciliation": {
            "all_frames_reconciled": all(bool(frame["reconciled"]) for frame in frames),
            "frame_gpu_ns": frame_gpu_ns,
            "frame_gpu_us": frame_gpu_ns / 1_000.0,
            "operation_gpu_ns": operation_gpu_ns,
            "operation_gpu_us": operation_gpu_ns / 1_000.0,
            "unattributed_gpu_ns": unattributed_gpu_ns,
            "unattributed_gpu_us": unattributed_gpu_ns / 1_000.0,
            "delta_ns": frame_gpu_ns - operation_gpu_ns - unattributed_gpu_ns,
            "operation_accounted_fraction": operation_gpu_ns / frame_gpu_ns,
        },
    }


def run_sample(args: argparse.Namespace, layer: int, run: int) -> dict[str, Any]:
    label = f"layer-{layer:02d}-run-{run:02d}"
    log_path = args.out_dir / f"{label}.log"
    json_path = args.out_dir / f"{label}.json"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(POLICY_PREFIXES)
    }
    environment.update(
        {
            "TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH": "1",
            "TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE": "1",
            "TERMITE_METAL_STAGE_TIMING": "1",
            "TERMITE_METAL_STAGE_TIMING_A4B_DETAIL": "1",
            "TERMITE_METAL_STAGE_TIMING_ROOFLINE": "1",
            "TERMITE_METAL_STAGE_TIMING_A4B_LAYER": str(layer),
            "TERMITE_METAL_STAGE_TIMING_PREFILL_MAX": "0",
            "TERMITE_METAL_STAGE_TIMING_DECODE_START": "8",
            "TERMITE_METAL_STAGE_TIMING_DECODE_STRIDE": "16",
            "TERMITE_METAL_STAGE_TIMING_DECODE_MAX": "3",
            "TERMITE_METAL_TRACE_DISPATCH_PROFILE": "1",
            "TERMITE_METAL_TRACE_DECODER_RUNTIME_DECODE": "1",
            "TERMITE_GEN_STAGE_DEBUG": "1",
            "ANTFLY_INFERENCE_JSON_TOKEN_IDS": "1",
        }
    )
    if not args.specialized:
        environment["TERMITE_METAL_DISABLE_A4B_SPECIALIZED_ID"] = "1"
    command = [
        "/usr/bin/time",
        "-l",
        str(args.binary),
        "generate",
        str(args.model),
        args.prompt,
        "--backend",
        "metal",
        "--mode",
        "compiled",
        "--compiled-target",
        "whole-model",
        "--cache-dtype",
        "f16",
        "--a4b-residency-mode",
        "resident",
        "--a4b-memory-budget-mb",
        str(args.budget_mb),
        "--backend-budget-mb",
        str(args.budget_mb),
        "--combined-budget-mb",
        str(args.budget_mb),
        "--raw-prompt",
        "--temperature",
        "0",
        "--ignore-eos",
        "--max-tokens",
        str(args.output_tokens),
        "--print-token-ids",
        "--json-timing",
        str(json_path),
    ]
    with log_path.open("w") as log:
        completed = subprocess.run(
            command,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )
    if completed.returncode != 0:
        raise ProfileError(f"{label} exited {completed.returncode}; see {log_path}")
    parsed = parse_log(
        log_path,
        layer,
        args.specialized,
        require_roofline=True,
    )
    parsed.update({"label": label, "layer": layer, "run": run})
    return parsed


def summarize(samples: list[dict[str, Any]], specialized: bool) -> dict[str, Any]:
    token_hashes = {sample["token_ids_sha256"] for sample in samples}
    token_counts = {sample["token_count"] for sample in samples}
    if len(token_hashes) != 1 or len(token_counts) != 1:
        raise ProfileError("profile runs did not preserve deterministic token output")
    records = [record for sample in samples for record in sample["samples"]]
    phases = ("moe_gate_up", "moe_activation", "moe_down", "moe_reduce")
    medians = {
        phase: statistics.median(int(record[phase]) for record in records)
        for phase in phases
    }
    per_layer = sum(medians.values())
    roofline_presence = ["roofline" in sample for sample in samples]
    if any(roofline_presence) and not all(roofline_presence):
        raise ProfileError("profile runs mixed legacy and roofline log formats")
    summary = {
        "schema": PROFILE_SCHEMA_V2 if all(roofline_presence) else PROFILE_SCHEMA_V1,
        "specialized": specialized,
        "token_count": next(iter(token_counts)),
        "token_ids_sha256": next(iter(token_hashes)),
        "sampled_frames": len(records),
        "median_selected_layer_ns": medians,
        "median_selected_layer_total_ns": per_layer,
        "estimated_30_layer_routed_moe_ns": per_layer * 30,
        "samples": samples,
    }
    if all(roofline_presence):
        summary["roofline"] = aggregate_roofline(samples)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--layers", default="0,15,29")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--output-tokens", type=int, default=64)
    parser.add_argument("--budget-mb", type=int, default=16384)
    parser.add_argument("--specialized", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--prompt",
        default=(
            "<|turn>user\nExplain in two concise sentences why LSM trees are useful "
            "for write-heavy databases.<turn|>\n<|turn>model\n<|channel>final\n<channel|>"
        ),
    )
    args = parser.parse_args()
    try:
        args.layers = [int(value) for value in args.layers.split(",")]
    except ValueError as exc:
        parser.error(f"invalid --layers: {exc}")
    if not args.layers or any(layer < 0 or layer >= 30 for layer in args.layers):
        parser.error("--layers must contain Gemma4 A4B layer indices 0..29")
    if args.runs <= 0 or args.output_tokens <= 48:
        parser.error("--runs must be positive and --output-tokens must exceed 48")
    args.binary = args.binary.resolve()
    args.model = args.model.resolve()
    args.out_dir = args.out_dir.resolve()
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error(f"binary is not executable: {args.binary}")
    if not args.model.is_file():
        parser.error(f"model is not a file: {args.model}")
    if args.out_dir.exists() and any(args.out_dir.iterdir()):
        parser.error(f"--out-dir must be empty: {args.out_dir}")
    return args


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    samples = [
        run_sample(args, layer, run)
        for layer in args.layers
        for run in range(1, args.runs + 1)
    ]
    summary = summarize(samples, args.specialized)
    (args.out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    medians = summary["median_selected_layer_ns"]
    print(
        "A4B routed profile: "
        f"gate_up={medians['moe_gate_up'] / 1e6:.3f}ms "
        f"activation={medians['moe_activation'] / 1e6:.3f}ms "
        f"down={medians['moe_down'] / 1e6:.3f}ms "
        f"reduce={medians['moe_reduce'] / 1e6:.3f}ms "
        f"estimated_30_layer={summary['estimated_30_layer_routed_moe_ns'] / 1e6:.3f}ms"
    )
    if "roofline" in summary:
        roofline = summary["roofline"]
        print(
            "A4B roofline ledger: "
            f"operations={len(roofline['operations'])} "
            f"frames={roofline['coverage']['frame_count']} "
            f"layers={len(roofline['coverage']['layers'])}/30 "
            f"kv_positions={roofline['coverage']['kv_positions']} "
            f"accounted={roofline['reconciliation']['operation_accounted_fraction']:.4f} "
            f"barriers={roofline['barriers']['count']}"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProfileError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
