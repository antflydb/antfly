#!/usr/bin/env python3
"""Fail-closed evidence gate for Nomic v1.5 Metal competitiveness.

The four required inputs are JSONL captures from the native direct benchmark,
PyTorch direct reference, native HTTP benchmark, and PyTorch HTTP benchmark.
Optional embedding records turn the same captures into the six-cell numeric
parity gate.  This tool never writes into the source tree.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


EXPECTED_CELLS = {(batch, sequence_length) for batch in (1, 2, 4) for sequence_length in (16, 128)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--native-direct", required=True)
    parser.add_argument("--pytorch-direct", required=True)
    parser.add_argument("--native-http", required=True)
    parser.add_argument("--pytorch-http", required=True)
    parser.add_argument("--parity", help="compact six-cell output from bench_nomic_pytorch_mps.py parity")
    parser.add_argument("--ffn-fusion-disabled")
    parser.add_argument("--pool-normalize-disabled")
    parser.add_argument("--q8-sdpa-disabled")
    parser.add_argument("--out")
    return parser.parse_args()


def load_jsonl(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(Path(path).read_text().splitlines(), start=1):
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_number}: invalid JSON evidence") from exc
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number}: evidence row must be an object")
        rows.append(value)
    if not rows:
        raise ValueError(f"{path}: contains no JSON evidence rows")
    return rows


def cells(
    rows: list[dict[str, Any]],
    kind: str,
    expected: set[tuple[int, int]] = EXPECTED_CELLS,
) -> dict[tuple[int, int], dict[str, Any]]:
    result: dict[tuple[int, int], dict[str, Any]] = {}
    for row in rows:
        if row.get("kind") != kind:
            continue
        try:
            key = (int(row["batch"]), int(row["sequence_length"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{kind}: row has no valid batch/sequence length") from exc
        if key in result:
            raise ValueError(f"{kind}: duplicate evidence for cell {key}")
        result[key] = row
    if set(result) != expected:
        raise ValueError(f"{kind}: expected cells {sorted(expected)}, got {sorted(result)}")
    return result


def embedding_cells(rows: list[dict[str, Any]], kind: str) -> dict[tuple[int, int], list[list[float]]]:
    result: dict[tuple[int, int], list[list[float]]] = {}
    for row in rows:
        if row.get("kind") != kind:
            continue
        key = (int(row["batch"]), int(row["sequence_length"]))
        embeddings = row.get("embeddings")
        if not isinstance(embeddings, list) or not embeddings or any(not isinstance(item, list) for item in embeddings):
            raise ValueError(f"{kind}: malformed embedding evidence for {key}")
        result[key] = embeddings
    return result


def ratio(candidate: dict[str, Any], reference: dict[str, Any]) -> float:
    candidate_ms = float(candidate["mean_ms"])
    reference_ms = float(reference["mean_ms"])
    if not math.isfinite(candidate_ms) or not math.isfinite(reference_ms) or candidate_ms <= 0 or reference_ms <= 0:
        raise ValueError("non-positive or non-finite mean_ms")
    return candidate_ms / reference_ms


def compare_embeddings(actual: list[list[float]], reference: list[list[float]]) -> dict[str, float]:
    if len(actual) != len(reference) or any(len(left) != len(right) for left, right in zip(actual, reference)):
        raise ValueError("embedding shapes differ")
    values = [(left_value, right_value) for left, right in zip(actual, reference) for left_value, right_value in zip(left, right)]
    errors = [abs(left - right) for left, right in values]
    cosines = []
    for left, right in zip(actual, reference):
        dot = sum(a * b for a, b in zip(left, right))
        left_norm = math.sqrt(sum(a * a for a in left))
        right_norm = math.sqrt(sum(b * b for b in right))
        cosines.append(dot / (left_norm * right_norm))
    return {"max_abs_error": max(errors), "mean_abs_error": sum(errors) / len(errors), "min_cosine": min(cosines)}


def validate_optimization(
    name: str,
    optimized: dict[tuple[int, int], dict[str, Any]],
    disabled: dict[tuple[int, int], dict[str, Any]],
    affected: set[tuple[int, int]],
    improvement_cells: set[tuple[int, int]],
    failures: list[str],
) -> dict[str, float]:
    ratios = {
        f"b{batch}_s{sequence_length}": ratio(optimized[(batch, sequence_length)], disabled[(batch, sequence_length)])
        for batch, sequence_length in affected
    }
    for batch, sequence_length in improvement_cells:
        key = f"b{batch}_s{sequence_length}"
        value = ratios[key]
        if value > 0.95:
            failures.append(
                f"{name}: {key} improvement is {(1 - value) * 100:.2f}%, below required 5%"
            )
    for key, value in ratios.items():
        if value > 1.02:
            failures.append(f"{name}: {key} regresses {(value - 1) * 100:.2f}%, above 2%")
    return ratios


def main() -> int:
    args = parse_args()
    native_direct_rows = load_jsonl(args.native_direct)
    pytorch_direct_rows = load_jsonl(args.pytorch_direct)
    native_http_rows = load_jsonl(args.native_http)
    pytorch_http_rows = load_jsonl(args.pytorch_http)
    native_direct = cells(native_direct_rows, "nomic_direct")
    pytorch_direct = cells(pytorch_direct_rows, "nomic_direct_reference")
    native_http = cells(native_http_rows, "nomic_http_endpoint")
    pytorch_http = cells(pytorch_http_rows, "nomic_http_endpoint")

    failures: list[str] = []
    direct_ratios: dict[str, float] = {}
    http_ratios: dict[str, float] = {}
    for key in sorted(EXPECTED_CELLS):
        native = native_direct[key]
        reference = pytorch_direct[key]
        if native.get("warmups") != 3 or native.get("repeats") != 10:
            failures.append(f"direct {key}: benchmark contract requires 3 warmups and 10 repeats")
        if native.get("model_sha") != reference.get("model_sha"):
            failures.append(f"direct {key}: model SHA differs")
        direct_ratio = ratio(native, reference)
        direct_ratios[f"b{key[0]}_s{key[1]}"] = direct_ratio
        if direct_ratio > 1.20:
            failures.append(f"direct {key}: ratio {direct_ratio:.4f} exceeds 1.20")
        layers = native.get("nomic_layers")
        if not isinstance(layers, dict) or layers.get("fallbacks") != 0 or int(layers.get("successes", 0)) < 12:
            failures.append(f"direct {key}: Nomic layer executor did not complete without fallback")
        expected_rope_pairs = 0
        if int(layers.get("rope_pairs", 0)) != expected_rope_pairs:
            failures.append(f"direct {key}: expected {expected_rope_pairs} paired-RoPE layers")
        # The fused FFN remains an opt-in experiment because its clean A/B did
        # not meet the 5% anchor / 2% no-regression retention contract.
        expected_ffn_fused = 0
        if int(layers.get("ffn_fused", 0)) != expected_ffn_fused:
            failures.append(f"direct {key}: expected {expected_ffn_fused} fused-FFN layers")
        if int(layers.get("pool_normalize_successes", 0)) < 1 or int(layers.get("pool_normalize_failures", 0)) != 0:
            failures.append(f"direct {key}: fused pooling/normalization was not active")
        expected_q8 = 12 if key[1] == 128 or key == (4, 16) else 0
        if int(layers.get("sdpa_q8", 0)) != expected_q8:
            failures.append(f"direct {key}: expected {expected_q8} Nomic tiled-SDPA layers")
        residency = native.get("residency")
        expected_output_materializations = int(native.get("warmups", 0)) + int(native.get("repeats", 0))
        if not isinstance(residency, dict) or int(residency.get("to_host_device_calls", -1)) != expected_output_materializations:
            failures.append(f"direct {key}: unexpected device-to-host materialization inside the measured pipeline")
        swap = native.get("swap_bytes")
        if isinstance(swap, dict) and swap.get("available") and int(swap.get("after", 0)) > int(swap.get("before", 0)):
            failures.append(f"direct {key}: swap grew during the measured cell")
        native_endpoint = native_http[key]
        pytorch_endpoint = pytorch_http[key]
        if native_endpoint.get("model_sha") != pytorch_endpoint.get("model_sha"):
            failures.append(f"HTTP {key}: model SHA differs")
        http_ratio = ratio(native_endpoint, pytorch_endpoint)
        http_ratios[f"b{key[0]}_s{key[1]}"] = http_ratio
        if http_ratio > 1.40:
            failures.append(f"HTTP {key}: ratio {http_ratio:.4f} exceeds 1.40")

    parity: dict[str, dict[str, float]] = {}
    native_embeddings = embedding_cells(native_direct_rows, "nomic_direct_embeddings")
    pytorch_embeddings = embedding_cells(pytorch_direct_rows, "nomic_direct_reference_embeddings")
    if args.parity:
        compact_parity = cells(load_jsonl(args.parity), "nomic_direct_parity")
        for key, row in compact_parity.items():
            result = {
                "max_abs_error": float(row["max_abs_error"]),
                "mean_abs_error": float(row["mean_abs_error"]),
                "min_cosine": float(row["min_cosine"]),
            }
            parity[f"b{key[0]}_s{key[1]}"] = result
            if result["max_abs_error"] > 2e-6 or result["mean_abs_error"] > 3e-7 or result["min_cosine"] < 0.9999999:
                failures.append(f"parity {key}: {result}")
    elif native_embeddings or pytorch_embeddings:
        if set(native_embeddings) != EXPECTED_CELLS or set(pytorch_embeddings) != EXPECTED_CELLS:
            failures.append("parity: expected embedded vectors for all six direct cells")
        else:
            for key in sorted(EXPECTED_CELLS):
                result = compare_embeddings(native_embeddings[key], pytorch_embeddings[key])
                parity[f"b{key[0]}_s{key[1]}"] = result
                if result["max_abs_error"] > 2e-6 or result["mean_abs_error"] > 3e-7 or result["min_cosine"] < 0.9999999:
                    failures.append(f"parity {key}: {result}")
    else:
        failures.append("parity: no embedded vector evidence was supplied")

    optimization: dict[str, dict[str, float]] = {}
    # Shape-gated kernels use their smallest eligible benchmark cell as the
    # 5% retention anchor; every other affected cell still has the 2% no-
    # regression requirement. The tiled SDPA deliberately excludes b1/s16
    # and b2/s16 because neither cleared the clean retention threshold.
    optimization_contracts = (
        ("pool_normalize", args.pool_normalize_disabled, EXPECTED_CELLS, {(1, 16)}),
        (
            "q8_sdpa",
            args.q8_sdpa_disabled,
            {(4, 16), (1, 128), (2, 128), (4, 128)},
            {(4, 16)},
        ),
    )
    for name, path, affected, improvement_cells in optimization_contracts:
        if path is None:
            failures.append(f"{name}: disabled-baseline evidence was not supplied")
            continue
        disabled = cells(load_jsonl(path), "nomic_direct", affected)
        optimization[name] = validate_optimization(
            name,
            native_direct,
            disabled,
            affected,
            improvement_cells,
            failures,
        )

    summary = {
        "kind": "nomic_metal_competitiveness_gate",
        "direct_gate": 1.20,
        "http_gate": 1.40,
        "direct_ratios": direct_ratios,
        "http_ratios": http_ratios,
        "parity": parity,
        "optimization_ratios": optimization,
        "passed": not failures,
        "failures": failures,
    }
    encoded = json.dumps(summary, sort_keys=True, indent=2)
    if args.out:
        Path(args.out).write_text(encoded + "\n")
    print(encoded)
    return 0 if not failures else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"nomic competitiveness evidence error: {exc}", file=sys.stderr)
        raise SystemExit(2)
