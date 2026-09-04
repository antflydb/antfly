#!/usr/bin/env python3
"""Shared deterministic pairing, statistics, and evidence-manifest helpers.

The performance harnesses deliberately keep process launching and domain
validation local.  This module owns the parts that must not drift between
them: balanced AB/BA ordering, paired log-ratio confidence intervals, and
content-addressed evidence manifests.
"""

from __future__ import annotations

import hashlib
import json
import math
import pathlib
import random
import statistics
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, TypeVar


MANIFEST_SCHEMA = "antfly.benchmark_evidence_manifest.v1"
T = TypeVar("T")


def balanced_pair_order(pair_index: int, first: T, second: T) -> tuple[T, T]:
    """Return AB for odd one-based pair indices and BA for even indices."""
    if pair_index < 1:
        raise ValueError("pair index must be positive")
    return (first, second) if pair_index % 2 else (second, first)


def percentile(values: Iterable[float], probability: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("percentile requires at least one value")
    if not 0.0 <= probability <= 1.0:
        raise ValueError("percentile probability must be between zero and one")
    if len(ordered) == 1:
        return ordered[0]
    position = probability * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def distribution(values: Iterable[float]) -> dict[str, float]:
    samples = [float(value) for value in values]
    if not samples or any(not math.isfinite(value) for value in samples):
        raise ValueError("distribution requires finite samples")
    mean = statistics.fmean(samples)
    return {
        "min": min(samples),
        "median": statistics.median(samples),
        "mean": mean,
        "p95": percentile(samples, 0.95),
        "max": max(samples),
        "cv": statistics.pstdev(samples) / mean if len(samples) > 1 and mean else 0.0,
    }


def paired_log_ratio_ci(
    pairs: Sequence[tuple[float, float]],
    *,
    samples: int = 10_000,
    seed: int = 20260730,
) -> dict[str, float | int | str]:
    """Bootstrap the median numerator/denominator ratio in log space.

    Every tuple is one experimental pair.  Resampling whole tuples preserves
    pairing; log space makes reciprocal improvements/regressions symmetric.
    Ratios below one mean the numerator has lower latency than the denominator.
    """
    if not pairs:
        raise ValueError("paired log-ratio confidence interval requires pairs")
    if samples < 1:
        raise ValueError("bootstrap sample count must be positive")
    log_ratios: list[float] = []
    for numerator, denominator in pairs:
        numerator = float(numerator)
        denominator = float(denominator)
        if not math.isfinite(numerator) or not math.isfinite(denominator):
            raise ValueError("paired metrics must be finite")
        if numerator <= 0.0 or denominator <= 0.0:
            raise ValueError("paired metrics must be positive")
        log_ratios.append(math.log(numerator / denominator))

    source = random.Random(seed)
    bootstrapped: list[float] = []
    for _ in range(samples):
        selected = [log_ratios[source.randrange(len(log_ratios))] for _ in log_ratios]
        bootstrapped.append(math.exp(statistics.median(selected)))
    observed = math.exp(statistics.median(log_ratios))
    return {
        "lower_95": percentile(bootstrapped, 0.025),
        "median": observed,
        "upper_95": percentile(bootstrapped, 0.975),
        "samples": samples,
        "seed": seed,
        "estimator": "median_paired_log_ratio",
    }


def paired_metric_log_ratio_ci(
    rows: Sequence[Mapping[str, Any]],
    *,
    numerator_arm: str,
    denominator_arm: str,
    metric: str,
    samples: int = 10_000,
    seed: int = 20260730,
) -> dict[str, float | int | str]:
    return paired_log_ratio_ci(
        [
            (float(row[numerator_arm][metric]), float(row[denominator_arm][metric]))
            for row in rows
        ],
        samples=samples,
        seed=seed,
    )


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_evidence_manifest(
    root: pathlib.Path,
    *,
    exclude: Iterable[pathlib.Path | str] = (),
) -> dict[str, Any]:
    root = root.resolve()
    excluded = {pathlib.PurePosixPath(path).as_posix() for path in exclude}
    files: list[dict[str, Any]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if relative in excluded:
            continue
        files.append(
            {
                "path": relative,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    identity = {"schema": MANIFEST_SCHEMA, "files": files}
    return {
        **identity,
        "file_count": len(files),
        "files_sha256": canonical_sha256(identity),
    }


def write_evidence_manifest(
    root: pathlib.Path,
    output: pathlib.Path | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    output = (output or root / "evidence_manifest.json").resolve()
    try:
        relative_output = output.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(
            "evidence manifest must be written inside its evidence root"
        ) from exc
    manifest = build_evidence_manifest(root, exclude=(relative_output,))
    output.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return manifest
