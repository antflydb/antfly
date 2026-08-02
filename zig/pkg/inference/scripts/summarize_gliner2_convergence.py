#!/usr/bin/env python3
"""Build the evidence-bound five-seed Python/Zig GLiNER2 convergence gate.

The input is a small manifest of report paths, never a bag of caller-supplied
metrics.  For each seed this tool loads the comparison harness report plus the
Python-oracle held-out evaluation of both independently trained adapters.  It
then derives optimizer-step counts, loss curves, metrics, artifact digests,
and oracle provenance from those generated reports.  The output retains all
source paths and SHA-256 digests so the production finalizer can re-run this
derivation and reject stale or edited evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any

from compare_gliner2_lora_python_zig import COMPARISON_CONTRACT
from evaluate_gliner2_full_task import EVALUATION_CONTRACT, REQUIRED_MINIMA
from gliner2_release_contract import (
    CANONICAL_NORMALIZATION,
    CANONICAL_UNICODE_VERSION,
    UPSTREAM_COMMIT,
    path_fingerprint,
    verify_canonical_python_runtime,
    verify_upstream_checkout,
)
from validate_gliner2_release_data import (
    adapter_bundle_fingerprint,
    base_model_fingerprint,
    sha256_file,
)


INPUT_CONTRACT = "gliner2_python_zig_convergence_evidence_manifest/v2"
DERIVED_CONTRACT = "gliner2_python_zig_convergence_derived/v2"
OUTPUT_CONTRACT = "gliner2_python_zig_convergence_summary/v2"
EVIDENCE_CONTRACT = "gliner2_python_zig_convergence_evidence/v1"
SIDES = ("python", "zig")


def is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == 71
        and all(char in "0123456789abcdef" for char in value[7:])
    )


def json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is missing or invalid: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return value


def evidence_path(raw: Any, base_dir: Path, label: str) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{label} must be a non-empty report path")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    path = path.resolve()
    if not path.is_file():
        raise ValueError(f"{label} is not a file: {path}")
    return path


def oracle_subset_matches(value: Any, verified: dict[str, str]) -> bool:
    return bool(
        isinstance(value, dict)
        and value.get("commit") == UPSTREAM_COMMIT
        and value.get("checkout") == verified.get("checkout")
    )


def oracle_runtime_matches(value: Any, verified: dict[str, str]) -> bool:
    if not oracle_subset_matches(value, verified) or not isinstance(value, dict):
        return False
    imported_module = value.get("imported_module")
    checkout = verified.get("checkout")
    return bool(
        isinstance(imported_module, str)
        and isinstance(checkout, str)
        and Path(imported_module).is_relative_to(Path(checkout))
        and str(value.get("python_version", "")).startswith("3.12.")
        and value.get("unicode_version") == CANONICAL_UNICODE_VERSION
    )


def metric(metrics: dict[str, Any], key: str) -> float | None:
    section, name = key.split(".", 1)
    raw = metrics.get(section, {}).get(name)
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        return None
    value = float(raw)
    return value if math.isfinite(value) and 0.0 <= value <= 1.0 else None


def valid_losses(value: Any) -> tuple[list[float] | None, str | None]:
    if not isinstance(value, list) or not value:
        return None, "loss curve is missing or empty"
    losses: list[float] = []
    for raw in value:
        if not isinstance(raw, (int, float)) or isinstance(raw, bool) or not math.isfinite(float(raw)):
            return None, "loss curve contains a nonfinite or nonnumeric value"
        losses.append(float(raw))
    if losses[-1] > max(losses[0] * 2.0, losses[0] + 1.0):
        return None, f"loss curve diverged from {losses[0]} to {losses[-1]}"
    return losses, None


def parsed_minima(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict) or set(raw) != set(REQUIRED_MINIMA):
        raise ValueError("minimums must contain exactly the nine release metrics")
    result: dict[str, float] = {}
    for key, value in raw.items():
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or not 0.0 < float(value) <= 1.0
        ):
            raise ValueError(f"invalid minimum for {key}: {value!r}")
        result[key] = float(value)
    return result


def side_training_evidence(
    comparison: dict[str, Any],
    side: str,
    requested_steps: int,
    verified_oracle: dict[str, str],
) -> tuple[int, list[float], dict[str, Any] | None]:
    result = comparison.get(side)
    if not isinstance(result, dict) or result.get("returncode") != 0:
        raise ValueError(f"{side} trainer did not complete successfully")
    if side == "python":
        generated = result.get("metrics")
        if not isinstance(generated, dict):
            raise ValueError("Python comparison_metrics.json evidence is missing")
        steps = generated.get("total_steps")
        rows = generated.get("train_metrics_history")
        if not oracle_runtime_matches(generated.get("oracle"), verified_oracle):
            raise ValueError("Python trainer oracle metadata is missing, unpinned, or imported outside its checkout")
        side_oracle = generated["oracle"]
    else:
        manifest = result.get("training_manifest")
        rows = result.get("training_metrics")
        if not isinstance(manifest, dict):
            raise ValueError("Zig training manifest evidence is missing")
        if manifest.get("backend") != "metal" or manifest.get("objective") != "gliner2-total-loss":
            raise ValueError("Zig trainer evidence is not a Metal full-task run")
        if manifest.get("lora_only_trainables") is not True:
            raise ValueError("Zig trainer evidence is not LoRA-only")
        steps = manifest.get("optimizer_steps")
        side_oracle = None
    if not isinstance(steps, int) or isinstance(steps, bool) or steps != requested_steps or steps <= 0:
        raise ValueError(f"{side} optimizer_steps must equal requested comparison steps ({requested_steps})")
    if not isinstance(rows, list):
        raise ValueError(f"{side} chronological training metrics are missing")
    if side == "python":
        loss_rows = rows
    else:
        loss_rows = [row for row in rows if isinstance(row, dict) and row.get("event") == "step"]
    losses = [row.get("loss") for row in loss_rows if isinstance(row, dict)]
    if len(losses) != steps:
        raise ValueError(f"{side} loss count {len(losses)} does not match optimizer_steps {steps}")
    return steps, losses, side_oracle


def evaluation_evidence(
    report: dict[str, Any],
    *,
    label: str,
    model_dir: Path,
    adapter_dir: Path,
    eval_data: Path,
    fingerprints: dict[str, str],
    adapter_fingerprint: str,
    minimums: dict[str, float],
    verified_oracle: dict[str, str],
) -> dict[str, Any]:
    artifacts = report.get("artifacts")
    inference = report.get("inference")
    if report.get("contract") != EVALUATION_CONTRACT or report.get("pass") is not True:
        raise ValueError(f"{label} is not a passing {EVALUATION_CONTRACT} report")
    if report.get("minimums") != minimums:
        raise ValueError(f"{label} was not evaluated with the convergence minimums")
    if (
        report.get("model_dir") != str(model_dir)
        or report.get("adapter_dir") != str(adapter_dir)
        or report.get("eval_data") != str(eval_data)
    ):
        raise ValueError(f"{label} paths do not match its seed artifacts")
    if (
        not isinstance(inference, dict)
        or inference.get("normalization") != CANONICAL_NORMALIZATION
        or inference.get("unicode_version") != CANONICAL_UNICODE_VERSION
    ):
        raise ValueError(f"{label} does not use the canonical scoring runtime")
    if not oracle_runtime_matches(report.get("oracle"), verified_oracle):
        raise ValueError(f"{label} oracle metadata is missing, unpinned, or imported outside its checkout")
    if not isinstance(artifacts, dict) or artifacts != {
        "base_model_fingerprint_sha256": fingerprints["base_model"],
        "adapter_bundle_fingerprint_sha256": adapter_fingerprint,
        "eval_data_fingerprint_sha256": fingerprints["eval_data"],
    }:
        raise ValueError(f"{label} artifact fingerprints do not match the selected evidence")
    metrics = report.get("metrics")
    if not isinstance(metrics, dict) or any(metric(metrics, key) is None for key in REQUIRED_MINIMA):
        raise ValueError(f"{label} is missing one or more canonical release metrics")
    return metrics


def materialize_study(
    manifest: dict[str, Any],
    *,
    manifest_dir: Path,
    model_dir: Path,
    train_data: Path,
    eval_data: Path,
    fingerprints: dict[str, str],
    verified_oracle: dict[str, str],
) -> dict[str, Any]:
    manifest_dir = manifest_dir.expanduser().resolve()
    model_dir = model_dir.expanduser().resolve()
    train_data = train_data.expanduser().resolve()
    eval_data = eval_data.expanduser().resolve()
    if manifest.get("contract") != INPUT_CONTRACT:
        raise ValueError(f"input contract must be {INPUT_CONTRACT}")
    if set(manifest) != {"contract", "minimums", "runs"}:
        raise ValueError("evidence manifest may contain only contract, minimums, and runs")
    minimums = parsed_minima(manifest.get("minimums"))
    runs = manifest.get("runs")
    if not isinstance(runs, list) or len(runs) != 5:
        raise ValueError("evidence manifest requires exactly five paired runs")
    derived_runs: list[dict[str, Any]] = []
    seen_seeds: set[int | str] = set()
    seen_reports: set[Path] = set()
    for index, run in enumerate(runs):
        label = f"run {index + 1}"
        if not isinstance(run, dict) or set(run) != {
            "seed", "comparison_report", "python_evaluation_report", "zig_evaluation_report"
        }:
            raise ValueError(
                f"{label} must contain only seed and the comparison/Python-evaluation/Zig-evaluation report paths"
            )
        seed = run.get("seed")
        if not isinstance(seed, (int, str)) or isinstance(seed, bool) or seed in seen_seeds:
            raise ValueError(f"{label} seed must be a unique integer or string")
        seen_seeds.add(seed)
        report_paths = {
            "comparison": evidence_path(run.get("comparison_report"), manifest_dir, f"{label} comparison_report"),
            "python_evaluation": evidence_path(
                run.get("python_evaluation_report"), manifest_dir, f"{label} python_evaluation_report"
            ),
            "zig_evaluation": evidence_path(
                run.get("zig_evaluation_report"), manifest_dir, f"{label} zig_evaluation_report"
            ),
        }
        if any(path in seen_reports for path in report_paths.values()):
            raise ValueError(f"{label} reuses an evidence report from another seed")
        seen_reports.update(report_paths.values())
        comparison = json_object(report_paths["comparison"], f"{label} comparison report")
        config = comparison.get("config")
        summary = comparison.get("summary")
        if comparison.get("contract") != COMPARISON_CONTRACT or not isinstance(config, dict):
            raise ValueError(f"{label} comparison report is not a {COMPARISON_CONTRACT} report")
        if (
            config.get("seed") != seed
            or config.get("model_dir") != str(model_dir)
            or config.get("train_data") != str(train_data)
            or config.get("model_fingerprint_sha256") != fingerprints["base_model"]
            or config.get("training_data_fingerprint_sha256") != fingerprints["train_data"]
            or config.get("scoring_normalization") != CANONICAL_NORMALIZATION
            or config.get("zig_backend") != "metal"
            or config.get("zig_objective") != "gliner2-total-loss"
            or config.get("zig_lora_only_trainables") is not True
            or config.get("deterministic") is not False
            or not oracle_subset_matches(config.get("oracle"), verified_oracle)
        ):
            raise ValueError(f"{label} comparison configuration is not the selected stochastic Metal study")
        requested_steps = config.get("steps")
        if (
            not isinstance(requested_steps, int)
            or isinstance(requested_steps, bool)
            or requested_steps <= 0
            or not isinstance(summary, dict)
            or summary.get("requested_step_count") != requested_steps
            or summary.get("step_count_valid") is not True
        ):
            raise ValueError(f"{label} comparison report has invalid requested/completed step evidence")
        adapter_dirs = {
            "python": (report_paths["comparison"].parent / "python" / "final").resolve(),
            "zig": (report_paths["comparison"].parent / "zig").resolve(),
        }
        adapter_fingerprints: dict[str, str] = {}
        for side in SIDES:
            try:
                adapter_fingerprints[side] = adapter_bundle_fingerprint(adapter_dirs[side])
            except (OSError, ValueError) as exc:
                raise ValueError(f"{label} {side} adapter bundle is missing or invalid: {exc}") from exc
        sides: dict[str, dict[str, Any]] = {}
        for side in SIDES:
            steps, losses, side_oracle = side_training_evidence(
                comparison, side, requested_steps, verified_oracle
            )
            eval_key = f"{side}_evaluation"
            evaluation = json_object(report_paths[eval_key], f"{label} {side} evaluation")
            metrics = evaluation_evidence(
                evaluation,
                label=f"{label} {side} evaluation",
                model_dir=model_dir,
                adapter_dir=adapter_dirs[side],
                eval_data=eval_data,
                fingerprints=fingerprints,
                adapter_fingerprint=adapter_fingerprints[side],
                minimums=minimums,
                verified_oracle=verified_oracle,
            )
            sides[side] = {
                "optimizer_steps": steps,
                "losses": losses,
                "metrics": metrics,
                "adapter": {
                    "path": str(adapter_dirs[side]),
                    "fingerprint": adapter_fingerprints[side],
                },
                "evaluation_report": {
                    "path": str(report_paths[eval_key]),
                    "sha256": sha256_file(report_paths[eval_key]),
                },
                **({"oracle": side_oracle} if side == "python" else {}),
            }
        derived_runs.append({
            "seed": seed,
            "comparison_report": {
                "path": str(report_paths["comparison"]),
                "sha256": sha256_file(report_paths["comparison"]),
            },
            **sides,
        })
    return {
        "contract": DERIVED_CONTRACT,
        "evidence_contract": EVIDENCE_CONTRACT,
        "oracle": verified_oracle,
        "normalization": CANONICAL_NORMALIZATION,
        "unicode_version": CANONICAL_UNICODE_VERSION,
        "fingerprints": fingerprints,
        "artifact_paths": {
            "base_model": str(model_dir),
            "train_data": str(train_data),
            "eval_data": str(eval_data),
        },
        "minimums": minimums,
        "runs": derived_runs,
    }


def summarize(payload: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    if payload.get("contract") != DERIVED_CONTRACT or payload.get("evidence_contract") != EVIDENCE_CONTRACT:
        failures.append("convergence data was not derived from the versioned evidence manifest")
    upstream = payload.get("oracle")
    if not isinstance(upstream, dict) or upstream.get("commit") != UPSTREAM_COMMIT:
        failures.append(f"oracle commit must be {UPSTREAM_COMMIT}")
        upstream = {"commit": upstream.get("commit") if isinstance(upstream, dict) else None}
    if payload.get("normalization") != CANONICAL_NORMALIZATION:
        failures.append(f"normalization must be {CANONICAL_NORMALIZATION}")
    if payload.get("unicode_version") != CANONICAL_UNICODE_VERSION:
        failures.append(f"unicode_version must be {CANONICAL_UNICODE_VERSION}")
    fingerprints = payload.get("fingerprints")
    if not isinstance(fingerprints, dict):
        fingerprints = {}
    for name in ("base_model", "train_data", "eval_data"):
        if not is_sha256(fingerprints.get(name)):
            failures.append(f"fingerprints.{name} must be a sha256 fingerprint")
    try:
        minimums = parsed_minima(payload.get("minimums"))
    except ValueError as exc:
        failures.append(str(exc))
        minimums = {}
    runs = payload.get("runs")
    if not isinstance(runs, list):
        failures.append("runs must be an array")
        runs = []
    if len(runs) != 5:
        failures.append(f"exactly five paired seeds are required, got {len(runs)}")

    seen_seeds: set[Any] = set()
    run_results: list[dict[str, Any]] = []
    by_metric: dict[str, list[tuple[float, float]]] = {key: [] for key in REQUIRED_MINIMA}
    for index, raw_run in enumerate(runs):
        run_failures: list[str] = []
        if not isinstance(raw_run, dict):
            failures.append(f"run {index + 1} must be an object")
            continue
        seed = raw_run.get("seed")
        if not isinstance(seed, (int, str)) or isinstance(seed, bool) or seed in seen_seeds:
            run_failures.append("seed must be a unique integer or string")
        else:
            seen_seeds.add(seed)
        sides: dict[str, dict[str, Any]] = {}
        for side_name in SIDES:
            side = raw_run.get(side_name)
            if not isinstance(side, dict):
                run_failures.append(f"{side_name} result is missing")
                continue
            metrics = side.get("metrics")
            if not isinstance(metrics, dict):
                run_failures.append(f"{side_name} metrics are missing")
                continue
            steps = side.get("optimizer_steps")
            if not isinstance(steps, int) or isinstance(steps, bool) or steps <= 0:
                run_failures.append(f"{side_name} optimizer_steps must be positive")
            losses, loss_error = valid_losses(side.get("losses"))
            if loss_error:
                run_failures.append(f"{side_name} {loss_error}")
            adapter = side.get("adapter")
            evaluation_report = side.get("evaluation_report")
            if (
                not isinstance(adapter, dict)
                or not is_sha256(adapter.get("fingerprint"))
                or not isinstance(adapter.get("path"), str)
                or not isinstance(evaluation_report, dict)
                or not is_sha256(evaluation_report.get("sha256"))
                or not isinstance(evaluation_report.get("path"), str)
            ):
                run_failures.append(f"{side_name} adapter/evaluation evidence is missing")
            side_oracle: dict[str, Any] | None = None
            if side_name == "python":
                raw_oracle = side.get("oracle")
                if not oracle_runtime_matches(raw_oracle, upstream):
                    run_failures.append("python oracle metadata is missing, unpinned, or imported outside its checkout")
                else:
                    side_oracle = raw_oracle
            parsed_metrics: dict[str, float] = {}
            for key in REQUIRED_MINIMA:
                value = metric(metrics, key)
                if value is None:
                    run_failures.append(f"{side_name} metric {key} is missing or invalid")
                else:
                    parsed_metrics[key] = value
            sides[side_name] = {
                "optimizer_steps": steps,
                "losses": losses,
                "metrics": parsed_metrics,
                "adapter": adapter,
                "evaluation_report": evaluation_report,
                **({"oracle": side_oracle} if side_name == "python" else {}),
            }
        if {"python", "zig"} <= sides.keys():
            if sides["python"]["optimizer_steps"] != sides["zig"]["optimizer_steps"]:
                run_failures.append(
                    "optimizer-step mismatch: "
                    f"python={sides['python']['optimizer_steps']} zig={sides['zig']['optimizer_steps']}"
                )
            if len(sides["python"]["metrics"]) == len(REQUIRED_MINIMA) and len(sides["zig"]["metrics"]) == len(REQUIRED_MINIMA):
                for key in REQUIRED_MINIMA:
                    python_value = sides["python"]["metrics"][key]
                    zig_value = sides["zig"]["metrics"][key]
                    by_metric[key].append((python_value, zig_value))
                    if key in minimums and zig_value < minimums[key]:
                        run_failures.append(f"Zig {key}={zig_value} below floor {minimums[key]}")
                    if python_value - zig_value > 0.05:
                        run_failures.append(f"paired {key} deficit {python_value - zig_value:.6f} exceeds 0.05")
        if run_failures:
            failures.extend(f"seed {seed!r}: {failure}" for failure in run_failures)
        comparison_report = raw_run.get("comparison_report")
        if (
            not isinstance(comparison_report, dict)
            or not is_sha256(comparison_report.get("sha256"))
            or not isinstance(comparison_report.get("path"), str)
        ):
            failures.append(f"seed {seed!r}: comparison-report evidence is missing")
        run_results.append({
            "seed": seed,
            "pass": not run_failures,
            "failures": run_failures,
            "comparison_report": comparison_report,
            **sides,
        })

    metric_results: dict[str, Any] = {}
    for key in sorted(REQUIRED_MINIMA):
        pairs = by_metric[key]
        python_mean = statistics.fmean(pair[0] for pair in pairs) if pairs else None
        zig_mean = statistics.fmean(pair[1] for pair in pairs) if pairs else None
        mean_deficit = python_mean - zig_mean if python_mean is not None and zig_mean is not None else None
        max_pair_deficit = max((python - zig for python, zig in pairs), default=None)
        passes = bool(
            len(pairs) == 5
            and mean_deficit is not None
            and mean_deficit <= 0.02
            and max_pair_deficit is not None
            and max_pair_deficit <= 0.05
        )
        if len(pairs) == 5 and mean_deficit is not None and mean_deficit > 0.02:
            failures.append(f"mean {key} deficit {mean_deficit:.6f} exceeds 0.02")
        metric_results[key] = {
            "python_mean": python_mean,
            "zig_mean": zig_mean,
            "mean_deficit": mean_deficit,
            "max_pair_deficit": max_pair_deficit,
            "pass": passes,
        }

    return {
        "contract": OUTPUT_CONTRACT,
        "evidence_contract": EVIDENCE_CONTRACT,
        "evidence_bound": payload.get("contract") == DERIVED_CONTRACT,
        "pass": not failures,
        "failures": failures,
        "oracle": upstream,
        "normalization": CANONICAL_NORMALIZATION,
        "unicode_version": CANONICAL_UNICODE_VERSION,
        "fingerprints": fingerprints,
        "artifact_paths": payload.get("artifact_paths"),
        "seed_count": len(runs),
        "minimums": minimums,
        "thresholds": {"max_mean_deficit": 0.02, "max_paired_deficit": 0.05},
        "loss_curve_policy": "all values finite; final loss no greater than max(2x initial, initial+1)",
        "stochastic_policy": "paired independent seeds; exact Python RNG-stream equality is not required",
        "runs": run_results,
        "metrics": metric_results,
    }


PROJECTION_KEYS = (
    "contract", "evidence_contract", "evidence_bound", "pass", "failures", "oracle",
    "normalization", "unicode_version", "fingerprints", "artifact_paths", "seed_count",
    "minimums", "thresholds", "loss_curve_policy", "stochastic_policy", "runs", "metrics",
)


def summary_projection(summary: dict[str, Any]) -> dict[str, Any]:
    return {key: summary.get(key) for key in PROJECTION_KEYS}


def verify_summary_evidence(summary: dict[str, Any]) -> list[str]:
    """Re-derive a convergence summary from its retained evidence paths."""
    try:
        source = summary.get("source")
        manifest_ref = source.get("manifest") if isinstance(source, dict) else None
        artifacts = source.get("artifacts") if isinstance(source, dict) else None
        upstream_source = source.get("upstream_source") if isinstance(source, dict) else None
        if not isinstance(manifest_ref, dict) or not isinstance(artifacts, dict) or not isinstance(upstream_source, str):
            raise ValueError("convergence summary has no re-verifiable source evidence")
        manifest_path = Path(str(manifest_ref.get("path"))).resolve()
        if sha256_file(manifest_path) != manifest_ref.get("sha256"):
            raise ValueError("convergence evidence manifest fingerprint changed")
        paths: dict[str, Path] = {}
        fingerprints: dict[str, str] = {}
        for name in ("base_model", "train_data", "eval_data"):
            ref = artifacts.get(name)
            if not isinstance(ref, dict) or not isinstance(ref.get("path"), str) or not is_sha256(ref.get("fingerprint")):
                raise ValueError(f"convergence source artifact {name} is missing")
            paths[name] = Path(ref["path"]).resolve()
        fingerprints["base_model"] = base_model_fingerprint(paths["base_model"])
        fingerprints["train_data"] = path_fingerprint(paths["train_data"])
        fingerprints["eval_data"] = path_fingerprint(paths["eval_data"])
        if any(fingerprints[name] != artifacts[name].get("fingerprint") for name in fingerprints):
            raise ValueError("convergence source artifact fingerprint changed")
        verify_canonical_python_runtime()
        oracle = verify_upstream_checkout(Path(upstream_source).resolve())
        manifest = json_object(manifest_path, "convergence evidence manifest")
        derived = materialize_study(
            manifest,
            manifest_dir=manifest_path.parent,
            model_dir=paths["base_model"],
            train_data=paths["train_data"],
            eval_data=paths["eval_data"],
            fingerprints=fingerprints,
            verified_oracle=oracle,
        )
        rebuilt = summarize(derived)
        if summary_projection(rebuilt) != summary_projection(summary):
            raise ValueError("convergence summary no longer matches its trainer/evaluator evidence")
        if rebuilt.get("pass") is not True:
            raise ValueError("re-derived convergence evidence does not pass")
    except (OSError, ValueError) as exc:
        return [str(exc)]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Evidence manifest containing only per-seed report paths")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--upstream-source", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--train-data", type=Path, required=True)
    parser.add_argument("--eval-data", type=Path, required=True)
    args = parser.parse_args()
    args.input = args.input.expanduser().resolve()
    args.output = args.output.expanduser().resolve()
    model_dir = args.model_dir.expanduser().resolve()
    train_data = args.train_data.expanduser().resolve()
    eval_data = args.eval_data.expanduser().resolve()
    upstream_source = args.upstream_source.expanduser().resolve()
    try:
        verify_canonical_python_runtime()
        manifest = json_object(args.input, "convergence evidence manifest")
        oracle = verify_upstream_checkout(upstream_source)
        fingerprints = {
            "base_model": base_model_fingerprint(model_dir),
            "train_data": path_fingerprint(train_data),
            "eval_data": path_fingerprint(eval_data),
        }
        derived = materialize_study(
            manifest,
            manifest_dir=args.input.parent,
            model_dir=model_dir,
            train_data=train_data,
            eval_data=eval_data,
            fingerprints=fingerprints,
            verified_oracle=oracle,
        )
        summary = summarize(derived)
        summary["source"] = {
            "manifest": {"path": str(args.input), "sha256": sha256_file(args.input)},
            "artifacts": {
                name: {"path": derived["artifact_paths"][name], "fingerprint": fingerprints[name]}
                for name in ("base_model", "train_data", "eval_data")
            },
            "upstream_source": str(upstream_source),
        }
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        summary = {
            "contract": OUTPUT_CONTRACT,
            "evidence_contract": EVIDENCE_CONTRACT,
            "evidence_bound": False,
            "pass": False,
            "failures": [str(exc)],
            "oracle": {"commit": None},
            "normalization": CANONICAL_NORMALIZATION,
            "unicode_version": CANONICAL_UNICODE_VERSION,
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(f"convergence summary: {args.output}")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
