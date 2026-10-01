#!/usr/bin/env python3
"""Supervised GLiNER2.5 CUDA training parity and paired throughput campaign."""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
from pathlib import Path
import sys

import numpy as np
import benchmark_cpu as common
import oracle
import training_cuda_quality as quality_checks
from metal_benchmark_supervisor import ResourceGuard, Worker
from training_cuda_worker import SCOPE, adapt_row

HERE = Path(__file__).resolve().parent
FIELDS = ("weight", "gradient", "m", "v")
# Declared before observations. Losses and all tensor elements are checked;
# near-zero cancellation uses an absolute floor, never a relaxed task decision.
TOLERANCES = {
    "weight": (2e-6, 2e-5),
    "gradient": (2e-4, 5e-3),
    "m": (2e-5, 5e-3),
    "v": (2e-7, 1e-2),
}


def response(worker, op, case, timeout):
    result = worker.request(op, case, timeout)
    if result["event"] == "error":
        raise common.BenchmarkError(
            f"{worker.arm}: {result.get('error_type')}: {result.get('message')}"
        )
    return result


def compare_snapshots(left, right):
    """Compare complete FP32 tensors in bounded chunks, including Adam state."""
    if (
        left["identity"] != right["identity"]
        or left["accumulated_microbatches"] != right["accumulated_microbatches"]
    ):
        raise common.BenchmarkError("training state counters differ")
    maps = [
        {slot["canonical_name"]: slot for slot in receipt["slots"]}
        for receipt in (left, right)
    ]
    if (
        any(
            len(mapping) != len(receipt["slots"])
            for mapping, receipt in zip(maps, (left, right))
        )
        or maps[0].keys() != maps[1].keys()
    ):
        raise common.BenchmarkError("trainable parameter sets differ")
    arrays = []
    for receipt in (left, right):
        path = Path(receipt["snapshot"])
        if (
            path.stat().st_size != receipt["size_bytes"]
            or receipt["size_bytes"] > 4 * 1024**3
        ):
            raise common.BenchmarkError("snapshot size exceeds contract")
        arrays.append(np.memmap(path, dtype="<f4", mode="r"))
    summary, failures = {}, []
    for name, ls in maps[0].items():
        rs = maps[1][name]
        for key in ("shape", "elements", "present", "adam_step", "group"):
            if ls[key] != rs[key]:
                failures.append(f"{name}: {key}: native={ls[key]} python={rs[key]}")
        n = ls["elements"]
        if n != math.prod(ls["shape"]):
            raise common.BenchmarkError("invalid snapshot tensor shape")
        metrics = {}
        for index, field in enumerate(FIELDS):
            max_error = max_reference = square_error = square_reference = 0.0
            worst = None
            bitwise_mismatches = 0
            for first in range(0, n, 262144):
                count = min(n - first, 262144)
                pieces, raw_pieces = [], []
                for arr, slot in zip(arrays, (ls, rs)):
                    start = slot["offset"] // 4 + index * n + first
                    raw = arr[start : start + count]
                    raw_pieces.append(raw)
                    piece = np.asarray(raw, dtype=np.float64)
                    if len(piece) != count or not np.isfinite(piece).all():
                        raise common.BenchmarkError(
                            "non-finite or truncated training tensor"
                        )
                    pieces.append(piece)
                bitwise_mismatches += int(
                    np.count_nonzero(
                        raw_pieces[0].view("<u4") != raw_pieces[1].view("<u4")
                    )
                )
                error = pieces[0] - pieces[1]
                local_index = int(np.argmax(np.abs(error)))
                local_error = float(abs(error[local_index]))
                if worst is None or local_error > max_error:
                    worst = {
                        "flat_index": first + local_index,
                        "native": float(pieces[0][local_index]),
                        "python": float(pieces[1][local_index]),
                    }
                max_error = max(max_error, local_error)
                max_reference = max(max_reference, float(np.max(np.abs(pieces[1]))))
                square_error += float(error @ error)
                square_reference += float(pieces[1] @ pieces[1])
            absolute, relative = TOLERANCES[field]
            allowed = absolute + relative * max_reference
            passed = max_error <= allowed and math.sqrt(
                square_error
            ) <= absolute * math.sqrt(n) + relative * math.sqrt(square_reference)
            metrics[field] = {
                "max_absolute_error": max_error,
                "reference_max_absolute": max_reference,
                "l2_error": math.sqrt(square_error),
                "reference_l2": math.sqrt(square_reference),
                "worst_element": worst,
                "passed": passed,
                "bitwise_equal": bitwise_mismatches == 0,
                "bitwise_mismatches": bitwise_mismatches,
            }
            if not passed:
                failures.append(
                    f"{name}.{field}: max_error={max_error:g}, allowed={allowed:g}"
                )
        if not metrics["weight"]["passed"]:
            coordinate = metrics["weight"]["worst_element"]["flat_index"]
            # Preserve the associated Adam state at the failing weight, not
            # only the independently worst coordinate of each state field.
            metrics["weight"]["state_at_worst"] = {
                arm: {
                    field: float(arr[slot["offset"] // 4 + index * n + coordinate])
                    for index, field in enumerate(FIELDS)
                }
                for arm, arr, slot in zip(("native", "python"), arrays, (ls, rs))
            }
        summary[name] = metrics
    del arrays
    return {
        "passed": not failures,
        "parameters": len(summary),
        "elements_per_state_field": sum(x["elements"] for x in maps[0].values()),
        "tolerances_absolute_relative": TOLERANCES,
        "failures": failures,
        "tensors": summary,
    }


def compare_steps(native, python):
    report = native["report"]
    if (
        report["zero_loss_fallback"]
        or report["terms"] is None
        or report["examples"] != python["examples"]
    ):
        raise common.BenchmarkError(
            "training step lost supervision or changed batch size"
        )
    counts = native["cuda_transfers"]
    memory = native.get("cuda_allocations")
    if (
        memory is not None
        and not 0 <= memory["live"] <= memory["peak"] <= memory["limit"]
    ):
        raise common.BenchmarkError("native physical CUDA allocation ceiling exceeded")
    if counts["kernel_launches"] <= 0 or counts["host_fallback_calls"] != 0:
        raise common.BenchmarkError("native training CUDA evidence invalid")
    optimizer = report["optimizer"]
    if optimizer["optimizer_stepped"] != python["optimizer_stepped"] or optimizer[
        "identity"
    ] != {
        "optimizer_step": python["optimizer_step"],
        "microbatch_step": python["microbatch_step"],
    }:
        raise common.BenchmarkError("optimizer update identity differs")
    terms = report["terms"]
    if terms.keys() != python["terms"].keys():
        raise common.BenchmarkError(
            "training loss components differ; an auxiliary loss may have been dropped"
        )
    differences = {}
    for name, expected in python["terms"].items():
        key = "total" if name == "total" else name
        if key not in terms:
            raise common.BenchmarkError(f"unmapped Python loss term: {name}")
        actual = terms[key]
        if not math.isfinite(actual) or not math.isfinite(expected):
            raise common.BenchmarkError("non-finite training loss")
        differences[name] = {
            "native": actual,
            "python": expected,
            "absolute_error": abs(actual - expected),
            "passed": abs(actual - expected) <= 2e-4 + 1e-3 * abs(expected),
        }
    return {
        "passed": all(x["passed"] for x in differences.values()),
        "terms": differences,
    }


def run(args):
    if (
        args.output.exists()
        or not args.native_bin.is_file()
        or args.batch_size not in (1, 2, 4, 8)
        or not 2 <= args.pairs <= 30
        or not 0 <= args.warmup <= 5
    ):
        raise common.BenchmarkError("invalid training benchmark paths or bounds")
    if not math.isfinite(args.adam_epsilon) or not 0 < args.adam_epsilon <= 1:
        raise common.BenchmarkError("Adam epsilon must be finite and in (0, 1]")
    if not 0 <= args.qualification_updates <= 512 or (
        args.validate_only and args.qualification_updates
    ):
        raise common.BenchmarkError("invalid qualification update count")
    encoder_forward_bytes = getattr(args, "encoder_forward_bytes", 4 * 1024**3)
    if (
        type(encoder_forward_bytes) is not int
        or not 0 < encoder_forward_bytes <= 1024 * 1024**3
    ):
        raise common.BenchmarkError("invalid logical encoder forward byte limit")
    cublas_library = getattr(args, "cublas_library", None)
    if cublas_library is None:
        configured = os.environ.get("ANTFLY_INFERENCE_CUDA_TRAINING_CUBLAS_LIBRARY")
        cublas_library = Path(configured) if configured is not None else None
    if cublas_library is not None and (
        not cublas_library.is_absolute() or not cublas_library.is_file()
    ):
        raise common.BenchmarkError(
            "training cuBLAS library must be an existing absolute file"
        )
    cublas_identity = (
        {"path": str(cublas_library), "sha256": oracle.sha256_file(cublas_library)}
        if cublas_library is not None
        else None
    )
    oracle.verify_dependencies()
    oracle.verify_reference_fixtures()
    provenance = oracle.verify_upstream_checkout(args.upstream)
    bundle = oracle.verify_model_dir(args.model, args.model_dir)
    fixture = oracle.FIXTURES / "training_job_small_v1"
    if (args.training_file is None) != (args.validation_file is None):
        raise common.BenchmarkError("provide both training and validation files")
    original = args.training_file or fixture / "train.jsonl"
    validation_source = args.validation_file or fixture / "validation.jsonl"
    if args.training_file is None:
        manifest = oracle.read_json(fixture / "manifest.json")
        for name, path in (("train", original), ("validation", validation_source)):
            if oracle.sha256_file(path) != manifest["files"][name]["sha256"]:
                raise common.BenchmarkError(f"{name} fixture hash differs")
    source_rows = quality_checks.read_rows(original)
    validation_rows = quality_checks.read_rows(validation_source)
    quality_checks.require_disjoint(source_rows, validation_rows)
    args.output.mkdir(parents=True)
    args.snapshot_root.mkdir(parents=True, exist_ok=True)
    validation_path = args.output / "validation.jsonl"
    validation_path.write_bytes(validation_source.read_bytes())
    rows = []
    for repeat in range(max(2, args.batch_size)):
        for source in source_rows:
            row = copy.deepcopy(source)
            row["id"] += f"/benchmark-copy-{repeat}"
            for record in row.get("records", []):
                record["id"] += f"/benchmark-copy-{repeat}"
            adapt_row(row)
            rows.append(row)
    dataset = args.output / "train.jsonl"
    dataset.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            for row in rows
        )
    )
    config = {
        "version": 1,
        "source_dir": str(args.model_dir),
        "train_file": str(dataset),
        "output_dir": str(args.output / "unused-job-output"),
        "execution": "resident_cuda",
        "run": {
            "mode": args.mode,
            "epochs": 2048,
            "batch_size": args.batch_size,
            "accumulation": 2,
            "encoder_lr": 1e-5,
            "task_lr": 5e-4,
            "weight_decay": 0.01,
            "beta1": 0.9,
            "beta2": 0.999,
            "epsilon": args.adam_epsilon,
            "max_grad_norm": 1,
            "scheduler": "constant",
            "warmup_steps": 0,
            "seed": 42,
            "shuffle": False,
        },
        "gold_start": 1,
        "gold_end": 1,
        # The multilingual tokenizer needs the source owner's normal
        # auxiliary allowance; the small/base fixture override is lower.
        "source_limits": {
            "max_auxiliary_bytes": (384 if args.model == "multi" else 128) * 1024**2
        },
        "memory": {"host_bytes": 5 * 1024**3},
        "training_limits": {
            "encoder": {"max_forward_tensor_bytes": encoder_forward_bytes},
            "differentiation": {"max_tape_bytes": 2 * 1024**3},
            "resident": {"program": {"max_working_bytes": 2 * 1024**3}},
        },
    }
    config_path = args.output / "config.json"
    oracle.write_json(config_path, config)
    env = dict(os.environ)
    env.update({name: "1" for name in common.THREAD_ENV})
    env.update(
        PYTHONDONTWRITEBYTECODE="1",
        TOKENIZERS_PARALLELISM="false",
        HF_HUB_OFFLINE="1",
        TRANSFORMERS_OFFLINE="1",
        TORCHINDUCTOR_COMPILE_THREADS="1",
    )
    env.pop("USE_FLASHDEBERTA", None)
    if cublas_library is not None:
        env["ANTFLY_INFERENCE_CUDA_TRAINING_CUBLAS_LIBRARY"] = str(cublas_library)
    snapshots = {
        arm: args.snapshot_root / f"{args.output.name}-{arm}.bin"
        for arm in ("native", "python")
    }
    if any(p.exists() for p in snapshots.values()):
        raise common.BenchmarkError("snapshot destination already exists")
    commands = {
        "native": [
            str(args.native_bin),
            "--config",
            str(config_path),
            "--snapshot",
            str(snapshots["native"]),
        ],
        "python": [
            sys.executable,
            str(HERE / "training_cuda_worker.py"),
            "--config",
            str(config_path),
            "--snapshot",
            str(snapshots["python"]),
            "--upstream",
            str(args.upstream),
            "--profile",
            args.python_profile,
            "--model",
            args.model,
        ],
    }
    evaluation_layout = args.output / "evaluation-layouts.json"
    if args.evaluate:
        commands["python"] += [
            "--evaluation-layout",
            str(evaluation_layout),
            "--validation-file",
            str(validation_path),
            "--validation-sha256",
            oracle.sha256_file(validation_path),
        ]
    guard = ResourceGuard(11 * 1024**3, max_processes=1024)
    workers, report = (
        {},
        {
            "scope": SCOPE,
            "status": "running",
            "performance_qualified": False,
            "profile": args.python_profile,
            "model": args.model,
            "mode": args.mode,
            "batch_size": args.batch_size,
            "accumulation": 2,
            "qualification_updates": args.qualification_updates,
            "heldout_scope": (
                "user-supplied validation"
                if args.training_file
                else "two existing synthetic validation examples"
            )
            + "; both weight sets evaluated in pinned Python CUDA"
            if args.evaluate
            else None,
            "validation_fixture_sha256": oracle.sha256_file(validation_path),
            "dataset_paths": {
                "training": str(original),
                "validation": str(validation_source),
            },
            "dataset_counts": {
                "training": len(source_rows),
                "validation": len(validation_rows),
            },
            "optimizer_config": {
                key: config["run"][key]
                for key in (
                    "encoder_lr",
                    "task_lr",
                    "weight_decay",
                    "beta1",
                    "beta2",
                    "epsilon",
                    "max_grad_norm",
                )
            },
            "native_resource_limits": {
                key: config[key]
                for key in ("memory", "source_limits", "training_limits")
            },
            "native_cublas_library": cublas_identity,
            "deterministic_overrides": {
                "dropout": 0,
                "schema_augmentation": False,
                "negative_query_sampling": False,
                "gold_injection_probability": 1,
                "shuffle": False,
            },
            "timing_boundary": "dataset_schema_processing+H2D+forward+loss_matching+backward+accumulation+clipping+AdamW+temporary_cleanup+synchronize",
            "excluded": [
                "loading",
                "initial_graph_compilation",
                "snapshots",
                "JSON",
                "checkpoint_IO",
            ],
            "source": provenance,
            "model_bundle": bundle,
            "fixture_sha256": oracle.sha256_file(original),
            "derived_dataset_sha256": oracle.sha256_file(dataset),
            "commands": commands,
            "native_binary_sha256": oracle.sha256_file(args.native_bin),
            "helpers_sha256": {
                name: oracle.sha256_file(HERE / name)
                for name in (
                    Path(__file__).name,
                    "training_cuda_worker.py",
                    "training_cuda_quality.py",
                    "training_cuda_trace.py",
                    "oracle.py",
                    "metal_python_worker.py",
                    "metal_benchmark_supervisor.py",
                    "benchmark_cpu.py",
                )
            },
            "diagnostic_throughput": args.diagnostic_throughput,
            "parity_failures": [],
            "readiness": {},
            "validation": [],
            "pairs": [],
            "cleanup": {},
        },
    )

    def save():
        oracle.write_json(args.output / "report.json", report)

    def parity_failure(message):
        report["parity_failures"].append(message)
        save()
        if not args.diagnostic_throughput:
            raise common.BenchmarkError(message)

    def pair_step(op, order=("native", "python")):
        results = {}
        for arm in order:
            results[arm] = response(workers[arm], op, "step", args.timeout)
            with (args.output / "steps.jsonl").open("a") as journal:
                journal.write(
                    json.dumps({"op": op, "response": results[arm]}, allow_nan=False)
                    + "\n"
                )
        comparison = compare_steps(results["native"], results["python"])
        return {"responses": results, "comparison": comparison}

    def inspect(label):
        receipts = {
            arm: response(workers[arm], "validate", "snapshot", args.timeout)
            for arm in workers
        }
        oracle.write_json(args.output / f"{label}-snapshot-layouts.json", receipts)
        comparison = compare_snapshots(receipts["native"], receipts["python"])
        oracle.write_json(args.output / f"{label}-tensor-parity.json", comparison)
        if args.evaluate and label in ("initial", "final", "microbatch-2"):
            oracle.write_json(evaluation_layout, receipts)
            quality = response(workers["python"], "validate", "heldout", args.timeout)
            if label == "initial":
                report["initial_quality"] = {
                    arm: quality[arm]["metrics"] for arm in ("native", "python")
                }
            quality["regressions_from_initial"] = [
                f"{arm}.{task}"
                for arm in ("native", "python")
                for task, metric in quality[arm]["metrics"].items()
                if metric["f1"] < report["initial_quality"][arm][task]["f1"]
            ]
            report.setdefault("heldout", {})[label] = {
                "metrics": {
                    arm: quality[arm]["metrics"] for arm in ("native", "python")
                },
                "regressions_from_initial": quality["regressions_from_initial"],
            }
            try:
                common.require_equal(
                    quality["python"]["outputs"], quality["native"]["outputs"]
                )
                quality["passed"] = True
            except common.BenchmarkError as exc:
                quality.update(passed=False, error=str(exc))
            oracle.write_json(args.output / f"{label}-heldout.json", quality)
            if not quality["passed"]:
                parity_failure(
                    f"{label} held-out extraction parity failed: {quality['error']}"
                )
        for path in snapshots.values():
            path.unlink()
        if not comparison["passed"]:
            parity_failure(
                f"{label} tensor parity failed: "
                + "; ".join(comparison["failures"][:5])
            )
        return {key: value for key, value in comparison.items() if key != "tensors"}

    try:
        for arm in ("python", "native"):
            workers[arm] = Worker(arm, commands[arm], env, args.output, guard)
            ready = workers[arm].receive(args.timeout)
            if (
                ready.get("scope") != SCOPE
                or ready.get("dtype") != "float32"
                or ready.get("dropout") != 0
                or ready.get("dataset_sha256") != report["derived_dataset_sha256"]
                or ready.get("config_sha256") != oracle.sha256_file(config_path)
            ):
                raise common.BenchmarkError("training readiness contract differs")
            if ready.get("negative_query_sampling") is not False:
                raise common.BenchmarkError("training query sampling is not disabled")
            if arm == "native":
                if cublas_identity is not None and (
                    type(ready.get("cublas_version")) is not int
                    or ready["cublas_version"] <= 0
                ):
                    raise common.BenchmarkError(
                        "native worker did not identify the configured cuBLAS runtime"
                    )
                expected_source = {
                    "backbone": args.model,
                    "precision": "fp32",
                    "weight": bundle["files"]["model.safetensors"],
                    "sidecars": [
                        bundle["files"][name]
                        for name in (
                            "config.json",
                            "encoder_config/config.json",
                            "tokenizer.json",
                            "tokenizer_config.json",
                        )
                    ],
                }
                if ready.get("source") != expected_source:
                    raise common.BenchmarkError(
                        "native source differs from the pinned Python bundle"
                    )
            elif ready.get("model_bundle") != bundle:
                raise common.BenchmarkError(
                    "Python source differs from the pinned bundle"
                )
            report["readiness"][arm] = ready
            save()
        report["initial_state_parity"] = inspect("initial")
        for micro in range(2):
            inputs = {
                arm: response(workers[arm], "validate", "inputs", args.timeout)
                for arm in workers
            }
            oracle.write_json(args.output / f"inputs-{micro}.json", inputs)
            if (
                inputs["native"]["input_ids"] != inputs["python"]["input_ids"]
                or inputs["native"]["encoder_shape"]
                != inputs["python"]["encoder_shape"]
            ):
                raise common.BenchmarkError("training encoder inputs differ")
            observation = pair_step("validate")
            report["validation"].append(observation)
            save()
            # Capture all arrays even when the scalar comparison fails, to
            # retain enough evidence to locate the mismatch.
            report[f"validation_{micro}_tensor_parity"] = inspect(
                f"microbatch-{micro + 1}"
            )
            if not observation["comparison"]["passed"]:
                parity_failure("training component loss parity failed")
        if not args.validate_only:
            for _ in range(args.warmup):
                for _ in range(2):
                    result = pair_step("run")
                    if not result["comparison"]["passed"]:
                        parity_failure("warmup loss parity failed")
            for number in range(1, args.pairs + 1):
                order = common.paired_benchmark.balanced_pair_order(
                    number, "native", "python"
                )
                samples = [pair_step("run", order) for _ in range(2)]
                row = {
                    "pair": number,
                    "order": order,
                    "microbatches": samples,
                    **{
                        arm + "_ns": sum(
                            x["responses"][arm]["duration_ns"] for x in samples
                        )
                        for arm in workers
                    },
                }
                report["pairs"].append(row)
                save()
                if any(not x["comparison"]["passed"] for x in samples):
                    parity_failure("measured training loss parity failed")
            for update in range(1, args.qualification_updates + 1):
                for _ in range(2):
                    result = pair_step("validate")
                    if not result["comparison"]["passed"]:
                        parity_failure(
                            f"qualification update {update} loss parity failed"
                        )
                if update % 10 == 0:
                    report[f"qualification_{update}_tensor_parity"] = inspect(
                        f"qualification-{update}"
                    )
                    save()
            report["final_state_parity"] = inspect("final")
            pairs = [(row["native_ns"], row["python_ns"]) for row in report["pairs"]]
            report["comparison"] = {
                "native_over_python_latency": common.paired_benchmark.paired_log_ratio_ci(
                    pairs, samples=2000
                ),
                **{
                    arm + "_examples_per_second": common.paired_benchmark.distribution(
                        2 * args.batch_size * 1e9 / row[arm + "_ns"]
                        for row in report["pairs"]
                    )
                    for arm in workers
                },
            }
        for w in workers.values():
            w.request("stop", timeout=args.timeout)
        if oracle.sha256_file(args.native_bin) != report["native_binary_sha256"] or any(
            oracle.sha256_file(HERE / name) != value
            for name, value in report["helpers_sha256"].items()
        ):
            raise common.BenchmarkError(
                "benchmark implementation changed during measurement"
            )
        if (
            cublas_identity is not None
            and oracle.sha256_file(cublas_library) != cublas_identity["sha256"]
        ):
            raise common.BenchmarkError(
                "configured training cuBLAS library changed during measurement"
            )
        passed = not report["parity_failures"]
        report.update(
            status="complete" if passed else "diagnostic_complete",
            parity_validated=passed,
        )
        if "comparison" in report:
            report["comparison"]["interpretation"] = (
                "parity_passed" if passed else "diagnostic_only_parity_failed"
            )
    except BaseException as exc:
        report.update(
            status="failed",
            parity_validated=False,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise
    finally:
        cleanup_errors = []
        for arm, w in workers.items():
            try:
                w.close()
            except BaseException as exc:
                cleanup_errors.append(f"{arm}: {type(exc).__name__}: {exc}")
            finally:
                report["cleanup"][arm] = w.cleanup
        for path in snapshots.values():
            path.unlink(missing_ok=True)
        report["peak_combined_worker_rss_bytes"] = guard.peak_rss_bytes
        if cleanup_errors:
            report.update(
                status="failed", parity_validated=False, cleanup_errors=cleanup_errors
            )
        save()
        common.paired_benchmark.write_evidence_manifest(args.output)
        if cleanup_errors:
            raise common.BenchmarkError("; ".join(cleanup_errors))
    print(
        json.dumps(
            {"status": report["status"], "report": str(args.output / "report.json")}
        )
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--native-bin", type=Path, required=True)
    p.add_argument("--model-dir", type=Path, required=True)
    p.add_argument("--model", choices=("small", "base", "multi"), default="small")
    p.add_argument("--upstream", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--snapshot-root", type=Path, required=True)
    p.add_argument("--mode", choices=("heads", "full"), default="heads")
    p.add_argument("--batch-size", type=int, choices=(1, 2, 4, 8), default=2)
    p.add_argument(
        "--encoder-forward-bytes",
        type=int,
        default=4 * 1024**3,
        help="logical sum of encoder intermediates (default 4 GiB); does not raise physical CUDA memory limits",
    )
    p.add_argument(
        "--cublas-library",
        type=Path,
        help="explicit absolute native training cuBLAS library; use the reference runtime for arithmetic parity",
    )
    p.add_argument(
        "--python-profile", choices=("eager_fp32", "compile_fp32"), default="eager_fp32"
    )
    p.add_argument(
        "--adam-epsilon",
        type=float,
        default=1e-8,
        help="matched AdamW epsilon for both arms; default preserves the original comparison",
    )
    p.add_argument("--pairs", type=int, default=10)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument(
        "--qualification-updates",
        type=int,
        default=0,
        help="additional untimed optimizer updates, 0–512, with complete state checks every ten",
    )
    p.add_argument(
        "--evaluate",
        action="store_true",
        help="compare held-out extraction and gold metrics using the existing validation fixture",
    )
    p.add_argument(
        "--training-file",
        type=Path,
        help="optional bounded annotated JSONL; requires a disjoint validation file",
    )
    p.add_argument("--validation-file", type=Path)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--validate-only", action="store_true")
    p.add_argument(
        "--diagnostic-throughput",
        action="store_true",
        help="record timings despite numerical mismatches; retain failed parity and never qualify performance",
    )
    args = p.parse_args()
    for name in ("native_bin", "model_dir", "upstream", "output", "snapshot_root"):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    for name in ("training_file", "validation_file"):
        if getattr(args, name) is not None:
            setattr(args, name, getattr(args, name).expanduser().resolve())
    run(args)


if __name__ == "__main__":
    main()
