#!/usr/bin/env python3
"""Compare live loss inputs and same-logit derivatives outside timed training."""

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

import numpy as np
import benchmark_cpu as common
import benchmark_training_cuda as training
import oracle
from metal_benchmark_supervisor import ResourceGuard, Worker

HERE = Path(__file__).resolve().parent


def metrics(left, right):
    a, b = np.asarray(left, dtype=np.float64), np.asarray(right, dtype=np.float64)
    if a.shape != b.shape or not np.isfinite(a).all() or not np.isfinite(b).all():
        raise ValueError(
            "diagnostic tensors differ in shape or contain non-finite values"
        )
    difference = a - b
    index = int(np.argmax(np.abs(difference)))
    return {
        "elements": a.size,
        "max_absolute_error": float(abs(difference[index])),
        "l2_error": float(np.linalg.norm(difference)),
        "reference_l2": float(np.linalg.norm(b)),
        "worst_index": index,
        "native": float(a[index]),
        "python": float(b[index]),
    }


def run(args):
    microbatches = getattr(args, "microbatches", 2)
    if not 1 <= microbatches <= 128:
        raise ValueError("trace microbatches must be between 1 and 128")
    config = oracle.read_json(args.config)
    if (
        args.modules
        and config.get("activation_profile", "retained_v1") != "retained_v1"
    ):
        raise ValueError("module tracing requires retained_v1 activations")
    if args.output.exists():
        raise ValueError("diagnostic output already exists")
    args.output.mkdir(parents=True)
    args.snapshot_root.mkdir(parents=True, exist_ok=True)
    scratch = Path(
        tempfile.mkdtemp(prefix="gliner25-loss-trace-", dir=args.snapshot_root)
    )
    native_inputs = args.output / "native-input.json"
    env = dict(os.environ)
    env.update({name: "1" for name in common.THREAD_ENV})
    env.update(
        PYTHONDONTWRITEBYTECODE="1",
        TOKENIZERS_PARALLELISM="false",
        HF_HUB_OFFLINE="1",
        TRANSFORMERS_OFFLINE="1",
    )
    env.pop("USE_FLASHDEBERTA", None)
    guard = ResourceGuard(11 * 1024**3, max_processes=1024)
    commands = {
        "native": [
            str(args.native_bin),
            "--config",
            str(args.config),
            "--snapshot",
            str(scratch / "native.bin"),
        ],
        "python": [
            sys.executable,
            str(HERE / "training_cuda_worker.py"),
            "--config",
            str(args.config),
            "--snapshot",
            str(scratch / "python.bin"),
            "--upstream",
            str(args.upstream),
            "--model",
            args.model,
            "--trace-native-input",
            str(native_inputs),
        ],
    }
    if getattr(args, "records", False):
        commands["native"] += ["--trace-records"]
    if args.modules:
        commands["native"] += ["--trace-tensors", str(scratch / "native-tensors.bin")]
    report = {
        "scope": "diagnostic_loss_boundary",
        "production_qualified": False,
        "runtime_environment": {
            key: env[key]
            for key in (
                "LD_LIBRARY_PATH",
                "CUBLAS_WORKSPACE_CONFIG",
                *common.THREAD_ENV,
            )
            if key in env
        },
        "config_sha256": oracle.sha256_file(args.config),
        "native_binary_sha256": oracle.sha256_file(args.native_bin),
        "helpers_sha256": {
            name: oracle.sha256_file(HERE / name)
            for name in (
                Path(__file__).name,
                "training_cuda_trace.py",
                "training_cuda_worker.py",
                "benchmark_training_cuda.py",
                "metal_benchmark_supervisor.py",
            )
        },
        "commands": commands,
        "observations": [],
        "cleanup": {},
    }
    workers = {}
    try:
        for arm in ("python", "native"):
            workers[arm] = Worker(arm, commands[arm], env, args.output, guard)
            report[arm + "_ready"] = workers[arm].receive(args.timeout)
            if report[arm + "_ready"].get("config_sha256") != report["config_sha256"]:
                raise ValueError("worker configuration differs")
        for microbatch in range(1, microbatches + 1):
            inputs = {
                arm: training.response(w, "validate", "inputs", args.timeout)
                for arm, w in workers.items()
            }
            for field in ("input_ids", "encoder_shape"):
                if inputs["native"][field] != inputs["python"][field]:
                    raise ValueError("encoder inputs differ: " + field)
            native = training.response(
                workers["native"], "validate", "trace_step", args.timeout
            )
            events = {
                event["kind"]: event
                for event in map(common.strict_json, native["trace_events"])
            }
            payload = dict(events["boundary_inputs"]["value"])
            if getattr(args, "records", False):
                # Multiple record groups share an event kind; preserve each.
                payload["_records"] = [
                    event
                    for event in map(common.strict_json, native["trace_events"])
                    if event["kind"] == "record_inputs"
                ]
            if args.modules:
                payload["_modules"] = native["module_trace"]
            if getattr(args, "records", False):
                # Dense record tensors fit the existing 2 MiB receiver budget
                # when encoded compactly; indentation is not part of the trace.
                encoded = (
                    json.dumps(
                        payload,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                    + "\n"
                ).encode("utf-8")
                if len(encoded) > 2 * 1024**2:
                    raise ValueError("native loss trace exceeds limit")
                native_inputs.write_bytes(encoded)
            else:
                oracle.write_json(native_inputs, payload)
            python = training.response(
                workers["python"], "validate", "trace_step", args.timeout
            )
            oracle.write_json(
                args.output / f"trace-{microbatch}.json",
                {"native": native, "python": python},
            )
            trace = python["loss_trace"]
            observation = {
                "microbatch": microbatch,
                "geometry_errors": trace.get("geometry_errors"),
                "forward": {},
                "gradient": {},
                "same_logits_gradient": {},
                "modules": trace.get("modules", []),
            }
            for key in (
                "starts",
                "ends",
                "inside",
                "pairs",
                "proposals",
                "nulls",
                "counts",
            ):
                actual = events["boundary_inputs"]["value"][key]
                if actual is None:
                    continue
                if trace.get("geometry_errors") and key in ("pairs", "proposals"):
                    continue  # Candidate slots cannot be compared without matching geometry.
                observation["forward"][key] = metrics(actual, trace["inputs"][key])
                actual = events["boundary_loss"]["gradients"][key]
                observation["gradient"][key] = metrics(
                    actual, trace["python"]["gradients"][key]
                )
                if "native_logits_python_backward" in trace:
                    observation["same_logits_gradient"][key] = metrics(
                        actual, trace["native_logits_python_backward"]["gradients"][key]
                    )
            receipts = {
                arm: training.response(w, "validate", "snapshot", args.timeout)
                for arm, w in workers.items()
            }
            comparison = training.compare_snapshots(
                receipts["native"], receipts["python"]
            )
            oracle.write_json(
                args.output / f"microbatch-{microbatch}-tensor-parity.json", comparison
            )
            observation["state_parity"] = comparison["passed"]
            report["observations"].append(observation)
            oracle.write_json(args.output / "report.json", report)
            for path in scratch.iterdir():
                path.unlink()
        for w in workers.values():
            w.request("stop", timeout=args.timeout)
        report["status"] = "complete"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        cleanup_errors = []
        for arm, w in workers.items():
            try:
                w.close()
            except BaseException as error:
                cleanup_errors.append(f"{arm}: {type(error).__name__}: {error}")
            report["cleanup"][arm] = w.cleanup
        try:
            shutil.rmtree(scratch)
        except OSError as error:
            cleanup_errors.append(str(error))
        if cleanup_errors:
            report.update(status="failed", cleanup_errors=cleanup_errors)
        oracle.write_json(args.output / "report.json", report)
        if cleanup_errors:
            raise common.BenchmarkError("; ".join(cleanup_errors))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("native-bin", "config", "upstream", "output", "snapshot-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--model", choices=("small", "base", "multi"), default="small")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument(
        "--microbatches",
        type=int,
        default=2,
        help="Bounded diagnostic length (1–128 microbatches)",
    )
    parser.add_argument(
        "--records",
        action="store_true",
        help="Capture bounded record logits and detached matching metadata",
    )
    parser.add_argument(
        "--modules",
        action="store_true",
        help="Compare retained Linear/LayerNorm inputs and outputs",
    )
    args = parser.parse_args()
    for name in ("native_bin", "config", "upstream", "output", "snapshot_root"):
        setattr(args, name, getattr(args, name).resolve())
    run(args)
