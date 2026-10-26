#!/usr/bin/env python3
"""Record frozen Python full-CG behavior without changing its decisions."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time

for _name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS",
              "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[_name] = "1"
sys.path.insert(0, "/private/tmp")

import numpy as np
import attention_matching_mass_lp_cg_subprocess_worker_v6_witness_retry as worker

SCHEMA = "antfly.attention-matching.mass-python-full-cg-oracle.v2"
FIXTURE_SCHEMA = "antfly.attention-matching.mass-full-cg-fixture.v2"
CG = Path("/private/tmp/attention_matching_mass_lp_constraint_generation_warm_v4.py")
CG_SHA = "fc07ae76a21272281690cd46af41034e7c995a0ecca313c684dda19fc4dac028"
WORKER = Path("/private/tmp/attention_matching_mass_lp_cg_subprocess_worker_v6_witness_retry.py")
WORKER_SHA = "3de2e56ccc74ba35814e2db85c0974c762bdce5299c1a6661521f711b3521a61"
ROWS, COLUMNS = 3000, 288


def require(value, message):
    if not value:
        raise ValueError(message)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def digest_array(value) -> str:
    array = np.ascontiguousarray(value)
    return hashlib.sha256(array.tobytes(order="C")).hexdigest()


def write_all(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(data)
        while view:
            count = os.write(fd, view)
            require(count > 0, "short write")
            view = view[count:]
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json(path: Path, value) -> None:
    write_all(path, (json.dumps(value, sort_keys=True, indent=2,
                                 allow_nan=False) + "\n").encode())


def edge(path: Path) -> dict:
    return {"path": str(path), "sha256": sha256(path),
            "bytes": path.stat().st_size}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--fixture-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--deadline-seconds", type=float, default=560.0)
    args = parser.parse_args()
    require(args.fixture_manifest.is_absolute() and args.output_dir.is_absolute(),
            "paths must be absolute")
    require(not args.output_dir.exists() and not args.output_dir.is_symlink(),
            "output must be fresh")
    require(math.isfinite(args.deadline_seconds) and
            0 < args.deadline_seconds <= 560, "invalid deadline")
    require(sha256(args.fixture_manifest) == args.fixture_sha256,
            "fixture manifest changed")
    require(sha256(CG) == CG_SHA and sha256(WORKER) == WORKER_SHA,
            "frozen Python CG changed")
    fixture = json.loads(args.fixture_manifest.read_text())
    require(fixture.get("schema") == FIXTURE_SCHEMA and
            fixture.get("completed") is True and
            fixture.get("fit_invoked") is False and
            fixture.get("held_tensors_loaded") is False and
            fixture.get("input", {}).get("warm_rows") is None,
            "invalid full fixture")
    me = fixture["matrix"]
    require(type(me.get("path")) is str and me["path"] == "full-matrix.f64le",
            "matrix path changed")
    matrix_path = args.fixture_manifest.parent / me["path"]
    require(me.get("shape") == [ROWS, COLUMNS] and
            me.get("dtype") == "float64-le" and me.get("order") == "C" and
            sha256(matrix_path) == me["sha256"] and
            matrix_path.stat().st_size == ROWS * COLUMNS * 8,
            "full matrix changed")
    matrix = np.memmap(matrix_path, dtype="<f8", mode="r",
                       shape=(ROWS, COLUMNS), order="C")
    require(np.isfinite(matrix).all(), "nonfinite matrix")

    args.output_dir.mkdir(mode=0o700)
    telemetry = args.output_dir / "retry-telemetry"
    telemetry.mkdir(mode=0o700)
    os.environ[worker._RETRY_ENV] = str(telemetry)
    attempts, rounds = [], []
    original_proto = worker._ORIGINAL_PROTO_SOLVE
    original_violations = worker.cg._violating_rows

    def recorded_proto(restricted, remaining_seconds):
        tolerance = (worker._RETRY_TOLERANCE if
                     worker.cg.proto.linprog is worker._tight_linprog else 1e-9)
        row = {"sequence": len(attempts), "rows": int(restricted.shape[0]),
               "columns": int(restricted.shape[1]),
               "restricted_matrix_sha256": digest_array(restricted),
               "feasibility_tolerance": tolerance}
        try:
            solved = original_proto(restricted, remaining_seconds)
        except BaseException as exc:
            row.update(status="error", error_type=type(exc).__name__,
                       error_message=str(exc))
            attempts.append(row)
            raise
        row.update(status="timeout" if solved is None else "optimal")
        if solved is not None:
            row.update(iterations=int(solved["iterations"]),
                       objective=float(solved["objective"]),
                       raw_weights_sha256=digest_array(solved["raw_weights"]),
                       alpha_sha256=digest_array(solved["alpha"]),
                       beta_sha256=digest_array(solved["beta"]))
        attempts.append(row)
        return solved

    def recorded_violations(a, raw, t, selected, tolerance):
        additions, worst = original_violations(a, raw, t, selected, tolerance)
        fitted = a @ raw
        upper = fitted - 1.0 - t
        lower = 1.0 - fitted - t
        candidates = []
        for index in range(a.shape[0]):
            if selected[index]:
                continue
            if upper[index] > tolerance:
                candidates.append((float(upper[index]), int(index), 0))
            if lower[index] > tolerance:
                candidates.append((float(lower[index]), int(index), 1))
        candidates.sort(key=lambda x: (-x[0], x[1], x[2]))
        unique, seen = [], set()
        for amount, index, sign in candidates:
            if index not in seen:
                unique.append((index, sign, amount))
                seen.add(index)
                if len(unique) == worker.cg.ADD_ROWS + 1:
                    break
        unselected = np.logical_not(selected)
        threshold_margin = float(min(
            np.min(np.abs(upper[unselected] - tolerance), initial=np.inf),
            np.min(np.abs(lower[unselected] - tolerance), initial=np.inf)))
        cutoff_margin = (float(unique[worker.cg.ADD_ROWS - 1][2] -
                               unique[worker.cg.ADD_ROWS][2])
                         if len(unique) > worker.cg.ADD_ROWS else None)
        rounds.append({
            "round": len(rounds),
            "selected_before": np.flatnonzero(selected).tolist(),
            "selected_before_sha256": digest_array(np.flatnonzero(selected).astype("<i8")),
            "tolerance": float(tolerance),
            "worst_full_violation": float(worst),
            "minimum_threshold_margin": threshold_margin,
            "addition_cutoff_margin": cutoff_margin,
            "fitted_sha256": digest_array(fitted),
            "upper_sha256": digest_array(upper),
            "lower_sha256": digest_array(lower),
            "added_rows": list(additions),
            "iterations": attempts[-1]["iterations"],
            "objective": attempts[-1]["objective"],
            "retried_witness": (len(attempts) >= 2 and
                                attempts[-2]["restricted_matrix_sha256"] ==
                                attempts[-1]["restricted_matrix_sha256"] and
                                attempts[-2]["feasibility_tolerance"] == 1e-9 and
                                attempts[-1]["feasibility_tolerance"] == worker._RETRY_TOLERANCE),
            "priced_prefix": [{"row": x[0], "sign": "upper" if x[1] == 0 else "lower",
                               "amount": x[2]} for x in unique],
        })
        return additions, worst

    worker._ORIGINAL_PROTO_SOLVE = recorded_proto
    worker.cg._violating_rows = recorded_violations
    started = time.monotonic()
    try:
        result = worker.cg.solve_mass_lp(matrix, args.deadline_seconds,
                                         initial_indices=None)
    finally:
        worker.cg._violating_rows = original_violations
        worker._ORIGINAL_PROTO_SOLVE = original_proto
    elapsed = time.monotonic() - started
    require(result.get("accepted") is True and result.get("solution") is not None,
            "Python full CG did not accept")
    solution = result["solution"]
    arrays = {name: np.ascontiguousarray(solution[name]) for name in
              ("raw_weights", "bias", "stored_weights", "mass", "alpha", "beta")}
    array_path = args.output_dir / "terminal-arrays.npz"
    fd = os.open(array_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=False) as stream:
            np.savez(stream, **arrays)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(fd)
    telemetry_edges = [edge(path) for path in sorted(telemetry.glob("*.json"))]
    require(sha256(args.fixture_manifest) == args.fixture_sha256 and
            sha256(matrix_path) == me["sha256"] and
            sha256(CG) == CG_SHA and sha256(WORKER) == WORKER_SHA,
            "consumed edge changed before oracle seal")
    receipt = {
        "schema": SCHEMA, "completed": True, "diagnostic_only": True,
        "execution_authority": False, "publication_authority": False,
        "fixture": {"path": str(args.fixture_manifest),
                    "sha256": args.fixture_sha256},
        "origins": {"constraint_generation": {"path": str(CG), "sha256": CG_SHA},
                    "worker": {"path": str(WORKER), "sha256": WORKER_SHA}},
        "deadline_seconds": args.deadline_seconds, "elapsed_seconds": elapsed,
        "warm_rows": None, "attempts": attempts, "rounds": rounds,
        "terminal": {
            "objective": float(solution["objective"]),
            "iterations": int(solution["iterations"]),
            "selected_row_indices": solution["constraint_generation"]["selected_row_indices"],
            "full_witness": solution["constraint_generation"]["full_witness"],
            "stored_f32_log_bias_mass": solution["constraint_generation"]["stored_f32_log_bias_mass"],
            "array_hashes": {name: digest_array(value) for name, value in arrays.items()},
            "arrays": edge(array_path),
        },
        "retry_telemetry": telemetry_edges,
        "limits": {"no_fit": True, "no_held_data": True,
                   "oracle_only": True, "qualification_authority": False},
    }
    write_json(args.output_dir / "oracle.json", receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
