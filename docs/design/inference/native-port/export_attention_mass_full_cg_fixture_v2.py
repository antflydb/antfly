#!/usr/bin/env python3
"""Export one exact initial Main3 mass matrix; never fit or access held data."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys

for _name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS",
              "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[_name] = "1"
sys.path.insert(0, "/private/tmp")

import numpy as np
import run_attention_matching_full36_serial_final_build_heavy_v15_confirmed_parent_lifetime_main3 as heavy

SCHEMA = "antfly.attention-matching.mass-full-cg-fixture.v2"
OWNER = 14
BLOCK = 2048
QUERY_ROWS = 3000
EXACT_ROWS = 512
DIM = 512
COLUMNS = 288
HEAVY = Path("/private/tmp/run_attention_matching_full36_serial_final_build_heavy_v15_confirmed_parent_lifetime_main3.py")
HEAVY_SHA = "44ec9d1ec20b19f9aff19a4996f47b38b8b1b72d8fb22ef956a87708d4810e52"
MASS = Path("/private/tmp/attention_matching_next_36_recipe_v25_witness_retry.py")
MASS_SHA = "59fdd876c3e5397a530dd94e3f6c4503d2b780f813eb9a145f9b0d677d98b592"


def require(value, message):
    if not value:
        raise ValueError(message)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


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


def _module_path(module) -> Path:
    value = getattr(module, "__file__", None)
    require(type(value) is str, "loaded module has no file")
    return Path(value).resolve()


def terminal_inputs_checked(plan_path: Path, plan_sha256: str, initial_plan: dict):
    """Revalidate all plan-bound inputs and loaded helper origins."""
    require(sha256(plan_path) == plan_sha256, "plan changed")
    plan = heavy.checked(plan_path, plan_sha256, True)
    require(plan == initial_plan, "plan semantics changed")
    dependencies = {row["path"]: row["sha256"] for row in plan["dependencies"]}
    require(len(dependencies) == len(plan["dependencies"]),
            "duplicate dependency path")
    for row in plan["dependencies"] + list(plan["source"].values()):
        require(set(row) >= {"path", "sha256"} and
                sha256(Path(row["path"])) == row["sha256"],
                "input edge changed: " + str(row.get("path")))

    require(_module_path(heavy) == HEAVY.resolve() and
            dependencies.get(str(HEAVY)) == HEAVY_SHA and
            sha256(HEAVY) == HEAVY_SHA, "loaded heavy identity changed")
    mass_recipe = heavy.serial.base.mass288
    require(_module_path(mass_recipe) == MASS.resolve() and
            dependencies.get(str(MASS)) == MASS_SHA and
            sha256(MASS) == MASS_SHA, "loaded mass recipe identity changed")
    mass_kernel = mass_recipe.mass
    mass_kernel_path = _module_path(mass_kernel)
    require(dependencies.get(str(mass_kernel_path)) == sha256(mass_kernel_path),
            "mass kernel is not bound by plan")

    origins = heavy.origins(plan)
    require(origins == plan["expected_loaded_origins"],
            "loaded helper origin closure changed")
    for row in origins:
        require(dependencies.get(row["path"]) == row["sha256"] and
                sha256(Path(row["path"])) == row["sha256"],
                "loaded helper changed: " + row["path"])
    return plan, origins, mass_kernel, mass_kernel_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    require(args.plan.is_absolute() and args.output_dir.is_absolute(),
            "paths must be absolute")
    require(not args.output_dir.exists() and not args.output_dir.is_symlink(),
            "output must be fresh")
    require(sha256(HEAVY) == HEAVY_SHA and sha256(MASS) == MASS_SHA,
            "frozen source changed")

    plan = heavy.checked(args.plan, args.plan_sha256, True)
    require(plan["cell"] == {"owner_layer": OWNER, "block_start": BLOCK,
                             "retained_rows": COLUMNS}, "wrong cell")
    plan, loaded_origins, mass_kernel, mass_kernel_path = \
        terminal_inputs_checked(args.plan, args.plan_sha256, plan)
    _meta, _arrays, loaded = heavy.load(plan)
    require(loaded["owner_layer"] == OWNER, "owner changed")
    retained, keys, _values, _bias, _anchor, _reference = \
        heavy.serial.base.seed_checked(OWNER, loaded["original_seed"])
    q, exact_keys, _exact_values, scale, strata = \
        heavy.serial.base.training_checked(loaded["training"])
    require(retained == COLUMNS and q.shape == (QUERY_ROWS, DIM) and
            exact_keys.shape == (EXACT_ROWS, DIM) and len(strata) > 0 and
            math.isfinite(scale) and scale > 0, "source geometry changed")

    # These are the exact frozen expressions used before the first mass solve.
    context = mass_kernel._mass_context(q, exact_keys, scale)
    require(type(context) is tuple and len(context) == 3,
            "mass context return contract changed")
    maxima, exact_mass, _shifted = context
    matrix = mass_kernel._mass_matrix(
        q, keys.astype(np.float64), scale, maxima, exact_mass)
    matrix = np.ascontiguousarray(matrix, dtype=np.dtype("<f8"))
    require(matrix.shape == (QUERY_ROWS, COLUMNS) and
            np.isfinite(matrix).all(), "invalid full matrix")

    args.output_dir.mkdir(mode=0o700)
    matrix_path = args.output_dir / "full-matrix.f64le"
    write_all(matrix_path, matrix.tobytes(order="C"))

    # No completed manifest is sealed unless every consumed input and loaded
    # helper still matches the original authorized plan after computation.
    _terminal_plan, terminal_origins, terminal_kernel, terminal_kernel_path = \
        terminal_inputs_checked(args.plan, args.plan_sha256, plan)
    require(terminal_origins == loaded_origins and
            terminal_kernel is mass_kernel and
            terminal_kernel_path == mass_kernel_path,
            "terminal loaded origin closure changed")

    manifest = {
        "schema": SCHEMA,
        "completed": True,
        "diagnostic_only": True,
        "execution_authority": False,
        "publication_authority": False,
        "fit_invoked": False,
        "held_tensors_loaded": False,
        "cell": plan["cell"],
        "plan": {"path": str(args.plan), "sha256": args.plan_sha256,
                 "bytes": args.plan.stat().st_size},
        "source": plan["source"],
        "expected_final_build": plan["expected_final_build"],
        "origins": {
            "heavy": edge(HEAVY),
            "mass_recipe": edge(MASS),
            "mass_kernel": edge(mass_kernel_path),
            "loaded_helpers": terminal_origins,
            "mass_context": "mass288.mass._mass_context",
            "mass_matrix": "mass288.mass._mass_matrix",
        },
        "input": {"warm_rows": None, "original_seed_only": True,
                  "query_rows": QUERY_ROWS, "exact_rows": EXACT_ROWS,
                  "dimension": DIM, "scale": float(scale),
                  "strata_count": len(strata)},
        "matrix": {**edge(matrix_path), "path": matrix_path.name,
                   "name": "full_matrix_f64", "dtype": "float64-le",
                   "order": "C", "shape": [QUERY_ROWS, COLUMNS],
                   "max_abs": float(np.max(np.abs(matrix)))},
        "limits": {"no_fit": True, "no_held_data": True,
                   "no_checkpoint_reuse": True},
    }
    manifest_path = args.output_dir / "manifest.json"
    write_json(manifest_path, manifest)
    directory_fd = os.open(args.output_dir, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
