#!/usr/bin/env python3
"""Export one frozen real restricted-mass LP fixture without NumPy/SciPy.

This is qualification tooling, not runtime code.  It validates exact source
artifacts, parses only NPY headers with the Python standard library, streams
the member payloads byte-for-byte, and writes a fresh self-describing fixture.
It never interprets floating-point values or invokes an optimizer.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import tempfile
import zipfile


WITNESS = Path(
    "/private/tmp/attention-matching-main3-o14-b2048-lp-witness-probe-v1/"
    "witness/witness-000000-pid-92013.json"
)
NPZ = Path(
    "/private/tmp/attention-matching-main3-o14-b2048-lp-witness-probe-v1/"
    "witness/witness-000000-pid-92013.npz"
)
VARIANTS = Path("/private/tmp/attention-matching-main3-lp-residual-variants-v1/variants.json")

SOURCE_EDGES = {
    "witness": {
        "path": str(WITNESS),
        "sha256": "bd98a1c8caf0ff485a06bb88e47638d9d45ddec0b548aa9e0ea0e5f655dd677b",
        "bytes": 3582,
    },
    "npz": {
        "path": str(NPZ),
        "sha256": "bc0655ad1d4632bd37cf89a90605805078162bab1f15abe6986b5e2cb3e5f944",
        "bytes": 2638892,
    },
    "variants": {
        "path": str(VARIANTS),
        "sha256": "285229303e41b7eab47d9646c555b357852eb6652c56b14461e921faeaff2159",
        "bytes": 11652,
    },
}

# ZIP member order is part of the fixture.  Semantic source ordering in the
# witness receipt is validated separately because np.savez and sorted metadata
# deliberately use different orders.
MEMBERS = [
    ("restricted_matrix_f64.npy", "<f8", (1128, 288), 2598912,
     "4c24c0b1e26aa16e533317bcbb1a2a84a4171e71f6183bd5a59722e3438d9bbe"),
    ("raw_weights_f64.npy", "<f8", (288,), 2304,
     "60f806fc4da81cedaa747c2eca19ed33dd048a4afafec3ec2ce986171f4cc1db"),
    ("alpha_f64.npy", "<f8", (1128,), 9024,
     "d3e080bb240a1d36e49a7c0e0cb7d8810ec72ca22d711d275fdcbf4d16a29358"),
    ("beta_f64.npy", "<f8", (1128,), 9024,
     "ed7a99f47c621b24dc38ccba9c68a44c6dfad8be20ef08d10f8070eabbdb6555"),
    ("upper_slack_f64.npy", "<f8", (1128,), 9024,
     "066a48327e0485c05eeb32c6da8a645bf98b8871254cb3c9409227e070932177"),
    ("lower_slack_f64.npy", "<f8", (1128,), 9024,
     "44e9570111a632bf5ac593b724fdfffe8d37fb88aa4f1b98ce3806c4dbffb408"),
]

SEMANTIC_NAMES = [
    "alpha_f64", "beta_f64", "lower_slack_f64", "raw_weights_f64",
    "restricted_matrix_f64", "upper_slack_f64",
]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def checked_edge(edge: dict[str, object]) -> Path:
    require(set(edge) == {"path", "sha256", "bytes"}, "invalid source edge")
    path = Path(str(edge["path"]))
    require(path.is_absolute() and path.is_file() and not path.is_symlink(), "invalid source path")
    require(path.stat().st_size == edge["bytes"], "source byte size changed")
    require(sha256_path(path) == edge["sha256"], "source digest changed")
    return path


def read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text())
    require(isinstance(value, dict), "expected JSON object")
    return value


def read_npy_header(member) -> dict[str, object]:
    require(member.read(6) == b"\x93NUMPY", "invalid NPY magic")
    version = member.read(2)
    require(version in (b"\x01\x00", b"\x02\x00", b"\x03\x00"), "unsupported NPY version")
    size_width = 2 if version[0] == 1 else 4
    packed = member.read(size_width)
    require(len(packed) == size_width, "truncated NPY header size")
    header_size = struct.unpack("<H" if size_width == 2 else "<I", packed)[0]
    raw = member.read(header_size)
    require(len(raw) == header_size, "truncated NPY header")
    header = ast.literal_eval(raw.decode("latin1").strip())
    require(isinstance(header, dict), "invalid NPY header")
    require(set(header) == {"descr", "fortran_order", "shape"}, "unexpected NPY header fields")
    return header


def stream_payload(member, destination=None) -> tuple[int, str]:
    h = hashlib.sha256()
    count = 0
    while True:
        block = member.read(1024 * 1024)
        if not block:
            break
        h.update(block)
        count += len(block)
        if destination is not None:
            destination.write(block)
    return count, h.hexdigest()


def exact_variant(variants: dict[str, object], name: str) -> dict[str, object]:
    rows = variants.get("variants")
    require(isinstance(rows, list), "invalid variant rows")
    matches = [row for row in rows if isinstance(row, dict) and row.get("variant") == name]
    require(len(matches) == 1, f"missing or duplicate {name} variant")
    return matches[0]


def validate_sources() -> tuple[dict[str, object], dict[str, object]]:
    witness_path = checked_edge(SOURCE_EDGES["witness"])
    npz_path = checked_edge(SOURCE_EDGES["npz"])
    variants_path = checked_edge(SOURCE_EDGES["variants"])
    witness = read_json(witness_path)
    variants = read_json(variants_path)

    require(witness.get("schema") == "antfly.attention-matching.cg-worker-witness-failure-diagnostic.v1", "witness schema changed")
    require(witness.get("completed") is True and witness.get("diagnostic_only") is True, "invalid witness disposition")
    require(witness.get("accepted_payload") is False, "diagnostic unexpectedly accepted a payload")
    arrays = witness.get("arrays")
    require(isinstance(arrays, dict), "missing arrays edge")
    require(arrays.get("path") == str(npz_path) and arrays.get("sha256") == SOURCE_EDGES["npz"]["sha256"], "witness NPZ edge changed")
    require(arrays.get("names") == SEMANTIC_NAMES, "semantic array order changed")
    require(witness.get("matrix_identity") == {
        "dtype": "float64", "shape": [1128, 288],
        "sha256": MEMBERS[0][4],
    }, "matrix identity changed")

    require(variants.get("schema") == "antfly.attention-matching.main3-lp-residual-variants.v1", "variant schema changed")
    require(variants.get("completed") is True and variants.get("diagnostic_only") is True, "invalid variants disposition")
    require(variants.get("accepted_payload") is False and variants.get("held_tensors_loaded") is False, "variants gained authority")
    require(variants.get("input") == {"path": str(npz_path), "sha256": SOURCE_EDGES["npz"]["sha256"]}, "variant input changed")
    require([row.get("variant") for row in variants["variants"]] == [
        "original_settings", "tighter_feasibility", "without_presolve", "column_scaled",
    ], "variant order changed")

    original = exact_variant(variants, "original_settings")
    tighter = exact_variant(variants, "tighter_feasibility")
    require(original["solver"]["primal_feasibility_tolerance"] == 1e-9 and original["solver"]["dual_feasibility_tolerance"] == 1e-9, "original tolerances changed")
    require(original["solver"]["presolve"] is True and original["solver"]["iterations"] == 1424, "original solver record changed")
    require(original["witness"]["all_checks_passed"] is False, "original witness disposition changed")
    require(original["witness"]["primal_max_violation"] == 1.2125560508030597e-6, "original residual changed")
    require(tighter["solver"]["primal_feasibility_tolerance"] == 1e-10 and tighter["solver"]["dual_feasibility_tolerance"] == 1e-10, "retry tolerances changed")
    require(tighter["solver"]["presolve"] is True and tighter["solver"]["iterations"] == 1451, "retry solver record changed")
    require(tighter["witness"]["all_checks_passed"] is True, "retry witness disposition changed")
    require(tighter["witness"]["primal_max_violation"] == 1.2878680066830128e-10, "retry residual changed")

    with zipfile.ZipFile(npz_path) as archive:
        require(archive.namelist() == [row[0] for row in MEMBERS], "NPZ member order changed")
        for name, descr, shape, payload_bytes, payload_sha in MEMBERS:
            info = archive.getinfo(name)
            require(info.compress_type == zipfile.ZIP_STORED, "unexpected NPZ compression")
            with archive.open(info) as member:
                header = read_npy_header(member)
                require(header == {"descr": descr, "fortran_order": False, "shape": shape}, "NPY layout changed")
                count, digest = stream_payload(member)
            require(count == payload_bytes and digest == payload_sha, "NPY payload changed")
    return witness, variants


def fsync_json(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8") as f:
        json.dump(value, f, sort_keys=True, separators=(",", ":"))
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def export(output: Path) -> dict[str, object]:
    witness, variants = validate_sources()
    require(output.is_absolute(), "output must be absolute")
    require(not output.exists() and not output.is_symlink(), "output must be fresh")
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=output.name + ".candidate-", dir=output.parent))
    try:
        raw_edges = []
        with zipfile.ZipFile(NPZ) as archive:
            for name, descr, shape, payload_bytes, payload_sha in MEMBERS:
                target = temp / name.removesuffix(".npy").replace("_f64", ".f64le")
                with archive.open(name) as member, target.open("xb") as destination:
                    header = read_npy_header(member)
                    require(header == {"descr": descr, "fortran_order": False, "shape": shape}, "NPY layout changed during export")
                    count, digest = stream_payload(member, destination)
                    destination.flush()
                    os.fsync(destination.fileno())
                require(count == payload_bytes and digest == payload_sha, "NPY payload changed during export")
                raw_edges.append({"name": name, "path": target.name, "sha256": digest, "bytes": count, "dtype": "float64-le", "shape": list(shape), "order": "C"})

        shutil.copyfile(WITNESS, temp / "source-witness.json")
        shutil.copyfile(VARIANTS, temp / "source-variants.json")
        for copied in (temp / "source-witness.json", temp / "source-variants.json"):
            with copied.open("rb") as f:
                os.fsync(f.fileno())

        original = exact_variant(variants, "original_settings")
        tighter = exact_variant(variants, "tighter_feasibility")
        manifest = {
            "schema": "antfly.attention-matching.mass-highs-restricted-fixture.v1",
            "completed": True,
            "diagnostic_only": True,
            "execution_authority": False,
            "publication_authority": False,
            "source_edges": SOURCE_EDGES,
            "raw_arrays": raw_edges,
            "problem": {
                "builder": "attention_mass_lp_v4.buildPrimaryProblem",
                "source_rows": 1128,
                "weight_columns": 288,
                "variables": 289,
                "constraint_rows": 2256,
                "variable_order": "w[0:288],t",
                "constraint_order": "upper[0:1128],lower[0:1128]",
                "bounds": {"weight_min": 1e-9, "weight_max": 1e6, "t_lower": 0.0, "t_upper": "+inf"},
                "objective": "minimize t",
                "raw_row_dual_mapping": "alpha=-row_dual[0:1128],beta=-row_dual[1128:2256]",
            },
            "oracles": {
                "original_1e_9": {
                    "vector_evidence": "exact raw_weights/alpha/beta payloads",
                    "objective": original["witness"]["objective"],
                    "iterations": original["solver"]["iterations"],
                    "witness_all_checks_passed": False,
                    "primal_max_violation": original["witness"]["primal_max_violation"],
                    "expected_controller_action": "retry_once_at_1e-10",
                },
                "retry_1e_10": {
                    "vector_evidence": "unavailable; metrics-only oracle",
                    "objective": tighter["witness"]["objective"],
                    "iterations": tighter["solver"]["iterations"],
                    "witness_all_checks_passed": True,
                    "primal_max_violation": tighter["witness"]["primal_max_violation"],
                    "expected_controller_action": "accept_restricted_solution_then_continue_CG",
                },
            },
            "limits": {
                "no_numpy_or_scipy_used": True,
                "no_optimizer_run": True,
                "retry_vectors_not_preserved": True,
                "full_CG_acceptance_not_proven_by_this_restricted_fixture": True,
            },
        }
        fsync_json(temp / "manifest.json", manifest)
        dir_fd = os.open(temp, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        os.replace(temp, output)
        parent_fd = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return manifest
    except BaseException:
        shutil.rmtree(temp, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    require(args.check_only != (args.output is not None), "choose exactly one of --check-only or --output")
    if args.check_only:
        validate_sources()
        print("1/1 frozen restricted fixture sources PASS")
    else:
        manifest = export(args.output)
        print(json.dumps({"completed": True, "output": str(args.output), "schema": manifest["schema"]}, sort_keys=True))


if __name__ == "__main__":
    main()
