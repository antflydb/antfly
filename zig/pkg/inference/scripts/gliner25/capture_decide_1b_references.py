#!/usr/bin/env python3
"""Capture classification-only references for GLiNER2.5-Decide-1B.

The checkpoint declares a Transformers 5.17 ModernBERT runtime.  This command
requires that runtime in an isolated target directory, proves both local and
global layer types use theta 160000, and never imports the system Transformers
4.x package.  Output is an upstream oracle, not native-runtime qualification.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import struct
import sys
import tempfile
from pathlib import Path
from typing import Any

import capture_family_references as common
import capture_family_decide_references as public_decide
import verify_family_contract as family


HERE = Path(__file__).resolve().parent
DEFAULT_REQUESTS = HERE / "decide_1b_reference_requests.json"
DEFAULT_RUNTIME_CONTRACT = HERE / "decide_1b_oracle_runtime.json"
PROFILE = "decide_1b"


def normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_record_tree(runtime_dir: Path, expected_tree_sha256: str | None) -> dict[str, Any]:
    """Verify every isolated-runtime file against wheel RECORD identities."""
    runtime_dir = runtime_dir.resolve()
    listed: set[Path] = set()
    records = sorted(runtime_dir.glob("*.dist-info/RECORD"))
    if not records:
        raise common.CaptureError("isolated runtime has no wheel RECORD files")
    for record in records:
        with record.open(newline="", encoding="utf-8") as stream:
            for row in csv.reader(stream):
                if len(row) != 3:
                    raise common.CaptureError(f"malformed wheel RECORD row in {record.name}")
                path = (runtime_dir / row[0]).resolve()
                if not path.is_relative_to(runtime_dir) or path in listed:
                    raise common.CaptureError("wheel RECORD path escapes or is duplicated")
                listed.add(path)
                if not path.is_file():
                    raise common.CaptureError(f"wheel RECORD file is absent: {row[0]}")
                if path == record:
                    if row[1] or row[2]:
                        raise common.CaptureError("wheel RECORD self-row must be unhashed")
                    continue
                if not row[1].startswith("sha256=") or not row[2].isdigit():
                    raise common.CaptureError(f"wheel RECORD identity is incomplete: {row[0]}")
                if path.stat().st_size != int(row[2]):
                    raise common.CaptureError(f"wheel RECORD size differs: {row[0]}")
                encoded = base64.urlsafe_b64encode(bytes.fromhex(sha256(path))).decode().rstrip("=")
                if encoded != row[1][len("sha256="):]:
                    raise common.CaptureError(f"wheel RECORD hash differs: {row[0]}")
    actual = {path.resolve() for path in runtime_dir.rglob("*") if path.is_file()}
    if actual != listed:
        extra = sorted(str(path.relative_to(runtime_dir)) for path in actual - listed)
        raise common.CaptureError(f"isolated runtime contains unrecorded files: {extra!r}")
    tree = hashlib.sha256()
    for path in sorted(actual):
        relative = str(path.relative_to(runtime_dir)).encode()
        tree.update(relative + b"\0" + bytes.fromhex(sha256(path)))
        tree.update(struct.pack("<Q", path.stat().st_size))
    digest = tree.hexdigest()
    if expected_tree_sha256 is not None and digest != expected_tree_sha256:
        raise common.CaptureError("isolated runtime tree identity differs")
    return {"files": len(actual), "record_files": len(records), "tree_sha256": digest}


def verify_runtime_dir(runtime_dir: Path, contract: dict[str, Any]) -> dict[str, Any]:
    runtime_dir = runtime_dir.expanduser().resolve()
    expected = contract["oracle_runtime_decide_1b"]
    actual_python = platform.python_version()
    actual_unicode = __import__("unicodedata").unidata_version
    if actual_python != expected["python"] or actual_unicode != expected["unicode"]:
        raise common.CaptureError(
            f"1B Python/Unicode runtime differs: {actual_python}/{actual_unicode}"
        )
    if not runtime_dir.is_dir():
        raise common.CaptureError(f"isolated runtime directory is absent: {runtime_dir}")
    actual = {
        normalized(dist.metadata["Name"]): dist.version
        for dist in importlib.metadata.distributions(path=[str(runtime_dir)])
    }
    wanted = {normalized(name): version for name, version in expected["isolated_packages"].items()}
    if actual != wanted:
        raise common.CaptureError(
            f"isolated runtime inventory differs: actual={actual!r} expected={wanted!r}"
        )
    info = runtime_dir / "transformers-5.17.0.dist-info"
    distribution = expected["transformers_distribution"]
    for name, key in (("METADATA", "installed_metadata_sha256"), ("RECORD", "installed_record_sha256")):
        path = info / name
        if not path.is_file() or sha256(path) != distribution[key]:
            raise common.CaptureError(f"isolated Transformers {name} identity differs")
    tree = verify_record_tree(runtime_dir, expected["runtime_tree_sha256"])
    external = {
        name: importlib.metadata.version(name)
        for name in expected["external_packages"]
    }
    if external != expected["external_packages"]:
        raise common.CaptureError(f"external runtime differs: actual={external!r}")
    return {
        "python": actual_python,
        "unicode": actual_unicode,
        "isolated_packages": actual,
        "external_packages": external,
        "transformers_distribution": distribution,
        "verified_tree": tree,
        "runtime_dir": str(runtime_dir),
    }


def activate(runtime_dir: Path, upstream: Path) -> None:
    # The exact isolated target must win over globally installed Transformers.
    sys.path.insert(0, str(runtime_dir.expanduser().resolve()))
    sys.path.insert(1, str(upstream.expanduser().resolve()))


def tensor_sha256(tensor: Any) -> str:
    values = tensor.detach().cpu().to(dtype=__import__("torch").float32).tolist()
    return hashlib.sha256(struct.pack(f"<{len(values)}f", *values)).hexdigest()


def verify_rope(model_dir: Path, contract: dict[str, Any], encoder: Any | None = None) -> dict[str, Any]:
    import torch
    from transformers import AutoConfig
    from transformers.models.modernbert.modeling_modernbert import ModernBertRotaryEmbedding

    spec = contract["oracle_runtime_decide_1b"]["rope_contract"]
    config = AutoConfig.from_pretrained(
        str(model_dir / "encoder_config" / "config.json"), local_files_only=True
    )
    expected_params = {
        layer_type: {"rope_theta": spec["rope_theta"], "rope_type": spec["rope_type"]}
        for layer_type in spec["layer_types"]
    }
    if config.rope_parameters != expected_params:
        raise common.CaptureError(f"ModernBERT rope_parameters differ: {config.rope_parameters!r}")
    if config.hidden_size // config.num_attention_heads != spec["head_dim"]:
        raise common.CaptureError("ModernBERT head dimension differs")
    rotary = ModernBertRotaryEmbedding(config) if encoder is None else encoder.rotary_emb
    expected = 1.0 / (
        spec["rope_theta"]
        ** (torch.arange(0, spec["head_dim"], 2, dtype=torch.float32) / spec["head_dim"])
    )
    layers: dict[str, Any] = {}
    for layer_type in spec["layer_types"]:
        actual = getattr(rotary, f"{layer_type}_inv_freq").detach().cpu().float()
        if not torch.equal(actual, expected):
            raise common.CaptureError(f"{layer_type} does not honor the pinned RoPE theta")
        digest = tensor_sha256(actual)
        if digest != spec["inv_freq_sha256_f32le"]:
            raise common.CaptureError(f"{layer_type} RoPE frequency identity differs")
        layers[layer_type] = {
            "rope_theta": spec["rope_theta"],
            "inv_freq_count": actual.numel(),
            "inv_freq_sha256_f32le": digest,
            "first": actual[0].item(),
            "last": actual[-1].item(),
        }
    return {"head_dim": spec["head_dim"], "layers": layers}


def validate_requests(path: Path) -> dict[str, Any]:
    document = common.validate_requests(path)
    if document.get("scope") != "classification_only_span_modernbert_1b":
        raise common.CaptureError("1B requests must declare their classification-only scope")
    if any(row["kind"] != "classification" for row in document["requests"]):
        raise common.CaptureError("Decide-1B oracle only accepts classification requests")
    geometry = document.get("prepared_geometry")
    expected = {
        "long_context_cutoff": {
            "input_tokens": 198,
            "processor_text_tokens": 114,
            "schema_token_lengths": [12],
        }
    }
    if geometry != expected:
        raise common.CaptureError("1B prepared geometry contract differs")
    return document


def verify_prepared_geometry(rows: list[dict[str, Any]], expected: dict[str, Any]) -> None:
    by_id = {row["id"]: row for row in rows}
    for request_id, geometry in expected.items():
        if request_id not in by_id:
            raise common.CaptureError(f"prepared geometry row is absent: {request_id}")
        encoded = by_id[request_id]["encoded"]
        actual = {
            "input_tokens": len(encoded["input_ids"]),
            "processor_text_tokens": len(encoded["text_tokens"]),
            "schema_token_lengths": [len(tokens) for tokens in encoded["schema_tokens"]],
        }
        if actual != geometry:
            raise common.CaptureError(
                f"prepared geometry differs for {request_id}: actual={actual!r} expected={geometry!r}"
            )


def capture(args: argparse.Namespace) -> dict[str, Any]:
    if os.environ.get("PYTHONHASHSEED") != "0":
        raise common.CaptureError("capture requires PYTHONHASHSEED=0")
    contract = family.strict_json(args.contract)
    runtime_contract = family.strict_json(args.runtime_contract)
    if runtime_contract.get("format_version") != 1:
        raise common.CaptureError("1B runtime contract must have format_version 1")
    source = common.verify_source(args.upstream, contract)
    expected_helper = runtime_contract["oracle_runtime_decide_1b"]["shared_helper_sha256"]
    if sha256(Path(common.__file__)) != expected_helper:
        raise common.CaptureError("shared family capture helper identity differs")
    expected_public_helper = runtime_contract["oracle_runtime_decide_1b"]["public_decide_helper_sha256"]
    if sha256(Path(public_decide.__file__)) != expected_public_helper:
        raise common.CaptureError("public /decide capture helper identity differs")
    requests = validate_requests(args.requests)
    public_requests = public_decide.validate_requests(args.public_requests)
    model_before = family.verify_model(
        PROFILE, args.model_dir, contract_path=args.contract, verify_model_sha256=True
    )
    runtime = verify_runtime_dir(args.runtime_dir, runtime_contract)

    sys.dont_write_bytecode = True
    os.environ.update({
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "TOKENIZERS_PARALLELISM": "false",
        "OMP_NUM_THREADS": str(args.threads),
        "MKL_NUM_THREADS": str(args.threads),
    })
    activate(args.runtime_dir, args.upstream)
    import torch
    import transformers
    import gliner2
    from gliner2 import AutoExtractor

    expected_runtime = runtime_contract["oracle_runtime_decide_1b"]
    if platform.python_version() != expected_runtime["python"]:
        raise common.CaptureError("Python version differs from the 1B oracle runtime")
    if transformers.__version__ != "5.17.0" or gliner2.__version__ != "2.0.0":
        raise common.CaptureError("1B oracle imported an unexpected source runtime")
    runtime_root = args.runtime_dir.resolve()
    if not Path(transformers.__file__).resolve().is_relative_to(runtime_root):
        raise common.CaptureError("Transformers was imported outside the isolated runtime")
    rope_before = verify_rope(args.model_dir, runtime_contract)

    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(0)
    model = AutoExtractor.from_pretrained(
        str(args.model_dir.resolve()),
        local_files_only=True,
        map_location="cpu",
        use_flashdeberta=False,
    ).float().cpu().eval()
    if getattr(model, "architecture", None) != "span":
        raise common.CaptureError("Decide-1B oracle requires the declared span checkpoint")
    rope_loaded = verify_rope(args.model_dir, runtime_contract, model.encoder)
    if rope_loaded != rope_before:
        raise common.CaptureError("loaded encoder RoPE differs from configuration proof")
    rows = common.capture_requests(model, requests["requests"], torch)
    verify_prepared_geometry(rows, requests["prepared_geometry"])
    public_rows = public_decide.capture_rows(
        model,
        public_requests["requests"],
        torch,
        contract["models"][PROFILE]["repo"],
    )

    model_after = family.verify_model(
        PROFILE, args.model_dir, contract_path=args.contract, verify_model_sha256=True
    )
    if model_after != model_before:
        raise common.CaptureError("model identity changed during capture")
    common.verify_source(args.upstream, contract)
    report = {
        "format_version": 1,
        "status": "captured",
        "scope": "upstream_decide_1b_classification_reference",
        "qualification": False,
        "native_runtime_qualified": False,
        "production_qualified": False,
        "semantic_pass": all(row["semantic"]["pass"] for row in rows)
        and all(row["semantic"]["pass"] for row in public_rows),
        "model": model_before,
        "source": {**source, "package_version": gliner2.__version__},
        "runtime": {**runtime, "device": "cpu", "dtype": "float32", "threads": args.threads},
        "rope": rope_loaded,
        "artifacts": {
            "generator_sha256": sha256(Path(__file__)),
            "contract_sha256": sha256(args.contract),
            "runtime_contract_sha256": sha256(args.runtime_contract),
            "shared_helper_sha256": sha256(Path(common.__file__)),
            "public_decide_helper_sha256": sha256(Path(public_decide.__file__)),
            "requests_sha256": sha256(args.requests),
            "public_requests_sha256": sha256(args.public_requests),
        },
        "requests": rows,
        "public_decide_requests": public_rows,
    }
    destination = args.output.expanduser().absolute()
    if destination.exists() or destination.is_symlink():
        raise common.CaptureError(f"refusing to overwrite output: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination.parent))
    try:
        (staging / "capture.json").write_text(
            json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        staging.rename(destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {"status": "captured", "output": str(destination), "requests": len(rows),
            "public_decide_requests": len(public_rows),
            "semantic_pass": report["semantic_pass"], "qualification": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--requests", type=Path, default=DEFAULT_REQUESTS)
    parser.add_argument("--public-requests", type=Path, default=public_decide.DEFAULT_REQUESTS)
    parser.add_argument("--contract", type=Path, default=family.CONTRACT_PATH)
    parser.add_argument("--runtime-contract", type=Path, default=DEFAULT_RUNTIME_CONTRACT)
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    try:
        if not 1 <= args.threads <= 8:
            raise common.CaptureError("threads must be in 1..8")
        print(json.dumps(capture(args), sort_keys=True, allow_nan=False))
        return 0
    except (common.CaptureError, family.ContractError, OSError, ImportError, RuntimeError,
            ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__,
                          "error": str(exc), "qualification": False}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
