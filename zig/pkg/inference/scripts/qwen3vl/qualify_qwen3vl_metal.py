#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Fail-closed Qwen3-VL Transformers preprocessing and Metal acceptance gate.

This lane is deliberately serial and bounded. It proves an exact managed
bundle, frozen Transformers processor/tokenizer/M-RoPE parity, real Metal
execution, numeric image-preprocessing tolerances, and a resource envelope.
With --weights-dir it also gates the complete final-row logit vector against
the pinned BF16 checkpoint. It does not claim long-run production qualification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import platform
import re
import signal
import stat
import subprocess
import sys
import time
from typing import Any


SCHEMA = "antfly.qwen3vl.metal_qualification.v1"
RECEIPT_NAME = ".antfly-download-complete.json"
BLOCKING_MARKERS = (".antfly-download-in-progress", ".antfly-download-plan.json")
EXPECTED_SOURCE = {
    "owner": "Qwen",
    "name": "Qwen3-VL-2B-Instruct-GGUF",
    "variant": "q4-k-m-bundle-v1",
}
RERANKER_EXPECTED_SOURCE = {
    "owner": "Qwen",
    "name": "Qwen3-VL-Reranker-2B",
    "variant": "bf16-safetensors-bundle-v1",
}
RERANKER_EXPECTED_ARTIFACTS = {
    "model.safetensors": (4_255_140_312, "466ec01961061e9d7f804b4fb1625fb6f406106cd1567e026096d4736fa9d5b9"),
    "config.json": (1_652, "82d38a8f803e38e13986fdd622114a6fec12a834adbd3cee9253d757a257d23d"),
    "tokenizer.json": (11_422_654, "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4"),
    "tokenizer_config.json": (5_445, "81ec7bb9530159b326c0bef1d0b6c33d392090524014ea3f0123a3c1eb9c2af5"),
    "preprocessor_config.json": (628, "fd32af55c2d3846adb0bc46df8eb07c92c332b31b34c338ae85259f3f3951f24"),
    "video_preprocessor_config.json": (817, "59c5c9eb52182eb14c06ffb10ca9effd29adce5f238a95de23ca14a38dbd2cb1"),
    "chat_template.jinja": (5_292, "3636d0f0bd6bef02654cdffdc447b79cb2cef8ab02cc75267345946291a489e4"),
    "additional_chat_templates/reranker.jinja": (2_443, "47c758cb74d7f1e20e22483949a5ba4c8c1f4515126ad173da1c63211f472aa7"),
    "1_LogitScore/config.json": (57, "73e3156450564d8a98b7e47bcf5aace0f29600828b51937da545571e84db3ff3"),
    "modules.json": (280, "6f13b6b4a89e577b591b2077bca40c67c26541a6740a8809267cb474f90806a9"),
    "sentence_bert_config.json": (756, "729676c811dadb5cf2cefdfcfca1bd04de40d0f0caed8a6482016d8a2937341d"),
    "scripts/qwen3_vl_reranker.py": (10_873, "bd5d2f5d97fc4a738864d93f6b15d8850243e60da4484f3ea78867a46efdebd6"),
}
RERANKER_GENERATED_ARTIFACTS = {
    "antfly_inference_bundle.json": (81, "1e2df99d4e60b29e4d95faf2d18f5097a1af02bd74d2a40d037d204358913e46"),
    "model_manifest.json": (64, "696935ae5821d0e7babf351925e900d7dd6aaa96e1780608283359554694af16"),
}
PATCH_LIMITS = {
    # Versioned cross-implementation tolerances for normalized spatial patch
    # inputs. They cover the checked-in square downscale and non-square color
    # upscale fixtures while remaining far below one normalized 8-bit step in
    # aggregate. Geometry, token expansion, and M-RoPE remain exact gates.
    "mean_abs": 0.003,
    "rmse": 0.004,
    "p99_abs": 0.010,
    "max_abs": 0.150,
}
LOGIT_LIMITS = {
    # Initial Qwen3-VL-2B Q4_K_M decoder + Q8_0 projector acceptance envelope.
    # Greedy argmax remains an independent exact gate.
    "min_cosine_similarity": 0.95,
    "min_pearson_correlation": 0.95,
    "max_mean_abs": 1.0,
    "max_rmse": 1.25,
    "max_max_abs": 6.0,
    "min_top_10_overlap": 8,
}
FORBIDDEN_RUNTIME_OUTPUT = re.compile(
    r"falling back|unsupportedvision|segmentation fault|metal command buffer[^\n]*error|panic:",
    re.IGNORECASE,
)


class QualificationError(RuntimeError):
    pass


class ResourceViolation(QualificationError):
    def __init__(self, message: str, execution: dict[str, Any]) -> None:
        super().__init__(message)
        self.execution = execution


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QualificationError(f"invalid JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise QualificationError(f"JSON root is not an object: {path}")
    return value


def safe_artifact_path(raw: object) -> str:
    if not isinstance(raw, str) or not raw or "\\" in raw or ":" in raw or "\x00" in raw:
        raise QualificationError(f"unsafe managed artifact path: {raw!r}")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise QualificationError(f"unsafe managed artifact path: {raw!r}")
    return raw


def _validate_managed_install(
    model_dir: Path,
    expected_source: dict[str, str],
) -> dict[str, Any]:
    root = model_dir.resolve(strict=True)
    if not root.is_dir():
        raise QualificationError(f"model path is not a directory: {root}")
    for marker in BLOCKING_MARKERS:
        if (root / marker).exists():
            raise QualificationError(f"managed publication is incomplete: {marker}")

    receipt_path = root / RECEIPT_NAME
    receipt = load_json(receipt_path)
    if receipt.get("version") != 2:
        raise QualificationError("qualification requires a version-2 managed receipt")
    if receipt.get("source") != expected_source:
        raise QualificationError(
            f"managed source mismatch: expected {expected_source}, got {receipt.get('source')}"
        )
    raw_artifacts = receipt.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise QualificationError("managed receipt has no artifacts")

    seen: set[str] = set()
    artifacts: list[dict[str, Any]] = []
    for item in raw_artifacts:
        if not isinstance(item, dict):
            raise QualificationError("managed artifact receipt is not an object")
        relative = safe_artifact_path(item.get("path"))
        if relative in seen:
            raise QualificationError(f"duplicate managed artifact: {relative}")
        seen.add(relative)
        path = root / relative
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise QualificationError(f"missing managed artifact {relative}: {exc}") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise QualificationError(f"managed artifact is not a regular file: {relative}")
        canonical = path.resolve(strict=True)
        if not canonical.is_relative_to(root):
            raise QualificationError(f"managed artifact escapes model root: {relative}")
        expected_size = item.get("size")
        if not isinstance(expected_size, int) or expected_size < 0 or metadata.st_size != expected_size:
            raise QualificationError(
                f"managed artifact size mismatch for {relative}: "
                f"expected {expected_size}, got {metadata.st_size}"
            )
        actual_sha = sha256_file(path)
        expected_sha = item.get("sha256")
        if expected_sha is not None:
            if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
                raise QualificationError(f"invalid receipt SHA-256 for {relative}")
            if actual_sha != expected_sha:
                raise QualificationError(
                    f"managed artifact SHA-256 mismatch for {relative}: "
                    f"expected {expected_sha}, got {actual_sha}"
                )
        artifacts.append(
            {
                "path": relative,
                "size": metadata.st_size,
                "sha256": actual_sha,
                "receipt_sha256": expected_sha,
            }
        )

    actual_files = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path.name != RECEIPT_NAME
    }
    if actual_files != seen:
        raise QualificationError(
            f"managed receipt/file set mismatch: missing={sorted(seen - actual_files)} "
            f"unexpected={sorted(actual_files - seen)}"
        )

    return {
        "model_dir": str(root),
        "receipt_path": str(receipt_path),
        "receipt_sha256": sha256_file(receipt_path),
        "source": receipt["source"],
        "artifacts": artifacts,
    }


def validate_managed_bundle(
    model_dir: Path,
    expected_source: dict[str, str] = EXPECTED_SOURCE,
) -> dict[str, Any]:
    evidence = _validate_managed_install(model_dir, expected_source)
    root = Path(evidence["model_dir"])
    seen = {artifact["path"] for artifact in evidence["artifacts"]}

    bundle = load_json(root / "antfly_inference_bundle.json")
    if bundle.get("family") != "qwen3_vl_gguf_bundle/v1":
        raise QualificationError(f"unexpected bundle family: {bundle.get('family')!r}")
    decoder = safe_artifact_path(bundle.get("decoder"))
    projector = safe_artifact_path(bundle.get("projector"))
    if decoder not in seen or projector not in seen:
        raise QualificationError("bundle decoder/projector is absent from the managed receipt")
    if not decoder.endswith(".gguf") or not projector.endswith(".gguf"):
        raise QualificationError("Qwen3-VL generation bundle must contain GGUF decoder/projector")
    evidence.update(
        {
            "bundle": bundle,
            "decoder_path": str(root / decoder),
            "projector_path": str(root / projector),
        }
    )
    return evidence


def validate_managed_reranker_bundle(model_dir: Path) -> dict[str, Any]:
    evidence = _validate_managed_install(model_dir, RERANKER_EXPECTED_SOURCE)
    root = Path(evidence["model_dir"])
    received = {artifact["path"]: artifact for artifact in evidence["artifacts"]}
    expected_paths = set(RERANKER_EXPECTED_ARTIFACTS) | set(RERANKER_GENERATED_ARTIFACTS)
    if set(received) != expected_paths:
        raise QualificationError(
            "reranker artifact set mismatch: "
            f"missing={sorted(expected_paths - set(received))} "
            f"unexpected={sorted(set(received) - expected_paths)}"
        )
    for path, (expected_size, expected_sha) in RERANKER_EXPECTED_ARTIFACTS.items():
        artifact = received[path]
        if artifact["size"] != expected_size or artifact["sha256"] != expected_sha:
            raise QualificationError(
                f"pinned reranker artifact mismatch for {path}: "
                f"expected size={expected_size} sha256={expected_sha}, "
                f"got size={artifact['size']} sha256={artifact['sha256']}"
            )
        if artifact["receipt_sha256"] != expected_sha:
            raise QualificationError(f"reranker receipt does not pin the catalog SHA-256 for {path}")
    for path, (expected_size, expected_sha) in RERANKER_GENERATED_ARTIFACTS.items():
        artifact = received[path]
        if artifact["size"] != expected_size or artifact["sha256"] != expected_sha:
            raise QualificationError(
                f"generated reranker contract mismatch for {path}: "
                f"expected size={expected_size} sha256={expected_sha}, "
                f"got size={artifact['size']} sha256={artifact['sha256']}"
            )
        if artifact["receipt_sha256"] is not None:
            raise QualificationError(f"generated reranker artifact unexpectedly has a catalog digest: {path}")

    bundle = load_json(root / "antfly_inference_bundle.json")
    if bundle.get("family") != "qwen3_vl_reranker_safetensors_bundle/v1":
        raise QualificationError(f"unexpected reranker bundle family: {bundle.get('family')!r}")
    model = safe_artifact_path(bundle.get("model"))
    if model != "model.safetensors" or model not in received:
        raise QualificationError("reranker bundle does not select the pinned safetensors model")
    evidence.update({"bundle": bundle, "model_path": str(root / model)})
    return evidence


def git_provenance(binary: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"binary": str(binary), "binary_sha256": sha256_file(binary)}
    try:
        repo = subprocess.run(
            ["git", "-C", str(binary.parent), "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        head = subprocess.run(
            ["git", "-C", repo, "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        status_text = subprocess.run(
            ["git", "-C", repo, "status", "--porcelain=v1", "--untracked-files=all"],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        ).stdout
        diff = subprocess.run(
            ["git", "-C", repo, "diff", "--binary", "HEAD"],
            check=True,
            capture_output=True,
            timeout=30,
        ).stdout
        result.update(
            {
                "git_root": repo,
                "git_head": head,
                "git_dirty": bool(status_text),
                "git_status_sha256": hashlib.sha256(status_text.encode()).hexdigest(),
                "git_diff_sha256": hashlib.sha256(diff).hexdigest(),
                "git_status": status_text.splitlines(),
            }
        )
    except (OSError, subprocess.SubprocessError):
        result["git_provenance_unavailable"] = True
    return result


def run_oracle(args: argparse.Namespace, work_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    output = work_dir / "transformers_oracle.json"
    patches = work_dir / "transformers_spatial_patches.f32le"
    command = [
        sys.executable,
        str(args.oracle_script),
        "--model-dir",
        str(args.model_dir),
        "--prompt",
        args.prompt,
        "--image",
        str(args.image),
        "--max-merged-tokens",
        "576",
        "--patch-output",
        str(patches),
        "--output",
        str(output),
    ]
    env = os.environ.copy()
    env.update(
        {
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "HF_HUB_DISABLE_PROGRESS_BARS": "1",
        }
    )
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=args.oracle_timeout_seconds,
            env=env,
            start_new_session=True,
        )
    except subprocess.TimeoutExpired as exc:
        raise QualificationError(f"Transformers oracle timed out after {exc.timeout}s") from exc
    (work_dir / "transformers_oracle.stdout.log").write_text(completed.stdout, encoding="utf-8")
    (work_dir / "transformers_oracle.stderr.log").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise QualificationError(
            f"Transformers oracle exited {completed.returncode}: {completed.stderr.strip()}"
        )
    payload = load_json(output)
    if payload.get("schema") != "antfly.qwen3vl.transformers_oracle.v1":
        raise QualificationError(f"unexpected Transformers oracle schema: {payload.get('schema')}")
    return payload, {"command": command, "output": str(output), "patches": str(patches)}


def parse_vm_stat_swapout_bytes(output: str) -> int:
    page = re.search(r"page size of\s+(\d+)\s+bytes", output)
    swap = re.search(r"^Swapouts:\s*(\d+)\.", output, re.MULTILINE)
    if page is None or swap is None:
        raise QualificationError("vm_stat did not report page size and Swapouts")
    return int(page.group(1)) * int(swap.group(1))


def swapout_bytes() -> int:
    completed = subprocess.run(
        ["/usr/bin/vm_stat"], check=False, capture_output=True, text=True, timeout=10
    )
    if completed.returncode != 0:
        raise QualificationError("vm_stat resource probe failed")
    return parse_vm_stat_swapout_bytes(completed.stdout)


def memory_free_percent() -> int:
    completed = subprocess.run(
        ["/usr/bin/memory_pressure", "-Q"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    match = re.search(r"System-wide memory free percentage:\s*(\d+)%", completed.stdout)
    if completed.returncode != 0 or match is None:
        raise QualificationError("memory_pressure resource probe failed")
    return int(match.group(1))


def process_rss_mib(pid: int) -> float:
    completed = subprocess.run(
        ["/bin/ps", "-o", "rss=", "-p", str(pid)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    value = completed.stdout.strip()
    if completed.returncode != 0 or not value.isdigit():
        raise QualificationError(f"ps RSS probe failed for pid {pid}")
    return int(value) / 1024.0


def terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def metal_command(args: argparse.Namespace, work_dir: Path) -> list[str]:
    # The production CLI accepts repeated --image flags (up to its explicit
    # request limit).  Qualification historically exercised one image, but
    # benchmarks must use this same builder for a multi-image request so their
    # command, budgets, timing artifacts, and resource guard remain identical.
    image_paths = getattr(args, "images", None)
    if image_paths is None:
        image_paths = [args.image]
    if not image_paths:
        raise QualificationError("Metal Qwen3-VL request requires at least one image")

    command = [
        str(args.antfly_bin),
        "generate",
        str(args.model_dir),
        args.prompt,
    ]
    for image_path in image_paths:
        command.extend(("--image", str(image_path)))
    command.extend((
        "--backend",
        "metal",
        "--max-tokens",
        str(args.max_tokens),
        "--temperature",
        "0",
        "--top-k",
        "1",
        "--top-p",
        "1",
        "--repetition-penalty",
        "1",
        "--host-budget-mb",
        str(args.host_budget_mb),
        "--backend-budget-mb",
        str(args.backend_budget_mb),
        "--combined-budget-mb",
        str(args.combined_budget_mb),
        "--kv-budget-mb",
        str(args.kv_budget_mb),
        "--scratch-budget-mb",
        str(args.scratch_budget_mb),
        "--disable-thinking",
        "--print-token-ids",
        "--print-prompt-token-ids",
        "--print-prompt",
        "--print-finish-reason",
        "--print-token-count",
        "--json-timing",
        str(work_dir / "antfly_metal_timing.json"),
        "--qwen3vl-parity-json",
        str(work_dir / "antfly_parity.json"),
        "--qwen3vl-parity-patch-f32le",
        str(work_dir / "antfly_spatial_patches.f32le"),
        "--qwen3vl-parity-logits-f32le",
        str(work_dir / "antfly_prefill_logits.f32le"),
    ))
    return command


def run_resource_monitored(
    command: list[str],
    stdout_path: Path,
    stderr_path: Path,
    *,
    timeout_seconds: float,
    max_rss_mib: float,
    min_free_percent: int,
    max_swap_growth_mib: float,
    sample_interval_seconds: float,
    env: dict[str, str] | None = None,
    label: str,
) -> dict[str, Any]:
    baseline_swap = swapout_bytes()
    initial_free = memory_free_percent()
    max_rss = 0.0
    min_free = initial_free
    sample_count = 0
    started = time.monotonic()
    next_system_probe = started
    violation: str | None = None
    with stdout_path.open("wb") as stdout_stream, stderr_path.open("wb") as stderr_stream:
        process = subprocess.Popen(
            command,
            stdout=stdout_stream,
            stderr=stderr_stream,
            env=env,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                now = time.monotonic()
                if now - started > timeout_seconds:
                    violation = f"{label} exceeded {timeout_seconds}s timeout"
                    break
                try:
                    rss = process_rss_mib(process.pid)
                    max_rss = max(max_rss, rss)
                    sample_count += 1
                    if rss > max_rss_mib:
                        violation = f"{label} RSS {rss:.1f} MiB exceeded {max_rss_mib:.1f} MiB"
                        break
                    if now >= next_system_probe:
                        free = memory_free_percent()
                        min_free = min(min_free, free)
                        swap_growth = max(swapout_bytes() - baseline_swap, 0)
                        if free < min_free_percent:
                            violation = (
                                f"system memory free {free}% fell below {min_free_percent}%"
                            )
                            break
                        if swap_growth > max_swap_growth_mib * 1024 * 1024:
                            violation = (
                                f"swapout growth {swap_growth / 1048576:.1f} MiB exceeded "
                                f"{max_swap_growth_mib:.1f} MiB"
                            )
                            break
                        next_system_probe = now + 1.0
                except QualificationError as exc:
                    if process.poll() is None:
                        violation = f"resource monitoring failed closed: {exc}"
                        break
                time.sleep(sample_interval_seconds)
        finally:
            if violation is not None:
                terminate_process_group(process)
        returncode = process.wait(timeout=5)

    elapsed = time.monotonic() - started
    stdout = stdout_path.read_text(encoding="utf-8", errors="replace")
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace")
    final_swap = swapout_bytes()
    final_free = memory_free_percent()
    min_free = min(min_free, final_free)
    final_swap_growth_mib = max(final_swap - baseline_swap, 0) / 1048576.0
    resources = {
        "elapsed_seconds": elapsed,
        "sample_count": sample_count,
        "max_rss_mib": max_rss,
        "initial_free_percent": initial_free,
        "min_free_percent": min_free,
        "swapout_growth_mib": final_swap_growth_mib,
    }
    if violation is None and final_free < min_free_percent:
        violation = f"system memory free {final_free}% fell below {min_free_percent}%"
    if violation is None and final_swap_growth_mib > max_swap_growth_mib:
        violation = (
            f"swapout growth {final_swap_growth_mib:.1f} MiB exceeded "
            f"{max_swap_growth_mib:.1f} MiB"
        )
    execution = {
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "resources": resources,
    }
    if violation is not None:
        raise ResourceViolation(violation, execution)
    return execution


def run_metal(args: argparse.Namespace, work_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    command = metal_command(args, work_dir)
    env_overrides = {"ANTFLY_INFERENCE_JSON_TOKEN_IDS": "1"}
    if args.vision_trace_layer is not None:
        env_overrides["ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER"] = str(
            args.vision_trace_layer
        )
    env = os.environ.copy()
    env.update(env_overrides)
    execution = run_resource_monitored(
        command,
        work_dir / "antfly_metal.stdout.log",
        work_dir / "antfly_metal.stderr.log",
        timeout_seconds=args.timeout_seconds,
        max_rss_mib=args.max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        sample_interval_seconds=args.sample_interval_seconds,
        env=env,
        label="Antfly Metal",
    )
    if execution["returncode"] != 0:
        raise QualificationError(
            f"Antfly Metal exited {execution['returncode']}: "
            f"{execution['stderr'].strip()[-2000:]}"
        )
    parity = load_json(work_dir / "antfly_parity.json")
    timing = load_json(work_dir / "antfly_metal_timing.json")
    return parity, timing, {
        "command": command,
        "environment": env_overrides,
        **execution,
        "patches": str(work_dir / "antfly_spatial_patches.f32le"),
        "logits": str(work_dir / "antfly_prefill_logits.f32le"),
    }


def run_weights_oracle(args: argparse.Namespace, work_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    output = work_dir / "transformers_weights_oracle.json"
    logits = work_dir / "transformers_bf16_prefill_logits.f32le"
    command = [
        sys.executable,
        str(args.weights_oracle_script),
        "--weights-dir",
        str(args.weights_dir),
        "--processor-dir",
        str(args.model_dir),
        "--image",
        str(args.image),
        "--prompt",
        args.prompt,
        "--device",
        args.weights_device,
        "--dtype",
        args.weights_dtype,
        "--attn-implementation",
        args.weights_attn_implementation,
        "--load-strategy",
        args.weights_load_strategy,
        "--logit-transfer",
        args.weights_logit_transfer,
        "--warmup-runs",
        "0",
        "--timed-runs",
        "1",
        "--max-merged-tokens",
        "576",
        "--logits-output",
        str(logits),
        "--output",
        str(output),
    ]
    env = os.environ.copy()
    env.update(
        {
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "HF_HUB_DISABLE_PROGRESS_BARS": "1",
        }
    )
    execution = run_resource_monitored(
        command,
        work_dir / "transformers_weights_oracle.stdout.log",
        work_dir / "transformers_weights_oracle.stderr.log",
        timeout_seconds=args.weights_timeout_seconds,
        max_rss_mib=args.weights_max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        sample_interval_seconds=args.sample_interval_seconds,
        env=env,
        label="Transformers BF16 oracle",
    )
    if execution["returncode"] != 0:
        raise QualificationError(
            f"Transformers BF16 oracle exited {execution['returncode']}: "
            f"{execution['stderr'].strip()[-2000:]}"
        )
    payload = load_json(output)
    if payload.get("schema") != "antfly.qwen3vl.transformers_weights_oracle.v1":
        raise QualificationError(f"unexpected weights oracle schema: {payload.get('schema')}")
    return payload, {
        "command": command,
        **execution,
        "output": str(output),
        "logits": str(logits),
    }


def patch_metrics(reference_path: Path, actual_path: Path) -> dict[str, float | int | bool]:
    import numpy as np

    reference = np.fromfile(reference_path, dtype="<f4")
    actual = np.fromfile(actual_path, dtype="<f4")
    if reference.size == 0 or actual.size != reference.size:
        return {
            "size_match": False,
            "reference_value_count": int(reference.size),
            "actual_value_count": int(actual.size),
        }
    difference = np.abs(actual.astype(np.float64) - reference.astype(np.float64))
    return {
        "size_match": True,
        "value_count": int(reference.size),
        "mean_abs": float(difference.mean()),
        "rmse": float(math.sqrt(float(np.square(difference).mean()))),
        "p99_abs": float(np.quantile(difference, 0.99, method="higher")),
        "max_abs": float(difference.max()),
    }


def logit_metrics(reference_path: Path, actual_path: Path, top_k: int = 10) -> dict[str, Any]:
    import numpy as np

    reference = np.fromfile(reference_path, dtype="<f4").astype(np.float64)
    actual = np.fromfile(actual_path, dtype="<f4").astype(np.float64)
    if reference.size == 0 or actual.size != reference.size:
        return {
            "size_match": False,
            "reference_value_count": int(reference.size),
            "actual_value_count": int(actual.size),
        }
    finite = bool(np.isfinite(reference).all() and np.isfinite(actual).all())
    if not finite:
        return {"size_match": True, "finite": False, "value_count": int(reference.size)}
    difference = actual - reference
    reference_order = np.argsort(reference)[::-1]
    actual_order = np.argsort(actual)[::-1]
    reference_top = [int(item) for item in reference_order[:top_k]]
    actual_top = [int(item) for item in actual_order[:top_k]]
    denominator = float(np.linalg.norm(reference) * np.linalg.norm(actual))
    return {
        "size_match": True,
        "finite": True,
        "value_count": int(reference.size),
        "reference_argmax": reference_top[0],
        "actual_argmax": actual_top[0],
        "reference_top_k": reference_top,
        "actual_top_k": actual_top,
        "top_k_overlap": len(set(reference_top) & set(actual_top)),
        "reference_top1_margin": float(reference[reference_order[0]] - reference[reference_order[1]]),
        "actual_top1_margin": float(actual[actual_order[0]] - actual[actual_order[1]]),
        "mean_abs": float(np.abs(difference).mean()),
        "rmse": float(math.sqrt(float(np.square(difference).mean()))),
        "max_abs": float(np.abs(difference).max()),
        "cosine_similarity": float(np.dot(reference, actual) / denominator) if denominator else 0.0,
        "pearson_correlation": float(np.corrcoef(reference, actual)[0, 1]),
    }


def logit_quality_pass(metrics: dict[str, Any]) -> bool:
    return (
        metrics.get("size_match") is True
        and metrics.get("finite") is True
        and metrics.get("cosine_similarity", -math.inf)
        >= LOGIT_LIMITS["min_cosine_similarity"]
        and metrics.get("pearson_correlation", -math.inf)
        >= LOGIT_LIMITS["min_pearson_correlation"]
        and metrics.get("mean_abs", math.inf) <= LOGIT_LIMITS["max_mean_abs"]
        and metrics.get("rmse", math.inf) <= LOGIT_LIMITS["max_rmse"]
        and metrics.get("max_abs", math.inf) <= LOGIT_LIMITS["max_max_abs"]
        and metrics.get("top_k_overlap", -1) >= LOGIT_LIMITS["min_top_10_overlap"]
    )


def metal_determinism_metrics(
    samples: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]],
) -> dict[str, Any]:
    """Compare repeated identical Metal requests at architecture boundaries."""

    evidence: list[dict[str, Any]] = []
    structural_fields = (
        "placeholder_token_ids",
        "expanded_token_ids",
        "mrope_position_ids",
        "visual_token_count",
        "deepstack_layer_count",
        "mrope_position_delta",
    )
    for index, (parity, timing, run) in enumerate(samples, start=1):
        _, _, generated_ids = parse_prompt_output(run["stdout"] + "\n" + run["stderr"])
        logits_path = Path(run["logits"])
        images = parity.get("images")
        positioned_hashes = (
            [image.get("positioned_embedding_f32le_sha256") for image in images]
            if isinstance(images, list) and all(isinstance(image, dict) for image in images)
            else None
        )
        vision_traces = (
            [
                {
                    "layer": image.get("vision_trace_layer"),
                    "f32le_sha256": image.get("vision_trace_f32le_sha256"),
                }
                for image in images
            ]
            if isinstance(images, list) and all(isinstance(image, dict) for image in images)
            else None
        )
        structural_images = (
            [
                {
                    key: image.get(key)
                    for key in (
                        "source_width",
                        "source_height",
                        "resized_width",
                        "resized_height",
                        "grid_thw",
                        "patch_rows",
                        "patch_columns",
                        "spatial_patch_f32le_sha256",
                    )
                }
                for image in images
            ]
            if isinstance(images, list) and all(isinstance(image, dict) for image in images)
            else None
        )
        evidence.append(
            {
                "run": index,
                "positioned_embedding_f32le_sha256": positioned_hashes,
                "vision_trace": vision_traces,
                "projected_embedding_value_count": parity.get(
                    "projected_embedding_value_count"
                ),
                "projected_embedding_f32le_sha256": parity.get(
                    "projected_embedding_f32le_sha256"
                ),
                "deepstack_embedding_value_count": parity.get(
                    "deepstack_embedding_value_count"
                ),
                "deepstack_embedding_f32le_sha256": parity.get(
                    "deepstack_embedding_f32le_sha256"
                ),
                "deepstack_taps": parity.get("deepstack_taps"),
                "prefill_logits_f32le_sha256": sha256_file(logits_path),
                "prefill_logits_size": logits_path.stat().st_size,
                "generated_token_ids": generated_ids,
                "timing_token_ids": timing.get("token_ids"),
                "structural_sha256": hashlib.sha256(
                    json.dumps(
                        {
                            **{field: parity.get(field) for field in structural_fields},
                            "images": structural_images,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest(),
            }
        )

    def exact(field: str) -> bool:
        values = [item.get(field) for item in evidence]
        return len(values) >= 2 and values[0] is not None and all(
            value == values[0] for value in values[1:]
        )

    digest_pattern = re.compile(r"[0-9a-f]{64}")
    projector_digests_valid = all(
        isinstance(item["projected_embedding_f32le_sha256"], str)
        and digest_pattern.fullmatch(item["projected_embedding_f32le_sha256"])
        and isinstance(item["projected_embedding_value_count"], int)
        and item["projected_embedding_value_count"] > 0
        for item in evidence
    )
    deepstack_digests_valid = all(
        isinstance(item["deepstack_embedding_f32le_sha256"], str)
        and digest_pattern.fullmatch(item["deepstack_embedding_f32le_sha256"])
        and isinstance(item["deepstack_embedding_value_count"], int)
        and item["deepstack_embedding_value_count"] > 0
        for item in evidence
    )
    return {
        "run_count": len(evidence),
        "samples": evidence,
        "structure_exact": exact("structural_sha256"),
        "positioned_embeddings_exact": exact("positioned_embedding_f32le_sha256"),
        "vision_trace_exact": exact("vision_trace"),
        "projector_exact": projector_digests_valid
        and exact("projected_embedding_value_count")
        and exact("projected_embedding_f32le_sha256"),
        "deepstack_exact": deepstack_digests_valid
        and exact("deepstack_embedding_value_count")
        and exact("deepstack_embedding_f32le_sha256")
        and exact("deepstack_taps"),
        "prefill_logits_exact": exact("prefill_logits_size")
        and exact("prefill_logits_f32le_sha256"),
        "generated_tokens_exact": exact("generated_token_ids")
        and exact("timing_token_ids"),
    }


def parse_prompt_output(stdout: str) -> tuple[str, list[int], list[int]]:
    match = re.search(r"(?:^|\n)prompt:\n(.*?)\nprompt_token_ids:([^\n]*)", stdout, re.DOTALL)
    generated = re.search(r"(?:^|\n)token_ids:([^\n]*)", stdout)
    if match is None or generated is None:
        raise QualificationError("Antfly output lacks prompt or token ID diagnostics")
    try:
        prompt_ids = [int(item) for item in match.group(2).split()]
        generated_ids = [int(item) for item in generated.group(1).split()]
    except ValueError as exc:
        raise QualificationError("Antfly emitted invalid token ID diagnostics") from exc
    return match.group(1), prompt_ids, generated_ids


def add_gate(gates: dict[str, Any], name: str, passed: bool, details: object) -> None:
    gates[name] = {"pass": bool(passed), "details": details}


def fallback_counters_are_zero(timing: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    metal = timing.get("metal")
    if not isinstance(metal, dict):
        return False, {"error": "missing Metal timing object"}
    command_fallback = metal.get("runtime_command_operators", {}).get("fallback")
    quant = metal.get("quant_kernel_plan", {})
    quant_keys = (
        "unsupported_routes",
        "fast_path_misses",
        "generated_artifact_missing",
        "generated_runtime_not_wired",
        "unsupported",
        "top_fallback_count",
    )
    quant_values = {key: quant.get(key) for key in quant_keys}
    frame = metal.get("frame_fallbacks", {})
    frame_values = {
        key: value
        for key, value in frame.items()
        if key.endswith("fallback") or key.endswith("fail") or key.endswith("missing_ple")
    }
    values = [command_fallback, *quant_values.values(), *frame_values.values()]
    return all(value == 0 for value in values), {
        "runtime_command_fallback": command_fallback,
        "quant_kernel_plan": quant_values,
        "frame_fallbacks": frame_values,
    }


def parity_gates(
    args: argparse.Namespace,
    oracle: dict[str, Any],
    parity: dict[str, Any],
    timing: dict[str, Any],
    run: dict[str, Any],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    gates: dict[str, Any] = {}
    diagnostic_output = run["stdout"] + "\n" + run["stderr"]
    rendered, prompt_ids, generated_ids = parse_prompt_output(diagnostic_output)
    add_gate(gates, "rendered_prompt_exact", rendered == oracle.get("rendered_prompt"), {})
    expected_placeholder = oracle.get("placeholder_token_ids")
    add_gate(
        gates,
        "placeholder_token_ids_exact",
        prompt_ids == expected_placeholder == parity.get("placeholder_token_ids"),
        {"token_count": len(prompt_ids)},
    )
    expected_expanded = oracle.get("input_ids")
    add_gate(
        gates,
        "expanded_token_ids_exact",
        parity.get("expanded_token_ids") == expected_expanded,
        {"token_count": len(expected_expanded or [])},
    )
    oracle_mrope = oracle.get("mrope", {})
    add_gate(
        gates,
        "mrope_position_ids_exact",
        parity.get("mrope_position_ids") == oracle_mrope.get("position_ids"),
        {"value_count": len(oracle_mrope.get("position_ids", []))},
    )
    deltas = oracle_mrope.get("position_deltas")
    add_gate(
        gates,
        "mrope_position_delta_exact",
        isinstance(deltas, list)
        and len(deltas) == 1
        and parity.get("mrope_position_delta") == deltas[0],
        {"transformers": deltas, "antfly": parity.get("mrope_position_delta")},
    )
    oracle_grid = oracle.get("image", {}).get("grid_thw")
    antfly_images = parity.get("images")
    antfly_grid = (
        [image.get("grid_thw") for image in antfly_images]
        if isinstance(antfly_images, list) and all(isinstance(image, dict) for image in antfly_images)
        else None
    )
    add_gate(
        gates,
        "image_grid_exact",
        oracle_grid == antfly_grid,
        {"transformers": oracle_grid, "antfly": antfly_grid},
    )
    oracle_resized = oracle.get("image", {}).get("resized_sizes")
    antfly_resized = (
        [[image.get("resized_width"), image.get("resized_height")] for image in antfly_images]
        if isinstance(antfly_images, list) and all(isinstance(image, dict) for image in antfly_images)
        else None
    )
    add_gate(
        gates,
        "image_resized_geometry_exact",
        oracle_resized == antfly_resized,
        {"transformers": oracle_resized, "antfly": antfly_resized},
    )
    architecture = oracle.get("architecture", {})
    add_gate(
        gates,
        "visual_token_count_exact",
        parity.get("visual_token_count") == architecture.get("visual_token_count"),
        {
            "transformers": architecture.get("visual_token_count"),
            "antfly": parity.get("visual_token_count"),
        },
    )
    expected_deepstack = len(architecture.get("deepstack_visual_indexes", []))
    add_gate(
        gates,
        "deepstack_layer_count_exact",
        parity.get("deepstack_layer_count") == expected_deepstack,
        {"transformers": expected_deepstack, "antfly": parity.get("deepstack_layer_count")},
    )
    text_hidden_size = architecture.get("text_hidden_size")
    expected_projected_values = (
        architecture.get("visual_token_count") * text_hidden_size
        if isinstance(architecture.get("visual_token_count"), int)
        and isinstance(text_hidden_size, int)
        else None
    )
    projected_values = parity.get("projected_embedding_value_count")
    deepstack_values = parity.get("deepstack_embedding_value_count")
    expected_deepstack_values = (
        expected_projected_values * expected_deepstack
        if isinstance(expected_projected_values, int)
        else None
    )
    add_gate(
        gates,
        "projector_output_shape_exact",
        projected_values == expected_projected_values,
        {"expected": expected_projected_values, "actual": projected_values},
    )
    add_gate(
        gates,
        "deepstack_output_shape_exact",
        deepstack_values == expected_deepstack_values,
        {"expected": expected_deepstack_values, "actual": deepstack_values},
    )
    size_match = metrics.get("size_match") is True
    add_gate(gates, "preprocess_patch_shape_exact", size_match, metrics)
    patch_pass = size_match and all(metrics[name] <= limit for name, limit in PATCH_LIMITS.items())
    add_gate(gates, "preprocess_numeric_tolerance", patch_pass, {"metrics": metrics, "limits": PATCH_LIMITS})
    add_gate(
        gates,
        "metal_backend_exact",
        timing.get("backend") == "metal" and isinstance(timing.get("metal"), dict),
        {"backend": timing.get("backend"), "device": timing.get("metal", {}).get("device")},
    )
    fallback_pass, fallback_details = fallback_counters_are_zero(timing)
    add_gate(gates, "metal_fallback_counters_zero", fallback_pass, fallback_details)
    forbidden = FORBIDDEN_RUNTIME_OUTPUT.findall(run["stdout"] + "\n" + run["stderr"])
    add_gate(gates, "runtime_output_clean", not forbidden, {"matches": forbidden})
    resources = run["resources"]
    add_gate(
        gates,
        "rss_within_limit",
        resources["max_rss_mib"] <= args.max_rss_mib,
        {"actual_mib": resources["max_rss_mib"], "limit_mib": args.max_rss_mib},
    )
    add_gate(
        gates,
        "swapout_growth_within_limit",
        resources["swapout_growth_mib"] <= args.max_swap_growth_mib,
        {"actual_mib": resources["swapout_growth_mib"], "limit_mib": args.max_swap_growth_mib},
    )
    add_gate(
        gates,
        "memory_pressure_within_limit",
        resources["min_free_percent"] >= args.min_free_percent,
        {"actual_percent": resources["min_free_percent"], "minimum_percent": args.min_free_percent},
    )
    token_count_ok = len(generated_ids) == args.max_tokens == timing.get("tokens")
    add_gate(
        gates,
        "bounded_generation_completed",
        token_count_ok and timing.get("token_ids") == generated_ids,
        {"token_ids": generated_ids, "max_tokens": args.max_tokens},
    )
    if args.expected_token_id:
        add_gate(
            gates,
            "expected_token_ids_exact",
            generated_ids == args.expected_token_id,
            {"expected": args.expected_token_id, "actual": generated_ids},
        )
    return gates


def validate_args(args: argparse.Namespace) -> None:
    if platform.system() != "Darwin":
        raise QualificationError("real Qwen3-VL Metal qualification requires macOS")
    args.model_dir = args.model_dir.resolve(strict=True)
    args.image = args.image.resolve(strict=True)
    args.antfly_bin = args.antfly_bin.resolve(strict=True)
    args.oracle_script = args.oracle_script.resolve(strict=True)
    args.weights_oracle_script = args.weights_oracle_script.resolve(strict=True)
    if args.weights_dir is not None:
        args.weights_dir = args.weights_dir.resolve(strict=True)
    if not args.antfly_bin.is_file() or not os.access(args.antfly_bin, os.X_OK):
        raise QualificationError(f"Antfly binary is not executable: {args.antfly_bin}")
    if not 1 <= args.max_tokens <= 8:
        raise QualificationError("max tokens must be in [1, 8]")
    if args.expected_token_id and len(args.expected_token_id) != args.max_tokens:
        raise QualificationError("expected token count must equal --max-tokens")
    if not 0 < args.sample_interval_seconds <= 1:
        raise QualificationError("sample interval must be in (0, 1] seconds")
    if args.combined_budget_mb > args.max_rss_mib:
        raise QualificationError("combined budget must not exceed the RSS watchdog")
    if args.weights_dir is not None and args.weights_max_rss_mib <= args.max_rss_mib:
        raise QualificationError("BF16 oracle RSS limit must exceed the Metal RSS limit")
    if not 2 <= args.metal_repeat_count <= 5:
        raise QualificationError("metal repeat count must be in [2, 5]")
    if args.vision_trace_layer is not None and not 0 <= args.vision_trace_layer < 24:
        raise QualificationError("2B vision trace layer must be in [0, 23]")


def parse_args(argv: list[str]) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--antfly-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--oracle-script", type=Path, default=here / "transformers_oracle.py")
    parser.add_argument("--weights-dir", type=Path)
    parser.add_argument(
        "--weights-oracle-script",
        type=Path,
        default=here / "transformers_weights_oracle.py",
    )
    parser.add_argument("--weights-device", choices=("cpu", "mps"), default="cpu")
    parser.add_argument(
        "--weights-dtype",
        choices=("bfloat16", "float16"),
        default="bfloat16",
    )
    parser.add_argument(
        "--weights-attn-implementation",
        choices=("eager", "sdpa"),
        default="eager",
    )
    parser.add_argument(
        "--weights-load-strategy",
        choices=("device_map", "cpu_then_move"),
        default="device_map",
    )
    parser.add_argument(
        "--weights-logit-transfer",
        choices=("view", "clone"),
        default="view",
    )
    parser.add_argument("--weights-timeout-seconds", type=float, default=300.0)
    parser.add_argument("--weights-max-rss-mib", type=float, default=10240.0)
    parser.add_argument("--prompt", default="Describe the image briefly.")
    parser.add_argument("--max-tokens", type=int, default=1)
    parser.add_argument("--vision-trace-layer", type=int)
    parser.add_argument(
        "--metal-repeat-count",
        type=int,
        default=2,
        help="identical serial Metal requests required for bitwise determinism",
    )
    parser.add_argument("--expected-token-id", type=int, action="append")
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--oracle-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.25)
    parser.add_argument("--max-rss-mib", type=float, default=4096.0)
    parser.add_argument("--min-free-percent", type=int, default=10)
    parser.add_argument("--max-swap-growth-mib", type=float, default=0.0)
    parser.add_argument("--host-budget-mb", type=int, default=2048)
    parser.add_argument("--backend-budget-mb", type=int, default=3072)
    parser.add_argument("--combined-budget-mb", type=int, default=4096)
    parser.add_argument("--kv-budget-mb", type=int, default=256)
    parser.add_argument("--scratch-budget-mb", type=int, default=768)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "pass": False,
        "release_ready": False,
        "scope": "preprocess_and_bounded_single_request_metal_acceptance",
        "created_unix_seconds": int(time.time()),
        "host": {"platform": platform.platform(), "python": platform.python_version()},
    }
    try:
        validate_args(args)
        output = args.output.resolve()
        work_dir = (args.work_dir or output.with_suffix(".artifacts")).resolve()
        work_dir.mkdir(parents=True, exist_ok=False)
        report["work_dir"] = str(work_dir)
        report["model"] = validate_managed_bundle(args.model_dir)
        report["image"] = {
            "path": str(args.image),
            "size": args.image.stat().st_size,
            "sha256": sha256_file(args.image),
        }
        report["runtime_build"] = git_provenance(args.antfly_bin)
        oracle, oracle_run = run_oracle(args, work_dir)
        report["transformers_oracle"] = oracle_run
        report["transformers_runtime"] = oracle.get("runtime")
        weights_payload: dict[str, Any] | None = None
        weights_run: dict[str, Any] | None = None
        if args.weights_dir is not None:
            report["scope"] = "bf16_logits_preprocess_and_bounded_single_request_metal_acceptance"
            weights_payload, weights_run = run_weights_oracle(args, work_dir)
            report["transformers_weights_oracle"] = {
                "command": weights_run["command"],
                "output": weights_run["output"],
                "stdout_path": weights_run["stdout_path"],
                "stderr_path": weights_run["stderr_path"],
                "resources": weights_run["resources"],
                "model": weights_payload.get("model"),
                "runtime": weights_payload.get("runtime"),
                "timing_seconds": weights_payload.get("timing_seconds"),
            }
        parity, timing, metal_run = run_metal(args, work_dir)
        metal_samples = [(parity, timing, metal_run)]
        repeat_summaries: list[dict[str, Any]] = []
        repeat_gate_failures: list[dict[str, Any]] = []
        for repeat_index in range(2, args.metal_repeat_count + 1):
            repeat_dir = work_dir / f"metal_repeat_{repeat_index}"
            repeat_dir.mkdir()
            repeat_parity, repeat_timing, repeat_run = run_metal(args, repeat_dir)
            metal_samples.append((repeat_parity, repeat_timing, repeat_run))
            repeat_metrics = patch_metrics(
                Path(oracle_run["patches"]), Path(repeat_run["patches"])
            )
            repeat_gates = parity_gates(
                args,
                oracle,
                repeat_parity,
                repeat_timing,
                repeat_run,
                repeat_metrics,
            )
            failed = sorted(name for name, gate in repeat_gates.items() if not gate["pass"])
            if failed:
                repeat_gate_failures.append({"run": repeat_index, "failed_gates": failed})
            repeat_summaries.append(
                {
                    "run": repeat_index,
                    "command": repeat_run["command"],
                    "stdout_path": repeat_run["stdout_path"],
                    "stderr_path": repeat_run["stderr_path"],
                    "parity_path": str(repeat_dir / "antfly_parity.json"),
                    "logits_path": repeat_run["logits"],
                    "resources": repeat_run["resources"],
                    "timing": repeat_timing,
                    "patch_metrics": repeat_metrics,
                    "failed_gates": failed,
                }
            )
        metrics = patch_metrics(
            Path(oracle_run["patches"]),
            Path(metal_run["patches"]),
        )
        gates = parity_gates(args, oracle, parity, timing, metal_run, metrics)
        determinism = metal_determinism_metrics(metal_samples)
        add_gate(
            gates,
            "all_metal_repeats_pass_acceptance",
            not repeat_gate_failures,
            {"run_count": args.metal_repeat_count, "failures": repeat_gate_failures},
        )
        add_gate(
            gates,
            "metal_structure_bitwise_deterministic",
            determinism["structure_exact"],
            determinism,
        )
        add_gate(
            gates,
            "metal_positioned_embeddings_bitwise_deterministic",
            determinism["positioned_embeddings_exact"],
            determinism,
        )
        if args.vision_trace_layer is not None:
            add_gate(
                gates,
                "metal_vision_trace_bitwise_deterministic",
                determinism["vision_trace_exact"],
                {
                    "layer": args.vision_trace_layer,
                    "samples": determinism["samples"],
                },
            )
        add_gate(
            gates,
            "metal_projector_bitwise_deterministic",
            determinism["projector_exact"],
            determinism,
        )
        add_gate(
            gates,
            "metal_deepstack_bitwise_deterministic",
            determinism["deepstack_exact"],
            determinism,
        )
        add_gate(
            gates,
            "metal_prefill_logits_bitwise_deterministic",
            determinism["prefill_logits_exact"],
            determinism,
        )
        add_gate(
            gates,
            "metal_generated_tokens_deterministic",
            determinism["generated_tokens_exact"],
            determinism,
        )
        logits: dict[str, Any] | None = None
        if weights_run is not None and weights_payload is not None:
            logits = logit_metrics(Path(weights_run["logits"]), Path(metal_run["logits"]))
            add_gate(gates, "bf16_logit_vector_shape_exact", logits.get("size_match") is True, logits)
            add_gate(gates, "bf16_and_q4_logits_finite", logits.get("finite") is True, logits)
            add_gate(
                gates,
                "bf16_q4_logit_quality_within_limits",
                logit_quality_pass(logits),
                {"metrics": logits, "limits": LOGIT_LIMITS},
            )
            generated = timing.get("token_ids")
            argmax_exact = (
                logits.get("reference_argmax") == logits.get("actual_argmax")
                and isinstance(generated, list)
                and bool(generated)
                and logits.get("actual_argmax") == generated[0]
                and logits.get("reference_argmax")
                == weights_payload.get("last_logits", {}).get("argmax_token_id")
            )
            add_gate(
                gates,
                "bf16_q4_greedy_argmax_exact",
                argmax_exact,
                {
                    "transformers": logits.get("reference_argmax"),
                    "antfly": logits.get("actual_argmax"),
                    "generated": generated,
                },
            )
        report["metal_run"] = {
            "command": metal_run["command"],
            "environment": metal_run["environment"],
            "stdout_path": metal_run["stdout_path"],
            "stderr_path": metal_run["stderr_path"],
            "resources": metal_run["resources"],
            "timing": timing,
        }
        report["metal_repeats"] = repeat_summaries
        report["parity"] = {
            "transformers_oracle_path": oracle_run["output"],
            "antfly_parity_path": str(work_dir / "antfly_parity.json"),
            "patch_metrics": metrics,
            "logit_metrics": logits,
            "metal_determinism": determinism,
        }
        report["gates"] = gates
        report["pass"] = all(gate["pass"] for gate in gates.values())
        if not report["pass"]:
            report["failure"] = "one or more qualification gates failed"
    except (QualificationError, OSError, subprocess.SubprocessError, ValueError) as exc:
        report["failure"] = str(exc)
    write_json_atomic(args.output.resolve(), report)
    print(json.dumps({"pass": report["pass"], "report": str(args.output.resolve())}))
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
