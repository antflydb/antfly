#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Collect source/model-bound native Metal successive-owner diagnostics.

A complete twenty-cycle report supplies one release gate. This collector does
not grant corpus quality, serving, performance, soak, or production authority.
"""
import argparse
import json
import os
from pathlib import Path
import sys

import context_release_build_adapter as builds
from metal_checkpoint_cycle_evidence import validate_report
import native_repeat_campaign_resources as resources
import qualify_metal_native_checkpoint as campaign
from qualify_context_management import git_provenance
from record_context_release_build import source_snapshot

REPO = Path(__file__).resolve().parents[5]
FILTER = "context Metal checkpoint successive installed owner model qualification"


def validate_source_build(repo, build):
    head, source = source_snapshot(repo)
    campaign.require(head == build["head"] and source == build["source"],
                     "cycle executable does not cover current source")
    return git_provenance(repo)


def validate_input(value):
    campaign.require(type(value) is dict and value.get("schema") ==
                     "antfly.metal-checkpoint-cycles.input.v1" and
                     value.get("model") in {
                         "google/gemma-4-E2B-it-qat-q4_0-gguf",
                         "google/gemma-4-E4B-it-qat-q4_0-gguf"}, "invalid cycle input")
    request = value.get("request")
    campaign.require(type(request) is dict and request.get("encoding") in
                     {"f16", "fp8", "rotated4"}, "invalid cycle encoding")
    bounds = {"cycles": (1, 20), "append_tokens": (1, 2048), "answer_tokens": (1, 32)}
    for key, (minimum, maximum) in bounds.items():
        campaign.require(type(request.get(key)) is int and minimum <= request[key] <= maximum,
                         "invalid cycle " + key)
    initial = value.get("expected_prompt_tokens")
    campaign.require(type(initial) is int and initial >= 2 and
                     initial + request["cycles"] *
                     (request["append_tokens"] + request["answer_tokens"]) <= 32768 - 512,
                     "cycle logical capacity exceeds query-tail reserve")
    campaign.require(type(value.get("messages")) is list and value["messages"],
                     "cycle messages are missing")


def select_artifact(artifacts, path):
    campaign.require(isinstance(path, str) and path.endswith(".gguf"),
                     "cycle model artifact must be an explicit GGUF path")
    selected = [row for row in artifacts if row["path"] == path]
    campaign.require(len(selected) == 1, "main GGUF artifact must match exactly one manifest entry")
    return selected[0]


def collect(args):
    repo = args.root.resolve(strict=True)
    build = builds.validate(args.build_receipt)
    source = validate_source_build(repo, build)
    input_path = args.input.resolve(strict=True)
    data = json.loads(input_path.read_bytes())
    validate_input(data)
    model_dir = args.model_directory.resolve(strict=True)
    models_root = model_dir.parent.parent
    campaign.checked_model_registry(models_root, model_dir, data["model"])
    identity_path = args.identity_receipt.resolve(strict=True)
    metadata = json.loads(identity_path.read_bytes())
    refs = {}
    for size in ("8k", "32k"):
        path = identity_path.parent / metadata["requests"]["request_" + size]["file"]
        refs[size] = {"path": str(path.resolve(strict=True)), "sha256": campaign.sha(path)}
    identity = campaign.identity_receipt_contract(identity_path, data["model"], refs)
    manifest = model_dir / ".antfly-download-complete.json"
    artifacts = json.loads(manifest.read_bytes())["artifacts"]
    artifact = select_artifact(artifacts, args.artifact)
    campaign.checked_model_artifact(model_dir, artifact["path"], artifact["sha256"])
    output = args.output.absolute()
    campaign.require(not output.is_symlink(), "symlinked cycle output")
    output.mkdir(parents=True, exist_ok=False)
    limits = {"timeout_seconds": 7200, "max_combined_rss_mib": 8192,
              "min_free_percent": 15, "max_swap_growth_mib": 0,
              "min_disk_free_mib": 4096, "load_preflight_free_percent": 76}
    spec = {
        "schema": "antfly.metal-native-cycles.spec.v1", "diagnostic_only": True,
        "production_ready": False, "source": source,
        "build_receipt": {"path": str(build["receipt_path"]), "sha256": build["receipt_sha256"]},
        "source_archive_sha256": build["source_archive_sha256"],
        "binary": {"path": str(build["binary_path"]), "sha256": build["binary_sha256"]},
        "input": {"path": str(input_path), "sha256": campaign.sha(input_path)},
        "runner_sha256": campaign.sha(__file__),
        "model_directory": str(model_dir), "artifact": artifact,
        "model_manifest_sha256": campaign.sha(manifest),
        "identity_receipt": {"path": str(identity_path), "sha256": campaign.sha(identity_path)},
        "performance_requests": refs, "loaded_identity": identity,
        "request": data["request"], "limits": limits,
    }
    campaign.write_new(output / "spec.json", spec)
    campaign.headroom(output, "native-cycles")
    saved = os.environ.copy()
    os.environ.clear()
    os.environ.update({key: value for key, value in saved.items()
                       if not key.startswith(campaign.RUNTIME_PREFIXES)})
    os.environ.update({
        "ANTFLY_METAL_CHECKPOINT_MODELS_ROOT": str(models_root),
        "ANTFLY_METAL_CHECKPOINT_MODEL": data["model"],
        "ANTFLY_METAL_CHECKPOINT_CYCLE_INPUT": str(input_path),
        "ANTFLY_METAL_CHECKPOINT_CYCLE_OUTPUT": str(output / "native-report.json"),
    })
    try:
        guard = resources.run_guarded_combined(
            [str(build["binary_path"]), "--test-filter", FILTER], output / "resource.json",
            timeout=limits["timeout_seconds"], max_rss_mib=limits["max_combined_rss_mib"],
            min_free_percent=limits["min_free_percent"], max_swap_growth_mib=0,
            min_disk_free_mib=limits["min_disk_free_mib"])
    finally:
        os.environ.clear()
        os.environ.update(saved)
    campaign.require(guard.get("pass") is True and guard.get("returncode") == 0 and
                     guard.get("swapout_growth_mib") == 0 and
                     guard.get("remaining_child_group_live_processes") == 0,
                     "native cycle resource/execution failure")
    campaign.require(validate_source_build(repo, build) == source,
                     "source changed during native cycles")
    for ref in (spec["input"], spec["binary"], spec["build_receipt"],
                spec["identity_receipt"], *spec["performance_requests"].values()):
        campaign.checked_ref(ref, "cycle bound input")
    campaign.require(campaign.sha(manifest) == spec["model_manifest_sha256"] and
                     campaign.sha(__file__) == spec["runner_sha256"], "cycle producer inputs changed")
    campaign.checked_model_artifact(model_dir, artifact["path"], artifact["sha256"])
    value = json.loads((output / "native-report.json").read_bytes())
    summary = validate_report(value, data, input_sha256=spec["input"]["sha256"],
                              model_directory=model_dir, loaded_identity=identity)
    report = {
        "schema": "antfly.metal-native-cycles.seal.v1", "passed": True,
        "diagnostic_only": True, "production_ready": False,
        "twenty_cycle_gate_completed": summary["completed_cycles"] == 20,
        "summary": summary,
        "spec_sha256": campaign.sha(output / "spec.json"),
        "resource_sha256": campaign.sha(output / "resource.json"),
        "native_report_sha256": campaign.sha(output / "native-report.json"),
        "stdout_sha256": campaign.sha(output / "resource.stdout"),
        "stderr_sha256": campaign.sha(output / "resource.stderr"),
    }
    campaign.write_new(output / "seal.json", report)
    print(json.dumps(report, indent=2))


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--root", type=Path, default=REPO)
    result.add_argument("--build-receipt", type=Path, required=True)
    result.add_argument("--input", type=Path, required=True)
    result.add_argument("--model-directory", type=Path, required=True)
    result.add_argument("--artifact", required=True)
    result.add_argument("--identity-receipt", type=Path, required=True)
    result.add_argument("--output", type=Path, required=True)
    return result


if __name__ == "__main__":
    collect(parser().parse_args())
