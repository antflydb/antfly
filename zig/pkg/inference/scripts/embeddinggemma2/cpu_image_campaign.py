#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run scoped CPU image production qualification against one owned executable."""

import argparse
import contextlib
import json
import os
import signal
import socket
import subprocess
import time
from pathlib import Path

import cpu_http_qualification as common
import cpu_image_qualification as image
import reference


@contextlib.contextmanager
def server(args):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    command = [
        str(args.binary.resolve()),
        "run",
        "--models-dir",
        str(args.model_dir.parent.resolve()),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--preload-model",
        f"embedder:native:{args.model_dir.name}",
        "--max-concurrent-requests",
        "4",
        "--max-work-units",
        "64",
        "--host-budget-mb",
        "12288",
        "--backend-budget-mb",
        "1024",
        "--combined-budget-mb",
        "13312",
        "--scratch-budget-mb",
        "6144",
    ]
    with (args.results / "server.log").open("w") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "ANTFLY_INFERENCE_REQUIRED_BACKEND": "native"},
            start_new_session=True,
        )
        args.server_pid = process.pid
        args.url = f"http://127.0.0.1:{port}"
        try:
            for _ in range(900):
                if process.poll() is not None:
                    raise RuntimeError("owned CPU image server exited during startup")
                try:
                    with image.urllib.request.urlopen(
                        args.url + "/healthz", timeout=1
                    ) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                raise TimeoutError("CPU image server startup deadline")
            yield
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def run(args):
    reference.verify_checkpoint(args.model_dir)
    args.results.mkdir(parents=True, exist_ok=True)
    args.suite = args.results / "suite.json"
    args.reference = args.results / "reference.json"
    args.candidate = args.results / "http-vectors.json"
    args.cuda_captions = args.results / "cuda-captions.json"
    args.cuda_images = args.results / "cuda-images.json"
    suite = image.load_suite(args)
    policy = json.loads(args.policy.read_text())
    summary = {
        "version": 1,
        "status": "running",
        "production_qualified": False,
        "scope": policy["scope"],
        "policy": policy,
        "policy_sha256": common.digest(args.policy),
        "suite_sha256": common.digest(args.suite),
        "checkpoint_receipt_sha256": common.digest(
            args.model_dir / "embeddinggemma2_receipt.json"
        ),
        "model_revision": reference.REVISION,
        "steps": {},
        "artifacts": {},
    }

    def step(name, action, filename):
        args.output = args.results / filename
        print("starting", name, time.strftime("%H:%M:%S", time.gmtime()), flush=True)
        action(args)
        result = json.loads(args.output.read_text())
        if result["status"] != "pass":
            raise ValueError(name + " did not pass")
        compact = dict(result)
        if name in {"collection", "text_regression"}:
            compact["case_count"] = len(compact.pop("cases"))
        if name == "load":
            compact["profiles"] = [
                {k: v for k, v in row.items() if k != "latencies_seconds"}
                for row in compact["profiles"]
            ]
        summary["steps"][name] = compact
        summary["artifacts"][name] = {
            "path": str(args.output.resolve()),
            "sha256": common.digest(args.output),
        }
        common.save(args.report, summary)

    try:
        with server(args):
            provenance = common.server_provenance(args)
            summary["server"] = provenance
            common.save(args.results / "server.json", provenance)
            case = next(row for row in suite["cases"] if row["name"] == "flickr-000")
            result, seconds = image.call(args, image.payload(suite, case))
            summary["cold_image"] = {
                "seconds": seconds,
                "case": case["name"],
                "memory": common.memory_tree(args.server_pid),
            }
            common.save(
                args.results / "cold-image.json",
                {**summary["cold_image"], "response": result, "server": provenance},
            )
            if seconds > policy["serving"]["cold_first_image_max_seconds"]:
                raise ValueError("cold image deadline exceeded")
            print("cold first image", seconds, flush=True)
            if args.reuse_collection:
                collected = image.candidate(args, suite)
                old_server = collected["server"]
                if (
                    old_server["binary_sha256"] != provenance["binary_sha256"]
                    or old_server["machine"] != provenance["machine"]
                    or old_server["allowed_cpus"] != provenance["allowed_cpus"]
                ):
                    raise ValueError(
                        "numerical collection reuse requires identical executable, architecture and affinity"
                    )

                def without_port(command):
                    index = command.index("--port")
                    return command[:index] + command[index + 2 :]

                if without_port(old_server["command"]) != without_port(
                    provenance["command"]
                ):
                    raise ValueError(
                        "numerical collection reuse requires matching server configuration"
                    )
                if collected["model_identity"] != result["model_identity"]:
                    raise ValueError(
                        "numerical collection reuse changed model identity"
                    )
                summary["collection_reused_from_identical_executable"] = True
                summary["steps"]["collection"] = {
                    k: v for k, v in collected.items() if k != "cases"
                }
                summary["steps"]["collection"]["case_count"] = len(collected["cases"])
                summary["artifacts"]["collection"] = {
                    "path": str(args.candidate.resolve()),
                    "sha256": common.digest(args.candidate),
                }
            else:
                step("collection", image.collect, "http-vectors.json")
                collected = json.loads(args.candidate.read_text())
            image.qualify.compare(
                result["data"][0]["embedding"],
                collected["cases"][case["name"]]["vector"],
                limit=1e-5,
            )
            step("accuracy_and_retrieval", image.evaluate, "accuracy-retrieval.json")
            step("contracts", image.contracts, "contracts.json")
            step("recovery_before_load", image.recovery, "recovery-before-load.json")
            step("load", image.load, "load.json")
            step("overload", image.overload, "overload.json")
            step("recovery_after_load", image.recovery, "recovery-after-load.json")
            args.oracle = args.text_oracle
            step("text_regression", common.collect, "text-regression.json")
            if common.server_provenance(args) != provenance:
                raise ValueError("server executable or process lifetime changed")
            summary["final_metrics"] = image.idle(args.url)
            memory = common.memory_tree(args.server_pid)
            summary["memory_at_end"] = memory
            if (
                max(row["VmHWM"] for row in memory.values())
                > policy["serving"]["max_worker_rss_bytes"]
            ):
                raise ValueError("worker lifetime RSS policy exceeded")
            worker = max(memory, key=lambda pid: memory[pid]["VmRSS"])
            if "libcuda" in (Path("/proc") / worker / "maps").read_text():
                raise ValueError("CPU worker loaded CUDA")
            summary["cuda_library_loaded"] = False
            summary["model_identity"] = summary["steps"]["collection"]["model_identity"]
            summary["qualified_at_utc"] = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
            )
            for path in [
                args.reference,
                args.suite,
                args.cuda_captions,
                args.cuda_images,
                args.text_oracle,
                args.policy,
                args.results / "cold-image.json",
                args.results / "server.json",
            ]:
                summary["artifacts"][path.name] = {
                    "path": str(path.resolve()),
                    "sha256": common.digest(path),
                }
            repo = Path(__file__).resolve().parents[5]
            sources = [
                Path(__file__),
                Path(image.__file__),
                Path(common.__file__),
                repo
                / "zig/pkg/inference/scripts/embeddinggemma2/cpu_image_reference.py",
                repo / "zig/lib/image/src/webp.zig",
                repo / "zig/testdata/image/manifest.zon",
            ]
            summary["source_sha256"] = {
                str(path.relative_to(repo)): common.digest(path) for path in sources
            }
            summary.update(status="pass", production_qualified=True)
    except Exception as error:
        summary.update(status="fail", production_qualified=False, failure=repr(error))
        raise
    finally:
        common.save(args.report, summary)
    print("image qualification PASSED", args.report, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--text-oracle", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--reuse-collection",
        action="store_true",
        help="Reuse a completed numerical collection only for the identical binary, suite, identity and configuration",
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
