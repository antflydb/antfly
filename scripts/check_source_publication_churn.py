"""Supplement fresh publication A/B with changed-vector/delete/restore recovery.

Uses independent clones of the first completed 50K pair. Timings under this
correctness check are not fresh-load/QPS qualification. Originals are untouched.
"""

import argparse
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
from run_dense_recovery_query_ab import ready, stop
from run_posting_locality_ab import digest


def check_identity(payload, incarnation):
    status = payload.get("status", {})
    if (
        status.get("readiness", {}).get("incarnation") != incarnation
        or status.get("doc_count") != 50000
    ):
        raise RuntimeError("wrong cloned index identity or restored document count")


def check_restored_recall(before, after):
    if any(
        profile.get("count") != 1000
        or not isinstance(profile.get("recall"), (int, float))
        or not math.isfinite(profile["recall"])
        or not 0 <= profile["recall"] <= 1
        for profile in (before, after)
    ):
        raise RuntimeError("missing or invalid fixed-query recall")
    if before["recall"] - after["recall"] > 0.01 + 1e-9:
        raise RuntimeError("restored/restarted recall lost more than one point")


def wait_ready(server, port, incarnation):
    deadline = time.monotonic() + 180
    with httpx.Client(timeout=5, trust_env=False) as client:
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError("server exited before native readiness")
            try:
                response = client.get(
                    f"http://127.0.0.1:{port}/db/v1/tables/vdbbench/indexes/vec"
                )
                response.raise_for_status()
                if ready(response.json()):
                    check_identity(response.json(), incarnation)
                    return response.json()
            except httpx.HTTPError:
                pass
            time.sleep(0.25)
    raise TimeoutError("native readiness deadline exceeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("qualification_root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--port", type=int, default=19464)
    args = parser.parse_args()
    roots = args.qualification_root.resolve()
    source_receipts = json.loads((roots / "ab-runs.json").read_text())
    arms = [
        arm
        for arm in source_receipts
        if arm["case"] == "Performance1536D50K" and arm["pair"] == 1
    ]
    if (
        len(arms) != 2
        or {arm["mode"] for arm in arms} != {"control", "candidate"}
        or any(arm.get("exit_code") != 0 or arm.get("invalid_reason") for arm in arms)
    ):
        parser.error("a completed, successful first 50K pair is required")
    scripts = Path(__file__).resolve().parent
    output = args.output.resolve()
    if output.is_relative_to(roots):
        parser.error("supplemental evidence must be outside the original run")
    output.mkdir(parents=True, exist_ok=False)
    receipts = []
    for arm in arms:
        source = Path(arm["command"][1]) / "data"
        original_status = json.loads(
            (source.parent / "index-after-restart.json").read_text()
        )
        incarnation = original_status["status"]["readiness"]["incarnation"]
        check_identity(original_status, incarnation)
        binary = Path(arm["environment"]["ANTFLY_BIN"])
        if digest(binary) != arm["inputs_sha256"][str(binary)]:
            raise RuntimeError("original pinned binary changed")
        target = output / arm["mode"]
        target.mkdir()
        if sys.platform == "darwin":
            subprocess.run(["cp", "-cR", str(source), str(target / "data")], check=True)
        else:
            shutil.copytree(source, target / "data")
        inputs = [
            binary,
            Path(__file__).resolve(),
            scripts / "run_dense_recovery_query_ab.py",
            scripts / "run_posting_locality_ab.py",
            scripts / "profile_vector_store_churn.py",
            scripts / "profile_vdbbench_public_query.py",
            scripts / "vector_store_disk_accounting.py",
            *(
                args.dataset / name
                for name in (
                    "shuffle_train.parquet",
                    "test.parquet",
                    "neighbors.parquet",
                )
            ),
        ]
        hashes = {str(path.resolve()): digest(path) for path in inputs}
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("ANTFLY_", "VDBBENCH_"))
        }
        environment.update(arm["environment"])
        environment.update(
            ANTFLY_HBC_POSTING_SIDECAR="1",
            ANTFLY_HBC_POSTING_WAL_STORE="1",
            ANTFLY_HBC_VECTOR_BLOCK_STORE="1",
            ANTFLY_HBC_VECTOR_BLOCK_ENCODING="float16",
        )
        receipt = {
            "mode": arm["mode"],
            "source_data": str(source),
            "inputs_sha256": hashes,
            "environment": arm["environment"],
            "passed": False,
            "diagnostic_only": True,
        }
        receipts.append(receipt)
        receipt_path = output / "receipts.json"
        receipt_path.write_text(json.dumps(receipts, indent=2) + "\n")
        command = [
            str(binary),
            "standalone",
            "--host",
            "127.0.0.1",
            "--port",
            str(args.port),
            "--health-port",
            str(args.port + 1),
            "--auth",
            "false",
            "--data-dir",
            str(target / "data"),
        ]
        server = None
        try:
            with (target / "server.log").open("w") as log:

                def start(command=command, environment=environment):
                    for port in (args.port, args.port + 1):
                        with socket.socket() as probe:
                            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            probe.bind(("127.0.0.1", port))
                    return subprocess.Popen(
                        command, env=environment, stdout=log, stderr=subprocess.STDOUT
                    )

                def profile(name, target=target):
                    path = target / f"{name}.json"
                    subprocess.run(
                        [
                            sys.executable,
                            str(scripts / "profile_vdbbench_public_query.py"),
                            "--dataset",
                            str(args.dataset),
                            "--port",
                            str(args.port),
                            "--count",
                            "1000",
                            "--output",
                            str(path),
                        ],
                        stdout=subprocess.DEVNULL,
                        check=True,
                        timeout=300,
                    )
                    return json.loads(path.read_text())

                server = start()
                wait_ready(server, args.port, incarnation)
                before = profile("before")
                subprocess.run(
                    [
                        sys.executable,
                        str(scripts / "profile_vector_store_churn.py"),
                        "--dataset",
                        str(args.dataset),
                        "--port",
                        str(args.port),
                        "--rows",
                        "2000",
                        "--rounds",
                        "2",
                        "--batch",
                        "100",
                        "--output",
                        str(target / "churn.json"),
                    ],
                    check=True,
                    timeout=600,
                )
                wait_ready(server, args.port, incarnation)
                stop(server)
                server = start()
                receipt["restarted_status"] = wait_ready(server, args.port, incarnation)
                after = profile("restored-restarted")
                receipt["recall_before"] = before["recall"]
                receipt["recall_after"] = after["recall"]
                check_restored_recall(before, after)
                stop(server)
                subprocess.run(
                    [
                        sys.executable,
                        str(scripts / "vector_store_disk_accounting.py"),
                        str(target / "data"),
                        str(target / "disk.json"),
                    ],
                    check=True,
                )
                if any(
                    digest(Path(path)) != checksum for path, checksum in hashes.items()
                ):
                    raise RuntimeError("input changed during recovery check")
                receipt["passed"] = True
        except BaseException as error:
            receipt["error"] = str(error)
            raise
        finally:
            stop(server)
            receipt_path.write_text(json.dumps(receipts, indent=2) + "\n")
        print(f"Passed changed-vector recovery: {arm['mode']}", flush=True)


if __name__ == "__main__":
    main()
