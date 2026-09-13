"""Capture bounded projection-read attribution, never performance measurements."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx
from run_dense_recovery_query_ab import ready, stop
from run_posting_locality_ab import digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--source-data", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--port", type=int, default=19462)
    parser.add_argument("--health-port", type=int, default=19463)
    args = parser.parse_args()
    root, binary, source = (
        args.root.resolve(),
        args.binary.resolve(strict=True),
        args.source_data.resolve(strict=True),
    )
    if root.is_relative_to(source) or args.port == args.health_port:
        parser.error("independent clone and distinct ports required")
    root.mkdir(parents=True, exist_ok=False)
    if sys.platform == "darwin":
        subprocess.run(["cp", "-cR", str(source), str(root / "data")], check=True)
    else:
        shutil.copytree(source, root / "data")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_", "VDBBENCH_"))
    }
    env.update(
        ANTFLY_HBC_POSTING_SIDECAR="1",
        ANTFLY_HBC_POSTING_WAL_STORE="1",
        ANTFLY_HBC_VECTOR_BLOCK_STORE="1",
        ANTFLY_HBC_VECTOR_BLOCK_ENCODING="float16",
        ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS="0",
        ANTFLY_EXPERIMENT_PROJECTION_TRACE="1",
    )
    scripts = Path(__file__).resolve().parent
    inputs = [
        binary,
        Path(__file__).resolve(),
        scripts / "profile_vdbbench_public_query.py",
        scripts / "export_projection_read_trace.py",
        scripts / "run_dense_recovery_query_ab.py",
        scripts / "run_posting_locality_ab.py",
        args.dataset / "test.parquet",
        args.dataset / "neighbors.parquet",
    ]
    hashes = {str(p.resolve()): digest(p) for p in inputs}
    receipt = {
        "diagnostic_only": True,
        "inputs_sha256": hashes,
        "source_data": str(source),
        "passed": False,
    }
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    server = None
    try:
        with (root / "server.log").open("w") as log:
            server = subprocess.Popen(
                [
                    str(binary),
                    "standalone",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(args.port),
                    "--health-port",
                    str(args.health_port),
                    "--auth",
                    "false",
                    "--data-dir",
                    str(root / "data"),
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 180
            with httpx.Client(timeout=5, trust_env=False) as client:
                while time.monotonic() < deadline:
                    if server.poll() is not None:
                        raise RuntimeError("diagnostic server exited before readiness")
                    try:
                        response = client.get(
                            f"http://127.0.0.1:{args.port}/db/v1/tables/vdbbench/indexes/vec"
                        )
                        response.raise_for_status()
                        if ready(response.json()):
                            break
                    except (httpx.HTTPError, ValueError):
                        pass
                    time.sleep(0.5)
                else:
                    raise TimeoutError("diagnostic readiness deadline")
            subprocess.run(
                [
                    sys.executable,
                    str(scripts / "profile_vdbbench_public_query.py"),
                    "--dataset",
                    str(args.dataset),
                    "--port",
                    str(args.port),
                    "--count",
                    "64",
                    "--output",
                    str(root / "trace-queries-not-performance.json"),
                ],
                check=True,
                timeout=900,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
    finally:
        stop(server)
    if hashes != {str(p.resolve()): digest(p) for p in inputs}:
        raise RuntimeError("capture inputs changed")
    subprocess.run(
        [
            sys.executable,
            str(scripts / "export_projection_read_trace.py"),
            str(root / "data"),
            str(root / "server.log"),
            str(root / "export"),
        ],
        check=True,
    )
    receipt["passed"] = True
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
