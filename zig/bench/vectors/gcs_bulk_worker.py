#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run one local qualification case against GCS with ADC or a stdin credential."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import threading
import shutil


def atomic(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def network():
    if not Path("/proc/net/dev").exists():
        return None
    lines = Path("/proc/net/dev").read_text().splitlines()[2:]
    values = [line.split(":", 1) for line in lines if ":" in line]
    return sum(
        int(v.split()[0]) + int(v.split()[8])
        for name, v in values
        if name.strip() != "lo"
    )


def main():
    cfg = json.loads(Path(sys.argv[1]).read_text())
    token = "" if cfg.get("auth") == "adc" else sys.stdin.readline(65537).strip()
    if cfg.get("auth") != "adc" and (not token or len(token) > 65536):
        raise ValueError("expected bounded credential on stdin")
    root = Path(cfg["work"])
    root.mkdir(exist_ok=False)
    os.chdir(root)

    def connection(capability, prefix):
        return dict(
            kind="external_io",
            capabilities=[capability],
            external_io=dict(
                protocol="gcs",
                buckets=[cfg["bucket"]],
                prefix=prefix,
                bucket_provisioning="require_existing",
                credentials=dict(
                    **(
                        {"source": "default"}
                        if cfg.get("auth") == "adc"
                        else {
                            "source": "bearer_token",
                            "bearer_token": "${secret:BULK_GCS_BEARER}",
                        }
                    )
                ),
            ),
        )

    config = dict(
        deployment_mode="standalone",
        storage=dict(
            engine="local",
            local={"base_dir": str(root / "data")},
            artifacts=dict(
                connection="bulk-artifacts",
                bucket=cfg["bucket"],
                prefix=cfg["prefix"] + "/artifacts",
            ),
        ),
        connections={
            "bulk-source": connection("lake.read", cfg["source_prefix"]),
            "bulk-artifacts": connection(
                "storage.primary", cfg["prefix"] + "/artifacts"
            ),
        },
    )
    Path("config.json").write_text(json.dumps(config))
    env = dict(
        os.environ,
        BULK_GCS_BEARER=token,
        ANTFLY_LAKE_BULK_CONFIG=str(root / "config.json"),
        ANTFLY_LAKE_BULK_PARQUET="gs://" + cfg["bucket"] + "/" + cfg["source_prefix"],
        ANTFLY_LAKE_BULK_CATALOG=str(root / "catalog.json"),
        ANTFLY_LAKE_BULK_RECALL_DIR=cfg["recall"],
        ANTFLY_LAKE_BULK_DIMS="768",
        ANTFLY_LAKE_BULK_INPUT_BYTES=str(cfg["allowance"]),
        ANTFLY_LAKE_BULK_IDENTITY_SPILL_BYTES=str(
            cfg.get("identity_allowance", 1024**3)
        ),
        ANTFLY_LAKE_BULK_ALGORITHM="incremental"
        if cfg["variant"] == "incremental"
        else "lloyd",
        ANTFLY_LAKE_BULK_FUSED="1" if cfg["variant"] == "optimized" else "0",
        ANTFLY_LAKE_BULK_PREBIND="1" if cfg["variant"] == "optimized" else "0",
        ANTFLY_LAKE_BULK_CHECKPOINT="1",
    )
    if cfg.get("reopen_catalog"):
        shutil.copyfile(cfg["reopen_catalog"], root / "catalog.json")
        env["ANTFLY_LAKE_BULK_REOPEN_ONLY"] = "1"
    report = dict(
        config=cfg,
        qualified=False,
        attempts=[],
        network_scope="Linux process network namespace interfaces, including control traffic; unavailable on macOS; not object-store payload bytes",
    )
    atomic(root / "result.json", report)
    stop = threading.Event()
    peaks = {
        "case_nominal_bytes": 0,
        "host_filesystem_min_free_bytes": shutil.disk_usage(root).free,
    }

    def sample_disk():
        while not stop.is_set():
            total = 0
            for base in (root,):
                for directory, _, names in os.walk(base):
                    for name in names:
                        try:
                            total += (Path(directory) / name).stat().st_size
                        except FileNotFoundError:
                            pass
            peaks["case_nominal_bytes"] = max(peaks["case_nominal_bytes"], total)
            peaks["host_filesystem_min_free_bytes"] = min(
                peaks["host_filesystem_min_free_bytes"], shutil.disk_usage(root).free
            )
            stop.wait(2)

    sampler = threading.Thread(target=sample_disk, daemon=True)
    sampler.start()
    for stage in (
        ["reopen"]
        if cfg.get("reopen_catalog")
        else ["interrupt", "resume"]
        if cfg.get("interrupt")
        else ["build"]
    ):
        if stage == "interrupt":
            env["ANTFLY_LAKE_BULK_INTERRUPT"] = str(cfg["interrupt"])
            env.pop("ANTFLY_LAKE_BULK_RECALL_DIR", None)
        if stage == "resume":
            env.pop("ANTFLY_LAKE_BULK_INTERRUPT", None)
            env["ANTFLY_LAKE_BULK_RESUME"] = "1"
            env["ANTFLY_LAKE_BULK_RECALL_DIR"] = cfg["recall"]
        before, start = network(), time.monotonic_ns()
        with (root / (stage + ".log")).open("w") as log:
            child = subprocess.Popen(
                [cfg["binary"]], env=env, stdout=log, stderr=subprocess.STDOUT
            )
            try:
                _, status, usage = os.wait4(child.pid, 0)
                child.returncode = os.waitstatus_to_exitcode(status)
            finally:
                if child.returncode is None:
                    child.kill()
                    child.wait()
        row = dict(
            stage=stage,
            exit_code=child.returncode,
            wall_ns=time.monotonic_ns() - start,
            user_seconds=usage.ru_utime,
            system_seconds=usage.ru_stime,
            peak_rss_bytes=usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
            pod_network_bytes=network() - before if before is not None else None,
        )
        report["attempts"].append(row)
        atomic(root / "result.json", report)
        text = (root / (stage + ".log")).read_text()
        if token and token in text:
            raise RuntimeError("credential appeared in child log")
        if stage == "interrupt":
            catalog = json.loads((root / "catalog.json").read_text())
            if (
                child.returncode == 0
                or "Cancelled" not in text
                or not catalog.get("checkpoint", {}).get("construction")
            ):
                raise RuntimeError("interruption did not retain construction receipt")
            row["retained_catalog_sha256"] = hashlib.sha256(
                (root / "catalog.json").read_bytes()
            ).hexdigest()
            continue
        match = re.search(
            r"^PARQUET_BULK algorithm=\w+ vectors=(\d+) dims=768 total_ns=(\d+) reopened_hits=10 checkpoints=(\d+) logical_identity=true$",
            text,
            re.M,
        )
        if child.returncode or not match or int(match[1]) != cfg["rows"]:
            raise RuntimeError("build/reopen verification failed")
        row["native_ns"] = int(match[2])
        row["checkpoints"] = int(match[3])
        row["records"] = {}
        for line in text.splitlines():
            if line.startswith("PARQUET_BULK_") and " {" in line:
                key, value = line.split(" ", 1)
                row["records"].setdefault(key, []).append(json.loads(value))
        from bench_bulk_pipeline import validate_recall

        truth = json.loads((Path(cfg["recall"]) / "prepare.json").read_text())
        validate_recall(row["records"].get("PARQUET_BULK_RECALL", []), truth)

        if cfg.get("reopen_catalog"):
            row["build_timing_available"] = False
            continue
        records = row["records"]
        modes = records.get("PARQUET_BULK_BUILD_MODE", [])
        if len(modes) != 1 or modes[0] != dict(
            caller="fast", caller_value=1, provider=1, provider_is_test=False
        ):
            raise RuntimeError("mixed or non-release compilation")
        phases = records.get("PARQUET_BULK_PHASES", [])
        if (
            len(phases) != 1
            or len(phases[0]["names"]) != len(set(phases[0]["names"]))
            or len(phases[0]["names"]) != len(phases[0]["ns"])
            or len(phases[0]["names"]) != len(phases[0]["calls"])
            or any(value < 0 for value in phases[0]["ns"] + phases[0]["calls"])
        ):
            raise RuntimeError("incomplete phase attribution")
        memory = records.get("PARQUET_BULK_MEMORY", [])
        if len(memory) != 1 or any(
            memory[0][key] > 256 * 1024**2
            for key in ("dense_allocator_peak_bytes", "source_allocator_peak_bytes")
        ):
            raise RuntimeError("missing memory evidence or private quota exceeded")
        if cfg["variant"] != "incremental":
            source = records.get("PARQUET_BULK_SOURCE", [])
            if (
                len(source) != 1
                or source[0]["runs"] <= 0
                or source[0]["new_bytes"] != 0
                or source[0]["reused_runs"] != source[0]["runs"]
                or source[0]["reused_bytes"] <= 0
            ):
                raise RuntimeError("exact source was not fully reused")
            # A completed-tree resume intentionally does no new clustering.
            if cfg.get("interrupt") != 2:
                work = records.get("PARQUET_BULK_WORK", [])
                if (
                    len(work) != 1
                    or not work[0].get("identity_census_verified")
                    or work[0]["owned_index_count"] != 0
                    or work[0]["identity_merge_inputs"] > 32
                ):
                    raise RuntimeError("unverified or unbounded identity construction")
                if (
                    cfg["variant"] == "optimized"
                    and work[0]["update_parent_calls"] != 0
                ):
                    raise RuntimeError("preassigned parents were rewritten")

    stop.set()
    sampler.join()
    report["scratch_sample"] = dict(
        peaks,
        interval_seconds=2,
        scope="case directory nominal lengths only; host free-space includes unrelated work, native /tmp scratch not attributed",
    )
    report["qualified"] = True
    atomic(root / "result.json", report)
    print(json.dumps(dict(qualified=True, case=cfg["case"])), flush=True)


if __name__ == "__main__":
    main()
