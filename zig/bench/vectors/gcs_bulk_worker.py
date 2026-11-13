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


from bench_bulk_pipeline import CancellationScope, OwnedProcess, gcs_probe_environment


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


def parse_records(text):
    records = {}
    for line in text.splitlines():
        if line.startswith("PARQUET_BULK_") and " {" in line:
            key, value = line.split(" ", 1)
            records.setdefault(key, []).append(json.loads(value))
    return records


def validate_inventory(cfg, records):
    inventories = records.get("PARQUET_BULK_INVENTORY", [])
    if len(inventories) != 1 or not isinstance(inventories[0].get("files"), list):
        raise RuntimeError("missing or duplicate native source inventory")
    files = inventories[0]["files"]
    if any(
        not isinstance(file, dict)
        or not isinstance(file.get("uri"), str)
        or not isinstance(file.get("version"), str)
        or not isinstance(file.get("etag", ""), str)
        or type(file.get("bytes")) is not int
        or file["bytes"] < 0
        for file in files
    ):
        raise RuntimeError("invalid native source inventory")
    expected = cfg["source_objects"]
    if len(files) != len(expected):
        raise RuntimeError(
            "native source inventory differs from admitted immutable objects"
        )
    for observed, admitted in zip(
        sorted(files, key=lambda file: file["uri"]),
        sorted(expected, key=lambda file: file["uri"]),
    ):
        if observed["uri"] != admitted["uri"] or observed["bytes"] != admitted["bytes"]:
            raise RuntimeError(
                "native source inventory differs from admitted immutable objects"
            )
        # Prefix readers can bind a strong JSON metadata ETag instead of an
        # explicit generation. GCS resolves that conditional revision to its
        # generation before fetching bytes; compare the admitted alias too.
        if observed["version"]:
            if observed["version"] != admitted["version"] or (
                observed.get("etag") and observed["etag"] != admitted.get("etag")
            ):
                raise RuntimeError(
                    "native source generation differs from admitted object"
                )
        elif not admitted.get("etag") or observed.get("etag") != admitted["etag"]:
            raise RuntimeError("native source has no admitted immutable revision")


def validate_interruption(cfg, row, text, catalog):
    phase = "source" if cfg["interrupt"] == 1 else "tree"
    records = parse_records(text)
    validate_inventory(cfg, records)
    receipts = records.get("PARQUET_BULK_RECEIPT", [])
    expected_phases = ["source"] if phase == "source" else ["source", "tree"]
    construction = (catalog.get("checkpoint") or {}).get("construction", [])
    if (
        row["exit_code"] == 0
        or "Cancelled" not in text
        or [receipt["phase"] for receipt in receipts] != expected_phases
        or len(construction) != 1
        or receipts[-1]["name"] != "emb"
        or receipts[-1]["rows"] != cfg["rows"]
        or construction[0]["name"] != receipts[-1]["name"]
        or construction[0]["manifest"]["artifact_id"] != receipts[-1]["manifest"]
    ):
        raise RuntimeError("interruption did not retain the requested receipt phase")
    return receipts[-1]


def validate_resume(records, receipt):
    if records.get("PARQUET_BULK_RESUME") != [receipt]:
        raise RuntimeError("resume did not accept the exact retained receipt")
    if records.get("PARQUET_BULK_INPUT") != [dict(rows=0, resumed=True)]:
        raise RuntimeError("resume replayed source input")
    phases = records["PARQUET_BULK_PHASES"][0]
    calls = dict(zip(phases["names"], phases["calls"]))
    expected_clusters = 0 if receipt["phase"] == "tree" else 1
    if (
        calls.get("dense_cluster") != expected_clusters
        or calls.get("bulk_tee") != 0
        or calls.get("bulk_receipt") != 0
        or (receipt["phase"] == "tree" and records.get("PARQUET_BULK_WORK"))
    ):
        raise RuntimeError("resume repeated completed construction work")


def nonnegative_counts(record, fields):
    return all(type(record.get(key)) is int and record[key] >= 0 for key in fields)


def validate_owner_memory(records):
    owners = records.get("PARQUET_BULK_OWNER_MEMORY", [])
    limits = {
        "build": 1024**3,
        "transport": 128 * 1024**2,
        "generation_map": 64 * 1024**2,
    }
    if (
        len(owners) != len(limits)
        or {item.get("role") for item in owners} != set(limits)
        or any(
            not nonnegative_counts(item, ("peak_bytes", "limit_bytes"))
            or item["limit_bytes"] != limits.get(item.get("role"))
            or item["peak_bytes"] > item["limit_bytes"]
            for item in owners
        )
    ):
        raise RuntimeError("missing or invalid admitted owner memory evidence")
    if any(
        item["peak_bytes"] == 0 for item in owners if item["role"] != "generation_map"
    ):
        raise RuntimeError("missing build or transport allocation observation")


SOURCE_OPERATIONS = (
    "source_seal",
    "source_recovery_manifest",
    "source_pin_checkpoint",
    "source_checkpoint_publish",
    "source_sample_publish",
    "source_identity_directory",
    "source_receipt_save",
)


def validate_source_operations(records, *, fresh):
    operations = records.get("PARQUET_BULK_OPERATION", [])
    if [item.get("operation") for item in operations] != (
        list(SOURCE_OPERATIONS) if fresh else []
    ) or any(
        not nonnegative_counts(item, ("elapsed_ns",))
        or "error_name" not in item
        or item["error_name"] is not None
        for item in operations
    ):
        raise RuntimeError("missing, failed or repeated source operation evidence")


def validate_evidence(cfg, row, receipt=None):
    from bench_bulk_pipeline import BULK_PHASE_NAMES, validate_phases

    records = row["records"]
    validate_inventory(cfg, records)
    modes = records.get("PARQUET_BULK_BUILD_MODE", [])
    if (
        len(modes) != 1
        or modes[0].get("caller") != "fast"
        or modes[0].get("provider_is_test") is not False
        or any(
            type(modes[0].get(key)) is not int or modes[0][key] != 1
            for key in ("caller_value", "provider")
        )
    ):
        raise RuntimeError("mixed or non-release compilation")
    calls = validate_phases(records.get("PARQUET_BULK_PHASES", []), BULK_PHASE_NAMES)
    if cfg.get("reopen_catalog"):
        if (
            row["native_ns"] != 0
            or row["checkpoints"] != 0
            or any(calls.values())
            or any(
                records.get(name)
                for name in (
                    "PARQUET_BULK_INPUT",
                    "PARQUET_BULK_RESUME",
                    "PARQUET_BULK_RECEIPT",
                    "PARQUET_BULK_WORK",
                    "PARQUET_BULK_PROJECTION",
                    "PARQUET_BULK_SOURCE",
                    "PARQUET_BULK_OWNER_MEMORY",
                    "PARQUET_BULK_GENERATION_WORK",
                    "PARQUET_BULK_OPERATION",
                )
            )
        ):
            raise RuntimeError("verification-only reopen performed construction")
        return
    validate_owner_memory(records)
    generation = records.get("PARQUET_BULK_GENERATION_WORK", [])
    fresh_bulk = cfg["variant"] == "optimized" and receipt is None
    validate_source_operations(records, fresh=fresh_bulk)
    expected_batch = 4096 if fresh_bulk else 128
    if (
        len(generation) != 1
        or not nonnegative_counts(
            generation[0],
            (
                "batch_records",
                "batch_bytes",
                "flushes",
                "peak_pending_records",
                "peak_pending_bytes",
            ),
        )
        or generation[0]["batch_records"] != expected_batch
        or generation[0]["batch_bytes"] != 1024**2
        or generation[0]["peak_pending_records"] > 2 * expected_batch
        or generation[0]["peak_pending_bytes"] > 2 * 1024**2 + 8192
    ):
        raise RuntimeError("missing or unbounded generation mutation evidence")
    if any(
        calls[name] != 1
        for name in (
            "identity",
            "dense",
            "dense_ingest",
            "dense_checkpoint",
            "dense_upload",
        )
    ):
        raise RuntimeError("missing or repeated construction phase")
    inputs = records.get("PARQUET_BULK_INPUT", [])
    if (
        len(inputs) != 1
        or not nonnegative_counts(inputs[0], ("rows",))
        or type(inputs[0].get("resumed")) is not bool
    ):
        raise RuntimeError("invalid dense input evidence")
    if stage_resume := receipt is not None:
        validate_resume(records, receipt)
    else:
        fused = cfg["variant"] == "optimized"
        bulk = cfg["variant"] != "incremental"
        if (
            calls["dense_cluster"] != int(bulk)
            or bool(calls["bulk_tee"]) != fused
            or calls["bulk_receipt"] != int(fused)
        ):
            raise RuntimeError("construction phases do not match the requested variant")
        if records.get("PARQUET_BULK_INPUT") != [
            dict(rows=0 if fused else cfg["rows"], resumed=fused)
        ]:
            raise RuntimeError(
                "dense input evidence does not match the requested variant"
            )
    if cfg["variant"] != "incremental":
        expected = (
            []
            if stage_resume and receipt["phase"] == "tree"
            else ["tree"]
            if stage_resume
            else ["source", "tree"]
        )
        saved = records.get("PARQUET_BULK_RECEIPT", [])
        if (
            [value.get("phase") for value in saved] != expected
            or row["checkpoints"] != len(expected)
            or any(
                value.get("name") != "emb" or value.get("rows") != cfg["rows"]
                for value in saved
            )
        ):
            raise RuntimeError("incomplete construction receipt evidence")
    elif row["checkpoints"] != 0 or records.get("PARQUET_BULK_RECEIPT"):
        raise RuntimeError("incremental case unexpectedly used bulk receipts")
    memory = records.get("PARQUET_BULK_MEMORY", [])
    fields = (
        "dense_allocator_peak_bytes",
        "source_allocator_peak_bytes",
        "max_vector_allocator_peak_bytes",
    )
    if (
        len(memory) != 1
        or not nonnegative_counts(memory[0], fields)
        or memory[0][fields[0]] == 0
        or any(memory[0][key] > 256 * 1024**2 for key in fields)
        or memory[0][fields[2]] != max(memory[0][fields[0]], memory[0][fields[1]])
    ):
        raise RuntimeError("missing memory evidence or private quota exceeded")
    if cfg["variant"] == "incremental":
        if any(
            records.get(name)
            for name in (
                "PARQUET_BULK_WORK",
                "PARQUET_BULK_PROJECTION",
                "PARQUET_BULK_SOURCE",
            )
        ):
            raise RuntimeError("incremental case unexpectedly emitted bulk work")
        return
    source = records.get("PARQUET_BULK_SOURCE", [])
    if (
        len(source) != 1
        or not nonnegative_counts(
            source[0], ("runs", "reused_runs", "reused_bytes", "new_bytes")
        )
        or source[0]["runs"] <= 0
        or source[0]["new_bytes"] != 0
        or source[0]["reused_runs"] != source[0]["runs"]
        or source[0]["reused_bytes"] <= 0
    ):
        raise RuntimeError("exact source was not fully reused")
    if not (stage_resume and receipt["phase"] == "tree"):
        work = records.get("PARQUET_BULK_WORK", [])
        fields = (
            "identity_read_bytes",
            "identity_written_bytes",
            "identity_merge_inputs",
            "projection_cache_peak_bytes",
            "owned_index_count",
            "partition_bytes",
            "partition_read_bytes",
            "compact_rows",
            "projection_commit_ns",
            "save_node_ns",
            "update_parent_ns",
            "update_parent_calls",
            "training_ns",
            "routing_ns",
            "allocator_peak_bytes",
        )
        if (
            len(work) != 1
            or not nonnegative_counts(work[0], fields)
            or work[0].get("identity_census_verified") is not True
            or work[0]["owned_index_count"] != 0
            or work[0]["identity_merge_inputs"] > 32
        ):
            raise RuntimeError("unverified or unbounded identity construction")
        if cfg["variant"] == "optimized" and work[0]["update_parent_calls"] != 0:
            raise RuntimeError("preassigned parents were rewritten")


class DiskSampler:
    """Checkpoint scratch evidence independently of successful qualification."""

    def __init__(self, root, report):
        self.root, self.report = root, report
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.error_type = None
        self.peaks = dict(
            case_nominal_bytes=0,
            host_filesystem_min_free_bytes=None,
        )
        self.thread = None
        self.final_deadline = None
        self.final_sample_complete = False
        self.skip_final = False

    def snapshot(self):
        with self.lock:
            return dict(
                self.peaks,
                error_type=self.error_type,
                interval_seconds=2,
                scope="case directory nominal lengths only; host free-space includes unrelated work, native /tmp scratch not attributed",
            )

    @staticmethod
    def walk_error(error):
        if not isinstance(error, FileNotFoundError):
            raise error

    def check_sample(self):
        scope = CancellationScope.current
        if scope is not None and scope.signum is not None:
            raise InterruptedError("scratch sampling cancelled")
        if self.final_deadline is not None and time.monotonic() >= self.final_deadline:
            raise TimeoutError("scratch sampling deadline")

    def sample(self):
        self.check_sample()
        total = 0
        for directory, _, names in os.walk(self.root, onerror=self.walk_error):
            self.check_sample()
            for name in names:
                self.check_sample()
                try:
                    total += (Path(directory) / name).stat().st_size
                except FileNotFoundError:
                    pass
        self.check_sample()
        free = shutil.disk_usage(self.root).free
        self.check_sample()
        with self.lock:
            self.peaks["case_nominal_bytes"] = max(
                self.peaks["case_nominal_bytes"], total
            )
            previous = self.peaks["host_filesystem_min_free_bytes"]
            self.peaks["host_filesystem_min_free_bytes"] = (
                free if previous is None else min(previous, free)
            )
        atomic(self.root / "disk-samples.json", self.snapshot())

    def loop(self):
        try:
            while not self.stop.wait(2):
                self.sample()
            # Final collection stays in this daemon, never the reporting thread:
            # a blocked filesystem call cannot delay terminal evidence forever.
            if not self.skip_final:
                self.sample()
                self.final_sample_complete = True
        except Exception as error:
            with self.lock:
                self.error_type = type(error).__name__

    def __enter__(self):
        try:
            self.sample()
            self.thread = threading.Thread(target=self.loop, daemon=True)
            self.thread.start()
        except BaseException as error:
            self.error_type = type(error).__name__
            scope = CancellationScope.current
            self.report.update(
                qualified=False,
                status="cancelled"
                if scope is not None and scope.signum is not None
                else "failed",
                error_type=self.error_type,
                scratch_sample=dict(
                    self.snapshot(), sampler_stopped=True, final_sample_complete=False
                ),
            )
            atomic(self.root / "result.json", self.report)
            raise
        return self

    def __exit__(self, kind, error, _trace):
        qualified = self.report.get("qualified", False)
        scope = CancellationScope.current
        cancelled = scope is not None and scope.signum is not None
        self.skip_final = kind is not None or cancelled
        self.final_deadline = time.monotonic() + 2
        # Retain the last complete observations before waiting on filesystem I/O.
        self.report.update(
            qualified=False,
            status="cancelled" if cancelled else "failed" if kind else "finalizing",
            scratch_sample=dict(
                self.snapshot(), final_sample_complete=False, sampler_stopped=False
            ),
        )
        if kind is not None or cancelled:
            self.report["error_type"] = (
                kind.__name__ if kind is not None else "SystemExit"
            )
        atomic(self.root / "result.json", self.report)
        self.stop.set()
        self.thread.join(timeout=3)
        sample = self.snapshot()
        sample["sampler_stopped"] = not self.thread.is_alive()
        sample["final_sample_complete"] = self.final_sample_complete
        self.report["scratch_sample"] = sample
        broken = (
            self.error_type is not None
            or not sample["sampler_stopped"]
            or (not self.skip_final and not self.final_sample_complete)
        )
        scope = CancellationScope.current
        cancelled = scope is not None and scope.signum is not None
        if kind is not None or broken or cancelled:
            self.report["qualified"] = False
            self.report["status"] = "cancelled" if cancelled else "failed"
            self.report["error_type"] = (
                kind.__name__
                if kind is not None
                else "SystemExit"
                if cancelled
                else "ScratchSamplingFailed"
            )
        else:
            self.report["qualified"] = qualified
            self.report["status"] = "qualified" if qualified else "failed"
        atomic(self.root / "result.json", self.report)
        if kind is None:
            if cancelled:
                scope.check()
            if broken:
                raise RuntimeError("scratch sampler failed")


def run():
    cfg = json.loads(Path(sys.argv[1]).read_text())
    token = "" if cfg.get("auth") == "adc" else sys.stdin.readline(65537).strip()
    if cfg.get("auth") != "adc" and (not token or len(token) > 65536):
        raise ValueError("expected bounded credential on stdin")
    native_env = gcs_probe_environment(cfg.get("auth", "bearer"))
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
    Path("config.json").chmod(0o400)
    config_hash = hashlib.sha256(Path("config.json").read_bytes()).hexdigest()
    Path("expected-inventory.json").write_text(json.dumps(cfg["source_objects"]))
    Path("expected-inventory.json").chmod(0o400)
    inventory_hash = hashlib.sha256(
        Path("expected-inventory.json").read_bytes()
    ).hexdigest()
    env = dict(
        native_env,
        BULK_GCS_BEARER=token,
        ANTFLY_LAKE_BULK_CONFIG=str(root / "config.json"),
        ANTFLY_LAKE_BULK_EXPECTED_INVENTORY=str(root / "expected-inventory.json"),
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
        ANTFLY_LAKE_BULK_COMPACT="0",
    )
    if cfg.get("reopen_catalog"):
        shutil.copyfile(cfg["reopen_catalog"], root / "catalog.json")
        env["ANTFLY_LAKE_BULK_REOPEN_ONLY"] = "1"
    report = dict(
        config=cfg,
        native_config_sha256=config_hash,
        expected_inventory_sha256=inventory_hash,
        qualified=False,
        provider_environment=dict(
            credential_mode=cfg.get("auth", "bearer"),
            storage_endpoint="https://storage.googleapis.com/storage/v1",
            upload_endpoint="https://storage.googleapis.com/upload/storage/v1",
        ),
        attempts=[],
        network_scope="Linux process network namespace interfaces, including control traffic; unavailable on macOS; not object-store payload bytes",
    )
    atomic(root / "result.json", report)
    with DiskSampler(root, report):
        retained_receipt = None
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
            owner = OwnedProcess(collect_usage=True)
            try:
                with (root / (stage + ".log")).open("w") as log, owner:
                    owner.start(
                        [cfg["binary"]],
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=False,
                    )
                    owner.wait()
            finally:
                usage = owner.usage
                row = dict(
                    stage=stage,
                    exit_code=owner.returncode,
                    wall_ns=time.monotonic_ns() - start,
                    resource_usage_available=usage is not None,
                    user_seconds=usage.ru_utime if usage is not None else None,
                    system_seconds=usage.ru_stime if usage is not None else None,
                    peak_rss_bytes=usage.ru_maxrss
                    * (1 if sys.platform == "darwin" else 1024)
                    if usage is not None
                    else None,
                    pod_network_bytes=network() - before
                    if before is not None
                    else None,
                    probe_environment={
                        key: value
                        for key, value in env.items()
                        if key.startswith(("ANTFLY_LAKE_BULK_", "ANTFLY_LAKE_PROBE_"))
                    },
                )
                report["attempts"].append(row)
                atomic(root / "result.json", report)
            if (
                hashlib.sha256(Path("config.json").read_bytes()).hexdigest()
                != config_hash
            ):
                raise RuntimeError("native configuration changed during execution")
            if (
                hashlib.sha256(Path("expected-inventory.json").read_bytes()).hexdigest()
                != inventory_hash
            ):
                raise RuntimeError("expected inventory changed during execution")
            text = (root / (stage + ".log")).read_text()
            if token and token in text:
                raise RuntimeError("credential appeared in child log")
            row["records"] = parse_records(text)
            atomic(root / "result.json", report)
            validate_inventory(cfg, row["records"])
            if stage == "interrupt":
                catalog = json.loads((root / "catalog.json").read_text())
                retained_receipt = validate_interruption(cfg, row, text, catalog)
                row["retained_receipt"] = retained_receipt
                row["retained_catalog_sha256"] = hashlib.sha256(
                    (root / "catalog.json").read_bytes()
                ).hexdigest()
                continue
            match = re.search(
                r"^PARQUET_BULK algorithm=\w+ vectors=(\d+) dims=768 total_ns=(\d+) reopened_hits=10 checkpoints=(\d+) logical_identity=true$",
                text,
                re.M,
            )
            if owner.returncode or not match or int(match[1]) != cfg["rows"]:
                raise RuntimeError("build/reopen verification failed")
            row["native_ns"] = int(match[2])
            row["checkpoints"] = int(match[3])
            from bench_bulk_pipeline import validate_recall

            truth = json.loads((Path(cfg["recall"]) / "prepare.json").read_text())
            validate_recall(row["records"].get("PARQUET_BULK_RECALL", []), truth)

            validate_evidence(cfg, row, retained_receipt if stage == "resume" else None)
            if cfg.get("reopen_catalog"):
                row["build_timing_available"] = False

        report["qualified"] = True
    print(json.dumps(dict(qualified=True, case=cfg["case"])), flush=True)


def main():
    with CancellationScope() as cancellation:
        try:
            run()
        except BaseException:
            cancellation.check()
            raise


if __name__ == "__main__":
    main()
