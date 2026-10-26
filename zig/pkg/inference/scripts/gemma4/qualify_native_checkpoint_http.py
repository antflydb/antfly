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

"""Public native checkpoint roundtrip qualification; no private compaction flags."""

import argparse
import contextlib
import copy
import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import threading
import time
import urllib.request

REPO = Path(__file__).resolve().parents[5]
SCRIPTS = REPO / "zig/pkg/inference/scripts/gemma4"
sys.path.insert(0, str(SCRIPTS))
from qualify_attention_match_queries import (
    active_group_members,
    canonical_choices,
    discover_worker_pid,
    read_response,
    requests,
    validate_fixture_choice,
    wait_server_ready,
)
from qualify_context_management import CudaProcessSampler, git_provenance, sha256_file
from native_checkpoint_transport import CONTENT_TYPE, receive_checkpoint, send_resume
sys.path.insert(0, str(SCRIPTS.parent))
from benchmark_resources import group_rss_mib, memory_free_percent, swapout_bytes


METAL_TEST_SERVER_SCHEMA = "antfly.metal-checkpoint-test-server.v1"
METAL_TEST_MODEL = "google/gemma-4-E4B-it-qat-q4_0-gguf"
METAL_COMBINED_RSS_LIMIT_BYTES = 8 * 1024**3
METAL_MINIMUM_FREE_MEMORY_PERCENT = 15
METAL_MINIMUM_DISK_FREE_MIB = 4096
METAL_LOADER_HEADROOM_PERCENT = 76
METAL_LOADER_HEADROOM_SAMPLES = 2
METAL_LOADER_HEADROOM_INTERVAL_SECONDS = 10
METAL_LOADER_HEADROOM_MAX_WAIT_SECONDS = 1800
METAL_TEST_MINIMUM_SAVINGS_HEADER = "X-Antfly-Qualification-Minimum-Savings"
PORTABLE_IDENTITY_FIELDS = {"weights", "tokenizer", "template", "layout"}


class MetalProcessSampler:
    """Bound unified process-group use without inventing a CUDA VRAM metric."""

    def __init__(
        self, process, output_directory, *, pgid, shared_pgid, interval_s=0.25
    ):
        self.process = process
        self.pgid = pgid
        self.shared_pgid = shared_pgid
        self.output_directory = output_directory
        self.interval_s = interval_s
        self.rows = []
        self.errors = []
        self.baseline_swapout_bytes = None
        self.fatal_error = None
        self.termination_signaled = False
        self._fatal_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        try:
            self.baseline_swapout_bytes = swapout_bytes()
        except Exception as error:
            self._probe_failure(error)

    def start(self):
        if self.fatal_error is None:
            self._sample()
        self.raise_if_fatal()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _terminate_owned_group(self):
        try:
            if self.process.poll() is not None:
                return
            if os.getpgid(self.process.pid) != self.pgid:
                self.errors.append("resource guard refused mismatched process group")
                return
            if self.shared_pgid:
                # The qualifier and server intentionally share the outer
                # resource guard's group. Never signal that group from within.
                self.process.terminate()
            else:
                os.killpg(self.pgid, signal.SIGTERM)
            self.termination_signaled = True
        except ProcessLookupError:
            return
        except Exception as error:
            if len(self.errors) < 10:
                self.errors.append(
                    f"resource guard termination {type(error).__name__}: {error}"
                )

    def _trip(self, reason):
        with self._fatal_lock:
            if self.fatal_error is not None:
                return
            self.fatal_error = reason
            self._stop.set()
            self._terminate_owned_group()

    def _probe_failure(self, error):
        detail = f"{type(error).__name__}: {error}"
        if len(self.errors) < 10:
            self.errors.append(detail)
        self._trip("Metal resource probe failed: " + detail)

    def raise_if_fatal(self):
        if self.fatal_error is not None:
            raise RuntimeError(self.fatal_error)

    def _sample(self):
        try:
            server_rss = round(group_rss_mib(self.pgid) * 1024 * 1024)
            collector_rss = 0
            if not self.shared_pgid:
                collector_text = subprocess.check_output(
                    ["/bin/ps", "-o", "rss=", "-p", str(os.getpid())],
                    text=True,
                    timeout=10,
                ).strip()
                collector_rss = int(collector_text) * 1024
            row = {
                "wall_time_s": time.time(),
                "monotonic_ns": time.monotonic_ns(),
                "collector_rss_bytes": collector_rss,
                "server_group_rss_bytes": server_rss,
                "group_rss_bytes": collector_rss + server_rss,
                "free_memory_percent": memory_free_percent(),
                "disk_free_mib": shutil.disk_usage(self.output_directory).free / 1048576,
                "swapout_bytes": swapout_bytes(),
            }
            self.rows.append(row)
            violations = []
            if row["group_rss_bytes"] > METAL_COMBINED_RSS_LIMIT_BYTES:
                violations.append("combined RSS exceeded 8 GiB")
            if row["free_memory_percent"] < METAL_MINIMUM_FREE_MEMORY_PERCENT:
                violations.append("free memory fell below 15 percent")
            if row["disk_free_mib"] < METAL_MINIMUM_DISK_FREE_MIB:
                violations.append("disk free fell below 4096 MiB")
            if row["swapout_bytes"] != self.baseline_swapout_bytes:
                violations.append("swapout growth was nonzero")
            if violations:
                self._trip("Metal resource guard failed: " + "; ".join(violations))
        except Exception as error:
            self._probe_failure(error)

    def _run(self):
        while not self._stop.wait(self.interval_s):
            self._sample()

    def finish(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self.fatal_error is None and self.process.poll() is None:
            self._sample()
        return {
            "source": "process-group+macos-unified-memory",
            "scope": "unified_process_group",
            "max_combined_rss_bytes": METAL_COMBINED_RSS_LIMIT_BYTES,
            "pgid": self.pgid,
            "shared_qualifier_pgid": self.shared_pgid,
            "samples": len(self.rows),
            "errors": self.errors,
            "fatal_error": self.fatal_error,
            "termination_signaled": self.termination_signaled,
            "baseline_swapout_bytes": self.baseline_swapout_bytes,
            "swapout_growth_bytes": max(
                (
                    row["swapout_bytes"] - self.baseline_swapout_bytes
                    for row in self.rows
                    if self.baseline_swapout_bytes is not None
                ),
                default=0,
            ),
            "minimum_free_memory_percent": min(
                (row["free_memory_percent"] for row in self.rows), default=None
            ),
            "minimum_disk_free_mib": min(
                (row["disk_free_mib"] for row in self.rows), default=None
            ),
            "peak_group_rss_bytes": max(
                (row["group_rss_bytes"] for row in self.rows), default=None
            ),
            "observations": self.rows,
        }


def _metal_test_paths(args, label):
    stem = args.output.with_suffix("")
    return (
        stem.with_name(stem.name + f".{label}.ready.json"),
        stem.with_name(stem.name + f".{label}.stop"),
    )


def _wait_metal_loader_headroom(
    timeout_seconds,
    receipt,
    *,
    memory_probe=memory_free_percent,
    swap_probe=swapout_bytes,
    monotonic=time.monotonic,
    wall_time=time.time,
    sleeper=time.sleep,
):
    """Require fresh loader headroom before every Metal server process exists."""
    if (
        type(receipt) is not dict
        or type(timeout_seconds) not in (int, float)
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ValueError("invalid Metal loader headroom wait")
    bounded_timeout = min(
        float(timeout_seconds), METAL_LOADER_HEADROOM_MAX_WAIT_SECONDS
    )
    started = monotonic()
    deadline = started + bounded_timeout
    receipt.update(
        {
            "source": "memory_pressure-Q",
            "required_free_memory_percent": METAL_LOADER_HEADROOM_PERCENT,
            "required_consecutive_samples": METAL_LOADER_HEADROOM_SAMPLES,
            "sample_interval_seconds": METAL_LOADER_HEADROOM_INTERVAL_SECONDS,
            "timeout_seconds": bounded_timeout,
            "passed": False,
            "samples": [],
        }
    )
    consecutive = 0
    try:
        baseline_swap = swap_probe()
        receipt["baseline_swapout_bytes"] = baseline_swap
        while True:
            observed_at = monotonic()
            free = memory_probe()
            swap = swap_probe()
            row = {
                "wall_time_s": wall_time(),
                "monotonic_seconds": observed_at,
                "free_memory_percent": free,
                "swapout_bytes": swap,
            }
            receipt["samples"].append(row)
            if swap != baseline_swap:
                raise RuntimeError(
                    "Metal loader headroom observed nonzero swapout growth"
                )
            consecutive = consecutive + 1 if free >= METAL_LOADER_HEADROOM_PERCENT else 0
            row["consecutive_passing_samples"] = consecutive
            if consecutive >= METAL_LOADER_HEADROOM_SAMPLES:
                receipt.update(
                    passed=True,
                    elapsed_seconds=monotonic() - started,
                    minimum_free_memory_percent=min(
                        sample["free_memory_percent"]
                        for sample in receipt["samples"]
                    ),
                    swapout_growth_bytes=0,
                )
                return receipt
            remaining = deadline - monotonic()
            if remaining < METAL_LOADER_HEADROOM_INTERVAL_SECONDS:
                raise TimeoutError(
                    "Metal loader headroom timed out before two fresh 76 percent samples"
                )
            sleeper(METAL_LOADER_HEADROOM_INTERVAL_SECONDS)
    except BaseException as error:
        receipt.update(
            passed=False,
            elapsed_seconds=max(0, monotonic() - started),
            error=f"{type(error).__name__}: {error}",
        )
        raise


def _portable_active_group_members(pgid):
    probe = subprocess.Popen(
        ["/bin/ps", "-axo", "pid=,pgid=,stat="],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        output, error = probe.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        probe.kill()
        probe.communicate()
        raise
    if probe.returncode != 0:
        raise subprocess.CalledProcessError(
            probe.returncode, probe.args, output=output, stderr=error
        )
    members = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) != 3:
            raise RuntimeError("malformed process-group listing")
        pid, group = map(int, fields[:2])
        if (
            pid != probe.pid
            and group == pgid
            and not fields[2].startswith(("Z", "X"))
        ):
            members.append(pid)
    return members


def _metal_server_topology():
    qualifier_pid = os.getpid()
    qualifier_pgid = os.getpgrp()
    inherits = qualifier_pid == qualifier_pgid
    return {
        "qualifier_pid": qualifier_pid,
        "pgid": qualifier_pgid if inherits else None,
        "shared_pgid": inherits,
        "start_new_session": not inherits,
    }


def _remaining_metal_server_group_members(pgid, shared_pgid):
    members = _portable_active_group_members(pgid)
    if shared_pgid:
        members = [pid for pid in members if pid != os.getpid()]
    return members


def _signal_metal_server(process, shared_pgid, signal_number):
    if shared_pgid:
        os.kill(process.pid, signal_number)
    else:
        os.killpg(process.pid, signal_number)


def _wait_metal_test_server(process, ready_path, log_path, args):
    deadline = time.monotonic() + min(args.timeout, 300)
    while not ready_path.exists():
        if process.poll() is not None:
            raise RuntimeError(f"Metal test server exited during startup; see {log_path}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Metal test server ready timeout; see {log_path}")
        time.sleep(0.1)
    if ready_path.is_symlink() or ready_path.stat().st_size > 65536:
        raise RuntimeError("invalid Metal test server ready file")
    ready = json.loads(ready_path.read_text())
    identity = ready.get("loaded_identity") if type(ready) is dict else None
    if (
        type(ready) is not dict
        or ready.get("schema") != METAL_TEST_SERVER_SCHEMA
        or ready.get("backend") != "metal"
        or ready.get("metal_enabled") is not True
        or ready.get("cuda_enabled") is not False
        or ready.get("pid") != process.pid
        or ready.get("port") != args.port
        or ready.get("model") != args.model
        or type(identity) is not dict
        or set(identity) != PORTABLE_IDENTITY_FIELDS
        or any(
            not isinstance(value, str)
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in identity.values()
        )
        or type(ready.get("load_generation")) is not int
        or ready["load_generation"] <= 0
    ):
        raise RuntimeError("invalid Metal test server ready attestation")
    with urllib.request.urlopen(f"http://127.0.0.1:{args.port}/healthz", timeout=2) as response:
        if response.status != 200:
            raise RuntimeError("Metal test server health check failed")
    return ready


def validate_metal_memory(memory, max_rss_bytes=METAL_COMBINED_RSS_LIMIT_BYTES):
    if (
        type(memory) is not dict
        or memory.get("scope") != "unified_process_group"
        or memory.get("max_combined_rss_bytes") != max_rss_bytes
        or type(memory.get("samples")) is not int
        or memory["samples"] < 2
        or memory.get("errors")
        or type(memory.get("peak_group_rss_bytes")) is not int
        or not 0 < memory["peak_group_rss_bytes"] <= max_rss_bytes
        or type(memory.get("minimum_free_memory_percent")) not in (int, float)
        or memory["minimum_free_memory_percent"] < 15
        or type(memory.get("minimum_disk_free_mib")) not in (int, float)
        or memory["minimum_disk_free_mib"] < 4096
        or memory.get("swapout_growth_bytes") != 0
    ):
        raise AssertionError("Metal unified-memory resource guard failed")


def validate_backend_configuration(args):
    if (args.backend == "metal") != (args.server_kind == "metal-test"):
        raise ValueError("--backend metal requires --server-kind metal-test and vice versa")
    if args.backend == "metal" and (
        args.encoding not in ("f16", "fp8")
        or getattr(args, "experimental_rotated4", False)
    ):
        raise ValueError("Metal qualification supports only f16 and fp8 checkpoints")
    if args.backend == "metal" and getattr(args, "model", METAL_TEST_MODEL) != METAL_TEST_MODEL:
        raise ValueError("Metal checkpoint qualification is pinned to Gemma4 E4B")
    test_minimum = getattr(args, "qualification_minimum_savings", None)
    if test_minimum is not None:
        if args.backend != "metal" or args.server_kind != "metal-test":
            raise ValueError(
                "--metal-test-minimum-savings is exclusive to the Metal test server"
            )
        if not math.isfinite(test_minimum) or not 1 < test_minimum <= 4:
            raise ValueError(
                "--metal-test-minimum-savings must be finite, greater than 1, and at most 4"
            )


def prepare_headers(args):
    headers = {
        "Content-Type": "application/json",
        "X-Antfly-KV-Encoding": getattr(args, "encoding", "f16"),
    }
    test_minimum = getattr(args, "qualification_minimum_savings", None)
    if test_minimum is not None:
        if args.backend != "metal" or args.server_kind != "metal-test":
            raise ValueError("test-only savings header requested outside Metal test server")
        if not math.isfinite(test_minimum) or not 1 < test_minimum <= 4:
            raise ValueError("invalid test-only savings threshold")
        headers[METAL_TEST_MINIMUM_SAVINGS_HEADER] = str(test_minimum)
    return headers


def normalize_request_body(args, body):
    """Bind Metal checkpoint qualification to direct eager execution."""
    if getattr(args, "backend", "cuda") != "metal":
        return body
    mode = body.get("mode")
    if mode not in (None, "eager"):
        raise ValueError(
            f"Metal native checkpoint qualification requires mode eager, got {mode!r}"
        )
    if mode == "eager":
        return body
    normalized = copy.deepcopy(body)
    normalized["mode"] = "eager"
    return normalized


def prepare_request_body(args, body):
    """Prepare never streams; preserve the caller's public measurement body."""
    normalized = normalize_request_body(args, body)
    prepared = copy.deepcopy(normalized)
    prepared["stream"] = False
    return prepared


def metal_ledger_start(args, record):
    if getattr(args, "backend", "cuda") != "metal":
        return None
    path = Path(record["log_path"])
    return path.stat().st_size


def metal_ledger_finish(
    args,
    record,
    offset,
    expected_route,
    *,
    require_binding,
    expected_representation=None,
    required_phase=None,
):
    if offset is None:
        return None
    from metal_checkpoint_qualification_ledger import (
        generation_contract,
        wait_ledger,
    )

    rows = wait_ledger(Path(record["log_path"]), offset)
    proof = generation_contract(
        rows,
        expected_route,
        require_binding=require_binding,
        expected_representation=expected_representation,
    )
    current = proof["committed"]
    if not require_binding and (
        current.get("binding") is not False
        or current.get("binding_representation") != "none"
        or any(
            current.get(field) != 0
            for field in (
                "binding_allocated_bytes",
                "binding_fp8_heads",
                "binding_fp8_rows",
                "binding_exact_resident_bytes",
                "binding_encoded_resident_bytes",
                "binding_metadata_resident_bytes",
            )
        )
    ):
        raise AssertionError("exact Metal request unexpectedly installed checkpoint state")
    if required_phase is not None:
        phases = [
            row
            for row in rows
            if row.get("schema") == "antfly.metal-checkpoint-phase.v1"
            and row.get("phase") == required_phase
        ]
        if (
            len(phases) != 1
            or (
                phases[0].get("request"),
                phases[0].get("route"),
                phases[0].get("owner_id"),
            )
            != (
                current.get("request"),
                current.get("route"),
                current.get("owner_id"),
            )
            or phases[0].get("binding") is not False
            or phases[0].get("binding_representation") != "none"
        ):
            raise AssertionError("checkpoint preparation lacks exact source ledger")
    record.setdefault("owner_ledgers", []).append(proof)
    return proof


@contextlib.contextmanager
def server(args, label, evidence):
    from native_checkpoint_unified_host import qualification_admission_capacity

    capacity = qualification_admission_capacity(args)
    if getattr(args, "server_kind", "inference") == "metal-test":
        ready_path, stop_path = _metal_test_paths(args, label)
        for path in (ready_path, stop_path):
            if path.exists() or path.is_symlink():
                raise FileExistsError(path)
        environment = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith("ANTFLY_INFERENCE_ATTENTION_MATCH_")
        }
        overrides = {
            "ANTFLY_METAL_CHECKPOINT_MODELS_ROOT": str(args.models_dir.resolve()),
            "ANTFLY_METAL_CHECKPOINT_PORT": str(args.port),
            "ANTFLY_METAL_CHECKPOINT_READY_FILE": str(ready_path.resolve()),
            "ANTFLY_METAL_CHECKPOINT_STOP_FILE": str(stop_path.resolve()),
        }
        environment.update(overrides)
        command = [
            str(args.server.resolve()),
            "--test-filter",
            "context Metal native checkpoint qualification server",
        ]
        record = {
            "command": command,
            "environment_overrides": overrides,
            "qualification_profile": "metal_test_native_checkpoint_v1",
            "requests": [],
        }
        evidence[label] = record
        loader_headroom = {}
        record["loader_headroom"] = loader_headroom
        _wait_metal_loader_headroom(
            min(float(args.timeout), METAL_LOADER_HEADROOM_MAX_WAIT_SECONDS),
            loader_headroom,
        )
        log_path = args.output.with_suffix("." + label + ".log")
        record["log_path"] = str(log_path)
        sampler = None
        with log_path.open("x") as log:
            topology = _metal_server_topology()
            record["process_topology"] = topology
            process = subprocess.Popen(
                command,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=topology["start_new_session"],
            )
            server_pgid = topology["pgid"] if topology["shared_pgid"] else process.pid
            primary_error = None
            ready_observed = False
            try:
                if os.getpgid(process.pid) != server_pgid:
                    raise RuntimeError("Metal test server process group changed at launch")
                sampler = MetalProcessSampler(
                    process,
                    args.output.parent.resolve(),
                    pgid=server_pgid,
                    shared_pgid=topology["shared_pgid"],
                )
                sampler.start()
                ready = _wait_metal_test_server(process, ready_path, log_path, args)
                ready_observed = True
                sampler.raise_if_fatal()
                record["worker_pid"] = process.pid
                record["ready"] = ready
                record["ready_sha256"] = sha256_file(ready_path)
                yield record
                sampler.raise_if_fatal()
                if ready_path.is_symlink() or sha256_file(ready_path) != record["ready_sha256"]:
                    raise RuntimeError("Metal test server ready attestation changed")
            except BaseException as error:
                primary_error = error
                raise
            finally:
                cleanup_errors = []
                record["startup_returncode"] = (
                    None if ready_observed else process.poll()
                )
                if sampler is not None:
                    try:
                        record["memory"] = sampler.finish()
                    except BaseException as error:
                        cleanup_errors.append(
                            f"memory sampler cleanup failed: {type(error).__name__}: {error}"
                        )
                graceful = False
                if process.poll() is None:
                    try:
                        with stop_path.open("x") as stop:
                            stop.write("stop\n")
                        process.wait(timeout=30)
                        graceful = process.returncode == 0
                    except (OSError, subprocess.TimeoutExpired):
                        graceful = False
                if process.poll() is None:
                    try:
                        _signal_metal_server(
                            process, topology["shared_pgid"], signal.SIGTERM
                        )
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        _signal_metal_server(
                            process, topology["shared_pgid"], signal.SIGKILL
                        )
                        process.wait(timeout=10)
                deadline = time.monotonic() + 30
                while (
                    _remaining_metal_server_group_members(
                        server_pgid,
                        topology["shared_pgid"],
                    )
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.05)
                remaining = _remaining_metal_server_group_members(
                    server_pgid,
                    topology["shared_pgid"],
                )
                record["graceful_stop"] = graceful
                record["remaining_group_members"] = remaining
                record["stopped"] = not remaining
                record["returncode"] = process.returncode
                if sampler is not None and sampler.fatal_error is not None:
                    cleanup_errors.append(sampler.fatal_error)
                if not graceful or remaining:
                    cleanup_errors.append(
                        "Metal qualification server did not drain cleanly"
                    )
                record["cleanup_errors"] = cleanup_errors
                if cleanup_errors and primary_error is None:
                    raise RuntimeError("; ".join(cleanup_errors))
        return
    if getattr(args, "server_kind", "inference") == "standalone":
        from native_checkpoint_unified_host import unified_server

        with unified_server(args, label, evidence, SCRIPTS) as record:
            yield record
        return
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("ANTFLY_INFERENCE_ATTENTION_MATCH_")
    }
    overrides = {
        "ANTFLY_INFERENCE_PREFERRED_BACKEND": "cuda",
        "ANTFLY_INFERENCE_REQUIRED_BACKEND": "cuda",
        "ANTFLY_SERVER_DECODE_GRAPH_REPLAY": "off",
        "ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS": "0",
        "ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING": "1",
    }
    overrides["ANTFLY_INFERENCE_EXPERIMENTAL_ROTATED4_CHECKPOINTS"] = str(
        int(getattr(args, "experimental_rotated4", False))
    )
    environment.update(overrides)
    if getattr(args, "generation_timing", False):
        overrides["TERMITE_SERVER_GENERATE_TIMING"] = "1"
        environment["TERMITE_SERVER_GENERATE_TIMING"] = "1"
    command = [
        "bash",
        str(SCRIPTS / "with_gemma4_qat_cuda_tuning.sh"),
        str(args.server.resolve()),
        "run",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
        "--models-dir",
        str(args.models_dir.resolve()),
        "--max-concurrent-requests",
        str(capacity),
        "--host-budget-mb",
        "8192",
        "--backend-budget-mb",
        "18432",
        "--combined-budget-mb",
        "26624",
        "--kv-budget-mb",
        "4096",
        "--scratch-budget-mb",
        "6144",
    ]
    record = {"command": command, "environment_overrides": overrides, "requests": []}
    evidence[label] = record
    log_path = args.output.with_suffix("." + label + ".log")
    sampler = None
    with log_path.open("w") as log:
        process = subprocess.Popen(
            command,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            wait_server_ready(process, log_path, args.port)
            worker = discover_worker_pid(process.pid, args.server)
            record["worker_pid"] = worker
            sampler = CudaProcessSampler(worker)
            sampler.start()
            yield record
        finally:
            if sampler is not None:
                record["memory"] = sampler.finish()
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=30)
            deadline = time.monotonic() + 30
            while active_group_members(process.pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            if active_group_members(process.pid):
                raise RuntimeError("qualification worker group did not exit")
            record["stopped"] = True


def response_headers(response, record, args):
    headers = {k.lower(): v for k, v in response.getheaders()}
    backend = getattr(args, "backend", "cuda")
    if headers.get("x-antfly-inference-backend") != backend:
        raise AssertionError("missing " + backend + " attestation")
    if int(headers.get("x-antfly-inference-pid", "0")) != record["worker_pid"]:
        raise AssertionError("worker changed")
    return headers


def generate(args, body, record):
    body = normalize_request_body(args, body)
    ledger_offset = metal_ledger_start(args, record)
    connection = http.client.HTTPConnection(
        "127.0.0.1", args.port, timeout=args.timeout
    )
    try:
        start = time.monotonic()
        connection.request(
            "POST",
            "/ai/v1/generate",
            json.dumps(body).encode(),
            {"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(
                f"baseline HTTP {response.status}: {response.read(65536)!r}"
            )
        headers = {k.lower(): v for k, v in response.getheaders()}
        result = read_response(response, False)
        record["requests"].append(
            {
                "kind": "baseline",
                "seconds": time.monotonic() - start,
                "headers": headers,
                "result": result,
            }
        )
        response_headers(response, record, args)
        metal_ledger_finish(
            args, record, ledger_offset, "exact", require_binding=False
        )
        return result
    finally:
        connection.close()


def prepare(args, body, record):
    body = prepare_request_body(args, body)
    ledger_offset = metal_ledger_start(args, record)
    connection = http.client.HTTPConnection(
        "127.0.0.1", args.port, timeout=args.timeout
    )
    try:
        start = time.monotonic()
        connection.request(
            "POST",
            "/ai/v1/generate/context/checkpoint",
            json.dumps(body).encode(),
            prepare_headers(args),
        )
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(
                f"prepare HTTP {response.status}: {response.read(65536)!r}"
            )
        headers = response_headers(response, record, args)
        if headers.get("content-type", "").split(";")[0] != CONTENT_TYPE:
            raise AssertionError("wrong export content type")
        result = receive_checkpoint(
            response,
            args.checkpoint,
            allow_rotated4=getattr(args, "encoding", "f16") == "rotated4",
        )
        metadata = result["metadata"]
        if (
            getattr(args, "backend", "cuda") == "metal"
            and result.get("identity") != record["ready"]["loaded_identity"]
        ):
            raise AssertionError("checkpoint identity differs from loaded Metal model")
        encoding = getattr(args, "encoding", "f16")
        if (
            metadata.get("encoding") != encoding
            or metadata.get("wire_version")
            != {"f32": 2, "f16": 3, "fp8": 3, "rotated4": 4}[encoding]
        ):
            raise AssertionError("checkpoint did not use the requested encoding")
        if (
            metadata["model"] != args.model
            or metadata["outcome"] != args.expected_outcome
        ):
            raise AssertionError(metadata)
        if metadata["outcome"] == "compacted":
            savings = metadata["global_savings_ratio"]
            if not math.isfinite(savings) or savings < args.minimum_savings:
                raise AssertionError(metadata)
        if encoding == "rotated4":
            from qualify_attention_match_queries import validate_quantization_evidence

            validate_quantization_evidence(
                [metadata],
                encoding,
                require_actual=args.expected_outcome == "compacted",
            )
        if metadata.get("fp8_heads", 0) < getattr(args, "minimum_fp8_heads", 0):
            raise AssertionError("FP8 campaign did not exercise enough quantized heads")
        record["requests"].append(
            {
                "kind": "prepare",
                "seconds": time.monotonic() - start,
                "headers": headers,
                "result": result,
            }
        )
        metal_ledger_finish(
            args,
            record,
            ledger_offset,
            "checkpoint_prepare",
            require_binding=False,
            required_phase="checkpoint_source",
        )
        return result
    finally:
        connection.close()


def resume(args, body, record, prepared, label, expected_status=200, mutate=None):
    body = normalize_request_body(args, body)
    ledger_offset = metal_ledger_start(args, record)
    if (
        getattr(args, "backend", "cuda") == "metal"
        and prepared.get("identity") != record["ready"]["loaded_identity"]
    ):
        raise AssertionError("checkpoint identity differs from loaded Metal model")
    connection = http.client.HTTPConnection(
        "127.0.0.1", args.port, timeout=args.timeout
    )
    try:
        start = time.monotonic()
        response = send_resume(
            connection, "/ai/v1/generate/context/resume", body, args.checkpoint, mutate
        )
        if response.status != expected_status:
            raise AssertionError(
                f"{label} HTTP {response.status}, wanted {expected_status}: {response.read(65536)!r}"
            )
        if expected_status != 200:
            if response.getheader("Content-Type", "").startswith("text/event-stream"):
                raise AssertionError("invalid checkpoint started SSE")
            result = json.loads(response.read(65536))
            if not result.get("error"):
                raise AssertionError("missing rejection error")
        else:
            response_headers(response, record, args)
            result = read_response(response, body.get("stream", False))
        record["requests"].append(
            {
                "kind": label,
                "status": response.status,
                "seconds": time.monotonic() - start,
                "result": result,
            }
        )
        if expected_status == 200:
            if (
                result.get("usage", {}).get("cached_prompt_tokens")
                != prepared["metadata"]["logical_tokens"]
            ):
                raise AssertionError("resume did not attest reuse of saved prefix")
            validate_fixture_choice(result["choices"][0], args.fixture)
            metal_ledger_finish(
                args,
                record,
                ledger_offset,
                "checkpoint_resume",
                require_binding=True,
                expected_representation=getattr(args, "encoding", "f16"),
            )
        return result
    finally:
        connection.close()


def python_sdk_roundtrip(args, body, record, prepared):
    # Run this qualifier with py/packages/sdk/.venv/bin/python when enabled.
    sys.path.insert(0, str(REPO / "py/packages/sdk/src"))
    from antfly import AntflyClient
    from antfly.client_generated.models import InferenceGenerateRequest

    client = AntflyClient(f"http://127.0.0.1:{args.port}", timeout=args.timeout)
    http = client._client.get_httpx_client()
    observed_headers = []

    def attest(response):
        headers = dict(response.headers)
        if response.status_code == 200:
            backend = getattr(args, "backend", "cuda")
            if headers.get("x-antfly-inference-backend") != backend:
                raise AssertionError("SDK missing " + backend + " attestation")
            if int(headers.get("x-antfly-inference-pid", "0")) != record["worker_pid"]:
                raise AssertionError("SDK worker changed")
        observed_headers.append({"status": response.status_code, "headers": headers})

    http.event_hooks["response"].append(attest)
    temporary = args.checkpoint.with_name(args.checkpoint.name + ".sdk.kv")
    if temporary.exists():
        raise FileExistsError(temporary)
    start = time.monotonic()
    try:
        request = InferenceGenerateRequest.from_dict(normalize_request_body(args, body))
        ledger_offset = metal_ledger_start(args, record)
        result = client.resume_native_kv_checkpoint(request, args.checkpoint).to_dict()
        metal_ledger_finish(
            args,
            record,
            ledger_offset,
            "checkpoint_resume",
            require_binding=True,
            expected_representation=getattr(args, "encoding", "f16"),
        )
        validate_fixture_choice(result["choices"][0], args.fixture)
        if (
            result["usage"]["cached_prompt_tokens"]
            != prepared["metadata"]["logical_tokens"]
        ):
            raise AssertionError("SDK resume did not reuse saved prefix")
        ledger_offset = metal_ledger_start(args, record)
        exported = client.prepare_native_kv_checkpoint(
            request, temporary, encoding=getattr(args, "encoding", "f16")
        )
        metal_ledger_finish(
            args,
            record,
            ledger_offset,
            "checkpoint_prepare",
            require_binding=False,
            required_phase="checkpoint_source",
        )
        if exported.sha256 != prepared["file_sha256"]:
            raise AssertionError(
                "SDK export differs from identical raw HTTP preparation"
            )
        record["requests"].append(
            {
                "kind": "python_sdk_roundtrip",
                "seconds": time.monotonic() - start,
                "headers": observed_headers,
                "result": result,
                "checkpoint_sha256": exported.sha256,
                "metadata": dict(exported.metadata),
            }
        )
    finally:
        http.close()
        temporary.unlink(missing_ok=True)


def reject_append_limit(args, body, record, prepared, label):
    # Request streaming so a rejection must precede both generation and SSE headers.
    request = copy.deepcopy(body)
    request["stream"] = True
    result = resume(args, request, record, prepared, label, expected_status=400)
    if result.get("message") != "KvCheckpointAppendLimit":
        raise AssertionError(f"append rejected for an unexpected reason: {result}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["cuda", "metal"], default="cuda")
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument(
        "--server-kind", choices=["inference", "standalone", "metal-test"], default="inference"
    )
    parser.add_argument("--models-dir", type=Path, default=Path(".models"))
    parser.add_argument("--model", default="google/gemma-4-E4B-it-qat-q4_0-gguf")
    parser.add_argument("--port", type=int, default=8500)
    parser.add_argument("--padding-lines", type=int, default=742)
    parser.add_argument(
        "--encoding", choices=["f32", "f16", "fp8", "rotated4"], default="f16"
    )
    parser.add_argument("--experimental-rotated4", action="store_true")
    parser.add_argument("--fixture", choices=["fact", "tool_call"], default="fact")
    parser.add_argument(
        "--expected-outcome",
        choices=["compacted", "unchanged_insufficient_savings"],
        default="compacted",
    )
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--python-sdk", action="store_true")
    parser.add_argument(
        "--append-mode", choices=["multichunk", "limit_rejection"], default="multichunk"
    )
    parser.add_argument("--minimum-savings", type=float, default=1.5)
    parser.add_argument(
        "--metal-test-minimum-savings",
        dest="qualification_minimum_savings",
        type=float,
        help=(
            "Test-only server-side acceptance threshold; emitted only for "
            "--backend metal --server-kind metal-test"
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    args = parser.parse_args()
    try:
        validate_backend_configuration(args)
    except ValueError as error:
        parser.error(str(error))
    args.minimum_fp8_heads = (
        1 if args.encoding == "fp8" and args.expected_outcome == "compacted" else 0
    )
    if not math.isfinite(args.minimum_savings) or not 1.5 <= args.minimum_savings <= 4:
        parser.error("--minimum-savings must be finite and between 1.5 and 4")
    if args.append_mode == "limit_rejection" and args.fixture != "fact":
        parser.error("--append-mode limit_rejection requires --fixture fact")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    evidence = {
        "passed": False,
        "scope": "public native checkpoint roundtrip and restart; not full production qualification",
        "server_kind": args.server_kind,
        "encoding": args.encoding,
        "append_mode": args.append_mode,
        "minimum_savings": args.minimum_savings,
        "source": git_provenance(REPO),
        "binary_sha256": sha256_file(args.server),
        "runner_sha256": sha256_file(Path(__file__)),
        "transport_sha256": sha256_file(
            Path(__file__).with_name("native_checkpoint_transport.py")
        ),
    }
    if args.backend == "metal":
        evidence.update(
            backend="metal",
            qualification_profile="metal_test_native_checkpoint_v1",
            production_ready=False,
            qualification_minimum_savings=args.qualification_minimum_savings,
            execution_mode="eager",
        )
    try:
        original = next(
            iter(requests(args.model, "f16", [args.padding_lines], False, args.fixture))
        )
        public = copy.deepcopy(original)
        public.pop("context_management")
        public.pop("cache_dtype")
        with server(args, "exporter", evidence) as record:
            baseline = generate(args, original, record)
            validate_fixture_choice(baseline["choices"][0], args.fixture)
            if (
                args.append_mode == "limit_rejection"
                and baseline["usage"]["prompt_tokens"] < 32000
            ):
                raise AssertionError(
                    "limit-rejection workload must exercise at least 32000 prompt tokens"
                )
            prepared = prepare(args, public, record)
            first = resume(args, public, record, prepared, "same_process")
            appended = None
            if args.fixture == "fact":
                appended = copy.deepcopy(public)
                appended["messages"] += [
                    {"role": "assistant", "content": "Acknowledged."},
                    {
                        "role": "user",
                        "content": "This follow-up log entry has no changes.\n" * 128
                        + "Retrieve the access code from the original log. Reply with only the access code.",
                    },
                ]
                if args.append_mode == "limit_rejection":
                    reject_append_limit(
                        args, appended, record, prepared, "appended_limit_rejection"
                    )
                else:
                    full_append = copy.deepcopy(appended)
                    full_append["cache_dtype"] = "f16"
                    # Ordinary generation exposes runtime headers when context
                    # diagnostics are requested, as on the original baseline.
                    full_append["context_management"] = copy.deepcopy(
                        original["context_management"]
                    )
                    append_baseline = generate(args, full_append, record)
                    validate_fixture_choice(append_baseline["choices"][0], args.fixture)
                    append_result = resume(
                        args, appended, record, prepared, "appended_multichunk"
                    )
                    suffix_tokens = (
                        append_result["usage"]["prompt_tokens"]
                        - prepared["metadata"]["logical_tokens"]
                    )
                    if suffix_tokens < 512:
                        raise AssertionError(
                            "append did not cover multiple 256-token prefill chunks"
                        )
                    record["appended_suffix_tokens"] = suffix_tokens
                stream = copy.deepcopy(public)
                stream["stream"] = True
                for i in range(2):
                    resume(args, stream, record, prepared, f"sse_{i}")
            wrong = copy.deepcopy(public)
            wrong["messages"][0]["content"] = (
                "Changed prefix.\n" + wrong["messages"][0]["content"]
            )
            wrong["stream"] = True
            resume(args, wrong, record, prepared, "wrong_prefix", 400)
            size = args.checkpoint.stat().st_size

            def corrupt(offset, block):
                return (
                    block[:-1] + bytes([block[-1] ^ 1])
                    if offset + len(block) == size
                    else block
                )

            bad = copy.deepcopy(public)
            bad["stream"] = True
            resume(args, bad, record, prepared, "checksum_corruption", 400, corrupt)
            resume(args, public, record, prepared, "recovery_after_rejections")
            if args.python_sdk:
                python_sdk_roundtrip(args, public, record, prepared)
        with server(args, "importer", evidence) as record:
            if record["worker_pid"] == evidence["exporter"]["worker_pid"]:
                raise AssertionError("restart reused worker PID")
            restored = resume(args, public, record, prepared, "cross_process")
            if canonical_choices(first["choices"]) != canonical_choices(
                restored["choices"]
            ):
                raise AssertionError("restart changed deterministic continuation")
            if appended is not None:
                if args.append_mode == "limit_rejection":
                    reject_append_limit(
                        args,
                        appended,
                        record,
                        prepared,
                        "cross_process_limit_rejection",
                    )
                    resume(
                        args,
                        public,
                        record,
                        prepared,
                        "cross_process_recovery_after_limit",
                    )
                else:
                    restored_append = resume(
                        args,
                        appended,
                        record,
                        prepared,
                        "cross_process_multichunk_append",
                    )
                    if canonical_choices(append_result["choices"]) != canonical_choices(
                        restored_append["choices"]
                    ):
                        raise AssertionError(
                            "restart changed appended deterministic continuation"
                        )
        if sha256_file(args.checkpoint) != prepared["file_sha256"]:
            raise AssertionError("checkpoint mutated")
        if git_provenance(REPO) != evidence["source"]:
            raise AssertionError("source changed during qualification")
        if sha256_file(args.server) != evidence["binary_sha256"]:
            raise AssertionError("server changed during qualification")
        for label in ["exporter", "importer"]:
            memory = evidence[label]["memory"]
            if args.backend == "metal":
                validate_metal_memory(memory)
                if not evidence[label].get("graceful_stop") or evidence[label].get(
                    "remaining_group_members"
                ):
                    raise AssertionError(f"{label}: Metal test server did not drain")
            else:
                if memory["samples"] < 2 or memory["errors"] or not memory["gpu_uuids"]:
                    raise AssertionError(f"{label}: incomplete memory evidence")
                for key, limit in [
                    ("peak_host_rss_bytes", 8 * 1024**3),
                    ("peak_gpu_memory_bytes", 18 * 1024**3),
                ]:
                    if memory[key] is None or memory[key] > limit:
                        raise AssertionError(
                            f"{label}: {key}={memory[key]} exceeds {limit}"
                        )
        evidence["python_sdk_exercised"] = args.python_sdk
        evidence["passed"] = True
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
    finally:
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(
        json.dumps(
            {
                "passed": evidence["passed"],
                "error": evidence.get("error"),
                "output": str(args.output),
            }
        ),
        flush=True,
    )
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
