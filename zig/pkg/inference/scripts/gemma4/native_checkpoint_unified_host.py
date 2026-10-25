"""Real standalone listener + isolated inference-worker qualification host."""

import contextlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

from qualify_attention_match_queries import active_group_members
from qualify_context_management import CudaProcessSampler, sample_cuda_process


def qualification_admission_capacity(args):
    """Finite weighted capacity; overlap campaigns must budget both workloads."""
    capacity = getattr(args, "inference_admission_capacity", 2)
    if type(capacity) is not int or capacity <= 0:
        raise ValueError("qualification requires a positive finite admission capacity")
    return capacity


def discover_unified_worker(parent_pid, binary, proc_root=Path("/proc")):
    children = set()
    for task in (proc_root / str(parent_pid) / "task").iterdir():
        try:
            children.update(int(pid) for pid in (task / "children").read_text().split())
        except (FileNotFoundError, ProcessLookupError):
            pass
    matches = []
    expected = binary.resolve()
    for pid in children:
        try:
            process = proc_root / str(pid)
            if (process / "exe").resolve(strict=True) != expected:
                continue
            argv = (process / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
            if argv[1:] == [b"inference", b"_worker"]:
                matches.append(pid)
        except (FileNotFoundError, ProcessLookupError):
            pass
    if len(matches) != 1:
        raise RuntimeError(
            f"expected one real standalone inference _worker child, found {len(matches)}"
        )
    return matches[0]


def sum_listener_rss(row, listener_pid, proc_root=Path("/proc")):
    result = dict(row)
    values = [
        line
        for line in (proc_root / str(listener_pid) / "status").read_text().splitlines()
        if line.startswith("VmRSS:")
    ]
    if len(values) != 1:
        raise RuntimeError("missing listener RSS")
    parent_bytes = int(values[0].split()[1]) * 1024
    result.update(
        listener_pid=listener_pid,
        listener_host_rss_bytes=parent_bytes,
        worker_host_rss_bytes=row["host_rss_bytes"],
        host_rss_bytes=parent_bytes + row["host_rss_bytes"],
    )
    return result


class UnifiedSampler(CudaProcessSampler):
    def __init__(self, worker_pid, listener_pid):
        super().__init__(worker_pid)
        self.listener_pid = listener_pid

    def _sample(self):
        try:
            self.rows.append(
                sum_listener_rss(sample_cuda_process(self.pid), self.listener_pid)
            )
        except Exception as error:
            if len(self.errors) < 10:
                self.errors.append(f"{type(error).__name__}: {error}")

    def finish(self):
        result = super().finish()
        result["host_rss_scope"] = (
            "sum of standalone listener and isolated worker RSS per sample"
        )
        for role in ("listener", "worker"):
            result[f"peak_{role}_host_rss_bytes"] = max(
                (r[f"{role}_host_rss_bytes"] for r in self.rows), default=None
            )
        return result


def standalone_command(args, scripts, data_dir):
    return [
        "bash",
        str(scripts / "with_gemma4_qat_cuda_tuning.sh"),
        str(args.server.resolve()),
        "standalone",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
        "--health-port",
        str(args.port + 1),
        "--control-tick-ms",
        "5",
        "--models-dir",
        str(args.models_dir.resolve()),
        "--config",
        str(data_dir / "qualification.json"),
        "--data-dir",
        str(data_dir),
        "--replica-root-dir",
        str(data_dir / "replicas"),
        "--replica-catalog-path",
        str(data_dir / "catalog.txt"),
        "--snapshot-root-dir",
        str(data_dir / "snapshots"),
        "--inference-host-budget-mb",
        "8192",
        "--inference-backend-budget-mb",
        "18432",
        "--inference-combined-budget-mb",
        "26624",
        "--inference-kv-budget-mb",
        "4096",
        "--inference-scratch-budget-mb",
        "6144",
    ]


def wait_ready(process, port, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("standalone server exited during startup")
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/readyz", timeout=1
            ) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise TimeoutError("standalone listener did not become ready")


def finish_standalone(record, process, sampler, graceful_timeout=45, kill_timeout=30):
    """Always retire owned processes, recording failures without masking the request."""
    errors = []
    if sampler is not None:
        try:
            record["memory"] = sampler.finish()
        except Exception as error:
            errors.append(f"memory sampling: {type(error).__name__}: {error}")
    forced = False

    def kill_group():
        nonlocal forced
        forced = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def wait_group(timeout):
        deadline = time.monotonic() + timeout
        while active_group_members(process.pid):
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.01)
        return True

    try:
        # Give the listener its normal opportunity to retire its worker first.
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=graceful_timeout)
        except subprocess.TimeoutExpired:
            errors.append("standalone listener required forced shutdown")
            kill_group()
            process.wait(timeout=kill_timeout)
        if not wait_group(kill_timeout):
            errors.append("standalone descendants required forced shutdown")
            kill_group()
            if not wait_group(kill_timeout):
                errors.append("standalone group survived SIGKILL")
    except Exception as error:
        errors.append(f"process cleanup: {type(error).__name__}: {error}")
        try:
            kill_group()
            process.wait(timeout=kill_timeout)
            if not wait_group(kill_timeout):
                errors.append("standalone group survived final cleanup")
        except Exception as final_error:
            errors.append(
                f"final process cleanup: {type(final_error).__name__}: {final_error}"
            )
    try:
        stopped = process.poll() is not None and not active_group_members(process.pid)
    except Exception as error:
        stopped = False
        errors.append(f"process verification: {type(error).__name__}: {error}")
    record.update(stopped=stopped, forced_kill=forced, returncode=process.returncode)
    if not stopped:
        errors.append("standalone cleanup unverified")
    if process.returncode != 0:
        errors.append(f"standalone unexpected exit: {process.returncode}")
    if errors:
        record["cleanup_errors"] = errors
    return errors


@contextlib.contextmanager
def unified_server(args, label, evidence, scripts):
    data_dir = args.output.resolve().with_suffix(f".{label}.data")
    data_dir.mkdir(exist_ok=False)
    config = {
        "admission": {
            "inference": {
                "max_concurrent_requests": qualification_admission_capacity(args)
            }
        }
    }
    (data_dir / "qualification.json").write_text(json.dumps(config) + "\n")
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("ANTFLY_INFERENCE_ATTENTION_MATCH_")
    }
    # This qualification must re-exec the exact production binary under test.
    environment.pop("ANTFLY_INFERENCE_WORKER", None)
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
    if getattr(args, "generation_timing", False):
        overrides["TERMITE_SERVER_GENERATE_TIMING"] = "1"
    environment.update(overrides)
    command = standalone_command(args, scripts, data_dir)
    record = {
        "command": command,
        "environment_overrides": overrides,
        "requests": [],
        "server_kind": "standalone",
        "config": config,
        "data_dir": str(data_dir),
    }
    evidence[label] = record
    sampler = None
    with args.output.with_suffix(f".{label}.log").open("w") as log:
        process = subprocess.Popen(
            command,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            wait_ready(process, args.port)
            worker = discover_unified_worker(process.pid, args.server)
            record.update(
                listener_pid=process.pid,
                worker_pid=worker,
                worker_command="inference _worker",
            )
            sampler = UnifiedSampler(worker, process.pid)
            sampler.start()
            yield record
        finally:
            primary_error = sys.exc_info()[1]
            errors = finish_standalone(record, process, sampler)
            if errors and primary_error is None:
                raise RuntimeError("; ".join(errors))
