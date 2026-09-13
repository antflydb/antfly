"""Bounded, synchronous supervision for the local Metal benchmark protocol.

No numerical runtime is imported. RSS is the sampled sum over owned process
creation identities, including resident caches; shared pages may count twice.
The original process group is inspected even after its leader exits. Previously
observed descendants that create another session remain owned. This is process
supervision for our workers, not a sandbox against an escaping double-fork.

Only the direct Popen child can be reaped here. Cleanup reports descendant exit
and any zombies separately, without claiming to have waited on nonchildren.
"""
from __future__ import annotations

import contextlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time
from typing import Any


MAX_RESPONSE_BYTES = 4 * 1024**2
MAX_COMMAND_BYTES = 2048
MAX_LOG_BYTES = 64 * 1024**2
MAX_PROCESSES = 64
MAX_COMMANDS = 4096
POLL_SECONDS = 0.05
TERM_SECONDS = 2.0
KILL_SECONDS = 2.0
_CHUNK_BYTES = 65536
_ARM = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_FALLBACK = re.compile(rb"fall(?:ing)?[\s_-]*back", re.IGNORECASE)


class BenchmarkError(RuntimeError):
    """Protocol, deadline, resource, or owned-process cleanup failure."""


def strict_json(data: bytes) -> dict[str, Any]:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise BenchmarkError(f"duplicate protocol JSON key: {key[:80]}")
            result[key] = value
        return result

    def invalid(value):
        raise BenchmarkError(f"non-finite protocol JSON: {value[:80]}")

    def finite_float(value):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else invalid(value)

    try:
        result = json.loads(data, object_pairs_hook=unique, parse_constant=invalid,
                            parse_float=finite_float)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise BenchmarkError(f"invalid protocol JSON: {type(error).__name__}") from error
    if not isinstance(result, dict):
        raise BenchmarkError("protocol event must be an object")
    return result


def _deadline(timeout: float) -> float:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise BenchmarkError("timeout must be positive and finite")
    return time.monotonic() + timeout


class _ProcessTree:
    def __init__(self, parent: subprocess.Popen, psutil):
        self.parent = parent
        self.psutil = psutil
        self.known: dict[tuple[int, float], dict[str, Any]] = {}
        self.groups: dict[int, tuple[int, float]] = {}
        self.actions: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._signalled: set[tuple[int, float, int]] = set()
        # Popen has not been polled/reaped, so even a very short-lived child
        # still has its original creation identity available for registration.
        self._register(psutil.Process(parent.pid), "direct_child")

    @staticmethod
    def public(entry):
        return {key: entry[key] for key in ("pid", "create_time", "relation")}

    def _register(self, process, relation):
        try:
            key = (process.pid, process.create_time())
            if key in self.known:
                return
            if len(self.known) >= MAX_PROCESSES:
                raise BenchmarkError("owned process identity ceiling exceeded")
            self.known[key] = {"process": process, "pid": key[0], "create_time": key[1], "relation": relation}
            # Every separately created session whose leader is an observed
            # descendant can also be scanned after that leader has exited.
            if os.getpgid(process.pid) == process.pid and os.getsid(process.pid) == process.pid:
                self.groups[process.pid] = key
        except (self.psutil.NoSuchProcess, ProcessLookupError):
            return

    def alive(self, entry, *, include_zombie=False):
        process = entry["process"]
        try:
            return process.is_running() and (include_zombie or process.status() != self.psutil.STATUS_ZOMBIE)
        except self.psutil.NoSuchProcess:
            return False

    def discover(self):
        for entry in list(self.known.values()):
            if not self.alive(entry, include_zombie=True):
                continue
            try:
                for process in entry["process"].children(recursive=True):
                    self._register(process, "observed_descendant")
            except self.psutil.NoSuchProcess:
                pass
        # A creation-matched group member or its unreaped leader anchors group
        # ownership. Once the group disappears, never reclaim its numeric PGID.
        anchored = set()
        if self.parent.returncode is None and self.parent.pid in self.groups:
            # On macOS getpgid can fail for an exited, unreaped leader. Popen
            # still owns its PID, so the original session cannot be reused.
            anchored.add(self.parent.pid)
        for entry in self.known.values():
            if self.alive(entry, include_zombie=True):
                with contextlib.suppress(ProcessLookupError):
                    group = os.getpgid(entry["pid"])
                    if group in self.groups and os.getsid(entry["pid"]) == group:
                        anchored.add(group)
        for group in set(self.groups) - anchored:
            del self.groups[group]
        if not anchored:
            return
        for pid in self.psutil.pids():
            try:
                group = os.getpgid(pid)
                if group not in anchored or os.getsid(pid) != group:
                    continue
                process = self.psutil.Process(pid)
                if process.create_time() < self.groups[group][1]:
                    raise BenchmarkError("owned process group creation identity differs")
                self._register(process, "owned_process_group")
            except (ProcessLookupError, self.psutil.NoSuchProcess):
                continue
            except PermissionError:
                # getpgid may reject inspection of unrelated host processes;
                # known owned identities are inspected without this exception.
                continue

    def sample(self):
        self.discover()
        rss = {}
        for key, entry in self.known.items():
            if self.alive(entry):
                try:
                    rss[key] = entry["process"].memory_info().rss
                except self.psutil.NoSuchProcess:
                    pass
        return rss

    def record_error(self, error):
        message = f"{type(error).__name__}: {error}"[:1024]
        if len(self.errors) < MAX_PROCESSES and message not in self.errors:
            self.errors.append(message)

    def cleanup_sample(self):
        try:
            self.discover()
        except Exception as error:
            self.record_error(error)

    def survivors(self):
        result = []
        for entry in self.known.values():
            try:
                if self.alive(entry):
                    result.append(entry)
            except Exception as error:
                self.record_error(error)
                result.append(entry)
        return result

    def signal(self, requested):
        # psutil send_signal checks the cached PID creation identity. Repeat
        # discovery during both phases to catch children born during shutdown.
        for entry in self.survivors():
            key = (entry["pid"], entry["create_time"], int(requested))
            if key in self._signalled:
                continue
            try:
                entry["process"].send_signal(requested)
                self._signalled.add(key)
                self.actions.append({**self.public(entry), "signal": int(requested)})
            except self.psutil.NoSuchProcess:
                pass
            except Exception as error:
                self.record_error(error)

    def receipt(self):
        survivors = self.survivors()
        zombies = []
        for entry in self.known.values():
            try:
                if entry["relation"] != "direct_child" and self.alive(entry, include_zombie=True) and not self.alive(entry):
                    zombies.append(self.public(entry))
            except Exception as error:
                self.record_error(error)
        living_keys = {(entry["pid"], entry["create_time"]) for entry in survivors}
        return {
            "tracked_processes": [self.public(entry) for entry in self.known.values()],
            "signals": self.actions.copy(), "inspection_errors": self.errors.copy(),
            "survivors": [self.public(entry) for entry in survivors],
            "observed_exited": [self.public(entry) for key, entry in self.known.items() if key not in living_keys],
            "unreaped_descendant_zombies": zombies,
            "nonchild_reaping": "not attempted; descendant waiters or the OS own reaping",
        }


class ResourceGuard:
    def __init__(self, max_rss_bytes: int = 8 * 1024**3, max_log_bytes: int = MAX_LOG_BYTES):
        if type(max_rss_bytes) is not int or max_rss_bytes <= 0:
            raise BenchmarkError("RSS ceiling must be a positive integer")
        if type(max_log_bytes) is not int or not 0 < max_log_bytes <= MAX_LOG_BYTES:
            raise BenchmarkError("log ceiling must be a positive integer at most 64 MiB")
        import psutil
        # A sandbox without process enumeration cannot provide tree ownership.
        # Detect that before launching any worker, including a model loader.
        own = psutil.Process(os.getpid())
        own.create_time()
        own.memory_info()
        own.children()
        self.psutil = psutil
        self.max_rss_bytes = max_rss_bytes
        self.max_log_bytes = max_log_bytes
        self.workers: list[Worker] = []
        self.peak_rss_bytes = 0
        self.peak_rss_by_arm: dict[str, int] = {}
        self.samples = 0
        self.max_sample_interval_seconds = 0.0
        self._last_sample: float | None = None
        self._completed: list[dict[str, Any]] = []

    def _register(self, worker):
        if len(self.workers) >= 2 or any(item.arm == worker.arm for item in self.workers):
            raise BenchmarkError("at most two distinct benchmark arms may be owned at once")
        self.workers.append(worker)

    def _unregister(self, worker):
        if worker in self.workers:
            self.workers.remove(worker)
            self._completed.append({"arm": worker.arm, "cleanup": worker.cleanup})
        if not self.workers:
            self._last_sample = None

    def check(self) -> None:
        now = time.monotonic()
        if self._last_sample is not None:
            self.max_sample_interval_seconds = max(self.max_sample_interval_seconds, now - self._last_sample)
        self._last_sample = now
        self.samples += 1
        combined = {}
        for worker in self.workers:
            worker._pump()
            rss = worker._tree.sample()
            combined.update(rss)
            self.peak_rss_by_arm[worker.arm] = max(self.peak_rss_by_arm.get(worker.arm, 0), sum(rss.values()))
        total = sum(combined.values())
        self.peak_rss_bytes = max(self.peak_rss_bytes, total)
        if total > self.max_rss_bytes:
            raise BenchmarkError(f"combined worker RSS {total} exceeds {self.max_rss_bytes}")

    def receipt(self) -> dict[str, Any]:
        return {
            "max_rss_bytes": self.max_rss_bytes, "max_log_bytes_per_arm": self.max_log_bytes,
            "peak_rss_bytes": self.peak_rss_bytes, "peak_rss_by_arm": self.peak_rss_by_arm.copy(),
            "samples": self.samples, "poll_seconds": POLL_SECONDS,
            "max_sample_interval_seconds": self.max_sample_interval_seconds,
            "rss_measurement": "sampled owned-process RSS; shared pages may count twice",
            "psutil_version": self.psutil.__version__, "completed_workers": self._completed.copy(),
        }


class Worker:
    def __init__(self, arm: str, command: list[str], env: dict[str, str], directory: Path, guard: ResourceGuard):
        if not isinstance(arm, str) or not _ARM.fullmatch(arm):
            raise BenchmarkError("invalid bounded worker arm identity")
        if not isinstance(command, list) or not command or not all(isinstance(item, str) and "\0" not in item for item in command):
            raise BenchmarkError("worker argv must be a nonempty string list")
        self.arm = arm
        self.guard = guard
        self.log_path = directory / f"{arm}.stderr.log"
        self.log = None
        self.process = None
        self.selector = None
        self._tree = None
        self.buffer = bytearray()
        self.sequence = 0
        self._ready = False
        self._stopped = False
        self._closed = False
        self._pending = None
        self._stdout_eof = False
        self._stderr_eof = False
        self._stderr_bytes = 0
        self._stderr_tail = b""
        self._violations: list[str] = []
        self.cleanup: dict[str, Any] | None = None
        try:
            self.log = self.log_path.open("xb", buffering=0)
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE, env=env, bufsize=0, start_new_session=True)
            # Register the owner before any fallible selector/stream setup.
            self.guard._register(self)
            self._tree = _ProcessTree(self.process, self.guard.psutil)
            for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                os.set_blocking(stream.fileno(), False)
            self.selector = selectors.DefaultSelector()
            self.selector.register(self.process.stdout, selectors.EVENT_READ)
            self.selector.register(self.process.stderr, selectors.EVENT_READ)
        except BaseException as error:
            self._rollback(error)
            raise

    def __enter__(self):
        return self

    def __exit__(self, _kind, error, _traceback):
        if error is None:
            self.close()
        else:
            self._rollback(error)

    def _rollback(self, error):
        try:
            self.close()
        except BaseException as cleanup_error:
            error.add_note(f"worker rollback: {cleanup_error}")

    def _pump(self, *, cleanup=False):
        for name in ("stdout", "stderr"):
            stream = getattr(self.process, name)
            if stream is None or stream.closed or getattr(self, f"_{name}_eof"):
                continue
            try:
                # Also applies when initialization failed before set_blocking.
                os.set_blocking(stream.fileno(), False)
                data = os.read(stream.fileno(), _CHUNK_BYTES)
            except BlockingIOError:
                continue
            if not data:
                setattr(self, f"_{name}_eof", True)
                if self.selector is not None:
                    with contextlib.suppress(KeyError):
                        self.selector.unregister(stream)
                continue
            if name == "stdout":
                if not cleanup:
                    if len(self.buffer) + len(data) > MAX_RESPONSE_BYTES:
                        raise BenchmarkError(f"{self.arm}: oversized stdout response")
                    self.buffer.extend(data)
            else:
                available = max(0, self.guard.max_log_bytes - self._stderr_bytes)
                self.log.write(data[:available])
                self._stderr_bytes += len(data)
                window = self._stderr_tail + data.lower()
                fallback = b"mps" in window and b"cpu" in window and _FALLBACK.search(window)
                self._stderr_tail = window[-8192:]
                if self._stderr_bytes > self.guard.max_log_bytes:
                    self._violation("stderr log byte ceiling exceeded", cleanup)
                if fallback:
                    self._violation("MPS CPU fallback warning in stderr", cleanup)

    def _violation(self, message, cleanup):
        if message not in self._violations:
            self._violations.append(message)
        if not cleanup:
            raise BenchmarkError(f"{self.arm}: {message}")

    def _wait(self, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise BenchmarkError(f"{self.arm}: response deadline exceeded")
        self.selector.select(min(POLL_SECONDS, remaining))

    def _receive(self, deadline):
        while True:
            if time.monotonic() >= deadline:
                raise BenchmarkError(f"{self.arm}: response deadline exceeded")
            self.guard.check()
            if b"\n" in self.buffer:
                line, _, rest = self.buffer.partition(b"\n")
                self.buffer = bytearray(rest)
                event = strict_json(line)
                self._validate(event)
                if time.monotonic() >= deadline:
                    raise BenchmarkError(f"{self.arm}: response deadline exceeded")
                return event
            if self._stdout_eof:
                raise BenchmarkError(f"{self.arm}: exited before response (code {self.process.poll()}); see {self.log_path}")
            self._wait(deadline)

    def _validate(self, event):
        kind = event.get("event")
        if kind not in ("ready", "result", "error", "stopped"):
            raise BenchmarkError(f"{self.arm}: unknown protocol event")
        if self._pending is None:
            if self._ready or kind != "ready" or event.get("arm") != self.arm or "request_id" in event:
                raise BenchmarkError(f"{self.arm}: ready identity mismatch")
            self._ready = True
            return
        seq, op, case_id = self._pending
        if type(event.get("request_id")) is not int or event["request_id"] != seq:
            raise BenchmarkError(f"{self.arm}: response request identity mismatch")
        if kind == "stopped" and op == "stop":
            if event.get("arm", self.arm) != self.arm or event.get("case_id", case_id) != case_id:
                raise BenchmarkError(f"{self.arm}: stopped identity mismatch")
            self._stopped = True
        elif kind in ("result", "error") and (kind == "error" or op != "stop"):
            if event.get("arm") != self.arm or event.get("case_id") != case_id:
                raise BenchmarkError(f"{self.arm}: result identity mismatch")
            if kind == "result" and (type(event.get("duration_ns")) is not int or event["duration_ns"] <= 0):
                raise BenchmarkError(f"{self.arm}: invalid result duration_ns")
            if kind == "error" and "duration_ns" in event:
                raise BenchmarkError(f"{self.arm}: error event contains successful timing")
        else:
            raise BenchmarkError(f"{self.arm}: unexpected protocol event for request")
        self._pending = None

    def receive(self, timeout: float) -> dict[str, Any]:
        try:
            if self._closed:
                raise BenchmarkError(f"{self.arm}: worker is closed")
            self.guard._last_sample = None  # Caller work between waits is not a sampling interval.
            return self._receive(_deadline(timeout))
        except BaseException as error:
            self._rollback(error)
            raise

    def request(self, op: str, case_id: str = "", timeout: float = 35) -> dict[str, Any]:
        try:
            deadline = _deadline(timeout)
            self.guard._last_sample = None
            if self._closed or not self._ready or self._stopped or self._pending is not None:
                raise BenchmarkError(f"{self.arm}: request outside ready protocol state")
            if op not in ("validate", "run", "stop") or not isinstance(case_id, str) or (op == "stop" and case_id) or (op != "stop" and not case_id):
                raise BenchmarkError(f"{self.arm}: invalid command identity")
            if self.sequence >= MAX_COMMANDS:
                raise BenchmarkError(f"{self.arm}: command count ceiling exceeded")
            if len(case_id) > MAX_COMMAND_BYTES:
                raise BenchmarkError(f"{self.arm}: command exceeds 2048 bytes")
            seq = self.sequence + 1
            data = (json.dumps({"request_id": seq, "op": op, "case_id": case_id}, ensure_ascii=False,
                               allow_nan=False, separators=(",", ":")) + "\n").encode()
            if len(data) > MAX_COMMAND_BYTES:
                raise BenchmarkError(f"{self.arm}: command exceeds 2048 bytes")
            self.guard.check()
            if self.buffer:
                raise BenchmarkError(f"{self.arm}: unsolicited protocol output before request")
            self.sequence = seq
            self._pending = (seq, op, case_id)
            self.selector.register(self.process.stdin, selectors.EVENT_WRITE)
            try:
                position = 0
                while position < len(data):
                    if time.monotonic() >= deadline:
                        raise BenchmarkError(f"{self.arm}: command deadline exceeded")
                    self.guard.check()
                    try:
                        position += os.write(self.process.stdin.fileno(), data[position:])
                    except BlockingIOError:
                        self._wait(deadline)
                    except BrokenPipeError as error:
                        raise BenchmarkError(f"{self.arm}: worker command pipe closed") from error
            finally:
                self.selector.unregister(self.process.stdin)
            return self._receive(deadline)
        except BaseException as error:
            self._rollback(error)
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        started = time.monotonic()
        errors = []
        if self.process is not None:
            # Preserve failure attribution even if unexpected teardown errors
            # interrupt the normal receipt construction. Idempotent subsequent
            # close calls must not erase evidence of incomplete ownership.
            self.cleanup = {"complete": False, "direct_child_reaped": False, "errors": errors}
        try:
            if self.process is not None:
                if self._tree is None:
                    try:
                        self._tree = _ProcessTree(self.process, self.guard.psutil)
                    except Exception as error:
                        errors.append(f"process registration: {type(error).__name__}: {error}")
                for requested, duration in ((signal.SIGTERM, TERM_SECONDS), (signal.SIGKILL, KILL_SECONDS)):
                    deadline = time.monotonic() + duration
                    while True:
                        if self._tree is not None:
                            self._tree.cleanup_sample()
                            self._tree.signal(requested)
                        elif self.process.returncode is None:
                            # Only the failed-construction path lacks creation
                            # metadata. Its direct child has not been reaped,
                            # and therefore still pins the newly owned PGID.
                            with contextlib.suppress(ProcessLookupError):
                                os.killpg(self.process.pid, requested)
                        # Popen owns the unreaped direct child even if process
                        # metadata registration failed; never lose that owner.
                        if self.process.poll() is None:
                            with contextlib.suppress(ProcessLookupError):
                                self.process.send_signal(requested)
                        try:
                            self._pump(cleanup=True)
                        except Exception as error:
                            if len(errors) < MAX_PROCESSES:
                                errors.append(f"pipe drain: {type(error).__name__}: {error}"[:1024])
                        direct_done = self.process.poll() is not None
                        pipes_done = self._stdout_eof and self._stderr_eof
                        if direct_done and pipes_done and (self._tree is None or not self._tree.survivors()):
                            break
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        time.sleep(min(POLL_SECONDS, remaining))
                    if direct_done and pipes_done and (self._tree is None or not self._tree.survivors()):
                        break
                if self.process.returncode is None:
                    errors.append("direct child did not exit within TERM 2s + KILL 2s")
                tree = self._tree.receipt() if self._tree is not None else {"survivors": [], "inspection_errors": []}
                self.cleanup = {**tree, "direct_child_reaped": self.process.returncode is not None,
                                "returncode": self.process.returncode, "errors": errors,
                                "stdout_drained": self._stdout_eof, "stderr_drained": self._stderr_eof,
                                "guard_violations": self._violations.copy(),
                                "elapsed_seconds": time.monotonic() - started,
                                "complete": self.process.returncode is not None and self._stdout_eof and self._stderr_eof and not tree["survivors"] and not tree["inspection_errors"] and not errors}
        except BaseException as error:
            errors.append(f"cleanup interrupted: {type(error).__name__}: {error}"[:1024])
            raise
        finally:
            resources = [self.selector]
            if self.process is not None:
                resources.extend((self.process.stdin, self.process.stdout, self.process.stderr))
            resources.append(self.log)
            for resource in resources:
                if resource is not None:
                    try:
                        resource.close()
                    except Exception as error:
                        errors.append(f"descriptor close: {type(error).__name__}: {error}"[:1024])
            if errors and self.cleanup is not None:
                self.cleanup["complete"] = False
            self.guard._unregister(self)
        if self.cleanup is not None and not self.cleanup["complete"]:
            raise BenchmarkError(f"{self.arm}: owned process cleanup incomplete: {self.cleanup}")
        if self._violations:
            raise BenchmarkError(f"{self.arm}: {'; '.join(self._violations)}")
