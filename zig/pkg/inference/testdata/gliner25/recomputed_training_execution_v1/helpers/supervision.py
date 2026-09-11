"""Sampled RSS/output/deadline guards for an owned disposable CLI tree.

psutil 7.1.3 is required, but Torch is never imported. Only the direct Popen
child is reaped here; grandchildren must disappear after their own waiter or
parent-loss monitor finishes. A surviving or uninspectable child fails cleanup.
"""
from __future__ import annotations

import importlib
import importlib.metadata
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time


def pinned_psutil():
    module = importlib.import_module("psutil")
    if module.__version__ != "7.1.3" or importlib.metadata.version("psutil") != "7.1.3":
        raise RuntimeError("psutil7.1.3 is required; use the pinned oracle Python")
    return module


class ProcessTree:
    def __init__(self, parent, psutil_module):
        self.parent = parent
        self.psutil = psutil_module
        self.known = {}
        self.peak_rss = 0
        self.samples = 0
        self.inspection_errors = []
        self.inspection_error_count = 0
        self.actions = []
        self.last_live = []

    def register(self, process, relation):
        try:
            birth = process.create_time()
        except self.psutil.NoSuchProcess:
            return
        key = (process.pid, birth)
        if key not in self.known:
            if len(self.known) >= 64:
                raise RuntimeError("owned process metadata limit exceeded (64 creation identities)")
            self.known[key] = {"process": process, "pid": process.pid, "create_time": birth, "relation": relation}

    def live(self, entry):
        # psutil7.1.3 is_running/send_signal both check the cached creation
        # identity against the current PID before treating it as this process.
        return entry["process"].is_running()

    def sample(self):
        if self.parent.poll() is None and not any(entry["pid"] == self.parent.pid for entry in self.known.values()):
            self.register(self.psutil.Process(self.parent.pid), "direct_child")
        for entry in list(self.known.values()):
            if not self.live(entry):
                continue
            try:
                descendants = entry["process"].children(recursive=True)
            except self.psutil.NoSuchProcess:
                continue
            for process in descendants:
                self.register(process, "observed_descendant")
        rss = 0
        live = []
        for entry in self.known.values():
            if not self.live(entry):
                continue
            try:
                rss += entry["process"].memory_info().rss
            except self.psutil.NoSuchProcess:
                continue
            live.append((entry["pid"], entry["create_time"]))
        self.last_live = live
        self.samples += 1
        self.peak_rss = max(self.peak_rss, rss)
        return rss

    def cleanup_sample(self):
        try:
            self.sample()
        except BaseException as error:
            self.record_error(f"{type(error).__name__}: {error}")

    def record_error(self, message):
        self.inspection_error_count += 1
        if len(self.inspection_errors) < 64 and message not in self.inspection_errors:
            self.inspection_errors.append(message[:1024])

    def survivors(self, descendants_only=False):
        result = []
        for entry in self.known.values():
            if descendants_only and entry["relation"] == "direct_child":
                continue
            try:
                if self.live(entry):
                    result.append(entry)
            except BaseException as error:
                self.record_error(f"{type(error).__name__}: {error}")
                result.append(entry)
        return result

    def signal_survivors(self, requested):
        for entry in self.survivors(descendants_only=True):
            try:
                if not self.live(entry):
                    continue
                entry["process"].send_signal(requested)
                self.actions.append({"pid": entry["pid"], "create_time": entry["create_time"], "signal": int(requested)})
            except self.psutil.NoSuchProcess:
                pass
            except BaseException as error:
                self.record_error(f"signal {entry['pid']}: {type(error).__name__}: {error}")

    def receipt(self):
        return {"psutil_version": self.psutil.__version__, "peak_child_tree_rss_bytes": self.peak_rss, "rss_samples": self.samples,
                "tracked_processes": [{key: value for key, value in entry.items() if key != "process"} for entry in self.known.values()],
                "cleanup_signals": self.actions, "inspection_errors": self.inspection_errors,
                "inspection_error_count": self.inspection_error_count}


def drain(parent):
    # Allow a cooperative CLI pause to publish its final event without
    # blocking on a full pipe after a supervising guard has already failed.
    for stream in (parent.stdout, parent.stderr):
        if stream is None or stream.closed:
            continue
        try:
            os.set_blocking(stream.fileno(), False)
            os.read(stream.fileno(), 65536)
        except (BlockingIOError, OSError):
            pass


def cleanup(parent, tree, grace_seconds, kill_seconds, worker_grace_seconds, tick):
    started = time.monotonic()
    errors = []
    tree.cleanup_sample()
    if parent.poll() is None:
        try:
            parent.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + grace_seconds
        while parent.poll() is None and time.monotonic() < deadline:
            tree.cleanup_sample()
            drain(parent)
            time.sleep(tick)
        if parent.poll() is None:
            # Popen is still the sole unreaped direct-child owner here.
            try:
                parent.kill()
            except ProcessLookupError:
                pass
    try:
        parent.wait(timeout=kill_seconds)
    except BaseException as error:
        errors.append(f"direct child wait: {type(error).__name__}: {error}")
    # A hard-killed public parent leaves the worker's parent-loss monitor in
    # charge first. Only previously discovered creation identities are then
    # eligible for explicit TERM/KILL; historical raw PIDs are never signaled.
    deadline = time.monotonic() + worker_grace_seconds
    while tree.survivors(descendants_only=True) and time.monotonic() < deadline:
        tree.cleanup_sample()
        drain(parent)
        time.sleep(tick)
    if tree.survivors(descendants_only=True):
        tree.signal_survivors(signal.SIGTERM)
        deadline = time.monotonic() + min(1.0, worker_grace_seconds)
        while tree.survivors(descendants_only=True) and time.monotonic() < deadline:
            tree.cleanup_sample()
            drain(parent)
            time.sleep(tick)
    tree.signal_survivors(signal.SIGKILL)
    deadline = time.monotonic() + kill_seconds
    while tree.survivors(descendants_only=True) and time.monotonic() < deadline:
        tree.cleanup_sample()
        drain(parent)
        time.sleep(tick)
    tree.cleanup_sample()
    survivors = [{"pid": entry["pid"], "create_time": entry["create_time"], "relation": entry["relation"]} for entry in tree.survivors()]
    return {"direct_child_reaped": parent.returncode is not None, "known_children_gone": not survivors,
            "survivors": survivors, "inspection_errors": tree.inspection_errors, "errors": errors,
            "complete": parent.returncode is not None and not survivors and not errors and not tree.inspection_errors,
            "elapsed_seconds": time.monotonic() - started,
            "grandchild_proof": "observed creation identities no longer exist; direct CLI parent is their normal waiter"}


def run(command, stdout_path, stderr_path, *, timeout_seconds, rss_limit_bytes=2 * 1024**3,
        output_limit_bytes=4 * 1024**2, grace_seconds=35.0, kill_seconds=10.0,
        worker_grace_seconds=2.0, tick=0.05, tree_factory=ProcessTree, on_registered=None):
    """Execute one child; always return bounded failure/cleanup evidence.

    The injectable hook is private test machinery, never read from user JSON
    or environment. Model runs use every default process/RSS limit unchanged.
    """
    result = {"failure": None, "failure_phase": None, "returncode": None,
              "max_child_tree_rss_bytes": rss_limit_bytes, "rss_poll_seconds": tick,
              "rss_measurement": "sampled sum of RSS for still-matching known child creation identities; shared pages can be counted twice",
              "timeout_seconds": timeout_seconds, "max_stdout_bytes": output_limit_bytes, "max_stderr_bytes": output_limit_bytes,
              "max_tracked_processes": 64,
              "parent_grace_seconds": grace_seconds, "kill_wait_seconds": kill_seconds, "worker_parent_loss_grace_seconds": worker_grace_seconds}
    parent = None
    tree = None
    phase = "preflight"
    started = time.monotonic()
    try:
        psutil = pinned_psutil()
        # Verify basic inspection permission before creating a process.
        own = psutil.Process(os.getpid())
        _ = own.create_time(), own.memory_info().rss, own.children()
        with Path(stdout_path).open("xb") as out, Path(stderr_path).open("xb") as err, selectors.DefaultSelector() as selected:
            phase = "launch"
            parent = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            result["parent_pid"] = parent.pid
            for stream in (parent.stdout, parent.stderr):
                os.set_blocking(stream.fileno(), False)
            for stream, destination in ((parent.stdout, out), (parent.stderr, err)):
                selected.register(stream, selectors.EVENT_READ, [destination, 0])
            phase = "registration"
            tree = tree_factory(parent, psutil)
            tree.sample()
            if on_registered is not None:
                on_registered(parent, tree)
            phase = "execution"
            while selected.get_map() or parent.poll() is None:
                if time.monotonic() - started >= timeout_seconds:
                    raise TimeoutError("outer deadline exceeded")
                rss = tree.sample()
                if rss > rss_limit_bytes:
                    raise MemoryError(f"child-tree RSS {rss} exceeds {rss_limit_bytes}")
                for key, _ in selected.select(tick):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selected.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    key.data[1] += len(chunk)
                    if key.data[1] > output_limit_bytes:
                        raise RuntimeError("child output limit exceeded")
                    key.data[0].write(chunk)
            parent.wait(timeout=5)
            if parent.returncode != 0:
                raise RuntimeError(f"public CLI exited {parent.returncode}")
    except BaseException as error:
        result["failure"] = f"{type(error).__name__}: {error}"
        result["failure_phase"] = phase
    finally:
        if parent is not None:
            if tree is None:
                # Construction/registration failure still owns Popen. Retry
                # inspection with the real tracker before bounded cleanup.
                tree = ProcessTree(parent, psutil)
            result["cleanup"] = cleanup(parent, tree, grace_seconds, kill_seconds, worker_grace_seconds, tick)
            result.update(tree.receipt())
            result["returncode"] = parent.returncode
            for stream in (parent.stdout, parent.stderr):
                if stream and not stream.closed:
                    stream.close()
            if not result["cleanup"]["complete"] and result["failure"] is None:
                result["failure"] = "RuntimeError: owned process cleanup incomplete"
                result["failure_phase"] = "cleanup"
        else:
            result["cleanup"] = {"complete": True, "direct_child_reaped": True, "known_children_gone": True, "survivors": [], "errors": []}
            result.update({"peak_child_tree_rss_bytes": 0, "rss_samples": 0, "tracked_processes": [], "cleanup_signals": [], "inspection_errors": []})
        result["elapsed_seconds"] = time.monotonic() - started
    return result
