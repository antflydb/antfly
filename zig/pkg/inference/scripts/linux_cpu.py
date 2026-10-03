"""Host provenance shared by Linux CPU benchmark drivers (standard library)."""

import hashlib
import os
from pathlib import Path
import platform
import subprocess


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def host_provenance():
    def read(path):
        try:
            return Path(path).read_text()
        except OSError:
            return None

    def git(*args):
        result = subprocess.run(
            ["git", *args],
            cwd=Path(__file__).resolve().parents[4],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else None

    root = Path(__file__).resolve().parents[4]
    untracked = {}
    for name in (git("ls-files", "--others", "--exclude-standard", "-z") or "").split(
        "\0"
    ):
        path = root / name
        if name and path.is_file() and not path.is_symlink():
            untracked[name] = sha256(path)
    return {
        "system": platform.system(),
        "machine": platform.machine(),
        "kernel": platform.release(),
        "cpuinfo": read("/proc/cpuinfo"),
        "affinity": sorted(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else None,
        "cgroup": read("/proc/self/cgroup"),
        "mountinfo": read("/proc/self/mountinfo"),
        "meminfo": read("/proc/meminfo"),
        "commit": git("rev-parse", "HEAD"),
        "source_dirty": git("status", "--porcelain") != "",
        "untracked_source_sha256": untracked,
        "tracked_diff_sha256": hashlib.sha256(
            (git("diff", "HEAD") or "").encode()
        ).hexdigest(),
        "kernel_override": os.environ.get("ANTFLY_INFERENCE_X86_KERNEL", "auto"),
        "thread_override": os.environ.get("ANTFLY_INFERENCE_CPU_THREADS"),
    }
