"""Read enforced Linux container limits; configured values alone are not receipts."""

import os
import pathlib
import platform
import resource


def enforced_limits():
    result = {
        "platform": platform.system(),
        "nofile_soft": resource.getrlimit(resource.RLIMIT_NOFILE)[0],
    }
    if hasattr(os, "sched_getaffinity"):
        result["affinity_cpu_count"] = len(os.sched_getaffinity(0))
    root = pathlib.Path("/sys/fs/cgroup")
    if platform.system() == "Linux":
        for name, field in (("memory.max", "memory_bytes"), ("pids.max", "pids")):
            path = root / name
            if path.exists():
                raw = path.read_text().strip()
                result[field] = None if raw == "max" else int(raw)
        path = root / "cpu.max"
        if path.exists():
            quota, period = path.read_text().split()
            result["cpu_count"] = None if quota == "max" else int(quota) / int(period)
    return result


def check_container(manifest, observed):
    required = manifest.get("container_limits", {})
    failures = []
    if required and observed["platform"] != "Linux":
        failures.append("Linux container manifest requires Linux")
    for field, value in required.items():
        if observed.get(field) != value:
            failures.append("enforced container limit differs: " + field)
    return failures
