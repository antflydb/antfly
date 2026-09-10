"""Low-overhead process-only RSS/footprint sampling via Darwin proc_pid_rusage.

Unlike vmmap this does not walk or suspend the process. Follows a qualification
pid file across restart; start_abstime identifies each distinct process lifetime.
Does not attribute node-wide wired memory or child processes to the server.
ABI: macOS SDK sys/resource.h rusage_info_v4, libproc.h proc_pid_rusage.
"""

import argparse
import ctypes
import errno
import functools
import json
import math
import signal
import sys
import time
from pathlib import Path


class RusageInfoV4(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64)
        for name in [
            "user_time",
            "system_time",
            "pkg_idle_wkups",
            "interrupt_wkups",
            "pageins",
            "wired_size",
            "resident_size",
            "phys_footprint",
            "proc_start_abstime",
            "proc_exit_abstime",
            "child_user_time",
            "child_system_time",
            "child_pkg_idle_wkups",
            "child_interrupt_wkups",
            "child_pageins",
            "child_elapsed_abstime",
            "diskio_bytesread",
            "diskio_byteswritten",
            "cpu_time_qos_default",
            "cpu_time_qos_maintenance",
            "cpu_time_qos_background",
            "cpu_time_qos_utility",
            "cpu_time_qos_legacy",
            "cpu_time_qos_user_initiated",
            "cpu_time_qos_user_interactive",
            "billed_system_time",
            "serviced_system_time",
            "logical_writes",
            "lifetime_max_phys_footprint",
            "instructions",
            "cycles",
            "billed_energy",
            "serviced_energy",
            "interval_max_phys_footprint",
            "runnable_time",
        ]
    ]


class MachTimebaseInfo(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


@functools.cache
def mach_timebase():
    info = MachTimebaseInfo()
    libsystem = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
    libsystem.mach_timebase_info.argtypes = [ctypes.POINTER(MachTimebaseInfo)]
    libsystem.mach_timebase_info.restype = ctypes.c_int
    if (
        libsystem.mach_timebase_info(ctypes.byref(info)) != 0
        or not info.numer
        or not info.denom
    ):
        raise RuntimeError("cannot determine Mach CPU time units")
    return info.numer, info.denom


def cpu_nanoseconds(ticks, timebase):
    numer, denom = timebase
    if ticks < 0 or numer <= 0 or denom <= 0:
        raise ValueError("invalid Mach timebase or counter")
    return ticks * numer // denom


def sample(libproc, pid):
    info = RusageInfoV4()
    if libproc.proc_pid_rusage(pid, 4, ctypes.byref(info)) != 0:
        code = ctypes.get_errno()
        if code == errno.ESRCH:
            return None
        raise OSError(code, "proc_pid_rusage failed", str(pid))
    timebase = mach_timebase()
    return {
        "wall_time_s": time.time(),
        "monotonic_ns": time.monotonic_ns(),
        "pid": pid,
        "start_abstime": info.proc_start_abstime,
        "rss_bytes": info.resident_size,
        "phys_footprint_bytes": info.phys_footprint,
        "lifetime_max_phys_footprint_bytes": info.lifetime_max_phys_footprint,
        "disk_read_bytes": info.diskio_bytesread,
        "disk_written_bytes": info.diskio_byteswritten,
        "logical_written_bytes": info.logical_writes,
        # These rusage counters use Mach ticks (not ns on ARM64). Preserve the
        # conversion in the receipt; process CPU is not per-query CPU.
        "cpu_timebase_numer": timebase[0],
        "cpu_timebase_denom": timebase[1],
        "user_cpu_ns": cpu_nanoseconds(info.user_time, timebase),
        "system_cpu_ns": cpu_nanoseconds(info.system_time, timebase),
        "pageins": info.pageins,
        "instructions": info.instructions,
        "cycles": info.cycles,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pid", type=int)
    source.add_argument("--pid-file", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seconds", type=float, default=3600)
    parser.add_argument("--interval", type=float, default=0.5)
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("this sampler requires macOS")
    if (
        not math.isfinite(args.seconds)
        or not math.isfinite(args.interval)
        or args.seconds <= 0
        or args.interval <= 0
        or (args.pid is not None and args.pid <= 0)
    ):
        parser.error("duration, interval, and pid must be positive")
    assert ctypes.sizeof(RusageInfoV4) == 296
    assert RusageInfoV4.lifetime_max_phys_footprint.offset == 240
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    libproc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    libproc.proc_pid_rusage.restype = ctypes.c_int
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    deadline = time.monotonic() + args.seconds
    samples = 0
    with args.output.open("x", buffering=1) as output:
        while not stopping and time.monotonic() < deadline:
            pid = args.pid
            if args.pid_file is not None:
                try:
                    pid = int(args.pid_file.read_text().strip())
                except (FileNotFoundError, ValueError):
                    pid = None
            if pid is not None and pid > 0:
                row = sample(libproc, pid)
                if row is not None:
                    output.write(json.dumps(row) + "\n")
                    samples += 1
            time.sleep(args.interval)
    if not samples:
        raise SystemExit("no process memory samples collected")
    print(json.dumps({"samples": samples, "output": str(args.output)}))


if __name__ == "__main__":
    main()
