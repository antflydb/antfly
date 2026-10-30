#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fail-closed physical-footprint sampling for one owned process group.

The Darwin ABI and ``proc_pid_rusage`` call remain owned by
``scripts/sample_macos_process_memory.py``.  This module only discovers the
members of an already-owned process group and aggregates those samples.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys


SCRIPTS = Path(__file__).resolve().parents[1]
SAMPLER_SOURCE = SCRIPTS / "sample_macos_process_memory.py"


def _load_sampler_module():
    spec = importlib.util.spec_from_file_location(
        "antfly_sample_macos_process_memory", SAMPLER_SOURCE
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load physical-memory sampler: {SAMPLER_SOURCE}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_darwin = _load_sampler_module()


def parse_process_group_listing(listing: str, process_group: int) -> list[int]:
    """Parse ``ps -axo pgid=,pid=`` and return unique members of ``pgid``."""
    if isinstance(process_group, bool) or not isinstance(process_group, int) or process_group <= 0:
        raise ValueError("process group must be a positive integer")
    members = []
    seen = set()
    for line_number, line in enumerate(listing.splitlines(), 1):
        fields = line.split()
        if len(fields) != 2:
            raise ValueError(f"malformed process-group listing at line {line_number}")
        try:
            pgid, pid = map(int, fields)
        except ValueError as error:
            raise ValueError(
                f"non-integer process-group listing at line {line_number}"
            ) from error
        if pgid <= 0 or pid <= 0:
            raise ValueError(f"non-positive process identity at line {line_number}")
        if pid in seen:
            raise ValueError(f"duplicate pid in process-group listing: {pid}")
        seen.add(pid)
        if pgid == process_group:
            members.append(pid)
    return sorted(members)


def _default_process_listing() -> str:
    return subprocess.check_output(
        ["/bin/ps", "-axo", "pgid=,pid="], text=True, timeout=10
    )


def _default_libproc():
    if sys.platform != "darwin":
        raise RuntimeError("physical footprint sampling requires macOS")
    if (
        _darwin.ctypes.sizeof(_darwin.RusageInfoV4) != 296
        or _darwin.RusageInfoV4.lifetime_max_phys_footprint.offset != 240
    ):
        raise RuntimeError("unexpected Darwin rusage_info_v4 ABI")
    libproc = _darwin.ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    libproc.proc_pid_rusage.argtypes = [
        _darwin.ctypes.c_int,
        _darwin.ctypes.c_int,
        _darwin.ctypes.c_void_p,
    ]
    libproc.proc_pid_rusage.restype = _darwin.ctypes.c_int
    return libproc


def _counter(row: dict, name: str) -> int:
    value = row.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"invalid physical-memory counter {name}: {value!r}")
    return value


class ProcessGroupPhysicalMemorySampler:
    """Track a conservative physical-footprint bound for each ``(pid, start)``.

    XNU credits a ledger before separately updating its tracked lifetime
    maximum, so an active process can transiently report current > lifetime.
    We resample that condition, requiring a stable process identity and a
    later kernel lifetime value which covers every current value observed.

    The aggregate is deliberately conservative: for every distinct identity
    ever seen, it sums the greater of the kernel lifetime maximum and our
    sampled-current high-water. It is *not* a concurrent process-group peak.
    Sequential worker lifetimes therefore accumulate rather than being
    silently reused.
    """

    bound_semantics = (
        "sum_of_per_process_identity_max_of_kernel_lifetime_and_sampled_current_"
        "phys_footprints;"
        "conservative_not_concurrent_group_peak"
    )

    def __init__(
        self,
        *,
        libproc=None,
        sample_fn=None,
        listing_fn=None,
        consistency_attempts=3,
    ):
        if (
            isinstance(consistency_attempts, bool)
            or not isinstance(consistency_attempts, int)
            or consistency_attempts < 1
        ):
            raise ValueError("consistency_attempts must be a positive integer")
        self._libproc = libproc if libproc is not None else _default_libproc()
        self._sample = sample_fn if sample_fn is not None else _darwin.sample
        self._listing = (
            listing_fn if listing_fn is not None else _default_process_listing
        )
        self._consistency_attempts = consistency_attempts
        self._identity_kernel_maxima: dict[tuple[int, int], int] = {}
        self._identity_sampled_current_maxima: dict[tuple[int, int], int] = {}

    @staticmethod
    def _validated_sample(sampled, pid: int) -> tuple[int, int, int, int]:
        if not isinstance(sampled, dict) or sampled.get("pid") != pid:
            raise RuntimeError(f"physical-memory sample identity mismatch for pid {pid}")
        start = _counter(sampled, "start_abstime")
        if start == 0:
            raise RuntimeError(f"missing process start identity for pid {pid}")
        return (
            start,
            _counter(sampled, "rss_bytes"),
            _counter(sampled, "phys_footprint_bytes"),
            _counter(sampled, "lifetime_max_phys_footprint_bytes"),
        )

    def _sample_consistent_identity(self, pid: int):
        sampled = self._sample(self._libproc, pid)
        if sampled is None:
            return None
        start, rss, footprint, lifetime = self._validated_sample(sampled, pid)
        identity = (pid, start)
        previous_kernel = self._identity_kernel_maxima.get(identity, 0)
        first_footprint = footprint
        first_lifetime = lifetime
        lag_observations = []

        for attempt in range(1, self._consistency_attempts + 1):
            if attempt > 1:
                sampled = self._sample(self._libproc, pid)
                if sampled is None:
                    raise RuntimeError(
                        "physical-memory process disappeared during consistency "
                        f"resampling: pid={pid} start={start} "
                        f"first_current={first_footprint} first_lifetime={first_lifetime} "
                        f"previous_kernel={previous_kernel} "
                        f"sampled_current_high_water="
                        f"{self._identity_sampled_current_maxima[identity]} "
                        f"attempts={attempt}"
                    )
                retry_start, rss, footprint, lifetime = self._validated_sample(
                    sampled, pid
                )
                if retry_start != start:
                    raise RuntimeError(
                        "physical-memory process identity changed during consistency "
                        f"resampling: pid={pid} first_start={start} "
                        f"retry_start={retry_start} retry_current={footprint} "
                        f"retry_lifetime={lifetime} previous_kernel={previous_kernel} "
                        f"attempts={attempt}"
                    )

            observed_current = max(
                self._identity_sampled_current_maxima.get(identity, 0), footprint
            )
            observed_kernel = max(
                self._identity_kernel_maxima.get(identity, 0), lifetime
            )
            # Retain both observations before deciding whether this read is
            # consistent. Even an exhausted retry cannot erase a high-water.
            self._identity_sampled_current_maxima[identity] = observed_current
            self._identity_kernel_maxima[identity] = observed_kernel
            required_kernel = max(previous_kernel, observed_current)
            if lifetime >= required_kernel:
                row = dict(sampled)
                row["identity_kernel_lifetime_max_phys_footprint_bytes"] = (
                    observed_kernel
                )
                row["identity_sampled_current_max_phys_footprint_bytes"] = (
                    observed_current
                )
                row["identity_effective_peak_bound_bytes"] = max(
                    observed_kernel, observed_current
                )
                row["consistency_sample_attempts"] = attempt
                row["transient_kernel_peak_lag_observations"] = lag_observations
                return row, rss, footprint

            lag_observations.append(
                {
                    "attempt": attempt,
                    "current_phys_footprint_bytes": footprint,
                    "kernel_lifetime_max_phys_footprint_bytes": lifetime,
                    "previous_kernel_lifetime_max_phys_footprint_bytes": previous_kernel,
                    "sampled_current_high_water_bytes": observed_current,
                    "required_kernel_lifetime_max_phys_footprint_bytes": required_kernel,
                }
            )

        raise RuntimeError(
            "kernel lifetime footprint consistency retry exhausted: "
            f"pid={pid} start={start} current={footprint} lifetime={lifetime} "
            f"first_current={first_footprint} first_lifetime={first_lifetime} "
            f"previous_kernel={previous_kernel} "
            f"sampled_current_high_water={self._identity_sampled_current_maxima[identity]} "
            f"kernel_lifetime_high_water={self._identity_kernel_maxima[identity]} "
            f"attempts={self._consistency_attempts}"
        )

    def snapshot(self, process_group: int) -> dict:
        pids = parse_process_group_listing(self._listing(), process_group)
        rows = []
        live_footprint = 0
        live_rss = 0
        for pid in pids:
            # The shared sampler returns None only for ESRCH. All other
            # inspection failures propagate and fail the qualification gate.
            result = self._sample_consistent_identity(pid)
            if result is None:
                continue
            row, rss, footprint = result
            rows.append(row)
            live_rss += rss
            live_footprint += footprint

        identities = set(self._identity_kernel_maxima) | set(
            self._identity_sampled_current_maxima
        )
        kernel_sum = sum(
            self._identity_kernel_maxima.get(identity, 0) for identity in identities
        )
        sampled_current_sum = sum(
            self._identity_sampled_current_maxima.get(identity, 0) for identity in identities
        )
        conservative = sum(
            max(
                self._identity_kernel_maxima.get(identity, 0),
                self._identity_sampled_current_maxima.get(identity, 0),
            )
            for identity in identities
        )
        return {
            "process_group": process_group,
            "rows": rows,
            "live_processes": len(rows),
            "live_rss_bytes": live_rss,
            "live_phys_footprint_bytes": live_footprint,
            "observed_process_identities": len(identities),
            "summed_kernel_lifetime_max_phys_footprint_bytes": kernel_sum,
            "summed_sampled_current_max_phys_footprint_bytes": sampled_current_sum,
            "conservative_summed_identity_peak_bound_bytes": conservative,
            # Compatibility alias for the qualification harness. The precise
            # mixed-source semantics are recorded in bound_semantics and the
            # identity_peak_bound field above.
            "conservative_summed_lifetime_max_phys_footprint_bytes": conservative,
            "bound_semantics": self.bound_semantics,
        }

