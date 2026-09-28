# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Bounded shutdown of an exclusively owned server role.

Signal the complete role before joining so one slow server cannot keep its
peers accepting work. The role shares one graceful deadline; kill and reap
remain separate, bounded operations. Callers retain listener leases until this
function returns and stop dependent roles in their established order.
"""

import signal
import subprocess
import time
from collections.abc import Sequence


def stop_processes(processes: Sequence[subprocess.Popen], *, grace_s=10.0, kill_s=5.0):
    deadline = time.monotonic() + grace_s
    for process in processes:
        if process.poll() is None:
            try:
                process.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
    remaining = []
    for process in processes:
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            remaining.append(process)
    for process in remaining:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    kill_deadline = time.monotonic() + kill_s
    failures = []
    for process in remaining:
        try:
            process.wait(timeout=max(0.0, kill_deadline - time.monotonic()))
        except subprocess.TimeoutExpired as error:
            failures.append(error)
    if failures:
        raise ExceptionGroup("owned server processes did not exit after kill", failures)
