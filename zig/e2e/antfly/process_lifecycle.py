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
