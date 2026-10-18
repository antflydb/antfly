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

"""Bound native regression children by stalled work, independent of host speed."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Sequence


def _text(value: str | bytes | None) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else value or ""


def run_with_progress_deadline(
    command: Sequence[str], *, env: dict[str, str], progress_prefix: str, timeout: float = 30
) -> subprocess.CompletedProcess[str]:
    """Keep the same deadline for each native call; retain timeout diagnostics.

    Only flushed, test-authored stdout markers renew the deadline. Native
    logging on stderr cannot keep a stuck call alive. The caller's workload
    has a finite number of calls, so progress cannot extend it indefinitely.
    """
    deadline = time.monotonic() + timeout
    progress = 0
    with subprocess.Popen(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors="replace"
    ) as child:
        try:
            while True:
                remaining = max(0.0, deadline - time.monotonic())
                try:
                    stdout, stderr = child.communicate(timeout=min(0.25, remaining))
                    assert child.returncode is not None
                    return subprocess.CompletedProcess(command, child.returncode, stdout, stderr)
                except subprocess.TimeoutExpired as exc:
                    observed = sum(line.startswith(progress_prefix) for line in _text(exc.stdout).splitlines())
                    now = time.monotonic()
                    if observed > progress:
                        progress = observed
                        deadline = now + timeout
                    if now >= deadline:
                        child.kill()
                        stdout, stderr = child.communicate()
                        raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr) from None
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
