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

"""The native-child watchdog permits progress and still bounds stalled calls."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from subprocess_watchdog import run_with_progress_deadline


def test_progressing_child_can_outlive_one_call_deadline():
    result = run_with_progress_deadline(
        [sys.executable, "-c", "import time; [(print('step:', i, flush=True), time.sleep(.1)) for i in range(40)]"],
        env=os.environ.copy(),
        progress_prefix="step:",
        timeout=2,
    )
    assert result.returncode == 0, result.stderr
    assert len(result.stdout.splitlines()) == 40


def test_native_logging_does_not_extend_a_stalled_call():
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        run_with_progress_deadline(
            [
                sys.executable,
                "-c",
                "import sys,time; print('step: native call', flush=True); [(print('noise', file=sys.stderr, flush=True), time.sleep(.1)) for _ in range(100)]",
            ],
            env=os.environ.copy(),
            progress_prefix="step:",
            timeout=2,
        )
    assert "step: native call" in str(caught.value.stdout)
    assert "noise" in str(caught.value.stderr)
