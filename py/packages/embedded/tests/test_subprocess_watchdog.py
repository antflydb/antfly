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
from types import SimpleNamespace

import pytest
import subprocess_watchdog as watchdog
from subprocess_watchdog import run_with_progress_deadline


def test_progressing_child_can_outlive_one_call_deadline(monkeypatch):
    class Child:
        now = 0.0
        returncode = None
        steps = 0
        stdout = ""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, *_):
            if exc_type is None:
                assert self.returncode == 0

        def poll(self):
            return self.returncode

        def communicate(self, timeout):
            # Model cumulative Popen output over five virtual seconds. Keep
            # the real watchdog's polling interval and deadline arithmetic.
            assert 0 < timeout <= 0.25
            self.now += timeout
            self.steps += 1
            self.stdout += f"step: {self.steps}\n"
            if self.steps == 20:
                self.returncode = 0
                return self.stdout, ""
            # communicate() timeouts expose bytes even with text=True; only
            # successful completion above returns decoded text.
            raise subprocess.TimeoutExpired("fixture", timeout, output=self.stdout.encode())

        def kill(self):
            pytest.fail("watchdog killed a progressing child")

    child = Child()
    monkeypatch.setattr(watchdog, "time", SimpleNamespace(monotonic=lambda: child.now))
    monkeypatch.setattr(
        watchdog,
        "subprocess",
        SimpleNamespace(
            Popen=lambda *args, **kwargs: child,
            PIPE=subprocess.PIPE,
            TimeoutExpired=subprocess.TimeoutExpired,
            CompletedProcess=subprocess.CompletedProcess,
        ),
    )
    result = run_with_progress_deadline(
        ["fixture"],
        env={},
        progress_prefix="step:",
        timeout=2,
    )
    assert result.returncode == 0, result.stderr
    assert len(result.stdout.splitlines()) == 20
    assert child.now == 5


@pytest.mark.parametrize("status", [0, 7])
def test_completed_child_preserves_real_pipe_output_and_status(status):
    result = run_with_progress_deadline(
        [
            sys.executable,
            "-c",
            f"import sys; print('step: done'); print('native log', file=sys.stderr); sys.exit({status})",
        ],
        env=os.environ.copy(),
        progress_prefix="step:",
        timeout=2,
    )
    assert result.returncode == status
    assert result.stdout == "step: done\n"
    assert result.stderr == "native log\n"


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
