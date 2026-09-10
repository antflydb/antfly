# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""The optional tracing launcher must preserve server PID and arguments."""

import os
import subprocess
import sys
from pathlib import Path

import native_debug
import pytest


@pytest.mark.parametrize("platform,enabled", [("linux", "0"), ("darwin", "1")])
def test_native_debug_is_opt_in_and_linux_only(monkeypatch, platform, enabled):
    monkeypatch.setattr(native_debug.sys, "platform", platform)
    monkeypatch.setenv("ANTFLY_E2E_NATIVE_STACKS", enabled)
    command = ["/bin/antfly", "data", "--config", "a path.json"]
    assert native_debug.debuggable_command(command) is command


def test_native_debug_preserves_command_arguments(monkeypatch):
    monkeypatch.setattr(native_debug.sys, "platform", "linux")
    monkeypatch.setenv("ANTFLY_E2E_NATIVE_STACKS", "1")
    command = ["/bin/antfly", "data", "--config", "a path.json"]
    wrapped = native_debug.debuggable_command(command)
    assert wrapped[:2] == [sys.executable, str(Path(native_debug.__file__).resolve())]
    assert wrapped[2:] == command


def test_native_debug_exec_preserves_child_pid():
    command = [
        sys.executable,
        str(Path(native_debug.__file__).resolve()),
        sys.executable,
        "-c",
        "import os, sys; print(os.getpid()); print(sys.argv[1])",
        "argument with spaces",
    ]
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as proc:
        output, _ = proc.communicate(timeout=10)
    assert proc.returncode == 0
    assert output.splitlines() == [str(proc.pid), "argument with spaces"]
    assert proc.pid != os.getpid()
