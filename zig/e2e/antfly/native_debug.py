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

"""Opt-in debugger access for disposable Linux E2E children, preserved by exec."""

from __future__ import annotations

import ctypes
import os
import shutil
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path


def debuggable_command(command: list[str]) -> list[str]:
    if sys.platform == "linux" and os.environ.get("ANTFLY_E2E_NATIVE_STACKS") == "1":
        return [sys.executable, str(Path(__file__).resolve()), *command]
    return command


def native_stack_dumps(
    processes: Iterable[tuple[str, subprocess.Popen]],
    *,
    per_process_timeout_s: float = 10.0,
) -> str:
    """Capture live failure evidence before teardown; never retry the operation."""
    if shutil.which("gdb") is None:
        return "<gdb not available>"
    parts = []
    for label, proc in processes:
        if proc.poll() is not None:
            parts.append(f"[{label} pid {proc.pid}] exited rc={proc.returncode}")
            continue
        try:
            result = subprocess.run(
                [
                    "gdb",
                    "-p",
                    str(proc.pid),
                    "-batch",
                    "-ex",
                    "set pagination off",
                    "-ex",
                    "thread apply all bt 30",
                ],
                capture_output=True,
                text=True,
                timeout=per_process_timeout_s,
            )
            body = result.stdout[-250000:]
            if result.returncode != 0:
                body += f"\n<gdb rc={result.returncode}>\n{result.stderr[-2000:]}"
            parts.append(f"[{label} pid {proc.pid}]\n{body}")
        except Exception as exc:
            parts.append(f"[{label} pid {proc.pid}] gdb failed: {exc!r}")
    return "\n".join(parts)


def main() -> None:
    if sys.platform == "linux":
        # Yama normally permits only ancestors to attach. The diagnostic gdb
        # is a sibling of the server. Opt in only in this test launcher; do not
        # change the host's ptrace policy or the production executable.
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(0x59616D61, ctypes.c_ulong(-1), 0, 0, 0) != 0:
            print(
                f"E2E debugger access unavailable: {os.strerror(ctypes.get_errno())}",
                file=sys.stderr,
            )
    os.execv(sys.argv[1], sys.argv[1:])


if __name__ == "__main__":
    main()
