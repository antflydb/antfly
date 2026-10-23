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
    sampler = shutil.which("sample") if sys.platform == "darwin" else None
    debugger = "sample" if sampler else "gdb"
    if not sampler and shutil.which("gdb") is None:
        return "<native stack debugger not available>"
    parts = []
    for label, proc in processes:
        if proc.poll() is not None:
            parts.append(f"[{label} pid {proc.pid}] exited rc={proc.returncode}")
            continue
        try:
            command = (
                [sampler, str(proc.pid), "1", "1", "-mayDie", "-file", "/dev/stdout"]
                if sampler
                else [
                    "gdb",
                    "--readnever",
                    "-q",
                    "-nx",
                    "-p",
                    str(proc.pid),
                    "-batch",
                    "-ex",
                    "set pagination off",
                    "-ex",
                    "thread apply all bt 30",
                ]
            )
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=per_process_timeout_s,
            )
            body = result.stdout[-250000:]
            if result.returncode != 0:
                body += (
                    f"\n<{debugger} rc={result.returncode}>\n{result.stderr[-2000:]}"
                )
            parts.append(f"[{label} pid {proc.pid}]\n{body}")
        except subprocess.TimeoutExpired as exc:
            # Preserve any frames emitted before the deadline. Loading full
            # release DWARF can exceed this budget before printing a frame;
            # --readnever retains minimal symbols and native unwind tables.
            partial = exc.stdout or b""
            if isinstance(partial, bytes):
                partial = partial.decode(errors="replace")
            parts.append(
                f"[{label} pid {proc.pid}] {debugger} timed out\n{partial[-250000:]}"
            )
        except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
            parts.append(f"[{label} pid {proc.pid}] {debugger} failed: {exc!r}")
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
