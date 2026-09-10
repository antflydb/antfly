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
import sys
from pathlib import Path


def debuggable_command(command: list[str]) -> list[str]:
    if sys.platform == "linux" and os.environ.get("ANTFLY_E2E_NATIVE_STACKS") == "1":
        return [sys.executable, str(Path(__file__).resolve()), *command]
    return command


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
