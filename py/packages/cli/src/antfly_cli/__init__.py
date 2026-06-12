from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

__all__ = ["main"]


def _binary_path() -> Path:
    binary_name = "antfly.exe" if os.name == "nt" else "antfly"
    return Path(__file__).resolve().parent / "bin" / binary_name


def main() -> int:
    binary = _binary_path()
    if not binary.exists():
        sys.stderr.write(f"antfly-cli wheel is missing packaged executable: {binary}\n")
        return 127

    if os.name == "nt":
        completed = subprocess.run([str(binary), *sys.argv[1:]], check=False)
        return completed.returncode

    os.execv(str(binary), [str(binary), *sys.argv[1:]])
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
