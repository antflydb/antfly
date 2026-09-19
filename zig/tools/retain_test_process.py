#!/usr/bin/env python3
"""Retain a live Linux test executable before the unit watchdog terminates it."""

import argparse
import hashlib
import json
import shutil
from pathlib import Path


def retain(pid: int, destination: Path, proc: Path = Path("/proc")) -> bool:
    process = proc / str(pid)
    executable = (process / "exe").readlink()
    name = executable.name.removesuffix(" (deleted)")
    if name != "test" and not name.endswith("-tests"):
        return False
    # Open through proc: even an unlinked cache executable remains recoverable.
    command = (process / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    cwd = str((process / "cwd").readlink())
    destination.mkdir(parents=True, exist_ok=True)
    output = destination / f"test-{pid}"
    shutil.copyfile(process / "exe", output)
    output.chmod(0o755)
    digest = hashlib.sha256()
    with output.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    output.with_suffix(".json").write_text(
        json.dumps(
            {
                "executable": str(executable),
                "argv": [arg.decode(errors="replace") for arg in command],
                "cwd": cwd,
                "sha256": digest.hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pid", type=int)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    retain(args.pid, args.destination)
