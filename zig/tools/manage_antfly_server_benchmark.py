#!/usr/bin/env python3
"""Start, stop, or restart a detached Antfly benchmark server."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from pathlib import Path


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "stop", "restart"))
    parser.add_argument("--mode", choices=("graceful", "crash"), default="graceful")
    parser.add_argument("--pid-file", required=True, type=Path)
    parser.add_argument("--log-file", required=True, type=Path)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument(
        "--server-subcommand",
        choices=("standalone", "swarm"),
        default="standalone",
        help="server entry point exposed by the selected Antfly binary",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--health-port", required=True, type=int)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--stop-timeout-seconds", type=float, default=30)
    return parser.parse_args()


def read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def stop(path: Path, mode: str, timeout: float) -> None:
    pid = read_pid(path)
    if pid is None or not alive(pid):
        path.unlink(missing_ok=True)
        return
    os.kill(pid, signal.SIGTERM if mode == "graceful" else signal.SIGKILL)
    deadline = time.monotonic() + (timeout if mode == "graceful" else 5)
    while time.monotonic() < deadline:
        if not alive(pid):
            path.unlink(missing_ok=True)
            return
        time.sleep(0.05)
    if mode == "graceful" and alive(pid):
        os.kill(pid, signal.SIGKILL)
        while time.monotonic() < deadline + 5 and alive(pid):
            time.sleep(0.05)
    if alive(pid):
        raise TimeoutError(f"Antfly process {pid} did not exit")
    path.unlink(missing_ok=True)


def start(args: argparse.Namespace) -> int:
    existing = read_pid(args.pid_file)
    if existing is not None and alive(existing):
        raise RuntimeError(f"Antfly process already running: {existing}")
    args.data_dir.mkdir(parents=True, exist_ok=True)
    args.pid_file.parent.mkdir(parents=True, exist_ok=True)
    args.log_file.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(args.binary),
        args.server_subcommand,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--health-port",
        str(args.health_port),
        "--data-dir",
        str(args.data_dir),
    ]
    with args.log_file.open("ab", buffering=0) as log:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    temporary = args.pid_file.with_suffix(args.pid_file.suffix + ".tmp")
    temporary.write_text(f"{process.pid}\n", encoding="utf-8")
    temporary.replace(args.pid_file)
    return process.pid


def main() -> int:
    args = arguments()
    if args.action in ("stop", "restart"):
        stop(args.pid_file, args.mode, args.stop_timeout_seconds)
    if args.action in ("start", "restart"):
        print(start(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
