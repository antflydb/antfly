#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Use a bounded fixture tmpfs, including unprivileged ARC runner pods."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


BUDGET = 512 * 1024 * 1024
MIN_FREE = 384 * 1024 * 1024


def validate_root(root):
    root = Path(root)
    if not root.is_absolute() or not root.is_dir():
        raise ValueError(f"fixture root must be an existing absolute directory: {root}")
    kind = subprocess.check_output(
        ["stat", "-f", "-c", "%T", str(root)], text=True
    ).strip()
    space = os.statvfs(root)
    total, available = space.f_blocks * space.f_frsize, space.f_bavail * space.f_frsize
    if kind != "tmpfs" or not MIN_FREE <= total <= BUDGET or available < MIN_FREE:
        raise ValueError(
            f"fixture root needs bounded tmpfs with 384 MiB free and at most 512 MiB total; {root}: type={kind}, total={total}, free={available}"
        )
    return root


def prepare(environ):
    configured = environ.get("ANTFLY_CI_FIXTURE_ROOT")
    root = Path(configured or "/mnt/antfly-fixtures")
    if configured or root.exists():
        # A configured but unusable volume is a deployment error, not a reason
        # to silently regress to disk. Each job gets its own private directory.
        validate_root(root)
        return Path(tempfile.mkdtemp(prefix="antfly-fixtures.", dir=root)), False
    workspace = Path(
        tempfile.mkdtemp(prefix="antfly-fixtures.", dir=environ["RUNNER_TEMP"])
    )
    result = subprocess.run(
        [
            "sudo",
            "-n",
            "mount",
            "-t",
            "tmpfs",
            "-o",
            f"size={BUDGET},mode=0700,uid={os.getuid()},gid={os.getgid()}",
            "tmpfs",
            str(workspace),
        ],
        check=False,
    )
    if result.returncode == 0:
        return workspace, True
    workspace.rmdir()
    print(
        "::warning::No bounded fixture tmpfs. Mount denied; using disk. Provision /mnt/antfly-fixtures as a 512Mi memory emptyDir (see zig/tools/CI_UNIT_TAIL_FOLLOWUP.md)."
    )
    return None


def cleanup(workspace, mounted):
    path = Path(workspace)
    if (
        not path.is_absolute()
        or not path.name.startswith("antfly-fixtures.")
        or path.is_symlink()
    ):
        raise ValueError(f"not a private fixture workspace: {path}")
    if mounted:
        subprocess.run(["sudo", "-n", "umount", str(path)], check=True)
        path.rmdir()
    else:
        # Never unmount a runner-owned volume; only remove this job's directory.
        shutil.rmtree(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "cleanup"))
    args = parser.parse_args()
    if args.action == "cleanup":
        cleanup(
            os.environ["ANTFLY_TEST_WORKSPACE"],
            os.environ.get("ANTFLY_TEST_WORKSPACE_MOUNTED") == "1",
        )
        return
    result = prepare(os.environ)
    if result is not None:
        workspace, mounted = result
        with open(os.environ["GITHUB_ENV"], "a") as env:
            env.write(
                f"ANTFLY_TEST_WORKSPACE={workspace}\nANTFLY_TEST_WORKSPACE_MOUNTED={int(mounted)}\n"
            )
        subprocess.run(["df", "-h", str(workspace)], check=True)


if __name__ == "__main__":
    main()
