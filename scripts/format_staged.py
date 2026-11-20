#!/usr/bin/env python3
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

"""Format staged blobs, preserving unrelated and partially staged edits."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


class HookError(Exception):
    pass


# Another git process (an editor, a status poller, a concurrent commit in
# another worktree's hooks) can hold the real index's lock for a moment.
# Waiting it out is better than failing the commit.
INDEX_LOCK_RETRY_SECONDS = 5.0
INDEX_LOCK_RETRY_INTERVAL = 0.05


def run(args, *, cwd, data=None, env=None):
    deadline = time.monotonic() + INDEX_LOCK_RETRY_SECONDS
    while True:
        try:
            return subprocess.run(
                args,
                cwd=cwd,
                input=data,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                env=env,
            ).stdout
        except FileNotFoundError as error:
            raise HookError(
                f"Required formatting tool is unavailable: {args[0]}"
            ) from error
        except subprocess.CalledProcessError as error:
            if b"index.lock" in (error.stderr or b"") and time.monotonic() < deadline:
                time.sleep(INDEX_LOCK_RETRY_INTERVAL)
                continue
            sys.stderr.buffer.write(error.stderr or b"")
            raise HookError(f"Command failed: {' '.join(args)}") from error


def formatter(root, path):
    suffix = Path(path).suffix
    if suffix == ".zig":
        return root, ["zig", "fmt", "--stdin"]
    if suffix == ".go":
        return root, ["gofmt"]
    if suffix == ".py":
        return root, [
            "uv",
            "run",
            "--project",
            "py/packages/sdk",
            "--locked",
            "ruff",
            "format",
            "--stdin-filename",
            path,
            "-",
        ]
    if path.startswith("rs/") and suffix == ".rs":
        return root / "rs", [
            "rustfmt",
            "--edition",
            "2024",
            "--emit",
            "stdout",
            "--config",
            "skip_children=true",
        ]
    if path.startswith("ts/") and suffix in {
        ".ts",
        ".tsx",
        ".js",
        ".jsx",
        ".mjs",
        ".cjs",
        ".mts",
        ".cts",
        ".json",
        ".jsonc",
        ".css",
        ".graphql",
        ".gql",
        ".html",
    }:
        return root / "ts", [
            "node",
            "scripts/run-pinned-toolchain.mjs",
            "pnpm",
            "--config.verify-deps-before-run=error",
            "exec",
            "biome",
            "format",
            "--stdin-file-path",
            path[3:],
        ]
    return None


def format_staged(root, format_blob=None):
    def git(*args, data=None, env=None):
        return run(["git", "--literal-pathspecs", *args], cwd=root, data=data, env=env)

    # Work only against a private index until every formatter has succeeded.
    # Git generates the patch, so renames, quoting, file modes, and new files
    # keep their normal Git semantics. Never restage the working-tree file.
    with tempfile.TemporaryDirectory(prefix="antfly-format-") as temp:
        temporary_index = Path(temp) / "index"
        env = {**os.environ, "GIT_INDEX_FILE": str(temporary_index)}
        # One snapshot of the staged state: the path list, the input blobs
        # and the patch baseline all come from this private copy, so a
        # concurrent `git add` cannot mix two versions of the index. Reading
        # a copy also never takes the real index's lock (`write-tree`
        # refreshes the index it reads).
        real_index = Path(os.fsdecode(git("rev-parse", "--git-path", "index").strip()))
        if not real_index.is_absolute():
            real_index = Path(root) / real_index
        snapshot = real_index.read_bytes()
        temporary_index.write_bytes(snapshot)
        paths = git(
            "diff",
            "--cached",
            "--no-color",
            "--name-only",
            "--diff-filter=ACMR",
            "-z",
            env=env,
        ).split(b"\0")
        paths = [os.fsdecode(path) for path in paths if path]
        selected = [(path, formatter(root, path)) for path in paths]
        selected = [(path, command) for path, command in selected if command]
        if not selected:
            return
        before = git("write-tree", env=env).strip().decode()
        for path, (cwd, command) in selected:
            entry = git("ls-files", "--stage", "-z", "--", path, env=env).split(b"\0")[
                0
            ]
            mode, blob, stage = entry.split(b"\t", 1)[0].split()
            if stage != b"0" or mode not in (b"100644", b"100755"):
                raise HookError(f"Cannot format non-regular or unmerged file: {path}")
            original = git("cat-file", "blob", blob.decode())
            formatted = (
                format_blob(path, original)
                if format_blob
                else run(command, cwd=cwd, data=original)
            )
            if formatted == original:
                continue
            new_blob = (
                git("hash-object", "-w", "--stdin", data=formatted).strip().decode()
            )
            git("update-index", "--cacheinfo", mode.decode(), new_blob, path, env=env)
        patch = git(
            "diff",
            "--cached",
            "--binary",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--unified=3",
            "--src-prefix=a/",
            "--dst-prefix=b/",
            before,
            env=env,
        )
        if not patch:
            return
        if real_index.read_bytes() != snapshot:
            raise HookError(
                "The index changed while formatting. No edits were applied; "
                "commit again."
            )
        # A formatting hunk that overlaps an unstaged edit needs manual
        # resolution. Reject it before changing either the index or files.
        try:
            git("apply", "--check", "--whitespace=nowarn", data=patch)
            git("apply", "--cached", "--check", "--whitespace=nowarn", data=patch)
        except HookError as error:
            raise HookError(
                "Formatting overlaps unstaged changes or the index changed. "
                "No edits were applied. Run make fmt and review/stage the desired hunks."
            ) from error
        git("apply", "--whitespace=nowarn", data=patch)
        try:
            update_index_under_lock(git, real_index, snapshot, patch)
        except HookError:
            git("apply", "--reverse", "--whitespace=nowarn", data=patch)
            raise
        print("Formatted staged files; unstaged edits remain unstaged.")


def update_index_under_lock(git, real_index, snapshot, patch):
    """Stage the formatting patch with Git's own index lock held throughout.

    The comparison against the snapshot and the staged update share one
    `index.lock`, Git's update protocol: build the new index inside the lock
    file and rename it into place. No concurrent `git add` can land between
    the check and the update, as one could between separate Git commands.
    """
    lock_path = real_index.with_name(real_index.name + ".lock")
    deadline = time.monotonic() + INDEX_LOCK_RETRY_SECONDS
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o666)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise HookError(
                    "The index is locked by another Git process. No edits were applied."
                )
            time.sleep(INDEX_LOCK_RETRY_INTERVAL)
    try:
        if real_index.read_bytes() != snapshot:
            raise HookError(
                "The index changed while formatting. No edits were applied; "
                "commit again."
            )
        with os.fdopen(fd, "wb") as lock_file:
            fd = None
            lock_file.write(snapshot)
        # The lock file is the index being built; Git locks it as its own
        # `<lock>.lock`, which nothing else contends for.
        git(
            "apply",
            "--cached",
            "--whitespace=nowarn",
            data=patch,
            env={**os.environ, "GIT_INDEX_FILE": str(lock_path)},
        )
        os.replace(lock_path, real_index)
    except BaseException:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(lock_path)
        except FileNotFoundError:
            pass
        raise


def main():
    try:
        root = Path(
            os.fsdecode(
                run(["git", "rev-parse", "--show-toplevel"], cwd=Path.cwd()).strip()
            )
        )
        format_staged(root)
    except HookError as error:
        print(
            f"pre-commit: {error}\nUse git commit --no-verify to bypass hooks.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
