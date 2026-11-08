#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Acquire and verify the exact public Qwen pair without mutable Hub refs.

Range downloads use bounded concurrency and resumable part receipts. Only a full
file digest permits publication. Existing model directories are never trusted on
their names, and no Hub credentials or executable remote code are needed.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import threading
import time
import urllib.request

from exporter import BASE_MODELS, read_json, sha256

LOCK_PATH = Path(__file__).with_name("base_models.json")


def lock_model(role: str) -> dict:
    lock = read_json(LOCK_PATH)
    if lock.get("schema") != "antfly.memorilla.base-models.v1":
        raise ValueError("unsupported base model lock")
    model = lock["models"][role]
    if any(model.get(key) != value for key, value in BASE_MODELS[role].items()):
        raise ValueError("base model identity differs from memory contract")
    seen = set()
    for entry in model["files"]:
        relative = PurePosixPath(entry["path"])
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or "\\" in entry["path"]
            or entry["path"] in seen
        ):
            raise ValueError("invalid artifact path in base model lock")
        seen.add(entry["path"])
    return model


def verify_file(path: Path, entry: dict) -> None:
    if path.stat().st_size != entry["size_bytes"]:
        raise ValueError(f"artifact size mismatch: {path}")
    if "sha256" in entry:
        actual, expected = sha256(path), entry["sha256"]
    else:
        data = path.read_bytes()
        actual = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
        expected = entry["git_blob_sha1"]
    if actual != expected:
        raise ValueError(f"artifact checksum mismatch: {path}")


def verify_base_model(model_dir: Path, role: str) -> dict:
    model = lock_model(role)
    for entry in model["files"]:
        verify_file(Path(model_dir) / entry["path"], entry)
    return model


def download_file(
    url: str,
    target: Path,
    entry: dict,
    *,
    workers: int = 16,
    part_bytes: int = 1024 * 1024,
):
    """Download a pinned artifact; interrupted verified ranges can be resumed."""
    target = Path(target)
    if workers < 1 or workers > 64 or part_bytes < 1:
        raise ValueError("invalid download concurrency or part size")
    if target.exists():
        verify_file(target, entry)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".memorilla-part")
    receipt = target.with_name(target.name + ".memorilla-ranges.json")
    identity = {"url": url, "artifact": entry, "part_bytes": part_bytes}
    completed = set()
    if receipt.exists():
        saved = read_json(receipt)
        previous = saved.get("identity", {})
        if not partial.exists():
            raise ValueError(f"incompatible download resume state: {target}")
        if previous == identity:
            completed = set(saved["completed"])
        elif (
            previous.get("url") == url
            and previous.get("artifact") == entry
            and type(previous.get("part_bytes")) is int
            and previous["part_bytes"] > part_bytes
            and previous["part_bytes"] % part_bytes == 0
        ):
            # Refine already completed ranges without downloading them again.
            # Shorter requests avoid wasting multi-megabyte prefixes when a
            # slow CDN truncates a stream. Source identity stays unchanged.
            previous_size = previous["part_bytes"]
            previous_count = (entry["size_bytes"] + previous_size - 1) // previous_size
            for index in set(saved["completed"]):
                if type(index) is not int or not 0 <= index < previous_count:
                    raise ValueError("invalid range receipt")
                start = index * previous_size
                stop = min(entry["size_bytes"], start + previous_size)
                completed.update(
                    range(start // part_bytes, (stop + part_bytes - 1) // part_bytes)
                )
        else:
            raise ValueError(f"incompatible download resume state: {target}")
    fd = os.open(partial, os.O_RDWR | os.O_CREAT, 0o644)
    size = entry["size_bytes"]
    count = (size + part_bytes - 1) // part_bytes
    if any(
        type(index) is not int or index < 0 or index >= count for index in completed
    ):
        os.close(fd)
        raise ValueError("invalid range receipt")
    os.ftruncate(fd, size)
    guard = threading.Lock()

    def publish_receipt():
        temporary = receipt.with_name(receipt.name + f".{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps({"identity": identity, "completed": sorted(completed)}) + "\n"
        )
        temporary.replace(receipt)

    def fetch(index):
        start, end = index * part_bytes, min(size, (index + 1) * part_bytes)
        for attempt in range(4):
            try:
                # Make the byte range part of the redirect cache key as well as
                # the header. Some intermediate caches otherwise replay a
                # redirect signed for a different range of the same Hub URL.
                separator = "&" if "?" in url else "?"
                request = urllib.request.Request(
                    f"{url}{separator}antfly_range={start}-{end - 1}&antfly_attempt={attempt}",
                    headers={"Range": f"bytes={start}-{end - 1}"},
                )
                with urllib.request.urlopen(request, timeout=90) as response:
                    if response.status == 206:
                        expected_range = f"bytes {start}-{end - 1}/{size}"
                        if response.headers.get("Content-Range") != expected_range:
                            raise ValueError("server returned a different byte range")
                    elif not (response.status == 200 and start == 0 and end == size):
                        raise ValueError("server ignored bounded Range request")
                    position = start
                    while position < end:
                        chunk = response.read(min(1024 * 1024, end - position))
                        if not chunk:
                            raise ValueError("truncated download range")
                        view = memoryview(chunk)
                        while view:
                            written = os.pwrite(fd, view, position)
                            if written <= 0:
                                raise OSError("short download write")
                            position += written
                            view = view[written:]
                    if response.read(1):
                        raise ValueError("server exceeded requested range")
                with guard:
                    completed.add(index)
                    publish_receipt()
                    if len(completed) % 16 == 0 or len(completed) == count:
                        print(
                            f"{target.name}: {len(completed)}/{count} verified-length ranges",
                            flush=True,
                        )
                return
            except (OSError, ValueError) as error:
                if attempt == 3:
                    raise
                print(f"{target.name}: retry range {index}: {error}", flush=True)
                time.sleep(min(8, 2**attempt))

    try:
        publish_receipt()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(fetch, index)
                for index in range(count)
                if index not in completed
            ]
            try:
                for future in as_completed(futures):
                    future.result()
            except BaseException:
                # Keep failures bounded to already-running requests rather than
                # fetching all queued ranges after a hard network failure.
                for future in futures:
                    future.cancel()
                raise
        os.fsync(fd)
    finally:
        os.close(fd)
    verify_file(partial, entry)
    partial.rename(target)
    receipt.unlink()


def acquire_model(model_dir: Path, role: str, workers: int, file_workers: int = 2):
    if not 1 <= file_workers <= 4 or workers * file_workers > 64:
        raise ValueError("at most four files and 64 total download workers are allowed")
    model = lock_model(role)

    def fetch(entry):
        url = f"https://huggingface.co/{model['model']}/resolve/{model['revision']}/{entry['path']}"
        download_file(url, Path(model_dir) / entry["path"], entry, workers=workers)

    with ThreadPoolExecutor(max_workers=file_workers) as pool:
        futures = [
            pool.submit(fetch, entry)
            for entry in sorted(model["files"], key=lambda value: value["size_bytes"])
        ]
        try:
            for future in as_completed(futures):
                future.result()
        except BaseException:
            for future in futures:
                future.cancel()
            raise
    verify_base_model(model_dir, role)
    print(f"verified {model['model']}@{model['revision']}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("acquire", "verify"))
    parser.add_argument("--role", choices=("encoder", "decoder"), required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--file-workers", type=int, default=2)
    args = parser.parse_args()
    if args.command == "acquire":
        acquire_model(args.model_dir, args.role, args.workers, args.file_workers)
    else:
        verify_base_model(args.model_dir, args.role)
        print(f"verified {args.role}")


if __name__ == "__main__":
    main()
