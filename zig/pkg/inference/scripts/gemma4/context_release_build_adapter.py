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

"""Strict adapter for antfly.context-release-build.v1/v2/v3/v4 evidence.

This module never upgrades historical/scoped receipts. It delegates the complete
archive, toolchain, command, resource, log, test, and executable audit to the
release auditor and returns only fields derived from the validated receipt.
"""
from pathlib import Path
import hashlib
import json
import os
import re
import stat


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _require(ok, message):
    if not ok:
        raise ValueError(message)


def validate(receipt_path, expected_binary_path=None, require_campaign_root=None):
    """Return validated build identity; every authority field comes from strict release evidence."""
    supplied_receipt = Path(receipt_path)
    _require(not supplied_receipt.is_symlink(), "invalid full release receipt path")
    receipt_path = supplied_receipt.resolve(strict=True)
    _require(receipt_path.is_file() and receipt_path.stat().st_size <= 16 * 1024 * 1024,
             "invalid full release receipt path")
    receipt = json.loads(receipt_path.read_text())
    _require(type(receipt) is dict and receipt.get("schema") in {"antfly.context-release-build.v1", "antfly.context-release-build.v2", "antfly.context-release-build.v3", "antfly.context-release-build.v4"} and
             receipt.get("evidence_scope") == "full_context_release_build" and
             receipt.get("release_eligible") is True and receipt.get("serving_qualified") is False and
             receipt.get("source_verified_after_build") is True,
             "historical, scoped, or release-ineligible build receipt")
    binary = receipt.get("binary")
    _require(type(binary) is dict and set(binary) == {"path", "sha256", "bytes"} and
             isinstance(binary["path"], str) and re.fullmatch(r"[0-9a-f]{64}", binary.get("sha256", "")) and
             type(binary["bytes"]) is int and not isinstance(binary["bytes"], bool) and binary["bytes"] > 0,
             "invalid full release binary binding")
    root = receipt_path.parent.resolve()
    if require_campaign_root is not None:
        _require(root == Path(require_campaign_root).resolve(strict=True),
                 "full release evidence and campaign must share one sealed root")
    # Lazy import prevents capture/release import cycles. This is the sole full
    # evidence authority, rather than a second weaker validator.
    import release_attention_matching as release
    audited = release.full_release_build_contract(root, receipt_path,
                                                   {"path": binary["path"], "sha256": binary["sha256"]})
    binary_path = (root / binary["path"]).resolve(strict=True)
    _require(binary_path.is_relative_to(root) and binary_path.is_file() and not binary_path.is_symlink() and
             os.access(binary_path, os.X_OK) and binary_path.stat().st_size == binary["bytes"] and
             digest(binary_path) == binary["sha256"], "full release executable changed")
    if expected_binary_path is not None:
        _require(Path(expected_binary_path).resolve(strict=True) == binary_path,
                 "selected executable is not the full release executable")
    return {
        "receipt_path": receipt_path,
        "receipt_sha256": digest(receipt_path),
        "binary_path": binary_path,
        "binary_sha256": audited["binary_sha256"],
        "source_archive_sha256": audited["source_archive_sha256"],
        "head": audited["head"],
        "source": receipt["source"],
        "receipt": receipt,
    }

BUNDLE_FIELDS = ("binary", "resource", "stdout", "stderr", "source_archive")


def _destination_path(root, relative_text, created_directories):
    relative = Path(relative_text)
    _require(not relative.is_absolute() and relative.parts and
             all(part not in ("", ".", "..") for part in relative.parts),
             "unsafe build artifact ref")
    parent = root
    for part in relative.parts[:-1]:
        parent = parent / part
        if parent.exists() or parent.is_symlink():
            _require(not parent.is_symlink() and parent.is_dir(),
                     "unsafe build evidence destination parent")
        else:
            parent.mkdir()
            created_directories.append(parent)
    target = parent / relative.parts[-1]
    _require(not target.exists() and not target.is_symlink(),
             "build evidence destination collision")
    return target


def _copy_exact(source, target, copied):
    # Register ownership immediately after exclusive creation so an exception
    # during the first read/write still removes the partial evidence file.
    with source.open("rb") as incoming:
        outgoing = target.open("xb")
        copied.append(target)
        try:
            for chunk in iter(lambda: incoming.read(1024 * 1024), b""):
                outgoing.write(chunk)
            outgoing.flush()
            os.fsync(outgoing.fileno())
        finally:
            outgoing.close()


def materialize_bundle(receipt_path, campaign_root, receipt_name="full-release-build.json"):
    """Copy an exact, closed release evidence graph; never rewrite the receipt."""
    source = validate(receipt_path)
    supplied_destination = Path(campaign_root)
    _require(not supplied_destination.is_symlink(), "invalid build evidence destination")
    destination = supplied_destination.resolve(strict=True)
    _require(destination.is_dir(), "invalid build evidence destination")
    receipt = source["receipt"]
    copied = []
    created_directories = []
    try:
        for field in BUNDLE_FIELDS:
            ref = receipt[field]
            _require(type(ref) is dict and isinstance(ref.get("path"), str), "invalid build artifact ref")
            relative = Path(ref["path"])
            _require(not relative.is_absolute() and all(part not in ("", ".", "..") for part in relative.parts),
                     "unsafe build artifact ref")
            src = (source["receipt_path"].parent / relative).resolve(strict=True)
            dst = _destination_path(destination, ref["path"], created_directories)
            _copy_exact(src, dst, copied)
            if field == "binary":
                dst.chmod(src.stat().st_mode)
        target = _destination_path(destination, receipt_name, created_directories)
        _copy_exact(source["receipt_path"], target, copied)
        result = validate(target, require_campaign_root=destination)
        _require(result["receipt_sha256"] == source["receipt_sha256"], "copied full release receipt changed")
        return result
    except Exception:
        for path in reversed(copied):
            try: path.unlink()
            except FileNotFoundError: pass
        for path in reversed(created_directories):
            try: path.rmdir()
            except (FileNotFoundError, OSError): pass
        raise


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("validate")
    check.add_argument("--receipt", type=Path, required=True)
    check.add_argument("--binary", type=Path)
    copy = sub.add_parser("materialize")
    copy.add_argument("--receipt", type=Path, required=True)
    copy.add_argument("--campaign-root", type=Path, required=True)
    args = parser.parse_args()
    result = (validate(args.receipt, args.binary) if args.command == "validate" else
              materialize_bundle(args.receipt, args.campaign_root))
    print(json.dumps({"receipt": str(result["receipt_path"]),
                      "receipt_sha256": result["receipt_sha256"],
                      "binary": str(result["binary_path"]),
                      "binary_sha256": result["binary_sha256"],
                      "source_archive_sha256": result["source_archive_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
