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

"""Prune archived build payloads, retaining measurements and honest seal tombstones."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

BASE = Path(__file__).resolve().parents[1]
REPO = BASE.parents[1]
DIST = BASE / "dist"
QUAL = DIST / "qualification"
MAGIC = {
    b"\x00asm",
    b"\x7fELF",
    bytes.fromhex("cffaedfe"),
    bytes.fromhex("cefaedfe"),
    bytes.fromhex("feedfacf"),
    bytes.fromhex("feedface"),
    bytes.fromhex("cafebabe"),
    bytes.fromhex("bebafeca"),
}


def protected(path):
    return (
        path.is_symlink()
        or not path.resolve().is_relative_to(DIST)
        or any(
            re.search(
                r"token|credential|secret|\.pem$|\.key$|^\.env|^\.dev\.vars", part, re.I
            )
            for part in path.relative_to(DIST).parts
        )
    )


def retained(path):
    return any(
        part.startswith("local-retention-") for part in path.relative_to(DIST).parts
    )


def digest(path):
    assert not protected(path)
    before = path.stat()
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError("Artifact changed during hashing: " + str(path))
    return h.hexdigest()


def git(*args):
    return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()


def archive_commit(path):
    try:
        with tarfile.open(path, "r:*") as archive:
            commit = archive.pax_headers.get("comment", "")
        if not re.fullmatch("[a-f0-9]{40}", commit):
            return None
        return (
            commit
            if subprocess.run(
                ["git", "cat-file", "-e", commit + "^{commit}"],
                cwd=REPO,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
            else None
        )
    except (tarfile.TarError, OSError):
        return None


def inventory():
    rows = []
    for root, dirs, files in os.walk(DIST):
        parent = Path(root)
        dirs[:] = [
            name
            for name in dirs
            if not protected(parent / name)
            and not retained(parent / name)
            and name not in {"node_modules", ".git"}
        ]
        for name in files:
            path = parent / name
            if protected(path) or parent == DIST:
                continue  # Keep active top-level runtime/build modules.
            if path.suffix == ".tar" or name.endswith((".tar.gz", ".tgz")):
                commit = archive_commit(path)
                if commit:
                    rows.append(
                        {
                            "path": str(path.relative_to(BASE)),
                            "bytes": path.stat().st_size,
                            "kind": "git-source-archive",
                            "commit": commit,
                        }
                    )
                continue  # Unknown/dirty source archives are retained.
            if path.stat().st_size < 1024 * 1024:
                continue
            with path.open("rb") as stream:
                magic = stream.read(4)
            if magic in MAGIC:
                rows.append(
                    {
                        "path": str(path.relative_to(BASE)),
                        "bytes": path.stat().st_size,
                        "kind": "archived-wasm"
                        if magic == b"\x00asm"
                        else "archived-native-executable",
                    }
                )
    return sorted(rows, key=lambda row: row["path"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually remove the inventoried payloads; default is dry run.",
    )
    args = parser.parse_args()
    rows = inventory()
    counts = {}
    for row in rows:
        bucket = counts.setdefault(row["kind"], {"files": 0, "bytes": 0})
        bucket["files"] += 1
        bucket["bytes"] += row["bytes"]
    print(
        json.dumps(
            {
                "apply": args.apply,
                "candidates": len(rows),
                "candidate_bytes": sum(r["bytes"] for r in rows),
                "by_kind": counts,
            }
        ),
        flush=True,
    )
    if not args.apply or not rows:
        return
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = QUAL / ("local-retention-" + timestamp)
    output.mkdir(parents=True, exist_ok=False)
    control = json.loads(
        (BASE / "qualification/caller-control-correctness-evidence.json").read_text()
    )
    control_hashes = control["native_sha256"]
    # Use retained manifests to find one copy of each exact approved control module.
    copies = {}
    for manifest in QUAL.glob("local-retention-*/retention.json"):
        for entry in json.loads(manifest.read_text()).get(
            "approved_control_modules", []
        ):
            source = BASE / entry["path"]
            if (
                entry["sha256"] in control_hashes.values()
                and source.exists()
                and not protected(source)
            ):
                copies.setdefault(entry["sha256"], source)
    for root, dirs, files in os.walk(DIST):
        parent = Path(root)
        dirs[:] = [
            name
            for name in dirs
            if not protected(parent / name)
            and not retained(parent / name)
            and name not in {"node_modules", ".git"}
        ]
        if "evidence.json" not in files:
            continue
        path = parent / "evidence.json"
        if protected(path):
            continue
        data = json.loads(path.read_text())
        for entry in data.get("artifacts", []):
            if entry.get("sha256") not in control_hashes.values():
                continue
            source = Path(entry["path"])
            source = source if source.is_absolute() else BASE / source
            if source.exists() and not protected(source):
                copies.setdefault(entry["sha256"], source)
    if set(copies) != set(control_hashes.values()):
        raise RuntimeError(
            "Cannot locate both approved control modules; no payloads deleted."
        )
    kept = []
    for name, expected in control_hashes.items():
        source = copies[expected]
        if digest(source) != expected:
            raise RuntimeError("Approved control hash mismatch; no payloads deleted.")
        target = source if retained(source) else output / "approved-control" / name
        target.parent.mkdir(exist_ok=True)
        if target != source:
            shutil.copy2(source, target)
        verified = hashlib.sha256()
        with target.open("rb") as stream:
            for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                verified.update(chunk)
        actual = verified.hexdigest()
        assert actual == expected
        kept.append(
            {
                "path": str(target.relative_to(BASE)),
                "sha256": expected,
                "bytes": target.stat().st_size,
            }
        )
    refs = []
    for commit in sorted(
        {r["commit"] for r in rows if "commit" in r}
        | {control["proposed_control_revision"]}
    ):
        ref = "refs/qualification/source/" + commit
        subprocess.run(["git", "update-ref", ref, commit], cwd=REPO, check=True)
        refs.append({"ref": ref, "commit": commit})
    # Journal every removal before unlinking. Original evidence manifests are never rewritten.
    journal = output / "removed.jsonl"
    with journal.open("x") as log:
        for row in rows:
            path = BASE / row["path"]
            assert not protected(path) and path.stat().st_size == row["bytes"]
            row["sha256"] = digest(path)
            log.write(json.dumps(row) + "\n")
            log.flush()
            os.fsync(log.fileno())
            path.unlink()
    result = {
        "at": timestamp,
        "qualification": False,
        "removed_files": len(rows),
        "removed_bytes": sum(r["bytes"] for r in rows),
        "by_kind": counts,
        "source_refs": refs,
        "approved_control_modules": kept,
        "active_modules_preserved": [
            str(p.relative_to(BASE)) for p in sorted(DIST.glob("*.wasm"))
        ],
        "removed_ledger": str(journal.relative_to(BASE)),
        "retained": "Reports, samples, logs, manifests, hashes, toolchain records, bundles, patches, and source archives without recoverable Git metadata.",
        "seal_status": "Original manifests are preserved as historical records. Seals referencing removed payloads no longer support full payload re-verification. Missing files are intentional pruning, not successful verification.",
        "rebuild": "Git refs keep archived commits reachable locally; use their lockfiles and recorded toolchains. A rebuild is not guaranteed to reproduce exact bytes. The two approved control modules and active runtime modules remain available.",
    }
    (output / "retention.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                "retention": str((output / "retention.json").relative_to(BASE)),
                "removed_files": len(rows),
                "removed_bytes": result["removed_bytes"],
                "preserved_control_bytes": sum(r["bytes"] for r in kept),
            }
        )
    )


if __name__ == "__main__":
    main()
