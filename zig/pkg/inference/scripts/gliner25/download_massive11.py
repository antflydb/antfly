#!/usr/bin/env python3
"""Fetch the exact public MASSIVE release and selected members, serially."""
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import tarfile

import evaluation_contract as evaluation
import oracle
from prepare_massive11 import MANIFEST


def fetch(url, destination, pin):
    destination.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["curl", "--fail", "--silent", "--show-error", "--location", "--connect-timeout", "10",
        "--max-time", "180", "--speed-limit", "16384", "--speed-time", "30", "--max-filesize", str(pin["size_bytes"]),
        "--output", str(destination), url], check=True)
    oracle.verify_file(destination, pin)


def extract_selected(archive_path, output, pins, prefix):
    evaluation.checked(isinstance(prefix, str), "unsafe archive prefix")
    prefix_path = Path(prefix)
    evaluation.checked(prefix.endswith("/") and not prefix_path.is_absolute()
                       and ".." not in prefix_path.parts, "unsafe archive prefix")
    wanted = {prefix + pin["path"]: pin for pin in pins}
    evaluation.checked(len(wanted) == len(pins), "duplicate requested archive member")
    for pin in pins:
        relative = Path(pin["path"])
        evaluation.checked(not relative.is_absolute() and ".." not in relative.parts and 0 < pin["size_bytes"] <= 16 * 1024 * 1024,
                           "unsafe or oversized selected archive path")
    seen, count, expanded = set(), 0, 0
    with tarfile.open(archive_path, "r|gz") as archive:
        for member in archive:
            evaluation.checked(member.size >= 0, "negative archive member size")
            count += 1; expanded += member.size
            evaluation.checked(count <= 256 and expanded <= 1024 * 1024 * 1024, "archive expansion budget exceeded")
            if member.name not in wanted:
                continue
            pin = wanted[member.name]
            evaluation.checked(member.name not in seen and member.isfile() and member.size == pin["size_bytes"],
                               "archive member identity/type differs")
            seen.add(member.name)
            stream = archive.extractfile(member)
            evaluation.checked(stream is not None, "missing archive member stream")
            with stream:
                data = stream.read(pin["size_bytes"] + 1)
            evaluation.checked(len(data) == pin["size_bytes"] and evaluation.digest(data) == pin["sha256"], "archive member content differs")
            destination = output / pin["path"]; destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as target: target.write(data)
    evaluation.checked(seen == set(wanted), "selected archive member is absent")


def download(output):
    manifest = oracle.read_json(MANIFEST)
    with oracle.atomic_output_directory(output) as staging:
        archive = manifest["archive"]
        fetch(archive["url"], staging / archive["path"], archive)
        members = [pin for pin in manifest["files"] if pin["path"] not in manifest["repository_paths"]]
        extract_selected(staging / archive["path"], staging, members, manifest["archive_member_prefix"])
        for pin in manifest["files"]:
            if pin["path"] in manifest["repository_paths"]:
                url = f"https://raw.githubusercontent.com/{manifest['repository']}/{manifest['revision']}/{manifest['repository_paths'][pin['path']]}"
                fetch(url, staging / pin["path"], pin)
        for pin in manifest["files"]: oracle.verify_file(staging / pin["path"], pin)
        oracle.write_json(staging / "download.json", {"scope": "gliner25_massive11_source_acquisition/v1", "qualification": False,
            "manifest_sha256": oracle.sha256_file(MANIFEST), "generator_sha256": oracle.sha256_file(Path(__file__)),
            "archive": archive, "files": manifest["files"]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    download(args.output_dir)
    print("verified source cache", args.output_dir)


if __name__ == "__main__":
    main()
