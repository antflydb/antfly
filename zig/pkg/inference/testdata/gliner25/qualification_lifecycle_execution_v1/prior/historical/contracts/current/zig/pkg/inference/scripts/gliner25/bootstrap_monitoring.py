#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run local contract commands with a verified, temporary official promtool.

Downloads have pinned sizes/hashes and bounded I/O; only the exact regular
promtool member is extracted. No package installation or server is started.
The child command inherits ANTFLY_GLINER25_PROMTOOL for the lifetime of the
private temporary directory. Nothing is retained in the checkout or PATH.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[5]
MANIFEST = ROOT / "devops/monitoring/gliner25/tooling.json"
CHUNK = 64 * 1024
RELEASE_BASE = "https://github.com/prometheus/prometheus/releases/download/v3.14.0/"


class ToolError(RuntimeError):
    pass


def current_platform() -> str:
    system = {"Linux": "linux", "Darwin": "darwin"}.get(platform.system())
    machine = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    if system is None or machine is None:
        raise ToolError("Unsupported promtool host platform")
    return f"{system}-{machine}"


def manifest():
    with MANIFEST.open("rb") as file:
        raw = file.read(16 * 1024 + 1)
    if len(raw) > 16 * 1024:
        raise ToolError("Tooling manifest exceeds its fixed ceiling")
    data = json.loads(raw)
    if data["version"] != 1 or data["release"] != "3.14.0" or data["tool"] != "promtool":
        raise ToolError("Unsupported tooling manifest")
    for item in [data["checksums"], *data["platforms"].values()]:
        url = item.get("url", item.get("archive_url"))
        digest = item.get("sha256", item.get("archive_sha256"))
        if not url.startswith(RELEASE_BASE) or not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ToolError("Tool identity is not a pinned official release")
    if set(data["platforms"]) != {"linux-amd64", "linux-arm64", "darwin-arm64"}:
        raise ToolError("Unexpected platform inventory")
    for host, item in data["platforms"].items():
        expected_name = f"prometheus-3.14.0.{host}.tar.gz"
        if (item["archive_name"] != expected_name or
                item["archive_url"] != RELEASE_BASE + expected_name or
                item["tool_member"] != f"prometheus-3.14.0.{host}/promtool" or
                type(item["archive_size_bytes"]) is not int or
                not 0 < item["archive_size_bytes"] <= 128 * 1024 * 1024):
            raise ToolError("Archive path or size is outside the fixed policy")
    if data["checksums"]["url"] != RELEASE_BASE + "sha256sums.txt":
        raise ToolError("Unexpected published checksum URL")
    ceilings = {"max_archive_bytes": 128 * 1024 * 1024,
                "max_tool_bytes": 256 * 1024 * 1024,
                "max_unpacked_bytes": 768 * 1024 * 1024,
                "max_tar_members": 64, "network_timeout_seconds": 30,
                "download_budget_seconds": 300, "command_timeout_seconds": 900}
    for key, maximum in ceilings.items():
        value = data["limits"][key]
        if type(value) is not int or not 0 < value <= maximum:
            raise ToolError("Tool preparation limit is outside the fixed policy")
    return data


class HttpsOnlyRedirects(urllib.request.HTTPRedirectHandler):
    max_redirections = 5
    max_repeats = 2

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme != "https" or parsed.username or parsed.password:
            raise ToolError("Tool download redirect must preserve HTTPS")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def check_deadline(deadline: float):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ToolError("Tool preparation deadline exceeded")
    return remaining


def download(url: str, destination: Path, size: int, digest: str,
             deadline: float, timeout: float, opener=None):
    """Read at most the pinned length plus one byte and retain only exact data."""
    if not url.startswith(RELEASE_BASE) or not (0 < size <= 128 * 1024 * 1024):
        raise ToolError("Download is outside the pinned release/size policy")
    open_url = opener or urllib.request.build_opener(HttpsOnlyRedirects()).open
    request = urllib.request.Request(url, headers={"User-Agent": "antfly-gliner25-contracts/1"})
    hasher = hashlib.sha256()
    copied = 0
    with open_url(request, timeout=min(timeout, check_deadline(deadline))) as response:
        if urllib.parse.urlparse(response.url).scheme != "https" or response.status != 200:
            raise ToolError("Tool download is not a successful HTTPS response")
        length = response.headers.get("Content-Length")
        if length is not None and int(length) != size:
            raise ToolError("Tool download declared size differs from the pin")
        encoding = response.headers.get("Content-Encoding", "identity")
        if encoding != "identity":
            raise ToolError("Encoded download differs from the pinned archive representation")
        with destination.open("xb") as output:
            while True:
                check_deadline(deadline)
                chunk = response.read(min(CHUNK, size - copied + 1))
                if not chunk:
                    break
                copied += len(chunk)
                if copied > size:
                    raise ToolError("Tool download exceeds its pinned size")
                hasher.update(chunk)
                output.write(chunk)
    if copied != size or hasher.hexdigest() != digest:
        raise ToolError("Tool download size or SHA-256 differs from the pin")


def check_published_checksum(path: Path, archive: dict):
    matches = []
    with path.open("rb") as file:
        raw = file.read(16 * 1024 + 1)
    if len(raw) > 16 * 1024:
        raise ToolError("Published checksum list exceeds its ceiling")
    for line in raw.decode("ascii").splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1].lstrip("*") == archive["archive_name"]:
            matches.append(fields[0])
    if matches != [archive["archive_sha256"]]:
        raise ToolError("Published archive checksum is missing, ambiguous or different")


def copy_tool(input_file, destination: Path, size: int, maximum: int,
              deadline: float, expected_digest: str | None = None):
    if not 0 < size <= maximum:
        raise ToolError("Unpacked promtool exceeds its ceiling")
    hasher = hashlib.sha256()
    remaining = size
    with destination.open("xb") as output:
        while remaining:
            check_deadline(deadline)
            chunk = input_file.read(min(CHUNK, remaining))
            if not chunk:
                raise ToolError("Truncated promtool payload")
            output.write(chunk)
            hasher.update(chunk)
            remaining -= len(chunk)
        if input_file.read(1):
            raise ToolError("Promtool payload exceeds its declared size")
    if expected_digest is not None and hasher.hexdigest() != expected_digest:
        raise ToolError("Existing promtool differs from its pinned binary digest")
    destination.chmod(0o700)
    return hasher.hexdigest()


def extract_tool(archive_path: Path, destination: Path, archive: dict,
                 limits: dict, deadline: float):
    found = False
    members = 0
    unpacked = 0
    digest = None
    with tarfile.open(archive_path, mode="r|gz") as tar:
        for member in tar:
            check_deadline(deadline)
            members += 1
            unpacked += member.size
            if (member.size < 0 or members > limits["max_tar_members"] or
                    unpacked > limits["max_unpacked_bytes"]):
                raise ToolError("Archive inventory exceeds its ceiling")
            if member.name != archive["tool_member"]:
                continue
            if found or not member.isreg():
                raise ToolError("Promtool member is duplicated or is not a regular file")
            found = True
            with tar.extractfile(member) as payload:
                digest = copy_tool(payload, destination, member.size, limits["max_tool_bytes"], deadline)
    if not found:
        raise ToolError("Official archive lacks its exact promtool member")
    return digest


@contextlib.contextmanager
def prepared_tool(existing: Path | None = None, temp_dir: Path | None = None):
    data = manifest()
    host = current_platform()
    if host not in data["platforms"]:
        raise ToolError("Host is outside the versioned promtool platform manifest")
    archive = data["platforms"][host]
    limits = data["limits"]
    if archive["archive_size_bytes"] > limits["max_archive_bytes"]:
        raise ToolError("Pinned archive exceeds the configured preparation ceiling")
    deadline = time.monotonic() + limits["download_budget_seconds"]
    with tempfile.TemporaryDirectory(prefix="antfly-gliner25-promtool-", dir=temp_dir) as temporary:
        owner = Path(temporary)
        owner.chmod(0o700)
        tool = owner / "promtool"
        if existing is not None:
            if "tool_sha256" not in archive or "tool_size_bytes" not in archive:
                raise ToolError("This platform has no separately pinned reusable binary")
            with existing.open("rb") as file:
                copy_tool(file, tool, archive["tool_size_bytes"], limits["max_tool_bytes"],
                          deadline, archive["tool_sha256"])
        else:
            sums = data["checksums"]
            checksum_path = owner / "sha256sums.txt"
            download(sums["url"], checksum_path, sums["size_bytes"], sums["sha256"],
                     deadline, limits["network_timeout_seconds"])
            check_published_checksum(checksum_path, archive)
            archive_path = owner / archive["archive_name"]
            download(archive["archive_url"], archive_path, archive["archive_size_bytes"],
                     archive["archive_sha256"], deadline, limits["network_timeout_seconds"])
            extract_tool(archive_path, tool, archive, limits, deadline)
        version = subprocess.run([str(tool), "--version"], check=True, capture_output=True,
                                 text=True, timeout=15, env=dict(os.environ, GOMAXPROCS="1"))
        if (f"version {data['release']} " not in version.stdout or
                data["source_revision"] not in version.stdout or
                host.replace("-", "/") not in version.stdout):
            raise ToolError("Verified promtool reported an unexpected build identity")
        print(f"Verified official promtool {data['release']} ({host}), "
              f"archive SHA-256 {archive['archive_sha256']}; temporary owner only", file=sys.stderr)
        yield tool, limits["command_timeout_seconds"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--existing", type=Path, help="Copy a separately pinned existing binary; never skip digest verification")
    parser.add_argument("--temp-dir", type=Path, help="Existing parent directory for the private temporary owner")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Command after --; receives ANTFLY_GLINER25_PROMTOOL")
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("A local contract command is required after --")
    try:
        with prepared_tool(args.existing, args.temp_dir) as (tool, timeout):
            env = dict(os.environ, ANTFLY_GLINER25_PROMTOOL=str(tool), GOMAXPROCS="1")
            result = subprocess.run(command, env=env, timeout=timeout)
            return result.returncode if result.returncode >= 0 else 128 - result.returncode
    except (ToolError, OSError, ValueError, tarfile.TarError,
            subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print(f"promtool preparation/check failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
