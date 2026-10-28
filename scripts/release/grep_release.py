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

"""Validate, qualify, seal and publish independently versioned grep releases."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import tempfile

from release_channels import is_canonical_tag

PREFIX = "antfly-grep/"
REQUEST_WORKFLOW = ".github/workflows/grep-release-request.yml"
CONTROL_PATHS = (
    REQUEST_WORKFLOW,
    ".github/workflows/grep-release.yml",
    "scripts/release/grep_release.py",
)
PLATFORMS = (
    ("linux-x64-musl", "ubuntu-latest", "x86_64-linux-musl", "Linux_x86_64"),
    ("linux-x64-gnu", "ubuntu-latest", "x86_64-linux-gnu.2.28", "Linux_x86_64_gnu"),
    ("linux-arm64-musl", "ubuntu-24.04-arm", "aarch64-linux-musl", "Linux_arm64"),
    (
        "linux-arm64-gnu",
        "ubuntu-24.04-arm",
        "aarch64-linux-gnu.2.28",
        "Linux_arm64_gnu",
    ),
    ("darwin-arm64", "macos-15", "aarch64-macos.15.0", "Darwin_arm64"),
)
SHA = re.compile(r"[0-9a-f]{40}")
IDENTITY_FIELDS = {
    "schema_version",
    "product",
    "tag",
    "version",
    "source_commit",
    "source_ref_head",
    "build_controller_commit",
    "request_run_id",
    "build_run_id",
    "source_date_epoch",
}


def version(tag: str) -> str:
    if not tag.startswith(PREFIX) or not is_canonical_tag(tag[len(PREFIX) :]):
        raise ValueError(
            "expected antfly-grep/vX.Y.Z or antfly-grep/vX.Y.Z-{alpha,beta,rc}.N"
        )
    return tag[len(PREFIX) + 1 :]


def filename(release_version: str, suffix: str) -> str:
    return f"antfly-grep_{release_version}_{suffix}.tar.gz"


def matrix() -> dict:
    return {
        "include": [
            {"id": identity, "runner": runner, "target": target, "suffix": suffix}
            for identity, runner, target, suffix in PLATFORMS
        ]
    }


def run(command: list[str], **kwargs) -> str:
    return subprocess.run(
        command, check=True, text=True, capture_output=True, **kwargs
    ).stdout.strip()


def git(repo: Path, *args: str) -> str:
    return run(["git", "-C", str(repo), *args])


def read_json(path: Path) -> dict:
    if path.stat().st_size > 1024 * 1024:
        raise ValueError(f"oversized metadata: {path}")
    document = json.loads(path.read_text())
    if not isinstance(document, dict):
        raise ValueError(f"expected object: {path}")
    return document


def validate_identity(identity: dict) -> None:
    if set(identity) != IDENTITY_FIELDS:
        raise ValueError("unexpected grep identity fields")
    if type(identity["schema_version"]) is not int or identity["schema_version"] != 1:
        raise ValueError("unsupported grep identity")
    if (
        identity["product"] != "antfly-grep"
        or version(identity["tag"]) != identity["version"]
    ):
        raise ValueError("grep version mismatch")
    for key in ("source_commit", "source_ref_head", "build_controller_commit"):
        if not isinstance(identity[key], str) or not SHA.fullmatch(identity[key]):
            raise ValueError(f"invalid {key}")
    for key in ("request_run_id", "build_run_id", "source_date_epoch"):
        if type(identity[key]) is not int or identity[key] < (
            0 if key == "source_date_epoch" else 1
        ):
            raise ValueError(f"invalid {key}")


def verify_source(repo: Path, identity: dict, default_branch: str) -> None:
    validate_identity(identity)
    git(repo, "fetch", "--no-tags", "origin", f"refs/heads/{default_branch}")
    current = git(repo, "rev-parse", "FETCH_HEAD")
    git(
        repo,
        "fetch",
        "--force",
        "origin",
        f"refs/tags/{identity['tag']}:refs/tags/{identity['tag']}",
    )
    if (
        git(repo, "rev-parse", f"refs/tags/{identity['tag']}^{{commit}}")
        != identity["source_commit"]
    ):
        raise ValueError("grep tag moved after the build request")
    for commit in (identity["source_commit"], identity["source_ref_head"]):
        git(repo, "merge-base", "--is-ancestor", commit, current)
    git(
        repo,
        "merge-base",
        "--is-ancestor",
        identity["source_commit"],
        identity["source_ref_head"],
    )
    for path in CONTROL_PATHS:
        if git(
            repo, "rev-parse", f"{identity['build_controller_commit']}:{path}"
        ) != git(repo, "rev-parse", f"{current}:{path}"):
            raise ValueError("grep release policy changed while this run was queued")


def resolve(
    repo: Path,
    request: dict,
    event: dict,
    repository: str,
    controller: str,
    build_run_id: int,
    default_branch: str,
) -> dict:
    trigger = event["workflow_run"]
    if (
        trigger.get("name") != "Grep release request"
        or trigger.get("path") != REQUEST_WORKFLOW
        or trigger.get("event") != "push"
        or trigger.get("conclusion") != "success"
        or trigger.get("head_repository", {}).get("full_name") != repository
        or event.get("repository", {}).get("full_name") != repository
    ):
        raise ValueError("untrusted grep release trigger")
    if (
        set(request) != {"schema_version", "tag", "source_commit"}
        or type(request["schema_version"]) is not int
        or request["schema_version"] != 1
    ):
        raise ValueError("invalid grep tag request")
    release_version = version(request["tag"])
    if request["source_commit"] != trigger.get("head_sha") or request[
        "tag"
    ] != trigger.get("head_branch"):
        raise ValueError("grep request differs from the tag event")
    git(repo, "fetch", "--no-tags", "origin", f"refs/heads/{default_branch}")
    identity = {
        "schema_version": 1,
        "product": "antfly-grep",
        "tag": request["tag"],
        "version": release_version,
        "source_commit": request["source_commit"],
        "source_ref_head": git(repo, "rev-parse", "FETCH_HEAD"),
        "build_controller_commit": controller,
        "request_run_id": trigger["id"],
        "build_run_id": build_run_id,
        "source_date_epoch": 0,
    }
    if git(repo, "rev-parse", "HEAD") != controller:
        raise ValueError("controller checkout mismatch")
    # Validate the SHA before interpolating it into a revision expression.
    validate_identity(identity)
    verify_source(repo, identity, default_branch)
    identity["source_date_epoch"] = int(
        git(repo, "show", "-s", "--format=%ct", identity["source_commit"])
    )
    return identity


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_archive(path: Path, identity: dict, target: str) -> None:
    if path.stat().st_size > 256 * 1024 * 1024:
        raise ValueError("grep archive exceeds size limit")
    members = {}
    total = 0
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            if len(members) >= 10000:
                raise ValueError("grep archive contains too many members")
            name = member.name.removeprefix("./")
            if not name or name == ".":
                if not member.isdir():
                    raise ValueError("invalid grep archive root")
                continue
            parts = PurePosixPath(name)
            if (
                parts.is_absolute()
                or ".." in parts.parts
                or "\\" in name
                or parts.as_posix() != name
                or not (member.isdir() or member.isfile())
                or name in members
            ):
                raise ValueError("unsafe or duplicate grep archive path")
            total += member.size
            if total > 512 * 1024 * 1024:
                raise ValueError("grep archive expands beyond size limit")
            if parts.parts[0] not in {
                "antfly-grep",
                "install.sh",
                "README.md",
                "LICENSE",
                "LICENSES",
                "THIRD_PARTY_NOTICES.md",
                "completions",
                "share",
            }:
                raise ValueError("non-grep content in standalone archive")
            members[name] = member
        required = (
            "antfly-grep",
            "install.sh",
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            "share/antfly-grep/build.json",
            "share/antfly-grep/schema/tables-v1.json",
            "completions/antfly-grep.bash",
            "completions/antfly-grep.zsh",
            "completions/antfly-grep.fish",
            "share/man/man1/antfly-grep.1",
        )
        if any(name not in members or not members[name].isfile() for name in required):
            raise ValueError("incomplete grep archive")
        if any(
            not members[name].mode & 0o111 for name in ("antfly-grep", "install.sh")
        ):
            raise ValueError("grep binary/installer is not executable")
        metadata_member = members["share/antfly-grep/build.json"]
        if metadata_member.size > 1024 * 1024:
            raise ValueError("oversized grep build metadata")
        with archive.extractfile(metadata_member) as stream:
            metadata = json.load(stream)
        with archive.extractfile(members["antfly-grep"]) as stream:
            binary_hash = hashlib.file_digest(stream, "sha256").hexdigest()
        expected = {
            "schema_version": 1,
            "binary": "antfly-grep",
            "sha256": binary_hash,
            "version": identity["version"],
            "target": target,
            "source_commit": identity["source_commit"],
        }
        if metadata != expected:
            raise ValueError("grep archive identity or binary digest mismatch")


def smoke(path: Path, identity: dict, target: str, work: Path) -> None:
    verify_archive(path, identity, target)
    work.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=work) as temporary:
        root = Path(temporary)
        extracted = root / "extracted"
        with tarfile.open(path, "r:gz") as archive:
            archive.extractall(extracted, filter="data")
        prefix = root / "installed prefix"
        run(["sh", str(extracted / "install.sh"), "--prefix", str(prefix)], timeout=30)
        binary = str(prefix / "bin/antfly-grep")
        reported = run([binary, "--version"], timeout=30).splitlines()[0]
        if reported != f"antfly-grep {identity['version']}":
            raise ValueError("installed grep version mismatch")
        run([binary, "--help"], timeout=30)
        sample = root / "sample.txt"
        sample.write_text("first line\nrelease-smoke\nlast line\n")
        output = run(
            [
                binary,
                "--no-config",
                "--color=never",
                "-n",
                "-F",
                "release-smoke",
                str(sample),
            ],
            timeout=30,
        )
        if "2:release-smoke" not in output:
            raise ValueError("installed grep search failed")
        missing = subprocess.run(
            [binary, "--no-config", "-F", "missing-smoke", str(sample)],
            capture_output=True,
            timeout=30,
        )
        if missing.returncode != 1:
            raise ValueError("installed grep miss did not return 1")
        for name in (
            "share/man/man1/antfly-grep.1",
            "share/antfly-grep/schema/tables-v1.json",
            "share/bash-completion/completions/antfly-grep",
            "share/zsh/site-functions/_antfly-grep",
            "share/fish/vendor_completions.d/antfly-grep.fish",
        ):
            if not (prefix / name).is_file():
                raise ValueError(f"missing installed grep asset: {name}")


def seal(directory: Path, identity: dict) -> dict:
    validate_identity(identity)
    expected = {filename(identity["version"], suffix) for _, _, _, suffix in PLATFORMS}
    observed = {path.name for path in directory.iterdir()}
    if observed != expected | {"grep-release-identity.json"}:
        raise ValueError("incomplete or unexpected grep platform payload")
    assets = []
    for _, _, target, suffix in PLATFORMS:
        path = directory / filename(identity["version"], suffix)
        verify_archive(path, identity, target)
        assets.append(
            {
                "name": path.name,
                "target": target,
                "sha256": digest(path),
                "size": path.stat().st_size,
            }
        )
    manifest = {**identity, "assets": assets}
    (directory / "grep-release.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    (directory / "grep-release-identity.json").unlink()
    names = sorted([*expected, "grep-release.json"])
    (directory / "SHA256SUMS").write_text(
        "".join(f"{digest(directory / name)}  {name}\n" for name in names)
    )
    return manifest


def verify_bundle(directory: Path) -> dict:
    manifest = read_json(directory / "grep-release.json")
    identity = {key: value for key, value in manifest.items() if key != "assets"}
    validate_identity(identity)
    expected_assets = [
        (filename(identity["version"], suffix), target)
        for _, _, target, suffix in PLATFORMS
    ]
    assets = manifest.get("assets")
    if (
        not isinstance(assets, list)
        or not all(isinstance(asset, dict) for asset in assets)
        or [(a.get("name"), a.get("target")) for a in assets] != expected_assets
    ):
        raise ValueError("grep release platform set mismatch")
    expected_files = {"grep-release.json", "SHA256SUMS"} | {
        name for name, _ in expected_assets
    }
    if {p.name for p in directory.iterdir()} != expected_files:
        raise ValueError("unexpected grep release assets")
    for asset in assets:
        path = directory / asset["name"]
        if (
            set(asset) != {"name", "target", "sha256", "size"}
            or digest(path) != asset["sha256"]
            or path.stat().st_size != asset["size"]
        ):
            raise ValueError("grep release checksum mismatch")
        verify_archive(path, identity, asset["target"])
    checksums = "".join(
        f"{digest(directory / name)}  {name}\n"
        for name in sorted(expected_files - {"SHA256SUMS"})
    )
    if (directory / "SHA256SUMS").read_text() != checksums:
        raise ValueError("grep release checksum ledger mismatch")
    return identity


def api(repository: str, path: str, method: str = "GET", body: dict | None = None):
    command = ["gh", "api", "--method", method, f"repos/{repository}/{path}"]
    if body is not None:
        command += ["--input", "-"]
    return json.loads(
        run(command, input=json.dumps(body) if body is not None else None)
    )


def publish(directory: Path, repository: str, identity: dict) -> None:
    pages = json.loads(
        run(
            [
                "gh",
                "api",
                "--paginate",
                "--slurp",
                f"repos/{repository}/releases?per_page=100",
            ]
        )
    )
    matches = [
        release
        for page in pages
        for release in page
        if release["tag_name"] == identity["tag"]
    ]
    if len(matches) > 1:
        raise ValueError("duplicate grep release identity")
    prerelease = "-" in identity["version"]
    if matches:
        release = matches[0]
        if (
            release["target_commitish"] != identity["source_commit"]
            or release["prerelease"] != prerelease
        ):
            raise ValueError("existing grep release source/version mismatch")
    else:
        release = api(
            repository,
            "releases",
            "POST",
            {
                "tag_name": identity["tag"],
                "target_commitish": identity["source_commit"],
                "name": f"Antfly Grep {identity['version']}",
                "draft": True,
                "prerelease": prerelease,
                "make_latest": "false",
                "body": (
                    f"Standalone antfly-grep {identity['version']}.\n\n"
                    f"Source: {identity['source_commit']}\n"
                    f"Build: https://github.com/{repository}/actions/runs/{identity['build_run_id']}\n\n"
                    "Verify SHA256SUMS, extract your platform archive, and run "
                    './install.sh --prefix "$HOME/.local". GNU Linux archives require glibc 2.28+; '
                    "portable musl archives do not support local dynamic inference. "
                    "Darwin ARM64 requires macOS 15+.\n\n"
                    "These archives are independent of Antfly server and libantfly releases."
                ),
            },
        )
    expected = {p.name: digest(p) for p in directory.iterdir()}

    def inspect(release):
        if (
            release["tag_name"] != identity["tag"]
            or release["target_commitish"] != identity["source_commit"]
            or release["prerelease"] != prerelease
        ):
            raise ValueError("existing grep release source/version mismatch")
        observed = {}
        for asset in release["assets"]:
            name = asset["name"]
            if (
                name in observed
                or name not in expected
                or asset.get("digest") != "sha256:" + expected[name]
            ):
                raise ValueError(
                    "existing grep release asset differs; never overwrite release bytes"
                )
            observed[name] = asset
        return observed

    present = inspect(release)
    missing = sorted(set(expected) - set(present))
    if missing and not release["draft"]:
        raise ValueError("published grep release is incomplete")
    if missing:
        run(
            [
                "gh",
                "release",
                "upload",
                identity["tag"],
                "--repo",
                repository,
                *[str(directory / name) for name in missing],
            ]
        )
    release = api(repository, f"releases/{release['id']}")
    if set(inspect(release)) != set(expected):
        raise ValueError("uploaded grep release is incomplete")
    if release["draft"]:
        api(
            repository,
            f"releases/{release['id']}",
            "PATCH",
            {
                "draft": False,
                "prerelease": prerelease,
                "make_latest": "false",
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("matrix")
    resolve_command = commands.add_parser("resolve")
    resolve_command.add_argument("--request", type=Path, required=True)
    resolve_command.add_argument("--identity", type=Path, required=True)
    resolve_command.add_argument("--github-output", type=Path, required=True)
    for name in ("seal", "publish"):
        command = commands.add_parser(name)
        command.add_argument("--directory", type=Path, required=True)
    smoke_command = commands.add_parser("smoke")
    smoke_command.add_argument("--archive", type=Path, required=True)
    smoke_command.add_argument("--version", required=True)
    smoke_command.add_argument("--target", required=True)
    smoke_command.add_argument("--commit", required=True)
    smoke_command.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    repo = Path.cwd()
    if args.command == "matrix":
        print(json.dumps(matrix()))
    elif args.command == "smoke":
        smoke(
            args.archive,
            {"version": args.version, "source_commit": args.commit},
            args.target,
            args.work,
        )
    elif args.command == "resolve":
        identity = resolve(
            repo,
            read_json(args.request),
            read_json(Path(os.environ["GITHUB_EVENT_PATH"])),
            os.environ["GITHUB_REPOSITORY"],
            os.environ["GITHUB_SHA"],
            int(os.environ["GITHUB_RUN_ID"]),
            os.environ["DEFAULT_BRANCH"],
        )
        args.identity.parent.mkdir(parents=True, exist_ok=True)
        args.identity.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")
        outputs = {**identity, "matrix": json.dumps(matrix())}
        with args.github_output.open("a") as output:
            for key, value in outputs.items():
                output.write(f"{key}={value}\n")
    else:
        directory = args.directory
        if args.command == "seal":
            identity = read_json(directory / "grep-release-identity.json")
        else:
            identity = verify_bundle(directory)
        if (
            identity["build_run_id"] != int(os.environ["GITHUB_RUN_ID"])
            or identity["build_controller_commit"] != os.environ["GITHUB_SHA"]
        ):
            raise ValueError("grep build/controller provenance mismatch")
        verify_source(repo, identity, os.environ["DEFAULT_BRANCH"])
        if args.command == "seal":
            seal(directory, identity)
        else:
            publish(directory, os.environ["GITHUB_REPOSITORY"], identity)


if __name__ == "__main__":
    main()
