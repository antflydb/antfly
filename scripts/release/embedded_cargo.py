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

"""Build and publish source-bound embedded crates, sys first, with safe retries."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import time
import tarfile
import tomllib
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath

from release_channels import normalize_release_version

CRATES = {"antfly-embedded-sys": "embedded-sys", "antfly-embedded": "embedded"}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(*arguments: str, cwd: Path, target: Path) -> None:
    subprocess.run(
        arguments,
        cwd=cwd,
        env={**os.environ, "CARGO_TARGET_DIR": str(target)},
        check=True,
    )


def stamp_manifest(text: str, name: str, version: str, commit: str) -> str:
    document = tomllib.loads(text)
    if (
        document["package"]["name"] != name
        or document["package"]["license"] != "Apache-2.0"
    ):
        raise ValueError(f"unexpected crate identity: {name}")
    text, count = re.subn(
        r'(?m)^version = "[^"]+"$', f'version = "{version}"', text, count=1
    )
    if count != 1:
        raise ValueError(f"cannot stamp crate version: {name}")
    if name == "antfly-embedded":
        text, count = re.subn(
            r'(?m)^(antfly-embedded-sys = \{[^\n]*version = ")[^"]+("[^\n]*\})$',
            lambda match: match[1] + "=" + version + match[2],
            text,
        )
        if count != 1:
            raise ValueError("cannot bind embedded to its exact sys crate")
    return text + f'\n[package.metadata.antfly-release]\nsource-commit = "{commit}"\n'


def prepare(repo: Path, commit: str, version: str, stage: Path) -> None:
    version = normalize_release_version(version)
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("source commit must be a full SHA-1")
    subprocess.run(
        ["git", "worktree", "add", "--detach", str(stage), commit], cwd=repo, check=True
    )
    for name, directory in CRATES.items():
        manifest = stage / "rs/crates" / directory / "Cargo.toml"
        manifest.write_text(stamp_manifest(manifest.read_text(), name, version, commit))
    lock = stage / "rs/Cargo.lock"
    text = lock.read_text()
    for name in CRATES:
        text, count = re.subn(
            rf'(\[\[package\]\]\nname = "{re.escape(name)}"\nversion = ")[^"]+("\n)',
            lambda match: match[1] + version + match[2],
            text,
        )
        if count != 1:
            raise ValueError(f"cannot stamp locked crate version: {name}")
    lock.write_text(text)
    # Source tests reference repository fixtures. Ship those fixtures with the
    # crates and relocate only their include paths; production Rust is unchanged.
    sys = stage / "rs/crates/embedded-sys"
    (sys / "include").mkdir(exist_ok=True)
    shutil.copy2(
        stage / "zig/pkg/antfly-embedded/include/antfly.h", sys / "include/antfly.h"
    )
    drift = sys / "tests/header_drift.rs"
    drift.write_text(
        drift.read_text().replace(
            '"/../../../zig/pkg/antfly-embedded/include/antfly.h"',
            '"/include/antfly.h"',
        )
    )
    binding = stage / "rs/crates/embedded"
    shutil.copytree(
        stage / "zig/pkg/antfly-embedded/capi-conformance/sql",
        binding / "tests/fixtures/sql",
    )
    sqlx = binding / "tests/sqlx.rs"
    sqlx.write_text(
        sqlx.read_text().replace(
            '"../../../../zig/pkg/antfly-embedded/capi-conformance/sql/',
            '"fixtures/sql/',
        )
    )


def package(stage: Path, target: Path) -> None:
    run(
        "cargo",
        "package",
        "--locked",
        "--allow-dirty",
        "-p",
        "antfly-embedded-sys",
        "-p",
        "antfly-embedded",
        cwd=stage / "rs",
        target=target,
    )


def crate_metadata(path: Path) -> dict:
    """Read Cargo's normalized manifest without extracting or running source."""
    with tarfile.open(path, "r:gz") as archive:
        manifests = [
            entry
            for entry in archive.getmembers()
            if entry.name.count("/") == 1 and entry.name.endswith("/Cargo.toml")
        ]
        if len(manifests) != 1 or not manifests[0].isfile():
            raise ValueError(f"expected one regular crate manifest: {path}")
        document = tomllib.loads(archive.extractfile(manifests[0]).read().decode())
        info = document["package"]
        if manifests[0].name != f"{info['name']}-{info['version']}/Cargo.toml":
            raise ValueError("crate archive root differs from its package identity")
        return document


def crate_identity(path: Path) -> tuple[str, str, str]:
    document = crate_metadata(path)
    info = document["package"]
    if info["license"] != "Apache-2.0":
        raise ValueError(f"non-Apache crate: {path}")
    if (
        info["name"] == "antfly-embedded"
        and document["dependencies"]["antfly-embedded-sys"]["version"]
        != "=" + info["version"]
    ):
        raise ValueError("embedded crate does not pin its sys version")
    return (
        info["name"],
        info["version"],
        info["metadata"]["antfly-release"]["source-commit"],
    )


def upload_metadata(path: Path) -> dict:
    """Translate the sealed normalized manifest to Cargo's registry API schema.

    https://doc.rust-lang.org/cargo/reference/registry-web-api.html#publish
    No Cargo/build.rs invocation is permitted in the credentialed publisher.
    """
    document = crate_metadata(path)
    info = document["package"]
    result = {
        "name": info["name"],
        "vers": info["version"],
        "deps": [],
        "features": document.get("features", {}),
        "authors": info.get("authors", []),
        "keywords": info.get("keywords", []),
        "categories": info.get("categories", []),
        "badges": document.get("badges", {}),
    }
    for field in (
        "description",
        "documentation",
        "homepage",
        "license",
        "license-file",
        "repository",
        "links",
        "rust-version",
    ):
        result[field.replace("-", "_")] = info.get(field)
    result["readme_file"] = info.get("readme") or None
    result["readme"] = None
    if result["readme_file"]:
        readme = PurePosixPath(result["readme_file"])
        if readme.is_absolute() or ".." in readme.parts:
            raise ValueError("crate README must be inside the archive")
        expected = f"{info['name']}-{info['version']}/{readme}"
        with tarfile.open(path, "r:gz") as archive:
            entries = [
                entry for entry in archive.getmembers() if entry.name == expected
            ]
            if len(entries) != 1 or not entries[0].isfile():
                raise ValueError("expected one regular crate README")
            result["readme"] = archive.extractfile(entries[0]).read().decode()
    tables = [(None, document), *document.get("target", {}).items()]
    for target, table in tables:
        for kind, key in (
            ("normal", "dependencies"),
            ("build", "build-dependencies"),
            ("dev", "dev-dependencies"),
        ):
            for alias, dependency in table.get(key, {}).items():
                if isinstance(dependency, str):
                    dependency = {"version": dependency}
                if set(dependency) - {
                    "version",
                    "package",
                    "features",
                    "optional",
                    "default-features",
                }:
                    raise ValueError(f"unsupported normalized dependency: {alias}")
                result["deps"].append(
                    {
                        "name": dependency.get("package", alias),
                        "version_req": dependency["version"],
                        "features": dependency.get("features", []),
                        "optional": dependency.get("optional", False),
                        "default_features": dependency.get("default-features", True),
                        "target": target,
                        "kind": kind,
                        "registry": None,
                        "explicit_name_in_toml": alias
                        if "package" in dependency
                        else None,
                    }
                )
    return result


def upload(path: Path, metadata: dict, token: str) -> None:
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode()
    archive_bytes = path.read_bytes()
    body = (
        struct.pack("<I", len(metadata_bytes))
        + metadata_bytes
        + struct.pack("<I", len(archive_bytes))
        + archive_bytes
    )
    request = urllib.request.Request(
        "https://crates.io/api/v1/crates/new",
        data=body,
        method="PUT",
        headers={
            "Authorization": token,
            "Content-Type": "application/octet-stream",
            "Accept": "application/json",
            "User-Agent": "antfly-release (https://github.com/antflydb/antfly)",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        document = json.load(response)
    if not isinstance(document, dict) or document.get("errors"):
        raise ValueError("crates.io rejected crate upload")


def publish(out: Path, version: str, commit: str, token: str) -> None:
    checksums = verify(out, version, commit)
    missing = preflight(checksums, version)
    # Validate BOTH request schemas before the first immutable upload.
    metadata = {
        name: upload_metadata(out / f"{name}-{version}.crate") for name in CRATES
    }
    if missing and not token:
        raise ValueError("missing short-lived crates.io token")
    for name in CRATES:
        if name not in missing:
            continue
        upload(out / f"{name}-{version}.crate", metadata[name], token)
        # Wait for the sys version to become publicly visible before bindings.
        for attempt in range(30):
            observed = registry_checksum(name, version)
            if observed == checksums[name]:
                break
            if observed is not None:
                raise ValueError(f"published crate checksum differs: {name}")
            if attempt == 29:
                raise ValueError(f"published crate did not become visible: {name}")
            time.sleep(2)


def seal(target: Path, out: Path, version: str, commit: str) -> None:
    if out.exists() and any(out.iterdir()):
        raise ValueError("Cargo snapshot directory must be empty")
    out.mkdir(parents=True, exist_ok=True)
    files = []
    for name in CRATES:
        source = target / "package" / f"{name}-{version}.crate"
        if crate_identity(source) != (name, version, commit):
            raise ValueError("Cargo package identity differs")
        upload_metadata(source)
        shutil.copy2(source, out / source.name)
        files.append(
            {
                "name": source.name,
                "sha256": digest(source),
                "size": source.stat().st_size,
            }
        )
    (out / "cargo-snapshot.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "version": version,
                "commit": commit,
                "artifacts": files,
            },
            indent=2,
        )
        + "\n"
    )


def verify(out: Path, version: str, commit: str) -> dict[str, str]:
    manifest = json.loads((out / "cargo-snapshot.json").read_text())
    names = {f"{name}-{version}.crate" for name in CRATES}
    if (
        manifest.get("schema_version"),
        manifest.get("version"),
        manifest.get("commit"),
    ) != (1, version, commit):
        raise ValueError("Cargo snapshot identity differs")
    artifacts = manifest["artifacts"]
    if (
        len(artifacts) != 2
        or {item["name"] for item in artifacts} != names
        or {path.name for path in out.iterdir()} != names | {"cargo-snapshot.json"}
    ):
        raise ValueError("Cargo snapshot file set differs")
    result = {}
    for item in artifacts:
        path = out / item["name"]
        if path.stat().st_size != item["size"] or digest(path) != item["sha256"]:
            raise ValueError(f"Cargo snapshot digest differs: {path.name}")
        name, actual_version, actual_commit = crate_identity(path)
        if (
            name not in CRATES
            or item["name"] != f"{name}-{version}.crate"
            or (actual_version, actual_commit) != (version, commit)
        ):
            raise ValueError("Cargo archive identity differs")
        result[name] = item["sha256"]
    return result


def registry_checksum(name: str, version: str) -> str | None:
    request = urllib.request.Request(
        f"https://crates.io/api/v1/crates/{name}/{version}",
        headers={"User-Agent": "antfly-release (https://github.com/antflydb/antfly)"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            document = json.load(response)["version"]
    except urllib.error.HTTPError as error:
        error.close()
        if error.code == 404:
            return None
        raise
    if (
        document.get("crate") != name
        or document.get("num") != version
        or document.get("yanked") is not False
        or not re.fullmatch(r"[0-9a-f]{64}", document.get("checksum", ""))
    ):
        raise ValueError(f"invalid crates.io identity: {name}@{version}")
    return document["checksum"]


def preflight(checksums: dict[str, str], version: str) -> list[str]:
    missing = []
    for name, expected in checksums.items():
        observed = registry_checksum(name, version)
        if observed is None:
            missing.append(name)
        elif observed != expected:
            raise ValueError(f"immutable crates.io version differs: {name}@{version}")
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "verify", "preflight", "publish"))
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--stage", type=Path)
    parser.add_argument("--target", type=Path)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    if args.command == "build":
        if args.stage is None or args.target is None:
            parser.error("build requires --stage and --target")
        args.stage = args.stage.resolve()
        args.target = args.target.resolve()
    version = normalize_release_version(args.version)
    if args.command == "build":
        prepare(args.repo.resolve(), args.commit, version, args.stage.resolve())
        package(args.stage.resolve(), args.target.resolve())
        seal(args.target.resolve(), args.snapshot, version, args.commit)
        return
    checksums = verify(args.snapshot, version, args.commit)
    if args.command == "verify":
        return
    missing = preflight(checksums, version)
    if args.github_output:
        with args.github_output.open("a") as output:
            output.write(f"has_packages={str(bool(missing)).lower()}\n")
    if args.command == "preflight" or not missing:
        return
    publish(
        args.snapshot, version, args.commit, os.environ.get("CARGO_REGISTRY_TOKEN", "")
    )


if __name__ == "__main__":
    main()
