#!/usr/bin/env python3
"""Recover and verify an immutable release payload from redundant mirrors."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from create_github_release import (
    GitHubError,
    download_bytes,
    get_release_by_tag,
    paginated_github_api,
)
from download_objectstorage import S3Reader, payload_names
from release_channels import load_policy
from verify_release_ledger import verify_payload


class PayloadReader(Protocol):
    def read(self, name: str) -> bytes: ...

    def list_names(self) -> set[str]: ...


class PrefixedReader:
    def __init__(self, reader: S3Reader, prefix: str) -> None:
        self.reader = reader
        self.prefix = prefix.strip("/")

    def read(self, name: str) -> bytes:
        return self.reader.read(f"{self.prefix}/{name}")

    def list_names(self) -> set[str]:
        return self.reader.list_names(self.prefix)


class GitHubReleaseReader:
    def __init__(self, repo: str, tag: str, token: str) -> None:
        release = get_release_by_tag(repo, tag, token)
        if release is None:
            raise GitHubError(f"GitHub release does not exist: {tag}")
        release_id = release.get("id")
        if not isinstance(release_id, int):
            raise GitHubError(f"GitHub release has no numeric id: {tag}")
        assets = paginated_github_api(repo, f"/releases/{release_id}/assets", token)
        self.assets: dict[str, str] = {}
        for asset in assets:
            name, url = asset.get("name"), asset.get("url")
            if not isinstance(name, str) or not isinstance(url, str):
                raise GitHubError(f"GitHub release has malformed assets: {tag}")
            if name in self.assets:
                raise GitHubError(f"GitHub release has duplicate asset: {name}")
            self.assets[name] = url
        self.token = token

    def read(self, name: str) -> bytes:
        url = self.assets.get(name)
        if url is None:
            raise GitHubError(f"GitHub release asset is missing: {name}")
        return download_bytes(url, self.token)

    def list_names(self) -> set[str]:
        return set(self.assets)


def restore_verified_payload(
    reader: PayloadReader,
    out_dir: Path,
    tag: str,
    commit: str,
    ledger_sha256: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    ledger_bytes = reader.read("artifacts.json")
    names = payload_names(ledger_bytes)
    expected_names = {"artifacts.json", *names}
    actual_names = reader.list_names()
    if actual_names != expected_names:
        raise SystemExit(
            "release mirror member set differs: "
            f"expected {sorted(expected_names)}, got {sorted(actual_names)}"
        )
    (out_dir / "artifacts.json").write_bytes(ledger_bytes)
    for name in names:
        (out_dir / name).write_bytes(reader.read(name))
    verify_payload(out_dir / "artifacts.json", out_dir, tag, commit, ledger_sha256)


def recover_payload(
    readers: list[tuple[str, Callable[[], PayloadReader]]],
    out_dir: Path,
    tag: str,
    commit: str,
    ledger_sha256: str,
) -> str:
    if out_dir.exists() and any(out_dir.iterdir()):
        raise SystemExit(f"release payload directory must be empty: {out_dir}")
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    for name, reader_factory in readers:
        attempt = Path(tempfile.mkdtemp(prefix=f"recover-{name}-", dir=out_dir.parent))
        try:
            reader = reader_factory()
            restore_verified_payload(reader, attempt, tag, commit, ledger_sha256)
        # Providers deliberately expose different network/not-found exception
        # types. Any failed or unverifiable mirror must fall through to the
        # next independently verified source.
        except (Exception, SystemExit) as exc:  # noqa: BLE001
            failures.append(f"{name}: {exc}")
            print(f"warning: recovery source {name} rejected: {exc}")
            shutil.rmtree(attempt)
            continue
        if out_dir.exists():
            out_dir.rmdir()
        attempt.replace(out_dir)
        print(f"recovered verified release payload from {name}")
        return name
    raise SystemExit(
        "no recovery source contained the verified payload:\n" + "\n".join(failures)
    )


def write_output(path: Path | None, source: str) -> None:
    if path is not None:
        with path.open("a", encoding="utf-8") as output:
            output.write(f"source={source}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--channel", required=True, choices=("stable", "next", "nightly")
    )
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--ledger-sha256", required=True)
    parser.add_argument("--endpoint")
    parser.add_argument("--bucket", default="antfly-releases")
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    policy = load_policy()["channels"][args.channel]
    readers: list[tuple[str, Callable[[], PayloadReader]]] = []
    for source in policy["recovery_sources"]:
        if source == "github-release":
            token = os.environ.get("GITHUB_TOKEN", "")
            if not args.repo or not token:
                raise SystemExit("GitHub recovery requires --repo and GITHUB_TOKEN")
            readers.append(
                (
                    source,
                    lambda repo=args.repo, tag=args.tag, token=token: GitHubReleaseReader(
                        repo, tag, token
                    ),
                )
            )
        elif source == "object-storage":
            reader = S3Reader(args.endpoint, args.bucket, "auto")
            readers.append(
                (
                    source,
                    lambda reader=reader, tag=args.tag: PrefixedReader(
                        reader, f"antfly/{tag}"
                    ),
                )
            )
        else:  # Policy validation makes this unreachable.
            raise AssertionError(source)

    source = recover_payload(
        readers, args.out_dir, args.tag, args.commit, args.ledger_sha256
    )
    write_output(
        args.github_output
        or (
            Path(os.environ["GITHUB_OUTPUT"])
            if os.environ.get("GITHUB_OUTPUT")
            else None
        ),
        source,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
