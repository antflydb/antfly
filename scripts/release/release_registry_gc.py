#!/usr/bin/env python3
"""Delete OCI images selected by a verified release-GC plan."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from registry.container import optional_digest
from registry.model import RegistryError

Runner = Callable[..., subprocess.CompletedProcess[str]]
SHA256 = re.compile(r"sha256:[0-9a-f]{64}")
MISSING = ("not found", "not_found", "404", "manifest unknown", "name unknown")
CHANNEL_TAGS = {"latest", "next", "nightly"}


def run(args: Sequence[str], runner: Runner) -> subprocess.CompletedProcess[str]:
    return runner(args, capture_output=True, text=True, check=False)


def load_deletions(path: Path) -> list[dict[str, str]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise SystemExit("unsupported release-GC plan")
    raw = document.get("container_deletions")
    if not isinstance(raw, list):
        raise SystemExit("release-GC plan has no container deletion set")
    deletions: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {
            "tag",
            "ledger_sha256",
            "container_digest",
            "record_key",
        }:
            raise SystemExit("release-GC plan has a malformed container deletion")
        values = {key: str(value) for key, value in item.items()}
        if (
            not re.fullmatch(r"v[0-9A-Za-z.-]+", values["tag"])
            or not re.fullmatch(r"[0-9a-f]{64}", values["ledger_sha256"])
            or not SHA256.fullmatch(values["container_digest"])
            or values["record_key"]
            != f"antfly/container-identities/{values['ledger_sha256']}.json"
        ):
            raise SystemExit("release-GC plan has a malformed container deletion")
        deletions.append(values)
    return deletions


def grouped_tags(deletions: list[dict[str, str]]) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for item in deletions:
        result.setdefault(item["container_digest"], set()).update(
            {item["tag"], f"release-ledger-{item['ledger_sha256']}"}
        )
    return result


def lookup(ref: str, runner: Runner) -> str | None:
    try:
        return optional_digest(ref, runner)
    except RegistryError as exc:
        raise SystemExit(str(exc)) from exc


def require_no_channel_reference(
    repository: str, deleting_digests: set[str], runner: Runner
) -> None:
    for tag in sorted(CHANNEL_TAGS):
        digest = lookup(f"{repository}:{tag}", runner)
        if digest in deleting_digests:
            raise SystemExit(
                f"refusing to delete container digest still selected by {repository}:{tag}"
            )


def resolve_expected_tags(
    repository: str,
    tags_by_digest: dict[str, set[str]],
    runner: Runner,
) -> None:
    for expected, tags in tags_by_digest.items():
        for tag in sorted(tags):
            observed = lookup(f"{repository}:{tag}", runner)
            if observed is not None and observed != expected:
                raise SystemExit(
                    f"container GC tag drift: {repository}:{tag}={observed}, expected {expected}"
                )


def repository_tags(repository: str, runner: Runner) -> set[str]:
    result = run(("crane", "ls", repository), runner)
    if result.returncode:
        raise SystemExit(
            f"container tag listing failed for {repository}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    tags = set(result.stdout.splitlines())
    if any(
        not tag or re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag) is None
        for tag in tags
    ):
        raise SystemExit(f"container registry returned an invalid tag for {repository}")
    return tags


def plan_gar_deletions(
    repository: str,
    deletions: list[dict[str, str]],
    tags_by_digest: dict[str, set[str]],
    runner: Runner,
) -> set[str]:
    allowed_tags_by_digest = {
        digest: set(tags) for digest, tags in tags_by_digest.items()
    }
    for item in deletions:
        for suffix in ("-amd64", "-arm64"):
            tag = f"{item['tag']}{suffix}"
            ref = f"{repository}:{tag}"
            if digest := lookup(ref, runner):
                allowed_tags_by_digest.setdefault(digest, set()).add(tag)

    deleting_digests = set(allowed_tags_by_digest)
    for tag in sorted(repository_tags(repository, runner)):
        digest = lookup(f"{repository}:{tag}", runner)
        if digest in deleting_digests and tag not in allowed_tags_by_digest[digest]:
            if tag in CHANNEL_TAGS:
                raise SystemExit(
                    f"refusing to delete container digest still selected by {repository}:{tag}"
                )
            raise SystemExit(
                f"refusing to delete GAR digest {digest} with unexpired tag: {tag}"
            )
    return deleting_digests


def delete_gar_images(
    repository: str, deleting_digests: set[str], runner: Runner
) -> None:
    for digest in sorted(deleting_digests):
        if lookup(f"{repository}@{digest}", runner) is None:
            continue
        result = run(
            (
                "gcloud",
                "artifacts",
                "docker",
                "images",
                "delete",
                f"{repository}@{digest}",
                "--delete-tags",
                "--quiet",
            ),
            runner,
        )
        if result.returncode:
            detail = f"{result.stdout}\n{result.stderr}".lower()
            if any(marker in detail for marker in MISSING):
                continue
            raise SystemExit(
                f"GAR container deletion failed for {repository}@{digest}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )


def ghcr_versions(owner: str, package: str, runner: Runner) -> list[dict[str, Any]]:
    endpoint = f"/orgs/{owner}/packages/container/{package}/versions?per_page=100"
    result = run(("gh", "api", "--paginate", "--slurp", endpoint), runner)
    if result.returncode:
        raise SystemExit(
            "GHCR package listing failed: "
            + (result.stderr.strip() or result.stdout.strip())
        )
    try:
        pages = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit("GHCR package listing returned invalid JSON") from exc
    if not isinstance(pages, list) or any(not isinstance(page, list) for page in pages):
        raise SystemExit("GHCR package listing returned an invalid document")
    return [version for page in pages for version in page]


def version_tags(version: dict[str, Any]) -> set[str]:
    metadata = version.get("metadata")
    container = metadata.get("container") if isinstance(metadata, dict) else None
    tags = container.get("tags") if isinstance(container, dict) else None
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise SystemExit("GHCR package version has malformed tags")
    return set(tags)


def plan_ghcr_deletions(
    owner: str,
    package: str,
    tags_by_digest: dict[str, set[str]],
    deleting_digests: set[str],
    runner: Runner,
) -> list[tuple[str, int]]:
    versions = ghcr_versions(owner, package, runner)
    targets: list[tuple[str, int]] = []
    for digest in deleting_digests:
        allowed_tags = tags_by_digest.get(digest, set())
        matching = [
            version
            for version in versions
            if version.get("name") == digest
            or bool(version_tags(version) & allowed_tags)
        ]
        if not matching:
            continue
        if len(matching) != 1:
            raise SystemExit(f"GHCR has multiple package versions for {digest}")
        version = matching[0]
        tags = version_tags(version)
        unexpected = tags - allowed_tags
        if unexpected:
            raise SystemExit(
                f"refusing to delete GHCR digest {digest} with unexpired tags: {sorted(unexpected)}"
            )
        version_id = version.get("id")
        if not isinstance(version_id, int):
            raise SystemExit("GHCR package version has no numeric id")
        targets.append((digest, version_id))
    return targets


def delete_ghcr_images(
    owner: str,
    package: str,
    targets: list[tuple[str, int]],
    runner: Runner,
) -> None:
    for digest, version_id in targets:
        endpoint = f"/orgs/{owner}/packages/container/{package}/versions/{version_id}"
        result = run(("gh", "api", "--method", "DELETE", endpoint), runner)
        if result.returncode:
            detail = f"{result.stdout}\n{result.stderr}".lower()
            if any(marker in detail for marker in MISSING):
                continue
            raise SystemExit(
                f"GHCR container deletion failed for {digest}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )


def apply_registry_gc(
    deletions: list[dict[str, str]],
    gar_image: str,
    ghcr_image: str,
    runner: Runner = subprocess.run,
) -> None:
    tags_by_digest = grouped_tags(deletions)
    if not tags_by_digest:
        return
    if "/" not in ghcr_image:
        raise SystemExit("GHCR image must be OWNER/PACKAGE")
    owner, package = ghcr_image.split("/", 1)
    ghcr_repository = f"ghcr.io/{ghcr_image}"
    for repository in (gar_image, ghcr_repository):
        resolve_expected_tags(repository, tags_by_digest, runner)
    gar_targets = plan_gar_deletions(gar_image, deletions, tags_by_digest, runner)
    for repository in (gar_image, ghcr_repository):
        require_no_channel_reference(repository, gar_targets, runner)
    ghcr_targets = plan_ghcr_deletions(
        owner, package, tags_by_digest, gar_targets, runner
    )
    delete_ghcr_images(owner, package, ghcr_targets, runner)
    delete_gar_images(gar_image, gar_targets, runner)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--gar-image", required=True)
    parser.add_argument("--ghcr-image", default="antflydb/antfly")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    deletions = load_deletions(args.plan)
    if not args.apply:
        print(json.dumps(deletions, indent=2, sort_keys=True))
        print(f"dry run: {len(deletions)} container release(s) would be deleted")
        return 0
    apply_registry_gc(deletions, args.gar_image, args.ghcr_image)
    print(f"deleted {len(deletions)} expired container release(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
