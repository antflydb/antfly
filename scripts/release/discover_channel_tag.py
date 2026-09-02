#!/usr/bin/env python3
"""Discover a channel alias while failing closed on registry errors."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from build_cli_snapshot import NPM_PACKAGES
from download_objectstorage import Reader, S3Reader
from registry.model import RegistryError
from registry.npm import dist_tag, version_integrity
from release_channel_state import S3ChannelStore
from release_channels import load_policy, validate_observed_channel_tag

OpenURL = Callable[..., Any]


def load_json(
    url: str, headers: dict[str, str], opener: OpenURL
) -> dict[str, Any] | None:
    request = urllib.request.Request(url, headers=headers)
    try:
        with opener(request, timeout=20) as response:
            document = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise SystemExit(
            f"channel discovery failed with HTTP {exc.code}: {url}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise SystemExit(f"channel discovery failed for {url}: {exc}") from exc
    if not isinstance(document, dict):
        raise SystemExit(f"channel discovery returned an invalid document: {url}")
    return document


def discover_github_latest(repository: str, token: str, opener: OpenURL) -> str:
    if not repository or not token:
        raise SystemExit("GitHub channel discovery requires a repository and token")
    document = load_json(
        f"https://api.github.com/repos/{repository}/releases/latest",
        {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "antfly-release-controller",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        opener,
    )
    if document is None:
        return ""
    tag = document.get("tag_name")
    if not isinstance(tag, str) or not tag:
        raise SystemExit("GitHub latest release has no tag_name")
    return tag


def discover_npm_tag(package: str, tag: str, opener: OpenURL) -> str:
    try:
        version = dist_tag(package, tag, opener)
    except RegistryError as exc:
        raise SystemExit(str(exc)) from exc
    return f"v{version}" if version else ""


def discover_npm_integrity(package: str, version: str, opener: OpenURL) -> str:
    try:
        return version_integrity(package, version, opener) or ""
    except RegistryError as exc:
        raise SystemExit(str(exc)) from exc


def discover_npm_projections(tag: str, opener: OpenURL) -> dict[str, str]:
    return {
        f"npm:{package}": discover_npm_tag(package, tag, opener)
        for package in NPM_PACKAGES
    }


def discover_npm_channel(tag: str, opener: OpenURL) -> str:
    observed = discover_npm_projections(tag, opener)
    present = {package: version for package, version in observed.items() if version}
    if not present:
        return ""
    if len(present) != len(observed):
        missing = sorted(set(observed) - set(present))
        raise SystemExit(
            f"npm channel {tag} is only partially initialized; missing {missing}"
        )
    versions = set(present.values())
    if len(versions) != 1:
        detail = ", ".join(
            f"{package}={version}" for package, version in observed.items()
        )
        raise SystemExit(f"npm channel {tag} disagrees across packages: {detail}")
    return versions.pop()


def discover_object_alias(reader: Reader, alias: str) -> str:
    read_optional = getattr(reader, "read_optional", None)
    if read_optional is None:
        try:
            payload = reader.read(f"antfly/{alias}/metadata.json")
        except FileNotFoundError:
            return ""
    else:
        payload = read_optional(f"antfly/{alias}/metadata.json")
    if payload is None:
        return ""
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit(
            f"object-storage channel {alias} has invalid metadata"
        ) from exc
    tag = document.get("tag") if isinstance(document, dict) else None
    if not isinstance(tag, str) or not tag:
        raise SystemExit(f"object-storage channel {alias} has no tag")
    return tag


def discover_homebrew_tag(opener: OpenURL) -> str:
    url = "https://raw.githubusercontent.com/antflydb/homebrew-taps/main/Formula/antfly.rb"
    request = urllib.request.Request(
        url, headers={"User-Agent": "antfly-release-controller"}
    )
    try:
        with opener(request, timeout=20) as response:
            formula = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return ""
        raise SystemExit(
            f"Homebrew channel discovery failed with HTTP {exc.code}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, UnicodeDecodeError) as exc:
        raise SystemExit(f"Homebrew channel discovery failed: {exc}") from exc
    tags = set(re.findall(r"releases\.antfly\.io/antfly/(v[^/\"']+)/", formula))
    if len(tags) != 1:
        raise SystemExit(
            f"Homebrew formula must reference exactly one release tag, found {sorted(tags)}"
        )
    return tags.pop()


def reconcile_bootstrap_observations(
    channel: str, observations: dict[str, str], policy: dict[str, Any]
) -> str:
    npm = {
        projection: tag
        for projection, tag in observations.items()
        if projection.startswith("npm:")
    }
    present_npm = {projection: tag for projection, tag in npm.items() if tag}
    if present_npm and len(present_npm) != len(npm):
        missing = sorted(set(npm) - set(present_npm))
        raise SystemExit(
            f"npm channel is only partially initialized; missing {missing}"
        )
    present = {source: tag for source, tag in observations.items() if tag}
    for tag in present.values():
        validate_observed_channel_tag(tag, channel, policy)
    tags = set(present.values())
    if len(tags) > 1:
        detail = ", ".join(f"{source}={tag}" for source, tag in sorted(present.items()))
        raise SystemExit(
            f"release channel {channel} bootstrap sources disagree: {detail}"
        )
    return tags.pop() if tags else ""


def journal_tags(
    stored: Any, channel: str, policy: dict[str, Any]
) -> tuple[str | None, str | None] | None:
    """Return None only when the journal object itself does not exist."""
    if stored.etag is None:
        return None
    document = stored.document
    if document.get("channel") not in {None, channel}:
        raise SystemExit(
            f"release channel journal belongs to {document.get('channel')}"
        )

    tags: list[str | None] = []
    for field in ("current", "pending"):
        identity = document.get(field)
        if identity is None:
            tags.append(None)
            continue
        tag = identity.get("tag") if isinstance(identity, dict) else None
        if not isinstance(tag, str):
            raise SystemExit(f"release channel journal has malformed {field} identity")
        validate_observed_channel_tag(tag, channel, policy)
        tags.append(tag)
    current, pending = tags
    if current is None and pending is None:
        raise SystemExit("release channel journal has no current or pending identity")
    return current, pending


def journal_current(stored: Any, channel: str, policy: dict[str, Any]) -> str | None:
    tags = journal_tags(stored, channel, policy)
    if tags is None:
        return None
    current, _pending = tags
    return current or ""


def reconcile_journal_observations(
    stored: Any,
    channel: str,
    observations: dict[str, str],
    policy: dict[str, Any],
) -> str:
    tags = journal_tags(stored, channel, policy)
    if tags is None:
        raise AssertionError("cannot reconcile projections without a journal")
    current, pending = tags
    allowed = {tag for tag in tags if tag is not None}
    for projection, observed in sorted(observations.items()):
        if not observed:
            # Missing aliases are repairable and cannot make a channel move
            # backward. Only a present, contradictory observation is unsafe.
            continue
        validate_observed_channel_tag(observed, channel, policy)
        if observed not in allowed:
            expected = ", ".join(sorted(allowed))
            raise SystemExit(
                f"release channel {channel} projection {projection} is {observed}; "
                f"expected one of {expected}"
            )
        if pending is None and observed != current:
            raise AssertionError("completed journal admitted a non-current projection")
    return current or ""


def discover_channel_observations(
    channel: str,
    policy: dict[str, Any],
    repository: str,
    token: str,
    object_reader: Reader,
    opener: OpenURL,
) -> dict[str, str]:
    channel_policy = policy["channels"][channel]
    observations: dict[str, str] = {}
    for source in channel_policy["bootstrap_sources"]:
        if source == "github-latest":
            observations[source] = discover_github_latest(repository, token, opener)
        elif source == "npm":
            observations.update(
                discover_npm_projections(channel_policy["npm_tag"], opener)
            )
        elif source == "object-storage":
            observations[source] = discover_object_alias(
                object_reader, channel_policy["object_alias"]
            )
        elif source == "homebrew":
            observations[source] = discover_homebrew_tag(opener)
        else:  # Policy validation makes this unreachable.
            raise AssertionError(source)
    return observations


def discover_bootstrap_channel(
    channel: str,
    policy: dict[str, Any],
    repository: str,
    token: str,
    object_reader: Reader,
    opener: OpenURL,
) -> str:
    return reconcile_bootstrap_observations(
        channel,
        discover_channel_observations(
            channel, policy, repository, token, object_reader, opener
        ),
        policy,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bootstrap", choices=("reconcile", "github-latest", "npm", "npm-version")
    )
    parser.add_argument("--channel", choices=("stable", "next", "nightly"))
    parser.add_argument("--repository")
    parser.add_argument("--npm-package", default="@antfly/cli")
    parser.add_argument("--npm-tag")
    parser.add_argument("--npm-version")
    parser.add_argument("--endpoint")
    parser.add_argument("--bucket", default="antfly-releases")
    args = parser.parse_args()

    if args.bootstrap == "reconcile":
        if not args.channel:
            parser.error("reconcile requires --channel")
        policy = load_policy()
        channel_policy = policy["channels"][args.channel]
        store = S3ChannelStore(
            args.endpoint, args.bucket, channel_policy["journal_key"]
        )
        stored = store.load()
        reader = S3Reader(args.endpoint, args.bucket, "auto")
        observations = discover_channel_observations(
            args.channel,
            policy,
            args.repository or os.environ.get("GITHUB_REPOSITORY", ""),
            os.environ.get("GH_TOKEN", ""),
            reader,
            urllib.request.urlopen,
        )
        if stored.etag is None:
            current = reconcile_bootstrap_observations(
                args.channel, observations, policy
            )
        else:
            current = reconcile_journal_observations(
                stored, args.channel, observations, policy
            )
    elif args.bootstrap == "github-latest":
        current = discover_github_latest(
            args.repository or os.environ.get("GITHUB_REPOSITORY", ""),
            os.environ.get("GH_TOKEN", ""),
            urllib.request.urlopen,
        )
    elif args.bootstrap == "npm":
        current = discover_npm_tag(
            args.npm_package, args.npm_tag or "", urllib.request.urlopen
        )
    else:
        current = discover_npm_integrity(
            args.npm_package, args.npm_version or "", urllib.request.urlopen
        )
    print(current)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
