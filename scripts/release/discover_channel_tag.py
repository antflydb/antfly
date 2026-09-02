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


def discover_npm_channel(tag: str, opener: OpenURL) -> str:
    observed = {
        package: discover_npm_tag(package, tag, opener) for package in NPM_PACKAGES
    }
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


def reconcile_observations(
    channel: str, observations: dict[str, str], policy: dict[str, Any]
) -> str:
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


def journal_current(stored: Any, channel: str, policy: dict[str, Any]) -> str | None:
    """Return None only when the journal object itself does not exist."""
    if stored.etag is None:
        return None
    document = stored.document
    if document.get("channel") not in {None, channel}:
        raise SystemExit(
            f"release channel journal belongs to {document.get('channel')}"
        )
    current = document.get("current")
    if current is None:
        return ""
    tag = current.get("tag") if isinstance(current, dict) else None
    if not isinstance(tag, str):
        raise SystemExit("release channel journal has malformed current identity")
    validate_observed_channel_tag(tag, channel, policy)
    return tag


def discover_bootstrap_channel(
    channel: str,
    policy: dict[str, Any],
    repository: str,
    token: str,
    object_reader: Reader,
    opener: OpenURL,
) -> str:
    channel_policy = policy["channels"][channel]
    observations: dict[str, str] = {}
    for source in channel_policy["bootstrap_sources"]:
        if source == "github-latest":
            observations[source] = discover_github_latest(repository, token, opener)
        elif source == "npm":
            observations[source] = discover_npm_channel(
                channel_policy["npm_tag"], opener
            )
        elif source == "object-storage":
            observations[source] = discover_object_alias(
                object_reader, channel_policy["object_alias"]
            )
        elif source == "homebrew":
            observations[source] = discover_homebrew_tag(opener)
        else:  # Policy validation makes this unreachable.
            raise AssertionError(source)
    return reconcile_observations(channel, observations, policy)


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
        current = journal_current(store.load(), args.channel, policy)
        if current is None:
            reader = S3Reader(args.endpoint, args.bucket, "auto")
            current = discover_bootstrap_channel(
                args.channel,
                policy,
                args.repository or os.environ.get("GITHUB_REPOSITORY", ""),
                os.environ.get("GH_TOKEN", ""),
                reader,
                urllib.request.urlopen,
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
