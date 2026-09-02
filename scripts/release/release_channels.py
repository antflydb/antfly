#!/usr/bin/env python3
"""Validate and resolve the canonical release-channel policy."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

POLICY_PATH = Path(__file__).with_name("channels.json")
CHANNEL_NAMES = {"stable", "next", "nightly"}
REQUIRED_FIELDS = {
    "tag_kind",
    "ordering",
    "journal_key",
    "bootstrap",
    "npm_tag",
    "publish_pypi",
    "publish_homebrew",
    "container_alias",
    "object_alias",
    "github_release",
    "recovery_source",
}
TAG_PATTERN = re.compile(
    r"^v(?P<major>0|[1-9][0-9]*)\."
    r"(?P<minor>0|[1-9][0-9]*)"
    r"(?:\.(?P<patch>0|[1-9][0-9]*))?"
    r"(?:-(?P<prerelease>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+(?P<build>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
NIGHTLY_PATTERN = re.compile(r"^v0\.0\.0-dev\.(?P<sequence>[1-9][0-9]*)$")


def parse_version(tag: str) -> tuple[tuple[int, int, int], tuple[str, ...] | None]:
    match = TAG_PATTERN.fullmatch(tag)
    if not match:
        raise SystemExit(f"release channel requires a semantic version tag: {tag}")
    prerelease = match.group("prerelease")
    identifiers = tuple(prerelease.split(".")) if prerelease else None
    if identifiers and any(
        identifier.isdigit() and len(identifier) > 1 and identifier.startswith("0")
        for identifier in identifiers
    ):
        raise SystemExit(f"invalid semantic version prerelease: {tag}")
    return (
        (
            int(match.group("major")),
            int(match.group("minor")),
            int(match.group("patch") or 0),
        ),
        identifiers,
    )


def compare_version_precedence(left: str, right: str) -> int:
    left_core, left_pre = parse_version(left)
    right_core, right_pre = parse_version(right)
    if left_core != right_core:
        return -1 if left_core < right_core else 1
    if left_pre is None or right_pre is None:
        if left_pre is right_pre:
            return 0
        return 1 if left_pre is None else -1
    for left_id, right_id in zip(left_pre, right_pre):
        if left_id == right_id:
            continue
        left_numeric = left_id.isdigit()
        right_numeric = right_id.isdigit()
        if left_numeric and right_numeric:
            return -1 if int(left_id) < int(right_id) else 1
        if left_numeric != right_numeric:
            return -1 if left_numeric else 1
        return -1 if left_id < right_id else 1
    if len(left_pre) == len(right_pre):
        return 0
    return -1 if len(left_pre) < len(right_pre) else 1


def load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    policy = json.loads(path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != 1:
        raise SystemExit(f"unsupported release-channel schema in {path}")
    channels = policy.get("channels")
    if not isinstance(channels, dict) or set(channels) != CHANNEL_NAMES:
        raise SystemExit("release-channel policy must define stable, next, and nightly")

    destinations: set[tuple[str, str]] = set()
    journal_keys: set[str] = set()
    for name, channel in channels.items():
        if not isinstance(channel, dict) or REQUIRED_FIELDS - channel.keys():
            raise SystemExit(f"release channel {name} is missing required fields")
        if channel["tag_kind"] not in {"stable", "prerelease", "nightly"}:
            raise SystemExit(f"release channel {name} has invalid tag_kind")
        if channel["ordering"] not in {"semver", "sequence"}:
            raise SystemExit(f"release channel {name} has invalid ordering")
        if channel["bootstrap"] not in {"github-latest", "npm"}:
            raise SystemExit(f"release channel {name} has invalid bootstrap")
        if channel["github_release"] not in {"latest", "prerelease", "none"}:
            raise SystemExit(f"release channel {name} has invalid GitHub mode")
        if channel["recovery_source"] not in {"github-release", "object-storage"}:
            raise SystemExit(f"release channel {name} has invalid recovery source")
        journal_key = channel["journal_key"]
        if (
            not isinstance(journal_key, str)
            or not re.fullmatch(r"antfly/channels/[a-z][a-z0-9-]*\.json", journal_key)
            or journal_key in journal_keys
        ):
            raise SystemExit(
                f"release channel {name} has invalid or duplicate journal_key"
            )
        journal_keys.add(journal_key)
        for flag in ("publish_pypi", "publish_homebrew"):
            if not isinstance(channel[flag], bool):
                raise SystemExit(f"release channel {name} has invalid {flag}")
        for sink, field in (
            ("npm", "npm_tag"),
            ("container", "container_alias"),
            ("object-storage", "object_alias"),
        ):
            alias = channel[field]
            if not isinstance(alias, str) or not re.fullmatch(
                r"[a-z][a-z0-9-]*", alias
            ):
                raise SystemExit(f"release channel {name} has invalid {field}")
            destination = (sink, alias)
            if destination in destinations:
                raise SystemExit(f"mutable destination is owned twice: {sink}:{alias}")
            destinations.add(destination)

    if {
        name: (channel["tag_kind"], channel["ordering"])
        for name, channel in channels.items()
    } != {
        "stable": ("stable", "semver"),
        "next": ("prerelease", "semver"),
        "nightly": ("nightly", "sequence"),
    }:
        raise SystemExit("release channels have invalid tag or ordering contracts")

    nightly = channels["nightly"]
    if (
        nightly["ordering"] != "sequence"
        or nightly["publish_pypi"]
        or nightly["publish_homebrew"]
        or nightly["github_release"] != "none"
        or nightly["recovery_source"] != "object-storage"
    ):
        raise SystemExit("nightly channel must be snapshot-only")
    return policy


def validate_channel_tag(tag: str, channel_name: str, policy: dict[str, Any]) -> None:
    channel = policy["channels"].get(channel_name)
    if not isinstance(channel, dict):
        raise SystemExit(f"unknown release channel: {channel_name}")
    _, prerelease = parse_version(tag)
    kind = channel["tag_kind"]
    if kind == "stable" and prerelease is not None:
        raise SystemExit(
            f"stable channel requires a stable semantic version tag: {tag}"
        )
    if kind == "prerelease" and prerelease is None:
        raise SystemExit(
            f"next channel requires a prerelease semantic version tag: {tag}"
        )
    if kind == "nightly" and not NIGHTLY_PATTERN.fullmatch(tag):
        raise SystemExit(f"nightly channel requires v0.0.0-dev.<sequence>: {tag}")
    if kind != "nightly" and NIGHTLY_PATTERN.fullmatch(tag):
        raise SystemExit(f"nightly snapshot tag requires the nightly channel: {tag}")


def resolve_channel(
    tag: str, requested: str = "auto", policy: dict[str, Any] | None = None
) -> tuple[str, dict[str, Any]]:
    policy = policy or load_policy()
    _, prerelease = parse_version(tag)
    if requested == "auto":
        requested = "next" if prerelease is not None else "stable"
    validate_channel_tag(tag, requested, policy)
    return requested, dict(policy["channels"][requested])


def compare_channel_tags(
    left: str, right: str, channel_name: str, policy: dict[str, Any]
) -> int:
    validate_channel_tag(left, channel_name, policy)
    validate_channel_tag(right, channel_name, policy)
    channel = policy["channels"][channel_name]
    if channel["ordering"] == "semver":
        return compare_version_precedence(left, right)
    left_match = NIGHTLY_PATTERN.fullmatch(left)
    right_match = NIGHTLY_PATTERN.fullmatch(right)
    assert left_match is not None and right_match is not None
    left_sequence = int(left_match.group("sequence"))
    right_sequence = int(right_match.group("sequence"))
    return (left_sequence > right_sequence) - (left_sequence < right_sequence)


def github_outputs(channel_name: str, channel: dict[str, Any]) -> dict[str, str]:
    result = {"channel": channel_name}
    for key, value in channel.items():
        if isinstance(value, bool):
            result[key] = str(value).lower()
        else:
            result[key] = str(value)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "resolve"))
    parser.add_argument("--tag")
    parser.add_argument("--channel", default="auto")
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    policy = load_policy()
    if args.command == "validate":
        print(f"validated {len(policy['channels'])} release channels")
        return 0
    if not args.tag:
        parser.error("resolve requires --tag")
    channel_name, channel = resolve_channel(args.tag, args.channel, policy)
    outputs = github_outputs(channel_name, channel)
    output_path = args.github_output
    if output_path is None and os.environ.get("GITHUB_OUTPUT"):
        output_path = Path(os.environ["GITHUB_OUTPUT"])
    if output_path:
        with output_path.open("a", encoding="utf-8") as output:
            for key, value in outputs.items():
                output.write(f"{key}={value}\n")
    print(json.dumps(outputs, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
