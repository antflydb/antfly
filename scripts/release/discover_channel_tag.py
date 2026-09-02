#!/usr/bin/env python3
"""Discover a channel alias while failing closed on registry errors."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from registry.model import RegistryError
from registry.npm import dist_tag, version_integrity

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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bootstrap", choices=("github-latest", "npm", "npm-version"))
    parser.add_argument("--repository")
    parser.add_argument("--npm-package", default="@antfly/cli")
    parser.add_argument("--npm-tag")
    parser.add_argument("--npm-version")
    args = parser.parse_args()

    if args.bootstrap == "github-latest":
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
