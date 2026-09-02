#!/usr/bin/env python3
"""Discover a channel alias while failing closed on registry errors."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

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
    if not package or not tag:
        raise SystemExit("npm channel discovery requires a package and dist-tag")
    encoded_package = urllib.parse.quote(package, safe="")
    document = load_json(
        f"https://registry.npmjs.org/{encoded_package}",
        {"Accept": "application/json", "User-Agent": "antfly-release-controller"},
        opener,
    )
    if document is None:
        return ""
    dist_tags = document.get("dist-tags")
    if not isinstance(dist_tags, dict):
        raise SystemExit("npm package metadata has no dist-tags object")
    version = dist_tags.get(tag)
    if version is None:
        return ""
    if not isinstance(version, str) or not version:
        raise SystemExit(f"npm dist-tag {tag} has an invalid version")
    return f"v{version}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bootstrap", choices=("github-latest", "npm"))
    parser.add_argument("--repository")
    parser.add_argument("--npm-package", default="@antfly/cli")
    parser.add_argument("--npm-tag")
    args = parser.parse_args()

    if args.bootstrap == "github-latest":
        current = discover_github_latest(
            args.repository or os.environ.get("GITHUB_REPOSITORY", ""),
            os.environ.get("GH_TOKEN", ""),
            urllib.request.urlopen,
        )
    else:
        current = discover_npm_tag(
            args.npm_package, args.npm_tag or "", urllib.request.urlopen
        )
    print(current)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
