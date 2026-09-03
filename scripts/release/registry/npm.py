from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from .model import RegistryError

OpenURL = Callable[..., Any]


def package_document(
    package: str, opener: OpenURL = urllib.request.urlopen
) -> dict[str, Any] | None:
    if not package:
        raise RegistryError("npm registry lookup requires a package")
    encoded = urllib.parse.quote(package, safe="")
    url = f"https://registry.npmjs.org/{encoded}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "antfly-release-controller",
        },
    )
    try:
        with opener(request, timeout=20) as response:
            document = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise RegistryError(f"npm registry returned HTTP {exc.code}: {url}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RegistryError(f"npm registry lookup failed for {url}: {exc}") from exc
    if not isinstance(document, dict):
        raise RegistryError(f"npm registry returned an invalid document: {url}")
    return document


def dist_tag(
    package: str, tag: str, opener: OpenURL = urllib.request.urlopen
) -> str | None:
    if not tag:
        raise RegistryError("npm dist-tag lookup requires a tag")
    document = package_document(package, opener)
    if document is None:
        return None
    tags = document.get("dist-tags")
    if not isinstance(tags, dict):
        raise RegistryError("npm package metadata has no dist-tags object")
    version = tags.get(tag)
    if version is None:
        return None
    if not isinstance(version, str) or not version:
        raise RegistryError(f"npm dist-tag {tag} has an invalid version")
    return version


def version_integrity(
    package: str, version: str, opener: OpenURL = urllib.request.urlopen
) -> str | None:
    if not version:
        raise RegistryError("npm version lookup requires a version")
    document = package_document(package, opener)
    if document is None:
        return None
    versions = document.get("versions")
    if not isinstance(versions, dict):
        raise RegistryError("npm package metadata has no versions object")
    release = versions.get(version)
    if release is None:
        return None
    if not isinstance(release, dict) or not isinstance(release.get("dist"), dict):
        raise RegistryError(f"npm version {version} has invalid metadata")
    integrity = release["dist"].get("integrity")
    if not isinstance(integrity, str) or not integrity:
        raise RegistryError(f"npm version {version} has no integrity")
    return integrity
