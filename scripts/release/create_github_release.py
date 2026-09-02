#!/usr/bin/env python3
"""Create or update the Antfly GitHub draft release and upload assets."""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_BASE = "https://api.github.com"
UPLOAD_BASE = "https://uploads.github.com"


class GitHubError(RuntimeError):
    pass


def request_json(
    method: str,
    url: str,
    token: str,
    body: dict | None = None,
    headers: dict[str, str] | None = None,
) -> dict | list | None:
    data = None
    merged_headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if headers:
        merged_headers.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        merged_headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=merged_headers, method=method)
    try:
        with urlopen(req) as resp:
            payload = resp.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise GitHubError(f"{method} {url} failed with {exc.code}: {detail}") from exc
    if not payload:
        return None
    return json.loads(payload)


def request_bytes(
    method: str, url: str, token: str, data: bytes, content_type: str
) -> dict:
    req = Request(
        url,
        data=data,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": content_type,
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method=method,
    )
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise GitHubError(f"{method} {url} failed with {exc.code}: {detail}") from exc


def download_bytes(url: str, token: str) -> bytes:
    req = Request(
        url,
        headers={
            "Accept": "application/octet-stream",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urlopen(req) as resp:
            return resp.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise GitHubError(f"GET {url} failed with {exc.code}: {detail}") from exc


def github_api(
    method: str, repo: str, path: str, token: str, body: dict | None = None
) -> dict | list | None:
    return request_json(method, f"{API_BASE}/repos/{repo}{path}", token, body)


def get_release_by_tag(repo: str, tag: str, token: str) -> dict | None:
    try:
        release = github_api("GET", repo, f"/releases/tags/{tag}", token)
    except GitHubError as exc:
        if "failed with 404" not in str(exc):
            raise
        releases = github_api("GET", repo, "/releases?per_page=100", token)
        if isinstance(releases, list):
            for candidate in releases:
                if isinstance(candidate, dict) and candidate.get("tag_name") == tag:
                    return candidate
            return None
        raise
    assert isinstance(release, dict)
    return release


def create_or_update_release(repo: str, tag: str, token: str, payload: dict) -> dict:
    release = get_release_by_tag(repo, tag, token)
    if release is None:
        created = github_api("POST", repo, "/releases", token, payload)
        assert isinstance(created, dict)
        print(f"created GitHub release draft for {tag}")
        return created

    # Publishing is a one-way state transition. Recovery may repair missing
    # assets on an existing release, but must never hide it by returning it to
    # draft state.
    if not release.get("draft", False) and payload.get("draft", False):
        print(f"preserving already-published GitHub release for {tag}")
        return release

    updated = github_api("PATCH", repo, f"/releases/{release['id']}", token, payload)
    assert isinstance(updated, dict)
    print(f"updated GitHub release for {tag}")
    return updated


def generate_notes(repo: str, tag: str, commit: str, token: str) -> str:
    try:
        generated = github_api(
            "POST",
            repo,
            "/releases/generate-notes",
            token,
            {"tag_name": tag, "target_commitish": commit},
        )
    except GitHubError as exc:
        print(f"warning: failed to generate GitHub release notes: {exc}")
        return ""
    if not isinstance(generated, dict):
        return ""
    return str(generated.get("body", "")).strip()


def release_body(repo: str, tag: str, commit: str, token: str) -> str:
    date = datetime.now(timezone.utc).date().isoformat()
    generated = generate_notes(repo, tag, commit, token)
    parts = [
        f"# Antfly version {tag}",
        "",
        f"### Documentation ({date})",
        "",
        "The documentation is available here:",
        "",
        "https://docs.antfly.io",
    ]
    if generated:
        parts.extend(["", generated])
    parts.extend(["", "---", "", "Released by Antfly release scripts."])
    return "\n".join(parts)


def expand_assets(patterns: list[str]) -> list[Path]:
    assets: list[Path] = []
    seen: set[str] = set()
    for pattern in patterns:
        matches = [Path(match) for match in glob.glob(pattern)]
        if not matches:
            raise SystemExit(f"asset pattern did not match anything: {pattern}")
        for path in matches:
            if not path.is_file():
                continue
            if path.name in seen:
                raise SystemExit(f"duplicate release asset name: {path.name}")
            seen.add(path.name)
            assets.append(path)
    return sorted(assets, key=lambda path: path.name)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def existing_asset_sha256(existing: dict, token: str) -> str:
    digest = existing.get("digest")
    if isinstance(digest, str) and digest.startswith("sha256:"):
        return digest.removeprefix("sha256:")
    url = existing.get("url") or existing.get("browser_download_url")
    if not isinstance(url, str) or not url:
        raise GitHubError(f"release asset {existing.get('name')} has no download URL")
    return hashlib.sha256(download_bytes(url, token)).hexdigest()


def upload_asset(
    repo: str,
    release: dict,
    asset: Path,
    token: str,
    replace_assets: bool,
    immutable_assets: bool,
) -> None:
    release_id = release["id"]
    for existing in release.get("assets", []):
        if existing.get("name") == asset.name:
            if immutable_assets:
                local_digest = sha256(asset)
                remote_digest = existing_asset_sha256(existing, token)
                if local_digest != remote_digest:
                    raise SystemExit(
                        f"immutable release asset differs: {asset.name}\n"
                        f"GitHub: {remote_digest}\nlocal:  {local_digest}"
                    )
                print(f"immutable GitHub release asset already matches: {asset.name}")
                return
            if not replace_assets:
                raise SystemExit(f"release asset already exists: {asset.name}")
            github_api("DELETE", repo, f"/releases/assets/{existing['id']}", token)
            break

    query = urlencode({"name": asset.name})
    url = f"{UPLOAD_BASE}/repos/{repo}/releases/{release_id}/assets?{query}"
    content_type = mimetypes.guess_type(asset.name)[0] or "application/octet-stream"
    request_bytes("POST", url, token, asset.read_bytes(), content_type)
    print(f"uploaded GitHub release asset: {asset.name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo", default=os.environ.get("GITHUB_REPOSITORY"), help="owner/repo"
    )
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument(
        "--asset", action="append", default=[], help="asset glob to upload"
    )
    parser.add_argument(
        "--draft", action="store_true", help="create/update as a draft release"
    )
    parser.add_argument(
        "--replace-assets",
        action="store_true",
        help="replace existing assets with matching names",
    )
    parser.add_argument(
        "--immutable-assets",
        action="store_true",
        help="skip byte-identical existing assets and reject content drift",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.replace_assets and args.immutable_assets:
        raise SystemExit(
            "--replace-assets and --immutable-assets are mutually exclusive"
        )

    if not args.repo:
        raise SystemExit("--repo is required when GITHUB_REPOSITORY is unset")
    assets = expand_assets(args.asset)
    prerelease = "-" in args.tag.lstrip("v")

    if args.dry_run:
        print(f"would create/update GitHub release {args.repo}@{args.tag}")
        print(f"  commit: {args.commit}")
        print(f"  draft: {args.draft}")
        print(f"  prerelease: {prerelease}")
        for asset in assets:
            print(f"  asset: {asset}")
        return 0

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required")

    body = release_body(args.repo, args.tag, args.commit, token)
    payload = {
        "tag_name": args.tag,
        "target_commitish": args.commit,
        "name": args.tag,
        "body": body,
        "draft": args.draft,
        "prerelease": prerelease,
    }

    release = create_or_update_release(args.repo, args.tag, token, payload)

    for asset in assets:
        upload_asset(
            args.repo, release, asset, token, args.replace_assets, args.immutable_assets
        )
        release = get_release_by_tag(args.repo, args.tag, token) or release
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
