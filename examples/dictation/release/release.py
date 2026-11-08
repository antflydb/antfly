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

"""Default-branch release controller for Antfly Dictation. No third-party Python dependencies."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import plistlib
import re
import subprocess
import time
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile
import stat
from pathlib import PurePosixPath
from pathlib import Path

HERE = Path(__file__).resolve().parent
POLICY = json.loads((HERE / "policy.json").read_text())
SPARKLE = "http://www.andymatuschak.org/xml-namespaces/sparkle"
ET.register_namespace("sparkle", SPARKLE)
TAG = re.compile(
    r"dictation/v(0|[1-9][0-9]{0,3})\.(0|[1-9][0-9]?)\.(0|[1-9][0-9]?)(?:-(beta|rc)\.([1-9][0-9]{0,2}))?\Z"
)
SHA = re.compile(r"[0-9a-f]{40}\Z")


def run(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def version(tag):
    match = TAG.fullmatch(tag)
    if not match or (match[5] and int(match[5]) > 255):
        raise ValueError(
            "Use dictation/vX.Y.Z or dictation/vX.Y.Z-{beta,rc}.N (N <= 255)"
        )
    major, minor, patch, stage, count = match.groups()
    short = tag.removeprefix("dictation/v")
    bundle = f"{major}.{minor}.{patch}" + (
        {"beta": "b", "rc": "fc"}[stage] + count if stage else ""
    )
    order = (
        int(major),
        int(minor),
        int(patch),
        {"beta": 0, "rc": 1, None: 2}[stage],
        int(count or 0),
    )
    return {
        "version": short,
        "bundle_version": bundle,
        "channel": "beta" if stage else "stable",
        "order": order,
    }


def feed_url(channel):
    if channel not in {"stable", "beta"}:
        raise ValueError("Invalid channel")
    return f"https://raw.githubusercontent.com/{POLICY['repository']}/{POLICY['feed_branch']}/{channel}/appcast.xml"


def public_key(value):
    decoded = base64.b64decode(value, validate=True)
    if len(decoded) != 32 or not any(decoded):
        raise ValueError("A real 32-byte Sparkle public EdDSA key is required")
    return value


def stamp(plist, tag=None, key=None):
    info = plistlib.loads(plist.read_bytes())
    if info["CFBundleIdentifier"] != POLICY["bundle_id"]:
        raise ValueError("Unexpected bundle identity")
    info["AntflyUpdatesEnabled"] = bool(tag)
    info["SUEnableAutomaticChecks"] = False  # User opts in through app settings.
    info["SUAutomaticallyUpdate"] = False
    info["SUEnableSystemProfiling"] = False
    info["SUVerifyUpdateBeforeExtraction"] = True
    info["SURequireSignedFeed"] = True
    if tag:
        identity = version(tag)
        info.update(
            CFBundleVersion=identity["bundle_version"],
            CFBundleShortVersionString=identity["version"],
            SUPublicEDKey=public_key(key),
            SUFeedURL=feed_url("stable"),
            AntflyBetaFeedURL=feed_url("beta"),
            AntflyReleaseChannel=identity["channel"],
        )
    else:
        for name in (
            "SUPublicEDKey",
            "SUFeedURL",
            "AntflyBetaFeedURL",
            "AntflyReleaseChannel",
        ):
            info.pop(name, None)
    plist.write_bytes(plistlib.dumps(info))


def resolve(request, event, controller, default_branch):
    repo = POLICY["repository"]
    trigger = event["workflow_run"]
    if (
        trigger.get("name") != "Dictation release request"
        or trigger.get("path") != ".github/workflows/dictation-release-request.yml"
        or trigger.get("event") != "push"
        or trigger.get("conclusion") != "success"
        or trigger.get("head_repository", {}).get("full_name") != repo
        or event.get("repository", {}).get("full_name") != repo
        or set(request) != {"schema_version", "tag", "source_commit"}
        or type(request["schema_version"]) is not int
        or request["schema_version"] != 1
    ):
        raise ValueError("Untrusted release request")
    identity = version(request["tag"])
    source = request["source_commit"]
    if not SHA.fullmatch(source) or not SHA.fullmatch(controller):
        raise ValueError("Invalid commit identity")
    if source != trigger.get("head_sha") or request["tag"] != trigger.get(
        "head_branch"
    ):
        raise ValueError("Request differs from the actual tag event")
    if run("git", "rev-parse", "HEAD") != controller:
        raise ValueError("Controller checkout mismatch")
    run("git", "fetch", "--no-tags", "origin", f"refs/heads/{default_branch}")
    run("git", "merge-base", "--is-ancestor", source, "FETCH_HEAD")
    if run("git", "rev-parse", f"refs/tags/{request['tag']}^{{commit}}") != source:
        raise ValueError("Release tag moved")
    identity.update(
        tag=request["tag"], source_commit=source, controller_commit=controller
    )
    identity.pop("order")
    return identity


def verify_bundle(bundle, identity, key):
    derived = version(identity["tag"])
    info = plistlib.loads((bundle / "Contents/Info.plist").read_bytes())
    expected = {
        "CFBundleIdentifier": POLICY["bundle_id"],
        "CFBundleExecutable": "AntflyDictation",
        "CFBundleVersion": derived["bundle_version"],
        "CFBundleShortVersionString": derived["version"],
        "AntflyReleaseChannel": derived["channel"],
        "AntflyUpdatesEnabled": True,
        "SUPublicEDKey": public_key(key),
        "SUFeedURL": feed_url("stable"),
        "AntflyBetaFeedURL": feed_url("beta"),
        "SURequireSignedFeed": True,
        "SUVerifyUpdateBeforeExtraction": True,
        "SUEnableSystemProfiling": False,
        "SUEnableAutomaticChecks": False,
        "SUAutomaticallyUpdate": False,
        "LSMinimumSystemVersion": POLICY["minimum_system_version"],
    }
    if any(info.get(k) != value for k, value in expected.items()):
        raise ValueError("Unsigned bundle metadata mismatch")
    framework = bundle / "Contents/Frameworks/Sparkle.framework/Resources/Info.plist"
    if (
        plistlib.loads(framework.read_bytes()).get("CFBundleShortVersionString")
        != POLICY["sparkle_version"]
    ):
        raise ValueError("Embedded Sparkle differs from the trusted release policy")
    binary = bundle / "Contents/MacOS/AntflyDictation"
    for arch in ("arm64", "x86_64"):
        run("lipo", "-verify_arch", arch, str(binary))
    load_commands = run("xcrun", "vtool", "-show-build", str(binary))
    minimums = re.findall(r"\bminos\s+(\d+(?:\.\d+)+)", load_commands)

    def components(value):
        return (tuple(int(v) for v in value.split(".")) + (0, 0, 0))[:3]

    if len(minimums) != 2 or any(
        components(m) > components(POLICY["minimum_system_version"]) for m in minimums
    ):
        raise ValueError(
            "App binary requires a newer macOS than the update feed advertises"
        )


def verify_unsigned_zip(archive):
    total = 0
    names = set()
    with zipfile.ZipFile(archive) as z:
        if len(z.infolist()) > 20000:
            raise ValueError("Too many archive entries")
        for item in z.infolist():
            p = PurePosixPath(item.filename)
            normalized = str(p).casefold()
            if normalized in names:
                raise ValueError("Duplicate app archive path")
            names.add(normalized)
            # ditto includes AppleDouble metadata alongside the one app bundle.
            if (
                p.is_absolute()
                or ".." in p.parts
                or "\\" in item.filename
                or not p.parts
            ):
                raise ValueError("Unsafe app archive path")
            if p.parts[0] not in {"Antfly Dictation.app", "__MACOSX"}:
                raise ValueError("Unexpected archive root")
            total += item.file_size
            if total > 512 * 1024 * 1024:
                raise ValueError("App archive is too large")
            if stat.S_ISLNK(item.external_attr >> 16):
                target = z.read(item).decode()
                if target.startswith("/"):
                    raise ValueError("Absolute symlink")
                resolved = list(p.parent.parts)
                for part in PurePosixPath(target).parts:
                    if part == "..":
                        if len(resolved) <= 1:
                            raise ValueError("Escaping symlink")
                        resolved.pop()
                    elif part != ".":
                        resolved.append(part)


class GitHubError(RuntimeError):
    def __init__(self, path, stderr):
        status = re.search(r"\bHTTP\s*(\d{3})\b", stderr, re.IGNORECASE)
        self.status = int(status[1]) if status else None
        self.stderr = stderr
        super().__init__(f"GitHub API failed at {path}: {stderr.strip()}")


def gh_api(path, method="GET", payload=None):
    args = ["gh", "api", path, "--method", method]
    if payload is not None:
        args += ["--input", "-"]
    response = subprocess.run(
        args,
        text=True,
        capture_output=True,
        input=json.dumps(payload) if payload is not None else None,
    )
    if response.returncode:
        raise GitHubError(path, response.stderr)
    return json.loads(response.stdout) if response.stdout.strip() else None


def verify_source(identity):
    if not SHA.fullmatch(identity["source_commit"]) or not SHA.fullmatch(
        identity["controller_commit"]
    ):
        raise ValueError("Invalid release identity")
    if set(identity) != {
        "version",
        "bundle_version",
        "channel",
        "tag",
        "source_commit",
        "controller_commit",
    }:
        raise ValueError("Unexpected release identity fields")
    derived = version(identity["tag"])
    if any(
        identity[key] != derived[key]
        for key in ("version", "bundle_version", "channel")
    ):
        raise ValueError("Release version mismatch")
    comparison = gh_api(
        f"repos/{POLICY['repository']}/compare/{identity['source_commit']}...{os.environ['DEFAULT_BRANCH']}"
    )
    if comparison["status"] not in {"ahead", "identical"}:
        raise ValueError("Source is no longer on the default branch")
    ref = gh_api(f"repos/{POLICY['repository']}/git/ref/tags/{identity['tag']}")[
        "object"
    ]
    for _ in range(4):
        if ref["type"] == "commit":
            break
        if ref["type"] != "tag":
            raise ValueError("Invalid tag object")
        ref = gh_api(f"repos/{POLICY['repository']}/git/tags/{ref['sha']}")["object"]
    if ref["type"] != "commit" or ref["sha"] != identity["source_commit"]:
        raise ValueError("Release tag moved")
    # A queued older controller must not publish after its release policy changed.
    for path in (
        "examples/dictation/release",
        ".github/workflows/dictation-release.yml",
    ):
        encoded = urllib.parse.quote(path, safe="/")
        current = gh_api(
            f"repos/{POLICY['repository']}/contents/{encoded}?ref={os.environ['DEFAULT_BRANCH']}"
        )
        pinned = gh_api(
            f"repos/{POLICY['repository']}/contents/{encoded}?ref={identity['controller_commit']}"
        )
        if current != pinned:
            # Contents API URLs differ by ref; compare only immutable object IDs.
            def ids(value):
                return (
                    sorted((v["name"], v["sha"]) for v in value)
                    if isinstance(value, list)
                    else value["sha"]
                )

            if ids(current) != ids(pinned):
                raise ValueError(
                    "Release controller policy changed; use a new release tag"
                )


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def appcast(identity, archive, signature):
    if len(base64.b64decode(signature, validate=True)) != 64:
        raise ValueError("Invalid EdDSA archive signature")
    root = ET.Element("rss", version="2.0")
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text = "Antfly Dictation"
    item = ET.SubElement(channel, "item")
    ET.SubElement(item, "title").text = "Antfly Dictation " + identity["version"]
    ET.SubElement(item, f"{{{SPARKLE}}}version").text = identity["bundle_version"]
    ET.SubElement(item, f"{{{SPARKLE}}}shortVersionString").text = identity["version"]
    ET.SubElement(item, f"{{{SPARKLE}}}minimumSystemVersion").text = POLICY[
        "minimum_system_version"
    ]
    ET.SubElement(
        item, "description"
    ).text = f"Antfly Dictation {identity['version']}. Settings and retained data are preserved."
    url = f"https://github.com/{POLICY['repository']}/releases/download/{identity['tag']}/{archive.name}"
    ET.SubElement(
        item, "link"
    ).text = f"https://github.com/{POLICY['repository']}/releases/tag/{identity['tag']}"
    ET.SubElement(
        item,
        "enclosure",
        {
            "url": url,
            "length": str(archive.stat().st_size),
            "type": "application/octet-stream",
            f"{{{SPARKLE}}}edSignature": signature,
        },
    )
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def verify_feed(identity, directory):
    feed = (directory / "appcast.xml").read_bytes()
    if len(feed) > 256 * 1024:
        raise ValueError("Appcast exceeds its size limit")
    item = ET.fromstring(feed).find("channel/item")
    if (
        item is None
        or item.findtext(f"{{{SPARKLE}}}version") != identity["bundle_version"]
    ):
        raise ValueError("Appcast bundle version mismatch")
    if item.findtext(f"{{{SPARKLE}}}shortVersionString") != identity["version"]:
        raise ValueError("Appcast release version mismatch")
    archive = directory / ("Antfly-Dictation-" + identity["version"] + "-universal.zip")
    enclosure = item.find("enclosure")
    expected_url = f"https://github.com/{POLICY['repository']}/releases/download/{identity['tag']}/{archive.name}"
    if (
        enclosure is None
        or enclosure.get("url") != expected_url
        or enclosure.get("length") != str(archive.stat().st_size)
    ):
        raise ValueError("Appcast download identity mismatch")
    if (
        len(
            base64.b64decode(
                enclosure.get(f"{{{SPARKLE}}}edSignature", ""), validate=True
            )
        )
        != 64
    ):
        raise ValueError("Appcast archive signature is missing")
    return feed


def update_feed(tree, channel, incoming):
    """One latest signed feed per channel; never downgrade or rewrite a version."""
    path = channel + "/appcast.xml"
    previous = tree.get(path)
    if previous:

        def release(data):
            return ET.fromstring(data).findtext(
                f"channel/item/{{{SPARKLE}}}shortVersionString"
            )

        old = version("dictation/v" + release(previous))["order"]
        new = version("dictation/v" + release(incoming))["order"]
        if new < old:
            return
        if new == old:
            if previous != incoming:
                raise ValueError("Conflicting bytes for an existing feed version")
            return
    tree[path] = incoming


def publish_feed(identity, feed):
    repo = POLICY["repository"]
    branch = POLICY["feed_branch"]
    refs = gh_api(f"repos/{repo}/git/matching-refs/heads/{branch}")
    refs = [r for r in refs if r["ref"] == "refs/heads/" + branch]
    head = refs[0]["object"]["sha"] if refs else None
    tree = {}
    base_tree = None
    if head:
        commit = gh_api(f"repos/{repo}/git/commits/{head}")
        base_tree = commit["tree"]["sha"]
        for item in gh_api(f"repos/{repo}/git/trees/{base_tree}?recursive=1")["tree"]:
            if item["path"] in {"stable/appcast.xml", "beta/appcast.xml"}:
                blob = gh_api(f"repos/{repo}/git/blobs/{item['sha']}")
                tree[item["path"]] = base64.b64decode(blob["content"])
    original = dict(tree)
    update_feed(tree, identity["channel"], feed)
    if identity["channel"] == "stable":
        update_feed(tree, "beta", feed)
    changed = {p: data for p, data in tree.items() if original.get(p) != data}
    if not changed:
        return
    entries = []
    for path, data in changed.items():
        blob = gh_api(
            f"repos/{repo}/git/blobs",
            "POST",
            {"content": base64.b64encode(data).decode(), "encoding": "base64"},
        )
        entries.append(
            {"path": path, "mode": "100644", "type": "blob", "sha": blob["sha"]}
        )
    payload = {"tree": entries}
    if base_tree:
        payload["base_tree"] = base_tree
    new_tree = gh_api(f"repos/{repo}/git/trees", "POST", payload)
    commit = gh_api(
        f"repos/{repo}/git/commits",
        "POST",
        {
            "message": "Publish Antfly Dictation " + identity["version"],
            "tree": new_tree["sha"],
            "parents": [head] if head else [],
        },
    )
    if head:
        # Fast-forward only; a concurrent publication fails instead of losing data.
        gh_api(
            f"repos/{repo}/git/refs/heads/{branch}",
            "PATCH",
            {"sha": commit["sha"], "force": False},
        )
    else:
        gh_api(
            f"repos/{repo}/git/refs",
            "POST",
            {"ref": "refs/heads/" + branch, "sha": commit["sha"]},
        )


def inspect_release_assets(release, identity, expected, sizes):
    """Validate the whole remote snapshot before deleting even a placeholder."""
    if (
        release.get("tag_name") != identity["tag"]
        or release.get("target_commitish") != identity["source_commit"]
        or release.get("prerelease") is not (identity["channel"] == "beta")
        or type(release.get("draft")) is not bool
        or type(release.get("id")) is not int
        or release["id"] <= 0
    ):
        raise ValueError("Existing release source/version identity mismatch")
    present = set()
    unfinished = []
    seen = set()
    for asset in release["assets"]:
        name = asset["name"]
        if name in seen or name not in expected:
            raise ValueError("Duplicate or unexpected release asset")
        seen.add(name)
        if type(asset.get("id")) is not int or asset["id"] <= 0:
            raise ValueError("Invalid release asset identity")
        if (
            release["draft"]
            and asset.get("state") == "starter"
            and type(asset.get("size")) is int
            and asset["size"] == 0
            and asset.get("digest")
            in (None, "sha256:" + hashlib.sha256(b"").hexdigest())
        ):
            unfinished.append(asset["id"])
        elif (
            asset.get("state") == "uploaded"
            and type(asset.get("size")) is int
            and asset["size"] == sizes[name]
            and asset.get("digest") == "sha256:" + expected[name]
        ):
            present.add(name)
        else:
            raise ValueError(
                "Existing release asset differs or is incomplete; never overwrite released bytes: "
                + name
            )
    return present, unfinished


def retryable_upload_error(error):
    detail = error.stderr or ""
    status = re.search(r"\bHTTP\s*(\d{3})\b", detail, re.IGNORECASE)
    if status:
        return int(status[1]) in {408, 429} or 500 <= int(status[1]) < 600
    return (
        re.search(
            r"timed? out|timeout|context deadline exceeded|connection (?:reset|closed|refused|aborted)|\bEOF\b|broken pipe|temporary failure",
            detail,
            re.IGNORECASE,
        )
        is not None
    )


def upload_release_assets(identity, directory, release, expected):
    repo = POLICY["repository"]
    sizes = {name: (directory / name).stat().st_size for name in expected}
    attempts = 3
    for attempt in range(attempts):
        present, unfinished = inspect_release_assets(release, identity, expected, sizes)
        if present == set(expected):
            return release
        if not release["draft"]:
            raise ValueError(
                "Published release is incomplete; never modify published assets"
            )
        error = None
        try:
            for asset_id in unfinished:
                try:
                    gh_api(f"repos/{repo}/releases/assets/{asset_id}", "DELETE")
                except GitHubError as deletion_error:
                    if deletion_error.status != 404:
                        raise
            missing = sorted(set(expected) - present)
            run(
                "gh",
                "release",
                "upload",
                identity["tag"],
                "--repo",
                repo,
                *[str(directory / name) for name in missing],
                stderr=subprocess.PIPE,
            )
        except (subprocess.CalledProcessError, GitHubError) as mutation_error:
            error = mutation_error
        # A failed response can still mean GitHub accepted some/all bytes.
        # Always reconcile before retrying; complete uploads are never repeated.
        release = gh_api(f"repos/{repo}/releases/tags/{identity['tag']}")
        present, _ = inspect_release_assets(release, identity, expected, sizes)
        if present == set(expected):
            return release
        if error is not None and (
            not retryable_upload_error(error) or attempt + 1 == attempts
        ):
            raise error
        if attempt + 1 == attempts:
            raise ValueError(
                "Release upload remains incomplete after three attempts; re-run failed publication jobs"
            )
        time.sleep(attempt + 1)
    raise AssertionError("Unreachable upload retry state")


def publish(identity, directory):
    verify_source(identity)
    manifest = json.loads((directory / "release.json").read_text())
    if manifest.get("schema_version") != 1:
        raise ValueError("Unsupported release manifest")
    if manifest["identity"] != identity:
        raise ValueError("Artifact identity mismatch")
    expected = {
        "Antfly-Dictation-" + identity["version"] + "-universal.zip",
        "appcast.xml",
        "SHA256SUMS",
    }
    if set(manifest["sha256"]) != expected:
        raise ValueError("Unexpected release payload")
    for name, checksum in manifest["sha256"].items():
        if digest(directory / name) != checksum:
            raise ValueError("Artifact digest mismatch")
    feed = verify_feed(identity, directory)
    repo = POLICY["repository"]
    try:
        release = gh_api(f"repos/{repo}/releases/tags/{identity['tag']}")
    except GitHubError as error:
        if error.status != 404:
            raise
        release = None
    if release is None:
        args = [
            "gh",
            "release",
            "create",
            identity["tag"],
            "--repo",
            repo,
            "--verify-tag",
            "--target",
            identity["source_commit"],
            "--draft",
            "--latest=false",
            "--title",
            "Antfly Dictation " + identity["version"],
            "--notes",
            "Signed and notarized universal macOS app. Install in Applications; see the dictation release guide for setup.",
        ]
        if identity["channel"] == "beta":
            args.append("--prerelease")
        run(*args)
        release = gh_api(f"repos/{repo}/releases/tags/{identity['tag']}")
    expected_digests = dict(manifest["sha256"])
    expected_digests["release.json"] = digest(directory / "release.json")
    release = upload_release_assets(identity, directory, release, expected_digests)
    verify_source(identity)
    if release["draft"]:
        run(
            "gh",
            "release",
            "edit",
            identity["tag"],
            "--repo",
            repo,
            "--draft=false",
            "--latest=false",
        )
    # The feed moves only after every immutable download is available.
    publish_feed(identity, feed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("stamp")
    p.add_argument("plist", type=Path)
    p.add_argument("--tag")
    p.add_argument("--key")
    p = sub.add_parser("resolve")
    p.add_argument("request", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("appcast")
    p.add_argument("identity", type=Path)
    p.add_argument("archive", type=Path)
    p.add_argument("signature")
    p = sub.add_parser("manifest")
    p.add_argument("identity", type=Path)
    p.add_argument("directory", type=Path)
    p = sub.add_parser("verify-bundle")
    p.add_argument("identity", type=Path)
    p.add_argument("bundle", type=Path)
    p.add_argument("--key", required=True)
    p = sub.add_parser("verify-zip")
    p.add_argument("archive", type=Path)
    p = sub.add_parser("publish")
    p.add_argument("identity", type=Path)
    p.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.command == "stamp":
        stamp(args.plist, args.tag, args.key)
    elif args.command == "resolve":
        identity = resolve(
            json.loads(args.request.read_text()),
            json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()),
            os.environ["GITHUB_SHA"],
            os.environ["DEFAULT_BRANCH"],
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(identity, sort_keys=True, indent=2) + "\n")
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            for key in ("source_commit", "version", "tag", "channel"):
                f.write(f"{key}={identity[key]}\n")
    elif args.command == "appcast":
        print(
            appcast(
                json.loads(args.identity.read_text()), args.archive, args.signature
            ).decode()
        )
    elif args.command == "manifest":
        files = sorted(
            p
            for p in args.directory.iterdir()
            if p.name != "release.json" and p.is_file()
        )
        (args.directory / "release.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "identity": json.loads(args.identity.read_text()),
                    "sha256": {p.name: digest(p) for p in files},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
    elif args.command == "verify-bundle":
        verify_bundle(args.bundle, json.loads(args.identity.read_text()), args.key)
    elif args.command == "verify-zip":
        verify_unsigned_zip(args.archive)
    elif args.command == "publish":
        publish(json.loads(args.identity.read_text()), args.directory)


if __name__ == "__main__":
    main()
