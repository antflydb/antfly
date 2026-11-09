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

"""Default-branch release controller for Antfly Dictation. Uses the shared release storage adapters."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import plistlib
import re
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile
import stat
from pathlib import PurePosixPath
from pathlib import Path

HERE = Path(__file__).resolve().parent
POLICY = json.loads((HERE / "policy.json").read_text())
SHARED_RELEASE_DIR = HERE.parents[2] / "scripts/release"
sys.path.append(str(SHARED_RELEASE_DIR))
from verify_release_ledger import verify_payload  # noqa: E402 — anchored repository imports
from download_objectstorage import PublicReader, restore_payload  # noqa: E402 — anchored repository imports
from objectstore_channel import ConditionalWriteConflict  # noqa: E402 — anchored repository imports
from create_github_release import get_release_by_tag, with_complete_assets  # noqa: E402 — anchored repository imports

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
    return f"{POLICY['public_base_url']}/channels/{channel}/appcast.xml"


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
    for arch in ("arm64",):
        run("lipo", "-verify_arch", arch, str(binary))
    load_commands = run("xcrun", "vtool", "-show-build", str(binary))
    minimums = re.findall(r"\bminos\s+(\d+(?:\.\d+)+)", load_commands)

    def components(value):
        return (tuple(int(v) for v in value.split(".")) + (0, 0, 0))[:3]

    if len(minimums) != 1 or any(
        components(m) > components(POLICY["minimum_system_version"]) for m in minimums
    ):
        raise ValueError(
            "App binary requires a newer macOS than the update feed advertises"
        )

    import importlib.util

    specification = importlib.util.spec_from_file_location(
        "dictation_runtime", HERE / "fetch-runtime.py"
    )
    runtime_policy = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(runtime_policy)
    runtime = bundle / "Contents/Resources/Runtime"
    runtime_policy.verify_staged(runtime)
    executable = runtime / "antfly"
    run("lipo", "-verify_arch", "arm64", str(executable))
    if run("lipo", "-archs", str(binary)).strip() != "arm64":
        raise ValueError("The app must contain only Apple Silicon code")
    if run("lipo", "-archs", str(executable)).strip() != "arm64":
        raise ValueError("The runtime must contain only Apple Silicon code")
    for dependency in run("otool", "-L", str(executable)).splitlines():
        if dependency[:1].isspace():
            linked = dependency.strip().split(" ", 1)[0]
            if linked.startswith("/") and not linked.startswith(
                ("/System/Library/", "/usr/lib/")
            ):
                raise ValueError(
                    "Released runtime has an external library dependency: " + linked
                )
    commands = run("xcrun", "vtool", "-show-build", str(executable))
    minimums = re.findall(r"\bminos\s+(\d+(?:\.\d+)+)", commands)
    if len(minimums) != 1 or components(minimums[0]) > components(
        POLICY["minimum_system_version"]
    ):
        raise ValueError("Released runtime requires a newer macOS than the app")


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


def validate_identity(identity):
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


def verify_source(identity):
    validate_identity(identity)
    controller = os.environ.get("GITHUB_SHA", identity["controller_commit"])
    if not SHA.fullmatch(controller):
        raise ValueError("Invalid executing controller")
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
        ".github/workflows/dictation-release-recovery.yml",
        "scripts/release/objectstore_channel.py",
        "scripts/release/publish_objectstorage.py",
        "scripts/release/download_objectstorage.py",
        "scripts/release/verify_release_ledger.py",
        "scripts/release/create_github_release.py",
        "scripts/release/release_channels.py",
        "scripts/release/release_lines.py",
        "scripts/release/requirements.lock",
    ):
        encoded = urllib.parse.quote(path, safe="/")
        current = gh_api(
            f"repos/{POLICY['repository']}/contents/{encoded}?ref={os.environ['DEFAULT_BRANCH']}"
        )
        pinned = gh_api(
            f"repos/{POLICY['repository']}/contents/{encoded}?ref={controller}"
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
    url = f"{POLICY['public_base_url']}/releases/{identity['version']}/{archive.name}"
    ET.SubElement(item, "link").text = url
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
    archive = directory / ("Antfly-Dictation-" + identity["version"] + "-arm64.zip")
    enclosure = item.find("enclosure")
    expected_url = (
        f"{POLICY['public_base_url']}/releases/{identity['version']}/{archive.name}"
    )
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


def public_signing_key():
    reader = PublicReader(POLICY["public_base_url"], max_bytes=4096)
    body = reader.read_optional("signing/public-key.json")
    if body is None:
        # A missing trust record is not permission to bootstrap over an existing feed.
        for channel in ("stable", "beta"):
            if (
                PublicReader(POLICY["public_base_url"]).read_optional(
                    f"channels/{channel}/appcast.xml"
                )
                is not None
            ):
                raise ValueError(
                    "R2 update feed exists without its signing identity; restore the original trust record"
                )
        return None
    document = json.loads(body)
    if document.get("schema_version") != 1 or set(document) != {
        "schema_version",
        "public_key",
    }:
        raise ValueError("Invalid R2 signing identity")
    return public_key(document["public_key"])


def release_prefix(identity):
    validate_identity(identity)
    return f"{POLICY['storage_prefix']}/releases/{identity['version']}"


def write_manifest(identity, directory, key):
    validate_identity(identity)
    names = {
        "Antfly-Dictation-" + identity["version"] + "-arm64.zip",
        "appcast.xml",
        "SHA256SUMS",
    }
    if {p.name for p in directory.iterdir()} != names:
        raise ValueError(
            "Release directory must contain exactly the signed app, feed, and checksums"
        )
    document = {
        "schema_version": 2,
        "product": "antfly-dictation",
        "tag": identity["tag"],
        "commit": identity["source_commit"],
        "identity": identity,
        "signing_public_key": public_key(key),
        "artifacts": [
            {
                "name": name,
                "sha256": digest(directory / name),
                "size": (directory / name).stat().st_size,
                "scope": "support",
            }
            for name in sorted(names)
        ],
    }
    (directory / "artifacts.json").write_text(
        json.dumps(document, sort_keys=True, indent=2) + "\n"
    )


def validate_payload(identity, directory, ledger_sha256=None):
    validate_identity(identity)
    ledger_path = directory / "artifacts.json"
    expected_digest = ledger_sha256 or digest(ledger_path)
    verify_payload(
        ledger_path,
        directory,
        identity["tag"],
        identity["source_commit"],
        expected_digest,
    )
    document = json.loads(ledger_path.read_bytes())
    expected_names = {
        "Antfly-Dictation-" + identity["version"] + "-arm64.zip",
        "appcast.xml",
        "SHA256SUMS",
    }
    if (
        document.get("schema_version") != 2
        or document.get("product") != "antfly-dictation"
        or document.get("identity") != identity
        or {entry["name"] for entry in document["artifacts"]} != expected_names
    ):
        raise ValueError("Dictation release ledger identity or member set differs")
    public_key(document["signing_public_key"])
    verify_feed(identity, directory)
    return document


def promote_feed(storage, channel, identity, feed):
    if channel not in {"stable", "beta"}:
        raise ValueError("Invalid dictation update channel")
    store = storage.channel_store(
        f"{POLICY['storage_prefix']}/channels/{channel}/appcast.xml"
    )
    for attempt in range(3):
        previous = store.load_bytes(max_bytes=256 * 1024)
        if previous.body is not None:
            root = ET.fromstring(previous.body)
            old_version = root.findtext(f"channel/item/{{{SPARKLE}}}shortVersionString")
            if old_version is None:
                raise ValueError("Current R2 feed has no release identity")
            old = version("dictation/v" + old_version)["order"]
            new = version(identity["tag"])["order"]
            if new < old:
                return "superseded"
            if new == old:
                if previous.body != feed:
                    raise ValueError(
                        "Conflicting signed feed bytes for an existing version"
                    )
                return "committed"
        try:
            store.compare_and_swap_bytes(
                previous, feed, "application/xml", "no-cache, no-transform"
            )
            return "committed"
        except ConditionalWriteConflict:
            if attempt == 2:
                raise
    raise AssertionError("Unreachable feed publication state")


def publish(identity, directory, storage, source_verifier=None):
    source_verifier = source_verifier or verify_source
    source_verifier(identity)
    ledger = validate_payload(identity, directory)
    ledger_path = directory / "artifacts.json"
    ledger_digest = digest(ledger_path)
    prefix = release_prefix(identity)
    # Reserve both long-lived signing trust and version identity before writing
    # the release prefix. Neither can be silently replaced by a competing build.
    import tempfile

    with tempfile.TemporaryDirectory(
        dir=directory.parent, prefix="dictation-seal-"
    ) as temporary:
        temporary = Path(temporary)
        trust = temporary / "public-key.json"
        trust.write_text(
            json.dumps(
                {"schema_version": 1, "public_key": ledger["signing_public_key"]},
                sort_keys=True,
            )
            + "\n"
        )
        storage.upload_immutable(
            trust, f"{POLICY['storage_prefix']}/signing/public-key.json"
        )
        reservation = temporary / "identity.json"
        reservation.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "identity": identity,
                    "ledger_sha256": ledger_digest,
                },
                sort_keys=True,
            )
            + "\n"
        )
        storage.upload_immutable(
            reservation,
            f"{POLICY['storage_prefix']}/identities/{identity['version']}.json",
        )
    members = {entry["name"] for entry in ledger["artifacts"]} | {"artifacts.json"}
    existing = set(storage.reader.list_names(prefix))
    if existing - members:
        raise ValueError("Unexpected objects in immutable release prefix")
    for name in sorted(members - {"artifacts.json"}) + ["artifacts.json"]:
        if name == "artifacts.json":
            # Never seal a prefix with extra/missing members.
            current = set(storage.reader.list_names(prefix))
            if current - {"artifacts.json"} != members - {"artifacts.json"}:
                raise ValueError("Release prefix is incomplete before sealing")
        path = directory / name
        storage.upload_immutable(
            path, f"{POLICY['storage_prefix']}/artifacts/sha256/{digest(path)}/{name}"
        )
        # The ledger is the final commit marker for an exact immutable prefix.
        storage.upload_immutable(path, f"{prefix}/{name}")
    storage.exact(prefix, members)
    source_verifier(identity)
    feed = (directory / "appcast.xml").read_bytes()
    channels = (
        [identity["channel"]] if identity["channel"] == "beta" else ["stable", "beta"]
    )
    return {
        channel: promote_feed(storage, channel, identity, feed) for channel in channels
    }


def recover(storage, tag, commit, ledger_sha256, out_dir):
    identity_version = version(tag)
    prefix = f"{POLICY['storage_prefix']}/releases/{identity_version['version']}"
    import tempfile

    if out_dir.exists():
        raise ValueError("Recovery output must not already exist")
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        dir=out_dir.parent, prefix="dictation-recover-"
    ) as temporary:
        stage = Path(temporary) / "payload"
        restore_payload(storage.reader, prefix, stage, tag, commit, ledger_sha256)
        document = json.loads((stage / "artifacts.json").read_bytes())
        identity = document["identity"]
        validate_payload(identity, stage, ledger_sha256)
        trust = storage.reader.read_optional(
            f"{POLICY['storage_prefix']}/signing/public-key.json"
        )
        if trust is None or json.loads(trust) != {
            "schema_version": 1,
            "public_key": document["signing_public_key"],
        }:
            raise ValueError(
                "Recovered release differs from the established R2 signing identity"
            )
        stage.rename(out_dir)
    return identity


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
        release_id = release["id"]
        release = gh_api(f"repos/{repo}/releases/{release_id}")
        release = with_complete_assets(
            repo,
            release,
            os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", ""),
        )
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


def publish_github_mirror(identity, directory):
    verify_source(identity)
    ledger = validate_payload(identity, directory)
    expected_digests = {entry["name"]: entry["sha256"] for entry in ledger["artifacts"]}
    expected_digests["artifacts.json"] = digest(directory / "artifacts.json")
    repo = POLICY["repository"]
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("A GitHub token is required for the optional mirror")
    release = get_release_by_tag(repo, identity["tag"], token)
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
            "Signed and notarized Apple Silicon macOS app. Install in Applications; see the dictation release guide for setup.",
        ]
        if identity["channel"] == "beta":
            args.append("--prerelease")
        run(*args)
        release = get_release_by_tag(repo, identity["tag"], token)
        if release is None:
            raise ValueError("Created GitHub mirror draft could not be resolved")
    release = with_complete_assets(repo, release, token)
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
    # The optional mirror never controls the canonical R2 update channel.


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
    p.add_argument("--key")
    p = sub.add_parser("verify-bundle")
    p.add_argument("identity", type=Path)
    p.add_argument("bundle", type=Path)
    p.add_argument("--key", required=True)
    p = sub.add_parser("verify-zip")
    p.add_argument("archive", type=Path)
    p = sub.add_parser("mirror")
    p.add_argument("identity", type=Path)
    p.add_argument("directory", type=Path)
    for command in ("publish", "recover"):
        p = sub.add_parser(command)
        if command == "publish":
            p.add_argument("identity", type=Path)
            p.add_argument("directory", type=Path)
        else:
            p.add_argument("--tag", required=True)
            p.add_argument("--commit", required=True)
            p.add_argument("--ledger-sha256", required=True)
            p.add_argument("--out-dir", type=Path, required=True)
            p.add_argument("--identity-output", type=Path, required=True)
        p.add_argument("--provider", choices=("s3", "local"), default="s3")
        p.add_argument("--endpoint", default=os.environ.get("AWS_ENDPOINT_URL"))
        p.add_argument("--local-root", type=Path, default=Path("dist/objectstorage"))
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
        write_manifest(
            json.loads(args.identity.read_text()),
            args.directory,
            args.key or os.environ.get("SPARKLE_PUBLIC_KEY"),
        )
    elif args.command == "verify-bundle":
        verify_bundle(args.bundle, json.loads(args.identity.read_text()), args.key)
    elif args.command == "verify-zip":
        verify_unsigned_zip(args.archive)
    elif args.command == "mirror":
        publish_github_mirror(json.loads(args.identity.read_text()), args.directory)
    elif args.command in {"publish", "recover"}:
        from storage import ObjectStorage

        storage = (
            ObjectStorage.s3(args.endpoint, POLICY["bucket"])
            if args.provider == "s3"
            else ObjectStorage.local(args.local_root, POLICY["bucket"])
        )
        if args.command == "publish":
            publish(
                json.loads(args.identity.read_text()),
                args.directory,
                storage,
                source_verifier=(lambda identity: None)
                if args.provider == "local"
                else verify_source,
            )
        else:
            identity = recover(
                storage, args.tag, args.commit, args.ledger_sha256, args.out_dir
            )
            args.identity_output.parent.mkdir(parents=True, exist_ok=True)
            args.identity_output.write_text(
                json.dumps(identity, sort_keys=True, indent=2) + "\n"
            )


if __name__ == "__main__":
    main()
