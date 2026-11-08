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

"""Configure the protected release environment; optionally upload local signing credentials."""

from __future__ import annotations

import argparse
import base64
import getpass
import os
import re
import subprocess
import tempfile
from pathlib import Path

from release import GitHubError, POLICY, gh_api, public_key


def gh(*args, data=None):
    subprocess.run(["gh", *args], input=data, check=True)


KEY_VARIABLE = "DICTATION_SPARKLE_PUBLIC_KEY"
KEY_ACCOUNT = "antflydb-dictation"


def existing_signing_key():
    """Missing configuration is bootstrap only if no release identity exists."""
    repo = POLICY["repository"]
    try:
        return public_key(
            gh_api(f"repos/{repo}/actions/variables/{KEY_VARIABLE}")["value"]
        )
    except GitHubError as error:
        if error.status != 404:
            raise
    refs = gh_api(f"repos/{repo}/git/matching-refs/heads/{POLICY['feed_branch']}")
    if any(ref["ref"] == "refs/heads/" + POLICY["feed_branch"] for ref in refs):
        raise ValueError(
            "Signing configuration is missing for an existing update feed. Restore its original public key; do not bootstrap a replacement."
        )
    page = 1
    while True:
        releases = gh_api(f"repos/{repo}/releases?per_page=100&page={page}")
        if any(release["tag_name"].startswith("dictation/v") for release in releases):
            raise ValueError(
                "Signing configuration is missing for an existing dictation release. Restore its original public key."
            )
        if len(releases) < 100:
            return None
        page += 1


def derive_public_key(package, seed):
    decoded = base64.b64decode(seed.strip(), validate=True)
    if len(decoded) != 32:
        raise ValueError("Restore a modern 32-byte Sparkle private seed.")
    return public_key(
        subprocess.check_output(
            [
                "swift",
                "-module-cache-path",
                str(package / ".build/modules"),
                str(package / "release/public-key.swift"),
            ],
            input=base64.b64encode(decoded) + b"\n",
        )
        .decode()
        .strip()
    )


def prepare_signing_key(package, backup=None):
    expected = existing_signing_key()
    generator = str(package / ".build/updater-tools/bin/generate_keys")
    if backup is not None:
        candidate = derive_public_key(package, backup.read_bytes())
        if expected is not None and candidate != expected:
            raise ValueError(
                "The backup does not match the deployed signing key. No credentials were changed. Key rotation requires a separately reviewed transition."
            )
        try:
            local = public_key(
                subprocess.check_output(
                    [generator, "--account", KEY_ACCOUNT, "-p"], text=True
                ).strip()
            )
        except subprocess.CalledProcessError:
            local = None
        if local is not None and local != candidate:
            raise ValueError(
                "This Mac has a different antflydb-dictation Keychain entry. Back it up and remove that conflicting entry before restoring the verified original key."
            )
        if local is None:
            subprocess.run(
                [generator, "--account", KEY_ACCOUNT, "-f", str(backup)], check=True
            )
    elif expected is None:
        subprocess.run([generator, "--account", KEY_ACCOUNT], check=True)
    try:
        key = public_key(
            subprocess.check_output(
                [generator, "--account", KEY_ACCOUNT, "-p"], text=True
            ).strip()
        )
    except subprocess.CalledProcessError as error:
        raise ValueError(
            "Restore the existing Sparkle key from its secure backup with --sparkle-key. Setup will not generate a replacement for deployed clients."
        ) from error
    if backup is not None and key != candidate:
        raise ValueError(
            "Imported key does not match the verified backup. No credentials were changed."
        )
    if expected is not None and key != expected:
        raise ValueError(
            "This Mac's Sparkle key differs from the deployed key. Restore the original key; no credentials were changed."
        )
    # Reserve the first identity before uploading any private material. POST is
    # create-only, so a concurrent initializer cannot overwrite another key.
    current = existing_signing_key()
    if current is None:
        gh_api(
            f"repos/{POLICY['repository']}/actions/variables",
            "POST",
            {"name": KEY_VARIABLE, "value": key},
        )
    elif current != key:
        raise ValueError(
            "The signing identity changed during setup. No credentials were uploaded."
        )
    return key


def environment():
    repo = POLICY["repository"]
    name = "dictation-release"
    user = gh_api("user")
    branch = gh_api(f"repos/{repo}")["default_branch"]
    try:
        info = gh_api(f"repos/{repo}/environments/{name}")
    except GitHubError as error:
        if error.status != 404:
            raise
        info = None
    if info is not None:
        # Do not silently replace an organization's existing approval policy.
        reviewers = [
            r
            for r in info.get("protection_rules", [])
            if r["type"] == "required_reviewers"
        ]
        policy = info.get("deployment_branch_policy") or {}
        if not reviewers or not policy.get("custom_branch_policies"):
            raise ValueError(
                "Existing dictation-release environment needs review approval and custom branch protection"
            )
    else:
        gh_api(
            f"repos/{repo}/environments/{name}",
            "PUT",
            {
                "wait_timer": 0,
                "prevent_self_review": False,
                "can_admins_bypass": False,
                "reviewers": [{"type": "User", "id": user["id"]}],
                "deployment_branch_policy": {
                    "protected_branches": False,
                    "custom_branch_policies": True,
                },
            },
        )
    policies = gh_api(f"repos/{repo}/environments/{name}/deployment-branch-policies")[
        "branch_policies"
    ]
    if not policies:
        gh_api(
            f"repos/{repo}/environments/{name}/deployment-branch-policies",
            "POST",
            {"name": branch, "type": "branch"},
        )
    elif any(
        p["name"] != branch or p.get("type", "branch") != "branch" for p in policies
    ):
        raise ValueError(
            "Release environment permits refs outside the default branch; review its rules"
        )
    print(
        f"Protected environment ready: {repo}/{name}; reviewer {user['login']}; branch {branch}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment-only", action="store_true")
    parser.add_argument(
        "--certificate",
        type=Path,
        help="Developer ID Application .p12 with private key",
    )
    parser.add_argument(
        "--api-key", type=Path, help="App Store Connect API private .p8 key"
    )
    parser.add_argument(
        "--sparkle-key",
        type=Path,
        help="Restore the original Sparkle private seed from a secure backup; never rotate it implicitly",
    )
    parser.add_argument("--api-key-id")
    parser.add_argument("--issuer-id")
    parser.add_argument("--team-id")
    parser.add_argument(
        "--identity", help="Full Developer ID Application certificate common name"
    )
    args = parser.parse_args()
    if not args.environment_only:
        if not all(
            [
                args.certificate,
                args.api_key,
                args.api_key_id,
                args.issuer_id,
                args.team_id,
                args.identity,
            ]
        ):
            parser.error(
                "Supply all certificate/API/team/identity arguments, or --environment-only"
            )
        if not args.identity.startswith(
            "Developer ID Application: "
        ) or not re.fullmatch(r"[A-Z0-9]{10}", args.team_id):
            parser.error(
                "Use a Developer ID Application identity and its 10-character organization Team ID"
            )
        if not args.certificate.is_file() or not args.api_key.is_file():
            parser.error("Credential files must exist")
    environment()
    if args.environment_only:
        return
    repo = POLICY["repository"]
    package = Path(__file__).resolve().parents[1]
    subprocess.run(["bash", str(package / "release/fetch-tools.sh")], check=True)
    tools = package / ".build/updater-tools/bin"
    key = prepare_signing_key(package, args.sparkle_key)
    password = getpass.getpass("Exported .p12 password (not stored locally): ")
    if not password:
        raise ValueError("Use a password-protected certificate export.")
    certificate = base64.b64encode(args.certificate.read_bytes())
    api_key = args.api_key.read_bytes()

    def secret(name, data):
        gh(
            "secret",
            "set",
            name,
            "--repo",
            repo,
            "--env",
            "dictation-release",
            data=data,
        )

    # Modern Sparkle private seeds are exported only to a protected temporary file.
    # Keep the Keychain entry and back it up securely before the first release.
    old_umask = os.umask(0o077)
    try:
        with tempfile.TemporaryDirectory(
            dir=package / ".build", prefix="signing-setup-"
        ) as d:
            seed = Path(d) / "sparkle-key"
            subprocess.run(
                [
                    str(tools / "generate_keys"),
                    "--account",
                    KEY_ACCOUNT,
                    "-x",
                    str(seed),
                ],
                check=True,
            )
            private_seed = seed.read_bytes()
            if (
                derive_public_key(package, private_seed) != key
                or existing_signing_key() != key
            ):
                raise ValueError(
                    "Signing identity changed before upload. No credentials were uploaded."
                )
            secret("DICTATION_SPARKLE_PRIVATE_KEY", private_seed)
        secret(
            "DICTATION_CERTIFICATE_P12_BASE64",
            certificate,
        )
        secret("DICTATION_CERTIFICATE_PASSWORD", password.encode())
        secret("DICTATION_APPLE_API_KEY_P8", api_key)
        secret("DICTATION_APPLE_API_KEY_ID", args.api_key_id.encode())
        secret("DICTATION_APPLE_API_ISSUER_ID", args.issuer_id.encode())
    finally:
        os.umask(old_umask)
    for name, value in {
        "DICTATION_SIGNING_IDENTITY": args.identity,
        "DICTATION_APPLE_TEAM_ID": args.team_id,
    }.items():
        gh(
            "variable",
            "set",
            name,
            "--repo",
            repo,
            "--env",
            "dictation-release",
            data=value.encode(),
        )
    print("Signing configuration uploaded. No release was tagged or published.")


if __name__ == "__main__":
    main()
