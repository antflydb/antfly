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

from release import POLICY, gh_api, public_key


def gh(*args, data=None):
    subprocess.run(["gh", *args], input=data, check=True)


def environment():
    repo = POLICY["repository"]
    name = "dictation-release"
    user = gh_api("user")
    branch = gh_api(f"repos/{repo}")["default_branch"]
    existing = gh_api(f"repos/{repo}/environments")["environments"]
    if any(e["name"] == name for e in existing):
        # Do not silently replace an organization's existing approval policy.
        info = gh_api(f"repos/{repo}/environments/{name}")
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
    account = "antflydb-dictation"
    subprocess.run([str(tools / "generate_keys"), "--account", account], check=True)
    key = subprocess.check_output(
        [str(tools / "generate_keys"), "--account", account, "-p"], text=True
    ).strip()
    public_key(key)
    password = getpass.getpass("Exported .p12 password (not stored locally): ")

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
                [str(tools / "generate_keys"), "--account", account, "-x", str(seed)],
                check=True,
            )
            secret("DICTATION_SPARKLE_PRIVATE_KEY", seed.read_bytes())
        secret(
            "DICTATION_CERTIFICATE_P12_BASE64",
            base64.b64encode(args.certificate.read_bytes()),
        )
        secret("DICTATION_CERTIFICATE_PASSWORD", password.encode())
        secret("DICTATION_APPLE_API_KEY_P8", args.api_key.read_bytes())
        secret("DICTATION_APPLE_API_KEY_ID", args.api_key_id.encode())
        secret("DICTATION_APPLE_API_ISSUER_ID", args.issuer_id.encode())
    finally:
        os.umask(old_umask)
    # The public key is repository-scoped so the unprivileged build can embed it.
    gh(
        "variable",
        "set",
        "DICTATION_SPARKLE_PUBLIC_KEY",
        "--repo",
        repo,
        data=key.encode(),
    )
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
