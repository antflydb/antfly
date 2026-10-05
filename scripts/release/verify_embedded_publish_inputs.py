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

"""Authenticate the controller run and commit-bound embedded release request."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

from build_release_payload import verify_release_spec
from release_channels import load_policy
from release_lines import resolve_tag

CONTROLLER_PATH = ".github/workflows/antfly-release-build-controller.yml"


def verify_producer(run: dict, workflow: dict, repository: str, branch: str) -> str:
    if (
        workflow.get("path") != CONTROLLER_PATH
        or type(workflow.get("id")) is not int
        or run.get("workflow_id") != workflow["id"]
        or run.get("path") != CONTROLLER_PATH
        or run.get("event") != "workflow_run"
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
        or run.get("head_branch") != branch
        or (run.get("repository") or {}).get("full_name") != repository
        or (run.get("head_repository") or {}).get("full_name") != repository
    ):
        raise ValueError(
            "artifact producer is not a successful trusted release controller"
        )
    commit = run.get("head_sha")
    if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("release controller has an invalid commit")
    return commit


def git(repository: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repository), *arguments], text=True
    ).strip()


def verify_identity(
    run: dict,
    workflow: dict,
    repository: str,
    branch: str,
    request: Path,
    tag: str,
    root: Path,
) -> dict[str, str]:
    controller = verify_producer(run, workflow, repository, branch)
    line = resolve_tag(tag, allow_closed=True)
    git(root, "fetch", "--no-tags", "origin", f"refs/tags/{tag}:refs/tags/{tag}")
    commit = git(root, "rev-parse", f"{tag}^{{commit}}")
    document = verify_release_spec(request, tag, commit)
    if (
        document.get("schema_version") != 5
        or document.get("build_contract_schema") != 2
        or document.get("build_controller_commit") != controller
        or document.get("release_line") != line.name
        or document.get("source_ref") != line.source_ref
        or document.get("channel") not in {"stable", "next"}
    ):
        raise ValueError(
            "embedded release request differs from its controller or release line"
        )
    git(root, "fetch", "--no-tags", "origin", f"refs/heads/{branch}")
    git(
        root,
        "merge-base",
        "--is-ancestor",
        controller,
        git(root, "rev-parse", "FETCH_HEAD"),
    )
    git(root, "fetch", "--no-tags", "origin", line.source_ref)
    source_head = git(root, "rev-parse", "FETCH_HEAD")
    git(root, "merge-base", "--is-ancestor", document["source_ref_head"], source_head)
    git(root, "merge-base", "--is-ancestor", commit, document["source_ref_head"])
    channel = load_policy()["channels"][document["channel"]]
    return {
        "commit": commit,
        "version": document["version"],
        "python_version": document["registry_versions"]["python"],
        "npm_tag": channel["npm_tag"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("producer", "identity"))
    parser.add_argument("--run-json", type=Path, required=True)
    parser.add_argument("--workflow-json", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--default-branch", required=True)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--tag")
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    run = json.loads(args.run_json.read_text())
    workflow = json.loads(args.workflow_json.read_text())
    if not isinstance(run, dict) or not isinstance(workflow, dict):
        raise SystemExit("invalid release controller metadata")
    if args.command == "producer":
        verify_producer(run, workflow, args.repository, args.default_branch)
    else:
        if args.request is None or args.tag is None:
            parser.error("identity requires --request and --tag")
        outputs = verify_identity(
            run,
            workflow,
            args.repository,
            args.default_branch,
            args.request,
            args.tag,
            args.repo_root,
        )
        if args.github_output:
            with args.github_output.open("a") as stream:
                for name, value in outputs.items():
                    stream.write(f"{name}={value}\n")
        print(json.dumps(outputs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
