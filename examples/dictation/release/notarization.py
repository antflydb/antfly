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

"""Bind a resumable Apple submission to its signed archive and release identity."""

import argparse
import json
import os
import re
import uuid
from pathlib import Path

import release


def producer(run_id):
    if not re.fullmatch(r"[1-9][0-9]*", str(run_id)):
        raise ValueError("Invalid producer run ID")
    run = release.gh_api(f"repos/{release.POLICY['repository']}/actions/runs/{run_id}")
    if (
        run.get("path") != ".github/workflows/dictation-release.yml"
        or run.get("event") != "workflow_run"
        or run.get("head_branch") != os.environ.get("DEFAULT_BRANCH", "main")
        or run.get("status") != "completed"
        or not release.SHA.fullmatch(run.get("head_sha", ""))
    ):
        raise ValueError(
            "Recovery requires a completed default-branch Dictation release"
        )
    jobs = release.gh_api(
        f"repos/{release.POLICY['repository']}/actions/runs/{run_id}/jobs?per_page=100"
    )
    required = {
        "Developer ID sign and submit to Apple",
        "Preserve signed notarization submission",
    }
    if not any(
        job.get("name") == "sign"
        and required.issubset(
            {
                step["name"]
                for step in job.get("steps", [])
                if step.get("conclusion") == "success"
            }
        )
        for job in jobs.get("jobs", [])
    ):
        raise ValueError("Producer did not preserve an approved signed submission")
    return run


def resume(run_id, identity):
    original = producer(run_id)
    if original["head_sha"] != identity.get("controller_commit"):
        raise ValueError("Identity differs from the original controller")
    release.verify_source(identity)
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        for name in ("source_commit", "version", "tag"):
            output.write(f"{name}={identity[name]}\n")


def record(directory, identity):
    directory = Path(directory)
    submission = json.loads((directory / "submission.json").read_text())
    submission_id = str(uuid.UUID(submission["id"]))
    release.validate_identity(identity)
    result = {
        "schema": 1,
        "identity": identity,
        "submission_id": submission_id,
        "archive_sha256": release.digest(directory / "signed.zip"),
    }
    (directory / "pending.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    return result


def validate(directory, identity):
    directory = Path(directory)
    pending = json.loads((directory / "pending.json").read_text())
    release.validate_identity(identity)
    if (
        pending.get("schema") != 1
        or pending.get("identity") != identity
        or pending.get("archive_sha256") != release.digest(directory / "signed.zip")
    ):
        raise ValueError(
            "Notarization artifact does not match its release identity or digest"
        )
    submission = str(uuid.UUID(pending["submission_id"]))
    if submission != pending["submission_id"]:
        raise ValueError("Noncanonical Apple submission ID")
    return submission


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["producer", "record", "validate", "resume"])
    parser.add_argument("target")
    parser.add_argument("identity", nargs="?")
    args = parser.parse_args()
    if args.command == "producer":
        producer(args.target)
    else:
        identity = json.loads(Path(args.identity).read_text())
        if args.command == "resume":
            resume(args.target, identity)
        elif args.command == "record":
            record(args.target, identity)
        else:
            print(validate(args.target, identity))
