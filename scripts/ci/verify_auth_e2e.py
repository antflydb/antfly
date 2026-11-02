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

"""Require same-commit main Zig E2E qualification before publishing Better Auth."""

import argparse
import json
import re
import subprocess


def qualified_runs(runs, commit, repository):
    return [
        run
        for run in runs
        if (
            run.get("head_sha") == commit
            and run.get("head_branch") == "main"
            and run.get("event") in {"push", "schedule", "workflow_dispatch"}
            and run.get("path") == ".github/workflows/zig-tests.yml"
            and run.get("status") == "completed"
            and run.get("conclusion") == "success"
            and (run.get("repository") or {}).get("full_name") == repository
            and (run.get("head_repository") or {}).get("full_name") == repository
        )
    ]


def has_auth_e2e(jobs):
    required = {"e2e-full", "better-auth / compatibility (supported)"}
    passed = {
        job["name"]
        for job in jobs
        if (job.get("status") == "completed" and job.get("conclusion") == "success")
    }
    return required <= passed


def api_pages(path, field):
    pages = json.loads(
        subprocess.check_output(["gh", "api", "--paginate", "--slurp", path], text=True)
    )
    return [item for page in pages for item in page[field]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository):
        parser.error("invalid repository")
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        parser.error("expected a full commit SHA")
    root = f"repos/{args.repository}/actions"
    runs = api_pages(
        f"{root}/workflows/zig-tests.yml/runs?head_sha={args.commit}&status=completed&per_page=100",
        "workflow_runs",
    )
    for run in qualified_runs(runs, args.commit, args.repository):
        # Check only the latest attempt; a superseded attempt cannot qualify it.
        jobs = api_pages(
            f"{root}/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100",
            "jobs",
        )
        if has_auth_e2e(jobs):
            print(
                f"Better Auth and full E2E qualified {args.commit}: {run['html_url']}"
            )
            return
    raise SystemExit(
        "No successful main Zig E2E run with Better Auth coverage for this exact commit. "
        "Complete Zig Tests on the commit before publishing; no native rebuild is started."
    )


if __name__ == "__main__":
    main()
