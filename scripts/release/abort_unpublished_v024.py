#!/usr/bin/env python3
"""One-time guarded recovery for the cancelled, unpublished v0.2.4 promotion."""

from __future__ import annotations

import argparse
import os
import urllib.error
import urllib.request

from build_cli_snapshot import NPM_PACKAGES
from create_github_release import get_release_by_tag, github_api
from discover_channel_tag import (
    discover_channel_observations,
    require_channel_observations,
)
from download_objectstorage import S3Reader
from registry.model import RegistryError
from registry.npm import version_integrity
from release_channel_state import S3ChannelStore, abort_promotion, release_identity
from release_channels import load_policy

REPOSITORY = "antflydb/antfly"
RUN_ID = 36062063327
TAG = "v0.2.4"
COMMIT = "2690946d6570a23ec208f789b5408977995febf3"
LEDGER_SHA256 = "dc91489efd51d67bd22e9a88c7f7aef42a7138c5d786ad27cb6ac16472a9480b"
CONTAINER_DIGEST = "sha256:19940271c38cf84ba18039a5c4dfd95ad59e873a7e21f6d0390a37e6294c2531"
PUBLICATION_JOBS = {
    "Publish antfly-cli to PyPI",
    "Publish @antfly/cli to npm",
    "Commit policy-selected mutable channel aliases",
    "Publish policy-selected GitHub release",
    "Commit release channel transaction",
}


def verify_cancelled_run(token: str) -> None:
    run = github_api("GET", REPOSITORY, f"/actions/runs/{RUN_ID}", token)
    if not isinstance(run, dict) or (
        run.get("name") != "Release promotion"
        or run.get("status") != "completed"
        or run.get("conclusion") != "cancelled"
        or run.get("event") != "workflow_run"
        or run.get("head_sha") != "9234abce4836b2c83325954eebee854ee89df2d1"
        or run.get("run_attempt") != 1
    ):
        raise SystemExit("v0.2.4 promotion run is not completed and cancelled")
    jobs = []
    page = 1
    while True:
        response = github_api(
            "GET", REPOSITORY, f"/actions/runs/{RUN_ID}/jobs?per_page=100&page={page}", token
        )
        if not isinstance(response, dict) or not isinstance(response.get("jobs"), list):
            raise SystemExit("v0.2.4 promotion returned malformed jobs")
        batch = response["jobs"]
        if any(not isinstance(job, dict) for job in batch):
            raise SystemExit("v0.2.4 promotion returned malformed jobs")
        jobs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    by_name = {job.get("name"): job for job in jobs}
    if len(by_name) != len(jobs) or not PUBLICATION_JOBS <= by_name.keys():
        raise SystemExit("v0.2.4 promotion has missing or duplicate publication jobs")
    reservation = by_name.get("Reserve complete release identity")
    if not isinstance(reservation, dict) or reservation.get("conclusion") != "success":
        raise SystemExit("v0.2.4 reservation did not succeed in the expected run")
    for name in PUBLICATION_JOBS:
        job = by_name[name]
        if (
            job.get("status") != "completed"
            or job.get("conclusion") != "cancelled"
            or job.get("steps") != []
        ):
            raise SystemExit(f"v0.2.4 publication job may have run: {name}")


def verify_unpublished_registries(token: str) -> None:
    release = get_release_by_tag(REPOSITORY, TAG, token)
    if not isinstance(release, dict) or (
        release.get("tag_name") != TAG
        or release.get("draft") is not True
        or release.get("published_at") is not None
    ):
        raise SystemExit("v0.2.4 GitHub release is not an unpublished draft")
    for package in NPM_PACKAGES:
        try:
            present = version_integrity(package, "0.2.4")
        except RegistryError as exc:
            raise SystemExit(str(exc)) from exc
        if present is not None:
            raise SystemExit(f"v0.2.4 npm package was published: {package}")
    request = urllib.request.Request(
        "https://pypi.org/pypi/antfly-cli/0.2.4/json",
        headers={"User-Agent": "antfly-release-controller"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20):
            raise SystemExit("v0.2.4 PyPI package was published")
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise SystemExit(f"PyPI lookup failed with HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise SystemExit(f"PyPI lookup failed: {exc}") from exc


def verify_no_active_promotions(token: str) -> None:
    page = 1
    while True:
        response = github_api(
            "GET",
            REPOSITORY,
            f"/actions/workflows/antfly-release.yml/runs?per_page=100&page={page}",
            token,
        )
        if not isinstance(response, dict) or not isinstance(
            response.get("workflow_runs"), list
        ):
            raise SystemExit("cannot verify active release promotions")
        batch = response["workflow_runs"]
        if any(not isinstance(run, dict) for run in batch):
            raise SystemExit("cannot verify active release promotions")
        active = [run.get("id") for run in batch if run.get("status") != "completed"]
        if active:
            raise SystemExit(f"release promotions are still active: {active}")
        if len(batch) < 100:
            return
        page += 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--bucket", default="antfly-releases")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    token = os.environ.get("GH_TOKEN", "")
    if not token or os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise SystemExit("recovery requires the Antfly repository and GH_TOKEN")
    policy = load_policy()
    channel_policy = policy["channels"]["stable"]
    identity = release_identity(
        TAG, COMMIT, LEDGER_SHA256, container_digest=CONTAINER_DIGEST
    )
    store = S3ChannelStore(args.endpoint, args.bucket, channel_policy["journal_key"])
    state = store.load().document
    if state.get("pending") != identity or state.get("channel") != "stable":
        raise SystemExit("stable journal does not hold the exact v0.2.4 reservation")
    current = state.get("current")
    if not isinstance(current, dict) or not isinstance(current.get("tag"), str):
        raise SystemExit("stable journal has no valid current release")
    verify_cancelled_run(token)
    verify_unpublished_registries(token)
    verify_no_active_promotions(token)
    observations = discover_channel_observations(
        "stable",
        policy,
        REPOSITORY,
        token,
        S3Reader(args.endpoint, args.bucket, "auto"),
        urllib.request.urlopen,
    )
    require_channel_observations("stable", current["tag"], observations, policy)
    if args.check_only:
        print(f"v0.2.4 abort preflight passed; stable remains {current['tag']}")
    else:
        abort_promotion(store, identity, RUN_ID)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
