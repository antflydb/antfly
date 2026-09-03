#!/usr/bin/env python3
"""Tests for the versioned GitHub release-environment contract."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from github_environment import environment_findings, load_contract

REVIEWERS = {("User", 1), ("Team", 2)}
DESIRED = {
    "reviewers_from": "npm",
    "prevent_self_review": True,
    "wait_timer": 0,
    "deployment_branch_policy": {
        "protected_branches": False,
        "custom_branch_policies": True,
    },
    "branch_policies": [
        {"name": "main", "type": "branch"},
        {"name": "v*", "type": "tag"},
    ],
}


def environment(*, reviewers: bool = True) -> dict[str, object]:
    rules: list[dict[str, object]] = [{"type": "branch_policy"}]
    if reviewers:
        rules.insert(
            0,
            {
                "type": "required_reviewers",
                "prevent_self_review": True,
                "reviewers": [
                    {"type": kind, "reviewer": {"id": reviewer_id}}
                    for kind, reviewer_id in sorted(REVIEWERS)
                ],
            },
        )
    return {
        "deployment_branch_policy": DESIRED["deployment_branch_policy"],
        "protection_rules": rules,
    }


def policies(*, main: bool = True) -> list[dict[str, object]]:
    result: list[dict[str, object]] = [{"id": 2, "name": "v*", "type": "tag"}]
    if main:
        result.insert(0, {"id": 1, "name": "main", "type": "branch"})
    return result


class GitHubEnvironmentContractTests(unittest.TestCase):
    def test_matching_environment_has_no_findings(self) -> None:
        self.assertEqual(
            environment_findings(
                "container-publish",
                DESIRED,
                environment(),
                policies(),
                REVIEWERS,
            ),
            [],
        )

    def test_missing_main_and_reviewers_are_both_reported(self) -> None:
        findings = environment_findings(
            "container-publish",
            DESIRED,
            environment(reviewers=False),
            policies(main=False),
            REVIEWERS,
        )
        self.assertIn("required-reviewer protection is missing", findings)
        self.assertIn("missing branch policies: [('main', 'branch')]", findings)

    def test_reviewer_drift_is_reported(self) -> None:
        findings = environment_findings(
            "container-publish",
            DESIRED,
            environment(),
            policies(),
            {("User", 99)},
        )
        self.assertIn("required reviewers differ from the source environment", findings)

    def test_contract_loader_rejects_non_object_document(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "contract.json"
            path.write_text("[]\n", encoding="utf-8")
            with self.assertRaisesRegex(
                SystemExit, "unsupported GitHub environment contract"
            ):
                load_contract(path)


if __name__ == "__main__":
    unittest.main()
