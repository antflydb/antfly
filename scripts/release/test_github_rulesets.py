#!/usr/bin/env python3
"""Tests for the versioned GitHub repository-ruleset contract."""

from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from github_rulesets import load_contract, normalized_ruleset, ruleset_findings


class GitHubRulesetContractTests(unittest.TestCase):
    def test_contract_protects_release_branches_and_version_tags(self) -> None:
        rulesets = load_contract()["rulesets"]
        branches = rulesets["antfly-release-policy-branches"]
        tags = rulesets["antfly-release-policy-tags"]

        self.assertEqual(
            branches["conditions"]["ref_name"]["include"],
            ["refs/heads/main", "refs/heads/v*.x"],
        )
        self.assertEqual(
            {rule["type"] for rule in branches["rules"]},
            {"deletion", "non_fast_forward", "pull_request", "required_status_checks"},
        )
        self.assertEqual(tags["conditions"]["ref_name"]["include"], ["refs/tags/v*"])
        self.assertEqual(
            {rule["type"] for rule in tags["rules"]}, {"deletion", "update"}
        )

    def test_findings_detect_missing_unexpected_and_drifted_rulesets(self) -> None:
        desired = load_contract()["rulesets"]
        names = sorted(desired)
        current = {
            names[0]: {"id": 1, "name": names[0], **copy.deepcopy(desired[names[0]])},
            "antfly-release-policy-obsolete": {"id": 2},
        }
        current[names[0]]["enforcement"] = "disabled"

        self.assertEqual(
            ruleset_findings(desired, current),
            [
                f"missing ruleset: {names[1]}",
                "unexpected managed ruleset: antfly-release-policy-obsolete",
                f"ruleset differs from contract: {names[0]}",
            ],
        )

    def test_normalization_ignores_api_response_metadata(self) -> None:
        desired = load_contract()["rulesets"]["antfly-release-policy-tags"]
        current = {"id": 42, "name": "ignored", **copy.deepcopy(desired)}
        self.assertEqual(normalized_ruleset(current), normalized_ruleset(desired))

    def test_contract_loader_rejects_non_object_document(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "rulesets.json"
            path.write_text("[]\n", encoding="utf-8")
            with self.assertRaisesRegex(SystemExit, "unsupported GitHub ruleset"):
                load_contract(path)


if __name__ == "__main__":
    unittest.main()
