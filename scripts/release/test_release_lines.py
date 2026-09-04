"""Tests for controller-owned release-line policy."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_lines


class ReleaseLinePolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = release_lines.load_policy()

    def test_active_line_resolves_from_canonical_tags(self) -> None:
        line = release_lines.resolve_tag("v0.2.1-rc.4", self.policy)
        self.assertEqual((line.name, line.source_ref), ("0.2", "refs/heads/main"))
        self.assertEqual(line.trusted_source_refs, ("refs/heads/main",))

    def test_tag_cannot_select_a_different_release_line(self) -> None:
        with self.assertRaisesRegex(SystemExit, "no trusted release line"):
            release_lines.resolve_tag("v0.3.0-rc.1", self.policy)

    def test_noncanonical_and_nightly_tags_cannot_select_release_lines(self) -> None:
        for tag in ("v0.2.1-rc4", "v0.0.0-dev.12"):
            with (
                self.subTest(tag=tag),
                self.assertRaisesRegex(SystemExit, "canonical non-nightly"),
            ):
                release_lines.resolve_tag(tag, self.policy)

    def test_closed_line_is_recovery_only(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["lines"]["0.2"]["status"] = "closed"
        release_lines.validate_policy(policy)
        with self.assertRaisesRegex(SystemExit, "closed"):
            release_lines.resolve_tag("v0.2.1", policy)
        release_lines.validate_provenance(
            "v0.2.1",
            "0.2",
            "refs/heads/main",
            policy,
            allow_closed=True,
        )

    def test_source_handoff_preserves_historical_provenance(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["lines"]["0.2"]["source_ref"] = "refs/heads/v0.2.x"
        policy["lines"]["0.2"]["trusted_source_refs"].append("refs/heads/v0.2.x")
        release_lines.validate_policy(policy)

        current = release_lines.resolve_tag("v0.2.2", policy)
        self.assertEqual(current.source_ref, "refs/heads/v0.2.x")
        release_lines.validate_provenance("v0.2.1", "0.2", "refs/heads/main", policy)
        release_lines.validate_provenance("v0.2.2", "0.2", "refs/heads/v0.2.x", policy)

    def test_provenance_rejects_wrong_line_or_untrusted_source(self) -> None:
        for line, source_ref in (
            ("0.3", "refs/heads/main"),
            ("0.2", "refs/heads/v0.2.x"),
        ):
            with (
                self.subTest(line=line, source_ref=source_ref),
                self.assertRaisesRegex(SystemExit, "invalid release-line provenance"),
            ):
                release_lines.validate_provenance(
                    "v0.2.1", line, source_ref, self.policy
                )

    def test_policy_rejects_arbitrary_or_cross_line_source_refs(self) -> None:
        for source_ref in (
            "refs/heads/feature/release",
            "refs/heads/v0.3.x",
            "refs/tags/v0.2.1",
            "main",
        ):
            policy = copy.deepcopy(self.policy)
            policy["lines"]["0.2"]["source_ref"] = source_ref
            with self.subTest(source_ref=source_ref), self.assertRaises(SystemExit):
                release_lines.validate_policy(policy)

    def test_policy_rejects_invalid_or_incomplete_trusted_source_history(self) -> None:
        for trusted_source_refs in (
            [],
            ["refs/heads/v0.2.x"],
            ["refs/heads/main", "refs/heads/main"],
            ["refs/heads/main", "refs/heads/v0.3.x"],
            ["refs/heads/main", "refs/heads/feature"],
        ):
            policy = copy.deepcopy(self.policy)
            policy["lines"]["0.2"]["trusted_source_refs"] = trusted_source_refs
            with (
                self.subTest(trusted_source_refs=trusted_source_refs),
                self.assertRaises(SystemExit),
            ):
                release_lines.validate_policy(policy)

    def test_nightly_is_always_main(self) -> None:
        self.assertEqual(
            release_lines.nightly_line(self.policy).source_ref,
            "refs/heads/main",
        )
        policy = copy.deepcopy(self.policy)
        policy["nightly_source_ref"] = "refs/heads/v0.2.x"
        with self.assertRaisesRegex(SystemExit, "nightly"):
            release_lines.validate_policy(policy)


if __name__ == "__main__":
    unittest.main()
