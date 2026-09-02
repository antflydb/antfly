#!/usr/bin/env python3
"""Tests for release workflow action-reference policy."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from validate_workflow_actions import validate


PINNED_CHECKOUT = "actions/checkout@" + "a" * 40


class WorkflowActionPolicyTests(unittest.TestCase):
    def write(self, root: Path, name: str, body: str) -> None:
        (root / name).write_text(body, encoding="utf-8")

    def test_release_control_plane_requires_full_sha(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(root, "antfly-release.yml", "jobs:\n  x:\n    steps:\n      - uses: actions/checkout@v6\n")
            with self.assertRaisesRegex(SystemExit, "full commit SHAs"):
                validate(root)

    def test_privileged_publish_workflow_is_selected_automatically(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "future-publisher.yml",
                "permissions:\n  id-token: write\njobs:\n  x:\n    steps:\n      - uses: vendor/publish@v1\n",
            )
            with self.assertRaisesRegex(SystemExit, "vendor/publish@v1"):
                validate(root)

    def test_write_all_yaml_workflow_is_selected_automatically(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "future-publisher.yaml",
                "permissions: write-all\njobs:\n  x:\n    steps:\n      - uses: vendor/publish@v1\n",
            )
            with self.assertRaisesRegex(SystemExit, "vendor/publish@v1"):
                validate(root)

    def test_inline_write_permission_is_selected_automatically(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "future-publisher.yml",
                "permissions: {contents: write}\njobs:\n  x:\n    steps:\n      - uses: vendor/publish@v1\n",
            )
            with self.assertRaisesRegex(SystemExit, "vendor/publish@v1"):
                validate(root)

    def test_every_github_write_scope_is_privileged(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "future-automation.yml",
                "permissions:\n  pull-requests: write\njobs:\n  x:\n    steps:\n      - uses: vendor/automation@v1\n",
            )
            with self.assertRaisesRegex(SystemExit, "vendor/automation@v1"):
                validate(root)

    def test_pinned_and_local_reusable_workflows_are_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "antfly-release.yml",
                f"jobs:\n  x:\n    steps:\n      - uses: {PINNED_CHECKOUT}\n  y:\n    uses: ./.github/workflows/build.yml\n",
            )
            self.assertEqual(validate(root), [root / "antfly-release.yml"])

    def test_local_reusable_workflow_inherits_policy(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "antfly-release.yml",
                "jobs:\n  build:\n    uses: ./.github/workflows/build.yml\n",
            )
            self.write(
                root,
                "build.yml",
                "jobs:\n  x:\n    steps:\n      - uses: vendor/build@v1\n",
            )
            with self.assertRaisesRegex(SystemExit, "vendor/build@v1"):
                validate(root)

    def test_unprivileged_nonrelease_workflow_is_out_of_scope(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(root, "ordinary-ci.yml", "jobs:\n  x:\n    steps:\n      - uses: actions/checkout@v6\n")
            self.assertEqual(validate(root), [])


if __name__ == "__main__":
    unittest.main()
