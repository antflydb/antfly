#!/usr/bin/env python3
"""Tests for the repository-wide workflow security policy."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from validate_workflow_actions import validate


PINNED_CHECKOUT = "actions/checkout@" + "a" * 40


class WorkflowActionPolicyTests(unittest.TestCase):
    def write(self, root: Path, name: str, body: str) -> None:
        (root / name).write_text(body, encoding="utf-8")

    def test_every_workflow_requires_full_sha(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "ordinary-ci.yml",
                "permissions:\n  contents: read\njobs:\n  x:\n    steps:\n      - uses: actions/checkout@v6\n",
            )
            with self.assertRaisesRegex(SystemExit, "full commit SHAs"):
                validate(root)

    def test_secret_bearing_workflow_requires_full_sha(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "future-publisher.yml",
                "permissions: {}\njobs:\n  x:\n    steps:\n      - uses: vendor/publish@v1\n        with:\n          token: ${{ secrets.PUBLISH_TOKEN }}\n",
            )
            with self.assertRaisesRegex(SystemExit, "vendor/publish@v1"):
                validate(root)

    def test_workflow_permissions_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "ordinary-ci.yaml",
                f"jobs:\n  x:\n    steps:\n      - uses: {PINNED_CHECKOUT}\n",
            )
            with self.assertRaisesRegex(SystemExit, "missing top-level permissions"):
                validate(root)

    def test_job_permissions_do_not_replace_workflow_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "publisher.yml",
                f"jobs:\n  x:\n    permissions:\n      contents: write\n    steps:\n      - uses: {PINNED_CHECKOUT}\n",
            )
            with self.assertRaisesRegex(SystemExit, "missing top-level permissions"):
                validate(root)

    def test_workflow_baseline_must_be_read_only(self) -> None:
        for name, permissions in (
            ("write-all.yml", "permissions: write-all"),
            ("inline-write.yml", "permissions: {contents: write}"),
            ("block-write.yml", "permissions:\n  packages: write"),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                self.write(
                    root,
                    name,
                    f"{permissions}\njobs:\n  x:\n    steps:\n      - uses: {PINNED_CHECKOUT}\n",
                )
                with self.assertRaisesRegex(
                    SystemExit, "top-level permissions must be read-only"
                ):
                    validate(root)

    def test_workflow_permissions_cannot_be_empty(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "ordinary-ci.yml",
                f"permissions:\njobs:\n  x:\n    steps:\n      - uses: {PINNED_CHECKOUT}\n",
            )
            with self.assertRaisesRegex(SystemExit, "permissions declaration is empty"):
                validate(root)

    def test_pinned_and_local_reusable_workflows_are_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "antfly-release.yml",
                f"permissions:\n  contents: read\njobs:\n  x:\n    steps:\n      - uses: {PINNED_CHECKOUT}\n  y:\n    uses: ./.github/workflows/build.yml\n",
            )
            self.write(
                root,
                "build.yml",
                "permissions: {}\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n",
            )
            self.assertEqual(
                validate(root), [root / "antfly-release.yml", root / "build.yml"]
            )

    def test_inline_workflow_permissions_are_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write(
                root,
                "ordinary-ci.yml",
                f"permissions: {{contents: read}}\njobs:\n  x:\n    steps:\n      - uses: {PINNED_CHECKOUT}\n",
            )
            self.assertEqual(validate(root), [root / "ordinary-ci.yml"])


if __name__ == "__main__":
    unittest.main()
