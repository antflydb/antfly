#!/usr/bin/env python3
"""Contract tests for the small, main-only runtime publication path."""
from pathlib import Path
import unittest


WORKFLOW = (Path(__file__).resolve().parents[2] / ".github/workflows/main-runtime.yml").read_text()


class MainRuntimeWorkflowTest(unittest.TestCase):
    def test_is_main_only_and_does_not_change_release_publication(self):
        self.assertIn("branches: [main]", WORKFLOW)
        self.assertNotIn("workflow_call", WORKFLOW)
        self.assertNotIn("GHCR", WORKFLOW)
        self.assertNotIn("latest", WORKFLOW)

    def test_names_an_immutable_main_commit_and_reuses_it(self):
        self.assertIn('tag="main-${source_sha}"', WORKFLOW)
        self.assertIn('crane digest "${repository}:${tag}"', WORKFLOW)
        self.assertIn("needs.resolve.outputs.exists", WORKFLOW)

    def test_emits_digest_sha_and_platform_identity(self):
        self.assertIn("source_sha:$source_sha", WORKFLOW)
        self.assertIn("image:$image", WORKFLOW)
        self.assertIn('platforms:["linux/amd64","linux/arm64"]', WORKFLOW)


if __name__ == "__main__":
    unittest.main()
