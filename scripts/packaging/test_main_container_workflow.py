#!/usr/bin/env python3
"""Contract tests for exact-SHA development runtime container builds."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/antfly-container.yml"
MANIFEST_BUILD = ROOT / "zig/cloudbuild.manifest.yaml"


class MainContainerWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text()
        cls.manifest_build = MANIFEST_BUILD.read_text()

    def test_exact_source_sha_is_used_for_both_checkouts_and_artifacts(self) -> None:
        self.assertGreaterEqual(
            self.workflow.count("ref: ${{ env.SOURCE_SHA }}"), 2
        )
        self.assertIn("zig/${SOURCE_SHA}/antfly-zig-", self.workflow)
        self.assertIn('[[ "$(git rev-parse HEAD)" == "$SOURCE_SHA" ]]', self.workflow)

    def test_development_mode_is_gar_only_and_never_updates_latest(self) -> None:
        self.assertRegex(
            self.workflow,
            re.compile(
                r"Mirror image GAR -> GHCR\n\s+if:.*publish_mode.*release",
                re.MULTILINE,
            ),
        )
        self.assertIn(
            'if [[ "${{ inputs.publish_mode || \'release\' }}" == development',
            self.workflow,
        )
        self.assertIn("ALIAS_TAG=__skip_alias__", self.workflow)

    def test_manifest_supports_only_selected_architectures(self) -> None:
        self.assertIn("_SOURCE_TAGS", self.manifest_build)
        self.assertIn('test "$${#sources[@]}" -gt 0', self.manifest_build)
        self.assertNotIn("_AMD64_TAG:", self.manifest_build)
        self.assertNotIn("_ARM64_TAG:", self.manifest_build)

    def test_manifest_is_digest_resolved_signed_and_retained(self) -> None:
        self.assertIn('DIGEST="$(crane digest', self.workflow)
        self.assertIn('IMAGE="${REPOSITORY}@${DIGEST}"', self.workflow)
        self.assertIn("cosign verify", self.workflow)
        self.assertIn("retention-days: 14", self.workflow)


if __name__ == "__main__":
    unittest.main()
