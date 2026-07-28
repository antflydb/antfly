#!/usr/bin/env python3
"""Contract tests for the small, main-only runtime publication path."""
import json
from pathlib import Path
import unittest


WORKFLOW = (Path(__file__).resolve().parents[2] / ".github/workflows/main-runtime.yml").read_text()
RUNTIME_BUILD = (Path(__file__).resolve().parents[2] / "zig/cloudbuild.runtime.yaml").read_text()
FIXTURES = Path(__file__).with_name("testdata")


class MainRuntimeWorkflowTest(unittest.TestCase):
    def test_is_main_only_and_does_not_change_release_publication(self):
        self.assertIn("branches: [main]", WORKFLOW)
        self.assertNotIn("workflow_call", WORKFLOW)
        self.assertNotIn("GHCR", WORKFLOW)
        self.assertNotIn("latest", WORKFLOW)

    def test_names_an_immutable_main_commit_and_reuses_it(self):
        self.assertIn('tag="main-${source_sha}"', WORKFLOW)
        self.assertIn("--image-ref", WORKFLOW)
        self.assertIn('crane digest "$image_tag"', WORKFLOW)
        self.assertIn("needs.resolve.outputs.exists", WORKFLOW)
        self.assertIn("MANIFEST_UNKNOWN", WORKFLOW)
        self.assertIn("group: main-runtime-main", WORKFLOW)
        self.assertIn("immutable image conflicts", WORKFLOW)

    def test_emits_digest_sha_and_platform_identity(self):
        self.assertIn("source_sha:$source_sha", WORKFLOW)
        self.assertIn("image:$image", WORKFLOW)
        self.assertIn('platforms:["linux/amd64","linux/arm64"]', WORKFLOW)

    def test_does_not_duplicate_private_infrastructure_configuration(self):
        self.assertNotIn("antfly-image-artifacts", WORKFLOW)
        self.assertNotIn("us-central1", WORKFLOW)
        self.assertNotIn("workerPools", WORKFLOW)

    def test_cache_miss_installs_the_pinned_zig_toolchain(self):
        self.assertIn("ZIG_VERSION: 0.16.0", WORKFLOW)
        self.assertIn("zig-${zig_arch}-linux-${ZIG_VERSION}.tar.xz", WORKFLOW)
        self.assertIn('zig" version | grep -Fx "$ZIG_VERSION"', WORKFLOW)

    def test_trigger_covers_the_helper_and_packaged_antfarm_assets(self):
        self.assertIn("scripts/publish-zig-runtime-dev.sh", WORKFLOW)
        self.assertIn("go/pkg/antfly/src/metadata/antfarm/**", WORKFLOW)

    def test_provenance_fixture_uses_runnable_children_and_receipt_final_digest(self):
        fixture = json.loads((FIXTURES / "runtime-manifest-with-attestation.json").read_text())
        runnable = {
            item["platform"]["architecture"]: item["digest"]
            for item in fixture["final"]["manifests"]
            if item["platform"].get("os") == "linux"
        }
        self.assertEqual(runnable, fixture["expected_runnable_children"])
        self.assertEqual(fixture["receipt"]["image"].split("@", 1)[1], fixture["final_digest"])
        self.assertIn("runnable_child()", WORKFLOW)
        self.assertIn("receipt digest does not equal final tag", WORKFLOW)
        self.assertIn("--provenance=true", RUNTIME_BUILD)

    def test_overlapping_delayed_events_share_one_stable_receipt(self):
        fixture = json.loads((FIXTURES / "runtime-overlap.json").read_text())
        self.assertEqual({run["image"] for run in fixture["runs"]}, {fixture["expected_image"]})
        self.assertEqual({run["receipt_digest"] for run in fixture["runs"]}, {fixture["expected_digest"]})


if __name__ == "__main__":
    unittest.main()
