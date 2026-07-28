#!/usr/bin/env python3
"""Contract tests for exact-SHA development runtime container builds."""

from pathlib import Path
import re
import os
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/antfly-container.yml"
MANIFEST_BUILD = ROOT / "zig/cloudbuild.manifest.yaml"
DEV_PUBLISH = ROOT / "scripts/publish-zig-runtime-dev.sh"


class MainContainerWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text()
        cls.manifest_build = MANIFEST_BUILD.read_text()

    def test_source_is_authorized_from_trusted_orchestration_before_auth(self) -> None:
        authorization = self.workflow.index("Authorize source and publication inputs")
        auth = self.workflow.index("Authenticate to Google Cloud")
        self.assertLess(authorization, auth)
        self.assertIn("ref: ${{ github.sha }}", self.workflow)
        self.assertIn("merge-base --is-ancestor", self.workflow)
        self.assertIn("release source_sha must equal", self.workflow)
        self.assertIn("artifact_source=github is only supported for release", self.workflow)
        self.assertIn("needs.plan.outputs.source_sha", self.workflow)

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

    def test_gcloud_source_tags_use_the_alternate_dictionary_delimiter(self) -> None:
        self.assertIn("--substitutions=\"^@^_IMAGE_NAME=antfly@", self.workflow)
        self.assertIn("@_SOURCE_TAGS=${SOURCES}\"", self.workflow)

    def test_dev_script_passes_escaped_source_tags_to_gcloud(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            bindir = Path(temp) / "bin"
            bindir.mkdir()
            capture = Path(temp) / "gcloud-args"
            fake_gcloud = bindir / "gcloud"
            fake_gcloud.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" >> \"$GCLOUD_CAPTURE\"\n"
                "if [[ \"$1 $2 $3 $4\" == 'artifacts docker images describe' ]]; then echo sha256:test; fi\n"
            )
            fake_gcloud.chmod(0o755)
            env = os.environ | {"PATH": f"{bindir}:{os.environ['PATH']}", "GCLOUD_CAPTURE": str(capture)}
            completed = subprocess.run([str(DEV_PUBLISH), "--tag", "test", "--manifest"], cwd=ROOT, env=env, text=True, capture_output=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("^@^_IMAGE_NAME=antfly@_VERSION_TAG=test@_ALIAS_TAG=__skip_alias__@_SOURCE_TAGS=test-amd64,test-arm64", capture.read_text())

    def test_gcloud_sdk_accepts_alternate_delimiter_syntax(self) -> None:
        # This intentionally fails only after argument parsing because the
        # source directory does not exist. A bad substitution dictionary fails
        # before that with a substitutions syntax error.
        completed = subprocess.run(
            ["gcloud", "builds", "submit", "/definitely/not/a/source", "--substitutions=^@^_A=one@_B=two,three"],
            text=True,
            capture_output=True,
            env=os.environ | {"CLOUDSDK_CONFIG": tempfile.mkdtemp()},
        )
        self.assertNotIn("substitution", completed.stderr.lower())

    def test_manifest_is_digest_resolved_signed_and_retained(self) -> None:
        self.assertIn('DIGEST="$(crane digest', self.workflow)
        self.assertIn('IMAGE="${REPOSITORY}@${DIGEST}"', self.workflow)
        self.assertIn("cosign verify", self.workflow)
        self.assertIn("retention-days: 14", self.workflow)

    def test_development_authenticates_gar_before_digest_and_never_logs_into_ghcr(self) -> None:
        gar_login = self.workflow.index("Authenticate crane to GAR")
        digest = self.workflow.index('DIGEST="$(crane digest')
        self.assertLess(gar_login, digest)
        mirror = self.workflow.index("Mirror image GAR -> GHCR")
        self.assertIn("publish_mode || 'release'", self.workflow[mirror : mirror + 200])

    def test_final_tag_uses_unique_staging_inputs_and_serialization(self) -> None:
        self.assertIn("STAGING_SUFFIX=\"${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}\"", self.workflow)
        self.assertIn("group: runtime-container-", self.workflow)
        self.assertIn('IMAGE="${{ steps.sign.outputs.image }}"', self.workflow)
        self.assertIn("rerun will not retag it", self.workflow)
        self.assertIn("already exists with different platforms", self.workflow)

    def test_architecture_and_source_matrix_contracts(self) -> None:
        for arch in ("amd64", "arm64", "amd64,arm64"):
            self.assertIn(f"{arch})", self.workflow)
        self.assertIn("for arch in \"${arches[@]}\"", self.workflow)
        self.assertIn("expected exactly one $arch release archive", self.workflow)
        self.assertIn("archive_prefix", self.workflow)
        self.assertIn("_ARTIFACT_SHA256", self.workflow)
        self.assertIn("gsutil stat", self.workflow)

    def test_protected_environment_requires_a_successful_plan(self) -> None:
        publish = self.workflow.index("  publish:")
        window = self.workflow[publish : publish + 1000]
        self.assertIn("needs.plan.result == 'success'", window)
        self.assertIn("container-publish-development", window)
        self.assertIn("GITHUB_REPOSITORY", self.workflow)


if __name__ == "__main__":
    unittest.main()
