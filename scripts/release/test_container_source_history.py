"""Execute the container workflow's actual guards against divergent Git branches."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/antfly-container.yml"


def step_script(name: str) -> str:
    step = WORKFLOW.read_text().split(f"      - name: {name}\n", 1)[1]
    return textwrap.dedent(
        step.split("        run: |\n", 1)[1].split("\n      - ", 1)[0]
    )


class ContainerSourceHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.origin = self.root / "origin"
        self.origin.mkdir()
        self.git(self.origin, "init", "-b", "main")
        self.git(self.origin, "config", "user.name", "Release test")
        self.git(self.origin, "config", "user.email", "release-test@example.invalid")
        self.git(
            self.origin,
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "main",
        )
        self.main = self.git(self.origin, "rev-parse", "HEAD")
        self.git(self.origin, "checkout", "-b", "v0.2.x")
        self.git(
            self.origin,
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "backport",
        )
        self.maintenance = self.git(self.origin, "rev-parse", "HEAD")
        self.git(self.origin, "checkout", "-b", "feature", self.main)
        self.git(
            self.origin,
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "untrusted",
        )
        self.foreign = self.git(self.origin, "rev-parse", "HEAD")
        self.git(self.origin, "checkout", "main")
        self.checkout = self.root / "checkout"
        self.git(self.root, "clone", str(self.origin), str(self.checkout))
        (self.checkout / "scripts").symlink_to(
            ROOT / "scripts", target_is_directory=True
        )
        binaries = self.root / "bin"
        binaries.mkdir()
        (binaries / "python").symlink_to(sys.executable)
        self.env = dict(
            os.environ,
            PATH=f"{binaries}{os.pathsep}{os.environ['PATH']}",
            GITHUB_EVENT_NAME="workflow_run",
            RELEASE_TAG="v0.2.2-rc.2",
            RELEASE_COMMIT=self.maintenance,
            PROMOTION_CONTROLLER_COMMIT=self.main,
            DEFAULT_BRANCH="main",
        )
        archives = self.checkout / "dist/release-archives"
        archives.mkdir(parents=True)
        self.archive = archives / "runtime.tar.gz"
        self.archive.write_bytes(b"fixture runtime archive")
        self.ledger_path = self.checkout / "dist/release-ledger/artifacts.json"
        self.ledger_path.parent.mkdir(parents=True)
        self.ledger = {
            "schema_version": 5,
            "tag": self.env["RELEASE_TAG"],
            "commit": self.maintenance,
            "build_controller_commit": self.main,
            "promotion_controller_commit": self.main,
            "release_line": "0.2",
            "source_ref": "refs/heads/v0.2.x",
            "source_ref_head": self.maintenance,
            "artifacts": [
                {
                    "name": self.archive.name,
                    "scope": "runtime",
                    "size": self.archive.stat().st_size,
                    "sha256": hashlib.sha256(self.archive.read_bytes()).hexdigest(),
                }
            ],
        }

    def git(self, cwd: Path, *args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
        ).stdout.strip()

    def run_guard(self, success: bool, *, corrupt_digest: bool = False) -> None:
        self.ledger_path.write_text(json.dumps(self.ledger))
        self.env["LEDGER_SHA256"] = hashlib.sha256(
            self.ledger_path.read_bytes()
        ).hexdigest()
        if corrupt_digest:
            self.env["LEDGER_SHA256"] = "0" * 64
        script = step_script("Revalidate trusted caller and release identity") + "\n"
        script += step_script(
            "Verify container inputs and recorded release-source history"
        )
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=self.checkout,
            env=self.env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)

    def test_maintenance_source_not_on_main_passes_for_build_and_recovery(self) -> None:
        unrelated = subprocess.run(
            ["git", "merge-base", "--is-ancestor", self.maintenance, self.main],
            cwd=self.checkout,
            capture_output=True,
        )
        self.assertEqual(unrelated.returncode, 1)
        for event in ("workflow_run", "repository_dispatch"):
            with self.subTest(event=event):
                self.env["GITHUB_EVENT_NAME"] = event
                self.run_guard(True)

    def test_main_source_and_historical_main_provenance_pass(self) -> None:
        self.ledger.update(
            commit=self.main, source_ref="refs/heads/main", source_ref_head=self.main
        )
        self.env["RELEASE_COMMIT"] = self.main
        self.run_guard(True)
        self.ledger.update(tag="v0.3.0-rc.1", release_line="0.3")
        self.env["RELEASE_TAG"] = self.ledger["tag"]
        self.run_guard(True)

    def test_legacy_ledgers_remain_main_only(self) -> None:
        for schema in (1, 2, 3, 4):
            with self.subTest(schema=schema):
                self.ledger["schema_version"] = schema
                self.ledger["commit"] = self.main
                self.env["RELEASE_COMMIT"] = self.main
                self.run_guard(True)
                self.ledger["commit"] = self.maintenance
                self.env["RELEASE_COMMIT"] = self.maintenance
                self.run_guard(False)

    def test_nightly_remains_main_only(self) -> None:
        self.env.update(RELEASE_TAG="v0.0.0-dev.123", RELEASE_COMMIT=self.main)
        self.ledger.update(
            tag=self.env["RELEASE_TAG"],
            commit=self.main,
            release_line="nightly",
            source_ref="refs/heads/main",
            source_ref_head=self.main,
        )
        self.run_guard(True)
        self.ledger.update(
            source_ref="refs/heads/v0.2.x", source_ref_head=self.maintenance
        )
        self.run_guard(False)

    def test_wrong_release_line_is_rejected(self) -> None:
        self.ledger["release_line"] = "0.3"
        self.run_guard(False)

    def test_untrusted_source_ref_is_rejected(self) -> None:
        self.ledger.update(
            source_ref="refs/heads/feature", source_ref_head=self.foreign
        )
        self.run_guard(False)

    def test_source_outside_recorded_head_is_rejected(self) -> None:
        self.ledger["source_ref_head"] = self.main
        self.run_guard(False)

    def test_rewritten_source_history_is_rejected(self) -> None:
        self.git(self.origin, "branch", "-f", "v0.2.x", self.foreign)
        self.run_guard(False)

    def test_off_main_controller_is_rejected(self) -> None:
        self.git(self.checkout, "checkout", "--detach", self.maintenance)
        self.env["PROMOTION_CONTROLLER_COMMIT"] = self.maintenance
        self.run_guard(False)

    def test_untrusted_event_is_rejected(self) -> None:
        self.env["GITHUB_EVENT_NAME"] = "workflow_dispatch"
        self.run_guard(False)

    def test_ledger_digest_mismatch_is_rejected(self) -> None:
        self.run_guard(False, corrupt_digest=True)

    def test_runtime_artifact_tampering_is_rejected(self) -> None:
        self.archive.write_bytes(b"changed bytes")
        self.run_guard(False)

    def test_ledger_identity_mismatch_is_rejected(self) -> None:
        self.ledger["commit"] = self.foreign
        self.run_guard(False)

    def test_verified_ledger_is_transferred_before_cloud_authentication(self) -> None:
        workflow = WORKFLOW.read_text()
        publisher = (ROOT / ".github/workflows/antfly-release.yml").read_text()
        self.assertIn(
            "name: antfly-release-ledger\n          path: dist/release-ledger/",
            publisher,
        )
        self.assertIn(
            "name: antfly-release-ledger\n          path: dist/release-ledger", workflow
        )
        self.assertLess(
            workflow.index(
                "name: Verify container inputs and recorded release-source history"
            ),
            workflow.index("name: Authenticate to Google Cloud"),
        )


if __name__ == "__main__":
    unittest.main()
