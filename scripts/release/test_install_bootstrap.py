"""Tests for the stable, release-independent installer bootstrap."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = REPO_ROOT / "scripts" / "release" / "install_bootstrap.sh"


class InstallBootstrapTests(unittest.TestCase):
    def run_bootstrap(self, argument: str | None) -> str:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "latest").mkdir()
            (root / "v1.2.3").mkdir()
            (root / "latest" / "metadata.json").write_text(
                '{"tag":"v1.2.3"}\n', encoding="utf-8"
            )
            (root / "v1.2.3" / "install.sh").write_text(
                'printf "%s\\n" "$*" > "$BOOTSTRAP_RESULT"\n', encoding="utf-8"
            )
            result = root / "result"
            env = {
                **os.environ,
                "ANTFLY_RELEASES_URL": root.as_uri(),
                "BOOTSTRAP_RESULT": str(result),
            }
            command = ["sh", str(BOOTSTRAP)]
            if argument is not None:
                command.append(argument)
            subprocess.run(command, check=True, env=env)
            return result.read_text(encoding="utf-8").strip()

    def test_default_resolves_once_and_delegates_to_immutable_installer(self) -> None:
        self.assertEqual(self.run_bootstrap(None), "v1.2.3")

    def test_help_is_delegated_without_rewriting_the_argument(self) -> None:
        self.assertEqual(self.run_bootstrap("--help"), "--help")


if __name__ == "__main__":
    unittest.main()
