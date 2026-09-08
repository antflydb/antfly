#!/usr/bin/env python3
"""Verify explicit corpus setup without downloading external fixtures."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("fetch-conformance-fixtures.sh")


class ConformanceSetupTest(unittest.TestCase):
    def test_all_and_selected_suites(self):
        for suite, fetches in (("all", 5), ("image", 2), ("toon", 1), ("audio", 2)):
            with self.subTest(suite=suite), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                (root / "scripts").mkdir()
                tools = root / "zig/zig-out/bin"
                tools.mkdir(parents=True)
                script = root / "scripts/fetch.sh"
                shutil.copyfile(SCRIPT, script)
                names = [
                    "zig",
                    "lib-toon-conformance",
                    "lib-image-conformance-fetch",
                    "image-jpeg-seed-corpora-e2e",
                    "lib-audio-xiph-conformance",
                    "lib-audio-misc-conformance",
                ]
                for name in names:
                    stub = tools / name
                    stub.write_text(
                        '#!/bin/sh\nprintf "%s" "${0##*/}" >> "$CALL_LOG"\nprintf "|%s" "$@" >> "$CALL_LOG"\nprintf "\\n" >> "$CALL_LOG"\n'
                    )
                    stub.chmod(0o755)
                log = root / "calls.txt"
                env = {
                    **os.environ,
                    "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                    "CALL_LOG": str(log),
                }
                dest = root / "fixture space"
                subprocess.run(
                    ["bash", str(script), "--suite", suite, "--dest", str(dest)],
                    env=env,
                    check=True,
                )
                calls = log.read_text().splitlines()
                self.assertEqual(calls[0], "zig|build|conformance-tools")
                self.assertEqual(len(calls), fetches + 1)
                for call in calls[1:]:
                    self.assertIn("|fetch|" + str(dest) + "/", call)

    def test_invalid_options_fail_before_setup(self):
        for args in (["--suite", "unknown"], ["--dest", "relative"], ["--dest"]):
            result = subprocess.run(["bash", str(SCRIPT), *args], capture_output=True)
            self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
