# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Ensure transport qualification cannot pass partial or mismatching observations."""

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "check_image_owner_transport",
    Path(__file__).with_name("check_image_owner_transport.py"),
)
assert SPEC and SPEC.loader
transport = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transport)


def complete_log():
    lines = []
    for mode in transport.MODES:
        lines.extend(
            f"IMAGE_FIXTURE mode={mode} path=image/{index}.png hash={index:064x}"
            for index in range(transport.MIN_FIXTURES)
        )
        lines.extend(
            f"IMAGE_VP8 mode={mode} fixture={name} packets=8 bytes=1024"
            for name in sorted(transport.REQUIRED_VP8)
        )
    lines.append("Build Summary: 26/26 steps succeeded")
    return "\n".join(lines) + "\n"


class ImageTransportObservations(unittest.TestCase):
    def test_complete_matching_reference_checks(self):
        result = transport.analyze(complete_log())
        self.assertTrue(result["passed"], result["errors"])
        self.assertEqual(result["fixture_counts"], {"source": 30, "native": 30})
        self.assertEqual(result["vp8_counts"], {"source": 4, "native": 4})

    def test_image_byte_mismatch(self):
        source = complete_log()
        source = source.replace(
            f"mode=native path=image/0.png hash={0:064x}",
            f"mode=native path=image/0.png hash={1:064x}",
        )
        self.assertIn(
            "Image pixels differ: image/0.png", transport.analyze(source)["errors"]
        )

    def test_missing_counterpart_and_insufficient_samples(self):
        source = complete_log().replace(
            f"IMAGE_FIXTURE mode=native path=image/0.png hash={0:064x}\n", ""
        )
        errors = transport.analyze(source)["errors"]
        self.assertIn("Missing source/native image counterpart: image/0.png", errors)
        self.assertIn("native has 29 image fixtures; need 30", errors)

    def test_duplicate_observations_rejected_even_when_equal(self):
        source = complete_log()
        for duplicate in (
            f"IMAGE_FIXTURE mode=source path=image/0.png hash={0:064x}",
            "IMAGE_VP8 mode=native fixture=vp8-inter packets=8 bytes=1024",
        ):
            with self.subTest(duplicate=duplicate):
                result = transport.analyze(source + duplicate + "\n")
                self.assertFalse(result["passed"])
                self.assertTrue(any("Duplicate" in error for error in result["errors"]))

    def test_incomplete_or_failed_build_cannot_pass_matching_pixels(self):
        source = complete_log()
        for summary in (
            "",
            "Build Summary: 25/26 steps succeeded (1 failed)",
            "Build Summary: 26/26 steps succeeded\nFAIL (TestUnexpectedResult)",
            "Build Summary: 26/26 steps succeeded\nBuild Summary: 26/26 steps succeeded",
        ):
            with self.subTest(summary=summary):
                output = source.replace("Build Summary: 26/26 steps succeeded", summary)
                self.assertFalse(transport.analyze(output)["passed"])

    def test_vp8_sequence_coverage_and_checked_byte_counts(self):
        source = complete_log()
        for before, after in (
            (
                "mode=native fixture=vp8-inter packets=8 bytes=1024",
                "mode=native fixture=vp8-inter packets=8 bytes=1023",
            ),
            ("IMAGE_VP8 mode=native fixture=vp8-inter packets=8 bytes=1024\n", ""),
            (
                "mode=native fixture=vp8-inter packets=8 bytes=1024",
                "mode=native fixture=vp8-inter packets=0 bytes=0",
            ),
        ):
            with self.subTest(after=after):
                self.assertFalse(
                    transport.analyze(source.replace(before, after))["passed"]
                )

    def test_malformed_marker_is_not_silently_ignored(self):
        for line in (
            "IMAGE_FIXTURE mode=unrecognized path=image.png hash=abc",
            "IMAGE_FIXTURE mode=native path=image.png hash=bad",
            "IMAGE_VP8 mode=source fixture=vp8-inter packets=-1 bytes=5",
        ):
            with self.subTest(line=line):
                self.assertFalse(transport.analyze(complete_log() + line)["passed"])

    def test_timeout_preserves_partial_output_and_stops_child(self):
        parent = transport.ZIG / ".zig-cache/tool-fixtures"
        parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as directory:
            log = Path(directory) / "partial.log"
            result = transport.run_build(
                [
                    sys.executable,
                    "-c",
                    "import time; print('partial qualification', flush=True); time.sleep(30)",
                ],
                log,
                timeout=0.5,
            )
            self.assertEqual(result["interruption"], "TimeoutExpired")
            self.assertNotEqual(result["returncode"], 0)
            self.assertIn("partial qualification", log.read_text())


if __name__ == "__main__":
    unittest.main()
