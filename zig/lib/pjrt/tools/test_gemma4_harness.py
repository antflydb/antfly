# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Regression for fresh-process suite paths and validation isolation."""

import copy
import unittest
from pathlib import Path

from benchmark_gemma4 import native_suite, validate_suite


class SuiteTests(unittest.TestCase):
    def test_same_length_prompt_edits_cannot_change_only_one_arm(self):
        suite = {
            "cases": [
                {
                    "id": "text",
                    "kind": "text",
                    "text": "hello",
                    "max_tokens": 64,
                    "expected_prompt_tokens": 32,
                }
            ]
        }
        manifest = copy.deepcopy(suite)
        self.assertEqual(validate_suite(suite, manifest), suite["cases"])
        suite["cases"][0]["text"] = "world"
        with self.assertRaises(ValueError):
            validate_suite(suite, manifest)

    def test_repeated_rounds_resolve_same_fixture_without_mutating_source(self):
        base = Path("/fixtures")
        source = {"cases": [{"id": "video", "path": "clip.mp4", "max_tokens": 64}]}
        original = copy.deepcopy(source)
        for round_index in range(3):
            out = Path(f"/campaign/r{round_index}-native")
            timing = native_suite(source, base, out, 3, False)
            validation = native_suite(source, base, out, 1, True)
            for suite in (timing, validation):
                self.assertEqual(
                    (out / suite["cases"][0]["path"]).resolve(), base / "clip.mp4"
                )
            self.assertEqual(timing["cases"][0]["max_tokens"], 64)
            self.assertNotIn("logits_path", timing["cases"][0])
            self.assertEqual(validation["cases"][0]["max_tokens"], 1)
            self.assertEqual(
                out / validation["cases"][0]["logits_path"], out / "video.f32"
            )
        self.assertEqual(source, original)

    def test_explicit_media_markers_match_structured_template_rendering(self):
        for kind in ("image", "audio"):
            source = {
                "cases": [
                    {
                        "id": kind,
                        "kind": kind,
                        "text": "Describe this.",
                        "max_tokens": 64,
                    }
                ]
            }
            result = native_suite(source, Path("/fixtures"), Path("/out"), 1, False)
            self.assertEqual(result["cases"][0]["text"], f"<|{kind}|>Describe this.")
            self.assertEqual(source["cases"][0]["text"], "Describe this.")


if __name__ == "__main__":
    unittest.main()
