# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

import qualify_metal_checkpoint_cycles as runner


class CollectorBinding(unittest.TestCase):
    def test_text_model_selection_allows_a_vision_projector_in_the_same_bundle(self):
        model = {"path": "gemma-4-E2B_q4_0-it.gguf", "sha256": "a" * 64}
        projector = {"path": "mmproj-gemma-4-E2B.gguf", "sha256": "b" * 64}
        self.assertEqual(runner.select_artifact([projector, model], model["path"]), model)

    def test_artifact_selection_rejects_missing_duplicate_and_non_gguf(self):
        model = {"path": "gemma-4-E2B_q4_0-it.gguf", "sha256": "a" * 64}
        for artifacts, path in (([], model["path"]), ([model, model], model["path"]),
                                ([model], "tokenizer.json")):
            with self.subTest(path=path), self.assertRaises(ValueError):
                runner.select_artifact(artifacts, path)

    def test_source_must_match_the_selected_executable_before_model_load(self):
        build = {"head": "a" * 40, "source": {"files": {"a.zig": "b" * 64}}}
        with patch.object(runner, "source_snapshot", return_value=(build["head"], build["source"])), \
                patch.object(runner, "git_provenance", return_value={"identity": "frozen"}):
            self.assertEqual(runner.validate_source_build(Path("/repo"), build), {"identity": "frozen"})
        for head, source in (("c" * 40, build["source"]),
                             (build["head"], {"files": {"a.zig": "d" * 64}})):
            with self.subTest(head=head, source=source), \
                    patch.object(runner, "source_snapshot", return_value=(head, source)), \
                    patch.object(runner, "git_provenance") as provenance:
                with self.assertRaisesRegex(ValueError, "does not cover current source"):
                    runner.validate_source_build(Path("/repo"), build)
                provenance.assert_not_called()

    def test_full_8k_and_32k_trajectories_leave_query_tail(self):
        value = {"schema": "antfly.metal-checkpoint-cycles.input.v1",
                 "model": "google/gemma-4-E2B-it-qat-q4_0-gguf",
                 "expected_prompt_tokens": 8128, "messages": [{"role": "user", "content": "frozen"}],
                 "request": {"encoding": "f16", "cycles": 20, "append_tokens": 512,
                             "answer_tokens": 4}}
        runner.validate_input(value)
        value["expected_prompt_tokens"] = 32096
        value["request"]["append_tokens"] = 4
        runner.validate_input(value)
        value["expected_prompt_tokens"] += 1
        with self.assertRaisesRegex(ValueError, "query-tail reserve"):
            runner.validate_input(value)

    def test_invalid_inputs_fail_before_child_launch(self):
        base = {"schema": "antfly.metal-checkpoint-cycles.input.v1",
                "model": "google/gemma-4-E2B-it-qat-q4_0-gguf",
                "expected_prompt_tokens": 8128, "messages": [{"role": "user", "content": "frozen"}],
                "request": {"encoding": "f16", "cycles": 20, "append_tokens": 512,
                            "answer_tokens": 4}}
        for key, invalid in (("cycles", True), ("cycles", 21), ("cycles", 0),
                             ("append_tokens", 2049), ("answer_tokens", 33),
                             ("encoding", "f32")):
            with self.subTest(key=key):
                value = copy.deepcopy(base); value["request"][key] = invalid
                with self.assertRaises(ValueError): runner.validate_input(value)
        for key, invalid in (("model", "wrong/model"), ("expected_prompt_tokens", True),
                             ("messages", []), ("schema", "old")):
            with self.subTest(key=key):
                value = copy.deepcopy(base); value[key] = invalid
                with self.assertRaises(ValueError): runner.validate_input(value)


if __name__ == "__main__":
    unittest.main()
