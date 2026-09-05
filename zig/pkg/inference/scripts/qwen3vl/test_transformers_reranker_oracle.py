#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import transformers_reranker_oracle as oracle


class RerankerOracleContractTests(unittest.TestCase):
    def test_expected_prompt_matches_native_contract(self) -> None:
        self.assertEqual(
            "<|im_start|>system\n"
            + oracle.SYSTEM_PROMPT
            + "<|im_end|>\n"
            + "<|im_start|>user\n<Instruct>: Retrieve text.<Query>:red planet\n"
            + "<Document>:Mars is red.<|im_end|>\n"
            + "<|im_start|>assistant\n",
            oracle.render_expected_prompt(
                "Retrieve text.",
                "red planet",
                "Mars is red.",
            ),
        )

    def test_upstream_truncation_preserves_specials_and_suffix(self) -> None:
        ids = [10, 101, 11, 12, 102, 13, 14, 15, 201, 202, 203, 204, 205]
        self.assertEqual(
            [10, 101, 11, 12, 102, 13, 14, 15, 201, 202, 203, 204, 205],
            oracle.truncate_upstream(ids, 8, {101, 102, 201, 202, 203, 204, 205}),
        )

    def test_multimodal_messages_place_image_before_document_text(self) -> None:
        generic = oracle.generic_messages("Retrieve.", "invoice", "page text", True)
        user_content = generic[1]["content"]
        self.assertEqual(
            ["text", "text", "text", "image", "text"],
            [part["type"] for part in user_content],
        )
        dedicated = oracle.reranker_messages("Retrieve.", "invoice", "page text", True)
        self.assertEqual(
            ["image", "text"],
            [part["type"] for part in dedicated[2]["content"]],
        )
        self.assertIn(
            oracle.IMAGE_MARKER + "page text",
            oracle.render_expected_prompt(
                "Retrieve.", "invoice", oracle.IMAGE_MARKER + "page text"
            ),
        )

    def test_short_sequences_are_not_rewritten(self) -> None:
        ids = [1, 2, 3]
        self.assertIs(ids, oracle.truncate_upstream(ids, 8, {1}))


if __name__ == "__main__":
    unittest.main()
