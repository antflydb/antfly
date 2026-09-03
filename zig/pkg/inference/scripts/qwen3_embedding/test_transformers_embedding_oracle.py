#!/usr/bin/env python3

from __future__ import annotations

import math
from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import transformers_embedding_oracle as oracle


class QueryFormattingTests(unittest.TestCase):
    def test_query_template_has_no_space_after_query_colon(self) -> None:
        self.assertEqual(
            "Instruct: Do the thing\nQuery:red planet",
            oracle.format_query("red planet", "Do the thing"),
        )

    def test_default_instruction_matches_model_card(self) -> None:
        self.assertEqual(
            "Given a web search query, retrieve relevant passages that answer the query",
            oracle.DEFAULT_INSTRUCTION,
        )

    def test_query_case_renders_with_instruction(self) -> None:
        case = {"id": "q", "role": "query", "instruction": "Find it", "text": "hello"}
        self.assertEqual("Instruct: Find it\nQuery:hello", oracle.render_case_text(case))

    def test_document_case_is_raw_text(self) -> None:
        case = {"id": "d", "role": "document", "instruction": None, "text": "hello"}
        self.assertEqual("hello", oracle.render_case_text(case))

    def test_document_with_instruction_fails_closed(self) -> None:
        case = {"id": "d", "role": "document", "instruction": "nope", "text": "hello"}
        with self.assertRaisesRegex(oracle.OracleError, "must not carry an instruction"):
            oracle.render_case_text(case)

    def test_unknown_role_fails_closed(self) -> None:
        case = {"id": "x", "role": "passage", "instruction": None, "text": "hello"}
        with self.assertRaisesRegex(oracle.OracleError, "unknown oracle role"):
            oracle.render_case_text(case)


class EosVerificationTests(unittest.TestCase):
    def test_single_trailing_eos_passes(self) -> None:
        oracle.verify_single_trailing_eos([10, 20, oracle.EOS_TOKEN_ID])

    def test_missing_trailing_eos_fails(self) -> None:
        with self.assertRaisesRegex(oracle.OracleError, "does not end with EOS"):
            oracle.verify_single_trailing_eos([oracle.EOS_TOKEN_ID, 20])

    def test_duplicate_eos_fails(self) -> None:
        with self.assertRaisesRegex(oracle.OracleError, "exactly one EOS"):
            oracle.verify_single_trailing_eos([10, oracle.EOS_TOKEN_ID, oracle.EOS_TOKEN_ID])

    def test_empty_sequence_fails(self) -> None:
        with self.assertRaisesRegex(oracle.OracleError, "empty sequence"):
            oracle.verify_single_trailing_eos([])


class MatryoshkaTests(unittest.TestCase):
    def test_truncation_renormalizes_to_unit_length(self) -> None:
        vector = [float(index + 1) for index in range(64)]
        reduced = oracle.truncate_and_renormalize(vector, 32)
        self.assertEqual(32, len(reduced))
        self.assertAlmostEqual(1.0, math.sqrt(sum(value * value for value in reduced)), places=12)

    def test_truncation_preserves_prefix_direction(self) -> None:
        vector = [3.0, 4.0] + [0.0] * 62
        reduced = oracle.truncate_and_renormalize(vector, 32)
        self.assertAlmostEqual(0.6, reduced[0], places=12)
        self.assertAlmostEqual(0.8, reduced[1], places=12)
        self.assertEqual([0.0] * 30, reduced[2:])

    def test_full_width_truncation_is_identity_direction(self) -> None:
        vector = [1.0] + [0.0] * 63
        self.assertEqual(vector, oracle.truncate_and_renormalize(vector, 64))

    def test_out_of_range_dimensions_fail_closed(self) -> None:
        vector = [1.0] * 64
        with self.assertRaisesRegex(oracle.OracleError, "MRL dimensions"):
            oracle.truncate_and_renormalize(vector, 16)
        with self.assertRaisesRegex(oracle.OracleError, "MRL dimensions"):
            oracle.truncate_and_renormalize(vector, 65)

    def test_zero_prefix_fails_closed(self) -> None:
        vector = [0.0] * 32 + [1.0] * 32
        with self.assertRaisesRegex(oracle.OracleError, "degenerate"):
            oracle.truncate_and_renormalize(vector, 32)


class CosineTests(unittest.TestCase):
    def test_identical_and_orthogonal(self) -> None:
        left = [1.0, 0.0, 0.0]
        self.assertAlmostEqual(1.0, oracle.cosine(left, left), places=12)
        self.assertAlmostEqual(0.0, oracle.cosine(left, [0.0, 1.0, 0.0]), places=12)

    def test_length_mismatch_fails_closed(self) -> None:
        with self.assertRaisesRegex(oracle.OracleError, "equal non-empty"):
            oracle.cosine([1.0], [1.0, 2.0])

    def test_zero_vector_fails_closed(self) -> None:
        with self.assertRaisesRegex(oracle.OracleError, "zero vectors"):
            oracle.cosine([0.0, 0.0], [1.0, 0.0])


class FloatFormattingTests(unittest.TestCase):
    def test_fixed_decimal_formatting_is_deterministic(self) -> None:
        values = [1.0 / 3.0, -2.0 / 7.0, 0.0]
        formatted = oracle.format_floats(values)
        self.assertEqual([0.33333333, -0.28571429, 0.0], formatted)
        self.assertEqual(formatted, oracle.format_floats(values))


class PromptSetTests(unittest.TestCase):
    def test_prompt_set_is_fixed_sorted_and_unique(self) -> None:
        cases = oracle.prompt_cases()
        ids = [case["id"] for case in cases]
        self.assertEqual(sorted(ids), ids)
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(11, len(cases))

    def test_prompt_set_covers_required_cases(self) -> None:
        cases = {case["id"]: case for case in oracle.prompt_cases()}
        self.assertLessEqual(
            {
                "doc_cjk",
                "doc_emoji",
                "doc_english",
                "doc_long_truncation",
                "doc_shared_text",
                "doc_single_token",
                "doc_whitespace",
                "query_custom",
                "query_default",
                "query_shared_text",
                "query_short",
            },
            set(cases),
        )
        # Three primary queries (default/custom/short) plus the shared-text query.
        self.assertEqual(4, sum(1 for case in cases.values() if case["role"] == "query"))
        self.assertEqual(7, sum(1 for case in cases.values() if case["role"] == "document"))

    def test_roles_and_instructions_are_consistent(self) -> None:
        for case in oracle.prompt_cases():
            if case["role"] == "document":
                self.assertIsNone(case["instruction"], case["id"])
            else:
                self.assertEqual("query", case["role"], case["id"])
                self.assertIsInstance(case["instruction"], str, case["id"])

    def test_shared_text_appears_as_both_roles(self) -> None:
        cases = {case["id"]: case for case in oracle.prompt_cases()}
        self.assertEqual(cases["doc_shared_text"]["text"], cases["query_shared_text"]["text"])
        self.assertEqual("document", cases["doc_shared_text"]["role"])
        self.assertEqual("query", cases["query_shared_text"]["role"])
        self.assertEqual(
            oracle.DEFAULT_INSTRUCTION, cases["query_shared_text"]["instruction"]
        )

    def test_custom_instruction_query_differs_from_default(self) -> None:
        cases = {case["id"]: case for case in oracle.prompt_cases()}
        self.assertNotEqual(oracle.DEFAULT_INSTRUCTION, cases["query_custom"]["instruction"])
        self.assertEqual(oracle.DEFAULT_INSTRUCTION, cases["query_default"]["instruction"])

    def test_short_query_has_two_words(self) -> None:
        cases = {case["id"]: case for case in oracle.prompt_cases()}
        self.assertEqual(2, len(cases["query_short"]["text"].split()))

    def test_long_case_is_deterministic_repetition(self) -> None:
        cases = {case["id"]: case for case in oracle.prompt_cases()}
        long_case = cases["doc_long_truncation"]
        self.assertEqual(oracle.LONG_SENTENCE * oracle.LONG_REPEATS, long_case["text"])
        self.assertTrue(long_case["expect_truncated"])
        # ~13 tokens per sentence * 900 repeats comfortably exceeds max_length=8192.
        self.assertGreater(len(long_case["text"]), 8 * oracle.DEFAULT_MAX_LENGTH // 2)

    def test_single_token_case_expects_text_plus_eos(self) -> None:
        cases = {case["id"]: case for case in oracle.prompt_cases()}
        self.assertEqual(2, cases["doc_single_token"]["expect_token_count"])

    def test_prompt_cases_are_freshly_built_per_call(self) -> None:
        first = oracle.prompt_cases()
        first[0]["text"] = "mutated"
        self.assertNotEqual("mutated", oracle.prompt_cases()[0]["text"])


class ContractConstantTests(unittest.TestCase):
    def test_schema_and_model_pins(self) -> None:
        self.assertEqual("antfly.qwen3_embedding.transformers_oracle.v1", oracle.SCHEMA)
        self.assertEqual("Qwen/Qwen3-Embedding-0.6B", oracle.MODEL_ID)
        self.assertEqual("97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3", oracle.DEFAULT_REVISION)
        self.assertEqual(151_643, oracle.EOS_TOKEN_ID)
        self.assertEqual(1_024, oracle.HIDDEN_SIZE)
        self.assertEqual(28, oracle.NUM_HIDDEN_LAYERS)
        self.assertEqual((1_024, 256, 32), oracle.OUTPUT_DIMS)
        self.assertEqual(8_192, oracle.DEFAULT_MAX_LENGTH)


if __name__ == "__main__":
    unittest.main()
