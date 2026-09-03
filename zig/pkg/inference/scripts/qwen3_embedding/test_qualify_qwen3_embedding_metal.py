#!/usr/bin/env python3

from __future__ import annotations

import math
from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))

import qualify_qwen3_embedding_metal as qualify
import transformers_embedding_oracle as oracle


def basis_vector(dimensions: int, index: int, scale: float = 1.0) -> list[float]:
    vector = [0.0] * dimensions
    vector[index] = scale
    return vector


def make_case(
    case_id: str,
    role: str,
    instruction: str | None,
    axis: int,
) -> dict[str, object]:
    return {
        "id": case_id,
        "role": role,
        "instruction": instruction,
        "text": f"text for {case_id}",
        "token_ids": [1, 2, oracle.EOS_TOKEN_ID],
        "embeddings": {
            "1024": basis_vector(1024, axis),
            "256": basis_vector(256, axis % 256),
            "32": basis_vector(32, axis % 32),
        },
    }


def make_oracle_payload() -> dict[str, object]:
    cases = [
        make_case("doc_cjk", "document", None, 1),
        make_case("doc_emoji", "document", None, 2),
        make_case("doc_english", "document", None, 0),
        make_case("doc_shared_text", "document", None, 3),
        make_case("query_custom", "query", "Custom capital instruction", 1),
        make_case("query_default", "query", oracle.DEFAULT_INSTRUCTION, 0),
        make_case("query_shared_text", "query", oracle.DEFAULT_INSTRUCTION, 3),
        make_case("query_short", "query", oracle.DEFAULT_INSTRUCTION, 2),
    ]
    return {
        "schema": qualify.ORACLE_SCHEMA,
        "contract": {"default_instruction": oracle.DEFAULT_INSTRUCTION},
        "cases": cases,
    }


class OracleValidationTests(unittest.TestCase):
    def test_valid_fixture_passes_and_indexes_by_id(self) -> None:
        cases = qualify.validate_oracle(make_oracle_payload())
        self.assertEqual(8, len(cases))
        self.assertEqual("document", cases["doc_english"]["role"])

    def test_wrong_schema_fails_closed(self) -> None:
        payload = make_oracle_payload()
        payload["schema"] = "antfly.qwen3_embedding.transformers_oracle.v0"
        with self.assertRaisesRegex(qualify.QualificationError, "unexpected oracle schema"):
            qualify.validate_oracle(payload)

    def test_missing_default_instruction_fails_closed(self) -> None:
        payload = make_oracle_payload()
        payload["contract"] = {}
        with self.assertRaisesRegex(qualify.QualificationError, "default_instruction"):
            qualify.validate_oracle(payload)

    def test_duplicate_case_id_fails_closed(self) -> None:
        payload = make_oracle_payload()
        payload["cases"].append(make_case("doc_cjk", "document", None, 4))
        with self.assertRaisesRegex(qualify.QualificationError, "duplicate oracle case id"):
            qualify.validate_oracle(payload)

    def test_wrong_embedding_width_fails_closed(self) -> None:
        payload = make_oracle_payload()
        payload["cases"][0]["embeddings"]["256"] = [1.0] * 255
        with self.assertRaisesRegex(qualify.QualificationError, "invalid 256-dim embedding"):
            qualify.validate_oracle(payload)

    def test_non_finite_embedding_fails_closed(self) -> None:
        payload = make_oracle_payload()
        payload["cases"][0]["embeddings"]["32"][0] = float("nan")
        with self.assertRaisesRegex(qualify.QualificationError, "invalid 32-dim embedding"):
            qualify.validate_oracle(payload)

    def test_missing_retrieval_matrix_case_fails_closed(self) -> None:
        payload = make_oracle_payload()
        payload["cases"] = [
            case for case in payload["cases"] if case["id"] != "doc_emoji"
        ]
        with self.assertRaisesRegex(qualify.QualificationError, "retrieval matrix"):
            qualify.validate_oracle(payload)


class RequestBodyTests(unittest.TestCase):
    def test_document_sends_raw_text_without_task_type(self) -> None:
        body = qualify.embedding_request_body(
            "qwen3-embedding-0.6b", "hello", "document", None, oracle.DEFAULT_INSTRUCTION
        )
        self.assertEqual({"model": "qwen3-embedding-0.6b", "input": "hello"}, body)

    def test_default_query_sends_task_type_only(self) -> None:
        body = qualify.embedding_request_body(
            "m", "hello", "query", oracle.DEFAULT_INSTRUCTION, oracle.DEFAULT_INSTRUCTION
        )
        self.assertEqual({"model": "m", "input": "hello", "task_type": "RETRIEVAL_QUERY"}, body)

    def test_custom_query_sends_instruction(self) -> None:
        body = qualify.embedding_request_body(
            "m", "hello", "query", "Custom instruction", oracle.DEFAULT_INSTRUCTION
        )
        self.assertEqual(
            {
                "model": "m",
                "input": "hello",
                "task_type": "RETRIEVAL_QUERY",
                "instruction": "Custom instruction",
            },
            body,
        )

    def test_dimensions_and_batch_inputs_are_forwarded(self) -> None:
        body = qualify.embedding_request_body(
            "m", ["a", "b"], "document", None, oracle.DEFAULT_INSTRUCTION, dimensions=256
        )
        self.assertEqual({"model": "m", "input": ["a", "b"], "dimensions": 256}, body)

    def test_unknown_role_fails_closed(self) -> None:
        with self.assertRaisesRegex(qualify.QualificationError, "unknown oracle role"):
            qualify.embedding_request_body("m", "x", "passage", None, oracle.DEFAULT_INSTRUCTION)


class TierGateTests(unittest.TestCase):
    def test_tier_thresholds_are_versioned(self) -> None:
        self.assertEqual(
            {"bf16": 0.999, "f16": 0.999, "q8_0": 0.995, "q4_k": 0.99},
            qualify.TIER_MIN_COSINE,
        )
        self.assertEqual(0.9999, qualify.BATCH_MIN_COSINE)
        self.assertEqual(0.99999, qualify.DIMENSIONS_MIN_COSINE)

    def test_identical_vectors_pass_every_gate(self) -> None:
        vector = basis_vector(1024, 5)
        rows = qualify.case_gates("case", vector, list(vector), qualify.TIER_MIN_COSINE["bf16"])
        self.assertTrue(all(row["pass"] for row in rows))

    def test_tier_selects_the_cosine_threshold(self) -> None:
        angle = math.radians(3.5)  # cosine ~0.99813: passes q8_0/q4_k, fails bf16/f16.
        oracle_vector = basis_vector(1024, 0)
        server_vector = basis_vector(1024, 0)
        server_vector[0] = math.cos(angle)
        server_vector[1] = math.sin(angle)
        strict = qualify.case_gates("case", oracle_vector, server_vector, qualify.TIER_MIN_COSINE["f16"])
        relaxed = qualify.case_gates("case", oracle_vector, server_vector, qualify.TIER_MIN_COSINE["q8_0"])
        self.assertFalse([row for row in strict if row["gate"] == "oracle_cosine"][0]["pass"])
        self.assertTrue([row for row in relaxed if row["gate"] == "oracle_cosine"][0]["pass"])

    def test_scaled_vector_fails_only_the_norm_gate(self) -> None:
        oracle_vector = basis_vector(1024, 0)
        server_vector = basis_vector(1024, 0, scale=2.0)
        rows = {row["gate"]: row for row in qualify.case_gates("case", oracle_vector, server_vector, 0.99)}
        self.assertFalse(rows["unit_norm"]["pass"])
        self.assertTrue(rows["oracle_cosine"]["pass"])

    def test_width_mismatch_short_circuits(self) -> None:
        rows = qualify.case_gates("case", basis_vector(1024, 0), basis_vector(256, 0), 0.99)
        self.assertEqual(1, len(rows))
        self.assertFalse(rows[0]["pass"])

    def test_zero_vector_fails_without_raising(self) -> None:
        rows = qualify.case_gates("case", basis_vector(1024, 0), [0.0] * 1024, 0.99)
        self.assertFalse(all(row["pass"] for row in rows))


class MrlGateTests(unittest.TestCase):
    def test_host_truncate_renormalize_of_server_vector_passes(self) -> None:
        full = [float(index + 1) for index in range(64)]
        reduced = oracle.truncate_and_renormalize(full, 32)
        rows = qualify.mrl_gates("case", full, reduced, 32)
        self.assertTrue(all(row["pass"] for row in rows))

    def test_perturbed_reduction_fails(self) -> None:
        full = [float(index + 1) for index in range(64)]
        reduced = oracle.truncate_and_renormalize(full, 32)
        reduced[0] += 0.05
        rows = {row["gate"]: row for row in qualify.mrl_gates("case", full, reduced, 32)}
        self.assertFalse(rows["mrl_truncate_renormalize"]["pass"])

    def test_wrong_reduced_width_short_circuits(self) -> None:
        full = [float(index + 1) for index in range(64)]
        rows = qualify.mrl_gates("case", full, full[:31], 32)
        self.assertEqual(1, len(rows))
        self.assertFalse(rows[0]["pass"])


class BatchGateTests(unittest.TestCase):
    def test_equal_batch_and_single_vectors_pass(self) -> None:
        vector = basis_vector(1024, 9)
        rows = qualify.batch_gates("case", vector, list(vector))
        self.assertTrue(rows[0]["pass"])

    def test_divergent_batch_vector_fails(self) -> None:
        rows = qualify.batch_gates("case", basis_vector(1024, 9), basis_vector(1024, 10))
        self.assertFalse(rows[0]["pass"])


class RetrievalGateTests(unittest.TestCase):
    def vectors(self) -> dict[str, list[float]]:
        payload = make_oracle_payload()
        return {case["id"]: case["embeddings"]["1024"] for case in payload["cases"]}

    def test_top1_ranks_by_cosine_with_id_tiebreak(self) -> None:
        documents = {"doc_a": basis_vector(8, 0), "doc_b": basis_vector(8, 1)}
        self.assertEqual("doc_b", qualify.retrieval_top1(basis_vector(8, 1), documents))
        # Exact tie falls back to lexicographic document id on both sides.
        self.assertEqual("doc_a", qualify.retrieval_top1(basis_vector(8, 7), documents))

    def test_agreeing_server_passes_all_queries(self) -> None:
        vectors = self.vectors()
        rows = qualify.retrieval_gates(vectors, {key: list(value) for key, value in vectors.items()})
        self.assertEqual(len(qualify.RETRIEVAL_QUERY_CASES), len(rows))
        self.assertTrue(all(row["pass"] for row in rows))

    def test_flipped_server_query_fails_its_gate(self) -> None:
        vectors = self.vectors()
        server = {key: list(value) for key, value in vectors.items()}
        server["query_default"] = basis_vector(1024, 1)  # now retrieves doc_cjk, not doc_english
        rows = {row["case"]: row for row in qualify.retrieval_gates(vectors, server)}
        self.assertFalse(rows["query_default"]["pass"])
        self.assertTrue(rows["query_short"]["pass"])


class SharedTextGateTests(unittest.TestCase):
    def test_distinct_vectors_pass_and_identical_fail(self) -> None:
        query = basis_vector(1024, 0)
        document = basis_vector(1024, 1)
        self.assertTrue(qualify.shared_text_gate(query, document)["pass"])
        self.assertFalse(qualify.shared_text_gate(query, list(query))["pass"])


class TableTests(unittest.TestCase):
    def test_render_table_includes_status_columns(self) -> None:
        rows = [
            qualify.gate("oracle_cosine", "doc_english", True, "cosine=1.000000 min=0.999"),
            qualify.gate("unit_norm", "doc_cjk", False, "|v|=2.000000 tol=0.001"),
        ]
        table = qualify.render_table(rows)
        lines = table.splitlines()
        self.assertEqual(3, len(lines))
        self.assertIn("GATE", lines[0])
        self.assertIn("PASS", lines[1])
        self.assertIn("FAIL", lines[2])


if __name__ == "__main__":
    unittest.main()
