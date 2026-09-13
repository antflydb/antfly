from __future__ import annotations

import copy
import hashlib
import re
import tempfile
from pathlib import Path
import unittest

import oracle
import prepare_benchmark_scaling_v1 as generator
import scaling_contract_v1 as contract
import scaling_runtime_v1 as runtime
from test_metal_runtime_v2 import admitted, response


def splitter(text):
    for match in re.finditer(r"\w+|[^\w\s]", text):
        yield match.group().lower(), match.start(), match.end()


def tokenize(word):
    # A multi-token period catches the real preparation edge case without
    # importing a tokenizer or adjusting any model/output expectation.
    return [3, 4] if word == "." else [10 + sum(map(ord, word)) % 1000]


def fixture():
    originals = oracle.read_json(oracle.FIXTURES / "requests.json")["requests"]
    evidence = {"validation": {row["id"]: {"input_ids": [7, 8] +
        generator.body_ids(row["text"], splitter, tokenize)} for row in originals}}
    return generator.generate_variant("small", evidence, splitter, tokenize)


class ScalingContractTests(unittest.TestCase):
    def test_complete_separate_matrix_preserves_original_sentences_and_padding(self):
        native, source = fixture()
        self.assertEqual(43, len(native["cases"]))
        self.assertEqual(129, 3 * len(contract.case_specs()))
        self.assertTrue(all("expected" not in row and "outputs" not in row for row in native["cases"]))
        self.assertTrue(all(set(row) == {"id", "schema", "items", "expected_encoded_lengths", "encoded_width"}
                            for row in native["cases"]))
        cases = contract.validate_pair(native, source, "small", hashlib.sha256(contract.encoded(source)).hexdigest())
        self.assertTrue(all(len(set(map(len, row["expected_input_ids"]))) > 1
                            for row in cases.values() if row["profile"]["mode"] == "ragged"))
        self.assertEqual(8, len(cases["mixed_tasks_s128_b8_smoke"]["items"]))

    def test_original_prefix_requires_actual_observed_suffix(self):
        evidence = {"validation": {name: {"input_ids": [1, 2, 3]}
                    for name in (*contract.REGULAR, *contract.RAGGED)}}
        with self.assertRaisesRegex(oracle.ContractError, "original source token suffix"):
            generator.generate_variant("small", evidence, splitter, tokenize)

    def test_first_sentence_remains_exact_when_fill_is_smaller_than_second_sentence(self):
        text, ids = generator.expand("İpek is here.", [99], 12, splitter, tokenize)
        self.assertIn("İpek is here.", text)
        self.assertEqual(12, len(ids))
        self.assertTrue(text.endswith("."))

    def test_schema_order_model_identity_and_missing_shardlike_case_fail_closed(self):
        mutations = [
            lambda native, source: native.update(revision="0" * 40),
            lambda native, source: native["cases"].pop(),
            lambda native, source: native["cases"][0]["schema"]["entities"].reverse(),
            lambda native, source: source["cases"][1]["items"].reverse(),
            lambda native, source: native["cases"][0].update(encoded_width=True),
            lambda native, source: source["cases"][0]["expected_input_ids"][0].append(9),
        ]
        for mutate in mutations:
            native, source = fixture()
            mutate(native, source)
            digest = hashlib.sha256(contract.encoded(source)).hexdigest()
            native["requests_sha256"] = digest
            with self.subTest(mutation=mutate), self.assertRaises((oracle.ContractError, runtime.cpu.BenchmarkError)):
                contract.validate_pair(native, source, "small", digest)

    def test_true_batch_packet_rejects_serial_tokens_transpose_bool_and_padding_tamper(self):
        sequences = [[4, 5, 6], [7]]
        expected = contract.packet(sequences)
        self.assertEqual([4, 5, 6, 7, 0, 0], expected["input_ids"])
        self.assertEqual([1, 1, 1, 1, 0, 0], expected["attention_mask"])
        for key, value in (("input_ids", [4, 5, 6]), ("input_shape", [3, 2]),
                           ("attention_mask", [1] * 6), ("input_ids", [True, 5, 6, 7, 0, 0])):
            with self.subTest(key=key), self.assertRaises(runtime.cpu.BenchmarkError):
                contract.check_packet({**expected, key: value}, sequences)

    def test_regular_bounded_file_reader_rejects_alias_and_changed_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "input.json"
            path.write_bytes(b'{"x":1}')
            alias = path.with_name("alias.json")
            alias.symlink_to(path)
            with self.assertRaises(OSError):
                contract.read(alias)
            with self.assertRaises(runtime.cpu.BenchmarkError):
                contract.read(path, {"size_bytes": 7, "sha256": "0" * 64})

    def test_native_scaling_checks_every_output_and_owned_memory(self):
        event, first = response()
        empty = {"entities": [], "classifications": [], "structures": [], "relations": []}
        case = {"items": [{"text": "John works at Apple."}, {"text": "Empty."}],
                "expected_input_ids": [[1, 2, 3], [4]]}
        # Use valid original source coordinates with two distinct output rows.
        first = copy.deepcopy(first)
        first["entities"] = []
        first["structures"] = []
        first["relations"] = []
        first["classifications"] = [{"name": "kind", "labels": [{"label": "one", "confidence": 0.8}]}]
        event.update(outputs=[first, empty], output=first, **contract.packet(case["expected_input_ids"]))
        expected = [first, empty]
        self.assertEqual(expected, runtime.result(runtime.v1.NATIVE, event, case, expected,
                         validation=True, phase="validation", native_receipt=admitted()))
        event["outputs"].reverse()
        with self.assertRaises(runtime.cpu.BenchmarkError):
            runtime.result(runtime.v1.NATIVE, event, case, expected,
                           validation=True, phase="validation", native_receipt=admitted())

    def test_output_span_checks_original_unicode_and_rejects_synthetic_suffix(self):
        text = "İpek 東京."
        good = {"text": "東京", "source": {"start": 5, "end": 7}}
        runtime.check_source_spans(good, text)
        with self.assertRaises(runtime.cpu.BenchmarkError):
            runtime.check_source_spans({"text": "東京.", "source": {"start": 5, "end": 9}}, text)


if __name__ == "__main__":
    unittest.main()
