from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import benchmark_qwen_pair as bench


class QwenPairTests(unittest.TestCase):
    def setUp(self):
        self.cases = bench.embedding.load_fixture(
            Path(__file__).parent
            / "qwen3_embedding/fixtures/qwen3_embedding_0_6b_exact_tokens.json"
        )
        self.args = SimpleNamespace(warmup=1, iters=2, timeout=1, model="qwen")
        self.targets = {"baseline": "baseline/", "candidate": "candidate/"}

    def test_ragged_pairs_check_usage_parity_and_exclude_warmup(self):
        vector = [1.0] + [0.0] * 1023
        pairs = []
        replies = [
            (ms, [vector, vector], "qwen", 274) for ms in (90, 80, 20, 10, 11, 21)
        ]
        with patch.object(
            bench.embedding, "request_embeddings", side_effect=replies
        ) as request:
            report = bench.measure_embeddings(
                [20, 256], self.cases, self.targets, self.args, pairs.append
            )
        self.assertEqual(
            report["samples_ms"], {"baseline": [10, 11], "candidate": [20, 21]}
        )
        self.assertTrue(report["bitwise_equal"])
        self.assertEqual([True, False, False], [pair["warmup"] for pair in pairs])
        self.assertEqual(
            [
                "baseline/embeddings",
                "candidate/embeddings",
                "candidate/embeddings",
                "baseline/embeddings",
                "baseline/embeddings",
                "candidate/embeddings",
            ],
            [call.args[0] for call in request.call_args_list],
        )
        self.assertEqual(len({tuple(pair["case_ids"]) for pair in pairs}), 3)

    def test_warmup_must_also_have_valid_vectors_and_tokens(self):
        for vector, tokens in (
            ([float("nan")] * 1024, 19),
            ([1.0], 19),
            ([1.0] + [0.0] * 1023, 20),
        ):
            with self.subTest(tokens=tokens, dimensions=len(vector)):
                with patch.object(
                    bench.embedding,
                    "request_embeddings",
                    return_value=(10, [vector], "qwen", tokens),
                ):
                    with self.assertRaises(ValueError):
                        bench.measure_embeddings(
                            [20], self.cases, self.targets, self.args, lambda _: None
                        )


if __name__ == "__main__":
    unittest.main()
