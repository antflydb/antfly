# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
import hashlib
import unittest
from embeddinggemma2_calibrate import fit

class CalibrationTests(unittest.TestCase):
    def fixture(self, n=100):
        return {"binding": {"model_identity": "a"*64, "prototype_set_hash": "b"*64,
                "renderer_version": "instruction-category-v1", "task_type": "CLUSTERING",
                "dimensions": 128, "labels": ["a", "b"], "mode": "single"},
                "samples": [{"state_sha256": hashlib.sha256(f"{split}-{i}".encode()).hexdigest(),
                    "split": split, "scores": [0.8, 0.2] if i%2 else [0.1, 0.9], "labels": ["a" if i%2 else "b"]}
                    for split in ("fit", "validation", "holdout") for i in range(n)]}

    def test_holdout_does_not_fit_thresholds(self):
        source = self.fixture()
        correct = fit(source)
        wrong = copy.deepcopy(source)
        for row in wrong["samples"]:
            if row["split"] == "holdout":
                row["labels"] = ["b" if row["labels"] == ["a"] else "a"]
        failed = fit(wrong)
        self.assertEqual(correct["thresholds"], failed["thresholds"])
        self.assertTrue(correct["qualified"])
        self.assertFalse(failed["qualified"])
        self.assertNotIn("probabilities", correct)

    def test_small_sample_is_unqualified(self):
        self.assertFalse(fit(self.fixture(10))["qualified"])

    def test_cross_split_duplicates_fail(self):
        source = self.fixture()
        source["samples"][100]["state_sha256"] = source["samples"][0]["state_sha256"]
        with self.assertRaisesRegex(ValueError, "leakage"):
            fit(source)

    def test_multilabel_qualification_and_finite_validation(self):
        source = self.fixture()
        source["binding"]["mode"] = "multi"
        artifact = fit(source)
        self.assertTrue(artifact["qualified"])
        self.assertEqual(set(artifact["thresholds"]["similarity_thresholds"]), {"a", "b"})
        source["samples"][0]["scores"][0] = float("nan")
        with self.assertRaisesRegex(ValueError, "cosine"):
            fit(source)

if __name__ == "__main__":
    unittest.main()
