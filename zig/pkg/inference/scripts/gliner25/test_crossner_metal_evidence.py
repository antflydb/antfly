"""Independent identity regressions for the compact CrossNER backend evidence."""
import copy
import unittest

import capture_crossner_metal_evidence as evidence
import evaluation_contract


class CrossNERMetalEvidenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = evidence.bio.decode(evidence.bio.read(evidence.LEDGER, evidence.MAX_LEDGER_BYTES))

    def rejected(self, value):
        with self.assertRaises(evaluation_contract.EvaluationError):
            evidence.validate(value)

    def test_repository_ledger_matches_published_source_and_historical_reports(self):
        evidence.validate(self.value)
        self.assertEqual(1293, sum(row["comparisons"]["metal_native_same_executable"]["requests"] for row in self.value["models"]))

    def test_metric_hash_is_stable_across_recomputed_and_serialized_key_order(self):
        self.assertEqual(evidence.digest({"entity_exact": {"tp": 2, "fp": 1}}),
                         evidence.digest({"entity_exact": {"fp": 1, "tp": 2}}))

    def test_foreign_model_or_sidecar_cannot_relabel_an_accepted_report(self):
        changed = copy.deepcopy(self.value)
        changed["models"][0]["source"] = copy.deepcopy(changed["models"][1]["source"])
        self.rejected(changed)
        changed = copy.deepcopy(self.value)
        changed["models"][0]["source"]["source_files"][0]["sha256"] = "0" * 64
        self.rejected(changed)
        changed = copy.deepcopy(self.value)
        foreign = copy.deepcopy(changed["models"][1]["reports"]["native"])
        foreign["model"] = "small"
        changed["models"][0]["reports"]["native"] = foreign
        self.rejected(changed)

    def test_consistent_old_cpu_build_cannot_replace_the_approved_shared_binary(self):
        changed = copy.deepcopy(self.value)
        changed["worker"]["sha256"] = evidence.HISTORICAL_WORKER_SHA
        for row in changed["models"]:
            row["reports"]["native"]["binary_sha256"] = evidence.HISTORICAL_WORKER_SHA
            row["reports"]["metal"]["binary_sha256"] = evidence.HISTORICAL_WORKER_SHA
        self.rejected(changed)

    def test_retrospective_proof_does_not_rewrite_the_metal_original_reference(self):
        changed = copy.deepcopy(self.value)
        row = changed["models"][0]
        row["reports"]["metal"]["native_reference_report_sha256"] = row["reports"]["native"]["pin"]["sha256"]
        self.rejected(changed)
        changed = copy.deepcopy(self.value)
        changed["models"][0]["reports"]["historical_native"]["pin"]["sha256"] = "0" * 64
        self.rejected(changed)

    def test_partial_coverage_and_looser_tolerance_cannot_be_promoted(self):
        for key, replacement in (("requests", 430), ("exact_token_sequences", 430),
                                 ("confidence_absolute_tolerance", .001), ("max_absolute_confidence_error", .00051)):
            with self.subTest(key=key):
                changed = copy.deepcopy(self.value)
                changed["models"][0]["comparisons"]["metal_native_same_executable"][key] = replacement
                self.rejected(changed)
        changed = copy.deepcopy(self.value)
        changed["models"][0]["reports"]["native"]["errors"] = 1
        self.rejected(changed)

    def test_source_quality_remains_bound_to_all_fourteen_declared_types(self):
        changed = copy.deepcopy(self.value)
        changed["models"][0]["quality"]["entity_exact"]["support"] -= 181
        changed["dataset"]["ontology"].remove("misc")
        self.rejected(changed)
        changed = copy.deepcopy(self.value)
        changed["models"][0]["quality"]["metrics_sha256"] = changed["models"][1]["quality"]["metrics_sha256"]
        self.rejected(changed)

    def test_stale_helper_and_added_release_claim_are_rejected(self):
        changed = copy.deepcopy(self.value)
        changed["contract_files"]["evaluate.py"] = "0" * 64
        self.rejected(changed)
        changed = copy.deepcopy(self.value)
        changed["claims"]["release_qualification"] = True
        self.rejected(changed)


if __name__ == "__main__":
    unittest.main()
