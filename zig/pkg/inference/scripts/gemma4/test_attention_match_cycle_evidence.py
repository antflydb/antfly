import copy
import unittest
import qualify_attention_match_queries as qualification


class CycleEvidenceTests(unittest.TestCase):
    def fixture(self):
        requests = [{"response": {"usage": {"prompt_tokens": 8189}}}]
        rows, fits = [], []
        length = 8189
        for index in range(2):
            length += 512 if index else 0
            fits.append(dict(source_tokens=length, snapshot_ns=1, fit_ns=1, export_ns=1, upload_ns=1, install_ns=1, total_ns=6,
                             source_host_bytes=100, candidate_host_bytes=80, candidate_device_bytes=40, export_bytes=90,
                             completed_chunks=6, max_nnls_sweeps=1, export_sha256="a" * 64, export_version=2,
                             validated_heads=6, max_head_mass_error=.01, max_head_output_rmse=.01, residual_fit=1,
                             preserved_heads=2, source_global_bytes=100, candidate_global_bytes=80, actual_global_savings_ratio=1.25,
                             cuda_fit=1, diagnostic_only=1, installed=1, cooperative_fit=1, cooperative_launches=128,
                             cooperative_restarts=0, fit_workers=4, workspace_reuses=20, host_workspace_limit_bytes=2 * 1024 ** 3))
            length += 2
            rows.append(dict(cycle=index+1, logical_tokens=length, completed_chunks=6, greedy_matches=3, greedy_comparisons=3,
                             answer_appends=2, max_logit_relative_l2=.01, ragged_repeat_calls=42 if index else 0,
                             installed_revision=2 + index * 10, checkpoint_roundtrip=1, source_unchanged=1, invalid=0,
                             incremental_savings_ratio=1.25))
        summaries = [dict(cycles=2, initial_tokens=8189, final_tokens=length, greedy_matches=6, greedy_comparisons=6, source_unchanged=1)]
        return rows, fits, summaries, requests

    def validate(self, evidence):
        qualification.validate_cycle_evidence(*evidence, cycles=2, append_tokens=512, answer_tokens=4,
                                             minimum_savings=1.01, first_minimum_savings=1.2)

    def test_middle_fact_is_outside_the_protected_prompt_prefix(self):
        prompt = next(qualification.requests("model", "f16", [742], fixture="fact_middle"))["messages"][0]["content"]
        before, after = prompt.split("The access code is violet-729.\n")
        self.assertEqual(before.count("This log entry"), 371)
        self.assertEqual(after.count("This log entry"), 371)
        self.assertTrue(after.endswith("Reply with only the access code."))

    def test_complete_successive_chain(self):
        self.validate(self.fixture())
        parsed = qualification.parse_capture("incremental_savings_ratio=1.25 max_logit_relative_l2=0.01 answer_appends=2")
        self.assertEqual(parsed, dict(incremental_savings_ratio=1.25, max_logit_relative_l2=.01, answer_appends=2))

    def test_rejects_missing_or_misordered_cycles_and_false_success(self):
        for key, value in (("cycle", 1), ("source_unchanged", 0), ("invalid", 1), ("checkpoint_roundtrip", 0),
                           ("ragged_repeat_calls", 0), ("greedy_matches", 2), ("answer_appends", 5),
                           ("logical_tokens", 8189), ("installed_revision", 2), ("max_logit_relative_l2", float("nan")),
                           ("max_logit_relative_l2", -1), ("incremental_savings_ratio", 1), ("completed_chunks", 0)):
            evidence = self.fixture()
            evidence[0][1][key] = value
            with self.subTest(key=key), self.assertRaises(AssertionError):
                self.validate(evidence)
        for part in (0, 1, 2):
            evidence = self.fixture()
            evidence[part].pop()
            with self.subTest(part=part), self.assertRaises(AssertionError):
                self.validate(evidence)

    def test_rejects_fit_or_summary_disagreement(self):
        for part, key, value in ((1, "source_tokens", 8189), (1, "export_version", 1), (1, "host_workspace_limit_bytes", 0),
                                 (1, "actual_global_savings_ratio", 2), (1, "completed_chunks", 7),
                                 (2, "greedy_matches", 0), (2, "final_tokens", 8189), (2, "cycles", 20)):
            evidence = self.fixture()
            evidence[part][-1][key] = value
            with self.subTest(part=part, key=key), self.assertRaises(AssertionError):
                self.validate(evidence)


class MemoryCoverageTests(unittest.TestCase):
    def test_memory_scope_tracks_cold_start_and_prefers_full_block_samples(self):
        from qualify_attention_match_queries import summarize_memory
        warm = {"samples": 10, "errors": [], "peak_host_rss_bytes": 3 * 1024**3, "peak_gpu_memory_bytes": 10 * 1024**3}
        cold = warm | {"peak_host_rss_bytes": 9 * 1024**3}
        result = summarize_memory([{"memory": cold, "memory_after_first_request": warm, "sampling_started_before_requests": True}])
        self.assertTrue(result["includes_cold_start"])
        self.assertFalse(result["memory_gate_met"])
        self.assertFalse(summarize_memory([{"memory_after_first_request": warm}])["includes_cold_start"])
        self.assertFalse(summarize_memory([])["includes_cold_start"])


if __name__ == "__main__":
    unittest.main()
