from __future__ import annotations

import importlib
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

import capture as target


ROOT = Path(__file__).resolve().parent
UPSTREAM = Path("/private/tmp/antfly-gliner25-upstream")
CONTRACT = json.loads((ROOT / "contract.json").read_bytes())


class SourceControlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = target.capture(UPSTREAM, CONTRACT)
        cls.rows = {row["id"]: row for row in cls.report["cases"]}

    def test_edge_first_pruning_differs_from_feasible_no_edge_assignment(self):
        row = self.rows["six_negative_edges"]
        self.assertEqual(row["beam"]["score"], 117)
        self.assertEqual(row["beam"]["node_indices"], list(range(12)))
        self.assertEqual([edge["source_edge_index"] for edge in row["beam"]["edges"]], [0, 1, 2])
        self.assertEqual(row["greedy"]["score"], 114)
        # Prove the120 assignment using the source's own final constraints.
        with target.source_modules(UPSTREAM, CONTRACT["source_files"]) as modules:
            problem = target.make_problem(row["input"], modules)
            optimizer = modules["beam"].BeamOptimizer(32)
            all_nodes = optimizer.solution(problem, [node.candidate_id for node in problem.nodes], (), 120)
            self.assertTrue(optimizer.validate_solution(problem, all_nodes))

    def test_independent_greedy_baseline_can_beat_every_beam_finish(self):
        row = self.rows["greedy_baseline_wins_beam32"]
        self.assertEqual(row["beam_finish_count"], 32)
        self.assertEqual(row["best_feasible_beam_finish_score"], 94)
        self.assertTrue(row["greedy_strictly_beats_all_feasible_beam_finishes"])
        self.assertEqual(row["greedy"]["score"], 114)
        self.assertEqual(row["beam"], row["greedy"])

    def test_final_semantic_tie_uses_max_and_source_repr(self):
        row = self.rows["semantic_tie_slots_2_10"]
        self.assertEqual(row["source_keys"]["node_strings"][0], "('组织', 2, 3)")
        self.assertEqual(row["source_keys"]["node_strings"][2], "('组织', 10, 11)")
        self.assertLess(row["source_keys"]["node_strings"][2], row["source_keys"]["node_strings"][0])
        self.assertEqual(row["source_keys"]["slot_strings"], ["2", "10"])
        self.assertEqual(row["beam"]["edges"][0]["source_edge_index"], 0)
        self.assertEqual(row["greedy"]["edges"][0]["source_edge_index"], 1)
        self.assertEqual(row["beam"]["node_indices"], [0, 1])
        self.assertIn("\\'", row["source_keys"]["node_strings"][1])

    def test_zero_gain_and_allowed_self_source_semantics(self):
        zero = self.rows["zero_gain_edge_is_retained"]
        self.assertEqual(zero["beam"]["score"], 0)
        self.assertEqual(len(zero["beam"]["edges"]), 1)
        self_row = self.rows["allowed_self_endpoint_counted_twice"]
        self.assertEqual(self_row["greedy"]["score"], 5)
        self.assertEqual(self_row["greedy"]["unique_node_plus_edge_sum"], -5)
        self.assertEqual(self_row["beam"]["score"], 10)
        self.assertEqual(self_row["beam"]["edges"], [])

    def test_derived_edges_are_revalidated_and_keep_explicit_feasibility(self):
        row = self.rows["derived_symmetric_invalid_cycle"]
        self.assertEqual(row["beam"]["score"], 2)
        self.assertEqual(row["beam"]["edges"], [])
        self.assertTrue(row["beam"]["feasible_flag"])
        self.assertFalse(row["greedy"]["feasible_flag"])
        # The returned empty fallback validates; its explicit false flag still matters.
        self.assertTrue(row["greedy"]["final_constraints_satisfied"])
        valid = self.rows["derived_inverse_valid"]["beam"]
        self.assertEqual(valid["score"], 5)
        self.assertEqual([edge["derived"] for edge in valid["edges"]], [False, True])
        self.assertEqual(valid["edges"][1]["candidate_id"], ("derived", "reverse'Ω", ("B", 10, 11), ("A", 2, 3)))

    def test_count_alternative_compatibility_and_free_node_overlap_tie(self):
        row = self.rows["count_alternatives_and_slots"]
        self.assertEqual(row["beam"]["score"], 9)
        self.assertEqual([edge["count_alternative"] for edge in row["beam"]["edges"]], [1, 1])
        self.assertEqual(row["greedy"]["score"], 8)
        self.assertEqual(self.rows["positive_free_node_overlap_tie"]["beam"]["node_indices"], [1])

    def test_source_pins_and_import_allowlist_fail_closed(self):
        pins = dict(CONTRACT["source_files"])
        relative = target.MODULES[0][1]
        pins[relative] = {**pins[relative], "sha256": "0" * 64}
        with self.assertRaisesRegex(ValueError, "pinned source mismatch"):
            with target.source_modules(UPSTREAM, pins):
                self.fail("tampered source admitted")
        with target.source_modules(UPSTREAM, CONTRACT["source_files"]):
            for name in ("torch", "transformers", "numpy", "gliner2.model", "gliner2.unlisted"):
                with self.assertRaisesRegex(RuntimeError, "forbidden|outside fixed allowlist"):
                    importlib.import_module(name)
        self.assertEqual(target.forbidden_loaded(), [])
        self.assertFalse(any(name == "gliner2" or name.startswith("gliner2.") for name in sys.modules))

    def test_source_exception_restores_observer_and_packages(self):
        class FailingConstraint:
            def allow_edge(self, *_args):
                raise RuntimeError("controlled source-call failure")
        with target.source_modules(UPSTREAM, CONTRACT["source_files"]) as modules:
            case = target.fixed_cases()[0]
            problem = target.make_problem(case, modules)
            broken = modules["candidates"].JointProblem(problem.nodes, problem.edges, (FailingConstraint(),))
            with mock.patch.object(target, "make_problem", return_value=broken):
                with self.assertRaisesRegex(RuntimeError, "controlled source-call failure"):
                    target.capture_case(case, modules)
            self.assertIsNone(sys.getprofile())
        self.assertFalse(any(name == "gliner2" or name.startswith("gliner2.") for name in sys.modules))

    def test_repeated_source_capture_is_exact_and_bounded(self):
        repeated = target.capture(UPSTREAM, CONTRACT)
        self.assertEqual(target.encode(repeated), target.encode(self.report))
        self.assertLess(len(target.encode(repeated)), target.MAX_OUTPUT)
        self.assertEqual(repeated["forbidden_imports_observed"], [])
        self.assertTrue(repeated["profile_hook_removed"])

    def test_saved_compact_capture_matches_source_replay_and_provenance(self):
        raw = target.read_regular(ROOT / "capture.json", target.MAX_OUTPUT)
        reproduced = {
            **self.report,
            "contract": target.digest(target.read_regular(ROOT / "contract.json")),
            "generator": target.digest(target.read_regular(Path(target.__file__))),
        }
        self.assertEqual(target.encode(reproduced), raw)
        for row in self.report["cases"]:
            self.assertNotIn("beam_finish_candidates", row)
            self.assertLessEqual(row["beam_finish_count"], row["input"]["beam_width"])


if __name__ == "__main__":
    unittest.main()
