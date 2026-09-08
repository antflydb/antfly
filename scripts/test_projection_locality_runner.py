"""Hermetic tests for the fresh-root ownership experiment's controls."""

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_posting_locality_ab as posting_runner
import run_projection_locality_ab as runner
from projection_locality_inputs import MEASUREMENT_HELPERS


class QualificationRunnerTest(unittest.TestCase):
    def run_fixture(
        self,
        *,
        fail=False,
        mutate=False,
        module=runner,
        resume=False,
        refinement=None,
        capture_stages=False,
        common_refinement=None,
        profile_count=None,
        control_binary=False,
    ):
        runner = module
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base = Path(temporary.name)
        scripts = base / "scripts"
        scripts.mkdir()
        script = scripts / "run_projection_locality_ab.py"
        script.write_text("runner fixture")
        harness = scripts / "run_vdbbench_qualification_snapshot_20260906.sh"
        harness.write_text("harness fixture")
        for name in MEASUREMENT_HELPERS:
            (scripts / name).write_text("measurement fixture")
        unused = scripts / "run_vector_store_enrichment_ab.py"
        unused.write_text("not used by this workload")
        binary = base / "antfly"
        binary.write_bytes(b"pinned executable")
        baseline_binary = base / "baseline-antfly"
        baseline_binary.write_bytes(b"preserved baseline executable")
        root = base / "results"
        calls = []

        def execute(command, **kwargs):
            calls.append((command, kwargs["env"].copy()))
            arm = Path(command[1])
            arm.mkdir(parents=True, exist_ok=True)
            (arm / "qualification-summary.json").write_text(
                json.dumps(
                    {
                        "runs": [
                            {"label": "fixture-online-live", "recall": 0.99},
                            {"label": "fixture-reopened-warm", "recall": 0.99},
                        ]
                    }
                )
            )
            (arm / "public-query-profile.json").write_text(
                json.dumps(
                    {
                        "count": profile_count if profile_count is not None else 1000,
                        "recall": 0.99,
                    }
                )
            )
            if (
                kwargs["env"].get("ANTFLY_EXPERIMENT_COMPACT_SUBGROUP_ROUTING") == "1"
                or kwargs["env"].get("ANTFLY_EXPERIMENT_PROJECTION_BORROW") == "1"
                or (
                    any(
                        kwargs["env"].get(flag) == "1"
                        for flag in (
                            "ANTFLY_EXPERIMENT_SUBGROUP_ROUTING",
                            "ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING",
                        )
                    )
                    and any(
                        kwargs["env"].get(f"ANTFLY_EXPERIMENT_SUBGROUPS_{count}") == "1"
                        for count in (4, 8, 16)
                    )
                )
            ):
                arm = Path(command[1])
                arm.mkdir(parents=True, exist_ok=True)
                (arm / "public-query-profile.json").write_text(
                    json.dumps(
                        {
                            "count": (
                                profile_count if profile_count is not None else 1000
                            ),
                            "recall": 0.99,
                            "profile_values": {
                                "hbc_subgroup_leaves_scored": {"mean": 12},
                                "hbc_subgroup_vectors_skipped": {"mean": 120},
                                "hbc_subgroup_compact_groups_scored": {"mean": 12},
                                "hbc_rerank_vector_projection_borrows": {"mean": 12},
                            },
                        }
                    )
                )
            if mutate == "helper":
                harness.write_text("changed workload")
            elif mutate == "unused":
                unused.write_text("unrelated edit")
            elif mutate:
                binary.write_bytes(b"changed executable")
            return subprocess.CompletedProcess(command, 1 if fail else 0)

        args = ["runner", str(root), "--binary", str(binary), "--include-1m"]
        if control_binary:
            args.extend(["--control-binary", str(baseline_binary)])
        if refinement:
            args.extend(["--refinement", refinement])
        if capture_stages:
            args.append("--capture-stages")
        if common_refinement:
            args.extend(["--common-refinement", common_refinement])
        if profile_count is not None:
            args.extend(["--profile-count", str(profile_count)])
        with (
            patch.object(runner, "__file__", str(script)),
            patch.object(sys, "argv", args),
            patch.object(runner.subprocess, "run", side_effect=execute),
            patch.dict(
                runner.os.environ,
                {
                    "ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES": "1",
                    "ANTFLY_HBC_FLAT_CENTROID_PROBE_COUNT": "1",
                    "VDBBENCH_VECTOR_BLOCK_ENCODING": "float32",
                },
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            if fail or (mutate and mutate != "unused"):
                with self.assertRaises(RuntimeError):
                    runner.main()
            else:
                runner.main()
                if resume:
                    args.append("--resume")
                    runner.main()
        return calls, json.loads((root / "ab-runs.json").read_text())

    def test_float16_alternates_and_scales_only_after_50k(self):
        calls, receipts = self.run_fixture()
        self.assertEqual(len(calls), 8)
        self.assertEqual(
            [r["mode"] for r in receipts],
            ["primary_lsm", "vector_store", "vector_store", "primary_lsm"] * 2,
        )
        self.assertTrue(all(r["case"] == "Performance1536D50K" for r in receipts[:4]))
        self.assertTrue(all(r["case"] == "Performance768D1M" for r in receipts[4:]))
        for command, env in calls:
            self.assertEqual(
                command[command.index("--vector-block-encoding") + 1], "float16"
            )
            self.assertEqual(command[command.index("--batch") + 1], "100")
            self.assertNotIn("--memory-budget-mb", command)
            self.assertEqual(env["ANTFLY_VDBBENCH_SYNC_LEVEL"], "write")
            self.assertEqual(env["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"], "1")
            self.assertNotIn("ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES", env)
            self.assertNotIn("ANTFLY_HBC_FLAT_CENTROID_PROBE_COUNT", env)
            self.assertNotIn("VDBBENCH_VECTOR_BLOCK_ENCODING", env)

    def test_failure_stops_later_arms(self):
        calls, receipts = self.run_fixture(fail=True)
        self.assertEqual(len(calls), 1)
        self.assertEqual(receipts[0]["exit_code"], 1)

    def test_preserved_binary_comparison_is_fresh_no_copy_and_pins_both(self):
        calls, receipts = self.run_fixture(
            module=posting_runner, control_binary=True, resume=True
        )
        self.assertEqual(
            [r["mode"] for r in receipts],
            ["control", "candidate", "candidate", "control"] * 2,
        )
        for (_, env), receipt in zip(calls, receipts, strict=True):
            self.assertEqual(env["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"], "0")
            self.assertEqual(
                Path(env["ANTFLY_BIN"]).name,
                "baseline-antfly" if receipt["mode"] == "control" else "antfly",
            )
            names = {Path(path).name for path in receipt["inputs_sha256"]}
            self.assertTrue({"antfly", "baseline-antfly"}.issubset(names))

    def test_changed_binary_invalidates_success(self):
        calls, receipts = self.run_fixture(mutate=True)
        self.assertEqual(len(calls), 1)
        self.assertIn("qualification input changed", receipts[0]["invalid_reason"])

    def test_changed_measurement_helper_invalidates_success(self):
        calls, receipts = self.run_fixture(mutate="helper")
        self.assertEqual(len(calls), 1)
        self.assertIn("qualification input changed", receipts[0]["invalid_reason"])

    def test_unrelated_enrichment_runner_does_not_invalidate(self):
        calls, receipts = self.run_fixture(mutate="unused")
        self.assertEqual(len(calls), 8)
        self.assertTrue(all("invalid_reason" not in receipt for receipt in receipts))

    def test_resume_preserves_completed_fresh_roots(self):
        calls, receipts = self.run_fixture(resume=True)
        self.assertEqual(len(calls), 8)
        self.assertEqual(len(receipts), 8)

    def test_posting_locality_changes_only_the_optional_plane(self):
        calls, receipts = self.run_fixture(module=posting_runner)
        self.assertEqual(
            [r["mode"] for r in receipts],
            ["local_on", "local_off", "local_off", "local_on"] * 2,
        )
        for (command, env), receipt in zip(calls, receipts, strict=True):
            self.assertEqual(
                command[command.index("--dense-embeddings") + 1], "vector_store"
            )
            self.assertEqual(
                command[command.index("--vector-block-encoding") + 1], "float16"
            )
            self.assertEqual(receipt["table_mode"], "vector_store")
            self.assertEqual(
                env["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"],
                "1" if receipt["mode"] == "local_on" else "0",
            )
            self.assertEqual(
                receipt["environment"]["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"],
                env["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"],
            )

    def test_posting_locality_failure_stops_scale_up(self):
        calls, receipts = self.run_fixture(module=posting_runner, fail=True)
        self.assertEqual(len(calls), 1)
        self.assertEqual(receipts[0]["exit_code"], 1)

    def test_posting_locality_resume_keeps_completed_arms(self):
        calls, receipts = self.run_fixture(module=posting_runner, resume=True)
        self.assertEqual(len(calls), 8)
        self.assertEqual(len(receipts), 8)

    def test_capture_tracing_is_identical_across_layout_arms(self):
        calls, _ = self.run_fixture(
            module=posting_runner, refinement="clustering", capture_stages=True
        )
        for _, env in calls:
            self.assertEqual(env["ANTFLY_EXPERIMENT_CAPTURE_STAGES"], "1")

    def test_layout_is_isolated_with_queued_readers_in_both_arms(self):
        calls, receipts = self.run_fixture(
            module=posting_runner,
            refinement="clustering",
            common_refinement="queued_pages",
            capture_stages=True,
        )
        for (_, env), receipt in zip(calls, receipts, strict=True):
            for key in posting_runner.REFINEMENTS["queued_pages"]:
                self.assertEqual(env[key], "1")
            self.assertEqual(
                env["ANTFLY_EXPERIMENT_PROJECTION_CLUSTERING"],
                "1" if receipt["mode"] == "candidate" else "0",
            )
            self.assertEqual(receipt["common_refinements"], ["queued_pages"])

    def test_common_treatment_cannot_hide_the_ab_difference(self):
        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            self.run_fixture(
                module=posting_runner,
                refinement="queued_pages",
                common_refinement="pages",
            )

    def test_conflicting_subgroup_layouts_are_rejected(self):
        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            self.run_fixture(
                module=posting_runner,
                refinement="subgroups_4",
                common_refinement="subgroups_8",
            )

    def test_fresh_subgroup_experiment_keeps_routing_policy_common(self):
        calls, receipts = self.run_fixture(
            module=posting_runner,
            refinement="subgroups_4",
            common_refinement="subgroup_routing",
        )
        for (_, env), receipt in zip(calls, receipts, strict=True):
            self.assertEqual(env["ANTFLY_EXPERIMENT_SUBGROUP_ROUTING"], "1")
            self.assertEqual(env["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"], "0")
            self.assertEqual(
                env["ANTFLY_EXPERIMENT_SUBGROUPS_4"],
                "1" if receipt["mode"] == "candidate" else "0",
            )

    def test_global_subgroup_routing_requires_and_records_live_evidence(self):
        calls, receipts = self.run_fixture(
            module=posting_runner,
            refinement="global_subgroup_routing",
            common_refinement="subgroups_4",
        )
        for (_, env), receipt in zip(calls, receipts, strict=True):
            self.assertEqual(env["ANTFLY_EXPERIMENT_SUBGROUPS_4"], "1")
            self.assertEqual(
                env["ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING"],
                "1" if receipt["mode"] == "candidate" else "0",
            )
        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            self.run_fixture(
                module=posting_runner,
                refinement="global_subgroup_routing",
                common_refinement="subgroups_4",
                profile_count=0,
            )

    def test_subgroup_qualification_rejects_missing_or_inert_evidence(self):
        environment = {
            "ANTFLY_EXPERIMENT_SUBGROUP_ROUTING": "1",
            "ANTFLY_EXPERIMENT_SUBGROUPS_4": "1",
        }
        with tempfile.TemporaryDirectory() as directory:
            arm = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "missing subgroup"):
                posting_runner.validate_subgroup_treatment(arm, environment)
            for mean in (0, -1, float("nan"), float("inf")):
                (arm / "public-query-profile.json").write_text(
                    json.dumps(
                        {
                            "profile_values": {
                                "hbc_subgroup_leaves_scored": {"mean": mean},
                                "hbc_subgroup_vectors_skipped": {"mean": 100},
                            }
                        }
                    )
                )
                with self.assertRaisesRegex(RuntimeError, "inert subgroup"):
                    posting_runner.validate_subgroup_treatment(arm, environment)
            self.assertIsNone(posting_runner.validate_subgroup_treatment(arm, {}))

    def test_refinements_are_independent_and_keep_no_copy_constant(self):
        for name, controls in posting_runner.REFINEMENTS.items():
            calls, receipts = self.run_fixture(module=posting_runner, refinement=name)
            self.assertEqual(
                [r["mode"] for r in receipts],
                ["control", "candidate", "candidate", "control"] * 2,
            )
            for (_, env), receipt in zip(calls, receipts, strict=True):
                self.assertEqual(
                    env["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"], "0"
                )
                self.assertEqual(receipt["refinement"], name)
                for key in posting_runner.ALL_REFINEMENT_FLAGS:
                    if key in controls:
                        self.assertEqual(
                            env[key], "1" if receipt["mode"] == "candidate" else "0"
                        )
                    else:
                        self.assertNotIn(key, env)


if __name__ == "__main__":
    unittest.main()
