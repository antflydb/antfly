import copy
import contextlib
import io
import json
import unittest
from unittest.mock import patch

from check_sql_parity_inventory import (
    FIXTURES,
    evidence_runs,
    family_report,
    main,
    release_blockers,
    run_evidence,
    select_evidence_gates,
    validate,
)


class ParityInventoryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inventory = (FIXTURES / "sql_parity_inventory.json").read_bytes()
        cls.ledger = json.loads(
            (FIXTURES / "sql_parity_dispositions.json").read_bytes()
        )

    def test_original_inventory_is_exhaustive(self):
        inventory, entries, _ = validate(self.inventory, self.ledger)
        self.assertEqual(1586, len(inventory["entries"]))
        self.assertEqual(1586, len(entries))

    def test_missing_and_duplicate_dispositions_are_rejected(self):
        for mutate in (
            lambda entries: entries.pop(),
            lambda entries: entries.append(entries[0]),
        ):
            ledger = copy.deepcopy(self.ledger)
            mutate(ledger["entries"])
            with self.assertRaises(ValueError):
                validate(self.inventory, ledger)

    def test_fixture_edit_cannot_silently_remove_original_scope(self):
        with self.assertRaisesRegex(ValueError, "checksum"):
            validate(self.inventory + b" ", self.ledger)

    def test_completed_status_needs_executable_evidence(self):
        for status in ("implemented", "rejected", "superseded"):
            ledger = copy.deepcopy(self.ledger)
            unresolved = next(
                entry
                for entry in ledger["entries"]
                if entry["status"] == "unresolved" and not entry.get("evidence")
            )
            unresolved["status"] = status
            with self.assertRaisesRegex(ValueError, "executable evidence"):
                validate(self.inventory, ledger)

    def test_required_original_behavior_cannot_be_relabelled_as_rejected(self):
        ledger = copy.deepcopy(self.ledger)
        implemented = next(
            entry for entry in ledger["entries"] if entry["status"] == "implemented"
        )
        implemented["status"] = "rejected"
        with self.assertRaisesRegex(ValueError, "non-rejection source contract"):
            validate(self.inventory, ledger)

    def test_original_rejection_needs_supersession_for_new_behavior(self):
        ledger = copy.deepcopy(self.ledger)
        rejected = next(
            entry for entry in ledger["entries"] if entry["status"] == "rejected"
        )
        rejected["status"] = "implemented"
        with self.assertRaisesRegex(ValueError, "original rejection"):
            validate(self.inventory, ledger)

    def test_case_id_must_be_in_a_cited_test_not_elsewhere_in_file(self):
        ledger = copy.deepcopy(self.ledger)
        entry = next(row for row in ledger["entries"] if row["id"] == "sql-0160")
        entry["evidence"][0]["test"] = entry["evidence"][1]["test"]
        entry["evidence"][2]["test"] = (
            "staged restore worker publishes a dependency complete mixed native cohort"
        )
        with self.assertRaisesRegex(ValueError, "at least one cited evidence test"):
            validate(self.inventory, ledger)

    def test_deferral_does_not_count_as_completion(self):
        self.assertEqual(1, len(release_blockers([{"status": "deferred"}])))

    def test_family_report_distinguishes_partial_and_absent_evidence(self):
        inventory = {
            "entries": [
                {
                    "id": "a",
                    "family": "read",
                    "source_expectation": "requires_behavior_review",
                },
                {"id": "b", "family": "read", "source_expectation": "rejection"},
                {
                    "id": "c",
                    "family": "read",
                    "source_expectation": "requires_behavior_review",
                },
                {
                    "id": "d",
                    "family": "ddl",
                    "source_expectation": "requires_behavior_review",
                },
            ]
        }
        entries = [
            {"id": "a", "status": "implemented", "evidence": [{"gate": "read"}]},
            {"id": "b", "status": "unresolved"},
            {"id": "c", "status": "unresolved", "evidence": [{"gate": "partial"}]},
            {"id": "d", "status": "deferred"},
        ]
        report = family_report(inventory, entries)
        self.assertEqual(["ddl", "read"], list(report))
        self.assertEqual(3, report["read"]["total"])
        self.assertEqual(1, report["read"]["partial_evidence"])
        self.assertEqual(1, report["read"]["no_evidence"])
        self.assertEqual(1, report["read"]["original_rejections"])
        self.assertEqual(1, report["ddl"]["no_evidence"])
        self.assertEqual(3, len(release_blockers(entries)))

    def test_family_gate_selection_preserves_partial_evidence(self):
        inventory, entries, gates = validate(self.inventory, self.ledger)
        blockers_before = release_blockers(entries)
        ddl = select_evidence_gates(inventory, entries, gates, "ddl")
        self.assertIn("sql-original-prepared-cte-runtime", ddl)
        self.assertEqual(
            ["sql-original-prepared-cte-runtime"],
            select_evidence_gates(
                inventory,
                entries,
                gates,
                "ddl",
                ["sql-original-prepared-cte-runtime"] * 2,
            ),
        )
        self.assertEqual(gates, select_evidence_gates(inventory, entries, gates))
        # A scoped execution must not change the full release denominator.
        self.assertEqual(blockers_before, release_blockers(entries))

    def test_empty_unknown_and_wrong_family_gate_selections_fail_closed(self):
        inventory = {
            "entries": [
                {"id": "a", "family": "read"},
                {"id": "b", "family": "window"},
            ]
        }
        entries = [
            {"id": "a", "evidence": [{"gate": "read-proof"}]},
            {"id": "b"},
        ]
        gates = ["read-proof"]
        for family, requested in (
            ("missing", None),
            ("window", None),
            (None, ["typo"]),
            ("window", ["read-proof"]),
        ):
            with self.assertRaises(ValueError):
                select_evidence_gates(inventory, entries, gates, family, requested)

    def test_gate_selection_cannot_narrow_release_validation(self):
        for mode in ([], ["--release"]):
            with (
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as raised,
            ):
                main(mode + ["--gate", "sql-compiler-rejections"])
            self.assertEqual(2, raised.exception.code)

    def test_recorded_zig_gates_do_not_rely_on_ignored_runtime_filters(self):
        for gate in self.ledger["gates"].values():
            command = gate["command"]
            if command[:2] == ["zig", "build"]:
                self.assertNotIn("--", command)
                self.assertNotIn("--test-filter", command)

    def test_cli_runs_only_selected_evidence_without_changing_dispositions(self):
        with (
            contextlib.redirect_stdout(io.StringIO()) as output,
            patch("check_sql_parity_inventory.run_evidence") as run,
        ):
            result = main(["--evidence", "--gate", "sql-explain-runtime"])
        self.assertEqual(0, result)
        self.assertEqual(["sql-explain-runtime"], run.call_args.args[0])
        self.assertIn("still block release", output.getvalue())

    def test_family_report_does_not_turn_release_into_a_subset_gate(self):
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()) as errors,
            patch("check_sql_parity_inventory.run_evidence") as run,
        ):
            result = main(["--release", "--family", "read", "--report"])
        self.assertEqual(1, result)
        self.assertIn("BLOCKED", errors.getvalue())
        run.assert_not_called()

    def test_unresolved_partial_evidence_is_checked_and_runs_without_release_credit(
        self,
    ):
        _, entries, gate_ids = validate(self.inventory, self.ledger)
        partial = next(entry for entry in entries if entry["id"] == "sql-0008")
        self.assertEqual("unresolved", partial["status"])
        self.assertIn("sql-original-prepared-cte-runtime", gate_ids)
        self.assertIn("pgwire-original-prepared-cte", gate_ids)
        ledger = copy.deepcopy(self.ledger)
        partial = next(
            entry for entry in ledger["entries"] if entry["id"] == "sql-0008"
        )
        # Build a controlled negative fixture: later valid evidence additions
        # must not satisfy the requirement these three unrelated tests probe.
        partial["evidence"] = partial["evidence"][:3]
        partial["evidence"][0]["test"] = (
            "SQL joined mutation equality work scales with inputs not Cartesian candidates"
        )
        partial["evidence"][1]["test"] = (
            "pgwire original prepared CTE INSERT defers mutation until execute"
        )
        partial["evidence"][2]["test"] = (
            "SQL document reads reject the relational stateless fallback before transport"
        )
        with self.assertRaisesRegex(ValueError, "at least one cited evidence test"):
            validate(self.inventory, ledger)

    def test_changed_original_source_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "source checksum"):
            validate(self.inventory, self.ledger, source_bytes=b"{}")

    def test_referenced_evidence_can_run_before_release_is_ready(self):
        _, entries, gate_ids = validate(self.inventory, self.ledger)
        self.assertGreater(len(release_blockers(entries)), 0)
        self.assertTrue(gate_ids)
        runs = evidence_runs(gate_ids, self.ledger["gates"])
        self.assertLess(len(runs), len(gate_ids))
        self.assertEqual(
            gate_ids, sorted(gate for run in runs for gate in run["gate_ids"])
        )
        with patch("check_sql_parity_inventory.subprocess.run") as run:
            run_evidence(gate_ids, self.ledger["gates"])
        self.assertEqual(len(runs), run.call_count)
        for planned, call in zip(runs, run.call_args_list, strict=True):
            self.assertEqual(planned["command"], call.args[0])
            self.assertEqual(planned["timeout_seconds"], call.kwargs["timeout"])
            self.assertTrue(call.kwargs["check"])

    def test_evidence_grouping_preserves_distinct_filters_and_isolation(self):
        gates = {
            "a": {
                "command": ["zig", "build", "sql-test", "--", "--test-filter", "alpha"],
                "cwd": "zig",
                "timeout_seconds": 5,
            },
            "b": {
                "command": [
                    "zig",
                    "build",
                    "sql-test",
                    "--",
                    "--test-filter",
                    "beta",
                    "--test-filter",
                    "alpha",
                ],
                "cwd": "zig",
                "timeout_seconds": 7,
            },
            "c": {
                "command": ["zig", "build", "sql-test", "--", "--test-filter", "gamma"],
                "cwd": "other",
                "timeout_seconds": 9,
            },
            "d": {
                "command": [
                    "zig",
                    "build",
                    "pgwire-test",
                    "--",
                    "--test-filter",
                    "delta",
                ],
                "cwd": "zig",
                "timeout_seconds": 11,
            },
            "e": {
                "command": ["zig", "build", "sql-test", "--", "CTE materialization"],
                "cwd": "zig",
                "timeout_seconds": 13,
            },
        }
        runs = evidence_runs(list(gates), gates)
        self.assertEqual(4, len(runs))
        self.assertEqual(["a", "b"], runs[0]["gate_ids"])
        self.assertEqual(
            [
                "zig",
                "build",
                "sql-test",
                "--",
                "--test-filter",
                "alpha",
                "--test-filter",
                "beta",
            ],
            runs[0]["command"],
        )
        self.assertEqual(12, runs[0]["timeout_seconds"])
        self.assertEqual(["c"], runs[1]["gate_ids"])
        self.assertEqual(["d"], runs[2]["gate_ids"])
        self.assertEqual(["e"], runs[3]["gate_ids"])

    def test_compile_filter_grouping_keeps_owner_and_build_options_isolated(self):
        def gate(filters, target="sql-test", options=()):
            return {
                "command": [
                    "zig",
                    "build",
                    target,
                    *options,
                    *("-Dtest-filter=" + value for value in filters),
                ],
                "cwd": "zig",
                "timeout_seconds": 5,
            }

        gates = {
            "a": gate(["alpha"]),
            "b": gate(["beta", "alpha", "beta"]),
            "c": gate(["gamma"], target="pgwire-test"),
            "d": gate(["delta"], options=["-Doptimize=ReleaseSafe"]),
        }
        runs = evidence_runs(list(gates), gates)
        self.assertEqual(3, len(runs))
        self.assertEqual(["a", "b"], runs[0]["gate_ids"])
        self.assertEqual(
            ["zig", "build", "sql-test", "-Dtest-filter=alpha", "-Dtest-filter=beta"],
            runs[0]["command"],
        )
        self.assertEqual(10, runs[0]["timeout_seconds"])
        self.assertEqual(["c"], runs[1]["gate_ids"])
        self.assertEqual(["d"], runs[2]["gate_ids"])


if __name__ == "__main__":
    unittest.main()
