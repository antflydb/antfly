import copy
import json
import unittest
from unittest.mock import patch

from check_sql_parity_inventory import (
    FIXTURES,
    release_blockers,
    run_evidence,
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
                entry for entry in ledger["entries"] if entry["status"] == "unresolved"
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

    def test_changed_original_source_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "source checksum"):
            validate(self.inventory, self.ledger, source_bytes=b"{}")

    def test_resolved_evidence_can_run_before_release_is_ready(self):
        _, entries, gate_ids = validate(self.inventory, self.ledger)
        self.assertGreater(len(release_blockers(entries)), 0)
        self.assertTrue(gate_ids)
        with patch("check_sql_parity_inventory.subprocess.run") as run:
            run_evidence(gate_ids, self.ledger["gates"])
        self.assertEqual(len(gate_ids), run.call_count)
        for gate_id, call in zip(gate_ids, run.call_args_list, strict=True):
            gate = self.ledger["gates"][gate_id]
            self.assertEqual(gate["command"], call.args[0])
            self.assertEqual(gate["timeout_seconds"], call.kwargs["timeout"])
            self.assertTrue(call.kwargs["check"])


if __name__ == "__main__":
    unittest.main()
