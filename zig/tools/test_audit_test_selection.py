# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import unittest

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "audit_test_selection", Path(__file__).with_name("audit_test_selection.py")
)
assert _SPEC and _SPEC.loader
_module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_module)
audit = _module.audit


class SelectionAuditTests(unittest.TestCase):
    def test_selection_can_span_owners(self):
        self.assertEqual(
            audit(
                ["TEST\tapi.test.route retry\n", "TEST\tstorage.test.lease drain\n"],
                ["route", "lease"],
            ),
            [],
        )

    def test_empty_partition_is_allowed_but_empty_union_is_not(self):
        self.assertEqual(audit(["", "TEST\tapi.test.route retry\n"], ["route"]), [])
        self.assertTrue(audit(["", "TEST\troot.test_0\n"], []))

    def test_missing_filter_is_rejected(self):
        self.assertIn(
            "test filter matched no declared tests: missing",
            audit(["TEST\tapi.test.route retry\n"], ["missing"]),
        )

    def test_explicit_empty_override_and_exclusions(self):
        self.assertEqual(audit([""], ["missing"], allow_empty=True), [])
        inventory = ["TEST\tapi.test.route retry\n"]
        self.assertIn(
            "test selection matched no runnable tests",
            audit(inventory, ["route"], ["route"]),
        )
        self.assertEqual(audit(inventory, ["route"], ["route"], allow_empty=True), [])

    def test_duplicate_owner_is_rejected(self):
        inventory = "TEST\tapi.test.route retry\n"
        self.assertTrue(audit([inventory, inventory], []))

    def test_plain_filters_do_not_match_module_names(self):
        self.assertTrue(audit(["TEST\tstorage.test.route retry\n"], ["storage"]))
        self.assertEqual(audit(["TEST\tstorage.test.route retry\n"], ["storage."]), [])


if __name__ == "__main__":
    unittest.main()
