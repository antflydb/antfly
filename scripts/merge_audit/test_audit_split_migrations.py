import argparse
import pathlib
import unittest

from scripts.merge_audit import audit_split_migrations as audit


class SplitMigrationAuditTest(unittest.TestCase):
    def test_companion_child_command_applies_owner_policy(self) -> None:
        args = argparse.Namespace(
            base="base",
            incoming="incoming",
            policy=pathlib.Path("policy.json"),
            minimum_similarity=0.7,
            ambiguity_margin=0.05,
            include_unchanged=False,
            include_missing_candidates=False,
            include_review_bodies=False,
            destination_ref=None,
            candidate_dir=None,
        )

        command = audit.companion_child_command(
            args,
            "core",
            "db",
            "db/core.zig",
            pathlib.Path("report.json"),
        )

        owner_index = command.index("--companion-owner")
        self.assertEqual("db", command[owner_index + 1])

    def test_parse_name_status_covers_both_sides_of_renames(self) -> None:
        self.assertEqual(
            {"db/old.zig", "db/new.zig", "db/modified.zig"},
            audit.parse_name_status(
                b"R100\0db/old.zig\0db/new.zig\0M\0db/modified.zig\0"
            ),
        )

    def test_declaration_companions_are_discovered_and_owned_once(self) -> None:
        policy = {
            "split_migrations": {
                "db": {
                    "source": "db/db.zig",
                    "declaration_companion_globs": ["db/*.zig"],
                },
                "table_reads": {
                    "source": "api/table_reads.zig",
                    "declaration_companion_globs": ["api/table_reads/*.zig"],
                },
            }
        }
        paths = {
            "db/db.zig",
            "db/core.zig",
            "db/readme.md",
            "api/table_reads.zig",
            "api/table_reads/core.zig",
            "outside.zig",
        }

        self.assertEqual(
            {
                "db": ["db/core.zig"],
                "table_reads": ["api/table_reads/core.zig"],
            },
            audit.declaration_companion_paths(
                policy,
                ["db", "table_reads"],
                paths,
            ),
        )

    def test_declaration_companions_reject_ambiguous_owners(self) -> None:
        policy = {
            "split_migrations": {
                "first": {
                    "source": "db.zig",
                    "declaration_companion_globs": ["shared/*.zig"],
                },
                "second": {
                    "source": "table.zig",
                    "declaration_companion_globs": ["shared/*.zig"],
                },
            }
        }

        with self.assertRaisesRegex(ValueError, "ambiguous owners"):
            audit.declaration_companion_paths(
                policy,
                ["first", "second"],
                {"shared/core.zig"},
            )

    def test_companion_file_classification_fails_closed(self) -> None:
        self.assertEqual(
            "companion_exact",
            audit.classify_companion_file(b"base", b"incoming", b"incoming"),
        )
        self.assertEqual(
            "companion_added_exact",
            audit.classify_companion_file(None, b"incoming", b"incoming"),
        )
        self.assertEqual(
            "companion_deleted_absent",
            audit.classify_companion_file(b"base", None, None),
        )
        self.assertEqual(
            "companion_declaration_audit_required",
            audit.classify_companion_file(b"base", b"incoming", b"composed"),
        )
        self.assertEqual(
            "companion_added_diverged",
            audit.classify_companion_file(None, b"incoming", b"composed"),
        )

    def test_load_migration_names_defaults_to_every_manifest_entry(self) -> None:
        policy = {
            "required_split_migrations": [
                "db",
                "table_reads",
                "table_writes",
                "zig_build",
                "inference_build",
            ],
            "split_migrations": {
                "db": {},
                "table_reads": {},
                "table_writes": {},
                "zig_build": {},
                "inference_build": {},
            }
        }

        self.assertEqual(
            ["db", "table_reads", "table_writes", "zig_build", "inference_build"],
            audit.load_migration_names(policy, []),
        )

    def test_load_migration_names_rejects_missing_required_entry(self) -> None:
        policy = {
            "required_split_migrations": ["db", "table_reads"],
            "split_migrations": {"db": {}},
        }

        with self.assertRaisesRegex(
            ValueError,
            "required split migrations are not configured: table_reads",
        ):
            audit.load_migration_names(policy, [])

    def test_load_migration_names_rejects_unknown_and_duplicate_selection(
        self,
    ) -> None:
        policy = {"split_migrations": {"db": {}}}

        with self.assertRaisesRegex(ValueError, "unknown split migrations"):
            audit.load_migration_names(policy, ["table_reads"])
        with self.assertRaisesRegex(ValueError, "duplicate --migration"):
            audit.load_migration_names(policy, ["db", "db"])

    def test_unresolved_statuses_fails_closed(self) -> None:
        self.assertEqual(
            {
                "missing_modified": 2,
                "split_alias_review": 1,
                "unexpected_future_status": 3,
            },
            audit.unresolved_statuses(
                {
                    "exact": 5,
                    "carried_branch_changed": 4,
                    "container_review_fields_present": 1,
                    "deleted_retained_reviewed": 2,
                    "intentional_deletion": 2,
                    "missing_modified": 2,
                    "split_alias_review": 1,
                    "unexpected_future_status": 3,
                }
            ),
        )

    def test_child_command_preserves_per_migration_artifacts(self) -> None:
        args = argparse.Namespace(
            base="base",
            incoming="incoming",
            policy=pathlib.Path("/tmp/policy.json"),
            minimum_similarity=0.7,
            ambiguity_margin=0.05,
            include_unchanged=True,
            include_missing_candidates=True,
            include_review_bodies=False,
            destination_ref="branch-before-merge",
            candidate_dir=pathlib.Path("/tmp/candidates"),
        )

        command = audit.child_command(
            args,
            "table_reads",
            pathlib.Path("/tmp/reports/table_reads.json"),
        )

        self.assertIn("--include-unchanged", command)
        self.assertIn("--include-missing-candidates", command)
        self.assertEqual(
            "branch-before-merge",
            command[command.index("--destination-ref") + 1],
        )
        self.assertEqual(
            "/tmp/candidates/table_reads",
            command[command.index("--candidate-dir") + 1],
        )
        self.assertEqual(
            "/tmp/reports/table_reads.json",
            command[command.index("--json-out") + 1],
        )

    def test_review_queue_is_source_ordered_by_the_caller(self) -> None:
        report = {
            "obligations": [
                {
                    "key": "function:newFix",
                    "kind": "function",
                    "change": "added",
                    "status": "missing_added",
                    "base_line": None,
                    "incoming_line": 42,
                    "current_path": None,
                    "suggested_path": "split.zig",
                    "previous_anchor": "function:before",
                    "next_anchor": "function:after",
                    "detail": "missing",
                },
                {
                    "key": "function:kept",
                    "kind": "function",
                    "change": "unchanged",
                    "status": "exact",
                    "base_line": 8,
                    "incoming_line": 10,
                },
            ]
        }

        self.assertEqual(
            [
                {
                    "migration": "db",
                    "key": "function:newFix",
                    "kind": "function",
                    "change": "added",
                    "status": "missing_added",
                    "base_line": None,
                    "incoming_line": 42,
                    "current_path": None,
                    "suggested_path": "split.zig",
                    "previous_anchor": "function:before",
                    "next_anchor": "function:after",
                    "detail": "missing",
                }
            ],
            audit.review_queue_items(
                "db",
                report,
                {"missing_added": 1},
            ),
        )

    def test_review_queue_validates_deletion_base_line(self) -> None:
        report = {
            "obligations": [
                {
                    "key": "function:removed",
                    "kind": "function",
                    "change": "deleted",
                    "status": "deleted_still_present",
                    "base_line": 0,
                    "incoming_line": None,
                }
            ]
        }

        with self.assertRaisesRegex(ValueError, "invalid base_line"):
            audit.review_queue_items(
                "db",
                report,
                {"deleted_still_present": 1},
            )


if __name__ == "__main__":
    unittest.main()
