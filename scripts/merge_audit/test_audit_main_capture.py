from __future__ import annotations

import pathlib
import tempfile
import unittest

from scripts.merge_audit import audit_main_capture as audit


class ManifestSplitMigrationTest(unittest.TestCase):
    def test_load_json_file_rejects_duplicate_object_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "policy.json"
            path.write_text('{"split_migrations": {}, "split_migrations": {}}')

            with self.assertRaisesRegex(
                ValueError,
                "duplicate JSON object key: split_migrations",
            ):
                audit.load_json_file(path)

    def policy(self) -> dict[str, object]:
        return {
            "required_split_migrations": ["writes"],
            "moved_paths": {
                "src/writes.zig": [
                    "src/writes.zig",
                    "src/writes/flush.zig",
                    "src/writes/impl.zig",
                    "src/writes/legacy.zig",
                    "src/writes/lifecycle.zig",
                    "src/writes/tests.zig",
                ],
            },
            "zig_relative_import_roots": ["zig"],
            "declaration_name_aliases": {
                "OldRuntime": ["NewRuntime"],
            },
            "zig_relative_import_exceptions": [
                {
                    "path": "zig/generated_owner.zig",
                    "import": "generated.zig",
                    "reason": "materialized by the build graph",
                },
            ],
            "split_migrations": {
                "writes": {
                    "source": "src/writes.zig",
                    "destinations": ["src/writes", "src/writes.zig"],
                    "build_surface_helpers": {
                        "steps": [
                            {"function": "addNamedTest", "argument": 2}
                        ],
                    },
                    "build_surface_dependency_helpers": [
                        {
                            "function": "addNamedTest",
                            "step_argument": 2,
                            "target_argument": 3,
                        },
                        {
                            "function": "addDescriptorTest",
                            "step_argument": 1,
                            "step_field": "name",
                        },
                    ],
                    "build_surface_delta_only_categories": [
                        "module_imports",
                        "step_dependencies",
                    ],
                    "build_surface_aliases": {
                        "steps": {"old-write-test": "write-test"},
                        "module_imports": {
                            "addImport(old)": "addImport(new)",
                        },
                    },
                    "build_surface_conditional_steps": {
                        "conditional-write-test": "enabled with -Dconditional=true",
                    },
                    "build_surface_omissions": {
                        "root_sources": {
                            "src/removed_root.zig": "covered by generated owner",
                        },
                    },
                    "module_path_migrations": {
                        "src/legacy.zig": ["src/writes/replacement.zig"],
                    },
                    "symbol_call_migrations": {
                        "legacyCall": ["replacement.legacyCall"],
                    },
                    "symbol_reference_migrations": {
                        "legacy_limit": ["replacement.limit"],
                    },
                    "declaration_mixins": [
                        {
                            "path": "src/writes/impl.zig",
                            "factory": "Impl",
                            "owner": "Source",
                        },
                    ],
                    "declaration_placements": {
                        "test:write behavior": "src/writes/tests.zig",
                    },
                    "declaration_placement_ranges": [
                        {
                            "start": "function:Source.first",
                            "end": "function:Source.last",
                            "path": "src/writes/impl.zig",
                            "reason": "contiguous owner migration",
                        },
                    ],
                    "declaration_owner_migrations": {
                        "function:Source.flush": {
                            "owner": None,
                            "path": "src/writes/flush.zig",
                        },
                    },
                    "declaration_companion_policies": {
                        "src/writes/helpers.zig": {
                            "reviewed_resolutions": {
                                "function:newHelper": {
                                    "status": "added_name_collision",
                                    "base_sha256": None,
                                    "incoming_sha256": "d" * 64,
                                    "current_sha256": "e" * 64,
                                    "path": "src/writes/helpers.zig",
                                    "reason": "equivalent branch helper adds deadline checks",
                                },
                            },
                        },
                    },
                    "relative_import_exceptions": [
                        {
                            "path": "src/writes/generated_owner.zig",
                            "import": "generated.zig",
                            "reason": "materialized by the build graph",
                        },
                    ],
                    "retained_deletions": {
                        "function:Source.legacy": {
                            "base_sha256": "a" * 64,
                            "current_sha256": "b" * 64,
                            "path": "src/writes/legacy.zig",
                            "reason": "branch compatibility surface",
                        },
                    },
                    "reviewed_compositions": {
                        "function:build": {
                            "base_sha256": "a" * 64,
                            "incoming_sha256": "b" * 64,
                            "current_sha256": "c" * 64,
                            "path": "src/writes.zig",
                            "reason": "child obligations are independently audited",
                        },
                    },
                    "reviewed_resolutions": {
                        "function:Source.flush": {
                            "status": "three_way_conflict",
                            "base_sha256": "a" * 64,
                            "incoming_sha256": "b" * 64,
                            "current_sha256": "c" * 64,
                            "path": "src/writes/flush.zig",
                            "reason": "incoming durability and branch routing retained",
                        },
                        "function:newHelper": {
                            "status": "added_name_collision",
                            "base_sha256": None,
                            "incoming_sha256": "d" * 64,
                            "current_sha256": "e" * 64,
                            "path": "src/writes/helpers.zig",
                            "reason": "equivalent branch helper adds deadline checks",
                        },
                    },
                    "test_name_rewrites": [
                        {
                            "path": "src/writes/lifecycle.zig",
                            "source_prefix": "write ",
                            "destination_prefix": "write lifecycle ",
                        },
                    ],
                },
            },
        }

    def test_accepts_split_migration_policy(self) -> None:
        policy = self.policy()
        self.assertIs(
            policy,
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json")),
        )

    def test_rejects_invalid_declaration_name_aliases(self) -> None:
        policy = self.policy()
        policy["declaration_name_aliases"] = {"OldRuntime": "NewRuntime"}
        with self.assertRaisesRegex(
            ValueError,
            "declaration_name_aliases: expected object of string -> list",
        ):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_unknown_companion_policy_key(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"][
            "declaration_companion_policies"
        ]["src/writes/helpers.zig"]["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "unknown keys unexpected"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_companion_destinations_without_source_path(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"][
            "declaration_companion_policies"
        ]["src/writes/helpers.zig"]["destinations"] = [
            "src/writes/other.zig",
        ]
        with self.assertRaisesRegex(
            ValueError,
            "containing the companion path",
        ):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_companion_resolution_hash(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"][
            "declaration_companion_policies"
        ]["src/writes/helpers.zig"]["reviewed_resolutions"][
            "function:newHelper"
        ]["incoming_sha256"] = "not-a-hash"
        with self.assertRaisesRegex(ValueError, "lowercase SHA-256 hashes"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_unknown_split_migration_key(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "unknown keys unexpected"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_build_surface_helper(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["build_surface_helpers"]["steps"][0]["argument"] = -1
        with self.assertRaisesRegex(ValueError, "non-negative argument integer"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_build_dependency_helper(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"][
            "build_surface_dependency_helpers"
        ][0]["target_argument"] = -1
        with self.assertRaisesRegex(ValueError, "optional step_field or target_argument"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_nonsemantic_delta_only_build_category(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"][
            "build_surface_delta_only_categories"
        ] = ["steps"]
        with self.assertRaisesRegex(ValueError, "known semantic category"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_build_surface_alias(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["build_surface_aliases"][
            "steps"
        ]["old-write-test"] = []
        with self.assertRaisesRegex(ValueError, "non-empty string aliases"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_build_surface_retention_alias(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"][
            "build_surface_retention_aliases"
        ] = {"steps": {"old-write-test": []}}
        with self.assertRaisesRegex(ValueError, "non-empty string aliases"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_empty_build_surface_omission_reason(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["build_surface_omissions"][
            "root_sources"
        ]["src/removed_root.zig"] = ""
        with self.assertRaisesRegex(ValueError, "non-empty reasons"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_empty_conditional_build_step_reason(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"][
            "build_surface_conditional_steps"
        ]["conditional-write-test"] = ""
        with self.assertRaisesRegex(ValueError, "non-empty step names"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_module_path_migration(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["module_path_migrations"][
            "src/legacy.zig"
        ] = []
        with self.assertRaisesRegex(ValueError, "module_path_migrations"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_symbol_call_migration(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["symbol_call_migrations"][
            "legacyCall"
        ] = []
        with self.assertRaisesRegex(ValueError, "symbol_call_migrations"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_symbol_reference_migration(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["symbol_reference_migrations"][
            "legacy_limit"
        ] = []
        with self.assertRaisesRegex(ValueError, "symbol_reference_migrations"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_incomplete_repository_relative_import_exception(self) -> None:
        policy = self.policy()
        del policy["zig_relative_import_exceptions"][0]["reason"]
        with self.assertRaisesRegex(ValueError, "path, import, and reason"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_incomplete_owner_migration(self) -> None:
        policy = self.policy()
        del policy["split_migrations"]["writes"][
            "declaration_owner_migrations"
        ]["function:Source.flush"]["owner"]
        with self.assertRaisesRegex(ValueError, "expected null or non-empty string"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_incomplete_mixin_rule(self) -> None:
        policy = self.policy()
        del policy["split_migrations"]["writes"]["declaration_mixins"][0][
            "owner"
        ]
        with self.assertRaisesRegex(ValueError, "path, factory, and owner"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_incomplete_test_name_rewrite(self) -> None:
        policy = self.policy()
        del policy["split_migrations"]["writes"]["test_name_rewrites"][0][
            "destination_prefix"
        ]
        with self.assertRaisesRegex(ValueError, "destination_prefix"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_incomplete_relative_import_exception(self) -> None:
        policy = self.policy()
        del policy["split_migrations"]["writes"][
            "relative_import_exceptions"
        ][0]["reason"]
        with self.assertRaisesRegex(ValueError, "path, import, and reason"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_empty_retained_deletion_reason(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["retained_deletions"][
            "function:Source.legacy"
        ]["reason"] = ""
        with self.assertRaisesRegex(ValueError, "retained_deletions"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_incomplete_reviewed_composition(self) -> None:
        policy = self.policy()
        del policy["split_migrations"]["writes"]["reviewed_compositions"][
            "function:build"
        ]["incoming_sha256"]
        with self.assertRaisesRegex(ValueError, "incoming_sha256"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_reviewed_resolution_status(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["reviewed_resolutions"][
            "function:Source.flush"
        ]["status"] = "container_fields_missing"
        with self.assertRaisesRegex(ValueError, "reviewed_resolutions"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_accepts_all_hash_locked_reviewable_resolution_statuses(self) -> None:
        for status in (
            "added_name_collision",
            "clean_candidate",
            "container_fields_diverged",
            "container_variants_diverged",
            "three_way_conflict",
        ):
            policy = self.policy()
            policy["split_migrations"]["writes"]["reviewed_resolutions"][
                "function:Source.flush"
            ]["status"] = status
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_missing_required_split_migration(self) -> None:
        policy = self.policy()
        policy["required_split_migrations"].append("reads")
        with self.assertRaisesRegex(ValueError, "missing split_migrations reads"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_invalid_required_split_migration_without_crashing(self) -> None:
        policy = self.policy()
        policy["required_split_migrations"].append({"invalid": True})
        with self.assertRaisesRegex(ValueError, r"list\[non-empty string\]"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_duplicate_required_split_migration(self) -> None:
        policy = self.policy()
        policy["required_split_migrations"].append("writes")
        with self.assertRaisesRegex(ValueError, "duplicate entries writes"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_required_migration_without_moved_path_entry(self) -> None:
        policy = self.policy()
        del policy["moved_paths"]["src/writes.zig"]
        with self.assertRaisesRegex(ValueError, "moved_paths has no entry"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_accepts_required_migration_with_removed_source_facade(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["source_facade_removed"] = True
        policy["split_migrations"]["writes"]["destinations"] = [
            "src/writes",
        ]
        policy["moved_paths"]["src/writes.zig"].remove("src/writes.zig")
        audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_removed_source_facade_still_in_moved_paths(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["source_facade_removed"] = True
        with self.assertRaisesRegex(ValueError, "must omit its removed source facade"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_non_boolean_source_facade_removed(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["source_facade_removed"] = "yes"
        with self.assertRaisesRegex(ValueError, "source_facade_removed: expected boolean"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_required_migration_moved_path_outside_destinations(self) -> None:
        policy = self.policy()
        policy["moved_paths"]["src/writes.zig"].append("src/other.zig")
        with self.assertRaisesRegex(ValueError, "outside destinations"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))

    def test_rejects_required_migration_destination_without_moved_path(self) -> None:
        policy = self.policy()
        policy["split_migrations"]["writes"]["destinations"].append(
            "src/runtime.zig"
        )
        with self.assertRaisesRegex(ValueError, "missing from moved_paths coverage"):
            audit.ensure_manifest_shape(policy, pathlib.Path("policy.json"))


if __name__ == "__main__":
    unittest.main()
