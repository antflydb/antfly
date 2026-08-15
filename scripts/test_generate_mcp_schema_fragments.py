# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import json
import unittest

import yaml
from jsonschema import Draft202012Validator

from scripts import generate_mcp_schema_fragments as generator


class McpSchemaFragmentTests(unittest.TestCase):
    def test_mcp_result_budget_schema_matches_runtime_zero_or_minimum(self) -> None:
        config_spec = generator.ROOT / "specs/openapi/antfly/config.yaml"
        with config_spec.open(encoding="utf-8") as handle:
            schema = yaml.safe_load(handle)["components"]["schemas"]["McpConfig"]["properties"]["max_tool_result_bytes"]

        validator = Draft202012Validator(schema)
        for valid in (0, 512, 98_304, 4_294_967_295):
            self.assertEqual([], list(validator.iter_errors(valid)), valid)
        for invalid in (-1, 1, 511, 4_294_967_296):
            self.assertNotEqual([], list(validator.iter_errors(invalid)), invalid)

    def test_deprecated_constraints_are_removed_without_dropping_canonical_constraints(self) -> None:
        schema = {
            "type": "object",
            "properties": {
                "canonical": {"type": "string"},
                "legacy": {"type": "string", "deprecated": True},
            },
            "allOf": [
                {"not": {"required": ["legacy"]}},
                {"not": {"required": ["canonical"]}},
            ],
        }

        removed = generator.deprecated_property_names(schema)
        compact = generator.without_deprecated_properties(schema)
        compact = generator.without_constraints_referencing_properties(compact, removed)

        self.assertEqual({"canonical"}, set(compact["properties"]))
        self.assertEqual([{"not": {"required": ["canonical"]}}], compact["allOf"])

    def test_query_hierarchy_keeps_the_canonical_group_ancestor_constraint(self) -> None:
        with generator.SPEC.open(encoding="utf-8") as handle:
            schemas = yaml.safe_load(handle)["components"]["schemas"]
        fragment = next(item for item in generator.FRAGMENTS if item.component == "QueryHierarchy")

        generated = json.loads(generator.generated_content(fragment, schemas))

        self.assertNotIn("return_level", generated["properties"])
        self.assertNotIn("rollup", generated["properties"])
        self.assertIn("allOf", generated)
        self.assertTrue(
            any(
                branch.get("not", {}).get("properties", {}).get("ancestors", {}).get("required") == ["source"]
                for branch in generated["allOf"]
            )
        )

    def test_compact_query_request_keeps_cross_field_constraints(self) -> None:
        with generator.SPEC.open(encoding="utf-8") as handle:
            schemas = yaml.safe_load(handle)["components"]["schemas"]

        generated = generator.compact_query_request_schema(schemas)

        self.assertNotIn("table", generated["properties"])
        self.assertIn("hierarchy", generated["properties"])
        self.assertEqual(
            ["hierarchy"],
            generated["not"]["allOf"][0]["required"],
        )
        self.assertEqual(
            ["group_by"],
            generated["not"]["allOf"][0]["properties"]["hierarchy"]["required"],
        )
        self.assertEqual(["fields"], generated["not"]["allOf"][1]["not"]["required"])
        self.assertIn(
            {"not": generator.property_has_non_null_value("table")},
            generated["allOf"],
        )

    def test_query_tool_schema_expresses_raw_shorthand_exclusivity(self) -> None:
        with generator.SPEC.open(encoding="utf-8") as handle:
            schemas = yaml.safe_load(handle)["components"]["schemas"]

        generated = generator.mcp_query_input_schema(schemas)
        conflicts = generated["not"]["anyOf"]

        self.assertFalse(generated["additionalProperties"])
        self.assertIn(
            {
                "allOf": [
                    generator.property_has_non_null_value("queryRequest"),
                    generator.property_has_non_null_value("semanticSearch"),
                ]
            },
            conflicts,
        )
        self.assertNotIn("default", generated["properties"]["limit"])

    def test_query_tool_schema_accepts_null_optionals_but_rejects_real_mode_conflicts(self) -> None:
        with generator.SPEC.open(encoding="utf-8") as handle:
            schemas = yaml.safe_load(handle)["components"]["schemas"]

        validator = Draft202012Validator(generator.mcp_query_input_schema(schemas))
        raw_with_generated_nulls = {
            "tableName": "docs",
            "queryRequest": {
                "table": None,
                "full_text_search": {"match": "hello", "field": "body"},
                "limit": 5,
            },
            "semanticSearch": None,
            "fields": None,
            "limit": None,
        }
        self.assertEqual([], list(validator.iter_errors(raw_with_generated_nulls)))

        conflicting = copy.deepcopy(raw_with_generated_nulls)
        conflicting["semanticSearch"] = "hello"
        self.assertNotEqual([], list(validator.iter_errors(conflicting)))

        shorthand_with_null_raw = {
            "tableName": "docs",
            "queryRequest": None,
            "semanticSearch": "hello",
        }
        self.assertEqual([], list(validator.iter_errors(shorthand_with_null_raw)))


if __name__ == "__main__":
    unittest.main()
