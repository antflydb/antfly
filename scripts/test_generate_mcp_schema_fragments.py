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
        self.assertTrue(
            any(
                branch.get("not", {}).get("required") == ["group_by", "children"]
                for branch in generated["allOf"]
            )
        )

    def test_compact_query_request_keeps_cross_field_constraints(self) -> None:
        with generator.SPEC.open(encoding="utf-8") as handle:
            schemas = yaml.safe_load(handle)["components"]["schemas"]

        generated = generator.compact_query_request_schema(schemas)

        self.assertEqual({"type": "null"}, generated["properties"]["table"])
        self.assertIn("hierarchy", generated["properties"])
        invalid_states = generated["not"]["anyOf"]
        missing_group_fields = next(
            branch
            for branch in invalid_states
            if branch.get("allOf", [{}])[0].get("properties", {}).get("hierarchy", {}).get("required") == ["group_by"]
        )
        self.assertEqual(["hierarchy"], missing_group_fields["allOf"][0]["required"])
        self.assertEqual(["fields"], missing_group_fields["allOf"][1]["not"]["required"])
        self.assertTrue(
            any(
                branch.get("allOf", [{}])[0].get("properties", {}).get("hierarchy", {}).get("required") == ["children"]
                for branch in invalid_states
            )
        )
        self.assertIn(
            {"not": generator.property_has_non_null_value("table")},
            generated["allOf"],
        )

    def test_child_navigation_schema_matches_the_dedicated_runtime_mode(self) -> None:
        with generator.SPEC.open(encoding="utf-8") as handle:
            schemas = yaml.safe_load(handle)["components"]["schemas"]

        query_schema = schemas["QueryRequest"]
        child_mode_rejection = next(
            branch
            for branch in query_schema["not"]["anyOf"]
            if branch.get("description", "").startswith("Relevance, filtering, backward/offset pagination")
        )
        rejected_child_fields = {
            item["required"][0]
            for item in child_mode_rejection["allOf"][1]["anyOf"]
        }
        allowed_child_fields = {
            "table",
            "fields",
            "hierarchy",
            "limit",
            "timeout_ms",
            "order_by",
            "search_after",
        }
        self.assertEqual(
            set(query_schema["properties"]),
            allowed_child_fields | rejected_child_fields,
            "every canonical query property must be explicitly allowed or rejected for hierarchy.children",
        )

        validator = Draft202012Validator(generator.compact_query_request_schema(schemas))
        valid = {
            "fields": ["unit_id", "unit_type", "text"],
            "hierarchy": {
                "children": {
                    "parent": {"level": "source", "id": "doc:a"},
                    "level": "unit",
                }
            },
            "order_by": [{"field": "_hierarchy.position"}],
            "limit": 20,
            "search_after": ["hn1/revision/artifact/1/0/fingerprint", "artifact:unit"],
        }
        self.assertEqual([], list(validator.iter_errors(valid)))

        invalid_variants = []
        for field, value in (
            ("query", {"match_all": {}}),
            ("filter_query", {"term": {"path": "/tenant", "value": "acme"}}),
            ("offset", 0),
            ("search_before", []),
            ("analyses", {"pca": True}),
            ("limit", 101),
            ("search_after", ["position-only"]),
            ("order_by", [{"field": "_hierarchy.position", "desc": True}]),
        ):
            candidate = copy.deepcopy(valid)
            candidate[field] = value
            invalid_variants.append(candidate)
        missing_order = copy.deepcopy(valid)
        del missing_order["order_by"]
        invalid_variants.append(missing_order)

        for invalid in invalid_variants:
            self.assertNotEqual([], list(validator.iter_errors(invalid)), invalid)

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
