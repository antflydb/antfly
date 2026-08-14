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

import json
import unittest

import yaml

from scripts import generate_mcp_schema_fragments as generator


class McpSchemaFragmentTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
