# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from __future__ import annotations

import unittest

import generate_go_graph_result_shapes as generator


class GoGraphResultShapesTest(unittest.TestCase):
    def test_required_presence_and_nested_shapes_come_from_openapi(self) -> None:
        schemas = generator.load_schemas()
        generated = generator.render_go(schemas)
        self.assertIn(
            'required: []string{"kind", "nodes", "paths", "stats"}', generated
        )
        self.assertIn('required: []string{"returned_items", "truncated"}', generated)
        self.assertIn("generatedGraphPathEndpointShape", generated)
        self.assertIn("nullable: true", generated)
        self.assertIn("allowAdditionalProperties: true", generated)
        self.assertIn(
            'name: "GraphBindingNode.document", opaqueObject: true', generated
        )

    def test_all_canonical_roots_and_dependencies_are_reachable(self) -> None:
        schemas = generator.load_schemas()
        reachable = set(generator.reachable_schema_names(schemas))
        self.assertTrue(set(generator.ROOT_SCHEMAS).issubset(reachable))
        self.assertTrue(
            {
                "GraphBindingNode",
                "GraphResultBinding",
                "GraphPath",
                "GraphResultNode",
                "GraphQueryStats",
            }.issubset(reachable)
        )

    def test_unsupported_compositions_fail_generation(self) -> None:
        for schema in (
            {"oneOf": [{"type": "string"}, {"type": "integer"}]},
            {"anyOf": [{"type": "string"}, {"type": "integer"}]},
            {"allOf": [{"type": "object"}, {"type": "object"}]},
        ):
            with self.subTest(schema=schema), self.assertRaises(ValueError):
                generator.render_shape(schema, "Unsupported")


if __name__ == "__main__":
    unittest.main()
