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
from jsonschema import Draft4Validator


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
        self.assertIn("maxProperties: 64, hasMaxProperties: true", generated)
        self.assertIn("minItems: 1, maxItems: 65, hasMaxItems: true", generated)
        self.assertIn(
            'allowedStrings: []string{"min_hops", "min_weight", "max_weight"}',
            generated,
        )
        self.assertIn("unsignedDecimalString: true", generated)
        self.assertIn("maximum: 10000, hasMaximum: true", generated)

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
            {
                "$ref": "#/components/schemas/GraphBindingNode",
                "maxLength": 1,
            },
            {
                "oneOf": [{"type": "string"}, {"enum": [None]}],
                "maxLength": 1,
            },
            {"allOf": [{"type": "string"}], "pattern": "^[0-9]+$"},
            {"oneOf": ["not-a-schema", {"enum": [None]}]},
            {
                "oneOf": [
                    {"type": "object"},
                    {"type": "string", "enum": [None]},
                ]
            },
            {
                "oneOf": [
                    {"type": "object"},
                    {"enum": [None], "maxLength": 1},
                ]
            },
            {
                "oneOf": [
                    {"type": "object"},
                    {"enum": [None]},
                    {"type": "null"},
                ]
            },
            {"$ref": None},
            {"allOf": ["not-a-schema"]},
            {"allOf": None},
        ):
            with self.subTest(schema=schema), self.assertRaises(ValueError):
                generator.render_shape(schema, "Unsupported")

    def test_nullable_scalar_shape_is_supported(self) -> None:
        for null_alternative in ({"enum": [None]}, {"type": "null"}):
            generated = generator.render_shape(
                {
                    "oneOf": [
                        {"type": "string", "minLength": 1},
                        null_alternative,
                    ]
                },
                "OptionalLabel",
            )
            self.assertIn("nullable: true", generated)
            self.assertIn("minLength: 1", generated)

    def test_unsupported_or_misplaced_constraints_fail_generation(self) -> None:
        for schema in (
            {"type": "string", "pattern": "^custom$"},
            {"type": "integer", "enum": [1, 2]},
            {"type": "array", "items": {"type": "string"}, "minLength": 1},
            {"type": "object", "additionalProperties": True, "minimum": 0},
            {"type": "string", "futureConstraint": 1},
            {},
        ):
            with self.subTest(schema=schema), self.assertRaises(ValueError):
                generator.render_shape(schema, "Unsupported")

    def test_graph_range_schemas_require_a_semantic_bound(self) -> None:
        schemas = generator.load_schemas()
        cases = (
            ("GraphDocumentNumericRangeBody", "min", 1, "inclusive_min"),
            ("GraphDocumentTermRangeBody", "max", "z", "inclusive_max"),
            (
                "GraphDocumentDateRangeBody",
                "start",
                "2026-01-01T00:00:00Z",
                "inclusive_start",
            ),
        )
        for name, bound, value, inclusive in cases:
            validator = Draft4Validator(schemas[name])
            with self.subTest(name=name):
                self.assertTrue(validator.is_valid({"path": "/value", bound: value}))
                self.assertFalse(validator.is_valid({"path": "/value"}))
                self.assertFalse(
                    validator.is_valid({"path": "/value", inclusive: True})
                )


if __name__ == "__main__":
    unittest.main()
