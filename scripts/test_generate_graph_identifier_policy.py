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

import yaml

import generate_graph_identifier_policy as generator


class GraphIdentifierPolicyTest(unittest.TestCase):
    def test_conformance_cases_match_generated_policy(self) -> None:
        policy, ranges = generator.load_policy()
        self.assertEqual("15.0.0", policy["unicode_version"])
        self.assertEqual(128, policy["max_code_points"])
        self.assertEqual(512, policy["max_utf8_bytes"])
        for case in policy["conformance_cases"]:
            with self.subTest(case=case["name"]):
                self.assertEqual(case["valid"], generator.valid_identifier(policy, ranges, case["value"]))

    def test_openapi_uses_the_shared_identifier_schema_everywhere(self) -> None:
        indexes = yaml.safe_load(
            (generator.ROOT / "specs/openapi/antfly/indexes.yaml").read_text(encoding="utf-8")
        )["components"]["schemas"]
        metadata = yaml.safe_load(
            (generator.ROOT / "specs/openapi/antfly/metadata.yaml").read_text(encoding="utf-8")
        )["components"]["schemas"]

        identifier_ref = {
            "$ref": "generated/graph_identifier.yaml#/components/schemas/GraphIdentifier"
        }
        self.assertEqual(identifier_ref, indexes["GraphMatchEdge"]["properties"]["from"])
        self.assertEqual(identifier_ref, indexes["GraphMatchEdge"]["properties"]["to"])
        self.assertEqual(identifier_ref, indexes["GraphAliasOperand"]["properties"]["alias"])
        self.assertEqual(identifier_ref, indexes["GraphMatch"]["properties"]["anchor"])
        self.assertEqual(identifier_ref, indexes["GraphBindingsReturn"]["properties"]["bindings"]["items"])
        selector = indexes["GraphResultRefNodeSelector"]
        self.assertEqual(identifier_ref, selector["properties"]["binding"])
        self.assertIn("prior MATCH result", selector["description"])
        self.assertEqual(
            identifier_ref,
            indexes["GraphResultRefNodeSelector"]["properties"]["result_ref"][
                "x-antfly-graph-result-name-schema"
            ],
        )
        self.assertEqual(
            identifier_ref,
            indexes["GraphMatch"]["properties"]["nodes"]["x-antfly-property-name-schema"],
        )
        self.assertEqual(
            identifier_ref,
            indexes["GraphOptionalMatch"]["properties"]["nodes"]["x-antfly-property-name-schema"],
        )
        self.assertEqual(
            identifier_ref,
            indexes["GraphAggregatesReturn"]["properties"]["aggregates"]["x-antfly-property-name-schema"],
        )
        self.assertEqual(
            identifier_ref,
            metadata["QueryRequest"]["properties"]["graph_queries"][
                "x-antfly-property-name-schema"
            ],
        )


if __name__ == "__main__":
    unittest.main()
