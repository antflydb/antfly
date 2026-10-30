# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

from __future__ import annotations

import re
import unittest

import yaml

import generate_graph_identifier_policy as generator
from openapi_inputs import load_yaml
from openapi_joiner import resolve_pointer, resolve_ref_path, split_ref


class GraphIdentifierPolicyTest(unittest.TestCase):
    def test_conformance_cases_match_generated_policy(self) -> None:
        policy, ranges = generator.load_policy()
        self.assertEqual("15.0.0", policy["unicode_version"])
        self.assertEqual(128, policy["max_code_points"])
        self.assertEqual(512, policy["max_utf8_bytes"])
        for case in policy["conformance_cases"]:
            with self.subTest(case=case["name"]):
                self.assertEqual(
                    case["valid"],
                    generator.valid_identifier(policy, ranges, case["value"]),
                )

    def test_openapi_uses_the_shared_identifier_schema_everywhere(self) -> None:
        generated = yaml.safe_load(generator.render_openapi(*generator.load_policy()))[
            "components"
        ]["schemas"]
        identifier = generated["GraphIdentifier"]
        self.assertEqual({"enum": ["*"]}, identifier["not"])
        self.assertIn(
            generated["GraphCountTarget"]["x-antfly-count-target"]["rowSentinel"],
            identifier["not"]["enum"],
        )

        indexes = yaml.safe_load(
            (generator.ROOT / "specs/openapi/antfly/indexes.yaml").read_text(
                encoding="utf-8"
            )
        )["components"]["schemas"]
        identifier_ref = {
            "$ref": "generated/graph_identifier.yaml#/components/schemas/GraphIdentifier"
        }
        self.assertEqual(
            identifier_ref, indexes["GraphMatchEdge"]["properties"]["from"]
        )
        self.assertEqual(identifier_ref, indexes["GraphMatchEdge"]["properties"]["to"])
        self.assertEqual(
            identifier_ref, indexes["GraphAliasOperand"]["properties"]["alias"]
        )
        self.assertEqual(identifier_ref, indexes["GraphMatch"]["properties"]["anchor"])
        self.assertEqual(
            identifier_ref,
            indexes["GraphBindingsReturn"]["properties"]["bindings"]["items"],
        )
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
            indexes["GraphMatch"]["properties"]["nodes"][
                "x-antfly-property-name-schema"
            ],
        )
        self.assertEqual(
            identifier_ref,
            indexes["GraphOptionalMatch"]["properties"]["nodes"][
                "x-antfly-property-name-schema"
            ],
        )
        self.assertEqual(
            identifier_ref,
            indexes["GraphAggregatesReturn"]["properties"]["aggregates"][
                "x-antfly-property-name-schema"
            ],
        )
        self.assertEqual(1, indexes["GraphResultRow"]["minProperties"])
        self.assertEqual(
            identifier_ref,
            indexes["GraphResultRow"]["x-antfly-property-name-schema"],
        )
        self.assertEqual(
            identifier_ref,
            indexes["GraphQueries"]["x-antfly-property-name-schema"],
        )

    def test_graph_execution_defaults_match_runtime_limits(self) -> None:
        # Follow the server's public configuration path rather than assuming
        # that an externally referenced policy lives in config.yaml itself.
        documents = {}

        def resolve(path, schema):
            seen = set()
            while "$ref" in schema:
                reference = schema["$ref"]
                self.assertNotIn((path, reference), seen, "cyclic policy reference")
                seen.add((path, reference))
                relative, pointer = split_ref(reference)
                if relative:
                    path = resolve_ref_path(relative, path)
                if path not in documents:
                    documents[path] = load_yaml(path)
                schema = resolve_pointer(documents[path], pointer.removeprefix("#"))
            return path, schema

        path = generator.ROOT / "specs/openapi/antfly/config.yaml"
        documents[path] = load_yaml(path)
        config = documents[path]["components"]["schemas"]["Config"]
        for field in ("resources", "query", "request_limits", "graph"):
            path, config = resolve(path, config["properties"][field])
        config = config["properties"]
        policy = (
            generator.ROOT / "zig/lib/platform/src/resource_policy.zig"
        ).read_text(encoding="utf-8")
        graph_limits = re.search(
            r"pub const GraphLimits = struct \{(.*?)\n    pub fn", policy, re.DOTALL
        )
        self.assertIsNotNone(graph_limits, "missing canonical GraphLimits")
        defaults = dict(
            re.findall(r"^    (\w+): usize = ([^,]+),$", graph_limits[1], re.MULTILINE)
        )
        self.assertTrue(defaults, "GraphLimits must expose nonempty defaults")
        self.assertEqual(
            set(config), set(defaults), "OpenAPI must cover every graph limit"
        )
        runtime = (
            generator.ROOT / "zig/pkg/antfly-embedded/src/graph/work_budget.zig"
        ).read_text(encoding="utf-8")

        for name, schema in config.items():
            constant = f"default_{name}"
            match = re.search(
                rf"pub const {re.escape(constant)}: usize = ([^;]+);",
                runtime,
            )
            self.assertIsNotNone(match, f"missing runtime default {constant}")
            for source, expression in (
                ("canonical GraphLimits", defaults[name]),
                (constant, match.group(1)),
            ):
                with self.subTest(field=name, source=source):
                    factors = [
                        int(factor.strip().replace("_", ""))
                        for factor in expression.split("*")
                    ]
                    runtime_default = 1
                    for factor in factors:
                        runtime_default *= factor
                    self.assertEqual(
                        schema["default"],
                        runtime_default,
                        f"OpenAPI default for {name} drifted from {source}",
                    )


if __name__ == "__main__":
    unittest.main()
