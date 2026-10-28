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

import unittest

import join_public_openapi


class ResourcePolicyContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schemas = join_public_openapi.join_specs()["components"]["schemas"]

    def resolve(self, schema):
        while True:
            if "$ref" in schema:
                reference = schema["$ref"]
                self.assertTrue(
                    reference.startswith("#/components/schemas/"), reference
                )
                schema = self.schemas[reference.rsplit("/", 1)[1]]
            elif len(schema.get("allOf", [])) == 1:
                schema = schema["allOf"][0]
            else:
                return schema

    def test_public_inference_config_uses_canonical_resource_policy(self):
        config = self.resolve(self.schemas["InferenceConfig"])["properties"]
        self.assertEqual(set(config), {"resources", "inference"})
        resources = self.resolve(config["resources"])["properties"]
        self.assertEqual(set(resources), {"memory", "query", "write", "inference"})
        inference = self.resolve(resources["inference"])["properties"]
        limits = self.resolve(inference["request_limits"])["properties"]
        self.assertEqual(
            set(limits),
            {
                "max_items_per_request",
                "max_input_tokens_per_item",
                "max_output_tokens_per_item",
                "max_decoded_pixels_per_batch",
            },
        )
        execution = self.resolve(config["inference"])["properties"]
        self.assertNotIn("max_concurrent_requests", execution)
        self.assertNotIn("admission", execution)

    def test_logging_config_does_not_resolve_to_server_config(self):
        server = self.resolve(self.schemas["InferenceConfig"])["properties"]
        execution = self.resolve(server["inference"])["properties"]
        logging = self.resolve(execution["log"])["properties"]
        self.assertIn("level", logging)
        self.assertIn("style", logging)
        self.assertNotIn("resources", logging)


if __name__ == "__main__":
    unittest.main()
