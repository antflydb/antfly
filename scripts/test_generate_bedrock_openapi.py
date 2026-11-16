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

"""Contract tests for the AWS Smithy -> restJson1 wire projection."""

import copy
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator, ValidationError
from openapi_spec_validator import validate

with patch.object(sys, "path", [str(Path(__file__).resolve().parent), *sys.path]):
    import generate_bedrock_openapi as exporter


class BedrockProjectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = json.loads(exporter.MODEL.read_text())
        cls.spec = exporter.export(cls.model)

    def validator(self, name):
        return Draft202012Validator(
            {
                "$ref": f"#/components/schemas/{name}",
                "components": self.spec["components"],
            }
        )

    def test_is_valid_openapi_and_reproducible(self):
        validate(self.spec)
        self.assertEqual(exporter.OUTPUT.read_text(), exporter.render(self.spec))

    def test_http_label_is_not_in_json_body(self):
        for operation in exporter.OPERATIONS:
            path = "/model/{modelId}/" + (
                "converse-stream" if operation == "ConverseStream" else "converse"
            )
            endpoint = self.spec["paths"][path]["post"]
            self.assertEqual(endpoint["parameters"][0]["name"], "modelId")
            self.assertEqual(endpoint["parameters"][0]["in"], "path")
            self.assertNotIn(
                "modelId",
                self.spec["components"]["schemas"][operation + "RequestBody"][
                    "properties"
                ],
            )

    def test_stream_is_binary_and_events_are_separate(self):
        response = self.spec["paths"]["/model/{modelId}/converse-stream"]["post"][
            "responses"
        ]["200"]
        schema = response["content"]["application/vnd.amazon.eventstream"]["schema"]
        self.assertEqual(schema["format"], "binary")
        self.validator("ConverseStreamOutput").validate(
            {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": "hi"}}}
        )
        with self.assertRaises(ValidationError):
            self.validator("ConverseStreamOutput").validate(
                {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": 42}}}
            )

    def test_unions_require_one_known_non_null_variant(self):
        validator = self.validator("ContentBlock")
        validator.validate({"text": "hi"})
        validator.validate(
            {"image": {"format": "png", "source": {"bytes": "aGVsbG8="}}}
        )
        for invalid in (
            {},
            {"text": None},
            {"unknown": "hi"},
            {
                "text": "hi",
                "toolUse": {"toolUseId": "c", "name": "lookup", "input": {}},
            },
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                validator.validate(invalid)

    def test_required_response_metrics_and_usage_are_preserved(self):
        response = {
            "output": {"message": {"role": "assistant", "content": [{"text": "hi"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
            "metrics": {"latencyMs": 5},
        }
        validator = self.validator("ConverseResponse")
        validator.validate(response)
        for key in ("metrics", "usage"):
            invalid = copy.deepcopy(response)
            del invalid[key]
            with self.subTest(key=key), self.assertRaises(ValidationError):
                validator.validate(invalid)
        response["usage"]["inputTokens"] = -1
        with self.assertRaises(ValidationError):
            validator.validate(response)

    def test_documents_stay_arbitrary_json_and_blobs_stay_base64_strings(self):
        validator = self.validator("Document")
        for value in (None, True, 42, "text", [1, 2], {"schema": {"type": "object"}}):
            validator.validate(value)
        self.assertEqual(self.spec["components"]["schemas"]["Blob"]["format"], "byte")

    def test_unsupported_reachable_type_fails_instead_of_widening(self):
        model = copy.deepcopy(self.model)
        model["shapes"][exporter.NAMESPACE + "ConverseRequest"]["members"][
            "newField"
        ] = {"target": exporter.NAMESPACE + "NewType"}
        model["shapes"][exporter.NAMESPACE + "NewType"] = {"type": "timestamp"}
        with self.assertRaisesRegex(ValueError, "Unsupported reachable Smithy type"):
            exporter.export(model)


if __name__ == "__main__":
    unittest.main()
