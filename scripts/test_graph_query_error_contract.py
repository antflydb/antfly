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

"""Every reason emitted by graph admission must be a published error value."""

import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SAFE_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


class GraphQueryErrorContractTests(unittest.TestCase):
    def test_runtime_reasons_are_in_source_and_distributed_specs(self):
        source = (
            ROOT / "zig/pkg/antfly/src/api/graph_query_diagnostic.zig"
        ).read_text()
        enum = re.search(r"pub const Reason = enum\s*\{(.*?)\};", source, re.S)
        self.assertIsNotNone(enum)
        reasons = set(re.findall(r"^\s*([a-z][a-z0-9_]*)\s*,", enum.group(1), re.M))
        self.assertTrue(reasons)
        for path in (
            "specs/openapi/antfly/metadata.yaml",
            "openapi.yaml",
            "rs/crates/sdk/openapi.yaml",
        ):
            with self.subTest(path=path):
                document = yaml.load((ROOT / path).read_text(), Loader=SAFE_LOADER)
                published = set(
                    document["components"]["schemas"]["GraphQueryUnsupportedError"][
                        "properties"
                    ]["reason"]["enum"]
                )
                self.assertFalse(
                    reasons - published,
                    f"unpublished runtime reasons: {sorted(reasons - published)}",
                )


if __name__ == "__main__":
    unittest.main()
