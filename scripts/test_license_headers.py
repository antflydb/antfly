#!/usr/bin/env python3
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

"""Ensure moved ABI and inference files retain their declared license."""

import unittest

import license_headers


class MovedSourceLicenseTests(unittest.TestCase):
    def test_moved_spdx_only_files_match_header_tool_classification(self):
        paths = (
            "zig/lib/runtime/src/runtime_io_abi.zig",
            "zig/lib/runtime/src/runtime_native_abi.zig",
            "zig/pkg/inference/src/host/embedding_wire.zig",
            "zig/pkg/inference/src/host/provider_failure.zig",
        )
        for path in paths:
            with self.subTest(path=path):
                header = (license_headers.ROOT / path).read_text().split("\n\n", 1)[0]
                declared = (
                    header.split("SPDX-License-Identifier:", 1)[1]
                    .splitlines()[0]
                    .strip()
                )
                expected = "apache" if declared == "Apache-2.0" else "elv2"
                self.assertEqual(license_headers.group_for(path, "all"), expected)


if __name__ == "__main__":
    unittest.main()
