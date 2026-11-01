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

import tempfile
from pathlib import Path
import unittest

from report_zig_binary_size import embedded_assets, parse_symbols, symbol_report


class BinarySizeTests(unittest.TestCase):
    def test_sized_symbols_group_specializations_without_counting_undefined(self):
        symbols = parse_symbols(
            "storage.db.mutation_commit.ImplementationFor(A).apply.12 t 100 40\n"
            "storage.db.mutation_commit.ImplementationFor(B).apply.33 t 140 60\n"
            "asset r 200 300\nundefined U 0\n"
        )
        report = symbol_report(symbols)
        self.assertEqual(report["top_symbols"][0]["bytes"], 300)
        self.assertEqual(report["mutation_text_bytes"], 100)
        self.assertEqual(report["mutation_families"][0]["variants"], 2)
        self.assertEqual(report["mutation_families"][0]["function"], "apply")

    def test_asset_inventory_deduplicates_and_does_not_claim_reachability(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "fixture.bin").write_bytes(b"asset")
            (root / "a.zig").write_text(
                'const a = @embedFile("fixture.bin");\nconst b = @embedFile("fixture.bin");\nconst c = @embedFile("generated.bin");'
            )
            cache = root / ".zig-cache"
            cache.mkdir()
            (cache / "ignored.zig").write_text('@embedFile("missing.bin")')
            report = embedded_assets(root)
            self.assertTrue(report["lexical_only"])
            self.assertEqual(len(report["assets"]), 1)
            self.assertEqual(report["assets"][0]["bytes"], 5)
            self.assertEqual(len(report["assets"][0]["sites"]), 2)
            self.assertEqual(
                report["unresolved_literals"][0]["literal"], "generated.bin"
            )


if __name__ == "__main__":
    unittest.main()
