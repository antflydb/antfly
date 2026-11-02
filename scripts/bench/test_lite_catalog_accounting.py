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

"""Cross-check offline payload accounting against the native integrity report.

Run with ANTFLY_LIBRARY pointing at a current native library. Fixtures stay in
this worktree and include external values, named SQL rows and reserved-looking
public IDs; corruption must still be detected independently of native checks.
"""

import os
from pathlib import Path
import tempfile
import unittest

from lite_catalog_accounting import CatalogAccounting
from lite_contention import sdk

ROOT = Path(__file__).resolve().parents[2]
LIBRARY = os.environ.get("ANTFLY_LIBRARY")


@unittest.skipUnless(LIBRARY and Path(LIBRARY).is_file(), "ANTFLY_LIBRARY is required")
class NativeAccountingParity(unittest.TestCase):
    def test_physical_categories_match_native_and_reject_corruption(self):
        directory = ROOT / ".tools" / "accounting-tests"
        directory.mkdir(parents=True, exist_ok=True)
        af = sdk(LIBRARY)
        with tempfile.TemporaryDirectory(dir=directory) as temporary:
            database = Path(temporary) / "accounting.aflite"
            with af.create(database, no_sync=True) as db:
                db.batch_json(
                    {
                        "inserts": {
                            "plain": {"text": "ordinary row"},
                            "id\x00\x00__txn_records__:still-authored": {
                                "text": "x" * 8192
                            },
                        }
                    }
                )
                db.sql("CREATE TABLE accounting_rows(n BIGINT, body TEXT)")
                db.sql(
                    "INSERT INTO accounting_rows(_id,n,body) VALUES ($1,7,$2)",
                    ["row__columnar__:still-authored", "y" * 8192],
                )
            with af.open_readonly(database) as db:
                native = db.check()
                self.assertTrue(native["valid"])
            source = CatalogAccounting(database)
            try:
                offline = source.account(include_documents=True)
                document_index = source.slot[7]
                page_size = source.page_size
            finally:
                source.close()
            for label, expected in native["live_accounting"].items():
                self.assertEqual(
                    {key: offline[label][key] for key in expected}, expected, label
                )
            self.assertEqual(offline["live_bytes"], native["live_bytes"])
            self.assertEqual(offline["live_records"], native["live_file_count"])
            self.assertGreater(offline["documents"]["records"], 0)
            self.assertGreater(offline["control_documents"]["records"], 0)
            self.assertGreater(document_index, 0)
            # Tamper with the actual index page, leaving its stored CRC intact.
            with database.open("r+b") as output:
                offset = document_index * page_size + 16
                output.seek(offset)
                byte = output.read(1)
                output.seek(offset)
                output.write(bytes([byte[0] ^ 1]))
            source = CatalogAccounting(database)
            try:
                with self.assertRaisesRegex(ValueError, "checksum"):
                    source.account(include_documents=True)
            finally:
                source.close()


if __name__ == "__main__":
    unittest.main()
