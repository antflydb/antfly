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

from report_zig_archive_work import category, family, parse_text_symbols, report


class ArchiveWorkTests(unittest.TestCase):
    def test_elf_function_sections_with_zero_addresses_remain_distinct(self):
        rows, estimated = parse_text_symbols(
            "a.a[obj.o]: first t 0 50\na.a[obj.o]: second T 0 80\na.a[obj.o]: label t 0 0\n"
        )
        self.assertFalse(estimated)
        self.assertEqual([r["bytes"] for r in rows], [50, 80])

    def test_macho_aliases_count_once_and_do_not_cross_objects(self):
        rows, estimated = parse_text_symbols(
            "a.a[one.o]: ltmp0 t 0 0\na.a[one.o]: l_first t 0 0\na.a[one.o]: l_alias t 0 0\na.a[one.o]: l_next t 30 0\na.a[two.o]: l_other t 0 0\na.a[two.o]: l_end t 10 0\n"
        )
        self.assertTrue(estimated)
        self.assertEqual(sorted(r["bytes"] for r in rows), [0, 0, 10, 30])
        self.assertEqual(sum(r["extent_unknown"] for r in rows), 2)
        self.assertTrue(any(r["aliases"] for r in rows))
        self.assertTrue(all(r["estimated"] for r in rows))

    def test_names_with_spaces_and_undefined_symbols(self):
        rows, _ = parse_text_symbols(
            "a.o: foo.Adapter(A, B).run t 10 100\na.o: external U 0 0\na.o: data d 0 20\n"
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "foo.Adapter(A, B).run")

    def test_overlap_requires_distinct_owners_and_labels_estimates(self):
        a, _ = parse_text_symbols("a.o: l_std.json.Stringify.write__anon_123 t 0 100\n")
        b, _ = parse_text_symbols("b.o: l_std.json.Stringify.write__anon_456 t 0 90\n")
        value = report([("a", a, False), ("b", b, False)])
        self.assertEqual(value["repeated_named_families"][0]["bytes"], 190)
        self.assertEqual(value["units"][0]["categories"]["json_encoding"], 100)
        self.assertEqual(report([("a", a + a, False)])["repeated_named_families"], [])
        self.assertEqual(family("l_foo.apply.123"), "foo.apply#")
        self.assertEqual(
            family("local_query_provider.execute"), "local_query_provider.execute"
        )
        self.assertEqual(category("storage.db.mutation_plan.prepare"), "mutation")
        self.assertEqual(
            category("types.openApiParseObject__anon_12"), "generated_contracts"
        )
        self.assertEqual(category("jpeg2000.codestream.parseState"), "image_codecs")


if __name__ == "__main__":
    unittest.main()
