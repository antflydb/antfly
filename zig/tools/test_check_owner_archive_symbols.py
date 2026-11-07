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

"""Negative regression tests for ownership evidence."""

import unittest
import check_owner_archive_symbols as guard


class OwnerSymbols(unittest.TestCase):
    def test_undefined_references_cannot_satisfy_ownership(self):
        parsed = guard.definitions(
            "                 U _antfly_image_native_decode_v2\n                 w missing\n000001 T _jpeg.decode\n000002 t _lake_execution_build_impl.build\n"
        )
        self.assertEqual(parsed, {"jpeg.decode", "lake_execution_build_impl.build"})

    def test_wrong_owner_rejects_codec_builder_and_endpoint(self):
        inventory = {
            guard.MEDIA: guard.classify(
                {
                    family + ".decode"
                    for family in (
                        "bmp",
                        "gif",
                        "jpeg",
                        "webp",
                        "jpeg2000",
                        "jbig2",
                        "ccitt",
                        "png",
                    )
                }
                | {"reader.Reader.init", "antfly_image_native_decode_v2"}
            ),
            guard.LAKE: guard.classify({"lake_execution_build_impl.build"}),
        }
        self.assertEqual(guard.audit(inventory, {"antfly_image_native_decode_v2"}), [])
        inventory["consumer"] = guard.classify(
            {
                "jpeg.decode",
                "lake_execution_build_impl.build",
                "antfly_image_native_decode_v2",
            }
        )
        self.assertEqual(
            len(guard.audit(inventory, {"antfly_image_native_decode_v2"})), 3
        )
        inventory["consumer"] = guard.classify({"png.encodeRgba"})
        self.assertEqual(guard.audit(inventory, {"antfly_image_native_decode_v2"}), [])

    def test_qualification_dispatch_is_forbidden_in_production(self):
        errors = guard.audit(
            {
                "consumer": guard.classify(
                    {"runtime_io_abi.dispatchSwitchForQualification"}
                )
            },
            set(),
        )
        self.assertIn(
            "consumer defines qualification-only alternate IO dispatch", errors
        )

    def test_missing_definitions_never_pass(self):
        self.assertEqual(len(guard.audit({}, {"antfly_pdf_native_open_v3"})), 4)


if __name__ == "__main__":
    unittest.main()
