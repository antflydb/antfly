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

"""Keep source-oracle compiler work separate from native-client cache evidence."""

import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "check_image_owner_cache", Path(__file__).with_name("check_image_owner_cache.py")
)
assert SPEC and SPEC.loader
cache = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cache)


def attribution(provider="success", native="cached", source="success"):
    return "\n".join(
        (
            f"+- compile lib antfly-runtime-enrichment_compute debug native-native {provider} 1s",
            f"+- compile test_obj image-native-owner-tests debug native-native {native} 1s",
            f"+- compile test_obj image-source-oracle-tests debug native-native {source} 1s",
        )
    )


class NativeImageCompilerAttribution(unittest.TestCase):
    def test_codec_edit_tracks_source_rebuild_without_charging_native_client(self):
        states, errors = cache.compiler_states(attribution(), "debug")
        self.assertEqual(errors, [])
        self.assertEqual(states, cache.expected_states("jpeg"))
        self.assertEqual(states, cache.expected_states("webp"))

    def test_source_oracle_alone_cannot_stand_in_for_native_client(self):
        output = "\n".join(
            row
            for row in attribution().splitlines()
            if "test_obj image-native-owner-tests " not in row
        )
        states, errors = cache.compiler_states(output, "debug")
        self.assertNotIn("native_client", states)
        self.assertTrue(any("image-native-owner-tests" in error for error in errors))

    def test_repeated_or_ambiguous_compiler_names_do_not_pass(self):
        for status in ("cached", "success"):
            output = (
                attribution()
                + f"\ncompile test_obj image-native-owner-tests debug native-native {status} 1s"
            )
            states, errors = cache.compiler_states(output, "debug")
            self.assertNotIn("native_client", states)
            self.assertTrue(errors)

    def test_cached_unrelated_owner_is_still_an_invalid_dependency(self):
        for owner in (
            "antfly-storage-engine",
            "antfly-storage-kernel",
            "antfly-inference-core",
        ):
            with self.subTest(owner=owner):
                _, errors = cache.compiler_states(
                    attribution() + f"\ncompile lib {owner} debug native-native cached",
                    "debug",
                )
                self.assertIn(
                    f"Image-only test pulled unrelated owner: {owner}", errors
                )

    def test_provider_code_is_separate_from_contract_invalidation(self):
        self.assertEqual(
            cache.expected_states("provider_code"),
            {
                "provider": "success",
                "native_client": "cached",
                "source_oracle": "cached",
            },
        )
        for phase in ("values", "contract", "protocol"):
            with self.subTest(phase=phase):
                self.assertEqual(
                    set(cache.expected_states(phase).values()), {"success"}
                )
        self.assertEqual(set(cache.expected_states("warm").values()), {"cached"})
        self.assertEqual(
            cache.expected_states("facade"),
            {
                "provider": "cached",
                "native_client": "success",
                "source_oracle": "cached",
            },
        )


if __name__ == "__main__":
    unittest.main()
