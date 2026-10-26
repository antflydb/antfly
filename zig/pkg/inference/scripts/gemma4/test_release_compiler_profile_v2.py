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

import hashlib
import unittest

import context_release_build_adapter as adapter
import record_context_release_build as recorder
import release_attention_matching as release
import test_release_full_build_provenance as provenance_fixture

PROFILE_LINE = 'context_compiler_profile: {"schema_version":1,"strip_debug_info":true,"have_error_return_tracing":false}\n'


class CompilerProfileParser(unittest.TestCase):
    def tearDown(self):
        recorder.configure_profile(False)

    def test_exact_supported_profile_and_command_selection(self):
        self.assertEqual(recorder.parse_compiler_profile(PROFILE_LINE),
                         recorder.EXPECTED_STRIPPED_COMPILER_PROFILE)
        recorder.configure_profile(True)
        self.assertEqual(recorder.SCHEMA, recorder.SCHEMA_V3)
        self.assertEqual(recorder.COMMAND, [*recorder.CURRENT_MAIN_BASE_COMMAND, '-Dstrip=true'])
        self.assertNotIn('-Donnx=false', recorder.COMMAND)
        self.assertNotIn('onnx', recorder.BUILD)
        self.assertIs(recorder.BUILD['command'], recorder.COMMAND)
        recorder.configure_profile(False)
        self.assertEqual(recorder.SCHEMA, recorder.SCHEMA_V1)
        self.assertEqual(recorder.COMMAND, recorder.BASE_COMMAND)

    def test_missing_duplicate_mismatch_and_boolean_schema_version_are_rejected(self):
        invalid = [
            '',
            PROFILE_LINE + PROFILE_LINE,
            'context_compiler_profile: {"schema_version":1,"strip_debug_info":false,"have_error_return_tracing":false}\n',
            'context_compiler_profile: {"schema_version":true,"strip_debug_info":true,"have_error_return_tracing":false}\n',
        ]
        for raw in invalid:
            with self.subTest(raw=raw), self.assertRaises(RuntimeError):
                recorder.parse_compiler_profile(raw)


class FullReleaseCompilerProfile(unittest.TestCase):
    def setUp(self):
        self.fixture = provenance_fixture.FullReleaseBuild(methodName='test_valid_full_release_build_evidence')
        self.fixture.setUp()

    def tearDown(self):
        self.fixture.tearDown()

    def make_v2(self):
        f = self.fixture
        f.value['schema'] = release.FULL_RELEASE_SCHEMA_V2
        f.value['compiler_profile'] = dict(release.FULL_RELEASE_EXPECTED_STRIPPED_PROFILE)
        f.command.append('-Dstrip=true')
        (f.root / 'stderr').write_text(PROFILE_LINE + '12 selected; 10 passed; 2 skipped.\n')
        f.value['stderr']['sha256'] = hashlib.sha256((f.root / 'stderr').read_bytes()).hexdigest()
        f.write('resource', f.resource)
        f.value['resource']['sha256'] = hashlib.sha256((f.root / 'resource').read_bytes()).hexdigest()
        f.archive.unlink()
        f.write_archive()
        f.value['source_archive'].update(
            sha256=hashlib.sha256(f.archive.read_bytes()).hexdigest(),
            bytes=f.archive.stat().st_size,
        )
        f.refresh_receipt()

    def test_valid_v2_and_adapter_use_same_strict_auditor(self):
        self.make_v2()
        self.assertTrue(self.fixture.audit()['release_build_evidence'])
        got = adapter.validate(self.fixture.root / 'receipt', self.fixture.root / 'bin')
        self.assertEqual(got['receipt']['schema'], release.FULL_RELEASE_SCHEMA_V2)

    def test_valid_current_main_v3_preserves_historical_v2_contract(self):
        self.make_v2()
        self.fixture.value['schema'] = release.FULL_RELEASE_SCHEMA_V3
        self.fixture.command[:] = release.FULL_RELEASE_CURRENT_STRIPPED_COMMAND
        self.fixture.value['build'].pop('onnx')
        self.fixture.write('resource', self.fixture.resource)
        self.fixture.value['resource']['sha256'] = hashlib.sha256((self.fixture.root / 'resource').read_bytes()).hexdigest()
        self.fixture.archive.unlink()
        self.fixture.write_archive()
        self.fixture.value['source_archive'].update(
            sha256=hashlib.sha256(self.fixture.archive.read_bytes()).hexdigest(),
            bytes=self.fixture.archive.stat().st_size,
        )
        self.fixture.refresh_receipt()
        got = adapter.validate(self.fixture.root / 'receipt', self.fixture.root / 'bin')
        self.assertEqual(got['receipt']['schema'], release.FULL_RELEASE_SCHEMA_V3)
        self.assertNotIn('-Donnx=false', got['receipt']['build']['command'])

    def test_numeric_receipt_booleans_are_rejected(self):
        self.make_v2()
        self.fixture.value['compiler_profile'] = {
            'strip_debug_info': 1,
            'have_error_return_tracing': 0,
        }
        self.fixture.refresh_receipt()
        with self.assertRaisesRegex(ValueError, 'invalid stripped compiler profile'):
            self.fixture.audit()

    def test_missing_duplicate_and_numeric_observations_are_rejected(self):
        self.make_v2()
        cases = [
            '12 selected; 10 passed; 2 skipped.\n',
            PROFILE_LINE + PROFILE_LINE + '12 selected; 10 passed; 2 skipped.\n',
            'context_compiler_profile: {"schema_version":1,"strip_debug_info":1,"have_error_return_tracing":0}\n12 selected; 10 passed; 2 skipped.\n',
        ]
        for raw in cases:
            with self.subTest(raw=raw):
                (self.fixture.root / 'stderr').write_text(raw)
                self.fixture.value['stderr']['sha256'] = hashlib.sha256((self.fixture.root / 'stderr').read_bytes()).hexdigest()
                self.fixture.refresh_receipt()
                with self.assertRaises(ValueError):
                    self.fixture.audit()

    def test_diagnostic_receipt_cannot_be_relabelled(self):
        self.make_v2()
        self.fixture.value['release_eligible'] = False
        self.fixture.value['evidence_scope'] = 'numerical_capture_and_metal_lifecycle_only'
        self.fixture.refresh_receipt()
        with self.assertRaisesRegex(ValueError, 'release-ineligible or scoped'):
            self.fixture.audit()

    def test_v1_schema_and_command_are_preserved_and_profile_lines_ignored(self):
        # New test output is allowed in a v1 build without changing its exact receipt schema.
        raw = PROFILE_LINE + PROFILE_LINE + '12 selected; 10 passed; 2 skipped.\n'
        (self.fixture.root / 'stderr').write_text(raw)
        self.fixture.value['stderr']['sha256'] = hashlib.sha256((self.fixture.root / 'stderr').read_bytes()).hexdigest()
        self.fixture.refresh_receipt()
        self.assertTrue(self.fixture.audit()['release_build_evidence'])
        self.assertEqual(set(self.fixture.value), release.FULL_RELEASE_BUILD_FIELDS_V1)
        self.assertEqual(self.fixture.value['build']['command'], release.FULL_RELEASE_COMMAND)


if __name__ == '__main__':
    unittest.main()
