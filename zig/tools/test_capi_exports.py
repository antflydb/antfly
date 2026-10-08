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

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('capi_exports', Path(__file__).with_name('capi_exports.py'))
exports = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exports)


class ExportPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.header = self.root / 'antfly.h'
        self.header.write_text('''
/* Calling antfly_comment_only() is not an API declaration. */
// antfly_line_comment()
typedef void (*antfly_callback)(void *);
int antfly_first(void);
void antfly_second(
    int argument);
''')

    def test_manifest_is_exact_and_excludes_callbacks_and_comments(self):
        self.assertEqual(exports.public_symbols(self.header), {'antfly_first', 'antfly_second'})
        macho, elf = self.root / 'exports', self.root / 'map'
        exports.manifest(self.header, macho, 'macho')
        exports.manifest(self.header, elf, 'elf')
        self.assertEqual(macho.read_text(), '_antfly_first\n_antfly_second\n')
        self.assertIn('antfly_first;', elf.read_text())
        self.assertIn('local: *;', elf.read_text())
        self.assertNotIn('antfly_*', elf.read_text())

    def test_checker_rejects_internal_exports_even_with_antfly_prefix(self):
        for name in ['antfly_private', 'termite_metal_buffer_alloc', '__dso_handle', 'memcpy']:
            with self.subTest(symbol=name), patch.object(exports, 'exported_symbols', return_value={'antfly_first', 'antfly_second', name}):
                with self.assertRaisesRegex(ValueError, 'unexpected='):
                    exports.check(self.header, self.root / 'lib', 'macho')

    def test_checker_rejects_missing_public_functions(self):
        with patch.object(exports, 'exported_symbols', return_value={'antfly_first'}):
            with self.assertRaisesRegex(ValueError, 'antfly_second'):
                exports.check(self.header, self.root / 'lib', 'elf')

    def test_nm_normalization_preserves_internal_leaks(self):
        output = '_antfly_first T 100 0\n___dso_handle D 200 0\n'
        with patch.object(exports.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, output, '')):
            self.assertEqual(exports.exported_symbols(self.root / 'lib', 'macho', 'nm'), {'antfly_first', '__dso_handle'})

    @unittest.skipUnless(platform.system() in {'Darwin', 'Linux'} and (shutil.which('clang') or shutil.which('cc')) and shutil.which('ar'), 'native compiler and archiver required')
    def test_native_link_policy_and_executable_and_shared_consumers(self):
        self.header.write_text('''
#include <stdint.h>
typedef int antfly_error_code;
typedef struct { uint32_t size; } antfly_open_options;
#define ANTFLY_OK 0
uint32_t antfly_abi_version(void);
antfly_error_code antfly_open_options_init(antfly_open_options *);
''')
        source = self.root / 'api.c'
        source.write_text('''
#include "antfly.h"
uint32_t antfly_abi_version(void) { return 1; }
int antfly_open_options_init(antfly_open_options *options) { options->size = sizeof(*options); return 0; }
int antfly_private(void) { return 7; }
int termite_metal_buffer_alloc(void) { return 8; }
''')
        object_file = self.root / 'api.o'
        cc = shutil.which('clang') or shutil.which('cc')
        subprocess.run([cc, '-fPIC', '-c', str(source), '-o', str(object_file)], check=True)
        macho = platform.system() == 'Darwin'
        fmt = 'macho' if macho else 'elf'
        manifest = self.root / 'exports'
        exports.manifest(self.header, manifest, fmt)
        library = self.root / ('libantfly.dylib' if macho else 'libantfly.so')
        if macho:
            sdk = Path(subprocess.check_output(['xcrun', '--show-sdk-path'], text=True).strip())
            archive = self.root / 'api.a'
            subprocess.run(['ar', 'rcs', str(archive), str(object_file)], check=True)
            exports.link_macho(argparse.Namespace(sdk=sdk, linker='/usr/bin/ld', arch='arm64' if platform.machine() == 'arm64' else 'x86_64', deployment='11.0', exports=manifest, output=library, archive=archive, link_args=[str(object_file)]))
        else:
            subprocess.run([cc, '-shared', str(object_file), '-Xlinker', '--version-script', '-Xlinker', str(manifest), '-o', str(library)], check=True)
        exports.check(self.header, library, fmt)
        consumer = Path(__file__).parents[1] / 'pkg/antfly-embedded/tests/capi_link_consumer.c'
        exports.consumer_test(self.header, library, consumer, self.root / 'consumer', fmt)


if __name__ == '__main__':
    unittest.main()
