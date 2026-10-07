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

"""Exercise pkg-config discovery after relocating a native installation."""

import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import render_libantfly_pkgconfig as metadata


@unittest.skipUnless(shutil.which("pkg-config"), "pkg-config is required")
class PkgConfigTests(unittest.TestCase):
    def test_relocated_install_with_spaces(self):
        with tempfile.TemporaryDirectory() as raw:
            original = Path(raw) / "original"
            pc_dir = original / "lib/pkgconfig"
            pc_dir.mkdir(parents=True)
            (pc_dir / "libantfly.pc").write_text(metadata.render("1.2.3"))
            relocated = Path(raw) / "relocated embedded"
            original.rename(relocated)
            env = {**os.environ, "PKG_CONFIG_PATH": str(relocated / "lib/pkgconfig")}

            def query(*args):
                return subprocess.check_output(
                    ["pkg-config", *args, "libantfly"], env=env, text=True
                ).strip()

            self.assertEqual(query("--modversion"), "1.2.3")
            flags = shlex.split(query("--libs", "--cflags"))
            lib_dir = str(relocated / "lib/pkgconfig/../../lib")
            self.assertIn("-L" + lib_dir, flags)
            self.assertIn("-lantfly", flags)
            self.assertIn("-Wl,-rpath," + lib_dir, flags)
            self.assertFalse(any(str(original) in flag for flag in flags))

    def test_version_cannot_inject_metadata(self):
        with self.assertRaises(ValueError):
            metadata.render("1.2.3\nLibs: -lother")
