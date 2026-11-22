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

"""Offline regeneration rejects stale exports before modifying installed images."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

module_spec = importlib.util.spec_from_file_location("regen_cuda_standalone", Path(__file__).with_name("regen_cuda_standalone.py"))
module = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(module)


class StandaloneImagesTest(unittest.TestCase):
    def test_check_reports_stale_without_writing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            generated = root / "generated"
            installed = root / "installed"
            generated.mkdir()
            installed.mkdir()
            data = b".visible .entry op() {}\n"
            (generated / "op.ptx").write_bytes(data)
            spec = dict(file="op.ptx", entry="op", family="test")
            self.assertFalse(module.check_images([spec], generated, installed)[0]["identical"])
            self.assertFalse((installed / "op.ptx").exists())
            module.check_images([spec], generated, installed, write=True)
            self.assertTrue(module.check_images([spec], generated, installed)[0]["identical"])
            self.assertEqual((installed / "op.ptx").read_bytes(), data)

    def test_bad_later_export_does_not_partially_publish(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            installed = root / "installed"
            installed.mkdir()
            (root / "a.ptx").write_bytes(b".visible .entry a() {}\n")
            (root / "b.ptx").write_bytes(b".visible .entry wrong() {}\n")
            specs = [dict(file=n + ".ptx", entry=n, family="test") for n in ["a", "b"]]
            with self.assertRaisesRegex(ValueError, "exports changed"):
                module.check_images(specs, root, installed, write=True)
            self.assertFalse(list(installed.iterdir()))

    def test_extra_exports_and_path_escape_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "op.ptx").write_bytes(b".visible .entry op() {}\n.visible .entry extra() {}\n")
            with self.assertRaisesRegex(ValueError, "exports changed"):
                module.check_images([dict(file="op.ptx", entry="op", family="test")], root, root)
            with self.assertRaisesRegex(ValueError, "image path"):
                module.check_images([dict(file="../op.ptx", entry="op", family="test")], root, root)


if __name__ == "__main__":
    unittest.main()
