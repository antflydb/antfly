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

import importlib.util
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "native_source_closure", Path(__file__).with_name("native_source_closure.py")
)
closure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(closure)


class NativeSourceClosureTest(unittest.TestCase):
    def check_source(self, files):
        compiler = shutil.which("cc")
        if not compiler:
            self.skipTest("C compiler unavailable")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, text in files.items():
                (root / name).write_text(text)
            source = closure.expand_native_source(root / "main.c")
            env = dict(os.environ, TMPDIR=directory)
            result = subprocess.run(
                [compiler, "-fsyntax-only", "-std=c11", "-x", "c", "-"],
                input=source,
                check=False,
                text=True,
                capture_output=True,
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_conditional_diamond_keeps_the_live_definition(self):
        self.check_source(
            {
                "leaf.h": "#pragma once\nstatic int value(void){return 7;}\n",
                "left.h": '#include "leaf.h"\n',
                "right.h": '#include "leaf.h"\n',
                "main.c": '#if 0\n#include "left.h"\n#endif\n#include "right.h"\n#include "leaf.h"\nint main(void){return value()!=7;}\n',
            }
        )

    def test_local_include_cycle_is_bounded(self):
        self.check_source(
            {
                "a.h": '#pragma once\n#include "b.h"\nstatic int a(void){return b();}\n',
                "b.h": '#pragma once\n#include "a.h"\nstatic int b(void){return 3;}\n',
                "main.c": '#include "a.h"\nint main(void){return a()!=3;}\n',
            }
        )


if __name__ == "__main__":
    unittest.main()
