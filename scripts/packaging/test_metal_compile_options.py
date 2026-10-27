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

"""Compare modern Metal precision options with the legacy native API."""

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKENDS = ROOT / "zig/pkg/inference/src/backends"


@unittest.skipUnless(
    sys.platform == "darwin" and shutil.which("clang"), "requires macOS and Clang"
)
class MetalCompileOptionsTests(unittest.TestCase):
    def test_precise_options_preserve_legacy_arithmetic_and_function_modes(self):
        cache = ROOT / ".zig-cache/metal-compile-options"
        cache.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=cache) as directory:
            root = Path(directory)
            source = root / "probe.m"
            source.write_text(r"""
#import <Foundation/Foundation.h>
#include "metal_compile_options.h"
#include <stdio.h>
int main(void) {
    @autoreleasepool {
        if (@available(macOS 15.0, *)) {
            MTLLanguageVersion versions[] = {MTLLanguageVersion3_1,
#if __MAC_OS_X_VERSION_MAX_ALLOWED >= 260000
                MTLLanguageVersion4_0,
#endif
            };
            for (unsigned i = 0; i < sizeof(versions) / sizeof(versions[0]); ++i) {
#if __MAC_OS_X_VERSION_MAX_ALLOWED >= 260000
                if (versions[i] == MTLLanguageVersion4_0) {
                    if (@available(macOS 26.0, *)) {} else continue;
                }
#endif
                MTLCompileOptions *legacy = [MTLCompileOptions new];
                legacy.languageVersion = versions[i];
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
                legacy.fastMathEnabled = NO;
#pragma clang diagnostic pop
                MTLCompileOptions *modern = [MTLCompileOptions new];
                modern.languageVersion = versions[i];
                termite_metal_set_precise_math(modern);
                if (legacy.mathMode != modern.mathMode ||
                    legacy.mathFloatingPointFunctions != modern.mathFloatingPointFunctions) {
                    fprintf(stderr, "precision mismatch: legacy=(%ld,%ld), modern=(%ld,%ld)\n",
                            (long)legacy.mathMode, (long)legacy.mathFloatingPointFunctions,
                            (long)modern.mathMode, (long)modern.mathFloatingPointFunctions);
                    return 1;
                }
            }
            return 0;
        }
        return 77;
    }
}
""")
            binary = root / "probe"
            compiled = subprocess.run(
                [
                    "clang",
                    "-fobjc-arc",
                    "-Werror=deprecated-declarations",
                    "-I",
                    str(BACKENDS),
                    str(source),
                    "-framework",
                    "Foundation",
                    "-framework",
                    "Metal",
                    "-o",
                    str(binary),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(compiled.returncode, 0, compiled.stdout + compiled.stderr)
            result = subprocess.run(
                [str(binary)], capture_output=True, text=True, check=False
            )
            if result.returncode == 77:
                self.skipTest("requires macOS 15 or later")
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
