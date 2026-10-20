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
from pathlib import Path
from types import SimpleNamespace
import unittest

spec = importlib.util.spec_from_file_location("native_search_comparison", Path(__file__).resolve().parents[1] / "native_search_comparison.py")
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


class NativeComparisonTests(unittest.TestCase):
    def test_matrix_pairs_identical_cases_and_alternates_binary_order(self):
        cases = list(comparison.matrix([1, 1024], ["plain", "highlights", "composed"], ["empty", "readiness"], 2))
        self.assertEqual(len(cases), 48)
        for i in range(0, len(cases), 2):
            base, head = cases[i:i+2]
            self.assertEqual({k:v for k,v in base.items() if k != "binary"}, {k:v for k,v in head.items() if k != "binary"})
            self.assertEqual(base["binary"], "base" if base["cycle"] == 0 else "head")

    def test_modes_separate_empty_cache_warming_and_bm25_projection(self):
        args = SimpleNamespace(port=8877, repeats=20, build_timeout=1800, process_memory_budget_mb=4096, term="database")
        for mode in ("plain", "highlights", "composed"):
            for cache in ("empty", "readiness"):
                case = dict(mode=mode, cache=cache)
                command = comparison.command(Path("poc.py"), Path("binary"), Path("fixture"), Path("private-state"), case, args, 142577)
                self.assertIn("--immediate-first-query", command)
                self.assertEqual("--no-highlights" in command, mode == "plain")
                self.assertEqual("--cold-cache" in command, cache == "empty")
                self.assertEqual("--disable-navigation-warming" in command, cache == "empty")
                self.assertEqual("--benchmark-composed" in command, mode == "composed")
                self.assertIn("--filesystem-source", command)
                self.assertNotIn("--bearer-stdin", command)
