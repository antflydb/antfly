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

"""Profile wrappers preserve automatic selection and explicit CLI overrides."""

import contextlib
import io
import json
import os
import unittest
from unittest.mock import patch

from cuda_ocr_profile import main

GRAPH = "ANTFLY_CUDA_ENABLE_LIGHTON_MODEL_GRAPH"


class AutomaticGraphProfileTests(unittest.TestCase):
    def profile(self, *args, inherited=None):
        env = {} if inherited is None else {GRAPH: inherited}
        output = io.StringIO()
        with (
            patch.dict(os.environ, env, clear=True),
            patch("sys.argv", ["profile", "lighton4b", *args, "--print-env"]),
            contextlib.redirect_stdout(output),
        ):
            main()
        return json.loads(output.getvalue())

    def test_omitted_graph_flag_does_not_override_runtime_default(self):
        self.assertNotIn(GRAPH, self.profile())

    def test_inherited_explicit_setting_is_preserved_and_reported(self):
        for value in ("0", "1"):
            with self.subTest(value=value):
                self.assertEqual(self.profile(inherited=value)[GRAPH], value)

    def test_explicit_cli_selection_overrides_environment_both_ways(self):
        self.assertEqual(self.profile("--decode-graph", inherited="0")[GRAPH], "1")
        self.assertEqual(self.profile("--no-decode-graph", inherited="1")[GRAPH], "0")


if __name__ == "__main__":
    unittest.main()
