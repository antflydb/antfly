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

import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "generator", Path(__file__).with_name("generate_go_sdk.py")
)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class GeneratorTests(unittest.TestCase):
    def test_host_toolchain_cannot_change_embedded_compression(self):
        with (
            patch.dict(
                os.environ, {"GOTOOLCHAIN": "local", "GOWORK": "/other/go.work"}
            ),
            patch.object(
                sys, "argv", ["generate_go_sdk.py", "--config=cfg.yaml", "schema.yaml"]
            ),
            patch.object(generator.subprocess, "run") as run,
        ):
            generator.main()
        args, kwargs = run.call_args
        expected = json.loads(
            (Path(__file__).parent / "ci/toolchain-policy.json").read_text()
        )["go"]["version"]
        self.assertEqual(
            args[0], ["go", "tool", "oapi-codegen", "--config=cfg.yaml", "schema.yaml"]
        )
        self.assertEqual(kwargs["env"]["GOTOOLCHAIN"], "go" + expected)
        self.assertEqual(kwargs["env"]["GOWORK"], "off")
        self.assertTrue(kwargs["check"])
