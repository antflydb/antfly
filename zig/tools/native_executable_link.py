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

"""Link a native Mach-O probe with the same system linker used by the C API."""

import argparse
import json
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--arch", required=True)
parser.add_argument("--deployment", required=True)
parser.add_argument("--sdk", required=True)
parser.add_argument("--object", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("inputs", nargs=argparse.REMAINDER)
args = parser.parse_args()
with open(args.sdk + "/SDKSettings.json") as source:
    sdk_version = json.load(source)["Version"]
inputs = args.inputs[1:] if args.inputs[:1] == ["--"] else args.inputs
subprocess.run(
    [
        "/usr/bin/ld",
        "-arch",
        args.arch,
        "-platform_version",
        "macos",
        args.deployment,
        sdk_version,
        "-syslibroot",
        args.sdk,
        "-dead_strip",
        "-adhoc_codesign",
        "-o",
        args.output,
        args.object,
        *inputs,
        "-lSystem",
    ],
    check=True,
)
