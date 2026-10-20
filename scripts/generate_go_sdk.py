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

"""Run SDK generation with the policy compiler, including its flate encoder.

Go's compression implementation can change across toolchains. oapi-codegen
embeds compressed specs, so pinning only oapi-codegen does not pin its output.
Consumer Go minimums are independent of this generation toolchain.
"""

import json
import os
from pathlib import Path
import subprocess
import sys


def main() -> None:
    policy = json.loads(
        (Path(__file__).parent / "ci/toolchain-policy.json").read_text()
    )
    env = {**os.environ, "GOTOOLCHAIN": "go" + policy["go"]["version"], "GOWORK": "off"}
    subprocess.run(["go", "tool", "oapi-codegen", *sys.argv[1:]], env=env, check=True)


if __name__ == "__main__":
    main()
