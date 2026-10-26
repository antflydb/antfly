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

"""Run a command with the experimental DGX Spark OCR profile defaults.

Existing environment values take precedence. This helper does not set memory
limits, install dependencies, select a checkpoint, or change the GPU driver.
"""

import argparse
import json
import os
import sys
from pathlib import Path


PROFILE_PATH = Path(__file__).with_name("cuda_ocr_profiles.json")


def profile_environment(profiles, name):
    profile = profiles[name]
    env = (
        profile_environment(profiles, profile["extends"])
        if "extends" in profile
        else {}
    )
    env.update(profile["environment"])
    return env


def main():
    data = json.loads(PROFILE_PATH.read_text())
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=data["profiles"])
    parser.add_argument("--print-env", action="store_true")
    parser.add_argument(
        "--bf16-prefill-ffn",
        action="store_true",
        help="Experimental BF16 FFN intermediates for large LightOn prefills; F32 decode/state remain unchanged.",
    )
    parser.add_argument(
        "--bf16-recurrent-prefill",
        type=Path,
        metavar="LIBRARY",
        help="Optional SM12.1 native BF16 recurrent prefill library; F32 state and decode remain unchanged.",
    )
    argv = sys.argv[1:]
    delimiter = argv.index("--") if "--" in argv else len(argv)
    command = argv[delimiter + 1 :]
    args = parser.parse_args(argv[:delimiter])
    if (
        args.bf16_prefill_ffn or args.bf16_recurrent_prefill
    ) and args.profile == "florence":
        parser.error("BF16 prefill options apply only to the LightOn models")
    defaults = profile_environment(data["profiles"], args.profile)
    if args.profile != "florence":
        defaults["ANTFLY_CUDA_ENABLE_LIGHTON_BF16_PREFILL_FFN"] = (
            "1" if args.bf16_prefill_ffn else "0"
        )
    if args.bf16_recurrent_prefill:
        defaults["ANTFLY_CUDA_LIGHTON_PREFILL_LIBRARY"] = str(
            args.bf16_recurrent_prefill.resolve()
        )
    env = os.environ.copy()
    for key, value in defaults.items():
        env.setdefault(key, value)
    if args.print_env:
        print(json.dumps({key: env[key] for key in sorted(defaults)}, indent=2))
        return
    if not command:
        parser.error("supply a command after --, or use --print-env")
    os.execvpe(command[0], command, env)


if __name__ == "__main__":
    main()
