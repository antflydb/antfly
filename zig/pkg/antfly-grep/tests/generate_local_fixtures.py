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

"""Reproducible local-search contracts from test-only ripgrep 15.2 / PCRE2 10.45.

Run with a pinned oracle path; stdout is a fixture manifest. These synthetic
inputs cover both engines and output/Unicode combinations without depending on
an installed oracle during ordinary tests.
"""

import argparse
import base64
import hashlib
import itertools
import json
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("oracle", type=Path)
args = parser.parse_args()
oracle = str(args.oracle.resolve())
version = subprocess.check_output([oracle, "--version"]).decode()
assert "ripgrep 15.2.0" in version
assert "10.45" in subprocess.check_output([oracle, "--pcre2-version"]).decode()
patterns = [
    r"[a-z]+",
    r"a(?:b|c)+",
    r"(?i)k",
    r"\b\w+\b",
    r"^foo$",
    r"a{1,3}?",
    r"(?m)^bar",
    r"\p{Greek}+",
    r"[[:digit:]]+",
    r"a\B",
    r"\Afoo",
    r"foo\z",
]
subjects = [
    "foo\nbar abc aaa 123\nfoo",
    "Été Kſ abc αβ １２３\n",
    "abc foo\r\nbar\nfoo\n",
]
cases = []
for engine, pattern, subject, mode in itertools.product(
    ["default", "pcre2"], patterns, subjects, ["-o", "-v", "-c", "-n"]
):
    flags = ["--engine", engine, "--color=never", mode, pattern]
    data = subject.encode()
    result = subprocess.run(
        [oracle, *flags], input=data, check=False, capture_output=True, timeout=5
    )
    cases.append(
        {
            "category": "regex-unicode-output",
            "args": flags,
            "input": base64.b64encode(data).decode(),
            "code": result.returncode,
            "stdout": base64.b64encode(result.stdout).decode(),
        }
    )
for flags, data in [
    (["-ao", "a+"], b"aa\0aaa\n"),
    (["--null-data", "-o", "a+"], b"aa\0aaa\0"),
    (["-a", "-c", "hit"], b"hit\n" + b"z" * 262144 + b"\0hit\n"),
    (["--encoding=none", "-ao", "[a]"], b"\xc2a\n"),
]:
    result = subprocess.run(
        [oracle, *flags], input=data, check=False, capture_output=True, timeout=5
    )
    cases.append(
        {
            "category": "binary-encoding",
            "args": flags,
            "input": base64.b64encode(data).decode(),
            "code": result.returncode,
            "stdout": base64.b64encode(result.stdout).decode(),
        }
    )
print(
    json.dumps(
        {
            "version": 1,
            "oracle": version,
            "oracle_sha256": hashlib.sha256(Path(oracle).read_bytes()).hexdigest(),
            "cases": cases,
        },
        indent=2,
    )
)
