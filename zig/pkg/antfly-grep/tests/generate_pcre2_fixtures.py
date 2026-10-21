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

import argparse
import base64
import hashlib
import json
import subprocess
from pathlib import Path

pairs = [
    (r"(?<=a|ba)(c)", "bac\n"),
    (r"(?<=(a)|(ba))c", "bac\n"),
    (r"(?<=é{1,3})x", "ééx\n"),
    (r"(?<!a{1,3})x", "ax bx\n"),
    (r"(?(?<=ab)c|d)", "abc d\n"),
    (r"(?<=a*)b", "aab\n"),
    (r"(?<=a{1,256})b", "ab\n"),
    (r"(?<=\X)b", "ab\n"),
    (r"(?<=\R)b", "ab\n"),
    (r"(?<=\C)b", "ab\n"),
    (r"(?<=a{256})b", "a" * 256 + "b\n"),
    (r"(?i)[[:upper:]]+", "éKſABC xyz\n"),
    (r"(?i)\p{Lu}+", "éKſABC xyz\n"),
    (r"(?i)\P{Ll}+", "ABC abc 123\n"),
    (r"(?ai)[[:upper:]]+", "KſABC xyz\n"),
    (r"(?r)(?i)[a-z]+", "KſABC xyz\n"),
    (r"(?aD)\d+", "123 １２３ ١٢\n"),
    (r"(?aW)\w+", "éABC_１２\n"),
    (r"(?aP)[[:alpha:]]+", "éABCK\n"),
    (r"(?[ [a-z] - [aeiou] ])+", "all brown foxes\n"),
    (r"(?[ \p{L} & ! [a-z] ])+", "é ABC １２\n"),
    (r"(?[ [ab] ^ [bc] ])+", "abc abc\n"),
    (r"[a&&b]+", "a&b c\n"),
    (r"[\Q-[]\E]+", "-[] abc\n"),
    (r"(?xx)[a b]+", "abab b a\n"),
    (r"\o{141}\N{U+62}\x{ 63 }", "abc\n"),
    (r"\123", "S 123\n"),
    (r"[\123]", "S\n"),
    (r"(?<é>a)\k<é>", "aa\n"),
    (r"(?J)(?<n>a)|(?<n>b)\k<n>", "a bb b\n"),
    (r"(?J)(?<n>a)?(?<n>b)?\k<n>", "aa bb abb\n"),
    (r"(a|b)(?1)", "ab ba\n"),
    (r"(?(DEFINE)(?<n>a))(?&n)", "a\n"),
    (r"(a)(?+1)(b)", "abb\n"),
    (r"(?<n>a(?(R&n)b|(?&n)))", "aab\n"),
    (r"(?(VERSION>=10.0)a|b)", "a b\n"),
    (r"(?(VERSION>=99.0)a|b)", "a b\n"),
    (r"a{,3}b", "aaab ab b\n"),
    (r"(?U)a++a", "aaa\n"),
    (r"a{ 2 , 3 }", "aaa\n"),
    (r"(?^i)a", "A a\n"),
    (r"(*sr:\S+)", "abc αβ abcα 日本カナ 한글カナ ١٢ ١2\n"),
    (r"(*asr:a|ab)c", "abc ac\n"),
    (r"(*sr:a|ab)c", "abc ac\n"),
    (r"(a+)(*scs:(1)a+)", "aaa\n"),
    (r"(?<x>ab)(*scan_substring:(<x>)a)", "ab\n"),
    (r"(a)(b)(*scs:(2,1)b)", "ab\n"),
    (r"(?*(a|ab))\1c", "abc ac\n"),
    (r"(?<*(a)|(ba))c", "bac\n"),
    (r"(*napla:(a|ab))\1c", "abc ac\n"),
    (r"a(*COMMIT)b|ac", "ac ab\n"),
    (r"a(*PRUNE)b|ac", "ac ab\n"),
    (r"a(*THEN)b|ac", "ac ab\n"),
    (r"a(*SKIP)b|ac", "ac ab\n"),
    (r"(?>(a(*COMMIT)))b|ac", "ac ab\n"),
    (r"(?=a(*COMMIT))b|a", "a\n"),
    (r"(?*a(*COMMIT))b|a", "a\n"),
    (r"(?:(a+)(*THEN)b|a+c)", "aaac aaab\n"),
    (r"a(*MARK:x)b(*SKIP:x)c|ab", "ab abc\n"),
    (r"(?=a(*MARK:x))a(*SKIP:x)b|a", "a ab\n"),
    (r"(?(?!a(*ACCEPT))x|a)", "a\n"),
    (r"(?<=a(*ACCEPT)b)c", "ac abc\n"),
    (r"\p{Greek}+", "ᾳ ·\n"),
    (r"\p{sc:Greek}+", "ᾳ ·\n"),
    (r"\p{Xwd}+", "á_ 12\n"),
    (r"\p{Xuc}+", "A $ é\n"),
    (r"[[:<:]]a[[:>:]]", "a aa\n"),
    (r"\b\w+\b", "á_ foo\n"),
    (r"(?i)kſ", "Kſ kS Ks\n"),
    (r"(?i)\p{sc:Lu}+", "A a\n"),
    (r"(?-u)a", "a\n"),
    (r"\u0061", "a\n"),
    (r"(?<x>a)(?<x>b)", "ab\n"),
    (r"(?(DEFINE)a|b)", "ab\n"),
    (r"(?999999999999999999999999)", "a\n"),
    (r"(?<=a{65536})b", "ab\n"),
]
# Original synthetic subjects, generated against pinned test-only executable.
parser = argparse.ArgumentParser(
    description="Generate original synthetic fixtures with ripgrep 15.2.0 / PCRE2 10.45 (test-only oracle)."
)
parser.add_argument("oracle", type=Path)
args = parser.parse_args()
rg = str(args.oracle.resolve())
assert "ripgrep 15.2.0" in subprocess.check_output([rg, "--version"]).decode()
assert "10.45" in subprocess.check_output([rg, "--pcre2-version"]).decode()
rows = []
for i, (pattern, text) in enumerate(pairs):
    args = ["-P", "-o", "--color=never", pattern]
    if i in (1, 26, 28, 29, 30, 31, 43, 45, 46, 47):
        args[1:1] = ["-r", "$1|$2|$0"]
    raw = text.encode()
    run = subprocess.run(
        [rg, *args], check=False, input=raw, capture_output=True, timeout=10
    )
    rows.append(
        {
            "args": args,
            "input": base64.b64encode(raw).decode(),
            "code": run.returncode,
            "stdout": base64.b64encode(run.stdout).decode(),
        }
    )
print(
    json.dumps(
        {
            "oracle": subprocess.check_output([rg, "--version"]).decode(),
            "oracle_sha256": hashlib.sha256(Path(rg).read_bytes()).hexdigest(),
            "cases": rows,
        },
        indent=2,
    )
)
