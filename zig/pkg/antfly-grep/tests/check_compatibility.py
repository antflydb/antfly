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

"""Generate/check the public compatibility matrix against its reviewed evidence."""

import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "compatibility.json").read_text())
for area in manifest["areas"]:
    for path in area["evidence"]:
        if not (root / path).is_file():
            raise ValueError(f"missing compatibility evidence: {path}")
counts = {
    name: len(json.loads((root / "tests" / name).read_text())["cases"])
    for name in (
        "local_fixtures.json",
        "pcre2_dialect_fixtures.json",
        "generated_fixtures.json",
    )
}
lines = [
    "# Compatibility",
    "",
    "Target: ripgrep 15.2.0 / PCRE2 10.45. Implementation: native Zig.",
    "",
    f"Checked-in differential contracts: {counts['local_fixtures.json']} local-search cases and {counts['pcre2_dialect_fixtures.json']} PCRE2 dialect cases, plus {counts['generated_fixtures.json']} generated cases. Normal tests require no oracle executable.",
    "",
    "| Area | Status | Evidence and limits |",
    "|---|---|---|",
]
for area in manifest["areas"]:
    evidence = ", ".join(f"[{Path(path).name}]({path})" for path in area["evidence"])
    lines.append(
        f"| {area['area']} | {area['status']} | {evidence}. {area['limits']} |"
    )
text = "\n".join(lines) + "\n"
path = root / "COMPATIBILITY.md"
if args.check:
    if path.read_text() != text:
        raise SystemExit(
            "compatibility matrix is stale; run tests/check_compatibility.py"
        )
else:
    path.write_text(text)
