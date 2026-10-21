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
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
# Source archives need no Git installation; repository checks additionally keep
# raw benchmark runs out of the tracked tree. Local ignored outputs are allowed.
try:
    tracked = subprocess.run(
        ["git", "ls-files", "-z", "--", "bench/results"],
        cwd=root,
        capture_output=True,
        check=False,
    )
except FileNotFoundError:
    tracked = None
if tracked is not None and tracked.returncode == 0:
    for raw_path in tracked.stdout.decode().split("\0"):
        if not raw_path:
            continue
        path = Path(raw_path)
        if path.parent != Path("bench/results"):
            raise ValueError(f"unexpected tracked benchmark directory: {path}")
        if path.name in {".gitignore", "README.md", "archive-manifest.json"}:
            continue
        if not path.name.endswith("-summary.json"):
            raise ValueError(
                f"archive raw benchmark output instead of tracking it: {path}"
            )
        if (root / path).stat().st_size > 64 * 1024:
            raise ValueError(f"benchmark summary exceeds 64 KiB: {path}")
manifest = json.loads((root / "compatibility.json").read_text())
archive = json.loads((root / "bench/results/archive-manifest.json").read_text())
archived_names = {entry["name"] for entry in archive["files"]}
for area in manifest["areas"]:
    for path in area["evidence"]:
        if not (root / path).is_file():
            raise ValueError(f"missing compatibility evidence: {path}")
    for name in area.get("archived_evidence", []):
        if name not in archived_names:
            raise ValueError(f"missing archived compatibility evidence: {name}")
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
    "Target: ripgrep 15.2.0 / PCRE2 10.45, with the oracle build identities retained in each report. Linux empty-UTF compatibility targets the GNU/JIT-enabled build. Implementation: native Zig.",
    "",
    f"Checked-in differential contracts: {counts['local_fixtures.json']} local-search cases and {counts['pcre2_dialect_fixtures.json']} PCRE2 dialect cases, plus {counts['generated_fixtures.json']} generated cases. Normal tests require no oracle executable.",
    "",
    "| Area | Status | Evidence and limits |",
    "|---|---|---|",
]
for area in manifest["areas"]:
    evidence = ", ".join(f"[{Path(path).name}]({path})" for path in area["evidence"])
    historical = ", ".join(
        f"[{name}](bench/results/README.md)"
        for name in area.get("archived_evidence", [])
    )
    evidence = ", ".join(part for part in (evidence, historical) if part)
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
