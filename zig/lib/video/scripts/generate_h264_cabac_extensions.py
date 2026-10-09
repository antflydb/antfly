#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate factual CABAC aliases from ITU-T H.264 (08/2021) text.

Input must be the official recommendation rendered with pdftotext -layout.
https://www.itu.int/rec/T-REC-H.264-202108-I/en
The base initial states retain their pinned OpenH264 BSD-2-Clause provenance.
"""

import argparse
import ast
import re
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("spec_text", type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
spec = args.spec_text.read_text().replace("−", "-")
assert "Rec. ITU-T H.264 (08/2021)" in spec
start = spec.rindex("Table 9-25 –")
end = spec.index("9.3.2", spec.rindex("Table 9-33 –"))
rows = {}
for line in spec[start:end].split("\n"):
    numbers = [int(value) for value in re.findall(r"(?<![A-Za-z])-?\d+", line)]
    if len(numbers) != 18:
        continue
    for column in (0, 9):
        index = numbers[column]
        # Table 9-26 has a printed row-label typo: 641 in the 541 row.
        if numbers[0] == 497 and column == 9 and index == 641:
            index = 541
        if 460 <= index <= 1023:
            assert index not in rows
            rows[index] = tuple(numbers[column + 1 : column + 9])
assert set(rows) == set(range(460, 1024))
tables = (root / "src/h264_cabac_tables.zig").read_text()
start = tables.index("=", tables.index("pub const initial")) + 1
end = tables.index(";", start)
base = ast.literal_eval(tables[start:end].replace(".{", "[").replace("}", "]").strip())
aliases = {tuple(sum(row, [])): i for i, row in enumerate(base)}
assert all(row in aliases for row in rows.values())
output = "// Copyright 2026 Antfly, Inc.\n// SPDX-License-Identifier: Apache-2.0\n"
output += "//! H.264 9.3.1.1 Tables 9-25..9-33. Extended contexts share these initial states\n"
output += "//! with the base context table. The aliases preserve independently evolving states.\n"
output += "pub const initial_alias = [564]u16{\n"
output += "".join(f"    {aliases[rows[i]]}, // {i}\n" for i in range(460, 1024))
output += "};\n"

# Stop at the end of Table 9-43, before any other numeric tables.
start = spec.index(
    "Table 9-43 –", spec.index("9.3.3.1.3 Assignment process", spec.index("9.3.2.1"))
)
end = spec.index("Let numDecodAbsLevelEq1", start)
field = {}
for line in spec[start:end].split("\n"):
    numbers = [int(v) for v in re.findall(r"\d+", line)]
    if len(numbers) not in (4, 8):
        continue
    for column in range(0, len(numbers), 4):
        index = numbers[column]
        if index < 63:
            assert index not in field
            field[index] = numbers[column + 2]
assert set(field) == set(range(63))
assert field[0] == 0 and field[1] == 1
transform = root / "src/h264_transform8.zig"
source = transform.read_text()
source = re.sub(
    r"pub const field_significant = .*?;",
    "pub const field_significant = [_]usize{ "
    + ", ".join(str(field[i]) for i in range(63))
    + " };",
    source,
)
(root / "src/h264_cabac_extensions.zig").write_text(output)
transform.write_text(source)
