#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Generate allocation-free graph identifier policy implementations.

The checked-in JSON policy is the wire-contract source of truth. Keeping the
Unicode version and code-point tables out of language runtimes prevents an SDK
toolchain upgrade from silently changing which graph queries are accepted.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
POLICY_PATH = ROOT / "specs/graph_identifier_policy.json"
ZIG_PATH = ROOT / "zig/pkg/antfly/src/graph/identifier_policy_generated.zig"
GO_PATH = ROOT / "go/pkg/sdk/graph_identifier_policy_generated.go"
PYTHON_PATH = ROOT / "py/packages/sdk/src/antfly/graph_identifier_policy_generated.py"
TYPESCRIPT_PATH = ROOT / "ts/packages/sdk/src/graph-identifier-policy.generated.ts"
RUST_PATH = ROOT / "rs/crates/sdk/src/graph_identifier_policy_generated.rs"
OPENAPI_PATH = ROOT / "specs/openapi/antfly/generated/graph_identifier.yaml"


def parse_ranges(raw_ranges: list[list[str]]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for raw in raw_ranges:
        if len(raw) != 2:
            raise ValueError(f"range must have two endpoints: {raw!r}")
        lo, hi = (int(endpoint, 16) for endpoint in raw)
        if not (0 <= lo <= hi <= 0x10FFFF):
            raise ValueError(f"invalid Unicode range: {raw!r}")
        ranges.append((lo, hi))
    return ranges


def merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for lo, hi in sorted(ranges):
        if merged and lo <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def subtract_points(ranges: list[tuple[int, int]], points: set[int]) -> list[tuple[int, int]]:
    result: list[tuple[int, int]] = []
    for lo, hi in ranges:
        fragments = [(lo, hi)]
        for point in sorted(point for point in points if lo <= point <= hi):
            next_fragments: list[tuple[int, int]] = []
            for fragment_lo, fragment_hi in fragments:
                if not (fragment_lo <= point <= fragment_hi):
                    next_fragments.append((fragment_lo, fragment_hi))
                    continue
                if fragment_lo < point:
                    next_fragments.append((fragment_lo, point - 1))
                if point < fragment_hi:
                    next_fragments.append((point + 1, fragment_hi))
            fragments = next_fragments
        result.extend(fragments)
    return result


def load_policy() -> tuple[dict[str, Any], list[tuple[int, int]]]:
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    if policy["policy_version"] != 1:
        raise ValueError("unsupported graph identifier policy version")
    if policy["max_utf8_bytes"] != policy["max_code_points"] * 4:
        raise ValueError("max_utf8_bytes must cover exactly max_code_points UTF-8 scalars")

    allowed = {int(value, 16) for value in policy["allowed_whitespace"]}
    whitespace_ranges = subtract_points(
        merge_ranges(parse_ranges(policy["unicode_white_space_ranges"])), allowed
    )
    disallowed_ranges = whitespace_ranges
    for key in ("general_category_cc_ranges", "general_category_cf_ranges", "invalid_scalar_ranges"):
        disallowed_ranges.extend(parse_ranges(policy[key]))
    disallowed = merge_ranges(disallowed_ranges)

    for case in policy["conformance_cases"]:
        actual = valid_identifier(policy, disallowed, case["value"])
        if actual != case["valid"]:
            raise ValueError(f"conformance case {case['name']!r} disagrees with policy")
    return policy, disallowed


def codepoint_disallowed(ranges: list[tuple[int, int]], codepoint: int) -> bool:
    lo = 0
    hi = len(ranges)
    while lo < hi:
        mid = lo + (hi - lo) // 2
        range_lo, range_hi = ranges[mid]
        if codepoint < range_lo:
            hi = mid
        elif codepoint > range_hi:
            lo = mid + 1
        else:
            return True
    return False


def valid_identifier(policy: dict[str, Any], ranges: list[tuple[int, int]], value: str) -> bool:
    encoded = value.encode("utf-8")
    if not encoded or len(encoded) > policy["max_utf8_bytes"]:
        return False
    if value in policy["reserved_exact"]:
        return False
    if any(value.startswith(prefix) for prefix in policy["reserved_prefixes"]):
        return False
    if len(value) > policy["max_code_points"]:
        return False
    if value.startswith(" ") or value.endswith(" "):
        return False
    return not any(codepoint_disallowed(ranges, ord(char)) for char in value)


def zig_string(value: str) -> str:
    escaped: list[str] = []
    for char in value:
        codepoint = ord(char)
        if char == "\\":
            escaped.append("\\\\")
        elif char == '"':
            escaped.append('\\"')
        elif codepoint < 0x20 or codepoint == 0x7F:
            escaped.append(f"\\x{codepoint:02x}")
        elif codepoint in (0x2028, 0x2029):
            escaped.append(f"\\u{{{codepoint:x}}}")
        else:
            escaped.append(char)
    return '"' + "".join(escaped) + '"'


def render_zig(policy: dict[str, Any], ranges: list[tuple[int, int]]) -> str:
    range_rows = "\n".join(f"    .{{ .lo = 0x{lo:x}, .hi = 0x{hi:x} }}," for lo, hi in ranges)
    exact_rows = ", ".join(zig_string(value) for value in policy["reserved_exact"])
    prefix_rows = ", ".join(zig_string(value) for value in policy["reserved_prefixes"])
    case_rows = "\n".join(
        f"    .{{ .name = {zig_string(case['name'])}, .value = {zig_string(case['value'])}, .valid = {str(case['valid']).lower()} }},"
        for case in policy["conformance_cases"]
    )
    return f'''// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Code generated by scripts/generate_graph_identifier_policy.py from
// specs/graph_identifier_policy.json; DO NOT EDIT.

const std = @import("std");

pub const policy_version: u32 = {policy['policy_version']};
pub const unicode_version = {zig_string(policy['unicode_version'])};
pub const max_codepoints: usize = {policy['max_code_points']};
pub const max_utf8_bytes: usize = {policy['max_utf8_bytes']};

const Range = struct {{ lo: u21, hi: u21 }};

const disallowed_ranges = [_]Range{{
{range_rows}
}};

const reserved_exact = [_][]const u8{{{exact_rows}}};
const reserved_prefixes = [_][]const u8{{{prefix_rows}}};

fn codepointDisallowed(codepoint: u21) bool {{
    var lo: usize = 0;
    var hi: usize = disallowed_ranges.len;
    while (lo < hi) {{
        const mid = lo + (hi - lo) / 2;
        const range = disallowed_ranges[mid];
        if (codepoint < range.lo) {{
            hi = mid;
        }} else if (codepoint > range.hi) {{
            lo = mid + 1;
        }} else {{
            return true;
        }}
    }}
    return false;
}}

pub fn isValid(value: []const u8) bool {{
    if (value.len == 0 or value.len > max_utf8_bytes) return false;
    inline for (reserved_exact) |reserved| {{
        if (std.mem.eql(u8, value, reserved)) return false;
    }}
    inline for (reserved_prefixes) |prefix| {{
        if (std.mem.startsWith(u8, value, prefix)) return false;
    }}

    var iterator = (std.unicode.Utf8View.init(value) catch return false).iterator();
    var codepoints: usize = 0;
    var first: u21 = undefined;
    var last: u21 = undefined;
    while (iterator.nextCodepoint()) |codepoint| {{
        if (codepoints == 0) first = codepoint;
        last = codepoint;
        codepoints += 1;
        if (codepoints > max_codepoints or codepointDisallowed(codepoint)) return false;
    }}
    return codepoints > 0 and first != ' ' and last != ' ';
}}

pub const ConformanceCase = struct {{
    name: []const u8,
    value: []const u8,
    valid: bool,
}};

pub const conformance_cases = [_]ConformanceCase{{
{case_rows}
}};
'''


def render_go(policy: dict[str, Any], ranges: list[tuple[int, int]]) -> str:
    range_rows = "\n".join(f"\t{{lo: 0x{lo:X}, hi: 0x{hi:X}}}," for lo, hi in ranges)
    exact_rows = ", ".join(json.dumps(value, ensure_ascii=True) for value in policy["reserved_exact"])
    prefix_rows = ", ".join(json.dumps(value, ensure_ascii=True) for value in policy["reserved_prefixes"])
    case_rows = "\n".join(
        f"\t{{name: {json.dumps(case['name'])}, value: {json.dumps(case['value'], ensure_ascii=True)}, valid: {str(case['valid']).lower()}}},"
        for case in policy["conformance_cases"]
    )
    return f'''// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Code generated by scripts/generate_graph_identifier_policy.py from
// specs/graph_identifier_policy.json; DO NOT EDIT.

package sdk

import (
\t"strings"
\t"unicode/utf8"
)

const (
\tGraphIdentifierPolicyVersion  = {policy['policy_version']}
\tGraphIdentifierUnicodeVersion = {json.dumps(policy['unicode_version'])}
\tmaxGraphIdentifierRunes       = {policy['max_code_points']}
\tmaxGraphIdentifierBytes       = {policy['max_utf8_bytes']}
)

type graphIdentifierCodepointRange struct {{
\tlo rune
\thi rune
}}

var graphIdentifierDisallowedRanges = [...]graphIdentifierCodepointRange{{
{range_rows}
}}

var graphIdentifierReservedExact = [...]string{{{exact_rows}}}
var graphIdentifierReservedPrefixes = [...]string{{{prefix_rows}}}

func graphIdentifierCodepointDisallowed(codepoint rune) bool {{
\tlo, hi := 0, len(graphIdentifierDisallowedRanges)
\tfor lo < hi {{
\t\tmid := lo + (hi-lo)/2
\t\tcodepointRange := graphIdentifierDisallowedRanges[mid]
\t\tswitch {{
\t\tcase codepoint < codepointRange.lo:
\t\t\thi = mid
\t\tcase codepoint > codepointRange.hi:
\t\t\tlo = mid + 1
\t\tdefault:
\t\t\treturn true
\t\t}}
\t}}
\treturn false
}}

// IsValidGraphIdentifier reports whether value satisfies the versioned Antfly wire policy.
func IsValidGraphIdentifier(value string) bool {{
\tif value == "" || len(value) > maxGraphIdentifierBytes || !utf8.ValidString(value) {{
\t\treturn false
\t}}
\tfor _, reserved := range graphIdentifierReservedExact {{
\t\tif value == reserved {{
\t\t\treturn false
\t\t}}
\t}}
\tfor _, prefix := range graphIdentifierReservedPrefixes {{
\t\tif strings.HasPrefix(value, prefix) {{
\t\t\treturn false
\t\t}}
\t}}

\tcount := 0
\tvar first, last rune
\tfor _, codepoint := range value {{
\t\tif count == 0 {{
\t\t\tfirst = codepoint
\t\t}}
\t\tlast = codepoint
\t\tcount++
\t\tif count > maxGraphIdentifierRunes || graphIdentifierCodepointDisallowed(codepoint) {{
\t\t\treturn false
\t\t}}
\t}}
\treturn count > 0 && first != ' ' && last != ' '
}}

func validGraphIdentifier(value string) bool {{
\treturn IsValidGraphIdentifier(value)
}}

type graphIdentifierConformanceCase struct {{
\tname  string
\tvalue string
\tvalid bool
}}

var graphIdentifierConformanceCases = [...]graphIdentifierConformanceCase{{
{case_rows}
}}
'''


def render_python(policy: dict[str, Any], ranges: list[tuple[int, int]]) -> str:
    range_rows = "\n".join(f"    (0x{lo:X}, 0x{hi:X})," for lo, hi in ranges)
    exact_rows = ", ".join(json.dumps(value, ensure_ascii=True) for value in policy["reserved_exact"])
    prefix_rows = ", ".join(json.dumps(value, ensure_ascii=True) for value in policy["reserved_prefixes"])
    case_rows = "\n".join(
        f"    ({json.dumps(case['name'])}, {json.dumps(case['value'], ensure_ascii=True)}, {case['valid']}),"
        for case in policy["conformance_cases"]
    )
    return f'''# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Code generated by scripts/generate_graph_identifier_policy.py from
# specs/graph_identifier_policy.json; DO NOT EDIT.

GRAPH_IDENTIFIER_POLICY_VERSION = {policy['policy_version']}
GRAPH_IDENTIFIER_UNICODE_VERSION = {json.dumps(policy['unicode_version'])}
MAX_GRAPH_IDENTIFIER_CODE_POINTS = {policy['max_code_points']}
MAX_GRAPH_IDENTIFIER_UTF8_BYTES = {policy['max_utf8_bytes']}

_DISALLOWED_RANGES = (
{range_rows}
)
_RESERVED_EXACT = ({exact_rows},)
_RESERVED_PREFIXES = ({prefix_rows},)


def _code_point_disallowed(code_point: int) -> bool:
    lo = 0
    hi = len(_DISALLOWED_RANGES)
    while lo < hi:
        mid = lo + (hi - lo) // 2
        range_lo, range_hi = _DISALLOWED_RANGES[mid]
        if code_point < range_lo:
            hi = mid
        elif code_point > range_hi:
            lo = mid + 1
        else:
            return True
    return False


def is_valid_graph_identifier(value: str) -> bool:
    """Return whether value satisfies the versioned Antfly wire policy."""
    if not value or value in _RESERVED_EXACT or value.startswith(_RESERVED_PREFIXES):
        return False
    if len(value) > MAX_GRAPH_IDENTIFIER_CODE_POINTS or value[0] == " " or value[-1] == " ":
        return False

    utf8_bytes = 0
    for char in value:
        code_point = ord(char)
        if _code_point_disallowed(code_point):
            return False
        utf8_bytes += 1 if code_point <= 0x7F else 2 if code_point <= 0x7FF else 3 if code_point <= 0xFFFF else 4
        if utf8_bytes > MAX_GRAPH_IDENTIFIER_UTF8_BYTES:
            return False
    return True


GRAPH_IDENTIFIER_CONFORMANCE_CASES = (
{case_rows}
)
'''


def render_typescript(policy: dict[str, Any], ranges: list[tuple[int, int]]) -> str:
    range_rows = "\n".join(f"  [0x{lo:x}, 0x{hi:x}]," for lo, hi in ranges)
    exact_rows = ", ".join(json.dumps(value) for value in policy["reserved_exact"])
    prefix_rows = ", ".join(json.dumps(value) for value in policy["reserved_prefixes"])
    case_rows = "\n".join(
        f"  {{ name: {json.dumps(case['name'])}, value: {json.dumps(case['value'], ensure_ascii=True)}, valid: {str(case['valid']).lower()} }},"
        for case in policy["conformance_cases"]
    )
    return f'''// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Code generated by scripts/generate_graph_identifier_policy.py from
// specs/graph_identifier_policy.json; DO NOT EDIT.

export const GRAPH_IDENTIFIER_POLICY_VERSION = {policy['policy_version']};
export const GRAPH_IDENTIFIER_UNICODE_VERSION = {json.dumps(policy['unicode_version'])};
export const MAX_GRAPH_IDENTIFIER_CODE_POINTS = {policy['max_code_points']};
export const MAX_GRAPH_IDENTIFIER_UTF8_BYTES = {policy['max_utf8_bytes']};

const DISALLOWED_RANGES: readonly (readonly [number, number])[] = [
{range_rows}
];
const RESERVED_EXACT = new Set([{exact_rows}]);
const RESERVED_PREFIXES = [{prefix_rows}] as const;

function codePointDisallowed(codePoint: number): boolean {{
  let lo = 0;
  let hi = DISALLOWED_RANGES.length;
  while (lo < hi) {{
    const mid = lo + Math.floor((hi - lo) / 2);
    const codePointRange = DISALLOWED_RANGES[mid];
    if (!codePointRange) return false;
    const [rangeLo, rangeHi] = codePointRange;
    if (codePoint < rangeLo) {{
      hi = mid;
    }} else if (codePoint > rangeHi) {{
      lo = mid + 1;
    }} else {{
      return true;
    }}
  }}
  return false;
}}

/** Return whether value satisfies the versioned Antfly wire policy. */
export function isValidGraphIdentifier(value: string): boolean {{
  if (
    !value ||
    RESERVED_EXACT.has(value) ||
    RESERVED_PREFIXES.some((prefix) => value.startsWith(prefix))
  ) {{
    return false;
  }}

  let codePoints = 0;
  let utf8Bytes = 0;
  let firstCodePoint: number | undefined;
  let lastCodePoint: number | undefined;
  for (const char of value) {{
    const codePoint = char.codePointAt(0);
    if (codePoint === undefined || codePointDisallowed(codePoint)) return false;
    firstCodePoint ??= codePoint;
    lastCodePoint = codePoint;
    codePoints += 1;
    utf8Bytes += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
    if (
      codePoints > MAX_GRAPH_IDENTIFIER_CODE_POINTS ||
      utf8Bytes > MAX_GRAPH_IDENTIFIER_UTF8_BYTES
    )
      return false;
  }}
  return firstCodePoint !== 0x20 && lastCodePoint !== 0x20;
}}

export const GRAPH_IDENTIFIER_CONFORMANCE_CASES = [
{case_rows}
] as const;
'''


def rust_string(value: str) -> str:
    escaped: list[str] = []
    for char in value:
        codepoint = ord(char)
        if char == "\\":
            escaped.append("\\\\")
        elif char == '"':
            escaped.append('\\"')
        elif char == "\n":
            escaped.append("\\n")
        elif char == "\r":
            escaped.append("\\r")
        elif char == "\t":
            escaped.append("\\t")
        elif codepoint == 0:
            escaped.append("\\0")
        elif codepoint < 0x20 or codepoint == 0x7F or codepoint > 0x7E:
            escaped.append(f"\\u{{{codepoint:x}}}")
        else:
            escaped.append(char)
    return '"' + "".join(escaped) + '"'


def render_rust(policy: dict[str, Any], ranges: list[tuple[int, int]]) -> str:
    range_rows = "\n".join(f"    (0x{lo:X}, 0x{hi:X})," for lo, hi in ranges)
    exact_rows = ", ".join(rust_string(value) for value in policy["reserved_exact"])
    prefix_rows = ", ".join(rust_string(value) for value in policy["reserved_prefixes"])
    case_rows = "\n".join(
        f"            ({rust_string(case['name'])}, {rust_string(case['value'])}, {str(case['valid']).lower()}),"
        for case in policy["conformance_cases"]
    )
    return f'''// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Code generated by scripts/generate_graph_identifier_policy.py from
// specs/graph_identifier_policy.json; DO NOT EDIT.

pub const GRAPH_IDENTIFIER_POLICY_VERSION: u32 = {policy['policy_version']};
pub const GRAPH_IDENTIFIER_UNICODE_VERSION: &str = {rust_string(policy['unicode_version'])};
pub const MAX_GRAPH_IDENTIFIER_CODE_POINTS: usize = {policy['max_code_points']};
pub const MAX_GRAPH_IDENTIFIER_UTF8_BYTES: usize = {policy['max_utf8_bytes']};

const DISALLOWED_RANGES: &[(u32, u32)] = &[
{range_rows}
];
const RESERVED_EXACT: &[&str] = &[{exact_rows}];
const RESERVED_PREFIXES: &[&str] = &[{prefix_rows}];

fn code_point_disallowed(code_point: u32) -> bool {{
    DISALLOWED_RANGES
        .binary_search_by(|(lo, hi)| {{
            if code_point < *lo {{
                std::cmp::Ordering::Greater
            }} else if code_point > *hi {{
                std::cmp::Ordering::Less
            }} else {{
                std::cmp::Ordering::Equal
            }}
        }})
        .is_ok()
}}

/// Return whether `value` satisfies the versioned Antfly wire policy.
pub fn is_valid_graph_identifier(value: &str) -> bool {{
    if value.is_empty()
        || value.len() > MAX_GRAPH_IDENTIFIER_UTF8_BYTES
        || RESERVED_EXACT.contains(&value)
        || RESERVED_PREFIXES
            .iter()
            .any(|prefix| value.starts_with(prefix))
    {{
        return false;
    }}

    let mut chars = value.chars();
    let Some(first) = chars.next() else {{
        return false;
    }};
    if first == ' ' || code_point_disallowed(first as u32) {{
        return false;
    }}
    let mut count = 1;
    let mut last = first;
    for code_point in chars {{
        count += 1;
        last = code_point;
        if count > MAX_GRAPH_IDENTIFIER_CODE_POINTS || code_point_disallowed(code_point as u32) {{
            return false;
        }}
    }}
    last != ' '
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn matches_versioned_wire_policy_conformance_cases() {{
        let cases = [
{case_rows}
        ];
        for (name, value, valid) in cases {{
            assert_eq!(is_valid_graph_identifier(value), valid, "{{name}}");
        }}
    }}
}}
'''


def render_openapi(policy: dict[str, Any], ranges: list[tuple[int, int]]) -> str:
    encoded_ranges = "\n".join(f"          - ['{lo:04X}', '{hi:04X}']" for lo, hi in ranges)
    return f'''# Code generated by scripts/generate_graph_identifier_policy.py from
# specs/graph_identifier_policy.json; DO NOT EDIT.
openapi: 3.0.3
info:
  title: Antfly graph identifier policy
  version: "{policy['policy_version']}"
components:
  schemas:
    GraphIdentifier:
      type: string
      minLength: 1
      maxLength: {policy['max_code_points']}
      description: >-
        User-visible graph alias or named result under Antfly graph identifier
        policy v{policy['policy_version']} (Unicode {policy['unicode_version']}). Identifiers are exact UTF-8 strings
        and are not normalized. Ordinary internal ASCII spaces are allowed.
        The value must not equal `*`, begin with `$`, have leading or trailing
        spaces, contain non-ASCII Unicode White_Space, or contain Unicode Cc
        control or Cf format code points. UTF-8 encoding is limited to
        {policy['max_utf8_bytes']} bytes.
      x-antfly-identifier-policy:
        version: {policy['policy_version']}
        unicodeVersion: "{policy['unicode_version']}"
        maxUtf8Bytes: {policy['max_utf8_bytes']}
        reservedExact: ['*']
        reservedPrefixes: ['$']
        disallowedCodePointRanges:
{encoded_ranges}
    GraphCountTarget:
      type: string
      minLength: 1
      maxLength: {policy['max_code_points']}
      description: >-
        Use the reserved token `*` to count rows, or a GraphIdentifier under
        Antfly graph identifier policy v{policy['policy_version']} to count non-null bindings.
      x-antfly-count-target:
        rowSentinel: '*'
        identifierSchema: '#/components/schemas/GraphIdentifier'
'''


def write_or_check(path: Path, content: str, check: bool) -> bool:
    content = content.rstrip() + "\n"
    if check:
        if not path.exists() or path.read_text(encoding="utf-8") != content:
            print(f"generated graph identifier policy is stale: {path.relative_to(ROOT)}", file=sys.stderr)
            return False
        return True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    policy, ranges = load_policy()
    outputs = (
        (ZIG_PATH, render_zig(policy, ranges)),
        (GO_PATH, render_go(policy, ranges)),
        (PYTHON_PATH, render_python(policy, ranges)),
        (TYPESCRIPT_PATH, render_typescript(policy, ranges)),
        (RUST_PATH, render_rust(policy, ranges)),
        (OPENAPI_PATH, render_openapi(policy, ranges)),
    )
    return 0 if all(write_or_check(path, content, args.check) for path, content in outputs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
