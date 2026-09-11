# Copyright 2026 Antfly, Inc.
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

"""Keep first-party checksum callers on antfly_hash, retaining std test oracles.

This is a lexical policy check, not Zig type checking. It recognizes ordinary
aliases of std/hash/crc and literal @field access, skips comments and strings,
and exempts test blocks and the shared checksum implementation itself. New
checksum algorithms with different polynomials are outside this policy.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

TOKEN = re.compile(
    r"(?P<skip>\s+|//[^\n]*|\\\\[^\n]*)"
    r'|(?P<quoted_identifier>@"(?:\\.|[^"\\])*")'
    r'|(?P<string>"(?:\\.|[^"\\])*")'
    r"|(?P<char>'(?:\\.|[^'\\])*')"
    r"|(?P<identifier>@?[A-Za-z_][A-Za-z_0-9]*)"
    r"|(?P<number>0[xX][0-9a-fA-F_]+|[0-9][0-9_]*)"
    r"|(?P<punct>.)"
)
REPLACEMENTS = {
    "std.hash.Crc32": "Crc32",
    "std.hash.crc.Crc32": "Crc32",
    "std.hash.crc.Crc32IsoHdlc": "Crc32",
    "std.hash.crc.Crc32Iscsi": "Crc32c",
    "std.hash.crc.Crc64Nvme": "Crc64Nvme",
    "std.hash.Adler32": "Adler32",
}
POLYNOMIALS = {
    0x04C11DB7: "Crc32",
    0x1EDC6F41: "Crc32c",
    0xAD93D23594C93659: "Crc64Nvme",
}
SKIP_DIRS = {"zig-out", "node_modules", "__pycache__"}


@dataclass(frozen=True)
class Token:
    kind: str
    value: str
    offset: int


def tokens(source: str) -> list[Token]:
    result = []
    for match in TOKEN.finditer(source):
        kind = match.lastgroup
        if kind == "skip":
            continue
        value = match.group()
        if kind == "quoted_identifier":
            value = value[2:-1]
        result.append(Token(kind, value, match.start()))
    return result


def expression(ts: list[Token], start: int, aliases: dict[str, str]) -> tuple[str, int]:
    """Resolve a std import/alias and its field chain, without evaluating Zig."""
    if start >= len(ts):
        return "", start
    value = ts[start].value
    end = start + 1
    if value == "@import" and [t.value for t in ts[start + 1 : start + 4]] == [
        "(",
        '"std"',
        ")",
    ]:
        path, end = "std", start + 4
    elif value == "@field" and start + 1 < len(ts) and ts[start + 1].value == "(":
        path, end = expression(ts, start + 2, aliases)
        if (
            not path
            or end + 2 >= len(ts)
            or ts[end].value != ","
            or ts[end + 1].kind != "string"
            or ts[end + 2].value != ")"
        ):
            return "", start + 1
        path += "." + ts[end + 1].value[1:-1]
        end += 3
    elif ts[start].kind in {"identifier", "quoted_identifier"}:
        path = aliases.get(value, "")
    else:
        return "", end
    if not path:
        return "", end
    while (
        end + 1 < len(ts)
        and ts[end].value == "."
        and ts[end + 1].kind in {"identifier", "quoted_identifier"}
    ):
        path += "." + ts[end + 1].value
        end += 2
    return path, end


def generic_replacement(ts: list[Token], start: int) -> str | None:
    if start + 2 >= len(ts) or ts[start].value != "(":
        return None
    width = ts[start + 1].value
    if width not in {"u32", "u64"} or ts[start + 2].value != ",":
        return None
    fields = {}
    depth = 0
    for index in range(start, len(ts)):
        token = ts[index]
        if token.kind == "punct":
            if token.value == "(":
                depth += 1
            elif token.value == ")":
                depth -= 1
                if depth == 0:
                    break
        if index + 3 < len(ts) and token.value == "." and ts[index + 2].value == "=":
            value = ts[index + 3]
            # Only recognize literal complete values, not computed expressions.
            if index + 4 >= len(ts) or ts[index + 4].value not in {",", "}"}:
                continue
            if value.kind == "number":
                number = value.value.replace("_", "")
                fields[ts[index + 1].value] = int(
                    number, 16 if number.lower().startswith("0x") else 10
                )
            elif value.value in {"true", "false"}:
                fields[ts[index + 1].value] = value.value == "true"
    # The polynomial alone does not identify a CRC. Preserve variants such as
    # CRC32/MPEG-2, which share IEEE's polynomial but use different parameters.
    mask = (1 << int(width[1:])) - 1
    if (
        fields.get("initial") == mask
        and fields.get("xor_output") == mask
        and fields.get("reflect_input") is True
        and fields.get("reflect_output") is True
    ):
        replacement = POLYNOMIALS.get(fields.get("polynomial"))
        if replacement and (width == "u64") == (replacement == "Crc64Nvme"):
            return replacement
    return None


def delimiter_pairs(ts: list[Token]) -> dict[int, int]:
    pairs = {}
    stack = []
    for index, token in enumerate(ts):
        if token.kind != "punct":
            continue
        if token.value in {"(", "[", "{"}:
            stack.append(index)
        elif token.value in {")", "]", "}"} and stack:
            opening = stack.pop()
            pairs[opening] = index
            pairs[index] = opening
    return pairs


def declaration(
    ts: list[Token], index: int, pairs: dict[int, int]
) -> tuple[str, int] | None:
    """Find an initializer, including declarations with explicit type annotations."""
    if (
        ts[index].kind != "identifier"
        or ts[index].value not in {"const", "var"}
        or index + 2 >= len(ts)
        or ts[index + 1].kind not in {"identifier", "quoted_identifier"}
    ):
        return None
    cursor = index + 2
    if ts[cursor].value == ":":
        cursor += 1
        while cursor < len(ts) and ts[cursor].value not in {"=", ";", "}"}:
            cursor = pairs[cursor] + 1 if pairs.get(cursor, -1) > cursor else cursor + 1
    if cursor < len(ts) and ts[cursor].value == "=":
        return ts[index + 1].value, cursor + 1
    return None


def container_body(ts: list[Token], opening: int, pairs: dict[int, int]) -> bool:
    previous = opening - 1
    if previous >= 0 and ts[previous].value == ")":
        previous = pairs.get(previous, previous) - 1
    return (
        previous >= 0
        and ts[previous].kind == "identifier"
        and ts[previous].value in {"struct", "union", "enum", "opaque"}
    )


def container_aliases(
    ts: list[Token],
    start: int,
    end: int,
    inherited: dict[str, str],
    pairs: dict[int, int],
) -> dict[str, str]:
    """Resolve container declarations before scanning uses; locals stay sequential."""
    declarations = {}
    index = start
    while index < end:
        if binding := declaration(ts, index, pairs):
            name, initializer = binding
            declarations[name] = initializer
        # Do not collect declarations from nested containers, functions or tests.
        index = pairs[index] + 1 if pairs.get(index, -1) > index else index + 1
    aliases = inherited.copy()
    for name in declarations:
        aliases.pop(name, None)
    # An alias may depend on another declared later, including multiple hops.
    # The bound also makes invalid cyclic declarations terminate predictably.
    for _ in range(len(declarations)):
        changed = False
        for name, initializer in declarations.items():
            path, _ = expression(ts, initializer, aliases)
            if aliases.get(name, "") != path:
                if path:
                    aliases[name] = path
                else:
                    aliases.pop(name, None)
                changed = True
        if not changed:
            break
    return aliases


def violations(source: str) -> list[tuple[int, str, str]]:
    # Every supported checksum reference (including the generic Crc factory)
    # contains one of these names. Avoid tokenizing unrelated generated files.
    if "Crc" not in source and "Adler32" not in source:
        return []
    ts = tokens(source)
    pairs = delimiter_pairs(ts)
    scopes = [container_aliases(ts, 0, len(ts), {}, pairs)]
    found = []
    index = 0
    while index < len(ts):
        token = ts[index]
        # test "name" { ... } and unnamed test { ... }, including nested braces.
        if token.kind == "identifier" and token.value == "test":
            body = index + 1
            if body < len(ts) and ts[body].kind == "string":
                body += 1
            if body < len(ts) and ts[body].value == "{":
                depth = 1
                index = body + 1
                while index < len(ts) and depth:
                    if ts[index].kind == "punct":
                        depth += (ts[index].value == "{") - (ts[index].value == "}")
                    index += 1
                continue
        if token.kind == "punct" and token.value == "{":
            aliases = scopes[-1].copy()
            if container_body(ts, index, pairs):
                aliases = container_aliases(
                    ts, index + 1, pairs.get(index, len(ts)), aliases, pairs
                )
            scopes.append(aliases)
        elif token.kind == "punct" and token.value == "}" and len(scopes) > 1:
            scopes.pop()
        # Resolve namespace aliases; clear aliases shadowed by unrelated locals.
        if binding := declaration(ts, index, pairs):
            name, initializer = binding
            path, _ = expression(ts, initializer, scopes[-1])
            if path:
                scopes[-1][name] = path
            else:
                scopes[-1].pop(name, None)
            # Check the annotation and initializer, not the newly declared name.
            index += 2
            continue
        path, end = expression(ts, index, scopes[-1])
        replacement = next(
            (
                new
                for old, new in REPLACEMENTS.items()
                if path == old or path.startswith(old + ".")
            ),
            None,
        )
        if path == "std.hash.crc.Crc":
            replacement = generic_replacement(ts, end)
        if replacement:
            found.append((source.count("\n", 0, token.offset) + 1, path, replacement))
        index = max(index + 1, end)
    return found


def check(root: Path) -> list[str]:
    errors = []
    for path in sorted((root / "zig").rglob("*.zig")):
        relative = path.relative_to(root)
        if any(part.startswith(".") or part in SKIP_DIRS for part in relative.parts):
            continue
        # This module owns the kernels and their standard-library oracles.
        if relative.parts[:3] == ("zig", "lib", "hash"):
            continue
        for line, original, replacement in violations(path.read_text()):
            errors.append(
                f'{relative}:{line}: use @import("antfly_hash").{replacement} instead of {original}'
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    args = parser.parse_args()
    if not (args.root / "zig").is_dir():
        parser.error(f"missing Zig tree: {args.root / 'zig'}")
    errors = check(args.root)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        print(
            "Keep std checksum oracles inside test blocks or zig/lib/hash.",
            file=sys.stderr,
        )
        return 1
    print("Zig checksum usage: all production callers use antfly_hash")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
