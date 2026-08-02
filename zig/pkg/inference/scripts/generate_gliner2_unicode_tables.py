#!/usr/bin/env python3
"""Generate/check GLiNER2's pinned Python 3.12 Unicode preprocessing tables."""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path


PYTHON = (3, 12)
UNICODE = "15.0.0"
OUTPUT = Path(__file__).resolve().parents[1] / "src/finetune/gliner2_unicode_tables.zig"
PINNED_NORMALIZER_CHARMAP_SHA256 = bytes.fromhex(
    "ce10d747170c5e102c99b841d1cdea976d54405ebc44791f9c3636741a894d8f"
)


def intervals(values: list[int]) -> list[tuple[int, int]]:
    if not values:
        return []
    out: list[tuple[int, int]] = []
    first = last = values[0]
    for value in values[1:]:
        if value == last + 1:
            last = value
        else:
            out.append((first, last))
            first = last = value
    out.append((first, last))
    return out


def lower_tables() -> tuple[list[tuple[int, int, int, int]], list[tuple[int, int]], list[tuple[int, tuple[int, ...]]]]:
    mappings: list[tuple[int, int]] = []
    expansions: list[tuple[int, tuple[int, ...]]] = []
    for cp in range(sys.maxunicode + 1):
        lowered = chr(cp).lower()
        if lowered == chr(cp):
            continue
        if len(lowered) == 1:
            mappings.append((cp, ord(lowered)))
        else:
            expansions.append((cp, tuple(map(ord, lowered))))

    covered: set[int] = set()
    ranges: list[tuple[int, int, int, int]] = []
    for step in (1, 2):
        for index, (first, mapped) in enumerate(mappings):
            if first in covered:
                continue
            delta = mapped - first
            last = first
            count = 1
            for cp, lower in mappings[index + 1 :]:
                if cp in covered:
                    continue
                if cp == last + step and lower - cp == delta:
                    last = cp
                    count += 1
                elif cp > last + step:
                    break
            if count >= 3:
                ranges.append((first, last, step, delta))
                covered.update(range(first, last + 1, step))
    ranges.sort()
    pairs = [(cp, lower) for cp, lower in mappings if cp not in covered]

    rebuilt = {cp: cp + delta for first, last, step, delta in ranges for cp in range(first, last + 1, step)}
    rebuilt.update(pairs)
    assert rebuilt == dict(mappings)
    return ranges, pairs, expansions


def final_sigma_property_tables() -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """Derive CPython's Cased/Case_Ignorable context from `str.lower` itself."""
    cased: list[int] = []
    case_ignorable: list[int] = []
    for cp in range(sys.maxunicode + 1):
        if 0xD800 <= cp <= 0xDFFF:
            continue
        char = chr(cp)
        if (char + "Σ").lower().endswith("ς"):
            cased.append(cp)
        elif ("A" + char + "Σ").lower().endswith("ς"):
            case_ignorable.append(cp)
    return intervals(cased), intervals(case_ignorable)


def pinned_normalizer_inert_table() -> list[tuple[int, int]]:
    """Return a conservative concatenation-safe subset of the pinned charsmap.

    The pinned SentencePiece map is NFKC-derived but has a few versioned/custom
    differences.  We admit only assigned L/N/P/S scalars that are individually
    NFKC-stable, cannot be the second scalar in a canonical composition, and
    were exhaustively checked against that exact charsmap.  Marks, separators,
    and controls are deliberately excluded; the runtime separately admits one
    non-leading, non-trailing, non-repeated ASCII space.
    """
    composition_seconds: set[int] = set()
    for cp in range(sys.maxunicode + 1):
        decomposition = unicodedata.decomposition(chr(cp))
        if not decomposition or decomposition.startswith("<"):
            continue
        parts = [int(part, 16) for part in decomposition.split()]
        if len(parts) == 2:
            composition_seconds.add(parts[1])

    charsmap_exceptions = {0x2581, 0xFFFD}
    inert = []
    for cp in range(sys.maxunicode + 1):
        char = chr(cp)
        if unicodedata.category(char)[0] in "MZC":
            continue
        if unicodedata.normalize("NFKC", char) != char:
            continue
        if cp in composition_seconds or cp in charsmap_exceptions:
            continue
        inert.append(cp)
    return intervals(inert)


def simple_casefold_table() -> list[tuple[int, int]]:
    """Scalars whose Unicode 15 casefold is exactly one simple-lower scalar.

    The Zig release scorer deliberately rejects expansions and mappings that
    differ from the already pinned simple-lower table. This makes its admitted
    profile exactly equivalent to Python's ``str.casefold`` without embedding
    a second variable-length mapping table in the runtime.
    """
    simple = []
    for cp in range(sys.maxunicode + 1):
        if 0xD800 <= cp <= 0xDFFF:
            continue
        char = chr(cp)
        folded = char.casefold()
        lowered = char.lower()
        if len(folded) == 1 and folded == lowered:
            simple.append(cp)
    return intervals(simple)


def verify_pinned_tokenizer_json(tokenizer_json: Path) -> None:
    """Differential-check the admitted subset against the exact real normalizer."""
    import hashlib
    import json

    payload = json.loads(tokenizer_json.read_text())
    normalizer = payload.get("normalizer")
    if not isinstance(normalizer, dict) or set(normalizer) != {"type", "normalizers"}:
        raise SystemExit("unsupported tokenizer normalizer shape")
    if normalizer.get("type") != "Sequence":
        raise SystemExit("unsupported tokenizer normalizer type")
    sequence = normalizer.get("normalizers")
    if not isinstance(sequence, list) or len(sequence) != 3:
        raise SystemExit("unsupported tokenizer normalizer sequence")
    if sequence[0] != {"type": "Strip", "strip_left": True, "strip_right": True}:
        raise SystemExit("unsupported tokenizer Strip normalizer")
    precompiled = sequence[1]
    if not isinstance(precompiled, dict) or set(precompiled) != {"type", "precompiled_charsmap"}:
        raise SystemExit("unsupported tokenizer Precompiled normalizer")
    if precompiled.get("type") != "Precompiled" or not isinstance(precompiled.get("precompiled_charsmap"), str):
        raise SystemExit("unsupported tokenizer precompiled charsmap")
    charsmap_digest = hashlib.sha256(precompiled["precompiled_charsmap"].encode()).digest()
    if charsmap_digest != PINNED_NORMALIZER_CHARMAP_SHA256:
        raise SystemExit("tokenizer precompiled charsmap fingerprint mismatch")
    if sequence[2] != {"type": "Replace", "pattern": {"Regex": " {2,}"}, "content": " "}:
        raise SystemExit("unsupported tokenizer Replace normalizer")

    try:
        from tokenizers import Tokenizer
    except ImportError as exc:
        raise SystemExit("--tokenizer-json requires the Python tokenizers package") from exc

    runtime_normalizer = Tokenizer.from_file(str(tokenizer_json)).normalizer
    if runtime_normalizer is None:
        raise SystemExit("tokenizer has no runtime normalizer")
    for first, last in pinned_normalizer_inert_table():
        for cp in range(first, last + 1):
            char = chr(cp)
            normalized = runtime_normalizer.normalize_str(char)
            if normalized != char:
                raise AssertionError((hex(cp), char, normalized))

    for text in ("Москва", "東京", "😀", "é", "Москва 東京 😀"):
        assert runtime_normalizer.normalize_str(text) == text
    for text in ("①", "Ａ", "ﬁ", "e\u0301", " a", "a ", "a  b", "👩‍💻"):
        assert runtime_normalizer.normalize_str(text) != text


def verify_lower_context(
    lower_ranges: list[tuple[int, int, int, int]],
    lower_pairs: list[tuple[int, int]],
    cased_ranges: list[tuple[int, int]],
    case_ignorable_ranges: list[tuple[int, int]],
) -> None:
    """Exhaustively differential-check scalar and final-sigma behavior."""
    lower = {cp: cp + delta for first, last, step, delta in lower_ranges for cp in range(first, last + 1, step)}
    lower.update(lower_pairs)
    cased = {cp for first, last in cased_ranges for cp in range(first, last + 1)}
    case_ignorable = {cp for first, last in case_ignorable_ranges for cp in range(first, last + 1)}

    def table_lower(text: str) -> str:
        out: list[str] = []
        for index, char in enumerate(text):
            cp = ord(char)
            if cp == 0x3A3:
                before = index - 1
                while before >= 0 and ord(text[before]) in case_ignorable:
                    before -= 1
                preceded = before >= 0 and ord(text[before]) in cased
                after = index + 1
                while after < len(text) and ord(text[after]) in case_ignorable:
                    after += 1
                followed = after < len(text) and ord(text[after]) in cased
                if preceded and not followed:
                    out.append("ς")
                    continue
            out.append(chr(lower.get(cp, cp)))
        return "".join(out)

    for cp in range(sys.maxunicode + 1):
        if 0xD800 <= cp <= 0xDFFF or cp == 0x130:
            continue
        char = chr(cp)
        assert table_lower(char) == char.lower()
        for context in ("A" + char + "Σ", "AΣ" + char + "A"):
            assert table_lower(context) == context.lower(), (hex(cp), context)


def render() -> str:
    if sys.version_info[:2] != PYTHON or unicodedata.unidata_version != UNICODE:
        raise SystemExit(
            f"requires Python {PYTHON[0]}.{PYTHON[1]} / Unicode {UNICODE}; "
            f"found {sys.version_info.major}.{sys.version_info.minor} / {unicodedata.unidata_version}"
        )

    word = intervals([cp for cp in range(sys.maxunicode + 1) if re.fullmatch(r"\w", chr(cp))])
    whitespace = intervals([cp for cp in range(sys.maxunicode + 1) if re.fullmatch(r"\s", chr(cp))])
    lower_ranges, lower_pairs, expansions = lower_tables()
    cased, case_ignorable = final_sigma_property_tables()
    normalizer_inert = pinned_normalizer_inert_table()
    simple_casefold = simple_casefold_table()
    assert expansions == [(0x130, (0x69, 0x307))]
    assert "ΟΣ".lower() == "ος" and "ΟΣΑ".lower() == "οσα"
    assert "A\u0301Σ".lower() == "a\u0301ς" and "A-Σ".lower() == "a-σ"
    assert all(any(first <= ord(char) <= last for first, last in normalizer_inert) for char in "Москва東京😀é")
    assert all(not any(first <= ord(char) <= last for first, last in normalizer_inert) for char in "①Ａﬁ\u0301")
    assert all(any(first <= ord(char) <= last for first, last in simple_casefold) for char in "AaÉéМосква東京😀")
    assert all(not any(first <= ord(char) <= last for first, last in simple_casefold) for char in "ßςİ")
    verify_lower_context(lower_ranges, lower_pairs, cased, case_ignorable)

    lines = [
        "// Generated by scripts/generate_gliner2_unicode_tables.py; do not edit.",
        f'pub const unicode_version = "{UNICODE}";',
        "pub const pinned_normalizer_charsmap_sha256 = [_]u8{" +
        ",".join(f" 0x{byte:02x}" for byte in PINNED_NORMALIZER_CHARMAP_SHA256) + " };",
        "",
        "const Range = struct { first: u21, last: u21 };",
        "const LowerRange = struct { first: u21, last: u21, step: u2, delta: i32 };",
        "const LowerPair = struct { upper: u21, lower: u21 };",
        "",
        "const word_ranges = [_]Range{",
    ]
    lines.extend(f"    .{{ .first = 0x{first:X}, .last = 0x{last:X} }}," for first, last in word)
    lines.extend(["};", "", "const whitespace_ranges = [_]Range{"])
    lines.extend(f"    .{{ .first = 0x{first:X}, .last = 0x{last:X} }}," for first, last in whitespace)
    lines.extend(["};", "", "const pinned_normalizer_inert_ranges = [_]Range{"])
    lines.extend(f"    .{{ .first = 0x{first:X}, .last = 0x{last:X} }}," for first, last in normalizer_inert)
    lines.extend(["};", "", "const simple_casefold_ranges = [_]Range{"])
    lines.extend(f"    .{{ .first = 0x{first:X}, .last = 0x{last:X} }}," for first, last in simple_casefold)
    lines.extend(["};", "", "const cased_ranges = [_]Range{"])
    lines.extend(f"    .{{ .first = 0x{first:X}, .last = 0x{last:X} }}," for first, last in cased)
    lines.extend(["};", "", "const case_ignorable_ranges = [_]Range{"])
    lines.extend(f"    .{{ .first = 0x{first:X}, .last = 0x{last:X} }}," for first, last in case_ignorable)
    lines.extend(["};", "", "const lower_ranges = [_]LowerRange{"])
    lines.extend(
        f"    .{{ .first = 0x{first:X}, .last = 0x{last:X}, .step = {step}, .delta = {delta} }},"
        for first, last, step, delta in lower_ranges
    )
    lines.extend(["};", "", "const lower_pairs = [_]LowerPair{"])
    lines.extend(f"    .{{ .upper = 0x{upper:X}, .lower = 0x{lower:X} }}," for upper, lower in lower_pairs)
    lines.extend(
        [
            "};",
            "",
            "pub fn isWord(cp: u21) bool {",
            "    return inRanges(cp, &word_ranges);",
            "}",
            "",
            "pub fn isWhitespace(cp: u21) bool {",
            "    return inRanges(cp, &whitespace_ranges);",
            "}",
            "",
            "/// Conservative scalar set that the exact pinned tokenizer normalizer",
            "/// leaves unchanged in any admitted fragment context.",
        "pub fn isPinnedNormalizerInert(cp: u21) bool {",
        "    return inRanges(cp, &pinned_normalizer_inert_ranges);",
        "}",
        "",
        "/// True when Python 3.12 / Unicode 15 casefold is exactly the",
        "/// scalar returned by simpleLower. Expanding and distinct mappings",
        "/// are excluded so release scoring can fail closed.",
        "pub fn isSimpleCasefold(cp: u21) bool {",
        "    return inRanges(cp, &simple_casefold_ranges);",
        "}",
        "",
        "pub fn isCased(cp: u21) bool {",
            "    return inRanges(cp, &cased_ranges);",
            "}",
            "",
            "pub fn isCaseIgnorable(cp: u21) bool {",
            "    return inRanges(cp, &case_ignorable_ranges);",
            "}",
            "",
            "pub fn simpleLower(cp: u21) u21 {",
            "    for (lower_ranges) |range| {",
            "        if (cp >= range.first and cp <= range.last and (cp - range.first) % range.step == 0) {",
            "            return @intCast(@as(i32, @intCast(cp)) + range.delta);",
            "        }",
            "    }",
            "    var low: usize = 0;",
            "    var high: usize = lower_pairs.len;",
            "    while (low < high) {",
            "        const mid = low + (high - low) / 2;",
            "        if (lower_pairs[mid].upper < cp) low = mid + 1 else high = mid;",
            "    }",
            "    return if (low < lower_pairs.len and lower_pairs[low].upper == cp) lower_pairs[low].lower else cp;",
            "}",
            "",
            "fn inRanges(cp: u21, ranges: []const Range) bool {",
            "    var low: usize = 0;",
            "    var high: usize = ranges.len;",
            "    while (low < high) {",
            "        const mid = low + (high - low) / 2;",
            "        if (ranges[mid].last < cp) low = mid + 1 else high = mid;",
            "    }",
            "    return low < ranges.len and ranges[low].first <= cp;",
            "}",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--tokenizer-json", type=Path)
    args = parser.parse_args()
    if args.check and args.write:
        parser.error("--check and --write are mutually exclusive")
    generated = render()
    if args.tokenizer_json is not None:
        verify_pinned_tokenizer_json(args.tokenizer_json)
    if args.check:
        actual = OUTPUT.read_text()
        if actual != generated:
            print(f"stale Unicode tables: run {Path(__file__).name} --write", file=sys.stderr)
            return 1
        return 0
    if args.write:
        OUTPUT.write_text(generated)
        return 0
    sys.stdout.write(generated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
