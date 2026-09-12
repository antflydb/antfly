#!/usr/bin/env python3
"""Capture pure Python 3.12 / Unicode 15 regex semantics without model loads."""
import argparse
import json
from pathlib import Path
import platform
import re
import sys
import unicodedata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[2] / "testdata/gliner25/regex.json")
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 12) or unicodedata.unidata_version != "15.0.0":
        raise SystemExit("requires pinned Python 3.12 and Unicode 15.0.0")
    patterns = [
        "", "a", "A", "a|b", "|a", "a|", "a||b", "(?:a|ab)b", "(a+)+b", "(a?)*b",
        "a*", "a+", "a?", "a{0}", "a{2}", "a{1,3}", "a{2,}", "a{,2}", "a{,}",
        "a*?b", "a+?b", "a??b", "a{1,3}?b", "a{999999999999999x", r"a\{2\}",
        ".", ".*", "^a$", "a$", "^", "$", r"\A", r"\Z", r"\Aa\Z", r"\b", r"\B",
        "(?:^)+", "(^)*", r"(?:\b){2}", "(?:$)?", "(?:a{0})+",
        r"\ba\b", r"\B_\B", r"\w+", r"\W+", r"\d+", r"\D+", r"\s+", r"\S+",
        "[a-z]+", "[^a-z]+", "[A-Z]", "[a-zA-Z0-9_]", r"[^\W_]", r"[\w.-]+",
        r"[\D_]", r"[\d\s]", "[]a-]+", "[[]", r"[\1-\7]", r"[\b]", r"\000",
        r"\141", r"\x61", r"\u0061", r"\U0001f600", r"\uD800", r"[\x61-\u007a]",
        "[İıI]+", "[Σςσ]+", "[KKk]+", "[ßẞ]+", "[\u0100-\u0200]+",
        r"[\[\]\\]+", r"a\ b", r"a\#b", "a # comment\n b", r"(a|b){0,3}",
    ]
    texts = [
        "", "a", "A", "b", "ab", "aab", "aaab", "aaabb", "aaaaac", "xabby", "ba", "aaa",
        "a\n", "a\nb", "x\na\ny", "\n", "\r\n", " ", "\t", "\x01", "\x07", "\b",
        "a b", "a#b", "[]\\", "-]a", "_", "__", "x_y", "123", "٠١٢", "½", "Ⅳ",
        "é", "E\u0301", "😀", "İ", "ı", "i", "I", "ſ", "K", "Σ", "ς", "σ", "ß", "ẞ", "ss",
        "\u00a0", "\u2028", "\U00011f50", "\U0001e4f0", "\U0001d7ce", "Ā", "Ȁ",
        "a{2}", "a{999999999999999x",
    ]
    cases = []
    for pattern in patterns:
        for flags in (0, re.I, re.I | re.A, re.M | re.S, re.I | re.M | re.X):
            compiled = re.compile(pattern, flags)
            cases.append({"pattern": pattern, "flags": int(flags), "expected": {
                mode: [getattr(compiled, mode)(text) is not None for text in texts]
                for mode in ("fullmatch", "search", "match")
            }})
    ranges = []
    for cp in range(0x110000):
        if unicodedata.category(chr(cp)) == "Nd":
            if ranges and ranges[-1]["last"] == cp - 1:
                ranges[-1]["last"] = cp
            else:
                ranges.append({"first": cp, "last": cp})
    result = {"format_version": 2, "provenance": {"python": platform.python_version(),
              "unicode": unicodedata.unidata_version, "upstream_commit": "3c913c7369301133d3b7699252074c4303ada50e"},
              "decimal_ranges": ranges, "texts": texts, "cases": cases}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=True, separators=(",", ":")) + "\n")
    print(f"wrote {len(cases)} programs, {len(cases) * len(texts)} text cases, {len(ranges)} Nd ranges")


if __name__ == "__main__":
    main()
