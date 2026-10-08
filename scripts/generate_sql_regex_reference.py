# Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
"""Independent PostgreSQL ARE spans; no Python/system regex oracle."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

CASES = [
    ("unicode-captures", "([A-Z])([0-9]+)", "雪😀A1B22", "", 3, 0, 2),
    ("unicode-next", "([A-Z])([0-9]+)", "雪😀A1B22", "", 3, 4, 2),
    ("longest-alternative", "a|ab", "abc", "", 3, 0, 0),
    ("shortest-quantifier", "a+?", "aaa", "", 3, 0, 0),
    ("empty-pattern", "", "雪", "", 3, 0, 0),
    ("empty-end", "$", "雪😀", "", 3, 1, 0),
    ("original-start-anchor", "^a", "ba", "", 3, 1, 0),
    ("unicode-dot", ".", "😀", "", 3, 0, 0),
    ("unicode-lookaround", "(?<=雪)😀(?=A)", "雪😀A", "", 3, 0, 0),
    ("backreference", r"([a-z]+)-\1", "x cat-cat y", "", 3, 0, 1),
    ("unmatched-capture", "(a)?(b)", "b", "", 3, 0, 2),
    ("nested-capture-precedence", "(a|ab)(b*)", "abb", "", 3, 0, 2),
    ("C-class", "[[:alpha:]]+", "雪😀Ab9", "", 3, 0, 0),
    ("C-case", "abc", "雪ABC", "i", 11, 0, 0),
    ("quoted-pattern", "a.b", "xa.by", "q", 4, 0, 0),
    ("expanded-pattern", "a # comment\n b", "zab", "x", 35, 0, 0),
    ("newline-default", ".+", "a\nb", "", 3, 0, 0),
    ("newline-sensitive", ".+", "a\nb", "n", 195, 0, 0),
    ("newline-anchors", "^b$", "a\nb\nc", "n", 195, 0, 0),
    ("counted-quantifier", "a{2,3}", "aaaa", "", 3, 0, 0),
    ("noncapturing", "(?:a|ab)+", "abab", "", 3, 0, 0),
    ("word-boundary", r"\mcat\M", "bobcat cat!", "", 3, 0, 0),
    ("ordered-sensitive", "abc", "ABC", "ic", 3, 0, 0),
    ("ordered-insensitive", "abc", "ABC", "ci", 11, 0, 0),
    ("ordered-single-line", ".+", "a\nb", "ns", 3, 0, 0),
    ("ordered-newline", ".+", "a\nb", "sn", 195, 0, 0),
    ("partial-newline", ".+", "a\nb", "np", 67, 0, 0),
    ("newline-anchors-only", "^b", "a\nb", "nw", 131, 0, 0),
    ("ordered-tight", "a b", "ab a b", "xt", 3, 0, 0),
    ("ordered-expanded", "a b", "ab a b", "tx", 35, 0, 0),
    ("basic-flavor", "a+", "aaa a+", "b", 0, 0, 0),
    ("server-extended-transition", "a+", "aaa a+", "e", 0, 0, 0),
    ("ordered-quoted-flavor", "(a)", "(a)", "eq", 4, 0, 0),
    ("ordered-basic-flavor", "a+", "a+", "qb", 0, 0, 0),
    ("ordered-basic-insensitive", "A+", "a+", "qib", 8, 0, 0),
    ("greedy-backtrack-capture", r"(a|ab|abc)+\1", "x abcabc y", "", 3, 0, 1),
    ("shortest-backtrack-capture", r"(a|ab|abc)+?\1", "x abcabc y", "", 3, 0, 1),
]


GLOBAL_CASES = [
    ("unicode-empty", "", "雪😀", "", 3, 0, 0),
    ("nonempty-then-empty", ".*", "雪😀", "", 3, 0, 0),
    ("optional-captures", "(a)?(b*)", "a雪bb😀", "", 3, 0, 2),
    ("shortest-quantifiers", "a+?", "aaa雪aa", "", 3, 0, 0),
    ("nonzero-start-lookbehind", "(?<=雪)😀|$", "雪😀雪😀", "", 3, 2, 0),
    ("nonzero-start-anchor", "^a|$", "aa", "", 3, 1, 0),
    ("empty-subject", "(a*)", "", "", 3, 0, 1),
    ("no-match", "z+", "雪😀aa", "", 3, 0, 0),
    ("repeated-backreferences", r"([a-z]+)-\1", "cat-cat dog-dog", "", 3, 0, 1),
    ("newline-anchors", "^|$", "a\nb\n", "n", 195, 0, 0),
]


def generate(global_matches=False):
    entries = []
    with postgres() as db:
        if db.execute(
            "SELECT datctype FROM pg_database WHERE datname=current_database()"
        ).fetchone()[0] not in ("C", "POSIX"):
            raise RuntimeError("the explicit C-collation oracle changed")
        cases = GLOBAL_CASES if global_matches else CASES
        for identity, pattern, text, flags, native_flags, start, captures in cases:
            count = (
                db.execute(
                    "SELECT regexp_count(%s,%s,%s,%s)",
                    (text, pattern, start + 1, flags),
                ).fetchone()[0]
                if global_matches
                else 1
            )
            occurrences = []
            for occurrence in range(1, count + 1):
                spans = []
                for group in range(captures + 1):
                    args = (text, pattern, start + 1, occurrence, flags, group)
                    row = db.execute(
                        "SELECT regexp_substr(%s,%s,%s,%s,%s,%s), "
                        "regexp_instr(%s,%s,%s,%s,0,%s,%s), "
                        "regexp_instr(%s,%s,%s,%s,1,%s,%s)",
                        args * 3,
                    ).fetchone()
                    spans.append(
                        {"start": row[1] - 1, "end": row[2] - 1, "text": row[0]}
                    )
                occurrences.append(spans)
            entry = {
                "id": identity,
                "pattern": pattern,
                "input": text,
                "flags": native_flags,
                "start": start,
                "captures": captures,
            }
            if global_matches:
                entry["occurrences"] = occurrences
            else:
                entry["options"] = flags
                entry["matched"] = occurrences[0][0]["text"] is not None
                entry["spans"] = occurrences[0]
            entries.append(entry)
    return {"format": 1, "collation": "C", "entries": entries}


REPLACEMENT_CASES = [
    ("whole-match-unicode", ".", "雪😀", r"<\&>", "", 3, 0, 0),
    ("global-empty", "", "雪😀", "X", "", 3, 0, 0),
    ("empty-after-nonempty", ".*", "雪😀", "X", "", 3, 0, 0),
    ("unmatched-capture", "(a)?(b)", "b", r"<\1>:<\2>", "", 3, 0, 0),
    ("missing-group", "(a)", "a", r"<\9>", "", 3, 0, 0),
    ("unknown-escapes", "a", "a", r"\q\0", "", 3, 0, 0),
    ("literal-backslash", "a", "a", r"\\", "", 3, 0, 0),
    ("trailing-backslash", "a", "a", "x\\", "", 3, 0, 0),
    ("capture-one-then-digit", "(a)", "a", r"\12", "", 3, 0, 0),
    ("escaped-capture-literal", "(a)", "a", r"\\1-\1", "", 3, 0, 0),
    ("second-occurrence", ".", "雪😀a", r"<\&>", "", 3, 0, 2),
    ("start-and-occurrence", ".", "雪😀abc", "X", "", 3, 2, 2),
    ("start-beyond-input", ".", "雪", "X", "", 3, 5, 0),
    ("missing-occurrence", "a", "aba", "X", "", 3, 0, 3),
    ("empty-replacement", "a", "aba", "", "", 3, 0, 0),
    ("nonzero-lookbehind", "(?<=雪)😀|$", "雪😀雪😀", r"<\&>", "", 3, 2, 0),
]


def generate_replacements():
    entries = []
    with postgres() as db:
        if db.execute(
            "SELECT datctype FROM pg_database WHERE datname=current_database()"
        ).fetchone()[0] not in ("C", "POSIX"):
            raise RuntimeError("the explicit C-collation oracle changed")
        for (
            identity,
            pattern,
            text,
            replacement,
            flags,
            native_flags,
            start,
            occurrence,
        ) in REPLACEMENT_CASES:
            result = db.execute(
                "SELECT regexp_replace(%s,%s,%s,%s,%s,%s)",
                (text, pattern, replacement, start + 1, occurrence, flags),
            ).fetchone()[0]
            entries.append(
                {
                    "id": identity,
                    "pattern": pattern,
                    "input": text,
                    "replacement": replacement,
                    "flags": native_flags,
                    "start": start,
                    "occurrence": occurrence,
                    "expected": result,
                }
            )
    return {"format": 1, "collation": "C", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    kind = parser.add_mutually_exclusive_group()
    kind.add_argument("--global-matches", action="store_true")
    kind.add_argument("--replacements", action="store_true")
    args = parser.parse_args()
    observed = (
        generate_replacements() if args.replacements else generate(args.global_matches)
    )
    if args.check:
        if json.loads(args.check.read_text()) != observed:
            raise SystemExit("PostgreSQL regex reference mismatch")
        label = (
            "replacement"
            if args.replacements
            else "global occurrence"
            if args.global_matches
            else "span"
        )
        print(f"Verified {len(observed['entries'])} PostgreSQL ARE {label} contracts")
    else:
        print(json.dumps(observed, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
