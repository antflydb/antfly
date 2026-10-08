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
]


def generate():
    entries = []
    with postgres() as db:
        if db.execute(
            "SELECT datctype FROM pg_database WHERE datname=current_database()"
        ).fetchone()[0] not in ("C", "POSIX"):
            raise RuntimeError("the explicit C-collation oracle changed")
        for identity, pattern, text, flags, native_flags, start, captures in CASES:
            spans = []
            for group in range(captures + 1):
                args = (text, pattern, start + 1, 1, flags, group)
                row = db.execute(
                    "SELECT regexp_substr(%s,%s,%s,%s,%s,%s), "
                    "regexp_instr(%s,%s,%s,%s,0,%s,%s), "
                    "regexp_instr(%s,%s,%s,%s,1,%s,%s)",
                    args * 3,
                ).fetchone()
                spans.append({"start": row[1] - 1, "end": row[2] - 1, "text": row[0]})
            entries.append(
                {
                    "id": identity,
                    "pattern": pattern,
                    "input": text,
                    "flags": native_flags,
                    "start": start,
                    "captures": captures,
                    "matched": spans[0]["text"] is not None,
                    "spans": spans,
                }
            )
    return {"format": 1, "collation": "C", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    observed = generate()
    if args.check:
        if json.loads(args.check.read_text()) != observed:
            raise SystemExit("PostgreSQL regex reference mismatch")
        print(f"Verified {len(observed['entries'])} PostgreSQL ARE span contracts")
    else:
        print(json.dumps(observed, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
