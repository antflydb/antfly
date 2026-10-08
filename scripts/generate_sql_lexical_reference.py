# Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
"""Independent PostgreSQL escape-string values and lexical SQLSTATEs."""

import argparse
import json
from pathlib import Path

import psycopg
from generate_sql_postgres_reference import postgres

CASES = [
    ("empty", "E''"),
    ("lowercase-prefix", "e'hello'"),
    ("ordinary-backslashes", r"'\n\u0041\\'"),
    ("control-escapes", r"E'\b\f\n\r\t'"),
    ("escaped-quotes", r"E'a\'b''c\\'"),
    ("unknown-escapes", r"E'\q\8\&\雪'"),
    ("octal-width", r"E'\1\12\1234'"),
    ("hex-width", r"E'\x1\x12\x123\xz'"),
    ("hex-utf8", r"E'\xC3\xA9'"),
    ("octal-utf8", r"E'\303\251'"),
    ("unicode", r"E'\u96EA\U0001F600'"),
    ("surrogate-pair", r"E'\uD83D\uDE00'"),
    ("wide-surrogate-pair", r"E'\U0000D83D\U0000DE00'"),
    ("mixed-surrogate-pair", r"E'\uD83D\U0000DE00'"),
    ("backslash-newline", "E'a\\\nb'"),
    ("newline-continuation", "E'a'\n'\\n'"),
    ("ordinary-continuation", "'a'\n'\\n'"),
    ("line-comment-continuation", "E'a' -- note\n'\\n'"),
    ("block-comment-newline-only", "E'a' /*\n*/ 'b'"),
    ("block-comment-after-newline", "E'a'\n/* note */'b'"),
    ("space-not-continuation", "E'a' 'b'"),
    ("prefix-not-continuation", "E'a'\nE'b'"),
    ("unicode-short", r"E'\u12'"),
    ("unicode-bad-digit", r"E'\uZZZZ'"),
    ("unicode-overflow", r"E'\UFFFFFFFF'"),
    ("unicode-outside-range", r"E'\U00110000'"),
    ("surrogate-alone", r"E'\uD800'"),
    ("low-surrogate-alone", r"E'\uDC00'"),
    ("surrogate-wrong-tail", r"E'\uD800\u0041'"),
    ("surrogate-separated", r"E'\uD800x\uDC00'"),
    ("unicode-zero", r"E'\u0000'"),
    ("octal-zero", r"E'\000'"),
    ("hex-zero", r"E'\x00'"),
    ("invalid-utf8", r"E'\xff'"),
    ("octal-truncation-invalid", r"E'\777'"),
    ("octal-truncation-valid", r"E'\541'"),
    ("unterminated", "E'abc"),
    ("trailing-backslash", "E'abc\\"),
]


def generate():
    entries = []
    with postgres() as db:
        for identity, expression in CASES:
            entry = {"id": identity, "expression": expression}
            try:
                entry.update(
                    value=db.execute("SELECT " + expression).fetchone()[0],
                    sqlstate=None,
                )
            except psycopg.Error as err:
                entry.update(value=None, sqlstate=err.sqlstate)
            entries.append(entry)
    return {"format": 1, "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    observed = generate()
    if args.check:
        if json.loads(args.check.read_text()) != observed:
            raise SystemExit("PostgreSQL lexical reference mismatch")
        print(f"Verified {len(observed['entries'])} PostgreSQL lexical contracts")
    else:
        print(json.dumps(observed, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
