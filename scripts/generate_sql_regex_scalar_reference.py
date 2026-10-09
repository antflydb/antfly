# Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
"""Independent PostgreSQL scalar regex values, result OIDs, and SQLSTATEs."""

import argparse
import json
from pathlib import Path

import psycopg
from generate_sql_postgres_reference import postgres

CASES = [
    ("operator-match", "'abc' ~ 'a'"),
    ("operator-imatch", "'abc' ~* 'A'"),
    ("operator-not-match", "'abc' !~ 'A'"),
    ("operator-not-imatch", "'abc' !~* 'A'"),
    ("operator-null-left", "NULL !~ '['"),
    ("operator-null-right", "'abc' ~ NULL"),
    ("operator-not-precedence", "NOT 'abc' ~ 'z'"),
    ("operator-concat-precedence", "'a' || 'bc' ~ '^abc$'"),
    ("operator-comparison-precedence", "'abc' ~ 'a' = true"),
    ("operator-invalid-regex", "'abc' ~ '['"),
    ("operator-wrong-type", "42 ~ 'a'"),
    ("operator-lazy-and", "false AND 'abc' ~ '['"),
    ("operator-inline-flags", "'abc' ~ '(?i)A'"),
    ("like", "regexp_like('雪ABC😀','abc','i')"),
    ("like-miss", "regexp_like('abc','z')"),
    ("like-null", "regexp_like(NULL,'[','z')"),
    ("like-global-refused", "regexp_like('a','a','g')"),
    ("count", "regexp_count('雪😀a😀','😀')"),
    ("count-start", "regexp_count('雪😀a😀','😀',3)"),
    ("count-flags", "regexp_count('aA','a',1,'i')"),
    ("count-empty", "regexp_count('雪😀','')"),
    ("count-past-end", "regexp_count('雪','.',9)"),
    ("count-negative-start", "regexp_count('a','a',-1)"),
    ("count-null-start", "regexp_count('a','[',NULL)"),
    ("instr", "regexp_instr('雪😀A1B22','([A-Z])([0-9]+)')"),
    ("instr-next", "regexp_instr('雪😀A1B22','([A-Z])([0-9]+)',1,2)"),
    ("instr-end", "regexp_instr('雪😀A1B22','([A-Z])([0-9]+)',1,2,1)"),
    ("instr-capture", "regexp_instr('雪😀A1B22','([a-z])([0-9]+)',1,2,0,'i',2)"),
    ("instr-missing-capture", "regexp_instr('a','(a)',1,1,0,'',9)"),
    ("instr-unmatched", "regexp_instr('b','(a)?b',1,1,0,'',1)"),
    ("instr-empty", "regexp_instr('雪😀','$',1,1,1)"),
    ("instr-bad-end", "regexp_instr('a','a',1,1,2)"),
    ("instr-zero-occurrence", "regexp_instr('a','a',1,0)"),
    ("substr", "regexp_substr('雪😀A1B22','([A-Z])([0-9]+)')"),
    ("substr-next", "regexp_substr('雪😀A1B22','([A-Z])([0-9]+)',1,2)"),
    ("substr-capture", "regexp_substr('雪😀A1B22','([a-z])([0-9]+)',1,2,'i',2)"),
    ("substr-empty", "regexp_substr('雪😀','$')"),
    ("substr-miss", "regexp_substr('a','z')"),
    ("substr-unmatched", "regexp_substr('b','(a)?b',1,1,'',1)"),
    ("substr-negative-group", "regexp_substr('a','(a)',1,1,'',-1)"),
    ("substr-text-position", "regexp_substr('雪😀a','.', '2')"),
    ("replace-first", "regexp_replace('aba','a','X')"),
    ("replace-global", "regexp_replace('aba','a','X','g')"),
    ("replace-start", "regexp_replace('ababa','a','X',2)"),
    ("replace-nth", "regexp_replace('ababa','a','X',2,2)"),
    ("replace-all-from-start", "regexp_replace('ababa','a','X',2,0)"),
    ("replace-explicit-n-ignores-g", "regexp_replace('aAa','a','X',1,2,'gi')"),
    ("replace-captures", r"regexp_replace('ab a','(a)(b)?',E'<\\2>-\\1-\\&','g')"),
    ("replace-empty", "regexp_replace('雪😀','','X','g')"),
    ("replace-null-pattern", "regexp_replace('a',NULL,'X','z')"),
    ("replace-null-fourth", "regexp_replace('a','[','X',NULL)"),
    ("replace-zero-start", "regexp_replace('a','a','X',0)"),
    ("replace-negative-nth", "regexp_replace('a','a','X',1,-1)"),
    ("invalid-regex", "regexp_replace('a','[','X')"),
    ("invalid-flags", "regexp_replace('a','a','X','z')"),
    ("lazy-case", "CASE WHEN false THEN regexp_replace('a','[','X') ELSE 'safe' END"),
    ("missing-overload", "regexp_replace('a','a')"),
    ("wrong-type", "regexp_like(42,'a')"),
    ("bigint-position", "regexp_count('abc','a',CAST(1 AS bigint))"),
    ("smallint-position", "regexp_count('abc','a',CAST(1 AS smallint))"),
    ("int-position", "regexp_count('abc','a',CAST(1 AS integer))"),
]


def generate():
    entries = []
    with postgres() as db:
        locale = db.execute(
            "SELECT datctype FROM pg_database WHERE datname=current_database()"
        ).fetchone()[0]
        if locale not in ("C", "POSIX"):
            raise RuntimeError("the explicit C-collation oracle changed")
        for identity, expression in CASES:
            entry = {"id": identity, "expression": expression}
            try:
                cursor = db.execute("SELECT " + expression)
                entry.update(
                    value=cursor.fetchone()[0],
                    oid=cursor.description[0].type_code,
                    sqlstate=None,
                )
            except psycopg.Error as err:
                entry.update(value=None, oid=None, sqlstate=err.sqlstate)
            entries.append(entry)
    return {"format": 1, "collation": "C", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    observed = generate()
    if args.check:
        if json.loads(args.check.read_text()) != observed:
            raise SystemExit("PostgreSQL scalar regex reference mismatch")
        print(f"Verified {len(observed['entries'])} PostgreSQL scalar regex contracts")
    else:
        print(json.dumps(observed, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
