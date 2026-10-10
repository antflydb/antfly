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

"""Independent bounded regex differential campaign; never reclassifies SQL cases."""

import argparse
import hashlib
import itertools
import json
import random
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from generate_sql_postgres_reference import postgres

ROOT = Path(__file__).resolve().parents[1]
QUANTIFIERS = (
    "",
    "*",
    "*?",
    "+",
    "+?",
    "?",
    "??",
    "{0}",
    "{1}",
    "{1}?",
    "{1,1}",
    "{1,1}?",
    "{2}",
    "{2}?",
    "{0,2}",
    "{0,2}?",
)


@dataclass(frozen=True)
class Pattern:
    text: str
    captures: int = 0


@dataclass(frozen=True)
class Case:
    pattern: str
    input: str
    captures: int
    options: str = ""
    start: int = 0

    def record(self):
        value = asdict(self)
        value["id"] = hashlib.sha256(
            json.dumps(value, sort_keys=True).encode()
        ).hexdigest()[:24]
        return value


def subjects(maximum):
    """All binary strings up to the declared bound; boundary witnesses are extra."""
    return [
        "".join(chars)
        for size in range(maximum + 1)
        for chars in itertools.product("ab", repeat=size)
    ]


def patterns():
    # This independent grammar deliberately distinguishes syntactically fixed
    # bounds from equal-endpoint variable bounds. No native AST is consulted.
    result = [Pattern("^"), Pattern("$")]
    result += [
        Pattern(atom + quantifier)
        for atom in ("a", "b", ".", "[ab]", "[^a]")
        for quantifier in QUANTIFIERS
    ]
    cores = ("a*", "a*?", "a+", "a+?", "a?", "a??", "a{1}", "a{1,1}", "a|aa", "a|b")
    result += [
        Pattern(f"({left})({right})", 2)
        for left, right in itertools.product(cores, repeat=2)
    ]
    result += [
        Pattern(f"({core}){quantifier}", 1)
        for core, quantifier in itertools.product(cores, QUANTIFIERS)
    ]
    result += [
        Pattern(f"(?:({core}){quantifier})(a*)", 2)
        for core in ("a*?", "a+?", "a|aa")
        for quantifier in QUANTIFIERS
    ]
    return list(dict.fromkeys(result))


def fuzz_pattern(rng, depth):
    if depth == 0:
        return Pattern(rng.choice(("a", "b", ".", "[ab]", "[^a]", r"\w", r"\D", "")))
    kind = rng.randrange(5)
    left = fuzz_pattern(rng, depth - 1)
    if kind in (0, 1):
        right = fuzz_pattern(rng, depth - 1)
        return Pattern(
            f"(?:{left.text}){'|' if kind == 1 else ''}(?:{right.text})",
            left.captures + right.captures,
        )
    if kind == 2:
        return Pattern(f"({left.text})", left.captures + 1)
    if kind == 3:
        return Pattern(f"(?:{left.text}){rng.choice(QUANTIFIERS)}", left.captures)
    return Pattern(f"(?{rng.choice(('=', '!', '<=', '<!'))}{left.text})")


def cases(maximum=2, seed=0, fuzz=0):
    corpus = []
    for pattern, text in itertools.product(patterns(), subjects(maximum)):
        for start in range(len(text) + 1):
            corpus.append(Case(pattern.text, text, pattern.captures, start=start))
    boundaries = ("a\nb", "\na\n", "雪a😀", "aA1_ ", "\\", "a{")
    for pattern in (
        Pattern(".+"),
        Pattern("[^a]+"),
        Pattern(r"[\D]+"),
        Pattern(r"\w+"),
        Pattern("^a$"),
        Pattern("([a-z]+)", 1),
    ):
        for text, options in itertools.product(
            boundaries, ("", "i", "n", "p", "w", "s", "ic", "ci", "ns", "sn")
        ):
            corpus.append(Case(pattern.text, text, pattern.captures, options))
    for pattern, captures in (
        (r"a^b", 0),
        (r"a$b", 0),
        (r"\(a*\)", 1),
        (r"a\{1,2\}", 0),
        (r"a\+", 0),
        (r"^*", 0),
        (r"a\(b\)\1", 1),
    ):
        for text, options in itertools.product(
            ("a", "aa", "a^b", "a$b", "abb", "*", "a+"), ("b", "e")
        ):
            corpus.append(Case(pattern, text, captures, options))
    for pattern in (
        "(",
        ")",
        "[",
        "[]",
        "a{256}",
        "a{2,1}",
        "(?z)a",
        r"\q",
        r"(a)\2",
        "a**",
        "^*",
        "(?=a)*",
        "a{1}{2}",
    ):
        corpus.append(Case(pattern, "aaa", 0))
    rng = random.Random(seed)
    for _ in range(fuzz):
        pattern = fuzz_pattern(rng, rng.randint(1, 4))
        text = "".join(rng.choices("ab\n雪", k=rng.randrange(13)))
        corpus.append(
            Case(
                pattern.text,
                text,
                pattern.captures,
                rng.choice(("", "i", "n", "p", "w")),
                rng.randrange(len(text) + 1),
            )
        )
    return list(dict.fromkeys(corpus))


def regressions():
    """Permanent minimized witnesses from differential discovery, not samples."""
    result = [
        Case(pattern, text, count, options)
        for pattern, text, count, options in (
            ("(a{1})(a*?)", "aa", 2, ""),
            ("(a{1}?)(a*?)", "aa", 2, ""),
            ("(a{1,1})(a*?)", "aa", 2, ""),
            ("(a*)*?", "", 1, ""),
            ("(a*?)*?", "", 1, ""),
            ("(a*)??", "", 1, ""),
            ("(a*?)??", "", 1, ""),
            ("(a*){1}?", "aa", 1, ""),
            ("(a*?){1}", "aa", 1, ""),
            ("(a*){2}?", "aa", 1, ""),
            ("(a*){0,2}?", "", 1, ""),
            ("(?:(a*?){0})(a*)", "aa", 2, ""),
            ("(?:(a+?){0})(a*)", "aa", 2, ""),
            ("a^b", "a^b", 0, "b"),
            ("a$b", "a$b", 0, "b"),
            ("^*", "*", 0, "b"),
            ("^*", "*", 0, "e"),
            (r"\(a*\)", "aa", 1, "b"),
            ("(?:(?!a)){2}", "b", 0, ""),
            ("(?:(?<=a))+", "ab", 0, ""),
            ("(?:(?:((?!.))){1,1}?)?", "", 1, "n"),
            (r"(?:((?:(?<!\w)){0,2}?))*?", "b", 1, ""),
            (r"(?:(?:(?!(?:[^a])(?:\D)))(?:(?:()){0,2}?)){0,2}", "a", 1, ""),
            (r"(?:\D){2}", "\n\n", 0, "n"),
            (r"\W+", "\n雪", 0, "n"),
            (r"[\D]+", "\n雪", 0, "n"),
            (r"[^\d]+", "\n雪", 0, "n"),
            (r"[^\D]+", "1\n2", 0, "n"),
        )
    ]
    result += [
        Case(pattern, "aaa", 0)
        for pattern in (
            "(",
            ")",
            "[",
            "a{256}",
            "(?z)a",
            "a**",
            "^*",
            "(?=a)*",
            "a{1}{2}",
        )
    ]
    return result


def profile(db, required_major):
    observed = db.execute(
        "SELECT current_setting('server_version_num')::int, current_setting('server_encoding'), datctype, version() FROM pg_database WHERE datname=current_database()"
    ).fetchone()
    major = observed[0] // 10000
    if (
        major != required_major
        or observed[1] != "UTF8"
        or observed[2] not in ("C", "POSIX")
    ):
        raise RuntimeError(
            f"oracle profile mismatch: required PostgreSQL {required_major} UTF8/C, observed {observed}"
        )
    binary = (
        Path(
            db.execute("SELECT setting FROM pg_config WHERE name='BINDIR'").fetchone()[
                0
            ]
        )
        / "postgres"
    )
    return {
        "postgres_major": major,
        "server_version_num": observed[0],
        "server_build": observed[3],
        "server_binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "encoding": observed[1],
        "collation": "C",
        "statement_timeout": db.execute("SHOW statement_timeout").fetchone()[0],
    }


def reference(db, case):
    import psycopg

    entry = case.record()
    # One independently executed PostgreSQL query returns every character span.
    # The generated grammar supplies capture arity, which native compilation
    # also checks. No Python regex engine or native output generates a golden.
    args = (case.input, case.pattern, case.start + 1, case.options)
    try:
        found, spans = db.execute(
            "SELECT regexp_substr(%s,%s,%s,1,%s,0) IS NOT NULL, "
            "jsonb_agg(jsonb_build_object('start',regexp_instr(%s,%s,%s,1,0,%s,g)-1,'end',regexp_instr(%s,%s,%s,1,1,%s,g)-1) ORDER BY g) "
            "FROM generate_series(0,%s) g GROUP BY 1",
            args * 3 + (case.captures,),
        ).fetchone()
    except psycopg.Error as exc:
        if exc.sqlstate not in ("2201B", "22023"):
            raise RuntimeError(
                f"inconclusive oracle {entry['id']}: {exc.sqlstate}"
            ) from exc
        entry.update(sqlstate=exc.sqlstate)
    else:
        entry.update(matched=found, spans=spans)
    return entry


def run_probe(probe, artifact, directory):
    witness = directory / "witness.json"
    report = directory / "native-report.json"
    witness.write_text(json.dumps(artifact, ensure_ascii=False))
    subprocess.run([probe, witness, report], check=True, timeout=120)
    value = json.loads(report.read_text())
    expected_profile = {
        key: artifact["profile"][key]
        for key in ("postgres_major", "encoding", "collation")
    }
    digest = hashlib.sha256(witness.read_bytes()).hexdigest()
    mismatch_count = value.get("mismatch_count", -1)
    failures = value.get("failures", [])
    ids = {entry["id"] for entry in artifact["entries"]}
    if (
        value["format"] != 1
        or value["checked"] != len(artifact["entries"])
        or value["profile"] != expected_profile
        or value.get("witness_sha256") != digest
        or not 0 <= mismatch_count <= len(artifact["entries"])
        or len(failures) != min(mismatch_count, 50)
        or len({failure["id"] for failure in failures}) != len(failures)
        or any(failure["id"] not in ids for failure in failures)
    ):
        raise RuntimeError("incomplete or incompatible native evidence")
    value["native_probe_sha256"] = hashlib.sha256(Path(probe).read_bytes()).hexdigest()
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-major", type=int, default=19, choices=(18, 19))
    parser.add_argument(
        "--suite", choices=("selection", "regressions"), default="selection"
    )
    parser.add_argument("--max-subject", type=int, default=2, choices=range(5))
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--fuzz", type=int, default=0)
    parser.add_argument(
        "--probe",
        type=Path,
        default=ROOT / "zig/lib/sql_regex/zig-out/bin/sql-regex-parity-probe",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Complete oracle witness artifact, including reference identity",
    )
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve() == args.report.resolve() or args.probe.resolve() in (
        args.output.resolve(),
        args.report.resolve(),
    ):
        parser.error("witness, report and probe paths must be distinct")
    if not 0 <= args.fuzz <= 10_000:
        parser.error("--fuzz must be between 0 and 10000")
    corpus = (
        regressions()
        if args.suite == "regressions"
        else cases(args.max_subject, args.seed, args.fuzz)
    )
    if len(corpus) > 100_000:
        parser.error("campaign exceeds the 100000-case runner envelope")
    with postgres() as db:
        campaign = {"grammar": args.suite + "-v1", "suite": args.suite}
        if args.suite == "selection":
            campaign.update(
                pattern_count=len(patterns()),
                alphabet="ab",
                max_subject=args.max_subject,
                all_start_positions=True,
                seed=args.seed,
                fuzz_requested=args.fuzz,
            )
        else:
            campaign["case_count"] = len(corpus)
        artifact = {
            "format": 2,
            "profile": profile(db, args.oracle_major),
            "campaign": campaign,
            "entries": [reference(db, case) for case in corpus],
        }
        args.output.write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2) + "\n"
        )
        with TemporaryDirectory(prefix="antfly-regex-parity-") as temporary:
            result = run_probe(args.probe.resolve(), artifact, Path(temporary))
            # A green runner must also detect a deliberately wrong accepted
            # witness. This exercises comparison, reporting and exit handling.
            accepted = next(
                entry for entry in artifact["entries"] if entry.get("matched")
            )
            wrong = {**accepted, "spans": [dict(span) for span in accepted["spans"]]}
            wrong["spans"][0]["end"] += 1
            control = run_probe(
                args.probe.resolve(), {**artifact, "entries": [wrong]}, Path(temporary)
            )
            if control["mismatch_count"] != 1:
                raise RuntimeError(
                    "native runner failed the incorrect-witness positive control"
                )
            result["incorrect_witness_detected"] = True
        # Reports preserve exact inputs and independent expectations for each
        # reported mismatch; the complete artifact retains all other cases.
        by_id = {entry["id"]: entry for entry in artifact["entries"]}
        for failure in result["failures"]:
            failure["expected"] = by_id[failure["id"]]
        result["reference"] = artifact["profile"]
        result["campaign"] = artifact["campaign"]
        args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(
        f"PostgreSQL {args.oracle_major}: {result['checked']} contracts, {result['mismatch_count']} mismatches"
    )
    for failure in result["failures"][:5]:
        print(json.dumps(failure, ensure_ascii=False))
    if result["mismatch_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
        print(f"Regex qualification did not complete: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
