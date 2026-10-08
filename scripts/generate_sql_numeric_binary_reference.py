#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify NUMERIC send/receive against PostgreSQL's real binary parameter path."""

import argparse
import hashlib
import json
import random
import struct
from pathlib import Path

from generate_sql_postgres_reference import postgres

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_exact_numeric_binary_reference.json"
)


def payload(groups=(), *, weight=0, sign=0, scale=0):
    return struct.pack("!HhHH", len(groups), weight, sign, scale) + b"".join(
        struct.pack("!H", group) for group in groups
    )


def senders():
    arithmetic = json.loads(
        (FIXTURE.parent / "sql_exact_numeric_reference.json").read_text()
    )
    inputs = [
        entry["left"]
        for entry in arithmetic["entries"]
        if entry["op"] == "parse" and "expected" in entry
    ]
    inputs.extend(
        ("1e131071", "1e-16383", "-1e-16383", "1.00000000000000000000", "9999.9999")
    )
    rng = random.Random(20261010)
    for _ in range(32):
        inputs.append(f"{rng.randint(-(10**200), 10**200)}e-{rng.randrange(401)}")
    return list(dict.fromkeys(inputs))


def receivers():
    cases = []

    def add(name, data, modifier=None):
        case = {"name": name, "binary": data.hex()}
        if modifier is not None:
            case["modifier"] = {"precision": modifier[0], "scale": modifier[1]}
        cases.append(case)

    add("zero", payload())
    add("negative-zero", payload(sign=0x4000, weight=32767, scale=3))
    add("leading-and-trailing-zeroes", payload((0, 1234, 0, 0), weight=1, scale=8))
    add("hidden-fraction", payload((1234, 5678), scale=2))
    add("negative-hidden-fraction", payload((1234,), weight=-1, sign=0x4000, scale=2))
    add("negative-truncated-zero", payload((1,), weight=-1, sign=0x4000, scale=2))
    add("minimum-weight", payload((1,), weight=-32768, scale=16383))
    add("maximum-weight", payload((1,), weight=32767))
    add("maximum-scale", payload((1000,), weight=-4096, scale=16383))
    for name, sign in (
        ("nan", 0xC000),
        ("infinity", 0xD000),
        ("negative-infinity", 0xF000),
    ):
        add(name, payload(sign=sign))
        add(
            name + "-ignored-payload",
            payload((12, 34), sign=sign, weight=32767, scale=16383),
        )
        add(name + "-invalid-payload", payload((10000,), sign=sign))
    for size in range(8):
        add(f"short-header-{size}", payload()[:size])
    add("short-body", struct.pack("!HhHHH", 2, 0, 0, 0, 1))
    add("trailing-body", payload() + b"\x00\x01")
    for sign in (1, 0x2000, 0x8000, 0xE000, 0xFFFF):
        add(f"invalid-sign-{sign}", payload(sign=sign))
    for scale in (0x4000, 0x8000, 0xFFFF):
        add(f"invalid-scale-{scale}", payload(scale=scale))
    for digit in (10000, 32767, 32768, 65535):
        add(f"invalid-digit-{digit}", payload((digit,)))
    add("typmod-after-hidden-truncation", payload((1234, 5678), scale=2), (8, 3))
    add("typmod-after-rounding", payload((1234, 5678), scale=3), (6, 2))
    add("typmod-carry-overflow", payload((99, 9950), scale=3), (4, 2))
    add("typmod-negative-scale", payload((9, 9499)), (2, -3))
    add("typmod-fractional-only", payload((99, 5000), weight=-1, scale=5), (2, 4))
    add("typmod-nan", payload(sign=0xC000), (1, 0))
    add("typmod-infinity", payload(sign=0xD000), (1000, 0))
    return cases


def observe(db, sql, args):
    with db.transaction(force_rollback=True):
        db.execute("SET TRANSACTION READ ONLY")
        text, binary = db.execute(sql, args).fetchone()
        return {
            "binary": binary,
            "text": text if len(text) <= 256 else None,
            "text_length": len(text),
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
        }


def main():
    import psycopg
    from psycopg.adapt import Dumper
    from psycopg.pq import Format

    class BinaryNumeric:
        def __init__(self, data):
            self.data = data

    class NumericDumper(Dumper):
        oid = 1700
        format = Format.BINARY

        def dump(self, obj):
            return obj.data

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    output = {
        "reference": "PostgreSQL NUMERIC binary boundary",
        "senders": [],
        "receivers": [],
    }
    with postgres() as db:
        db.adapters.register_dumper(BinaryNumeric, NumericDumper)
        for text in senders():
            with db.transaction(force_rollback=True):
                binary = db.execute(
                    "SELECT encode(numeric_send(%s::numeric),'hex')", (text,)
                ).fetchone()[0]
            output["senders"].append({"input": text, "binary": binary})
        for case in receivers():
            entry = dict(case)
            expression = "%s::numeric"
            if modifier := case.get("modifier"):
                expression += f"::numeric({modifier['precision']},{modifier['scale']})"
            sql = f"SELECT n::text,encode(numeric_send(n),'hex') FROM (SELECT {expression} AS n) q"
            try:
                entry["expected"] = observe(
                    db, sql, (BinaryNumeric(bytes.fromhex(case["binary"])),)
                )
            except psycopg.Error as error:
                entry["error"] = error.sqlstate
            output["receivers"].append(entry)
    if args.generate:
        print(json.dumps(output, indent=2))
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL NUMERIC binary oracle drift")
        print(
            f"Verified {len(output['senders'])} senders and {len(output['receivers'])} real PostgreSQL binary receivers"
        )


if __name__ == "__main__":
    main()
