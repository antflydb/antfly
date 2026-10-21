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

"""Seeded grammar-based CLI differential exploration with bounded ddmin reduction.

The oracle is test-only ripgrep 15.2.0 linked to PCRE2 10.45. Run both executables
on the same host/container. Reports contain replayable inputs and binary hashes.
"""

import argparse
import base64
import hashlib
import json
import os
import platform
import random
import subprocess
from pathlib import Path


def generated(seed, count):
    rng = random.Random(seed)
    atoms = ["a", "b", "é", "[ab]", "[^b]", r"\d", r"\w", r"\p{Greek}", "."]
    subjects = [
        "",
        "a",
        "ab",
        "aaa",
        "baab",
        "éÉ αβ_ １２3",
        "a\nb\naa\n",
        "abc abc42!",
    ]
    for i in range(count):
        a, b = rng.choice(atoms), rng.choice(atoms)
        forms = [
            a,
            f"({a})({b})",
            f"(?:{a}|{b})+",
            f"({a}+)\\1",
            f"(?={a}){b}",
            f"(?<={a}){b}",
            f"(?>{a}*){b}",
            f"{a}{{0,3}}?{b}",
            f"({a}|{b})*?",
            f"^{a}+$",
            f"(?i:{a}){b}",
            f"(?<n>{a})\\k<n>",
            f"{a}(*SKIP)(*F)|{b}",
            f"({a})(?1){b}",
            f"(?J)(?<n>{a})|(?<n>{b})\\k<n>",
        ]
        pattern = rng.choice(forms)
        if i % 13 == 0:
            pattern += rng.choice(
                ["(", "[", r"\p{UnknownProperty}", "{999999999999999999999}"]
            )
        text = rng.choice(subjects)
        if i % 3 == 0:
            text = "".join(rng.choice("abéα 12!\n") for _ in range(rng.randrange(48)))
        mode = rng.choice(
            [["-o"], ["-o", "-r", "$1|$2|${n}|$0"], ["-n"], ["-c"], ["--json"]]
        )
        flags = rng.choice(
            [[], ["-i"], ["-w"], ["-x"], ["-U"], ["-U", "--multiline-dotall"]]
        )
        yield {
            "args": ["-P", "--color=never", *mode, *flags, "-e", pattern],
            "input": base64.b64encode(text.encode()).decode(),
        }


def normalized_output(args, raw):
    if "--json" not in args:
        return raw
    events = []
    for line in raw.splitlines():
        item = json.loads(line)
        if item["type"] != "match":
            continue
        data = item["data"]
        events.append(
            {
                k: data[k]
                for k in ("lines", "line_number", "absolute_offset", "submatches")
            }
        )
    return json.dumps(events, sort_keys=True).encode()


def execute(binary, case, timeout, env):
    try:
        p = subprocess.run(
            [str(binary), *case["args"]],
            input=base64.b64decode(case["input"]),
            capture_output=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {"code": "timeout", "stdout": ""}
    code = p.returncode
    # Diagnostic wording varies; syntax acceptance and successful output must agree.
    try:
        output = normalized_output(case["args"], p.stdout) if code in (0, 1) else b""
    except (ValueError, KeyError, TypeError):
        return {"code": "invalid-json", "stdout": base64.b64encode(p.stdout).decode()}
    return {"code": code, "stdout": base64.b64encode(output).decode()}


def ddmin(value, predicate, budget):
    """Delete chunks until 1-minimal or the shared evaluation budget is exhausted."""
    width = 2
    while value and budget[0] > 0:
        size = max(1, (len(value) + width - 1) // width)
        changed = False
        for at in range(0, len(value), size):
            candidate = value[:at] + value[at + size :]
            budget[0] -= 1
            if predicate(candidate):
                value, width, changed = candidate, max(2, width - 1), True
                break
            if budget[0] <= 0:
                break
        if not changed:
            if size == 1:
                break
            width = min(len(value), width * 2)
    return value


def reduce_case(case, mismatch, budget=100):
    case = {"args": list(case["args"]), "input": case["input"]}
    remaining = [budget]
    if len(case["args"]) >= 2 and case["args"][-2] in ("-e", "--regexp"):
        units = []
        flags = case["args"][:-2]
        i = 0
        while i < len(flags):
            width = 2 if flags[i] in ("-r", "--replace", "-A", "-B", "-C", "-m") else 1
            units.append(flags[i : i + width])
            i += width

        def flags_ok(value):
            args = [flag for unit in value for flag in unit] + case["args"][-2:]
            return mismatch({**case, "args": args})

        units = ddmin(units, flags_ok, remaining)
        case["args"] = [flag for unit in units for flag in unit] + case["args"][-2:]

    def pattern_ok(value):
        candidate = {**case, "args": [*case["args"][:-1], value]}
        return mismatch(candidate)

    case["args"][-1] = ddmin(case["args"][-1], pattern_ok, remaining)
    raw = base64.b64decode(case["input"])
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw

    def encode(value):
        return value.encode("utf-8") if isinstance(value, str) else value

    def input_ok(value):
        return mismatch({**case, "input": base64.b64encode(encode(value)).decode()})

    text = ddmin(text, input_ok, remaining)
    case["input"] = base64.b64encode(encode(text)).decode()
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("oracle", type=Path)
    parser.add_argument("--seed", type=int, default=1151)
    parser.add_argument("--cases", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=2)
    parser.add_argument("--reduce-budget", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--replay", type=Path)
    parser.add_argument("--goldens", type=Path)
    parser.add_argument(
        "--frozen-oracle",
        action="store_true",
        help="Replay pinned outputs without spawning the oracle per case; requires --replay",
    )
    args = parser.parse_args()
    if args.frozen_oracle and not args.replay:
        parser.error("--frozen-oracle requires --replay")
    if args.cases < 1 or args.timeout <= 0 or args.reduce_budget < 0:
        parser.error("cases/timeout must be positive and reduction budget nonnegative")
    binary, oracle = args.binary.resolve(), args.oracle.resolve()
    version = subprocess.check_output([str(oracle), "--version"], text=True)
    pcre = subprocess.check_output([str(oracle), "--pcre2-version"], text=True)
    if "ripgrep 15.2.0" not in version or not pcre.startswith("PCRE2 10.45"):
        parser.error("oracle must be ripgrep 15.2.0 / runtime PCRE2 10.45")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
    }
    env.update(NO_COLOR="1", TERM="dumb")
    report = {
        "platform": platform.system().lower() + "-" + platform.machine(),
        "complete": False,
        "seed": args.seed,
        "oracle": version,
        "pcre2_runtime": pcre,
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "oracle_sha256": hashlib.sha256(oracle.read_bytes()).hexdigest(),
        "cases": 0,
        "failures": [],
        "timeouts": [],
    }
    replay = json.loads(args.replay.read_text()) if args.replay else None
    if args.frozen_oracle and replay.get("oracle_sha256") != report["oracle_sha256"]:
        parser.error("frozen fixture oracle identity differs")
    report["oracle_mode"] = (
        "pinned-fixture-replay" if args.frozen_oracle else "live-pinned-oracle"
    )
    if (
        args.frozen_oracle
        and replay.get("platform", report["platform"]) != report["platform"]
    ):
        parser.error("full frozen replay must run on its oracle platform")
    rows = replay["cases"] if replay else generated(args.seed, args.cases)
    goldens = []
    for case in rows:
        report["cases"] += 1
        if report["cases"] % 25 == 0:
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(
                "explored",
                report["cases"],
                "failures",
                len(report["failures"]),
                flush=True,
            )
        actual = execute(binary, case, args.timeout, env)
        expected = (
            {"code": case["code"], "stdout": case["stdout"]}
            if args.frozen_oracle
            else execute(oracle, case, args.timeout, env)
        )
        goldens.append({**case, **expected})
        if "timeout" in (actual["code"], expected["code"]):
            report["timeouts"].append(case)
            continue
        if actual == expected:
            continue
        category = (actual["code"], expected["code"])

        def mismatch(candidate):
            x, y = (
                execute(binary, candidate, args.timeout, env),
                execute(oracle, candidate, args.timeout, env),
            )
            return (x["code"], y["code"]) == category and x != y

        reduced = (
            case
            if args.frozen_oracle
            else reduce_case(case, mismatch, args.reduce_budget)
        )
        report["failures"].append(
            {
                "original": case,
                "reduced": reduced,
                "actual": execute(binary, reduced, args.timeout, env),
                "expected": expected
                if args.frozen_oracle
                else execute(oracle, reduced, args.timeout, env),
            }
        )
    report["complete"] = True
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    if args.goldens:
        args.goldens.write_text(
            json.dumps(
                {
                    "platform": report["platform"],
                    "seed": args.seed,
                    "oracle": version,
                    "oracle_sha256": report["oracle_sha256"],
                    "cases": goldens,
                },
                indent=2,
            )
            + "\n"
        )
    print(
        json.dumps(
            {
                k: len(v) if isinstance(v, list) else v
                for k, v in report.items()
                if k in ("seed", "cases", "failures", "timeouts")
            }
        )
    )
    return int(bool(report["failures"] or report["timeouts"]))


if __name__ == "__main__":
    raise SystemExit(main())
