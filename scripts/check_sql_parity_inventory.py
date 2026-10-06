#!/usr/bin/env python3
"""Audit original SQL extraction cases; inventory integrity is distinct from release readiness."""

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/local/sql/fixtures"
INVENTORY_HASH = "c203a4dcf0094b75e1d3764e90beebddaf844a9ce604543c0a4dbc0ffe8ae173"
SOURCE_HASH = "52b61411fa93be84b523c109eb6f79ea9e2f8a83d4e3639a831f4b8a697892c6"
SOURCE_COMMIT = "79644dfa1605e8da0f486d021d1c1393577d6265"
STATUSES = {"unresolved", "implemented", "rejected", "superseded", "deferred"}
RESOLVED = {"implemented", "rejected", "superseded"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def repository_path(root, name):
    require(isinstance(name, str) and bool(name), "missing repository path")
    path = (root / name).resolve()
    require(path.is_relative_to(root.resolve()), f"path escapes repository: {name}")
    return path


def zig_test_section(source, anchor):
    declaration = re.search(
        r'^\s*test\s+"' + re.escape(anchor) + r'"\s*\{',
        source,
        flags=re.MULTILINE,
    )
    require(declaration is not None, f"evidence test anchor missing: {anchor}")
    next_test = re.search(
        r'^\s*test\s+"', source[declaration.end() :], flags=re.MULTILINE
    )
    end = declaration.end() + next_test.start() if next_test else len(source)
    return source[declaration.start() : end]


def validate(inventory_bytes, ledger, root=ROOT, source_bytes=None):
    require(
        hashlib.sha256(inventory_bytes).hexdigest() == INVENTORY_HASH,
        "inventory checksum changed; review provenance before updating the pin",
    )
    inventory = json.loads(inventory_bytes)
    require(inventory["inventory_format"] == 1, "unsupported inventory format")
    require(
        inventory["source_commit"] == SOURCE_COMMIT
        and inventory["source_sha256"] == SOURCE_HASH,
        "source provenance changed",
    )
    cases = inventory["entries"]
    ids = [case["id"] for case in cases]
    require(inventory["entry_count"] == len(cases) == 1586, "original cases missing")
    require(len(set(ids)) == len(ids), "duplicate inventory ID")
    if source_bytes is not None:
        require(
            hashlib.sha256(source_bytes).hexdigest() == SOURCE_HASH,
            "original source checksum mismatch",
        )
        original = json.loads(source_bytes)["entries"]
        require(len(original) == len(cases), "source entry count mismatch")
        for case, entry in zip(cases, original, strict=True):
            canonical = json.dumps(
                entry, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
            require(
                hashlib.sha256(canonical).hexdigest() == case["source_entry_sha256"],
                f"{case['id']}: source entry mismatch",
            )
            require(
                all(case[key] == entry[key] for key in ("name", "family", "sql")),
                f"{case['id']}: source projection mismatch",
            )
            require(
                case["params"] == entry.get("params", []),
                f"{case['id']}: source parameters mismatch",
            )
    require(ledger.get("ledger_format") == 1, "unsupported ledger format")
    require(
        ledger.get("inventory_sha256") == INVENTORY_HASH,
        "ledger references another inventory",
    )
    entries = ledger["entries"]
    disposition_ids = [entry["id"] for entry in entries]
    require(
        len(disposition_ids) == len(set(disposition_ids)), "duplicate disposition ID"
    )
    require(
        set(disposition_ids) == set(ids),
        "dispositions must cover every original ID exactly once",
    )
    original_by_id = {case["id"]: case for case in cases}
    gates = ledger["gates"]
    require(isinstance(gates, dict), "gates must be an object")
    used_gates = set()
    for entry in entries:
        case_id = entry["id"]
        status = entry["status"]
        require(status in STATUSES, f"{case_id}: unknown disposition {status}")
        require(
            isinstance(entry.get("reason"), str) and bool(entry["reason"].strip()),
            f"{case_id}: missing rationale",
        )
        evidence = entry.get("evidence", [])
        if status in RESOLVED:
            require(
                bool(evidence),
                f"{case_id}: completed disposition requires executable evidence",
            )
        if status == "rejected":
            require(
                original_by_id[case_id]["source_expectation"] == "rejection",
                f"{case_id}: a non-rejection source contract cannot be rejected; use a tested supersession",
            )
        if status == "implemented":
            require(
                original_by_id[case_id]["source_expectation"] != "rejection",
                f"{case_id}: an original rejection cannot be implemented without a tested supersession",
            )
        anchored = False
        for proof in evidence:
            path = repository_path(root, proof["path"])
            require(path.is_file(), f"{case_id}: evidence file missing")
            evidence_text = path.read_text()
            anchor = proof.get("test", "")
            require(
                isinstance(anchor, str) and bool(anchor.strip()),
                f"{case_id}: evidence test anchor missing",
            )
            section = zig_test_section(evidence_text, anchor)
            anchored = anchored or case_id in section
            gate_id = proof["gate"]
            require(gate_id in gates, f"{case_id}: evidence gate missing")
            used_gates.add(gate_id)
        if evidence:
            require(
                anchored,
                f"{case_id}: at least one cited evidence test must identify the original case",
            )
    for gate_id in used_gates:
        gate = gates[gate_id]
        command = gate["command"]
        require(
            isinstance(command, list)
            and bool(command)
            and all(isinstance(arg, str) and arg for arg in command),
            f"{gate_id}: gate needs an argument-vector command",
        )
        require(
            repository_path(root, gate["cwd"]).is_dir(),
            f"{gate_id}: gate directory missing",
        )
        timeout = gate.get("timeout_seconds")
        require(
            isinstance(timeout, int)
            and not isinstance(timeout, bool)
            and 0 < timeout <= 3600,
            f"{gate_id}: gate needs a bounded timeout",
        )
    return inventory, entries, sorted(used_gates)


def release_blockers(entries):
    return [entry for entry in entries if entry["status"] not in RESOLVED]


def family_report(inventory, entries):
    """Describe recorded evidence, never infer execution support from SQL text."""
    dispositions = {entry["id"]: entry for entry in entries}
    families = {}
    for case in inventory["entries"]:
        row = families.setdefault(
            case["family"],
            {
                "total": 0,
                "statuses": Counter(),
                "partial_evidence": 0,
                "no_evidence": 0,
                "original_rejections": 0,
            },
        )
        entry = dispositions[case["id"]]
        row["total"] += 1
        row["statuses"][entry["status"]] += 1
        if entry["status"] not in RESOLVED:
            row["partial_evidence" if entry.get("evidence") else "no_evidence"] += 1
            row["original_rejections"] += case["source_expectation"] == "rejection"
    return dict(sorted(families.items()))


def select_evidence_gates(inventory, entries, used_gates, family=None, requested=None):
    """Narrow evidence execution without narrowing inventory or release validation."""
    selected = entries
    if family is not None:
        ids = {case["id"] for case in inventory["entries"] if case["family"] == family}
        require(bool(ids), f"unknown family: {family}")
        selected = [entry for entry in entries if entry["id"] in ids]
    available = {
        proof["gate"] for entry in selected for proof in entry.get("evidence", [])
    }
    if requested:
        for gate in requested:
            require(
                gate in used_gates, f"unknown or unreferenced evidence gate: {gate}"
            )
            require(
                gate in available, f"gate {gate} has no evidence in selected family"
            )
        available.intersection_update(requested)
    require(
        bool(available),
        "selection has no recorded executable evidence; no tests were run",
    )
    return sorted(available)


def evidence_runs(gate_ids, gates):
    """Share one Zig test binary across gates that differ only by test filters."""
    runs = []
    grouped = {}
    for gate_id in gate_ids:
        gate = gates[gate_id]
        command = gate["command"]
        separator = command.index("--") if "--" in command else -1
        filters = command[separator + 1 :] if separator >= 0 else []
        compile_filters = [arg for arg in command if arg.startswith("-Dtest-filter=")]
        if command[:2] == ["zig", "build"] and separator < 0 and compile_filters:
            prefix = [arg for arg in command if not arg.startswith("-Dtest-filter=")]
            key = (gate["cwd"], tuple(prefix))
            if key in grouped:
                run = grouped[key]
                run["gate_ids"].append(gate_id)
                existing = set(run["command"])
                run["command"].extend(
                    arg for arg in dict.fromkeys(compile_filters) if arg not in existing
                )
                run["timeout_seconds"] = min(
                    3600, run["timeout_seconds"] + gate["timeout_seconds"]
                )
            else:
                run = {
                    "gate_ids": [gate_id],
                    "command": prefix + list(dict.fromkeys(compile_filters)),
                    "cwd": gate["cwd"],
                    "timeout_seconds": gate["timeout_seconds"],
                }
                grouped[key] = run
                runs.append(run)
            continue
        mergeable = (
            command[:2] == ["zig", "build"]
            and separator >= 0
            and len(filters) >= 2
            and len(filters) % 2 == 0
            and all(flag == "--test-filter" for flag in filters[::2])
        )
        key = (gate["cwd"], tuple(command[:separator])) if mergeable else None
        if key is not None and key in grouped:
            run = grouped[key]
            run["gate_ids"].append(gate_id)
            existing = set(run["command"][run["command"].index("--") + 2 :: 2])
            for value in filters[1::2]:
                if value not in existing:
                    run["command"].extend(("--test-filter", value))
                    existing.add(value)
            run["timeout_seconds"] = min(
                3600, run["timeout_seconds"] + gate["timeout_seconds"]
            )
            continue
        run = {
            "gate_ids": [gate_id],
            "command": list(command),
            "cwd": gate["cwd"],
            "timeout_seconds": gate["timeout_seconds"],
        }
        runs.append(run)
        if key is not None:
            grouped[key] = run
    return runs


def run_evidence(gate_ids, gates, root=ROOT):
    for run in evidence_runs(gate_ids, gates):
        print(f"Running evidence gates: {', '.join(run['gate_ids'])}", flush=True)
        subprocess.run(
            run["command"],
            cwd=repository_path(root, run["cwd"]),
            timeout=run["timeout_seconds"],
            check=True,
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--release",
        action="store_true",
        help="fail on unresolved/deferred cases, then run all referenced evidence gates",
    )
    mode.add_argument(
        "--evidence",
        action="store_true",
        help="run referenced evidence gates without claiming release readiness",
    )
    parser.add_argument(
        "--source",
        type=Path,
        help="optionally verify against the original source corpus",
    )
    parser.add_argument(
        "--family", help="print stable IDs and SQL for one original family"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="summarize family dispositions and evidence gaps, not inferred feature support",
    )
    parser.add_argument(
        "--gate",
        action="append",
        help="with --evidence, run only this referenced gate (repeatable)",
    )
    args = parser.parse_args(argv)
    if args.gate and not args.evidence:
        parser.error(
            "--gate requires --evidence; release validation cannot be narrowed"
        )
    try:
        ledger = json.loads((FIXTURES / "sql_parity_dispositions.json").read_bytes())
        inventory, entries, gates = validate(
            (FIXTURES / "sql_parity_inventory.json").read_bytes(),
            ledger,
            source_bytes=args.source.read_bytes() if args.source else None,
        )
        print(
            f"Inventory integrity: {len(entries)} original cases; source {SOURCE_COMMIT[:12]}"
        )
        print(
            "Dispositions: "
            + ", ".join(
                f"{status}={count}"
                for status, count in sorted(
                    Counter(entry["status"] for entry in entries).items()
                )
            )
        )
        if args.family:
            matches = [
                case for case in inventory["entries"] if case["family"] == args.family
            ]
            require(bool(matches), f"unknown family: {args.family}")
            if not args.report and not args.evidence:
                for case in matches:
                    print(f"{case['id']} | {case['name']} | {case['sql']}")
        if args.report:
            print(
                "Family | Total | Implemented | Rejected | Superseded | Blocking | "
                "Partial evidence | No evidence | Blocking original rejections"
            )
            for family, row in family_report(inventory, entries).items():
                if args.family and family != args.family:
                    continue
                statuses = row["statuses"]
                blocking = row["partial_evidence"] + row["no_evidence"]
                print(
                    f"{family} | {row['total']} | {statuses['implemented']} | "
                    f"{statuses['rejected']} | {statuses['superseded']} | {blocking} | "
                    f"{row['partial_evidence']} | {row['no_evidence']} | "
                    f"{row['original_rejections']}"
                )
            print(
                "Blocking records are unadjudicated contracts, not counts of "
                "missing features. Partial evidence does not close a case."
            )
        blockers = release_blockers(entries)
        if args.evidence:
            selected = select_evidence_gates(
                inventory, entries, gates, args.family, args.gate
            )
            run_evidence(selected, ledger["gates"])
            print(
                f"Selected referenced evidence passed ({len(selected)} gates); {len(blockers)} unresolved/deferred cases still block release."
            )
            return 0
        if args.release:
            if blockers:
                print(
                    f"SQL extraction parity release BLOCKED: {len(blockers)} unresolved/deferred cases. Inventory validation is not execution parity.",
                    file=sys.stderr,
                )
                return 1
            run_evidence(gates, ledger["gates"])
            print(
                "SQL extraction original-corpus parity gates passed; broader distributed fault and workload gates remain separate."
            )
        else:
            print(
                f"Release readiness: {len(blockers)} blocking dispositions (not evaluated as a release gate)."
            )
        return 0
    except (
        ValueError,
        KeyError,
        TypeError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        print(f"SQL parity audit failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
