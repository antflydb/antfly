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
FIXTURES = ROOT / "zig/pkg/antfly/src/sql/fixtures"
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
        if status not in RESOLVED:
            continue
        evidence = entry.get("evidence", [])
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


def run_evidence(gate_ids, gates, root=ROOT):
    for gate_id in gate_ids:
        gate = gates[gate_id]
        print(f"Running evidence gate: {gate_id}", flush=True)
        subprocess.run(
            gate["command"],
            cwd=repository_path(root, gate["cwd"]),
            timeout=gate["timeout_seconds"],
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
        help="run evidence gates for resolved cases without claiming release readiness",
    )
    parser.add_argument(
        "--source",
        type=Path,
        help="optionally verify against the original source corpus",
    )
    parser.add_argument(
        "--family", help="print stable IDs and SQL for one original family"
    )
    args = parser.parse_args(argv)
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
            for case in matches:
                print(f"{case['id']} | {case['name']} | {case['sql']}")
        blockers = release_blockers(entries)
        if args.evidence:
            run_evidence(gates, ledger["gates"])
            print(
                f"Resolved-case evidence passed; {len(blockers)} unresolved/deferred cases still block release."
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
