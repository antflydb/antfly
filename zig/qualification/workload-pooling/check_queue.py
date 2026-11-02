#!/usr/bin/env python3
"""Read-only audit of the active qualification dependency graph.

Terminal predecessors are leaves: their recorded failure can be an intentional
negative receipt. This audit proves queue structure/liveness, never test success.
"""
import argparse
import json
import os
from pathlib import Path


def audit(seeds, read_record, check_pid=None):
    records, visiting, visited = {}, [], set()
    errors, warnings = [], []

    def visit(path):
        if path in visiting:
            start = visiting.index(path)
            errors.append("dependency cycle: " + " -> ".join(visiting[start:] + [path]))
            return
        if path in visited:
            return
        try:
            record = read_record(path)
        except (OSError, ValueError, TypeError) as error:
            errors.append(f"cannot read {path}: {error}")
            return
        if not isinstance(record, dict):
            errors.append(f"invalid record: {path}")
            return
        records[path] = record
        state = record.get("state")
        if state == "terminal":
            if type(record.get("exit_code")) is not int:
                errors.append(f"terminal record lacks an exit code: {path}")
            visited.add(path)
            return
        if state not in ("queued", "running"):
            errors.append(f"unknown state {state!r}: {path}")
            return
        pid = record.get("pid")
        if type(pid) is not int or pid <= 0:
            errors.append(f"invalid dispatcher PID: {path}")
        elif check_pid is not None:
            try:
                check_pid(pid)
            except ProcessLookupError:
                errors.append(f"dispatcher {pid} missing while {state}: {path}; inspect child/container state before recovery")
            except PermissionError:
                warnings.append(f"cannot verify dispatcher {pid}: {path}")
        dependencies = record.get("dependencies", [])
        if not isinstance(dependencies, list) or any(not isinstance(dep, str) for dep in dependencies):
            errors.append(f"invalid dependencies: {path}")
            return
        visiting.append(path)
        for dep in dependencies:
            visit(str(Path(dep).resolve()))
        visiting.pop()
        visited.add(path)

    for seed in seeds:
        visit(str(Path(seed).resolve()))
    return {
        "ok": not errors,
        "scope": "Queue dependency structure and optional live dispatcher checks; not compilation/runtime success",
        "records": {path: {key: record.get(key) for key in ("state", "pid", "compiler_pid", "exit_code")} for path, record in records.items()},
        "errors": errors,
        "warnings": warnings,
    }


def self_test():
    # Reproduce the actual hosted gate mistake: a renamed worker's prerequisite
    # accidentally becomes its own output instead of the preceding SQL gate.
    a, b, c = (str(Path("/private/tmp") / name) for name in ("hosted.json", "sql.json", "linux.json"))
    data = {a: {"state": "queued", "pid": 1, "dependencies": [a]}}
    assert not audit([a], data.__getitem__)["ok"]
    data = {a: {"state": "queued", "pid": 1, "dependencies": [b]}, b: {"state": "queued", "pid": 2, "dependencies": [c]}, c: {"state": "queued", "pid": 3, "dependencies": [a]}}
    assert not audit([a], data.__getitem__)["ok"]
    # Failed, terminal predecessors are legitimate ordering barriers; they do
    # not prove a new candidate passed or prohibit its new compiler invocation.
    data[b] = {"state": "terminal", "exit_code": 1, "dependencies": [a]}
    assert audit([a], data.__getitem__)["ok"]
    def missing(_pid):
        raise ProcessLookupError()
    assert not audit([a], data.__getitem__, missing)["ok"]
    print("Four queue regression checks passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", action="append", default=[])
    parser.add_argument("--check-live", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
    if args.record:
        report = audit(args.record, lambda path: json.loads(Path(path).read_text()), (lambda pid: os.kill(pid, 0)) if args.check_live else None)
        print(json.dumps(report, indent=2))
        return 0 if report["ok"] else 1
    if not args.self_test:
        parser.error("supply --record or --self-test")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
