#!/usr/bin/env python3
"""Require an explicit token baseline and immutable actions in every workflow."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


SHA_REF = re.compile(r"^[0-9a-f]{40}$")
USES = re.compile(r"^\s*-?\s*uses:\s*([^\s#]+)")
WORKFLOW_PERMISSIONS = re.compile(r"^permissions:\s*(.*)$")
WRITE_VALUE = re.compile(r"\bwrite(?:-all)?\b")


def action_references(path: Path) -> list[tuple[int, str]]:
    references: list[tuple[int, str]] = []
    text = path.read_text(encoding="utf-8")
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = USES.match(line)
        if match:
            references.append((line_number, match.group(1)))
    return references


def mutable_action_references(path: Path) -> list[str]:
    findings: list[str] = []
    for line_number, reference in action_references(path):
        if reference.startswith("./"):
            continue
        _, separator, revision = reference.rpartition("@")
        if not separator or not SHA_REF.fullmatch(revision):
            findings.append(f"{path}:{line_number}: {reference}")
    return findings


def workflow_permission_findings(path: Path, text: str) -> list[str]:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        match = WORKFLOW_PERMISSIONS.fullmatch(line)
        if not match:
            continue

        values = [match.group(1).split("#", 1)[0].strip()]
        for nested in lines[index + 1 :]:
            if nested and not nested[0].isspace():
                break
            values.append(nested.split("#", 1)[0].strip())
        if not any(values):
            return [f"{path}:{index + 1}: top-level permissions declaration is empty"]
        if any(WRITE_VALUE.search(value) for value in values):
            return [f"{path}:{index + 1}: top-level permissions must be read-only"]
        return []
    return [f"{path}: missing top-level permissions declaration"]


def validate(workflow_dir: Path) -> list[Path]:
    workflows = sorted({*workflow_dir.glob("*.yml"), *workflow_dir.glob("*.yaml")})
    findings: list[str] = []
    for path in workflows:
        text = path.read_text(encoding="utf-8")
        findings.extend(workflow_permission_findings(path, text))
        findings.extend(mutable_action_references(path))
    if findings:
        detail = "\n".join(f"  {finding}" for finding in findings)
        raise SystemExit(
            "workflows must declare token permissions and pin external actions "
            "to full commit SHAs:\n"
            f"{detail}"
        )
    return workflows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workflow-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / ".github" / "workflows",
    )
    args = parser.parse_args()
    selected = validate(args.workflow_dir.resolve())
    print(f"validated permissions and immutable action references in {len(selected)} workflows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
