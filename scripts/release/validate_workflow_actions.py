#!/usr/bin/env python3
"""Require immutable action references throughout the release trust boundary."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


SHA_REF = re.compile(r"^[0-9a-f]{40}$")
USES = re.compile(r"^\s*-?\s*uses:\s*([^\s#]+)")
WRITE_PERMISSION = re.compile(
    r"(?:^|[{,])\s*(?:actions|attestations|contents|id-token|packages):\s*write\b"
)
WRITE_ALL = re.compile(r"^\s*permissions:\s*write-all\s*(?:#.*)?$")


def is_release_control_plane(path: Path, text: str) -> bool:
    name = path.name
    return (
        name.startswith("antfly-release")
        or name in {"antfly-container.yml", "antfly-nightly.yml", "cli-package.yml"}
        or any(WRITE_PERMISSION.search(line) for line in text.splitlines())
        or any(WRITE_ALL.match(line) for line in text.splitlines())
    )


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


def validate(workflow_dir: Path) -> list[Path]:
    workflows = sorted({*workflow_dir.glob("*.yml"), *workflow_dir.glob("*.yaml")})
    selected = {
        path
        for path in workflows
        if is_release_control_plane(path, path.read_text(encoding="utf-8"))
    }
    # Follow local reusable-workflow calls so every executable dependency of a
    # selected controller inherits the same immutable-reference policy.
    changed = True
    while changed:
        changed = False
        for path in tuple(selected):
            for _, reference in action_references(path):
                prefix = "./.github/workflows/"
                if not reference.startswith(prefix):
                    continue
                dependency = workflow_dir / reference.removeprefix(prefix)
                if dependency.is_file() and dependency not in selected:
                    selected.add(dependency)
                    changed = True

    findings: list[str] = []
    ordered = sorted(selected)
    for path in ordered:
        findings.extend(mutable_action_references(path))
    if findings:
        detail = "\n".join(f"  {finding}" for finding in findings)
        raise SystemExit(
            "release workflows must pin external actions to full commit SHAs:\n"
            f"{detail}"
        )
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workflow-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / ".github" / "workflows",
    )
    args = parser.parse_args()
    selected = validate(args.workflow_dir.resolve())
    print(f"validated immutable action references in {len(selected)} release workflows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
