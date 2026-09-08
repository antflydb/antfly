#!/usr/bin/env python3
"""Resolve version tags to controller-owned release source branches."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_channels import NIGHTLY_PATTERN, is_canonical_tag, parse_version

POLICY_PATH = Path(__file__).with_name("release-lines.json")
LINE_PATTERN = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
MAINTENANCE_REF_PATTERN = re.compile(
    r"^refs/heads/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.x$"
)
DEFAULT_BRANCH_REF = "refs/heads/main"
LINE_STATUSES = {"active", "closed"}


@dataclass(frozen=True)
class ReleaseLine:
    name: str
    source_ref: str
    trusted_source_refs: tuple[str, ...]
    status: str

    def outputs(self) -> dict[str, str]:
        return {
            "release_line": self.name,
            "source_ref": self.source_ref,
            "line_status": self.status,
        }


def load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"invalid release-line policy: {path}") from exc
    validate_policy(policy)
    return policy


def validate_policy(policy: dict[str, Any]) -> None:
    if not isinstance(policy, dict) or set(policy) != {
        "schema_version",
        "lines",
        "nightly_source_ref",
    }:
        raise SystemExit("release-line policy has an invalid top-level contract")
    if policy["schema_version"] != 2:
        raise SystemExit("unsupported release-line policy schema")
    if policy["nightly_source_ref"] != DEFAULT_BRANCH_REF:
        raise SystemExit("nightly releases must use refs/heads/main")
    lines = policy["lines"]
    if not isinstance(lines, dict) or not lines:
        raise SystemExit("release-line policy defines no release lines")
    for name, document in lines.items():
        if not isinstance(name, str) or not LINE_PATTERN.fullmatch(name):
            raise SystemExit(f"invalid release line: {name!r}")
        if not isinstance(document, dict) or set(document) != {
            "source_ref",
            "trusted_source_refs",
            "status",
        }:
            raise SystemExit(f"release line {name} has an invalid contract")
        source_ref = document["source_ref"]
        trusted_source_refs = document["trusted_source_refs"]
        status = document["status"]
        if status not in LINE_STATUSES:
            raise SystemExit(f"release line {name} has an invalid status")
        if (
            not isinstance(trusted_source_refs, list)
            or not trusted_source_refs
            or any(not isinstance(ref, str) for ref in trusted_source_refs)
            or len(set(trusted_source_refs)) != len(trusted_source_refs)
            or source_ref not in trusted_source_refs
        ):
            raise SystemExit(
                f"release line {name} must define unique trusted source refs "
                "including its current source ref"
            )
        for trusted_source_ref in trusted_source_refs:
            maintenance = MAINTENANCE_REF_PATTERN.fullmatch(trusted_source_ref)
            if trusted_source_ref != DEFAULT_BRANCH_REF and (
                maintenance is None
                or f"{maintenance.group(1)}.{maintenance.group(2)}" != name
            ):
                raise SystemExit(
                    f"release line {name} must use refs/heads/main or "
                    f"refs/heads/v{name}.x"
                )


def line_name_for_tag(tag: str) -> str:
    if not is_canonical_tag(tag) or NIGHTLY_PATTERN.fullmatch(tag):
        raise SystemExit(f"release line requires a canonical non-nightly tag: {tag}")
    core, _ = parse_version(tag)
    return f"{core[0]}.{core[1]}"


def resolve_tag(
    tag: str,
    policy: dict[str, Any] | None = None,
    *,
    allow_closed: bool = False,
) -> ReleaseLine:
    policy = policy or load_policy()
    name = line_name_for_tag(tag)
    document = policy["lines"].get(name)
    if not isinstance(document, dict):
        raise SystemExit(f"no trusted release line is configured for {tag}")
    status = str(document["status"])
    if status != "active" and not allow_closed:
        raise SystemExit(f"release line {name} is closed")
    return ReleaseLine(
        name,
        str(document["source_ref"]),
        tuple(str(ref) for ref in document["trusted_source_refs"]),
        status,
    )


def validate_provenance(
    tag: str,
    release_line: str,
    source_ref: str,
    policy: dict[str, Any] | None = None,
    *,
    allow_closed: bool = False,
) -> ReleaseLine:
    """Validate recorded provenance without changing the source for new cuts."""
    expected = resolve_tag(tag, policy, allow_closed=allow_closed)
    if release_line != expected.name or source_ref not in expected.trusted_source_refs:
        raise SystemExit("invalid release-line provenance")
    return expected


def nightly_line(policy: dict[str, Any] | None = None) -> ReleaseLine:
    policy = policy or load_policy()
    source_ref = str(policy["nightly_source_ref"])
    return ReleaseLine("nightly", source_ref, (source_ref,), "active")


def write_outputs(outputs: dict[str, str], output_path: Path | None) -> None:
    if output_path is None and os.environ.get("GITHUB_OUTPUT"):
        output_path = Path(os.environ["GITHUB_OUTPUT"])
    if output_path:
        with output_path.open("a", encoding="utf-8") as output:
            for key, value in outputs.items():
                output.write(f"{key}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("validate", "resolve", "verify-provenance", "nightly")
    )
    parser.add_argument("--tag")
    parser.add_argument("--release-line")
    parser.add_argument("--source-ref")
    parser.add_argument("--allow-closed", action="store_true")
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    policy = load_policy()
    if args.command == "validate":
        print(f"validated {len(policy['lines'])} release lines")
        return 0
    if args.command in {"resolve", "verify-provenance"}:
        if not args.tag:
            parser.error(f"{args.command} requires --tag")
        if args.command == "verify-provenance":
            if not args.release_line or not args.source_ref:
                parser.error(
                    "verify-provenance requires --release-line and --source-ref"
                )
            line = validate_provenance(
                args.tag,
                args.release_line,
                args.source_ref,
                policy,
                allow_closed=args.allow_closed,
            )
        else:
            line = resolve_tag(args.tag, policy, allow_closed=args.allow_closed)
    else:
        line = nightly_line(policy)
    outputs = line.outputs()
    write_outputs(outputs, args.github_output)
    print(json.dumps(outputs, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
