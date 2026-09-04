#!/usr/bin/env python3
"""Check or apply Antfly's versioned GitHub repository rulesets."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from github_environment import GitHubAPI

CONTRACT_PATH = Path(__file__).with_name("github-rulesets.json")
RULESET_FIELDS = {
    "target",
    "enforcement",
    "bypass_actors",
    "conditions",
    "rules",
}


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot load GitHub ruleset contract: {path}") from exc
    rulesets = document.get("rulesets") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or set(document) != {"schema_version", "rulesets"}
        or document.get("schema_version") != 1
        or not isinstance(rulesets, dict)
        or not rulesets
    ):
        raise SystemExit("unsupported GitHub ruleset contract")
    for name, ruleset in rulesets.items():
        if (
            not isinstance(name, str)
            or not name.startswith("antfly-release-policy-")
            or not isinstance(ruleset, dict)
            or set(ruleset) != RULESET_FIELDS
            or ruleset.get("target") not in {"branch", "tag"}
            or ruleset.get("enforcement") != "active"
            or ruleset.get("bypass_actors") != []
            or not isinstance(ruleset.get("conditions"), dict)
            or not isinstance(ruleset.get("rules"), list)
            or not ruleset["rules"]
        ):
            raise SystemExit(f"invalid GitHub ruleset contract for {name}")
    return document


def ruleset_payload(name: str, desired: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, **desired}


def managed_rulesets(api: GitHubAPI, repository: str) -> dict[str, dict[str, Any]]:
    summaries = api.request(
        "GET", f"repos/{repository}/rulesets?includes_parents=false&per_page=100"
    )
    if not isinstance(summaries, list):
        raise SystemExit("GitHub returned malformed repository rulesets")
    result: dict[str, dict[str, Any]] = {}
    for summary in summaries:
        name = summary.get("name") if isinstance(summary, dict) else None
        ruleset_id = summary.get("id") if isinstance(summary, dict) else None
        if not isinstance(name, str) or not isinstance(ruleset_id, int):
            raise SystemExit("GitHub returned a malformed repository ruleset")
        if not name.startswith("antfly-release-policy-"):
            continue
        if name in result:
            raise SystemExit(f"GitHub returned duplicate managed ruleset {name}")
        current = api.request("GET", f"repos/{repository}/rulesets/{ruleset_id}")
        if not isinstance(current, dict):
            raise SystemExit(f"GitHub returned malformed ruleset {name}")
        result[name] = current
    return result


def normalized_ruleset(document: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        field: json.loads(json.dumps(document.get(field)))
        for field in sorted(RULESET_FIELDS)
    }
    # GitHub omits an empty bypass list for tokens without repository-rules
    # write access. Empty and omitted are equivalent for this no-bypass policy.
    normalized["bypass_actors"] = sorted(
        normalized["bypass_actors"] or [],
        key=lambda actor: (
            actor.get("actor_type", ""),
            actor.get("actor_id") or -1,
            actor.get("bypass_mode", ""),
        ),
    )
    ref_name = normalized["conditions"]["ref_name"]
    ref_name["include"] = sorted(ref_name["include"])
    ref_name["exclude"] = sorted(ref_name["exclude"])
    for rule in normalized["rules"]:
        parameters = rule.get("parameters", {})
        if "allowed_merge_methods" in parameters:
            parameters["allowed_merge_methods"] = sorted(
                parameters["allowed_merge_methods"]
            )
        if "required_status_checks" in parameters:
            parameters["required_status_checks"] = sorted(
                parameters["required_status_checks"],
                key=lambda check: (check["context"], check.get("integration_id", -1)),
            )
    normalized["rules"] = sorted(normalized["rules"], key=lambda rule: rule["type"])
    return normalized


def ruleset_findings(
    desired: dict[str, dict[str, Any]], current: dict[str, dict[str, Any]]
) -> list[str]:
    findings: list[str] = []
    for name in sorted(desired.keys() - current.keys()):
        findings.append(f"missing ruleset: {name}")
    for name in sorted(current.keys() - desired.keys()):
        findings.append(f"unexpected managed ruleset: {name}")
    for name in sorted(desired.keys() & current.keys()):
        if normalized_ruleset(current[name]) != normalized_ruleset(desired[name]):
            findings.append(f"ruleset differs from contract: {name}")
    return findings


def apply_rulesets(
    api: GitHubAPI,
    repository: str,
    desired: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
) -> None:
    for name, ruleset in desired.items():
        payload = ruleset_payload(name, ruleset)
        existing = current.get(name)
        if existing is None:
            api.request("POST", f"repos/{repository}/rulesets", payload)
            continue
        ruleset_id = existing.get("id")
        if not isinstance(ruleset_id, int):
            raise SystemExit(f"GitHub ruleset {name} has no numeric id")
        if normalized_ruleset(existing) != normalized_ruleset(ruleset):
            api.request("PUT", f"repos/{repository}/rulesets/{ruleset_id}", payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "apply"))
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--contract", type=Path, default=CONTRACT_PATH)
    args = parser.parse_args()
    if not args.repository or "/" not in args.repository:
        parser.error("--repository OWNER/REPO is required")

    desired = load_contract(args.contract)["rulesets"]
    api = GitHubAPI()
    current = managed_rulesets(api, args.repository)
    if args.command == "apply":
        apply_rulesets(api, args.repository, desired, current)
        current = managed_rulesets(api, args.repository)
    findings = ruleset_findings(desired, current)
    if findings:
        raise SystemExit(
            "GitHub repository rulesets do not match their contract:\n  "
            + "\n  ".join(findings)
            + "\nrun github_rulesets.py apply with an administrator token"
        )
    print(f"verified {len(desired)} GitHub repository ruleset(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
