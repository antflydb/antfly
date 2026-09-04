#!/usr/bin/env python3
"""Check or apply the repository's versioned GitHub environment contract."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import quote

CONTRACT_PATH = Path(__file__).with_name("github-environments.json")
Runner = Callable[..., subprocess.CompletedProcess[str]]


class GitHubAPI:
    def __init__(self, runner: Runner = subprocess.run) -> None:
        self.runner = runner

    def request(
        self, method: str, path: str, document: dict[str, Any] | None = None
    ) -> Any:
        args = [
            "gh",
            "api",
            "--method",
            method,
            "--header",
            "Accept: application/vnd.github+json",
            "--header",
            "X-GitHub-Api-Version: 2022-11-28",
            path,
        ]
        input_text = None
        if document is not None:
            args.extend(("--input", "-"))
            input_text = json.dumps(document, sort_keys=True)
        result = self.runner(
            args,
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise SystemExit(f"GitHub API {method} {path} failed: {detail}")
        if not result.stdout.strip():
            return None
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise SystemExit(
                f"GitHub API {method} {path} returned invalid JSON"
            ) from exc


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot load GitHub environment contract: {path}") from exc
    environments = document.get("environments") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != 1
        or not isinstance(environments, dict)
    ):
        raise SystemExit("unsupported GitHub environment contract")
    for name, environment in environments.items():
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(environment, dict)
            or not isinstance(environment.get("reviewers_from"), str)
            or environment.get("prevent_self_review") is not True
            or environment.get("wait_timer") != 0
            or environment.get("deployment_branch_policy")
            != {"protected_branches": False, "custom_branch_policies": True}
            or not isinstance(environment.get("branch_policies"), list)
        ):
            raise SystemExit(f"invalid GitHub environment contract for {name}")
        policies = environment["branch_policies"]
        normalized = {
            (policy.get("name"), policy.get("type"))
            for policy in policies
            if isinstance(policy, dict)
        }
        if len(normalized) != len(policies) or any(
            not isinstance(policy_name, str)
            or not policy_name
            or policy_type not in {"branch", "tag"}
            for policy_name, policy_type in normalized
        ):
            raise SystemExit(f"invalid branch policies for GitHub environment {name}")
    return document


def environment_path(repository: str, environment: str) -> str:
    return f"repos/{repository}/environments/{quote(environment, safe='')}"


def require_document(value: Any, description: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemExit(f"GitHub returned malformed {description}")
    return value


def get_environment(
    api: GitHubAPI, repository: str, environment: str
) -> dict[str, Any]:
    return require_document(
        api.request("GET", environment_path(repository, environment)),
        f"environment {environment}",
    )


def get_branch_policies(
    api: GitHubAPI, repository: str, environment: str
) -> list[dict[str, Any]]:
    document = require_document(
        api.request(
            "GET",
            f"{environment_path(repository, environment)}/deployment-branch-policies?per_page=100",
        ),
        f"branch policies for {environment}",
    )
    policies = document.get("branch_policies")
    if not isinstance(policies, list) or any(
        not isinstance(policy, dict) for policy in policies
    ):
        raise SystemExit(
            f"GitHub returned malformed branch policies for environment {environment}"
        )
    return policies


def reviewer_rule(environment: dict[str, Any], name: str) -> dict[str, Any]:
    rules = environment.get("protection_rules")
    if not isinstance(rules, list):
        raise SystemExit(f"GitHub returned malformed protection rules for {name}")
    matches = [
        rule
        for rule in rules
        if isinstance(rule, dict) and rule.get("type") == "required_reviewers"
    ]
    if len(matches) != 1:
        raise SystemExit(
            f"GitHub environment {name} must have exactly one required-reviewer rule"
        )
    return matches[0]


def reviewer_identities(rule: dict[str, Any], name: str) -> set[tuple[str, int]]:
    reviewers = rule.get("reviewers")
    if not isinstance(reviewers, list) or not reviewers:
        raise SystemExit(f"GitHub environment {name} must have required reviewers")
    result: set[tuple[str, int]] = set()
    for entry in reviewers:
        reviewer = entry.get("reviewer") if isinstance(entry, dict) else None
        reviewer_type = entry.get("type") if isinstance(entry, dict) else None
        reviewer_id = reviewer.get("id") if isinstance(reviewer, dict) else None
        if reviewer_type not in {"User", "Team"} or not isinstance(reviewer_id, int):
            raise SystemExit(f"GitHub environment {name} has malformed reviewers")
        result.add((reviewer_type, reviewer_id))
    if len(result) != len(reviewers):
        raise SystemExit(f"GitHub environment {name} has duplicate reviewers")
    return result


def environment_findings(
    name: str,
    desired: dict[str, Any],
    current: dict[str, Any],
    current_policies: list[dict[str, Any]],
    expected_reviewers: set[tuple[str, int]],
) -> list[str]:
    findings: list[str] = []
    if current.get("deployment_branch_policy") != desired["deployment_branch_policy"]:
        findings.append("deployment branch mode differs from the contract")

    rules = current.get("protection_rules")
    reviewer_rules = (
        [
            rule
            for rule in rules
            if isinstance(rule, dict) and rule.get("type") == "required_reviewers"
        ]
        if isinstance(rules, list)
        else []
    )
    if len(reviewer_rules) != 1:
        findings.append("required-reviewer protection is missing")
    else:
        rule = reviewer_rules[0]
        if rule.get("prevent_self_review") is not desired["prevent_self_review"]:
            findings.append("self-review protection differs from the contract")
        try:
            actual_reviewers = reviewer_identities(rule, name)
        except SystemExit:
            findings.append("required reviewers are malformed or empty")
        else:
            if actual_reviewers != expected_reviewers:
                findings.append("required reviewers differ from the source environment")

    wait_rules = (
        [
            rule
            for rule in rules
            if isinstance(rule, dict) and rule.get("type") == "wait_timer"
        ]
        if isinstance(rules, list)
        else []
    )
    actual_wait = wait_rules[0].get("wait_timer") if len(wait_rules) == 1 else 0
    if len(wait_rules) > 1 or actual_wait != desired["wait_timer"]:
        findings.append("wait timer differs from the contract")

    expected_policies = {
        (policy["name"], policy["type"]) for policy in desired["branch_policies"]
    }
    actual_policies = {
        (policy.get("name"), policy.get("type")) for policy in current_policies
    }
    if actual_policies != expected_policies:
        missing = sorted(expected_policies - actual_policies)
        unexpected = sorted(actual_policies - expected_policies)
        if missing:
            findings.append(f"missing branch policies: {missing}")
        if unexpected:
            findings.append(f"unexpected branch policies: {unexpected}")
    return findings


def expected_reviewer_payload(
    source: dict[str, Any], source_name: str
) -> list[dict[str, object]]:
    identities = reviewer_identities(reviewer_rule(source, source_name), source_name)
    return [
        {"type": reviewer_type, "id": reviewer_id}
        for reviewer_type, reviewer_id in sorted(identities)
    ]


def check_environment(
    api: GitHubAPI,
    repository: str,
    name: str,
    desired: dict[str, Any],
) -> list[str]:
    source_name = desired["reviewers_from"]
    source = get_environment(api, repository, source_name)
    expected_reviewers = reviewer_identities(
        reviewer_rule(source, source_name), source_name
    )
    current = get_environment(api, repository, name)
    policies = get_branch_policies(api, repository, name)
    return environment_findings(name, desired, current, policies, expected_reviewers)


def apply_environment(
    api: GitHubAPI,
    repository: str,
    name: str,
    desired: dict[str, Any],
) -> None:
    source_name = desired["reviewers_from"]
    source = get_environment(api, repository, source_name)
    source_rule = reviewer_rule(source, source_name)
    if source_rule.get("prevent_self_review") is not True:
        raise SystemExit(
            f"reviewer source environment {source_name} must prevent self-review"
        )
    api.request(
        "PUT",
        environment_path(repository, name),
        {
            "wait_timer": desired["wait_timer"],
            "prevent_self_review": desired["prevent_self_review"],
            "reviewers": expected_reviewer_payload(source, source_name),
            "deployment_branch_policy": desired["deployment_branch_policy"],
        },
    )

    expected = {
        (policy["name"], policy["type"]) for policy in desired["branch_policies"]
    }
    current = get_branch_policies(api, repository, name)
    current_by_identity = {
        (policy.get("name"), policy.get("type")): policy for policy in current
    }
    policy_path = f"{environment_path(repository, name)}/deployment-branch-policies"
    for policy_name, policy_type in sorted(expected - current_by_identity.keys()):
        api.request(
            "POST",
            policy_path,
            {"name": policy_name, "type": policy_type},
        )
    for identity in sorted(current_by_identity.keys() - expected):
        policy_id = current_by_identity[identity].get("id")
        if not isinstance(policy_id, int):
            raise SystemExit(
                f"GitHub environment {name} has a branch policy without an id"
            )
        api.request("DELETE", f"{policy_path}/{policy_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "apply"))
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--environment")
    parser.add_argument("--contract", type=Path, default=CONTRACT_PATH)
    args = parser.parse_args()
    if not args.repository or "/" not in args.repository:
        parser.error("--repository OWNER/REPO is required")

    contract = load_contract(args.contract)
    environments = contract["environments"]
    if args.environment:
        if args.environment not in environments:
            parser.error(f"environment is not in the contract: {args.environment}")
        selected = {args.environment: environments[args.environment]}
    else:
        selected = environments

    api = GitHubAPI()
    if args.command == "apply":
        for name, desired in selected.items():
            apply_environment(api, args.repository, name, desired)

    findings: list[str] = []
    for name, desired in selected.items():
        findings.extend(
            f"{name}: {finding}"
            for finding in check_environment(api, args.repository, name, desired)
        )
    if findings:
        raise SystemExit(
            "GitHub release environment does not match its contract:\n  "
            + "\n  ".join(findings)
            + "\nrun github_environment.py apply with an administrator token"
        )
    print(f"verified {len(selected)} GitHub release environment contract(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
