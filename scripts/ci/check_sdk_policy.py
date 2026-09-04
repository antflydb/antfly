#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Validate SDK runtime metadata against the repository support policy."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = Path(__file__).with_name("sdk-policy.json")


def load_policy() -> dict:
    return json.loads(POLICY_PATH.read_text())


def load_toml(relative_path: str) -> dict:
    import tomllib

    with (REPO_ROOT / relative_path).open("rb") as source:
        return tomllib.load(source)


def check_equal(
    errors: list[str], location: str, actual: object, expected: object
) -> None:
    if actual != expected:
        errors.append(f"{location}: expected {expected!r}, found {actual!r}")


def check_contains(errors: list[str], location: str, text: str, expected: str) -> None:
    if expected not in text:
        errors.append(f"{location}: missing {expected!r}")


def validate(policy: dict) -> list[str]:
    errors: list[str] = []
    python = policy["python"]
    minimum = python["minimum"]
    supported = python["supported"]
    latest = supported[-1]
    check_equal(errors, "policy:python.minimum", minimum, supported[0])
    check_equal(errors, "policy:python.build", python["build"], latest)
    check_equal(
        errors,
        "policy:python.ruffTarget",
        python["ruffTarget"],
        f"py{minimum.replace('.', '')}",
    )
    expected_classifiers = [
        f"Programming Language :: Python :: {version}" for version in supported
    ]

    for relative_path in (
        "py/packages/sdk/pyproject.toml",
        "py/packages/cli/pyproject.toml",
    ):
        project = load_toml(relative_path)["project"]
        check_equal(
            errors,
            f"{relative_path}:project.requires-python",
            project.get("requires-python"),
            f">={minimum}",
        )
        classifiers = [
            classifier
            for classifier in project.get("classifiers", [])
            if classifier.startswith("Programming Language :: Python :: 3.")
        ]
        check_equal(
            errors,
            f"{relative_path}:project.classifiers",
            classifiers,
            expected_classifiers,
        )

    sdk = load_toml("py/packages/sdk/pyproject.toml")
    check_equal(
        errors,
        "py/packages/sdk/pyproject.toml:tool.ruff.target-version",
        sdk["tool"]["ruff"].get("target-version"),
        python["ruffTarget"],
    )
    check_equal(
        errors,
        "py/packages/sdk/pyproject.toml:tool.pyright.pythonVersion",
        sdk["tool"]["pyright"].get("pythonVersion"),
        minimum,
    )

    generated_project = load_toml("py/packages/sdk/src/antfly/pyproject.toml")
    check_equal(
        errors,
        "py/packages/sdk/src/antfly/pyproject.toml:tool.poetry.dependencies.python",
        generated_project["tool"]["poetry"]["dependencies"].get("python"),
        f"^{minimum}",
    )
    check_equal(
        errors,
        "py/packages/sdk/src/antfly/pyproject.toml:tool.ruff.target-version",
        generated_project["tool"]["ruff"].get("target-version"),
        python["ruffTarget"],
    )

    poetry_template_path = "py/packages/sdk/templates/pyproject_poetry.toml.jinja"
    poetry_template = (REPO_ROOT / poetry_template_path).read_text()
    check_contains(
        errors, poetry_template_path, poetry_template, f'python = "^{minimum}"'
    )

    ruff_template_path = "py/packages/sdk/templates/pyproject_ruff.toml.jinja"
    ruff_template = (REPO_ROOT / ruff_template_path).read_text()
    check_contains(
        errors,
        ruff_template_path,
        ruff_template,
        f'target-version = "{python["ruffTarget"]}"',
    )

    installation_path = "py/packages/sdk/docs/installation.rst"
    installation = (REPO_ROOT / installation_path).read_text()
    check_contains(
        errors,
        installation_path,
        installation,
        f"{minimum} through {latest}",
    )

    sdk_readme_path = "py/packages/sdk/README.md"
    sdk_readme = (REPO_ROOT / sdk_readme_path).read_text()
    check_contains(
        errors,
        sdk_readme_path,
        sdk_readme,
        f"{minimum} through {latest}",
    )

    cli_readme_path = "py/packages/cli/README.md"
    cli_readme = (REPO_ROOT / cli_readme_path).read_text()
    check_contains(errors, cli_readme_path, cli_readme, f"{minimum} through {latest}")

    publish_workflow_path = ".github/workflows/py-pypi-publish.yml"
    publish_workflow = (REPO_ROOT / publish_workflow_path).read_text()
    check_contains(
        errors,
        publish_workflow_path,
        publish_workflow,
        "scripts/ci/check_sdk_policy.py --get python-build",
    )
    check_contains(
        errors,
        publish_workflow_path,
        publish_workflow,
        "steps.sdk-policy.outputs.python_version",
    )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--get",
        choices=(
            "python-minimum",
            "python-supported",
            "python-build",
            "go",
            "zig",
            "rust",
        ),
        help="print one policy value instead of validating the repository",
    )
    args = parser.parse_args()
    policy = load_policy()

    if args.get:
        values = {
            "python-minimum": policy["python"]["minimum"],
            "python-supported": " ".join(policy["python"]["supported"]),
            "python-build": policy["python"]["build"],
            "go": policy["go"]["version"],
            "zig": policy["zig"]["version"],
            "rust": policy["rust"]["version"],
        }
        print(values[args.get])
        return 0

    errors = validate(policy)
    if errors:
        print("SDK support policy is inconsistent:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print("SDK support policy is consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
