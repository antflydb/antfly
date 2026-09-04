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

"""Validate SDK metadata and CI toolchains against repository policy."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = Path(__file__).with_name("toolchain-policy.json")
TOOLCHAIN_ACTION = "uses: ./.github/actions/load-toolchain-policy"


def load_policy() -> dict:
    return json.loads(POLICY_PATH.read_text())


def policy_values(policy: dict) -> dict[str, str]:
    return {
        "python_minimum": policy["python"]["minimum"],
        "python_supported": " ".join(policy["python"]["supported"]),
        "python_build": policy["python"]["build"],
        "go_version": policy["go"]["version"],
        "npm_version": policy["npm"]["version"],
        "zig_version": policy["zig"]["version"],
        "zig_nix_attribute": policy["zig"]["nixAttribute"],
        "zig_nixpkgs_revision": policy["zig"]["nixpkgsRevision"],
        "zig_x86_64_linux_sha256": policy["zig"]["x86_64LinuxSha256"],
        "zig_aarch64_linux_sha256": policy["zig"]["aarch64LinuxSha256"],
        "macos_cross_sdk_version": policy["macosCrossSdk"]["version"],
        "macos_cross_sdk_sha256": policy["macosCrossSdk"]["sha256"],
        "rust_version": policy["rust"]["version"],
    }


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

    toolchain_action_path = ".github/actions/load-toolchain-policy/action.yml"
    toolchain_action = (REPO_ROOT / toolchain_action_path).read_text()
    check_contains(
        errors,
        toolchain_action_path,
        toolchain_action,
        "scripts/ci/check_toolchain_policy.py",
    )
    for output_name in policy_values(policy):
        check_contains(
            errors,
            toolchain_action_path,
            toolchain_action,
            f"steps.policy.outputs.{output_name}",
        )

    policy_driven_python_workflows = (
        ".github/workflows/sdks-ci.yml",
        ".github/workflows/py-pypi-publish.yml",
        ".github/workflows/antfly-release.yml",
        ".github/workflows/antfly-release-gc.yml",
        ".github/workflows/antfly-container.yml",
    )
    for workflow_path in policy_driven_python_workflows:
        workflow = (REPO_ROOT / workflow_path).read_text()
        if re.search(r"python-version:\s*['\"]?3\.\d+", workflow):
            errors.append(f"{workflow_path}: hard-codes a Python minor")
        check_contains(
            errors,
            workflow_path,
            workflow,
            TOOLCHAIN_ACTION,
        )
        check_contains(
            errors,
            workflow_path,
            workflow,
            "steps.toolchain.outputs.python_build",
        )

    sdks_ci_path = ".github/workflows/sdks-ci.yml"
    sdks_ci = (REPO_ROOT / sdks_ci_path).read_text()
    check_contains(
        errors,
        sdks_ci_path,
        sdks_ci,
        "run: python scripts/ci/check_toolchain_policy.py",
    )

    cli_package_path = ".github/workflows/cli-package.yml"
    cli_package = (REPO_ROOT / cli_package_path).read_text()
    for policy_input in (
        "inputs.python_build",
        "inputs.zig_version",
        "inputs.zig_x86_64_linux_sha256",
    ):
        check_contains(errors, cli_package_path, cli_package, policy_input)

    artifact_workflow_path = ".github/workflows/antfly-artifact-build.yml"
    artifact_workflow = (REPO_ROOT / artifact_workflow_path).read_text()
    check_contains(
        errors,
        artifact_workflow_path,
        artifact_workflow,
        TOOLCHAIN_ACTION,
    )
    for policy_output in (
        "steps.toolchain.outputs.python_build",
        "steps.toolchain.outputs.zig_version",
        "steps.toolchain.outputs.zig_nix_attribute",
        "steps.toolchain.outputs.zig_nixpkgs_revision",
        "steps.toolchain.outputs.zig_x86_64_linux_sha256",
        "steps.toolchain.outputs.macos_cross_sdk_version",
        "steps.toolchain.outputs.macos_cross_sdk_sha256",
    ):
        check_contains(errors, artifact_workflow_path, artifact_workflow, policy_output)

    for workflow_path in (
        ".github/workflows/antfly-operator-go.yml",
        ".github/workflows/antfly-proxy-go.yml",
    ):
        workflow = (REPO_ROOT / workflow_path).read_text()
        check_contains(errors, workflow_path, workflow, TOOLCHAIN_ACTION)
        check_contains(
            errors,
            workflow_path,
            workflow,
            "steps.toolchain.outputs.go_version",
        )
        check_contains(
            errors,
            workflow_path,
            workflow,
            "scripts/ci/toolchain-policy.json",
        )
        if re.search(r"GO_VERSION:\s*['\"]?\d+\.\d+", workflow):
            errors.append(f"{workflow_path}: hard-codes a Go toolchain")

    for workflow_path in (
        ".github/workflows/antfly-release.yml",
        ".github/workflows/ts-npm-publish.yml",
    ):
        workflow = (REPO_ROOT / workflow_path).read_text()
        check_contains(errors, workflow_path, workflow, TOOLCHAIN_ACTION)
        check_contains(
            errors,
            workflow_path,
            workflow,
            "steps.toolchain.outputs.npm_version",
        )
        if re.search(r"NPM_VERSION:\s*['\"]?\d+\.\d+", workflow):
            errors.append(f"{workflow_path}: hard-codes an npm toolchain")

    for workflow_path in (
        ".github/workflows/antfly-artifact-build.yml",
        ".github/workflows/diagnose-zig-arm64-bad-alloc.yml",
        ".github/workflows/sdks-ci.yml",
        ".github/workflows/zig-inference-l4-spot.yml",
        ".github/workflows/zig-scale-tests.yml",
        ".github/workflows/zig-tests.yml",
    ):
        workflow = (REPO_ROOT / workflow_path).read_text()
        check_contains(errors, workflow_path, workflow, TOOLCHAIN_ACTION)
        check_contains(
            errors,
            workflow_path,
            workflow,
            "steps.toolchain.outputs.zig_version",
        )
        if re.search(r"(?:version|ZIG_VERSION):\s*['\"]?0\.\d+", workflow):
            errors.append(f"{workflow_path}: hard-codes a Zig toolchain")

    check_contains(
        errors,
        ".github/workflows/zig-tests.yml",
        (REPO_ROOT / ".github/workflows/zig-tests.yml").read_text(),
        "steps.toolchain.outputs.zig_aarch64_linux_sha256",
    )

    diagnose_workflow_path = ".github/workflows/diagnose-zig-arm64-bad-alloc.yml"
    diagnose_workflow = (REPO_ROOT / diagnose_workflow_path).read_text()
    for policy_output in (
        "steps.toolchain.outputs.zig_nixpkgs_revision",
        "steps.toolchain.outputs.macos_cross_sdk_version",
        "steps.toolchain.outputs.macos_cross_sdk_sha256",
    ):
        check_contains(errors, diagnose_workflow_path, diagnose_workflow, policy_output)

    zig_version = policy["zig"]["version"]
    for dockerfile_path in ("zig/Dockerfile", "zig/Dockerfile.ci"):
        dockerfile = (REPO_ROOT / dockerfile_path).read_text()
        check_contains(
            errors,
            dockerfile_path,
            dockerfile,
            f"ARG ZIG_VERSION={zig_version}",
        )
    root_zon_path = "zig/build.zig.zon"
    root_zon = (REPO_ROOT / root_zon_path).read_text()
    check_contains(
        errors,
        root_zon_path,
        root_zon,
        f'.minimum_zig_version = "{zig_version}"',
    )

    zig_tests_path = ".github/workflows/zig-tests.yml"
    zig_tests = (REPO_ROOT / zig_tests_path).read_text()
    check_contains(
        errors,
        zig_tests_path,
        zig_tests,
        "scripts/ci/toolchain-policy.json",
    )
    check_contains(
        errors,
        zig_tests_path,
        zig_tests,
        "steps.toolchain.outputs.rust_version",
    )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--get",
        choices=(
            "python-minimum",
            "python-supported",
            "python-build",
            "go",
            "npm",
            "zig",
            "zig-nix-attribute",
            "zig-nixpkgs-revision",
            "zig-x86_64-linux-sha256",
            "zig-aarch64-linux-sha256",
            "macos-cross-sdk-version",
            "macos-cross-sdk-sha256",
            "rust",
        ),
        help="print one policy value instead of validating the repository",
    )
    output_group.add_argument(
        "--github-output",
        type=Path,
        help="append every toolchain value to a GitHub Actions output file",
    )
    args = parser.parse_args()
    policy = load_policy()

    values = policy_values(policy)
    if args.get:
        cli_values = {
            "python-minimum": values["python_minimum"],
            "python-supported": values["python_supported"],
            "python-build": values["python_build"],
            "go": values["go_version"],
            "npm": values["npm_version"],
            "zig": values["zig_version"],
            "zig-nix-attribute": values["zig_nix_attribute"],
            "zig-nixpkgs-revision": values["zig_nixpkgs_revision"],
            "zig-x86_64-linux-sha256": values["zig_x86_64_linux_sha256"],
            "zig-aarch64-linux-sha256": values["zig_aarch64_linux_sha256"],
            "macos-cross-sdk-version": values["macos_cross_sdk_version"],
            "macos-cross-sdk-sha256": values["macos_cross_sdk_sha256"],
            "rust": values["rust_version"],
        }
        print(cli_values[args.get])
        return 0
    # Toolchain export bootstraps setup-python itself, so keep this path
    # compatible with the runner's system Python and validate after setup.
    if args.github_output:
        with args.github_output.open("a") as output:
            for name, value in values.items():
                output.write(f"{name}={value}\n")
        return 0
    errors = validate(policy)
    if errors:
        print("Repository toolchain policy is inconsistent:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print("Repository toolchain policy is consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
