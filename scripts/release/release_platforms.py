#!/usr/bin/env python3
"""Read and validate the canonical native release platform policy."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


POLICY_PATH = Path(__file__).with_name("platforms.json")
REQUIRED_FIELDS = {
    "id",
    "artifact_name",
    "runner",
    "builder_os",
    "builder_arch",
    "zig_target",
    "archive_os",
    "archive_arch",
    "archive_suffix",
    "macos_sdk",
    "metal",
    "system_blas",
    "optimize",
    "jobs",
    "backends",
    "consumers",
}


def load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    policy = json.loads(path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != 1:
        raise SystemExit(f"unsupported release platform schema in {path}")
    glibc_minimum = policy.get("glibc_minimum")
    if not isinstance(glibc_minimum, str) or not re.fullmatch(
        r"[0-9]+\.[0-9]+", glibc_minimum
    ):
        raise SystemExit(f"invalid glibc_minimum in {path}")
    platforms = policy.get("platforms")
    if not isinstance(platforms, list) or not platforms:
        raise SystemExit(f"release platform policy has no platforms: {path}")

    ids: set[str] = set()
    artifacts: set[str] = set()
    archive_keys: set[tuple[str, str, str]] = set()
    for platform in platforms:
        if not isinstance(platform, dict) or REQUIRED_FIELDS - platform.keys():
            missing = (
                sorted(REQUIRED_FIELDS - set(platform))
                if isinstance(platform, dict)
                else []
            )
            raise SystemExit(f"invalid release platform entry; missing {missing}")
        platform_id = platform["id"]
        artifact_name = platform["artifact_name"]
        archive_key = (
            platform["archive_os"],
            platform["archive_arch"],
            platform["archive_suffix"],
        )
        if (
            platform_id in ids
            or artifact_name in artifacts
            or archive_key in archive_keys
        ):
            raise SystemExit(f"duplicate release platform identity: {platform_id}")
        ids.add(platform_id)
        artifacts.add(artifact_name)
        archive_keys.add(archive_key)
        if platform.get("libc") == "glibc":
            expected = f"-linux-gnu.{glibc_minimum}"
            if expected not in platform["zig_target"]:
                raise SystemExit(
                    f"{platform_id} must use the canonical glibc floor {glibc_minimum}"
                )
        if "container" in platform["consumers"] and platform.get("libc") != "glibc":
            raise SystemExit(f"container platform must be glibc: {platform_id}")
        if platform.get("npm_package_dir") and "npm" not in platform["consumers"]:
            raise SystemExit(f"npm package is not listed as a consumer: {platform_id}")
    return policy


def github_matrix(policy: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    include: list[dict[str, Any]] = []
    for platform in policy["platforms"]:
        include.append(
            {
                "artifact_name": platform["artifact_name"],
                "runner": platform["runner"],
                "os": platform["builder_os"],
                "zig_arch": platform["builder_arch"],
                "zig_target": platform["zig_target"],
                "archive_os": platform["archive_os"],
                "archive_arch": platform["archive_arch"],
                "archive_suffix": platform["archive_suffix"],
                "macos_sdk": str(platform["macos_sdk"]).lower(),
                "metal": str(platform["metal"]).lower(),
                "system_blas": str(platform["system_blas"]).lower(),
                "zig_optimize": platform["optimize"],
                "zig_jobs": platform["jobs"],
            }
        )
    return {"include": include}


def consumer_matrix(
    policy: dict[str, Any], consumer: str
) -> dict[str, list[dict[str, str]]]:
    include: list[dict[str, str]] = []
    container_arches = {"x86_64": "amd64", "arm64": "arm64"}
    for platform in policy["platforms"]:
        if consumer not in platform["consumers"]:
            continue
        entry = {
            "id": platform["id"],
            "archive_pattern": (
                f"antfly_*_{platform['archive_os']}_{platform['archive_arch']}"
                f"{platform['archive_suffix']}.tar.gz"
            ),
        }
        if consumer == "container":
            try:
                entry["container_arch"] = container_arches[platform["archive_arch"]]
            except KeyError as exc:
                raise SystemExit(
                    f"unsupported container architecture: {platform['archive_arch']}"
                ) from exc
        include.append(entry)
    if not include:
        raise SystemExit(f"release platform policy has no {consumer} consumers")
    if consumer == "container" and {entry["container_arch"] for entry in include} != {
        "amd64",
        "arm64",
    }:
        raise SystemExit("container policy must contain exactly amd64 and arm64")
    return {"include": include}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("validate", "github-matrix", "consumer-matrix"),
        nargs="?",
        default="validate",
    )
    parser.add_argument("consumer", nargs="?")
    args = parser.parse_args()
    policy = load_policy()
    if args.command == "github-matrix":
        print(json.dumps(github_matrix(policy), separators=(",", ":")))
    elif args.command == "consumer-matrix":
        if not args.consumer:
            parser.error("consumer-matrix requires a consumer")
        print(json.dumps(consumer_matrix(policy, args.consumer), separators=(",", ":")))
    else:
        if args.consumer:
            parser.error(f"{args.command} does not accept a consumer")
        print(f"validated {len(policy['platforms'])} release platforms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
