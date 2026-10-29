# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Render staged managed HN requests without creating infrastructure or credentials."""

import argparse
from copy import deepcopy
import json
from pathlib import Path, PurePosixPath
import re
from types import SimpleNamespace
from uuid import UUID

from configure_catalog import table_definition

RUNTIME = json.loads(Path(__file__).with_name("runtime.json").read_text())


def connection(auth_id):
    return "cloud-gcs-cred_" + UUID(auth_id).hex


def management_plan(project, image=None):
    image = image or RUNTIME["image"]
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
        raise ValueError("Invalid showcase project")
    match = re.fullmatch(
        r"[a-zA-Z0-9./_-]+(?::v?0\.3\.0-rc\.6)?@(sha256:[a-f0-9]{64})", image
    )
    if not match or match[1] != RUNTIME["image"].rsplit("@", 1)[1]:
        raise ValueError(
            "Supply the published, verified RC.6 multi-platform image digest recorded in runtime.json"
        )

    def binding(name, bucket, prefix, mode):
        return {
            "display_name": name,
            "provider": "gcs",
            "auth_type": "google_credentials",
            "secret": '{"type":"workload_identity"}',
            "config": {"bucket": bucket, "prefix": prefix, "access_mode": mode},
        }

    return {
        "create-instance.json": {
            "name": "Hacker News",
            "slug": "hackernews",
            "region": "us-central1",
            "tier": "starter",
            "mode": "single",
            "version_policy": "pinned",
            "node_config": {"cpu": "1000m", "memory": "4Gi", "data_storage": "20Gi"},
            "initial_runtime": {
                "version": RUNTIME["version"],
                "image": image,
                "image_digest": match[1],
            },
            "storage": {
                "artifacts": {
                    "protocol": "gcs",
                    "bucket": project + "-hn-warehouse",
                    "prefix": "native-indexes",
                },
            },
        },
        "archive-provider.json": binding(
            "HN archive", project + "-hn-archive", "archive", "read_only"
        ),
        "current-provider.json": binding(
            "HN current", project + "-hn-warehouse", "current", "read_write"
        ),
        "gateway-key.json": {
            "name": "HN gateway",
            "key_type": "read_only",
            "browser_access": False,
            "quota_queries_per_month": 100000,
            "grants": [
                {"table_name": name, "actions": ["table.read"]}
                for name in ("hn_archive", "hn_current", "hackernews")
            ],
        },
        "producer-key.json": {
            "name": "HN producer",
            "key_type": "read_write",
            "browser_access": False,
            "grants": [{"table_name": "hn_current", "actions": ["table.write"]}],
        },
    }


def native_plan(project, archive_auth, current_auth, archive_prefix="archive"):
    path = PurePosixPath(archive_prefix)
    if (
        str(path) != archive_prefix
        or path.is_absolute()
        or ".." in path.parts
        or path.parts[:1] != ("archive",)
        or not re.fullmatch(r"[A-Za-z0-9_./-]+", archive_prefix)
    ):
        raise ValueError("Archive prefix must be a canonical path within archive/")
    current = table_definition(
        SimpleNamespace(
            mode="managed",
            table_id="hn-current",
            warehouse=f"gs://{project}-hn-warehouse/current",
            source_connection=connection(current_auth),
        )
    )
    current["storage"] = {"engine": "object"}
    archive = deepcopy(current)
    archive["schema"]["base_source"] = {
        "kind": "external",
        "format": "parquet",
        "table_id": "hn-archive",
        "uri": f"gs://{project}-hn-archive/{archive_prefix}",
        "credentials": {"ref": connection(archive_auth)},
    }
    return {
        "archive-table.json": archive,
        "current-table.json": current,
        "hackernews-source.json": {
            "source": {
                "overlay": {
                    "base": {"table": "hn_archive"},
                    "changes": {"table": "hn_current"},
                    "key": ["hn_id"],
                }
            }
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="antfly-showcases")
    parser.add_argument("--output", type=Path, required=True)
    commands = parser.add_subparsers(dest="stage", required=True)
    management = commands.add_parser("management")
    management.add_argument(
        "--image", help="Optional mirror of the digest in runtime.json"
    )
    native = commands.add_parser("native")
    native.add_argument(
        "--archive-prefix",
        default="archive",
        help="Frozen archive snapshot or private qualification prefix",
    )
    native.add_argument("--archive-auth", required=True)
    native.add_argument("--current-auth", required=True)
    args = parser.parse_args()
    try:
        files = (
            management_plan(args.project, args.image)
            if args.stage == "management"
            else native_plan(
                args.project, args.archive_auth, args.current_auth, args.archive_prefix
            )
        )
        # A plan is review evidence; do not silently overwrite earlier inputs.
        args.output.mkdir(parents=True, exist_ok=False)
        for name, value in files.items():
            (args.output / name).write_text(json.dumps(value, indent=2) + "\n")
    except (ValueError, OSError) as error:
        parser.exit(1, str(error) + "\n")
    print(json.dumps({"stage": args.stage, "files": sorted(files), "applied": False}))


if __name__ == "__main__":
    main()
