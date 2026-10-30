#!/usr/bin/env python3
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

"""Render a durable development serving candidate; never apply infrastructure."""

import argparse
import json
from pathlib import Path
import re


def candidate(bucket, image, *, foundation_exists=False):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Expected a GCS bucket name")
    if not re.fullmatch(r"[a-zA-Z0-9./_:-]+@sha256:[a-f0-9]{64}", image):
        raise ValueError("Use an immutable image digest")
    namespace = "hackernews"
    labels = {"app": "hackernews"}

    def connection(capabilities, prefix):
        return {
            "kind": "external_io",
            "capabilities": capabilities,
            "external_io": {
                "protocol": "gcs",
                "buckets": [bucket],
                "prefix": prefix,
                "bucket_provisioning": "require_existing",
                "credentials": {"source": "default"},
            },
        }

    config = {
        "storage": {
            "engine": "local",
            "local": {"base_dir": "/data/antfly"},
            "artifacts": {
                "connection": "hn-artifacts",
                "bucket": bucket,
                "prefix": "indexes",
            },
        },
        "connections": {
            "hn-history": connection(["lake.read"], "archive"),
            "hn-current": connection(["lake.read", "lake.write"], "current"),
            "hn-artifacts": connection(["storage.primary"], "indexes"),
        },
        "lake_cache": {"root": "/data/cache", "max_disk_bytes": 2 * 1024**3},
        "lake_indexes": {"query_cursors": {"retention_ms": 3600000}},
    }
    resources = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}},
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "hackernews", "namespace": namespace},
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "hackernews", "namespace": namespace},
            "data": {"antfly.json": json.dumps(config)},
        },
        {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": "hackernews-state", "namespace": namespace},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "standard-rwo",
                "resources": {"requests": {"storage": "20Gi"}},
            },
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "hackernews", "namespace": namespace},
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "serviceAccountName": "hackernews",
                        "terminationGracePeriodSeconds": 60,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 1000,
                            "runAsGroup": 1000,
                            "fsGroup": 1000,
                        },
                        "containers": [
                            {
                                "name": "antfly",
                                "image": image,
                                "workingDir": "/data",
                                "command": ["/app/antfly"],
                                "args": [
                                    "standalone",
                                    "--config",
                                    "/config/antfly.json",
                                    "--data-dir",
                                    "/data/antfly",
                                    "--host",
                                    "127.0.0.1",
                                    "--port",
                                    "8877",
                                    "--auth",
                                    "false",
                                    "--health",
                                    "false",
                                    "--models-dir",
                                    "/app/empty-models",
                                ],
                                "resources": {
                                    "requests": {
                                        "cpu": "8",
                                        "memory": "48Gi",
                                        "ephemeral-storage": "1Gi",
                                    },
                                    "limits": {
                                        "cpu": "8",
                                        "memory": "48Gi",
                                        "ephemeral-storage": "1Gi",
                                    },
                                },
                                "volumeMounts": [
                                    {"name": "state", "mountPath": "/data"},
                                    {
                                        "name": "config",
                                        "mountPath": "/config",
                                        "readOnly": True,
                                    },
                                    {"name": "tmp", "mountPath": "/tmp"},
                                ],
                                "securityContext": {
                                    "readOnlyRootFilesystem": True,
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                            },
                            {
                                "name": "gateway",
                                "image": image,
                                "command": [
                                    "python3",
                                    "/app/serve.py",
                                    "--host",
                                    "0.0.0.0",
                                ],
                                "ports": [{"name": "http", "containerPort": 8080}],
                                "resources": {
                                    "requests": {
                                        "cpu": "250m",
                                        "memory": "256Mi",
                                        "ephemeral-storage": "1Gi",
                                    },
                                    "limits": {
                                        "cpu": "250m",
                                        "memory": "256Mi",
                                        "ephemeral-storage": "1Gi",
                                    },
                                },
                                "readinessProbe": {
                                    "httpGet": {"path": "/readyz", "port": "http"},
                                    "timeoutSeconds": 10,
                                    "periodSeconds": 30,
                                },
                                "livenessProbe": {
                                    "httpGet": {"path": "/healthz", "port": "http"},
                                    "timeoutSeconds": 3,
                                    "periodSeconds": 30,
                                },
                                "securityContext": {
                                    "readOnlyRootFilesystem": True,
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                            },
                        ],
                        "volumes": [
                            {
                                "name": "state",
                                "persistentVolumeClaim": {
                                    "claimName": "hackernews-state"
                                },
                            },
                            {"name": "config", "configMap": {"name": "hackernews"}},
                            {"name": "tmp", "emptyDir": {"sizeLimit": "1Gi"}},
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "hackernews", "namespace": namespace},
            "spec": {
                "type": "ClusterIP",
                "selector": labels,
                "ports": [{"port": 80, "targetPort": "http"}],
            },
        },
    ]
    archive = {
        "num_shards": 1,
        "schema": {
            "storage_mode": "relational",
            "relational_indexes": [
                {"name": name + "_idx", "keys": [{"column": name}]}
                for name in ("hn_id", "created_at", "item_type", "author", "points")
            ],
            "base_source": {
                "kind": "external",
                "format": "parquet",
                "table_id": "hn-archive",
                "uri": f"gs://{bucket}/archive",
                "credentials": {"ref": "hn-history"},
            },
        },
        "indexes": {"body_text": {"type": "full_text", "store_source": True}},
    }
    source = {
        "source": {
            "overlay": {
                "base": {"table": "hn_archive"},
                "changes": {"table": "hn_current"},
                "key": ["hn_id"],
            }
        }
    }
    if foundation_exists:
        resources = [
            resource
            for resource in resources
            if resource["kind"]
            not in {"Namespace", "ServiceAccount", "PersistentVolumeClaim"}
        ]
    return {"apiVersion": "v1", "kind": "List", "items": resources}, archive, source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--foundation-exists",
        action="store_true",
        help="Use the namespace, service account and retained claim owned by Colony",
    )
    args = parser.parse_args()
    manifest, archive, source = candidate(
        args.bucket, args.image, foundation_exists=args.foundation_exists
    )
    args.output.mkdir(parents=True, exist_ok=False)
    for name, value in (
        ("kubernetes.json", manifest),
        ("archive-table.json", archive),
        ("hackernews-source.json", source),
    ):
        (args.output / name).write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
