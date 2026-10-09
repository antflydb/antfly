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

"""Run comparative HN checks in one temporary GKE pod in the bucket's region.

Uses the caller's existing GCS authority via short-lived stdin credentials.
Does not create IAM bindings, secrets, buckets, services, or persistent volumes.
The pod is removed in finally and has a hard lifetime if the caller disconnects.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

SDK_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:cde9dbd556000c21c08449d8e5828904ef91e690bec95207f71fa6a6685922c9"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, required=True, help="Linux x86_64 standalone binary"
    )
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument("--cluster", default="antfly-dev")
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--bucket", default="colony-import-sources-antfly-dev-01")
    parser.add_argument("--source-prefix", default="hn-poc/20261007")
    parser.add_argument(
        "--run-prefix",
        required=True,
        help="New GCS artifact namespace; source is read only",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="Binary source revision")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--cycles", type=int, default=2, help="Empty-cache/restart cycles per table"
    )
    args = parser.parse_args()
    if not args.binary.is_file():
        parser.error("binary must be an existing Linux executable")
    if args.cycles < 1 or args.repeats < 1:
        parser.error("cycles and repeats must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    # kubectl's auth plugin may cache credentials beside its kubeconfig. Keep
    # those standard CLI files outside the delivered results and remove them.
    credentials = tempfile.TemporaryDirectory(prefix="hackernews-kube-")
    env = dict(os.environ, KUBECONFIG=str(Path(credentials.name) / "kubeconfig"))

    def run(argv, **kwargs):
        return subprocess.run(argv, env=env, check=True, **kwargs)

    run(
        [
            "gcloud",
            "container",
            "clusters",
            "get-credentials",
            args.cluster,
            "--region",
            args.region,
            "--project",
            args.project,
        ]
    )
    location = run(
        [
            "gcloud",
            "storage",
            "buckets",
            "describe",
            "gs://" + args.bucket,
            "--project",
            args.project,
            "--format=value(location)",
        ],
        capture_output=True,
        text=True,
    ).stdout.strip()
    if location.lower() != args.region:
        raise ValueError(
            f"Bucket location {location} differs from worker region {args.region}"
        )
    pod_name = "hackernews-benchmark-" + uuid.uuid4().hex[:12]
    kubectl = ["kubectl", "-n", args.namespace]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "labels": {"app": "hackernews-benchmark"}},
        "spec": {
            "restartPolicy": "Never",
            "activeDeadlineSeconds": 2700,
            "automountServiceAccountToken": False,
            "nodeSelector": {
                "topology.kubernetes.io/region": args.region,
                "kubernetes.io/arch": "amd64",
            },
            "containers": [
                {
                    "name": "benchmark",
                    "image": SDK_IMAGE,
                    "command": ["python3", "-c", "import time; time.sleep(2700)"],
                    "resources": {
                        "requests": {
                            "cpu": "2",
                            "memory": "8Gi",
                            "ephemeral-storage": "4Gi",
                        },
                        "limits": {
                            "cpu": "2",
                            "memory": "8Gi",
                            "ephemeral-storage": "4Gi",
                        },
                    },
                }
            ],
        },
    }
    created = False
    try:
        run(kubectl + ["create", "-f", "-"], input=json.dumps(pod), text=True)
        created = True
        run(
            kubectl
            + ["wait", "--for=condition=Ready", "pod/" + pod_name, "--timeout=300s"]
        )
        worker = json.loads(
            run(
                kubectl + ["get", "pod", pod_name, "-o", "json"],
                capture_output=True,
                text=True,
            ).stdout
        )
        run(kubectl + ["exec", pod_name, "--", "mkdir", "-p", "/workspace"])
        run(kubectl + ["cp", str(args.binary), pod_name + ":/workspace/antfly"])
        run(
            kubectl
            + [
                "cp",
                str(Path(__file__).with_name("poc.py")),
                pod_name + ":/workspace/poc.py",
            ]
        )
        run(kubectl + ["exec", pod_name, "--", "chmod", "+x", "/workspace/antfly"])
        reports = {}
        for mode in ("text-only", "indexed"):
            command = kubectl + [
                "exec",
                "-i",
                pod_name,
                "--",
                "python3",
                "/workspace/poc.py",
                "--binary",
                "/workspace/antfly",
                "--project",
                args.project,
                "--bucket",
                args.bucket,
                "--prefix",
                args.source_prefix,
                "--artifact-prefix",
                args.run_prefix + "/" + mode,
                "--state",
                "/workspace/" + mode,
                "--cold-cache",
                "--require-filters",
                "--bearer-stdin",
                "--repeats",
                str(args.repeats),
            ]
            if mode == "text-only":
                command.append("--text-only")
            reports[mode] = []
            for cycle in range(args.cycles):
                label = mode + "-" + str(cycle + 1)
                # Refresh for each cycle; OAuth credentials stay in memory.
                token = run(
                    ["gcloud", "auth", "print-access-token", "--project", args.project],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                with (args.output / (label + ".log")).open("w") as log:
                    completed = subprocess.run(
                        command,
                        env=env,
                        input=token + "\n",
                        text=True,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
                if completed.returncode:
                    run(
                        kubectl
                        + [
                            "cp",
                            pod_name + ":/workspace/" + mode + "/server.log",
                            str(args.output / (label + "-server.log")),
                        ]
                    )
                    raise RuntimeError(
                        f"{label} failed; see {args.output / (label + '.log')}"
                    )
                run(
                    kubectl
                    + [
                        "cp",
                        pod_name + ":/workspace/" + mode + "/report.json",
                        str(args.output / (label + ".json")),
                    ]
                )
                result = json.loads((args.output / (label + ".json")).read_text())
                reports[mode].append(result)
                print(
                    json.dumps(
                        {
                            "mode": mode,
                            "cycle": cycle + 1,
                            "cold_ms": result["first_search_ms"],
                            "warm_ms": result["warm_search_ms"],
                            "restart_ms": result["after_restart_ms"],
                        }
                    ),
                    flush=True,
                )
        expected = [
            (hit["_source"]["hn_id"], hit["_score"])
            for hit in reports["text-only"][0]["first_response"]["hits"]["hits"]
        ]
        for cycles in reports.values():
            for cycle in cycles:
                actual = [
                    (hit["_source"]["hn_id"], hit["_score"])
                    for hit in cycle["first_response"]["hits"]["hits"]
                ]
                if actual != expected:
                    raise RuntimeError(
                        "Ranked HN IDs/scores changed between table modes or cycles"
                    )
        digest = hashlib.sha256()
        with args.binary.open("rb") as binary:
            for chunk in iter(lambda: binary.read(1048576), b""):
                digest.update(chunk)
        report = {
            "project": args.project,
            "cluster": args.cluster,
            "region": args.region,
            "node": worker["spec"]["nodeName"],
            "resources": worker["spec"]["containers"][0]["resources"],
            "image": SDK_IMAGE,
            "revision": args.revision,
            "binary_sha256": digest.hexdigest(),
            "runs": reports,
            "note": "10k-row qualification; region and optimization differ from prior local Debug runs.",
        }
        (args.output / "regional.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        try:
            if created:
                run(kubectl + ["delete", "pod", pod_name, "--wait=false"])
        finally:
            credentials.cleanup()


if __name__ == "__main__":
    main()
