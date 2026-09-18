#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Isolated kind environments for workload scheduling qualification."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GIB = 1024**3
NODE_IMAGE = "kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"
TIERS = {
    "starter": {"cpu": 1, "memory_gib": 4, "disk_gib": 50},
    "standard": {"cpu": 2, "memory_gib": 4, "disk_gib": 100},
    "pro": {"cpu": 4, "memory_gib": 8, "disk_gib": 200},
}
NAMESPACE = "workload-qualification"
OPERATOR_NAMESPACE = "workload-operator"


def run(argv: list[str], *, output: Path | None = None, check: bool = True) -> str:
    print("+ " + " ".join(argv), flush=True)
    result = subprocess.run(
        argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False
    )
    if output:
        output.write_text(result.stdout)
    if check and result.returncode:
        raise RuntimeError(
            f"command exited {result.returncode}: {argv!r}\n{result.stdout[-8000:]}"
        )
    return result.stdout


def save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def required_capacity(tier: str, topology: str) -> dict[str, int]:
    package = TIERS[tier]
    nodes = 1 if topology == "single" else 6
    # The load generator, operator, kube-system, and VM need separate headroom.
    return {
        "cpu": nodes * package["cpu"] + 2,
        "memory_bytes": (nodes * package["memory_gib"] + 4) * GIB,
    }


def check_capacity(tier: str, topology: str, docker: dict) -> None:
    required = required_capacity(tier, topology)
    if (
        docker["NCPU"] < required["cpu"]
        or docker["MemTotal"] < required["memory_bytes"]
    ):
        raise ValueError(
            f"{tier}/{topology} requires at least {required['cpu']} Docker CPUs and "
            f"{required['memory_bytes'] / GIB:g} GiB including test headroom; Docker has "
            f"{docker['NCPU']} CPUs and {docker['MemTotal'] / GIB:.1f} GiB. "
            "Increase Docker capacity or select a fitting case; package limits are never reduced."
        )


def database_manifest(tier: str, topology: str, image: str, config: dict) -> dict:
    package = TIERS[tier]
    limits = {"cpu": str(package["cpu"]), "memory": f"{package['memory_gib']}Gi"}
    resources = {**limits, "limits": limits.copy()}
    spec = {
        "image": image,
        "imagePullPolicy": "Never",
        "publicAPI": {"enabled": True, "serviceType": "ClusterIP", "port": 80},
        "config": json.dumps(
            {**config, "replication_factor": 1 if topology == "single" else 3}
        ),
        "storage": {"storageClass": "standard"},
    }
    if topology == "single":
        spec.update(
            {
                "mode": "Standalone",
                "standalone": {
                    "replicas": 1,
                    "nodeID": 1,
                    "resources": resources,
                    "metadataAPI": {"port": 8080},
                    "metadataRaft": {"port": 9017},
                    "storeAPI": {"port": 12380},
                    "storeRaft": {"port": 9021},
                    "health": {"port": 4200},
                },
            }
        )
        spec["storage"]["standaloneStorage"] = f"{package['disk_gib']}Gi"
    else:
        spec.update(
            {
                "mode": "Distributed",
                "metadataNodes": {
                    "replicas": 3,
                    "resources": resources,
                    "metadataAPI": {"port": 12377},
                    "metadataRaft": {"port": 9017},
                },
                "dataNodes": {
                    "replicas": 3,
                    "resources": resources,
                    "api": {"port": 12380},
                    "raft": {"port": 9021},
                },
            }
        )
        spec["storage"].update(
            {"metadataStorage": "2Gi", "dataStorage": f"{package['disk_gib']}Gi"}
        )
    return {
        "apiVersion": "antfly.io/v1",
        "kind": "AntflyCluster",
        "metadata": {"name": "database", "namespace": NAMESPACE},
        "spec": spec,
    }


def operator_manifest(image: str) -> dict:
    labels = {"app": "workload-operator"}
    metadata = {"name": "workload-operator", "namespace": OPERATOR_NAMESPACE}
    return {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": OPERATOR_NAMESPACE},
            },
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}},
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata},
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": {"name": "workload-operator"},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "antfly-operator-cluster-role",
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "workload-operator",
                        "namespace": OPERATOR_NAMESPACE,
                    }
                ],
            },
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": metadata,
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": labels},
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": {
                            "serviceAccountName": "workload-operator",
                            "containers": [
                                {
                                    "name": "operator",
                                    "image": image,
                                    "imagePullPolicy": "Never",
                                    "args": ["--leader-elect"],
                                    "resources": {
                                        "requests": {"cpu": "100m", "memory": "128Mi"},
                                        "limits": {"cpu": "500m", "memory": "512Mi"},
                                    },
                                    "readinessProbe": {
                                        "httpGet": {"path": "/readyz", "port": 8081},
                                        "periodSeconds": 2,
                                    },
                                    "securityContext": {
                                        "runAsNonRoot": True,
                                        "allowPrivilegeEscalation": False,
                                        "capabilities": {"drop": ["ALL"]},
                                    },
                                }
                            ],
                        },
                    },
                },
            },
        ],
    }


def render(
    directory: Path,
    tier: str,
    topology: str,
    runtime_image: str,
    operator_image: str,
    config: dict,
) -> None:
    save(
        directory / "kind.json",
        {
            "kind": "Cluster",
            "apiVersion": "kind.x-k8s.io/v1alpha4",
            "nodes": [
                {"role": "control-plane"},
                *({"role": "worker"} for _ in range(1 if topology == "single" else 3)),
            ],
        },
    )
    save(
        directory / "database.json",
        database_manifest(tier, topology, runtime_image, config),
    )
    save(directory / "operator.json", operator_manifest(operator_image))
    save(directory / "policy.json", config)


class Environment:
    def __init__(self, directory: Path):
        self.directory = directory.resolve()
        self.receipt = json.loads((self.directory / "environment.json").read_text())
        self.name = self.receipt["cluster"]
        if not re.fullmatch(r"antfly-workload-[a-z0-9-]{1,35}", self.name):
            raise ValueError("refusing a cluster outside the qualification namespace")

    def kubectl(self, *args: str, output: str | None = None, check: bool = True) -> str:
        return run(
            [
                "kubectl",
                "--kubeconfig",
                str(self.directory / "kubeconfig"),
                "--context",
                f"kind-{self.name}",
                *args,
            ],
            output=self.directory / output if output else None,
            check=check,
        )

    def verify_identity(self) -> None:
        uid = self.kubectl(
            "get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"
        ).strip()
        if uid != self.receipt.get("cluster_uid"):
            raise ValueError("cluster identity differs from the creation receipt")

    def verify_runtime(self) -> None:
        package = TIERS[self.receipt["tier"]]
        pods = json.loads(self.kubectl("get", "pods", "-n", NAMESPACE, "-o", "json"))[
            "items"
        ]
        observed = []
        for pod in pods:
            for container in pod["spec"]["containers"]:
                if container["name"] != "antfly":
                    continue
                name = pod["metadata"]["name"]
                raw = self.kubectl(
                    "exec",
                    "-n",
                    NAMESPACE,
                    name,
                    "-c",
                    "antfly",
                    "--",
                    "/bin/sh",
                    "-c",
                    "cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max",
                )
                quota, period, memory = raw.split()
                if (
                    quota == "max"
                    or int(quota) != package["cpu"] * int(period)
                    or int(memory) != package["memory_gib"] * GIB
                ):
                    raise ValueError(
                        f"{name} effective cgroup limits differ from the package: {raw}"
                    )
                observed.append(
                    {
                        "pod": name,
                        "node": pod["spec"]["nodeName"],
                        "resources": container["resources"],
                        "cpu_max": f"{quota} {period}",
                        "memory_max": memory,
                        "qos": pod.get("status", {}).get("qosClass"),
                    }
                )
        expected = 1 if self.receipt["topology"] == "single" else 6
        if len(observed) != expected:
            raise ValueError(
                f"expected {expected} runtime containers; observed {len(observed)}"
            )
        save(self.directory / "effective-resources.json", observed)

    def collect(self) -> None:
        stamp = str(time.time_ns())
        self.kubectl(
            "get", "nodes", "-o", "json", output=f"nodes-{stamp}.json", check=False
        )
        self.kubectl(
            "get",
            "pods,pvc,services,events,antflyclusters",
            "-n",
            NAMESPACE,
            "-o",
            "json",
            output=f"state-{stamp}.json",
            check=False,
        )
        raw = self.kubectl("get", "pods", "-n", NAMESPACE, "-o", "json", check=False)
        try:
            pods = json.loads(raw).get("items", [])
        except json.JSONDecodeError:
            pods = []
        for pod in pods:
            name = pod["metadata"]["name"]
            for container in pod["spec"]["containers"]:
                cname = container["name"]
                self.kubectl(
                    "logs",
                    "-n",
                    NAMESPACE,
                    name,
                    "-c",
                    cname,
                    "--timestamps",
                    output=f"{name}-{cname}-{stamp}.log",
                    check=False,
                )
                self.kubectl(
                    "exec",
                    "-n",
                    NAMESPACE,
                    name,
                    "-c",
                    cname,
                    "--",
                    "/bin/sh",
                    "-c",
                    "for f in cpu.max cpu.stat memory.max memory.current memory.peak memory.events; do echo $f; cat /sys/fs/cgroup/$f; done",
                    output=f"{name}-{cname}-cgroup-{stamp}.txt",
                    check=False,
                )
        self.kubectl(
            "logs",
            "-n",
            OPERATOR_NAMESPACE,
            "deployment/workload-operator",
            "--timestamps",
            output=f"operator-{stamp}.log",
            check=False,
        )
        checksums = {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.directory.iterdir()
            if p.is_file() and p.name not in {"kubeconfig", "checksums.json"}
        }
        save(self.directory / "checksums.json", checksums)

    def deploy(self) -> None:
        self.verify_identity()
        for image in {self.receipt["runtime_image"], self.receipt["operator_image"]}:
            run(["kind", "load", "docker-image", image, "--name", self.name])
        self.kubectl(
            "apply", "--server-side", "-f", str(ROOT / "go/pkg/operator/manifests/crd")
        )
        self.kubectl(
            "apply", "-f", str(ROOT / "go/pkg/operator/manifests/rbac/role.yaml")
        )
        self.kubectl("apply", "-f", str(self.directory / "operator.json"))
        self.kubectl(
            "rollout",
            "status",
            "deployment/workload-operator",
            "-n",
            OPERATOR_NAMESPACE,
            "--timeout=300s",
        )
        self.kubectl("apply", "-f", str(self.directory / "database.json"))
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            obj = json.loads(
                self.kubectl(
                    "get", "antflycluster", "database", "-n", NAMESPACE, "-o", "json"
                )
            )
            if obj.get("status", {}).get("phase") == "Running":
                self.verify_runtime()
                self.collect()
                print(f"Ready. Kubeconfig: {self.directory / 'kubeconfig'}", flush=True)
                return
            time.sleep(5)
        self.collect()
        raise RuntimeError(
            "database did not become Running within 10 minutes; evidence retained"
        )


def create(args: argparse.Namespace) -> None:
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    config = (
        json.loads(args.config.read_text())
        if args.config
        else {"log": {"level": "info", "style": "json"}, "enable_metrics": True}
    )
    render(
        directory,
        args.tier,
        args.topology,
        args.runtime_image,
        args.operator_image,
        config,
    )
    name = args.cluster or f"antfly-workload-{args.tier}-{int(time.time())}"
    if not re.fullmatch(r"antfly-workload-[a-z0-9-]{1,35}", name):
        raise ValueError(
            "cluster names must start with antfly-workload- and contain lowercase DNS characters"
        )
    receipt = {
        "cluster": name,
        "tier": args.tier,
        "topology": args.topology,
        "runtime_image": args.runtime_image,
        "operator_image": args.operator_image,
        "node_image": args.node_image,
        "qualified": False,
        "harness_revision": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip(),
        "harness_status": run(
            ["git", "-C", str(ROOT), "status", "--porcelain"]
        ).strip(),
        "required_capacity": required_capacity(args.tier, args.topology),
    }
    save(directory / "environment.json", receipt)
    if args.render_only:
        return
    docker = json.loads(
        run(
            ["docker", "info", "--format", "{{json .}}"],
            output=directory / "docker-info.json",
        )
    )
    check_capacity(args.tier, args.topology, docker)
    clusters = run(["kind", "get", "clusters"]).splitlines()
    if name in clusters:
        raise ValueError(f"refusing to reuse existing cluster {name}")
    for label, image in [
        ("runtime", args.runtime_image),
        ("operator", args.operator_image),
    ]:
        run(
            ["docker", "image", "inspect", image],
            output=directory / f"{label}-image.json",
        )
    run(["kind", "version"], output=directory / "kind-version.txt")
    run(
        [
            "kind",
            "create",
            "cluster",
            "--name",
            name,
            "--image",
            args.node_image,
            "--config",
            str(directory / "kind.json"),
            "--kubeconfig",
            str(directory / "kubeconfig"),
            "--wait",
            "180s",
        ],
        output=directory / "create.log",
    )
    receipt["created"] = True
    receipt["cluster_uid"] = run(
        [
            "kubectl",
            "--kubeconfig",
            str(directory / "kubeconfig"),
            "--context",
            f"kind-{name}",
            "get",
            "namespace",
            "kube-system",
            "-o",
            "jsonpath={.metadata.uid}",
        ]
    ).strip()
    save(directory / "environment.json", receipt)
    environment = Environment(directory)
    try:
        environment.deploy()
    except BaseException:
        environment.collect()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    start = commands.add_parser("create")
    start.add_argument("--directory", type=Path, required=True)
    start.add_argument("--tier", choices=TIERS, default="starter")
    start.add_argument("--topology", choices=("single", "replicated"), default="single")
    start.add_argument("--runtime-image", required=True)
    start.add_argument("--operator-image", required=True)
    start.add_argument("--node-image", default=NODE_IMAGE)
    start.add_argument("--cluster")
    start.add_argument("--config", type=Path)
    start.add_argument("--render-only", action="store_true")
    for command in ("collect", "deploy", "delete", "connect"):
        sub = commands.add_parser(command)
        sub.add_argument("--directory", type=Path, required=True)
        if command == "connect":
            sub.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    if args.action == "create":
        create(args)
        return 0
    environment = Environment(args.directory)
    if not environment.receipt.get("created"):
        raise ValueError("this receipt does not own a created kind cluster")
    if args.action == "collect":
        environment.collect()
    elif args.action == "deploy":
        environment.deploy()
    elif args.action == "delete":
        environment.verify_identity()
        environment.collect()
        run(["kind", "delete", "cluster", "--name", environment.name])
        environment.receipt["created"] = False
        save(environment.directory / "environment.json", environment.receipt)
    else:
        argv = [
            "kubectl",
            "--kubeconfig",
            str(environment.directory / "kubeconfig"),
            "--context",
            f"kind-{environment.name}",
            "-n",
            NAMESPACE,
            "port-forward",
            "--address",
            "127.0.0.1",
            "service/database-public-api",
            f"{args.port}:80",
        ]
        os.execvp(argv[0], argv)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
