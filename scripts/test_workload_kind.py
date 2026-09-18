# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from workload_kind import (
    GIB,
    TIERS,
    Environment,
    check_capacity,
    database_manifest,
    render,
)


class WorkloadKindTests(unittest.TestCase):
    def test_packages_preserve_cpu_memory_disk_and_topology(self):
        for tier, package in TIERS.items():
            for topology in ("single", "replicated"):
                spec = database_manifest(
                    tier,
                    topology,
                    "runtime:sha",
                    {"admission": {"query": {"max_concurrent_requests": 32}}},
                )["spec"]
                sections = (
                    [spec["standalone"]]
                    if topology == "single"
                    else [spec["metadataNodes"], spec["dataNodes"]]
                )
                for section in sections:
                    resources = section["resources"]
                    self.assertEqual(resources["cpu"], str(package["cpu"]))
                    self.assertEqual(resources["memory"], f"{package['memory_gib']}Gi")
                    self.assertEqual(
                        resources["limits"],
                        {"cpu": resources["cpu"], "memory": resources["memory"]},
                    )
                self.assertEqual(spec["imagePullPolicy"], "Never")
                self.assertEqual(
                    json.loads(spec["config"])["replication_factor"],
                    1 if topology == "single" else 3,
                )
                self.assertEqual(
                    spec["storage"][
                        "standaloneStorage" if topology == "single" else "dataStorage"
                    ],
                    f"{package['disk_gib']}Gi",
                )

    def test_docker_vm_capacity_is_aggregate_across_kind_nodes(self):
        docker = {"NCPU": 14, "MemTotal": 24 * GIB}
        for tier in TIERS:
            check_capacity(tier, "single", docker)
        with self.assertRaisesRegex(ValueError, "28 GiB"):
            check_capacity("starter", "replicated", docker)
        with self.assertRaises(ValueError):
            check_capacity("pro", "single", {"NCPU": 4, "MemTotal": 16 * GIB})
        check_capacity("pro", "replicated", {"NCPU": 26, "MemTotal": 52 * GIB})

    def test_render_is_offline_and_does_not_mutate_input_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            config = {"replication_factor": 99, "enable_metrics": True}
            render(
                path, "standard", "replicated", "runtime:sha", "operator:sha", config
            )
            self.assertEqual(config["replication_factor"], 99)
            self.assertEqual(
                len(json.loads((path / "kind.json").read_text())["nodes"]), 4
            )
            self.assertEqual(
                json.loads((path / "database.json").read_text())["spec"]["mode"],
                "Distributed",
            )

    def test_receipt_never_targets_ambient_context_or_recreated_cluster(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "environment.json").write_text(
                json.dumps({"cluster": "antfly-workload-test", "cluster_uid": "old"})
            )
            environment = Environment(path)
            with patch("workload_kind.run", return_value="new") as command:
                with self.assertRaisesRegex(ValueError, "identity differs"):
                    environment.verify_identity()
                argv = command.call_args.args[0]
                self.assertIn("--kubeconfig", argv)
                self.assertIn("kind-antfly-workload-test", argv)
            (path / "environment.json").write_text(
                json.dumps({"cluster": "production"})
            )
            with self.assertRaises(ValueError):
                Environment(path)


if __name__ == "__main__":
    unittest.main()
