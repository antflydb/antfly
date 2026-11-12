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

"""Real quorum failover while maintained summaries become archive authorities."""

import datetime
import json
import os
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
import requests
from catalog_readiness import wait_for_catalog_protocol
from conftest import AUTH_BOOTSTRAP_PASSWORD, DEFAULT_ANTFLY_BIN
from test_backup_restore import _enroll_store_root_once
from test_scaling import MultiNodeScalingCluster

pytestmark = pytest.mark.e2e_resource("antfly_process")


def eventually(read, accept=lambda value: bool(value), timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = read()
            if accept(last):
                return last
        except requests.RequestException as exc:
            last = exc
        time.sleep(0.1)
    raise AssertionError(f"retained failover did not converge: {last!r}")


def test_retained_quorum_failover_and_archive_handoff(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTFLY_BOOTSTRAP_ADMIN_PASSWORD", AUTH_BOOTSTRAP_PASSWORD)
    if not shutil.which("minio") or not shutil.which("aws"):
        pytest.skip("minio and aws are required")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).resolve()
    if not binary.exists():
        pytest.skip(f"antfly binary not built: {binary}")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    env = dict(
        os.environ,
        MINIO_ROOT_USER="retainedfixture",
        MINIO_ROOT_PASSWORD="retainedfixturepassword",
        AWS_ACCESS_KEY_ID="retainedfixture",
        AWS_SECRET_ACCESS_KEY="retainedfixturepassword",
    )
    cluster = None
    stop = threading.Event()
    in_failover = threading.Event()
    writer = None
    failed = True
    with (tmp_path / "minio.log").open("w") as log:
        objects = subprocess.Popen(
            [
                "minio",
                "server",
                "--address",
                f"127.0.0.1:{port}",
                str(tmp_path / "objects"),
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            eventually(
                lambda: requests.get(endpoint + "/minio/health/ready", timeout=2).ok,
                timeout=20,
            )
            subprocess.run(
                [
                    "aws",
                    "--endpoint-url",
                    endpoint,
                    "s3api",
                    "create-bucket",
                    "--bucket",
                    "retained-fixture",
                ],
                env=env,
                check=True,
                capture_output=True,
            )

            class RetainedCluster(MultiNodeScalingCluster):
                @property
                def metadata_urls(self):
                    return [
                        url.replace(
                            "http://", f"http://admin:{AUTH_BOOTSTRAP_PASSWORD}@", 1
                        )
                        for url in super().metadata_urls
                    ]

                @property
                def data_api_urls(self):
                    return [
                        self.data_api_url_for_node(node) for node in self.data_nodes
                    ]

                def data_api_url_for_node(self, node):
                    return (
                        super()
                        .data_api_url_for_node(node)
                        .replace(
                            "http://", f"http://admin:{AUTH_BOOTSTRAP_PASSWORD}@", 1
                        )
                    )

                def _write_config(self):
                    super()._write_config()
                    secrets = self.root / "retained-secrets.json"
                    secrets.write_text(
                        json.dumps(
                            {
                                "secrets": [
                                    {
                                        "key": "fixture.access",
                                        "value": env["AWS_ACCESS_KEY_ID"],
                                    },
                                    {
                                        "key": "fixture.secret",
                                        "value": env["AWS_SECRET_ACCESS_KEY"],
                                    },
                                ]
                            }
                        )
                    )
                    secrets.chmod(0o600)
                    config = json.loads(self.config_path.read_text())
                    config["replication_factor"] = 3
                    config["enable_auth"] = True
                    config["secrets"] = {
                        "sources": [
                            {"name": "fixture", "type": "file", "path": str(secrets)}
                        ],
                        "environment": True,
                    }
                    config["storage"]["artifacts"] = {
                        "connection": "fixture",
                        "bucket": "retained-fixture",
                        "prefix": "retained",
                    }
                    config["connections"] = {
                        "fixture": {
                            "kind": "external_io",
                            "capabilities": [
                                "lake.read",
                                "lake.write",
                                "storage.primary",
                            ],
                            "external_io": {
                                "protocol": "s3",
                                "endpoint": endpoint,
                                "addressing_style": "path",
                                "buckets": ["retained-fixture"],
                                "prefix": "retained",
                                "credentials": {
                                    "source": "static",
                                    "access_key_id": "${secret:fixture.access}",
                                    "secret_access_key": "${secret:fixture.secret}",
                                },
                            },
                        }
                    }
                    self.config_path.write_text(json.dumps(config))

            cluster = RetainedCluster(
                str(binary), initial_data_node_count=3, tick_ms=None
            )
            wait_for_catalog_protocol(cluster)

            def enroll_roots():
                incarnation = cluster.metadata_snapshot()["status"][
                    "metadata_incarnation"
                ]
                for node in cluster.data_nodes:
                    proof = subprocess.run(
                        [
                            str(binary),
                            "internal",
                            "store-root",
                            "proof",
                            "--replica-root-dir",
                            str(cluster.root / f"data-{node['id']}-replicas"),
                            "--metadata-incarnation",
                            incarnation,
                            "--node-id",
                            str(node["id"]),
                            "--store-id",
                            str(node["store_id"]),
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    _enroll_store_root_once(
                        requests, cluster.data_api_urls[0], json.loads(proof.stdout)
                    )

            api = cluster.data_api_urls[0]

            def call(method, path, body=None):
                response = requests.request(method, api + path, json=body, timeout=15)
                if not response.ok:
                    raise requests.HTTPError(
                        f"{response.status_code}: {response.text}", response=response
                    )
                result = response.json()
                assert not isinstance(result, dict) or not result.get("error"), result
                return result

            def sql(statement):
                return call("POST", "/sql", {"statement": statement})

            sql("CREATE TABLE sensors (recorded_at TIMESTAMPTZ, v DOUBLE PRECISION)")
            table_id = int(
                next(
                    t["table_id"]
                    for t in call("GET", "/tables")
                    if t["name"] == "sensors"
                )
            )

            def placement():
                snapshot = cluster.metadata_snapshot()
                group = next(
                    r["group_id"]
                    for r in snapshot["ranges"]
                    if r["table_id"] == table_id
                )
                status = next(
                    (
                        s
                        for s in snapshot["merged_group_statuses"]
                        if s["group_id"] == group
                    ),
                    {},
                )
                if status.get("healthy_voter_reports", 0) < 3:
                    return None
                store = next(
                    (
                        s
                        for s in snapshot["stores"]
                        if s["store_id"] == status.get("leader_store_id")
                    ),
                    None,
                )
                return store and store["node_id"]

            seed_leader = eventually(placement)
            api = next(
                cluster.data_api_url_for_node(node)
                for node in cluster.data_nodes
                if node["id"] == seed_leader
            )
            interval = 60_000_000_000
            bucket = (time.time_ns() // interval - 1) * interval
            timestamp = datetime.datetime.fromtimestamp(
                (bucket + 1_000_000_000) / 1e9, datetime.timezone.utc
            ).isoformat()
            try:
                sql(
                    f"INSERT INTO sensors(_id,recorded_at,v) VALUES ('seed','{timestamp}',1)"
                )
            except requests.HTTPError as exc:
                if (
                    exc.response is None
                    or exc.response.status_code != 409
                    or '"40003"' not in exc.response.text
                ):
                    raise
                # Observe a known row identity after uncertain delivery; never
                # replay the INSERT or seed a second source observation.
                eventually(
                    lambda: sql("SELECT v FROM sensors WHERE _id='seed'")["rows"],
                    lambda rows: len(rows) == 1 and float(rows[0][0]) == 1,
                )

            path = "/databases/default/namespaces/public/datasets/sensor_max"
            call(
                "POST",
                path,
                {
                    "source": {"table": "sensors"},
                    "timestamp": "recorded_at",
                    "interval_ns": str(interval),
                    "mutable_for_ns": "3600000000000",
                    "retention_ns": "86400000000000",
                    "measures": [{"name": "max_v", "law": "max", "field": "v"}],
                },
            )
            # Ordinary registration cannot approve a physical signing root.
            # The definite preflight failure must be visible and recoverable.
            eventually(
                lambda: call("GET", path),
                lambda value: any(
                    owner.get("native", {}).get("last_error")
                    == "StoreRootEnrollmentChanged"
                    for owner in value.get("progress", {}).get("owners", [])
                    if owner.get("native")
                ),
            )
            enroll_roots()
            active = eventually(
                lambda: call("GET", path), lambda value: value["phase"] == "active"
            )
            query = {"start_ns": str(bucket), "end_ns": str(bucket + interval)}
            assert call("POST", path + "/query", query)["rows"]

            leader = eventually(placement)
            # Keep public requests on a survivor, independently of the owner.
            api = next(
                cluster.data_api_url_for_node(node)
                for node in cluster.data_nodes
                if node["id"] != leader
            )
            errors = []
            acknowledged = []

            def write():
                value = 2
                while not stop.is_set():
                    try:
                        sql(f"UPDATE sensors SET v={value}")
                        acknowledged.append(value)
                    except requests.HTTPError as exc:
                        response = exc.response
                        if response is None or not (
                            response.status_code in (409, 503, 504)
                            or (in_failover.is_set() and response.status_code == 500)
                            or (
                                response.status_code == 400
                                and '"40001"' in response.text
                            )
                        ):
                            errors.append(exc)
                            return
                    except (requests.ConnectionError, requests.Timeout):
                        # Never replay an uncertain write. The next value is a
                        # distinct absolute assignment and supersedes it.
                        pass
                    except Exception as exc:
                        errors.append(exc)
                        return
                    value += 1
                    stop.wait(0.02)

            writer = threading.Thread(target=write)
            writer.start()
            eventually(lambda: len(acknowledged) >= 3)
            in_failover.set()
            cluster.data_proc_by_node_id[leader].kill()
            cluster.data_proc_by_node_id[leader].wait(timeout=10)
            before = len(acknowledged)
            eventually(lambda: len(acknowledged) >= before + 3)
            in_failover.clear()
            observed = eventually(
                lambda: sql(
                    f"SELECT s.max_v,r.v FROM retained_summary(sensor_max,'{bucket}','{bucket + interval}') s CROSS JOIN sensors r"
                )
            )["rows"]
            assert len(observed) == 1 and observed[0][0] == observed[0][1], observed
            checkpoint = float(observed[0][0])
            call("POST", path + "/close", {"expected_revision": active["revision"]})
            eventually(
                lambda: call("GET", path), lambda value: value["phase"] == "closed"
            )
            frozen = call("POST", path + "/query", query)["rows"]
            assert len(frozen) == 1 and float(frozen[0]["values"][0]) >= checkpoint, (
                frozen
            )
            before = len(acknowledged)
            eventually(lambda: len(acknowledged) >= before + 3)
            assert call("POST", path + "/query", query)["rows"] == frozen
            stop.set()
            writer.join(timeout=20)
            assert not writer.is_alive() and not errors, errors
            assert float(frozen[0]["values"][0]) <= float(
                sql("SELECT v FROM sensors")["rows"][0][0]
            )
            # The old leader replays the committed handoff on restart. Its
            # native data must never regain authority over the frozen archive.
            node = next(n for n in cluster.data_nodes if n["id"] == leader)
            cluster._start_data_node(node)
            cluster._wait_for_data_nodes_http(
                [node],
                public_api=True,
                max_timeout_s=45,
                label="restarted retained owner",
            )
            eventually(placement)
            api = cluster.data_api_url_for_node(node)
            sql("UPDATE sensors SET v=1000000000")
            assert float(sql("SELECT v FROM sensors")["rows"][0][0]) == 1000000000
            assert (
                eventually(lambda: call("POST", path + "/query", query))["rows"]
                == frozen
            )
            sql("DROP TABLE sensors")
            assert call("POST", path + "/query", query)["rows"] == frozen
            archived = sql(
                f"SELECT max_v FROM retained_summary(sensor_max,'{bucket}','{bucket + interval}')"
            )["rows"]
            assert len(archived) == 1 and float(archived[0][0]) == float(
                frozen[0]["values"][0]
            )
            failed = False
        finally:
            stop.set()
            if writer is not None:
                writer.join(timeout=20)
            if cluster is not None:
                if failed:
                    for path in cluster.log_paths:
                        print(f"[{path}]\n{path.read_text(errors='replace')[-4096:]}")
                cluster.stop(test_failed=failed)
            objects.terminate()
            objects.wait(timeout=15)
