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

"""Public standalone summary cancellation and concurrent archived serving."""

import concurrent.futures
import datetime
import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request

import pytest
from conftest import DEFAULT_ANTFLY_BIN, resolve_binary_path

pytestmark = pytest.mark.e2e_resource("antfly_process")


@pytest.mark.parametrize("engine", ["local", "lite"])
def test_standalone_retained_summary_recovery_and_concurrent_archive(tmp_path, engine):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    for command in ("minio", "aws"):
        if shutil.which(command) is None:
            pytest.skip(f"{command} is required for standalone archive integration")
    data = tmp_path
    # Allocate independent ports so this fixture can run alongside other servers.
    with socket.socket() as s1, socket.socket() as s2, socket.socket() as s3:
        for listener in (s1, s2, s3):
            listener.bind(("127.0.0.1", 0))
        object_port, console_port, server_port = (
            s.getsockname()[1] for s in (s1, s2, s3)
        )
    endpoint = f"http://127.0.0.1:{object_port}"
    base = f"http://127.0.0.1:{server_port}/db/v1"
    secret = "retainedfixturepassword"
    env = dict(
        os.environ,
        MINIO_ROOT_USER="retainedfixture",
        MINIO_ROOT_PASSWORD=secret,
        AWS_ACCESS_KEY_ID="retainedfixture",
        AWS_SECRET_ACCESS_KEY=secret,
        AWS_DEFAULT_REGION="us-east-1",
        AWS_CONFIG_FILE="/dev/null",
        AWS_PAGER="",
    )
    processes = []
    use_lite = engine == "lite"
    expect_diagnostic = True

    def start(args, log):
        with (data / log).open("wb") as output:
            process = subprocess.Popen(
                args, stdout=output, stderr=subprocess.STDOUT, env=env
            )
        processes.append(process)
        return process

    def wait_for(action, ready, seconds=90, process=None):
        last = None
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            if process is not None and process.poll() is not None:
                raise RuntimeError(
                    f"Standalone exited during the lifecycle with status {process.returncode}"
                )
            try:
                last = action()
                if ready(last):
                    return last
            except (OSError, ValueError) as exc:
                last = str(exc)
            time.sleep(0.35)
        raise AssertionError(last)

    def api(method, path, body=None):
        payload = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            base + path, payload, {"content-type": "application/json"}, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            raise OSError(
                f"{method} {path} -> {exc.code}: {exc.read().decode()}"
            ) from exc

    def sql(statement):
        result = api("POST", "/sql", {"statement": statement})
        print("SQL", statement, result, flush=True)
        assert not result.get("error"), result
        return result

    def start_server(log):
        process = start(
            [
                str(binary),
                "standalone",
                "--config",
                str(data / "config.json"),
                "--secret-store-path",
                str(data / "secrets.json"),
                "--host",
                "127.0.0.1",
                "--port",
                str(server_port),
                "--data-dir",
                str(data / "db"),
                "--control-tick-ms",
                "20",
                "--health",
                "false",
                "--replica-root-dir",
                str(data / "replicas"),
                "--replica-catalog-path",
                str(data / "catalog.txt"),
                "--snapshot-root-dir",
                str(data / "snapshots"),
            ],
            log,
        )
        wait_for(lambda: api("GET", "/tables"), lambda _: True, 45, process)
        return process

    def stop(process):
        process.terminate()
        process.wait(timeout=20)

    try:
        print(data, flush=True)
        (data / "objects").mkdir()
        start(
            [
                "minio",
                "server",
                "--address",
                f"127.0.0.1:{object_port}",
                "--console-address",
                f"127.0.0.1:{console_port}",
                str(data / "objects"),
            ],
            "minio.log",
        )
        wait_for(
            lambda: (
                urllib.request.urlopen(
                    endpoint + "/minio/health/ready", timeout=2
                ).status
            ),
            lambda status: status == 200,
            15,
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
        )
        primary = (
            {"engine": "lite", "lite": {"path": str(data / "db.aflite")}}
            if use_lite
            else {"engine": "local", "local": {"base_dir": str(data / "db")}}
        )
        (data / "config.json").write_text(
            json.dumps(
                {
                    "deployment_mode": "standalone",
                    "storage": {
                        **primary,
                        "artifacts": {
                            "connection": "fixture",
                            "bucket": "retained-fixture",
                            "prefix": "fixture/artifacts",
                        },
                    },
                    "connections": {
                        "fixture": {
                            "kind": "external_io",
                            "capabilities": [
                                "lake.read",
                                "lake.write",
                                "storage.primary",
                            ],
                            "external_io": {
                                "protocol": "s3",
                                "endpoint": "${secret:fixture.endpoint}",
                                "addressing_style": "path",
                                "buckets": ["retained-fixture"],
                                "prefix": "fixture",
                                "credentials": {
                                    "source": "static",
                                    "access_key_id": "${secret:fixture.access}",
                                    "secret_access_key": "${secret:fixture.secret}",
                                },
                            },
                        }
                    },
                }
            )
        )
        secrets = data / "secrets.json"
        secrets.write_text(
            json.dumps(
                {
                    "secrets": [
                        {"key": key, "value": value}
                        for key, value in [
                            ("fixture.endpoint", endpoint),
                            ("fixture.access", "retainedfixture"),
                            ("fixture.secret", secret),
                        ]
                    ]
                }
            )
        )
        secrets.chmod(0o600)
        server = start_server("server-first.log")
        # A permanently invalid historical row must not prevent healthy owners
        # from activating, and cancellation must release its source after restart.
        sql("CREATE TABLE broken (recorded_at TIMESTAMPTZ, site TEXT NOT NULL)")
        sql("INSERT INTO broken (recorded_at, site) VALUES (NULL, 'north')")
        broken_path = "/databases/default/namespaces/public/datasets/broken_rollup"
        bad_definition = {
            "source": {"database": "default", "namespace": "public", "table": "broken"},
            "timestamp": "recorded_at",
            "dimensions": ["site"],
            "interval_ns": "60000000000",
            "mutable_for_ns": "3600000000000",
            "retention_ns": "86400000000000",
            "measures": [{"name": "site", "law": "count"}],
        }
        with pytest.raises(OSError, match="400.*InvalidDatasetDefinition"):
            api("POST", broken_path, bad_definition)
        bad_definition["measures"] = [{"name": "n", "law": "count"}]
        broken_state = api("POST", broken_path, bad_definition)
        sql(
            "CREATE TABLE sensors (id BIGINT NOT NULL, recorded_at TIMESTAMPTZ NOT NULL, site TEXT NOT NULL)"
        )
        now = time.time_ns()
        interval = 60 * 10**9
        retention = 86400 * 10**9
        mutable_for = 3600 * 10**9
        first_bucket = (now // interval - 1) * interval
        timestamp = datetime.datetime.fromtimestamp(
            (first_bucket + 10**9) / 10**9, datetime.timezone.utc
        ).isoformat()
        sql(
            f"INSERT INTO sensors (id, recorded_at, site) VALUES (1, '{timestamp}', 'north')"
        )
        # More than one native backfill/cleanup page, committed before install.
        values = ",".join(f"({i}, '{timestamp}', 'north')" for i in range(3, 503))
        sql("INSERT INTO sensors (id, recorded_at, site) VALUES " + values)
        path = "/databases/default/namespaces/public/datasets/sensor_counts"
        print(
            "create",
            api(
                "POST",
                path,
                {
                    "source": {
                        "database": "default",
                        "namespace": "public",
                        "table": "sensors",
                    },
                    "timestamp": "recorded_at",
                    "dimensions": ["site"],
                    "interval_ns": str(interval),
                    "mutable_for_ns": str(mutable_for),
                    "retention_ns": str(retention),
                    "measures": [{"name": "sample_count", "law": "count"}],
                },
            ),
            flush=True,
        )
        active = wait_for(
            lambda: api("GET", path),
            lambda result: result["phase"] == "active",
            process=server,
        )
        query = {"start_ns": str(first_bucket), "end_ns": str(first_bucket + interval)}
        with pytest.raises(OSError, match="409"):
            api(
                "POST",
                path + "/close",
                {"expected_revision": active["revision"], "cancel_activation": True},
            )
        assert api("GET", broken_path)["phase"] == "activating"
        with pytest.raises(OSError, match="409"):
            api(
                "POST",
                broken_path + "/close",
                {"expected_revision": broken_state["revision"]},
            )
        cancelled = api(
            "POST",
            broken_path + "/close",
            {"expected_revision": broken_state["revision"], "cancel_activation": True},
        )
        assert cancelled["phase"] == "retiring"
        stop(server)
        server = start_server("server-cancel-reopen.log")
        wait_for(lambda: sql("DROP TABLE broken"), lambda _: True, 45, server)

        try:
            sql("DROP TABLE sensors")
        except (OSError, AssertionError) as exc:
            if expect_diagnostic:
                assert '"code":"2BP01"' in str(exc), exc
            print("source drop blocked before handoff", exc, flush=True)
            sql("SELECT id FROM sensors LIMIT 1")
        else:
            raise AssertionError("source drop succeeded before archive handoff")
        initial = api("POST", path + "/query", query)
        print("native", initial, flush=True)
        assert initial["rows"], initial
        sql(
            f"INSERT INTO sensors (id, recorded_at, site) VALUES (2, '{timestamp}', 'north')"
        )
        updated = api("POST", path + "/query", query)
        print("native after live write", updated, flush=True)
        assert updated["rows"] != initial["rows"], updated
        stop(server)
        server = start_server("server-reopen.log")
        assert api("POST", path + "/query", query)["rows"] == updated["rows"]
        print(
            "close",
            api("POST", path + "/close", {"expected_revision": active["revision"]}),
            flush=True,
        )
        stop(server)
        server = start_server("server-closing-reopen.log")
        closed = wait_for(
            lambda: api("GET", path),
            lambda result: result["phase"] == "closed",
            120,
            server,
        )
        print("closed", closed, flush=True)
        assert api("POST", path + "/query", query)["rows"] == updated["rows"]
        sql("DROP TABLE sensors")
        stop(server)
        server = start_server("server-detached-reopen.log")
        archived = api("POST", path + "/query", query)
        print("detached archived query", archived, flush=True)
        assert archived["rows"] == updated["rows"], archived
        archived_sql = sql(
            f"SELECT sample_count FROM retained_summary(public.sensor_counts, '{first_bucket}', '{first_bucket + interval}')"
        )
        assert archived_sql["rows"] == [["502"]], archived_sql

        # Parallel dashboard queries must retain independent pins without exposing
        # conditional-root conflicts, after native state and the source are gone.
        def archived_query(index):
            if index % 2:
                return api("POST", path + "/query", query)["rows"]
            return api(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT sample_count FROM retained_summary(public.sensor_counts, '{first_bucket}', '{first_bucket + interval}')"
                },
            )["rows"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            answers = list(executor.map(archived_query, range(24)))
        for index, answer in enumerate(answers):
            assert answer == (updated["rows"] if index % 2 else [["502"]])

    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
