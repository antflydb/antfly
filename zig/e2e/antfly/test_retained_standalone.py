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

import base64
import concurrent.futures
import datetime
import json
import os
import shutil
import socket
import subprocess
import time
import threading
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
    with (
        socket.socket() as s1,
        socket.socket() as s2,
        socket.socket() as s3,
        socket.socket() as s4,
    ):
        for listener in (s1, s2, s3, s4):
            listener.bind(("127.0.0.1", 0))
        object_port, console_port, server_port, pgwire_port = (
            s.getsockname()[1] for s in (s1, s2, s3, s4)
        )
    endpoint = f"http://127.0.0.1:{object_port}"
    base = f"http://127.0.0.1:{server_port}/db/v1"
    secret = "retainedfixturepassword"
    env = dict(
        os.environ,
        ANTFLY_BOOTSTRAP_ADMIN_PASSWORD="e2e-bootstrap-password",
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
            (
                f"http://127.0.0.1:{server_port}" + path
                if path.startswith("/auth/")
                else base + path
            ),
            payload,
            {
                "content-type": "application/json",
                "authorization": "Basic "
                + base64.b64encode(b"admin:e2e-bootstrap-password").decode(),
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                content = response.read()
                return json.loads(content) if content else None
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
                "--auth",
                "true",
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
        object_server = start(
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
                    "pgwire": {
                        "enabled": True,
                        "bind_host": "127.0.0.1",
                        "bind_port": pgwire_port,
                    },
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
            "CREATE TABLE sensors (id BIGINT NOT NULL, recorded_at TIMESTAMPTZ, site TEXT NOT NULL)"
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
        for batch in range(9):
            values = ",".join(
                f"({1000 + i}, '{timestamp}', 'site-{i}')"
                for i in range(batch * 500, (batch + 1) * 500)
            )
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
        query = {
            "start_ns": str(first_bucket),
            "end_ns": str(first_bucket + interval),
            "dimensions": ["north"],
        }
        with pytest.raises(OSError, match="409"):
            api(
                "POST",
                path + "/close",
                {"expected_revision": active["revision"], "cancel_activation": True},
            )
        failed_status = wait_for(
            lambda: api("GET", broken_path),
            lambda result: any(
                owner.get("native", {}).get("last_error")
                for owner in result.get("progress", {}).get("owners", [])
                if owner.get("native")
            ),
            30,
            server,
        )
        assert failed_status["phase"] == "activating"
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
        # Source and maintained rows commit together. A compound SELECT must
        # either certify a common capture or return a retryable conflict.
        sql(
            "CREATE TABLE atomic_probe (id DOUBLE PRECISION NOT NULL, recorded_at TIMESTAMPTZ)"
        )
        sql(f"INSERT INTO atomic_probe (id, recorded_at) VALUES (1, '{timestamp}')")
        atomic_path = "/databases/default/namespaces/public/datasets/atomic_probe_max"
        api(
            "POST",
            atomic_path,
            {
                "source": {"table": "atomic_probe"},
                "timestamp": "recorded_at",
                "interval_ns": str(interval),
                "mutable_for_ns": str(mutable_for),
                "retention_ns": str(retention),
                "measures": [{"name": "max_id", "law": "max", "field": "id"}],
            },
        )
        wait_for(
            lambda: api("GET", atomic_path),
            lambda row: row["phase"] == "active",
            process=server,
        )
        stop_writes = threading.Event()
        write_errors = []

        def write_atomic_rows():
            for value in range(2, 202):
                if stop_writes.is_set():
                    break
                try:
                    api(
                        "POST",
                        "/sql",
                        {"statement": f"UPDATE atomic_probe SET id = {value}"},
                    )
                    time.sleep(0.001)
                except Exception as exc:
                    write_errors.append(str(exc))
                    break

        writer = threading.Thread(target=write_atomic_rows)
        writer.start()
        successes = 0
        concurrent_successes = 0
        try:
            statement = f"SELECT s.max_id, r.id FROM retained_summary(public.atomic_probe_max, '{first_bucket}', '{first_bucket + interval}') s CROSS JOIN atomic_probe r"
            for _ in range(80):
                try:
                    result = api("POST", "/sql", {"statement": statement})
                except OSError as exc:
                    assert '"code":"40001"' in str(exc), exc
                    continue
                assert len(result["rows"]) == 1, result
                maximum, raw_id = result["rows"][0]
                assert maximum == raw_id, result
                successes += 1
                concurrent_successes += writer.is_alive()
        finally:
            stop_writes.set()
            writer.join(timeout=15)
        assert not writer.is_alive()
        assert not write_errors, write_errors
        assert successes and concurrent_successes, (successes, concurrent_successes)
        subscription = api(
            "POST",
            atomic_path + "/subscriptions",
            {
                "start_ns": str(first_bucket),
                "end_ns": str(first_bucket + interval),
            },
        )
        # Exceed both one poll (8 frames) and the native replay history (256).
        for value in range(1000, 1260):
            api("POST", "/sql", {"statement": f"UPDATE atomic_probe SET id = {value}"})
        caught_up = api(
            "POST",
            atomic_path + "/subscriptions/poll",
            {"cursor": subscription["cursor"]},
        )
        assert caught_up["resynchronized"] is True
        changes = [event for event in caught_up["events"] if event["type"] == "upsert"]
        assert len(changes) == 1 and float(changes[0]["values"][0]) == 1259
        assert (
            api(
                "POST",
                atomic_path + "/subscriptions/poll",
                {"cursor": subscription["cursor"]},
            )
            == caught_up
        )
        api(
            "POST",
            atomic_path + "/subscriptions/cancel",
            {"cursor": caught_up["cursor"]},
        )
        atomic = api("GET", atomic_path)
        api(
            "POST",
            atomic_path + "/close",
            {"expected_revision": atomic["revision"], "discard": True},
        )
        wait_for(
            lambda: api("GET", atomic_path),
            lambda row: row["source_released"],
            45,
            server,
        )
        sql("DROP TABLE atomic_probe")
        export_query = {
            key: value for key, value in query.items() if key != "dimensions"
        }

        def read_all_pages():
            after = None
            collected = []
            seen = set()
            while True:
                request = dict(export_query, max_rows=1000)
                if after is not None:
                    request["after"] = after
                page = api("POST", path + "/query", request)
                collected.extend(page["rows"])
                after = page["next"]
                if after is None:
                    break
                assert after not in seen
                seen.add(after)
            assert len(collected) == 4501
            assert len({tuple(row["dimensions"]) for row in collected}) == 4501
            return collected

        read_all_pages()
        selective = sql(
            f"SELECT sample_count FROM retained_summary(public.sensor_counts, '{first_bucket}', '{first_bucket + interval}') WHERE site = 'site-4242'"
        )
        assert selective["rows"] == [["1"]]
        # Exercise the actual pgwire catalog wrapper with both simple and
        # prepared execution when the optional SQL driver is installed.
        try:
            import psycopg
        except ImportError:
            psycopg = None
        if psycopg is not None:
            api(
                "POST",
                "/auth/v1/users/retained-reader",
                {
                    "password": "retained-reader-password",
                    "initial_policies": [
                        {
                            "resource": "sensor_counts",
                            "resource_type": "dataset",
                            "type": "read",
                        }
                    ],
                },
            )
            with psycopg.connect(
                host="127.0.0.1",
                port=pgwire_port,
                dbname="default",
                user="retained-reader",
                password="retained-reader-password",
                sslmode="disable",
                autocommit=True,
            ) as connection:
                for prepared in (False, True):
                    result = connection.execute(
                        f"SELECT sample_count FROM retained_summary(public.sensor_counts, '{first_bucket}', '{first_bucket + interval}') WHERE site = 'site-4242'",
                        prepare=prepared,
                    ).fetchall()
                    assert len(result) == 1 and int(result[0][0]) == 1
        status = api("GET", path + "?max_owners=1")
        assert status["progress"]["registered_owners"] == 1
        assert len(status["progress"]["owners"]) == 1
        assert status["progress"]["owners"][0]["native"]["ready"]
        limited = sql(
            f"SELECT sample_count FROM retained_summary(public.sensor_counts, '{first_bucket}', '{first_bucket + interval}') LIMIT 1"
        )
        assert len(limited["rows"]) == 1
        with pytest.raises(OSError, match="400.*22023"):
            api(
                "POST",
                "/sql",
                {
                    "statement": "INSERT INTO sensors (id, recorded_at, site) VALUES (99999, NULL, 'invalid')"
                },
            )

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
            f"SELECT sample_count FROM retained_summary(public.sensor_counts, '{first_bucket}', '{first_bucket + interval}') WHERE site = 'north'"
        )
        assert archived_sql["rows"] == [["502"]], archived_sql
        read_all_pages()

        # Parallel dashboard queries must retain independent pins without exposing
        # conditional-root conflicts, after native state and the source are gone.
        def archived_query(index):
            if index % 2:
                return api("POST", path + "/query", query)["rows"]
            return api(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT sample_count FROM retained_summary(public.sensor_counts, '{first_bucket}', '{first_bucket + interval}') WHERE site = 'north'"
                },
            )["rows"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            answers = list(executor.map(archived_query, range(24)))
        for index, answer in enumerate(answers):
            assert answer == (updated["rows"] if index % 2 else [["502"]])

        object_paths = subprocess.check_output(
            [
                "aws",
                "--endpoint-url",
                endpoint,
                "s3api",
                "list-objects-v2",
                "--bucket",
                "retained-fixture",
            ],
            env=env,
            text=True,
        )
        assert json.loads(object_paths).get("Contents")
        sql("CREATE TABLE abandoned (recorded_at TIMESTAMPTZ, site TEXT)")
        sql(
            f"INSERT INTO abandoned (recorded_at, site) VALUES ('{timestamp}', 'north'), ('{timestamp}', NULL)"
        )
        abandoned_path = (
            "/databases/default/namespaces/public/datasets/abandoned_rollup"
        )
        abandoned_definition = dict(bad_definition, source={"table": "abandoned"})
        api("POST", abandoned_path, abandoned_definition)
        abandoned = wait_for(
            lambda: api("GET", abandoned_path),
            lambda row: row["phase"] == "active",
            process=server,
        )
        expiring_path = (
            "/databases/default/namespaces/public/datasets/abandoned_expired"
        )
        expiring_definition = dict(
            abandoned_definition,
            # A one-nanosecond interval forces the public read through durable
            # clock admission, even immediately after backfill, on both engines.
            interval_ns="1",
            mutable_for_ns="0",
            retention_ns="1000000000",
        )
        api("POST", expiring_path, expiring_definition)
        expiring = wait_for(
            lambda: api("GET", expiring_path),
            lambda row: row["phase"] == "active",
            process=server,
        )
        with pytest.raises(OSError, match="410.*RetainedSeriesHistoryExpired"):
            api(
                "POST",
                expiring_path + "/query",
                {"start_ns": str(first_bucket), "end_ns": str(first_bucket + 10**9)},
            )
        api(
            "POST",
            expiring_path + "/close",
            {"expected_revision": expiring["revision"], "discard": True},
        )
        wait_for(
            lambda: api("GET", expiring_path),
            lambda row: row["source_released"],
            45,
            server,
        )
        stop(object_server)
        closing = api(
            "POST",
            abandoned_path + "/close",
            {"expected_revision": abandoned["revision"]},
        )
        assert closing["phase"] == "closing"
        abandoned = api(
            "POST",
            abandoned_path + "/close",
            {"expected_revision": closing["revision"], "discard": True},
        )
        assert abandoned["phase"] == "retiring"
        wait_for(
            lambda: api("GET", abandoned_path),
            lambda row: row["source_released"],
            45,
            server,
        )
        sql("DROP TABLE abandoned")
        current = api("GET", path)
        discarded = api(
            "POST",
            path + "/close",
            {"expected_revision": current["revision"], "discard": True},
        )
        assert discarded["phase"] == "retiring"
        assert discarded["source_released"]
        with pytest.raises(OSError, match="409"):
            api("POST", path + "/query", query)
        object_server = start(
            [
                "minio",
                "server",
                "--address",
                f"127.0.0.1:{object_port}",
                "--console-address",
                f"127.0.0.1:{console_port}",
                str(data / "objects"),
            ],
            "minio-restarted.log",
        )
        wait_for(
            lambda: (
                urllib.request.urlopen(
                    endpoint + "/minio/health/ready", timeout=2
                ).status
            ),
            lambda value: value == 200,
            15,
        )

        def remaining_summary_objects():
            listing = json.loads(
                subprocess.check_output(
                    [
                        "aws",
                        "--endpoint-url",
                        endpoint,
                        "s3api",
                        "list-objects-v2",
                        "--bucket",
                        "retained-fixture",
                    ],
                    env=env,
                    text=True,
                )
            )
            return [
                row["Key"]
                for row in listing.get("Contents", [])
                if "/datasets/" in row["Key"] and f"/{current['id']}/" in row["Key"]
            ]

        wait_for(remaining_summary_objects, lambda keys: not keys, 60, server)

    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
