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

"""Stock Debezium snapshot/stream/restart qualification using only ordinary SQL."""

import argparse
import base64
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parent
IMAGE = "antfly-debezium-qualification:3.3.0"
POSTGRES = "postgres:17.6-bookworm@sha256:f3bd19c606e442c3d7bdfa8002e03fe260a1023351e0ea4598032022b68dd6e3"


class CommandFailure(RuntimeError):
    def __init__(self, result):
        super().__init__(
            f"command failed ({result.returncode}): {result.stderr[-8000:]}"
        )
        self.stdout = result.stdout
        self.stderr = result.stderr


def command(*args, timeout=120, env=None):
    result = subprocess.run(
        args, capture_output=True, text=True, timeout=timeout, env=env
    )
    if result.returncode:
        raise CommandFailure(result)
    return result.stdout.strip()


def property_value(value):
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace(" ", "\\ ")
    )


class Client:
    def __init__(self, directory, network, name, image):
        self.events = []
        self.errors = queue.Queue()
        self.directory = directory
        self.network = network
        self.name = name
        self.image = image
        self.process = None
        self.run_number = 0

    def start(self):
        self.run_number += 1
        self.stderr = (self.directory / f"client-{self.run_number}.stderr").open("w")
        self.event_file = (self.directory / f"client-{self.run_number}.jsonl").open("w")
        self.process = subprocess.Popen(
            [
                "docker",
                "run",
                "--rm",
                "--init",
                "--name",
                self.name,
                "--network",
                self.network,
                "--memory",
                "512m",
                "--cpus",
                "1",
                "-e",
                "JAVA_TOOL_OPTIONS=-Xmx256m -XX:ActiveProcessorCount=2",
                "--mount",
                f"type=bind,source={self.directory / 'connector.properties'},target=/connector.properties,readonly",
                "--mount",
                f"type=bind,source={self.directory / 'state'},target=/state",
                self.image,
                "/connector.properties",
            ],
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            text=True,
        )

        def receive():
            try:
                for line in self.process.stdout:
                    self.event_file.write(line)
                    self.event_file.flush()
                    self.events.append(json.loads(line))
            except Exception as error:
                self.errors.put(error)

        self.reader = threading.Thread(target=receive, daemon=True)
        self.reader.start()

    def wait(self, predicate, start=0, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.errors.empty():
                raise self.errors.get_nowait()
            for event in self.events[start:]:
                if predicate(event):
                    return event
            if self.process.poll() is not None:
                self.reader.join(timeout=5)
                raise RuntimeError(
                    f"client exited {self.process.returncode}: "
                    f"{(self.directory / f'client-{self.run_number}.stderr').read_text()[-12000:]}"
                )
            time.sleep(0.02)
        raise TimeoutError(f"CDC event deadline expired; inspect {self.directory}")

    def stop(self, abrupt=False):
        if self.process is None:
            return
        if self.process.poll() is None:
            if abrupt:
                command("docker", "kill", "--signal", "KILL", self.name)
            else:
                command("docker", "stop", "--time", "30", self.name)
        code = self.process.wait(timeout=45)
        self.reader.join(timeout=5)
        self.process.stdout.close()
        self.stderr.close()
        self.event_file.close()
        self.process = None
        if code not in ({137} if abrupt else {0, 143}):
            raise RuntimeError(f"unexpected client exit status {code}")


def row(event, operation, key):
    value = event.get("value")
    image = "before" if operation == "d" else "after"
    return (
        isinstance(value, dict)
        and value.get("op") == operation
        and (value.get(image) or {}).get("id") == key
    )


def run(options, directory):
    suffix = uuid.uuid4().hex[:12]
    pg, client_name = (f"antfly-cdc-{kind}-{suffix}" for kind in ("pg", "client"))
    network = options.network
    table, publication, slot = (
        f"qualification_{kind}_{suffix}" for kind in ("records", "pub", "slot")
    )
    second = f"{table}_other" if options.multi_table else None
    client = Client(directory, network, client_name, options.image)
    host = options.host if options.external else pg
    password = os.environ.get("QUALIFICATION_DATABASE_PASSWORD", "local-test-only")
    env = dict(os.environ, PGPASSWORD=password)

    def postgres_client(executable, *args):
        return command(
            "docker",
            "run",
            "--rm",
            "--network",
            network,
            "-e",
            "PGPASSWORD",
            "-e",
            "PGCONNECT_TIMEOUT=5",
            POSTGRES,
            executable,
            "-h",
            host,
            "-p",
            str(options.port),
            "-U",
            options.user,
            "-d",
            options.database,
            *args,
            env=env,
        )

    def sql(*statements):
        return postgres_client(
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            "VERBOSITY=verbose",
            "-At",
            *(arg for text in statements for arg in ("-c", text)),
        )

    def drop_fixture_table(name):
        statement = f"DROP TABLE IF EXISTS public.{name}"
        retirement_blocked = "ERROR:  2BP01: The table cannot be dropped"
        try:
            sql(statement)
            return
        except CommandFailure as error:
            if retirement_blocked not in error.stderr:
                raise

        try:
            sql(f"ALTER TABLE public.{name} DROP CONSTRAINT {name}_pkey")
        except CommandFailure as error:
            if (
                "ERROR:  55000: DDL declaration committed but is not ready"
                not in error.stderr
            ):
                raise

        deadline = time.monotonic() + 60
        while True:
            try:
                sql(statement)
                return
            except CommandFailure as error:
                # The drop guard proves non-admission while retirement runs.
                if (
                    retirement_blocked not in error.stderr
                    or time.monotonic() >= deadline
                ):
                    raise
            time.sleep(1)

    cleanup_errors = []
    try:
        if not options.external:
            command(
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                pg,
                "--network",
                network,
                "--label",
                "antfly.qualification=debezium",
                "-e",
                f"POSTGRES_PASSWORD={password}",
                "-e",
                f"POSTGRES_USER={options.user}",
                "-e",
                f"POSTGRES_DB={options.database}",
                POSTGRES,
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=10",
                "-c",
                f"port={options.port}",
            )
            host = command(
                "docker",
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                pg,
            )
            if not host:
                raise RuntimeError("PostgreSQL container has no network address")
            deadline = time.monotonic() + 60
            while True:
                try:
                    sql("SELECT 1")
                    break
                except RuntimeError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.25)
        columns = "(id, label, amount, flag)"
        sql(
            f"CREATE TABLE public.{table} (id integer PRIMARY KEY, label text NOT NULL, amount numeric(38,8) NOT NULL, flag boolean NOT NULL)"
        )
        sql(f"ALTER TABLE public.{table} REPLICA IDENTITY FULL")
        sql(
            f"INSERT INTO public.{table} {columns} VALUES (1, 'snapshot', 12345678901234567890.12345678, true)"
        )
        members = [f"public.{table}"]
        if second:
            sql(
                f"CREATE TABLE public.{second} (id integer PRIMARY KEY, label text NOT NULL)"
            )
            sql(
                f"INSERT INTO public.{second} (id, label) VALUES (11, 'second-snapshot')"
            )
            members.append(f"public.{second}")
        sql(f"CREATE PUBLICATION {publication} FOR TABLE {', '.join(members)}")
        config = {
            "name": "antfly-qualification",
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name": "pgoutput",
            "database.hostname": host,
            "database.port": options.port,
            "database.dbname": options.database,
            "database.user": options.user,
            "database.password": password,
            "database.sslmode": "disable",
            "topic.prefix": "qualification",
            "slot.name": slot,
            "slot.drop.on.stop": "false",
            "publication.name": publication,
            "publication.autocreate.mode": "disabled",
            "table.include.list": ",".join(members),
            "snapshot.mode": "initial",
            "decimal.handling.mode": "precise",
            "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
            "offset.storage.file.filename": "/state/offsets.dat",
            "offset.flush.interval.ms": "0",
            "record.processing.threads": "1",
            "record.processing.order": "ORDERED",
            "record.processing.with.serial.consumer": "true",
            "converter.schemas.enable": "false",
            "errors.max.retries": "0",
            "provide.transaction.metadata": "true",
        }
        (directory / "state").mkdir()
        properties = directory / "connector.properties"
        properties.write_text(
            "".join(f"{key}={property_value(value)}\n" for key, value in config.items())
        )
        properties.chmod(0o600)
        client.start()
        snapshot = client.wait(lambda event: row(event, "r", 1))
        assert snapshot["value"]["source"]["version"] == "3.3.0.Final", snapshot
        assert snapshot["value"]["source"]["connector"] == "postgresql", snapshot
        after = snapshot["value"]["after"]
        assert after["flag"] is True and after["label"] == "snapshot", snapshot
        scaled = 1234567890123456789012345678
        expected_decimal = base64.b64encode(
            scaled.to_bytes((scaled.bit_length() + 8) // 8, "big", signed=True)
        ).decode()
        assert after["amount"] == expected_decimal, snapshot
        extra_write = []
        if second:
            second_snapshot = client.wait(lambda event: row(event, "r", 11))
            assert second_snapshot["value"]["source"]["table"] == second, (
                second_snapshot
            )
            assert second_snapshot["value"]["after"]["label"] == "second-snapshot", (
                second_snapshot
            )
            extra_write = [
                f"INSERT INTO public.{second} (id, label) VALUES (12, 'second-stream')"
            ]
        for attempt in range(4):
            try:
                sql(
                    "BEGIN",
                    f"INSERT INTO public.{table} {columns} VALUES (2, 'stream', 2.5, false)",
                    f"UPDATE public.{table} SET label='updated' WHERE id=1",
                    *extra_write,
                    "COMMIT",
                )
                break
            except CommandFailure as error:
                # ON_ERROR_STOP skipped COMMIT when UPDATE returned a
                # temporary read error. Closing psql aborts the staged insert.
                if (
                    attempt == 3
                    or "ERROR:  53300:" not in error.stderr
                    or error.stdout.splitlines()
                    not in (
                        [["BEGIN", "INSERT 0 1"], ["BEGIN", "INSERT 0 1", "UPDATE 1"]]
                        if second
                        else [["BEGIN", "INSERT 0 1"]]
                    )
                ):
                    raise
                time.sleep(0.05 * (2**attempt))
        inserted = client.wait(lambda event: row(event, "c", 2))
        updated = client.wait(lambda event: row(event, "u", 1))
        assert updated["value"]["before"]["label"] == "snapshot", updated
        assert updated["value"]["before"]["amount"] == expected_decimal, updated
        assert updated["value"]["after"]["amount"] == expected_decimal, updated
        # PostgreSQL's Debezium transaction id includes each record's LSN;
        # source.txId is the stable transaction identity across row messages.
        assert inserted["value"]["source"]["txId"] == updated["value"]["source"]["txId"]
        if second:
            other = client.wait(lambda event: row(event, "c", 12))
            assert other["value"]["source"]["table"] == second, other
            assert (
                other["value"]["source"]["txId"] == inserted["value"]["source"]["txId"]
            )
        expected_rows = 3 if second else 2
        # PostgreSQL emits WAL record order. Antfly emits each transaction's
        # net row effects in durable storage-key order.
        transaction_rows = [
            event
            for event in client.events
            if row(event, "c", 2)
            or row(event, "u", 1)
            or (second and row(event, "c", 12))
        ]
        assert [
            event["value"]["transaction"]["total_order"] for event in transaction_rows
        ] == list(range(1, expected_rows + 1))
        client.wait(
            lambda event: (
                event.get("topic") == "qualification.transaction"
                and (event.get("value") or {}).get("status") == "END"
                and (event.get("value") or {}).get("event_count") == expected_rows
            )
        )
        client.stop()
        assert (directory / "state/offsets.dat").stat().st_size > 0
        resume = len(client.events)
        sql(f"INSERT INTO public.{table} {columns} VALUES (3, 'offline', 3, true)")
        client.start()
        client.wait(lambda event: row(event, "c", 3), start=resume)
        sql(f"UPDATE public.{table} SET id=10 WHERE id=1")
        client.wait(lambda event: row(event, "d", 1), start=resume)
        client.wait(lambda event: row(event, "c", 10), start=resume)
        client.wait(
            lambda event: (
                event.get("value") is None and (event.get("key") or {}).get("id") == 1
            ),
            start=resume,
        )
        assert not any(
            (event.get("value") or {}).get("op") == "r"
            for event in client.events[resume:]
        )
        sql(f"INSERT INTO public.{table} {columns} VALUES (4, 'crash', 4, false)")
        client.wait(lambda event: row(event, "c", 4), start=resume)
        client.stop(abrupt=True)
        crash_resume = len(client.events)
        sql(f"INSERT INTO public.{table} {columns} VALUES (5, 'crash-resume', 5, true)")
        client.start()
        client.wait(lambda event: row(event, "c", 5), start=crash_resume)
        assert not any(
            (event.get("value") or {}).get("op") == "r"
            for event in client.events[crash_resume:]
        )
        client.stop()
        result = {
            "client": "Debezium 3.3.0.Final",
            "target": "external" if options.external else POSTGRES,
            "tables": len(members),
            "snapshot": True,
            "stream": True,
            "numeric_precision": True,
            "graceful_resume": True,
            "primary_key_change": True,
            "primary_key_tombstone": True,
            "crash_resume": True,
            "events": len(client.events),
            "artifacts": str(directory),
        }
    finally:
        if client.process is not None:
            try:
                client.stop(abrupt=True)
            except Exception:
                subprocess.run(["docker", "rm", "-f", client_name], capture_output=True)
        if options.external:
            try:
                postgres_client("pg_recvlogical", "--drop-slot", "--slot", slot)
            except Exception as error:
                cleanup_errors.append(f"qualification slot cleanup failed: {error}")
            try:
                sql(f"DROP PUBLICATION IF EXISTS {publication}")
            except Exception as error:
                cleanup_errors.append(
                    f"qualification publication cleanup failed: {error}"
                )
            for name in (table, second) if second else (table,):
                try:
                    drop_fixture_table(name)
                    remaining = sql(
                        "SELECT count(*) FROM pg_catalog.pg_class c "
                        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                        f"WHERE n.nspname = 'public' AND c.relname = '{name}'"
                    )
                    if remaining != "0":
                        raise RuntimeError(
                            "fixture relation still exists after DROP TABLE"
                        )
                except Exception as error:
                    cleanup_errors.append(
                        f"qualification table cleanup failed for {name}: {error}"
                    )
            for error in cleanup_errors:
                print(error, file=sys.stderr)
        else:
            subprocess.run(["docker", "rm", "-f", pg], capture_output=True)
    if cleanup_errors:
        raise RuntimeError("Debezium qualification fixture cleanup failed")
    print(json.dumps(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--external",
        action="store_true",
        help="qualify an existing endpoint instead of a disposable PostgreSQL baseline",
    )
    parser.add_argument(
        "--network",
        default="bridge",
        help="existing Docker network; no daemon-wide network changes",
    )
    parser.add_argument(
        "--multi-table",
        action="store_true",
        help="qualify one transaction across two published tables",
    )
    parser.add_argument("--host", default="host.docker.internal")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="qualification")
    parser.add_argument("--user", default="qualification")
    parser.add_argument("--image", default=IMAGE)
    parser.add_argument(
        "--artifacts",
        type=Path,
        help="empty directory for retained event logs/offsets; contains credentials",
    )
    options = parser.parse_args()
    if options.external and "QUALIFICATION_DATABASE_PASSWORD" not in os.environ:
        parser.error("external qualification requires QUALIFICATION_DATABASE_PASSWORD")
    if not 1 <= options.port <= 65535:
        parser.error("port must be between 1 and 65535")
    if options.artifacts:
        options.artifacts.mkdir(parents=True, exist_ok=True)
        if any(options.artifacts.iterdir()):
            parser.error("artifact directory must be empty")
        options.artifacts.chmod(0o700)
        run(options, options.artifacts.resolve())
    else:
        (ROOT / ".local").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="qualification-", dir=ROOT / ".local"
        ) as temporary:
            run(options, Path(temporary).resolve())
