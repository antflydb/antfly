# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Real PyIceberg commits through public SQL attachment and cold restart.

Included in e2e-full. Run locally with --extra lake --extra iceberg.
"""
import hashlib
import io
import json
import os
import struct
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest
import requests
from conftest import AUTH_BOOTSTRAP_PASSWORD, DEFAULT_ANTFLY_BIN, StandaloneAntflyServer, resolve_binary_path

pytestmark = [pytest.mark.iceberg_integration, pytest.mark.fresh_antfly_process]


def _export_table(table, root):
    """Adapt the writer's local warehouse to the filesystem object-store URI.

    Avro schemas, field IDs, partition records, and Parquet bytes come from
    independent writers. Only storage addresses change; Antfly writes no data.
    """
    import fastavro

    location = table.location().rstrip("/")
    directory = Path(unquote(urlparse(location).path))
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)

    def remote(value):
        if isinstance(value, str):
            return value.replace(location, "object://antfly")
        if isinstance(value, list):
            return [remote(item) for item in value]
        if isinstance(value, dict):
            return {key: remote(item) for key, item in value.items()}
        return value

    def put(key, payload):
        path = objects / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"AFOBJ001" + struct.pack("<QI", len(payload), 0)
                         + hashlib.sha256(payload).hexdigest().encode() + payload)

    avro_files = []
    exported_lengths = {}
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        payload = path.read_bytes()
        if path.suffix == ".avro":
            reader = fastavro.reader(io.BytesIO(payload))
            records = remote(list(reader))
            avro_files.append((path, reader.writer_schema, reader.codec,
                               {k: v for k, v in reader.metadata.items() if not k.startswith("avro.")}, records))
            continue
        elif path.name.endswith(".metadata.json"):
            payload = json.dumps(remote(json.loads(payload))).encode()
        put(path.relative_to(directory).as_posix(), payload)
    # Manifest-list lengths describe the rewritten Avro payloads exactly.
    avro_files.sort(key=lambda entry: bool(entry[4] and "manifest_path" in entry[4][0]))
    for path, schema, codec, metadata, records in avro_files:
        for record in records:
            if "manifest_path" in record:
                record["manifest_length"] = exported_lengths[record["manifest_path"]]
        out = io.BytesIO()
        fastavro.writer(out, schema, records, codec=codec, metadata=metadata)
        payload = out.getvalue()
        key = path.relative_to(directory).as_posix()
        exported_lengths["object://antfly/" + key] = len(payload)
        put(key, payload)
    latest = Path(unquote(urlparse(table.metadata_location).path))
    put("metadata/v1.metadata.json", json.dumps(remote(json.loads(latest.read_bytes()))).encode())
    put("metadata/version-hint.text", b"1\n")


def test_real_iceberg_snapshots_schema_ids_partitions_deletes_and_restart(tmp_path):
    # e2e-full installs these extras. Missing dependencies must fail that suite.
    if not os.environ.get("ANTFLY_E2E_FULL_LAKE"):
        pytest.importorskip("pyiceberg")
        pytest.importorskip("fastavro")
        pytest.importorskip("pyarrow")
        pytest.importorskip("psycopg")
    import pyarrow as pa
    import psycopg
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    catalog = SqlCatalog("e2e", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("e2e")
    table = catalog.create_table("e2e.events", schema=Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "label", StringType(), required=False),
        NestedField(3, "region", StringType(), required=False),
    ), partition_spec=PartitionSpec(PartitionField(
        source_id=3, field_id=1000, transform=IdentityTransform(), name="region")))
    table.append(pa.table({"id": pa.array([1, 2, 3], type=pa.int64()),
                           "label": ["one", "two", None], "region": ["west", "east", "west"]}))
    first_snapshot = table.current_snapshot().snapshot_id
    # Old files still contain the physical name label and stable field ID 2.
    with table.update_schema() as update:
        update.rename_column("label", "title")
    table.append(pa.table({"id": pa.array([4, 5], type=pa.int64()),
                           "title": ["four", "five"], "region": ["east", "west"]}))
    table.delete("id = 2")
    assert sorted(row["id"] for row in table.scan().to_arrow().to_pylist()) == [1, 3, 4, 5]
    root = tmp_path / "lake"
    _export_table(table, root)
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    assert Path(binary).exists(), f"Antfly binary missing: {binary}"
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, pgwire=True)
    failed = True
    try:
        def request(path, body):
            response = requests.post(server.api_url + path, json=body,
                                     auth=("admin", AUTH_BOOTSTRAP_PASSWORD), timeout=60)
            assert response.ok, response.text + "\n" + server.debug_logs()
            return response.json()

        for name, snapshot in (("ice_events", None), ("ice_pinned", first_snapshot)):
            source = {"kind": "external", "table_id": name, "format": "iceberg", "uri": root.as_uri()}
            if snapshot is not None:
                source["snapshot"] = {"mode": "snapshot_id", "id": str(snapshot)}
            request(f"/tables/{name}", {"num_shards": 1, "schema": {
                "storage_mode": "relational", "base_source": source}})

        def sql(statement):
            return request("/sql", {"statement": statement})["rows"]

        deadline = time.monotonic() + 30
        while True:
            response = requests.post(server.api_url + "/sql", json={"statement": "SELECT COUNT(*) FROM ice_events"},
                                     auth=("admin", AUTH_BOOTSTRAP_PASSWORD), timeout=60)
            if response.ok:
                assert response.json()["rows"] == [["4"]]
                break
            assert response.status_code in (404, 409, 503), response.text + server.debug_logs()
            assert time.monotonic() < deadline, response.text + server.debug_logs()
            time.sleep(0.1)

        for cold in (False, True):
            if cold:
                server.restart()
            assert sql("SELECT id, title FROM ice_events ORDER BY id") == [["1", "one"], ["3", None], ["4", "four"], ["5", "five"]]
            assert sql("SELECT COUNT(*), SUM(id) FROM ice_events WHERE region = 'west'") == [["3", "9"]]
            assert sql("SELECT id, label FROM ice_pinned ORDER BY id") == [["1", "one"], ["2", "two"], ["3", None]]
            with psycopg.connect(host="127.0.0.1", port=server.pgwire_port, user="admin",
                                 password=AUTH_BOOTSTRAP_PASSWORD, dbname="antfly", autocommit=True) as connection:
                assert connection.execute("SELECT id, title FROM ice_events WHERE region = 'east'").fetchall() == [(4, "four")]
        failed = False
    finally:
        server.stop(test_failed=failed)
