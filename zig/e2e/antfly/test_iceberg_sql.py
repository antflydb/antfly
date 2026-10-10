# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

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
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
)

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
        path.write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
            + payload
        )

    avro_files = []
    exported_lengths = {}
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        payload = path.read_bytes()
        if path.suffix == ".avro":
            reader = fastavro.reader(io.BytesIO(payload))
            records = remote(list(reader))
            avro_files.append(
                (
                    path,
                    reader.writer_schema,
                    reader.codec,
                    {
                        k: v
                        for k, v in reader.metadata.items()
                        if not k.startswith("avro.")
                    },
                    records,
                )
            )
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
    put(
        "metadata/v1.metadata.json",
        json.dumps(remote(json.loads(latest.read_bytes()))).encode(),
    )
    put("metadata/version-hint.text", b"1\n")


def test_real_iceberg_snapshots_schema_ids_partitions_deletes_and_restart(tmp_path):
    # e2e-full installs these extras. Missing dependencies must fail that suite.
    if not os.environ.get("ANTFLY_E2E_FULL_LAKE"):
        pytest.importorskip("pyiceberg")
        pytest.importorskip("fastavro")
        pytest.importorskip("pyarrow")
        pytest.importorskip("psycopg")
    import psycopg
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    catalog = SqlCatalog(
        "e2e",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    catalog.create_namespace("e2e")
    table = catalog.create_table(
        "e2e.events",
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "label", StringType(), required=False),
            NestedField(3, "region", StringType(), required=False),
        ),
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=3, field_id=1000, transform=IdentityTransform(), name="region"
            )
        ),
    )
    table.append(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 6], type=pa.int64()),
                "label": ["one", "two", None, "six"],
                "region": ["west", "east", "west", "east"],
            }
        )
    )
    first_snapshot = table.current_snapshot().snapshot_id
    # Old files still contain the physical name label and stable field ID 2.
    with table.update_schema() as update:
        update.rename_column("label", "title")
    table.append(
        pa.table(
            {
                "id": pa.array([4, 5], type=pa.int64()),
                "title": ["four", "five"],
                "region": ["east", "west"],
            }
        )
    )
    # A second east row forces a real copy-on-write file rewrite.
    table.delete("id = 2")
    assert sorted(row["id"] for row in table.scan().to_arrow().to_pylist()) == [
        1,
        3,
        4,
        5,
        6,
    ]
    root = tmp_path / "lake"
    _export_table(table, root)
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    assert Path(binary).exists(), f"Antfly binary missing: {binary}"
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, pgwire=True)
    failed = True
    try:

        def request(path, body):
            response = requests.post(
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            assert response.ok, response.text + "\n" + server.debug_logs()
            return response.json()

        for name, snapshot in (("ice_events", None), ("ice_pinned", first_snapshot)):
            source = {
                "kind": "external",
                "table_id": name,
                "format": "iceberg",
                "uri": root.as_uri(),
            }
            if snapshot is not None:
                source["snapshot"] = {"mode": "snapshot_id", "id": str(snapshot)}
            request(
                f"/tables/{name}",
                {
                    "num_shards": 1,
                    "schema": {"storage_mode": "relational", "base_source": source},
                },
            )

        def sql(statement):
            return request("/sql", {"statement": statement})["rows"]

        deadline = time.monotonic() + 30
        while True:
            response = requests.post(
                server.api_url + "/sql",
                json={"statement": "SELECT COUNT(*) FROM ice_events"},
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            if response.ok:
                assert response.json()["rows"] == [["5"]]
                break
            assert response.status_code in (404, 409, 503), (
                response.text + server.debug_logs()
            )
            assert time.monotonic() < deadline, response.text + server.debug_logs()
            time.sleep(0.1)

        for cold in (False, True):
            if cold:
                server.restart()
            assert sql("SELECT id, title FROM ice_events ORDER BY id") == [
                ["1", "one"],
                ["3", None],
                ["4", "four"],
                ["5", "five"],
                ["6", "six"],
            ]
            assert sql(
                "SELECT COUNT(*), SUM(id) FROM ice_events WHERE region = 'west'"
            ) == [["3", "9"]]
            assert sql("SELECT id, label FROM ice_pinned ORDER BY id") == [
                ["1", "one"],
                ["2", "two"],
                ["3", None],
                ["6", "six"],
            ]
            with psycopg.connect(
                host="127.0.0.1",
                port=server.pgwire_port,
                user="admin",
                password=AUTH_BOOTSTRAP_PASSWORD,
                dbname="default",
                autocommit=True,
            ) as connection:
                assert connection.execute(
                    "SELECT id, title FROM ice_events WHERE region = 'east' ORDER BY id"
                ).fetchall() == [(4, "four"), (6, "six")]
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize("source_format", ["parquet", "iceberg"])
def test_indexed_metadata_conjunctions_preserve_sort_and_cursor_pages(
    tmp_path, source_format
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    count = 10003
    rows = pa.table(
        {
            "body": ["common"] * count,
            "category": ["story"] * 2 + ["comment"] * (count - 2),
            "amount": range(count),
            "sparse_native": ['{"1":1}'] * (count - 1) + ['{"1":1,"97":1}'],
            "dense_native": ["[1,0]"] * count,
            "label": ["other"] * (count - 3) + ["kept"] * 3,
        }
    )
    root = tmp_path / source_format
    if source_format == "iceberg":
        pytest.importorskip("fastavro")
        pytest.importorskip("pyiceberg")
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        catalog = SqlCatalog(
            "sort_pages",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=(tmp_path / "warehouse").as_uri(),
        )
        catalog.create_namespace("sort_pages")
        table = catalog.create_table(
            "sort_pages.items",
            schema=Schema(
                NestedField(1, "body", StringType(), required=False),
                NestedField(2, "category", StringType(), required=False),
                NestedField(3, "amount", LongType(), required=False),
                NestedField(4, "label", StringType(), required=False),
                NestedField(5, "sparse_native", StringType(), required=False),
                NestedField(6, "dense_native", StringType(), required=False),
            ),
        )
        table.append(rows)
        _export_table(table, root)
    else:
        objects = root / "buckets" / "antfly" / "objects"
        objects.mkdir(parents=True)
        parquet = tmp_path / "items.parquet"
        pq.write_table(rows, parquet, row_group_size=4096, compression="snappy")
        payload = parquet.read_bytes()
        (objects / "part.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
            + payload
        )
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:

        def call(method, path, body=None):
            response = requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            assert response.ok, response.text + "\n" + server.debug_logs()
            value = response.json()
            return value["responses"][0] if "responses" in value else value

        call(
            "POST",
            "/tables/sort_pages",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "sort-pages",
                        "format": source_format,
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {"name": "category_idx", "keys": [{"column": "category"}]},
                        {"name": "amount_idx", "keys": [{"column": "amount"}]},
                    ],
                },
                "indexes": {
                    "body_text": {"type": "full_text", "field": "body"},
                    "sparse_native": {
                        "type": "embeddings",
                        "external": True,
                        "sparse": True,
                    },
                    "dense_native": {
                        "type": "embeddings",
                        "external": True,
                        "dimension": 2,
                    },
                },
            },
        )
        deadline = time.monotonic() + 300
        while True:
            resource = call("GET", "/tables/sort_pages/indexes/body_text")
            sparse_resource = call("GET", "/tables/sort_pages/indexes/sparse_native")
            dense_resource = call("GET", "/tables/sort_pages/indexes/dense_native")
            if (
                resource["status"]["readiness"]["queryable"]
                and sparse_resource["status"]["readiness"]["queryable"]
                and dense_resource["status"]["readiness"]["queryable"]
            ):
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        category = {"term": {"path": "/category", "value": "comment"}}
        predicates = [
            {
                "bool": {
                    "filter": [
                        category,
                        {"range": {"path": "/amount", "gte": count - 3}},
                    ]
                }
            },
            {"conjuncts": [category, {"prefix": {"path": "/label", "value": "ke"}}]},
        ]
        expected = set(range(count - 3, count))
        for restart in (False, True):
            if restart:
                server.restart()
            # A rare posting can probe a broad indexed physical predicate;
            # a common posting must cross the budget into full membership.
            sparse_base = {
                "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
                "indexes": ["sparse_native"],
                "fields": ["amount"],
                "limit": 10,
            }
            rare = call(
                "POST",
                "/tables/sort_pages/query",
                dict(
                    sparse_base,
                    embeddings={"sparse_native": {"indices": [97], "values": [1]}},
                    filter_query=category,
                ),
            )
            assert [
                (h["_source"]["amount"], h["_score"]) for h in rare["hits"]["hits"]
            ] == [(count - 1, 1)]
            common = call(
                "POST",
                "/tables/sort_pages/query",
                dict(
                    sparse_base,
                    filter_query={"range": {"path": "/amount", "gte": count // 2}},
                ),
            )
            assert len(common["hits"]["hits"]) == 10
            assert all(
                h["_source"]["amount"] >= count // 2 and h["_score"] == 1
                for h in common["hits"]["hits"]
            )
            excluded = call(
                "POST",
                "/tables/sort_pages/query",
                dict(
                    sparse_base,
                    exclusion_query=category,
                ),
            )
            assert {h["_source"]["amount"] for h in excluded["hits"]["hits"]} == {0, 1}
            dense_base = dict(
                sparse_base,
                embeddings={"dense_native": [1, 0]},
                indexes=["dense_native"],
            )
            dense = call(
                "POST",
                "/tables/sort_pages/query",
                dict(
                    dense_base,
                    filter_query={"range": {"path": "/amount", "gte": count // 2}},
                ),
            )
            assert len(dense["hits"]["hits"]) == 10
            assert all(
                h["_source"]["amount"] >= count // 2 for h in dense["hits"]["hits"]
            )
            hybrid = call(
                "POST",
                "/tables/sort_pages/query",
                dict(
                    sparse_base,
                    embeddings={
                        "dense_native": [1, 0],
                        "sparse_native": {"indices": [97], "values": [1]},
                    },
                    indexes=["dense_native", "sparse_native"],
                    filter_query=category,
                ),
            )
            assert any(
                h["_source"]["amount"] == count - 1 for h in hybrid["hits"]["hits"]
            )
            assert all(h["_source"]["amount"] >= 2 for h in hybrid["hits"]["hits"])
            for predicate in predicates:
                request = {
                    "full_text_search": {"term": "common", "field": "body"},
                    "fields": ["amount"],
                    "filter_query": predicate,
                    "limit": 10,
                }
                baseline = call("POST", "/tables/sort_pages/query", request)
                assert {
                    h["_source"]["amount"] for h in baseline["hits"]["hits"]
                } == expected
                for sort in ("_score", "_id"):
                    ordered = dict(
                        request, order_by=[{"field": sort, "desc": sort != "_id"}]
                    )
                    reference = call("POST", "/tables/sort_pages/query", ordered)
                    assert reference["hits"]["total"] == {
                        "value": 3,
                        "relation": "exact",
                    }
                    hits = reference["hits"]["hits"]
                    assert {h["_source"]["amount"] for h in hits} == expected
                    first = call(
                        "POST", "/tables/sort_pages/query", dict(ordered, limit=1)
                    )
                    assert first["hits"]["hits"] == hits[:1]
                    continuation = dict(
                        ordered,
                        limit=1,
                        remote_snapshot=first["remote_snapshot"],
                    )
                    # A cursor with no explicit order_by has implicit ID order.
                    if sort == "_id":
                        continuation.pop("order_by")
                    after = call(
                        "POST",
                        "/tables/sort_pages/query",
                        dict(
                            continuation, search_after=first["hits"]["hits"][0]["_sort"]
                        ),
                    )
                    assert after["hits"]["hits"] == hits[1:2]
                    before = call(
                        "POST",
                        "/tables/sort_pages/query",
                        dict(
                            continuation,
                            search_before=after["hits"]["hits"][0]["_sort"],
                        ),
                    )
                    assert before["hits"]["hits"] == hits[:1]
        failed = False
    finally:
        server.stop(test_failed=failed)
