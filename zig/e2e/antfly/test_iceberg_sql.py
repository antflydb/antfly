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
import shutil
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
    objects.mkdir(parents=True, exist_ok=True)

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
                    "indexes": {
                        "region_stats": {
                            "type": "algebraic",
                            "derive_from_schema": True,
                            "aggregates": [
                                {"name": "rows", "op": "count", "group_by": ["region"]},
                                {
                                    "name": "total",
                                    "op": "sum",
                                    "measure": "id",
                                    "group_by": ["region"],
                                },
                            ],
                        }
                    }
                    if name == "ice_events"
                    else {},
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

        deadline = time.monotonic() + 60
        while True:
            response = requests.get(
                server.api_url + "/tables/ice_events/indexes/region_stats",
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            assert response.ok, response.text + server.debug_logs()
            resource = response.json()
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
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
            assert sql(
                "SELECT id, title FROM ice_events WHERE id >= 3 AND id < 6 "
                "AND region = 'west' ORDER BY id"
            ) == [["3", None], ["5", "five"]]
            assert sql(
                "SELECT region, SUM(id) FILTER (WHERE region='west'), "
                "COUNT(*) FILTER (WHERE region='east') FROM ice_events "
                "GROUP BY region ORDER BY region"
            ) == [["east", None, "2"], ["west", "9", "0"]]
            assert sql(
                "SELECT region, SUM(id), COUNT(*) FILTER (WHERE region='east') "
                "FROM ice_events WHERE region='west' GROUP BY region"
            ) == [["west", "9", "0"]]
            assert sql(
                "SELECT l.region, COUNT(*), SUM(l.id) FROM ice_events l "
                "JOIN (VALUES ('west'),('west'),('east')) r(region) "
                "ON l.region=r.region GROUP BY l.region ORDER BY l.region"
            ) == [["east", "2", "10"], ["west", "6", "18"]]
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
    # Exceed the 64 native directory-block planning budget so completed broad
    # memberships exercise lazy ordinal windows in both real source formats.
    count = 70003
    rows = pa.table(
        {
            "body": [
                "common phrase" if i % 2 == 0 else "phrase common" for i in range(count)
            ],
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
                        "object_mutability": "immutable",
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
            # Positional verification composes with a Boolean scorer and
            # indexed metadata candidate providers, including exclusions.
            phrase_filter = {
                "conjuncts": [
                    category,
                    {"range": {"path": "/amount", "gte": count - 5}},
                ]
            }
            phrase_query = {
                "conjuncts": [
                    {"match_phrase": "common phrase", "field": "body"},
                    {"term": "common", "field": "body"},
                ]
            }
            phrase_request = {
                "full_text_search": phrase_query,
                "filter_query": phrase_filter,
                "fields": ["amount"],
                "limit": 10,
            }
            phrase_result = call("POST", "/tables/sort_pages/query", phrase_request)
            assert {h["_source"]["amount"] for h in phrase_result["hits"]["hits"]} == {
                i for i in range(count - 5, count) if i % 2 == 0
            }
            phrase_excluded = call(
                "POST",
                "/tables/sort_pages/query",
                dict(
                    phrase_request,
                    exclusion_query={"term": {"path": "/amount", "value": count - 3}},
                ),
            )
            assert {
                h["_source"]["amount"] for h in phrase_excluded["hits"]["hits"]
            } == {i for i in range(count - 5, count) if i % 2 == 0 and i != count - 3}
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
            # Cold/warm exact ID selections must retain sparse seeks even
            # when loading the index metadata costs more than one point probe.
            for _ in range(2):
                point = call(
                    "POST",
                    "/tables/sort_pages/query",
                    dict(
                        sparse_base,
                        filter_query={
                            "conjuncts": [
                                category,
                                {"term": {"path": "/amount", "value": count - 1}},
                            ]
                        },
                    ),
                )
                assert [
                    (h["_source"]["amount"], h["_score"]) for h in point["hits"]["hits"]
                ] == [(count - 1, 1)]
            # Two independent indexes enforce the entire conjunction through
            # shared candidate membership, for rare and common sparse postings.
            multi = {
                "conjuncts": [
                    category,
                    {"range": {"path": "/amount", "gte": count // 2}},
                ]
            }
            for dimension in (97, 1):
                combined = call(
                    "POST",
                    "/tables/sort_pages/query",
                    dict(
                        sparse_base,
                        embeddings={
                            "sparse_native": {"indices": [dimension], "values": [1]}
                        },
                        filter_query=multi,
                    ),
                )
                assert len(combined["hits"]["hits"]) == (1 if dimension == 97 else 10)
                assert all(
                    h["_source"]["amount"] >= count // 2 and h["_score"] == 1
                    for h in combined["hits"]["hits"]
                )
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
            transitioned = call(
                "POST",
                "/tables/sort_pages/query",
                dict(
                    sparse_base,
                    filter_query={
                        "conjuncts": [
                            category,
                            {"range": {"path": "/amount", "lte": 4096}},
                        ]
                    },
                ),
            )
            assert len(transitioned["hits"]["hits"]) == 10
            assert all(
                2 <= h["_source"]["amount"] <= 4096
                for h in transitioned["hits"]["hits"]
            )
            multi_excluded = call(
                "POST",
                "/tables/sort_pages/query",
                dict(sparse_base, exclusion_query=multi),
            )
            assert len(multi_excluded["hits"]["hits"]) == 10
            assert all(
                h["_source"]["amount"] < count // 2
                for h in multi_excluded["hits"]["hits"]
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
            for weight in (-1, 0):
                signed_excluded = call(
                    "POST",
                    "/tables/sort_pages/query",
                    dict(
                        sparse_base,
                        embeddings={
                            "sparse_native": {"indices": [1], "values": [weight]}
                        },
                        exclusion_query=category,
                    ),
                )
                assert {
                    (h["_source"]["amount"], h["_score"])
                    for h in signed_excluded["hits"]["hits"]
                } == {(0, weight), (1, weight)}
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


@pytest.mark.parametrize("source_format", ["parquet", "iceberg"])
@pytest.mark.parametrize("layout", ["files", "segments"])
def test_fragmented_text_hierarchy_preserves_boolean_scores_and_restart(
    tmp_path, source_format, layout
):
    """Exercise file pruning and precise native segments inside one real file."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / source_format
    # Four eight-way tiers leave 64-, 8-, and 1-file packs for 73 tiny files.
    # Rare matches occupy only two physical files, guaranteeing an unrelated
    # serving descriptor regardless of immutable file-hash order.
    parts, rows = (73, 64) if layout == "files" else (1, 196609)
    expected = (
        {3 * 64, 3 * 64 + 1, 31 * 64} if layout == "files" else {0, 65536, 196608}
    )
    batches = [
        pa.table(
            {
                "amount": range(part * rows, (part + 1) * rows),
                "body": [
                    "common phrase quasar"
                    if part * rows + row in expected
                    else "common phrase"
                    for row in range(rows)
                ],
            }
        )
        for part in range(parts)
    ]
    if source_format == "iceberg":
        pytest.importorskip("fastavro")
        pytest.importorskip("pyiceberg")
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        catalog = SqlCatalog(
            "fragmented",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=(tmp_path / "warehouse").as_uri(),
        )
        catalog.create_namespace("fragmented")
        table = catalog.create_table(
            "fragmented.items",
            schema=Schema(
                NestedField(1, "amount", LongType(), required=False),
                NestedField(2, "body", StringType(), required=False),
            ),
        )
        for batch in batches:
            table.append(batch)
        assert len(table.inspect.files()) == parts
        _export_table(table, root)
    else:
        objects = root / "buckets" / "antfly" / "objects"
        objects.mkdir(parents=True)
        for ordinal, batch in enumerate(batches):
            output = io.BytesIO()
            pq.write_table(batch, output, compression="snappy")
            payload = output.getvalue()
            (objects / f"part-{ordinal:02}.parquet").write_bytes(
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
            "/tables/fragmented",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "fragmented",
                        "format": source_format,
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {"name": "amount_idx", "keys": [{"column": "amount"}]},
                    ],
                },
                "indexes": {"body_text": {"type": "full_text", "field": "body"}},
            },
        )
        deadline = time.monotonic() + 300
        while True:
            resource = call("GET", "/tables/fragmented/indexes/body_text")
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)

        def query(text):
            return call(
                "POST",
                "/tables/fragmented/query",
                {"full_text_search": text, "fields": ["amount"], "limit": 10},
            )["hits"]["hits"]

        def metadata_query(amount):
            result = call(
                "POST",
                "/tables/fragmented/query",
                {
                    "full_text_search": {"term": "common", "field": "body"},
                    "filter_query": {
                        "conjuncts": [
                            {"term": {"path": "/amount", "value": amount}},
                            # An OR is residual membership, never an admission
                            # proof; the surrounding required equality is safe.
                            {
                                "disjuncts": [
                                    {"term": {"path": "/amount", "value": amount}},
                                    {"term": {"path": "/amount", "value": -1}},
                                ]
                            },
                        ]
                    },
                    "fields": ["amount"],
                    "limit": 20,
                    "profile": True,
                    "highlight": {"fields": ["body"]},
                },
            )
            assert result["profile"]["sort"]["selection_reason"] == (
                "indexed_predicate_text_admission"
            ), result
            hits = result["hits"]["hits"]
            assert [hit["_source"]["amount"] for hit in hits] == [amount]
            assert hits[0]["_highlights"]["body"]
            assert "body" not in hits[0]["_source"]
            return hits

        rare = {"term": "quasar", "field": "body"}
        nested = {
            "conjuncts": [
                {"term": "common", "field": "body"},
                {"disjuncts": [rare, {"term": "missing", "field": "body"}]},
            ]
        }
        phrase = {
            "conjuncts": [
                {"match_phrase": "common phrase", "field": "body"},
                rare,
            ]
        }
        # An absent term may fit the native summary key while exceeding only
        # the optional escaped directory key. Exact refinement must fall back.
        large = call(
            "POST",
            "/tables/fragmented/query",
            {
                "full_text_search": {
                    "disjuncts": [rare, {"term": "x" * 262112, "field": "body"}]
                },
                "fields": ["amount"],
                "limit": 10,
            },
        )["hits"]["hits"]
        assert large == query(rare)
        metadata_baseline = {amount: metadata_query(amount) for amount in expected}
        baseline = None
        for restart in (False, False, True):
            if restart:
                server.restart()
            for amount in expected:
                assert metadata_query(amount) == metadata_baseline[amount]
            for text in (nested, phrase):
                hits = query(text)
                assert {hit["_source"]["amount"] for hit in hits} == expected
                assert all(hit["_score"] > 0 for hit in hits)
            hits = query(nested)
            filtered = call(
                "POST",
                "/tables/fragmented/query",
                {
                    "full_text_search": nested,
                    "fields": ["amount"],
                    "limit": 10,
                    "filter_query": {
                        "range": {"path": "/amount", "gte": max(expected)}
                    },
                },
            )["hits"]["hits"]
            assert [hit["_source"]["amount"] for hit in filtered] == [max(expected)]
            highlighted = call(
                "POST",
                "/tables/fragmented/query",
                {
                    "full_text_search": nested,
                    "fields": ["amount"],
                    "limit": 10,
                    "highlight": {"fields": ["body"]},
                },
            )["hits"]["hits"]
            assert {hit["_source"]["amount"] for hit in highlighted} == expected
            assert all(
                hit["_highlights"]["body"] and "body" not in hit["_source"]
                for hit in highlighted
            )
            top = call(
                "POST",
                "/tables/fragmented/query",
                {
                    "full_text_search": nested,
                    "fields": ["amount"],
                    "limit": 1,
                },
            )["hits"]["hits"]
            assert top == hits[:1]
            # Unknown/broad optional sets and prohibitions must retain the full
            # inventory, even when the positive rare term has exact candidates.
            broad = query({"disjuncts": [{"term": "common", "field": "body"}, rare]})
            assert len(broad) == 10
            assert expected <= {h["_source"]["amount"] for h in broad}
            excluded = query({"must_not": {"disjuncts": [rare]}})
            assert len(excluded) == 10
            assert not (expected & {h["_source"]["amount"] for h in excluded})
            if baseline is None:
                baseline = hits
            else:
                assert hits == baseline
            for boost in (0, -1):
                signed = query({"conjuncts": [dict(rare, boost=boost)]})
                assert {hit["_source"]["amount"] for hit in signed} == expected
                assert all(
                    hit["_score"] == 0 if boost == 0 else hit["_score"] < 0
                    for hit in signed
                )
        if layout == "files":
            pages = {}
            objects = {}
            documents = {}
            roots = {}
            for path in (server.root / "artifacts").rglob("*"):
                if not path.is_file():
                    continue
                payload = path.read_bytes()
                if not payload.startswith(b"AFOBJ001"):
                    continue
                checksum = payload[20:84].decode("ascii")
                if path.parts[-4] == "graph":
                    domain, attempt, stored_checksum = path.parts[-3:]
                    assert stored_checksum == checksum
                    artifact_id = f"sha256:{checksum}:graph:{domain}:{attempt}"
                else:
                    artifact_id = f"sha256:{checksum}"
                payload = payload[84 + struct.unpack_from("<I", payload, 16)[0] :]
                objects[artifact_id] = path
                begin = payload.find(b"AFGPT003")
                if begin >= 0:
                    page = payload[begin:]
                    pages[hashlib.sha256(page).digest()] = (path, page)
                begin = payload.find(b'{"version":')
                if begin >= 0:
                    try:
                        document = json.loads(payload[begin:])
                    except (ValueError, UnicodeDecodeError):
                        continue
                    documents[artifact_id] = document
                    if document.get("inventory_version") == 3:
                        roots[artifact_id] = document
            canonical = [
                document
                for document in roots.values()
                if document["file_count"] == parts
                and (document.get("serving") or document.get("compacted"))
            ]
            assert len(canonical) == 1, canonical
            serving = canonical[0].get("serving") or canonical[0]["compacted"]
            root_document = documents[serving["artifact_id"]]
            assert all(
                root_document.get(key) is None
                for key in ("serving_inventory", "serving_routes", "segment_files")
            )

            def leaves(ref):
                path, page = pages[bytes(ref["digest"])]
                count = struct.unpack_from("<I", page, 12)[0]
                offset = 56 + struct.unpack_from("<H", page, 10)[0]
                records = []
                for _ in range(count):
                    key_size, value_size = struct.unpack_from("<II", page, offset)
                    offset += 8
                    key = page[offset : offset + key_size]
                    offset += key_size
                    value = page[offset : offset + value_size]
                    offset += value_size
                    records.append((key, value))
                if ref["height"]:
                    for _, value in records:
                        yield from leaves(
                            {"digest": list(value[:32]), "height": value[36]}
                        )
                else:
                    yield path, records

            # A rare query must never admit unrelated native descriptors.
            # Packed routing can fit one shared leaf; descriptor payloads still
            # provide independently removable evidence of selective admission.
            # Restart clears in-memory admission and decoded page caches first.
            required = {
                key[-32:]
                for _, records in leaves(root_document["term_directory"]["leaves"])
                for key, _ in records
                if b"quasar" in key
            }
            assert 1 <= len(required) <= 3
            segments = {}
            for _, records in leaves(root_document["directories"]["segments"]):
                for key, value in records:
                    segments.setdefault(key[:32], []).append(
                        (struct.unpack_from(">I", key, 32)[0], value)
                    )
            unrelated = []
            for identity, chunks in segments.items():
                if identity in required:
                    continue
                record = json.loads(b"".join(value for _, value in sorted(chunks)))
                unrelated.append(objects[record["route"]["descriptor"]["artifact_id"]])
            assert unrelated, "Fixture must have an unrelated serving descriptor"
            server.pause()
            for cache_root in server.root.rglob("lake-ranges"):
                if cache_root.is_dir():
                    shutil.rmtree(cache_root)
            originals = [(path, path.read_bytes()) for path in unrelated]
            try:
                for path, _ in originals:
                    path.unlink()
                server.resume()
                assert query(nested) == baseline
                # A common text term with a one-row metadata filter must never
                # admit the deleted unrelated packs, even when K is unfilled.
                for amount in expected:
                    assert metadata_query(amount) == metadata_baseline[amount]
            finally:
                server.pause()
                for path, payload in originals:
                    path.write_bytes(payload)
                server.resume()
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize("vector_kind", [None, "dense", "sparse", "hybrid"])
def test_native_iceberg_accepted_text_windows_and_retained_cut_restart(
    tmp_path, monkeypatch, vector_kind
):
    """Real independent files, native CDC visibility and a durable search cut."""
    if not os.environ.get("ANTFLY_E2E_FULL_LAKE"):
        pytest.importorskip("pyiceberg")
        pytest.importorskip("fastavro")
        pytest.importorskip("pyarrow")
    import conftest
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
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
            NestedField(2, "body", StringType(), required=False),
            *(
                [
                    NestedField(3, "dense_native", StringType(), required=False),
                    NestedField(4, "sparse_native", StringType(), required=False),
                ]
                if vector_kind
                else []
            ),
        ),
    )
    file_count = 131
    row_count = file_count * 3
    for part in range(file_count):
        table.append(
            pa.table(
                {
                    "id": pa.array(range(part * 3, part * 3 + 3), type=pa.int64()),
                    "body": [
                        "quasar " * (i % 4 + 1) for i in range(part * 3, part * 3 + 3)
                    ],
                    **(
                        {
                            "dense_native": ["[1,0]"] * 3,
                            "sparse_native": ['{"1":1}'] * 3,
                        }
                        if vector_kind
                        else {}
                    ),
                }
            )
        )
    lake = tmp_path / "lake"
    _export_table(table, lake)
    payload = (lake / "buckets/antfly/objects/metadata/v1.metadata.json").read_bytes()
    metadata = json.loads(payload[84:])
    snapshot = next(
        s
        for s in metadata["snapshots"]
        if s["snapshot-id"] == metadata["current-snapshot-id"]
    )
    snapshot.pop("parent-snapshot-id", None)
    original_config = conftest._write_remote_content_e2e_config

    def configured(root, **kwargs):
        path = original_config(root, **kwargs)
        config = json.loads(path.read_text())
        config["deployment_mode"] = "standalone"
        config["storage"] = {"engine": "local", "local": {"base_dir": str(root)}}
        config.setdefault("lake_indexes", {}).setdefault("text_cache", {})[
            "admission_window_segments"
        ] = 1
        path.write_text(json.dumps(config))
        return path

    monkeypatch.setattr(conftest, "_write_remote_content_e2e_config", configured)
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
                timeout=120,
            )
            assert response.ok, response.text + "\n" + server.debug_logs()
            value = response.json()
            return value["responses"][0] if "responses" in value else value

        call(
            "POST",
            "/tables/wal_windows",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "id": {
                                        "type": "integer",
                                        "x-antfly-field": {
                                            "type": "number",
                                            "sortable": True,
                                        },
                                    },
                                    "body": {
                                        "type": "string",
                                        "x-antfly-field": {"type": "text"},
                                    },
                                    **(
                                        {
                                            "dense_native": {"type": "string"},
                                            "sparse_native": {"type": "string"},
                                        }
                                        if vector_kind
                                        else {}
                                    ),
                                },
                            }
                        }
                    },
                    "relational_indexes": [
                        {"name": "id_lookup", "keys": [{"column": "id"}]}
                    ],
                    "base_source": {
                        "kind": "external",
                        "format": "iceberg",
                        "uri": lake.as_uri(),
                        "table_id": "events",
                        "schema_fingerprint": "auto",
                        "write_policy": "iceberg_writer",
                        "catalog": {"type": "managed"},
                    },
                },
                "indexes": {
                    "body_text": {
                        "type": "full_text",
                        "field": "body",
                        "store_source": True,
                    },
                    **(
                        {
                            "dense_native": {
                                "type": "embeddings",
                                "external": True,
                                "dimension": 2,
                            },
                            "sparse_native": {
                                "type": "embeddings",
                                "external": True,
                                "sparse": True,
                            },
                        }
                        if vector_kind
                        else {}
                    ),
                },
            },
        )
        initial = call(
            "POST",
            "/tables/wal_windows/lake/catalog",
            {
                "commit_id": "initialize",
                "schema": metadata["schemas"][0],
            },
        )
        call(
            "POST",
            "/tables/wal_windows/lake/commits",
            {
                "commit_id": "independent-archive",
                "expected_metadata_location": initial["metadata_location"],
                "requirements": [],
                "updates": [
                    {"action": "add-snapshot", "snapshot": snapshot},
                    {
                        "action": "set-snapshot-ref",
                        "ref-name": "main",
                        "type": "branch",
                        "snapshot-id": snapshot["snapshot-id"],
                    },
                ],
            },
        )
        deadline = time.monotonic() + 120
        while True:
            index = call("GET", "/tables/wal_windows/indexes/body_text")
            if index["status"]["readiness"]["queryable"]:
                probe = call(
                    "POST",
                    "/tables/wal_windows/query",
                    {
                        "full_text_search": {"match": "quasar", "field": "body"},
                        "limit": row_count,
                    },
                )
                if len(probe["hits"]["hits"]) == row_count:
                    break
            assert time.monotonic() < deadline, index
            time.sleep(0.1)
        if vector_kind:
            # Keep the archive publication fixed while exercising incremental
            # accepted cuts. The background writer otherwise drains the first
            # batch before the second request, legitimately removing its rows
            # from the recent overlay. Reads need the catalog head/records but
            # only commits use the intent namespace.
            intents = lake / "buckets/antfly/objects/metadata/antfly-catalog/intents"
            intents.rename(intents.with_name("held-intents"))
            intents.write_bytes(b"hold catalog publication for accepted-cut reuse")
        vector_image = (
            {"dense_native": "[0,1]", "sparse_native": '{"2":2}'} if vector_kind else {}
        )
        accepted = call(
            "POST",
            "/tables/wal_windows/lake/changes",
            {
                "batch_id": "recent",
                "source": "e2e",
                "epoch": "1",
                "checkpoint": "1",
                "key_fields": ["id"],
                "changes": [
                    {"op": "delete", "row": {"id": 1}},
                    {
                        "op": "upsert",
                        "row": {"id": 0, "body": "quasar replacement", **vector_image},
                    },
                    {
                        "op": "upsert",
                        "row": {
                            "id": 1000,
                            "body": "quasar recent recent",
                            **vector_image,
                        },
                    },
                ],
            },
        )
        query = {
            "full_text_search": {"match": "quasar", "field": "body"},
            "fields": ["id", "body"],
            "limit": row_count,
            "profile": True,
            "read_requirements": {
                "visibility": "accepted",
                "after_write": accepted["read_receipt"],
            },
            "highlight": {"fields": ["body"]},
        }
        first = call("POST", "/tables/wal_windows/query", query)
        hits = first["hits"]["hits"]
        assert first["profile"]["sort"]["plan"] == "score_top_k", first
        assert first["profile"]["sort"]["selection_reason"] == (
            "bounded_global_text_frontier"
        ), first
        assert len(hits) == row_count, first
        rows = {h["_source"]["id"]: h["_source"]["body"] for h in hits}
        assert 1 not in rows and rows[0] == "quasar replacement"
        assert rows[1000] == "quasar recent recent"
        assert all(h.get("_highlights") for h in hits), first
        pinned = dict(query, remote_snapshot=first["remote_snapshot"])
        hybrid_request = None
        hybrid_first = None
        if vector_kind:
            embeddings = {}
            if vector_kind in ("dense", "hybrid"):
                embeddings["dense_native"] = [0, 1]
            if vector_kind in ("sparse", "hybrid"):
                embeddings["sparse_native"] = {"indices": [2], "values": [1]}
            hybrid_request = dict(query, embeddings=embeddings, fields=["id"])
            deadline = time.monotonic() + 120
            while True:
                response = requests.post(
                    server.api_url + "/tables/wal_windows/query",
                    json=hybrid_request,
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=120,
                )
                if response.ok:
                    value = response.json()
                    hybrid_first = (
                        value["responses"][0] if "responses" in value else value
                    )
                    break
                assert response.status_code == 503 and time.monotonic() < deadline, (
                    response.text + server.debug_logs()
                )
                time.sleep(0.1)
            assert len(hybrid_first["hits"]["hits"]) == row_count, hybrid_first
            assert {h["_source"]["id"] for h in hybrid_first["hits"]["hits"]} == set(
                rows
            )
            assert all(
                h.get("_highlights") and set(h["_source"]) == {"id"}
                for h in hybrid_first["hits"]["hits"]
            )
            enrichment = call(
                "POST",
                "/tables/wal_windows/lake/maintenance",
                {"action": "enrichment_status"},
            )
            assert enrichment["version"] == 2, enrichment
            assert enrichment["state"] == "ready", enrichment
            assert (
                enrichment["partition_count"] == enrichment["ready_partitions"] > 0
            ), enrichment
            assert enrichment["coverage_until_ms"] > int(time.time() * 1000), enrichment
            assert enrichment["wal_lsn"] == accepted["read_receipt"]["wal_lsn"], (
                enrichment
            )
            hybrid_request["remote_snapshot"] = hybrid_first["remote_snapshot"]

            def retained_artifact(artifact_id, *, raw=False):
                checksum = artifact_id.split(":")[1]
                paths = list((server.root / "artifacts").rglob(checksum))
                assert paths, artifact_id
                payload = paths[0].read_bytes()
                if payload.startswith(b"AFOBJ001"):
                    payload = payload[84 + struct.unpack_from("<I", payload, 16)[0] :]
                return payload if raw else json.loads(payload)

            def retained_state(token):
                envelope = retained_artifact(token[len("lake2:") :])
                assert envelope["version"] == 2, envelope
                state = retained_artifact(envelope["state"]["id"])
                assert state["version"] == 2, state
                assert all(
                    retained_artifact(part["id"], raw=True).startswith(b"AFWC1")
                    for part in state["change_parts"]
                ), state
                return (
                    envelope,
                    state,
                    {run["prefix"]: run for run in state["recent_runs"]},
                )

            old_envelope, old_state, old_runs = retained_state(
                hybrid_first["remote_snapshot"]
            )
            old_generation = old_state["publication"]["generation"]
            assert set(old_runs) == {ord("7"), ord("d")}, old_runs
            # A new query gets a fresh full lease but reuses immutable cut data.
            fresh_request = dict(hybrid_request)
            fresh_request.pop("remote_snapshot")
            started_ms = time.time_ns() // 1_000_000
            fresh = call("POST", "/tables/wal_windows/query", fresh_request)
            finished_ms = time.time_ns() // 1_000_000
            fresh_envelope, _, _ = retained_state(fresh["remote_snapshot"])
            assert fresh_envelope["state"] == old_envelope["state"]
            assert fresh_envelope["expires_ms"] >= old_envelope["expires_ms"]
            assert started_ms + 300_000 <= fresh_envelope["expires_ms"]
            assert fresh_envelope["expires_ms"] <= finished_ms + 300_000
            assert len(fresh_envelope["reader_token"]) == 16
            newer = call(
                "POST",
                "/tables/wal_windows/lake/changes",
                {
                    "batch_id": "recent-update",
                    "source": "e2e",
                    "epoch": "1",
                    "checkpoint": "2",
                    "expected_checkpoint": "1",
                    "key_fields": ["id"],
                    "changes": [
                        {
                            "op": "upsert",
                            "row": {
                                "id": 1000,
                                "body": "quasar newer recent",
                                **vector_image,
                            },
                        }
                    ],
                },
            )
            latest_request = dict(query, embeddings=embeddings)
            latest_request["read_requirements"] = {
                "visibility": "accepted",
                "after_write": newer["read_receipt"],
            }
            deadline = time.monotonic() + 120
            while True:
                response = requests.post(
                    server.api_url + "/tables/wal_windows/query",
                    json=latest_request,
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=120,
                )
                if response.ok:
                    latest = response.json()
                    latest = latest["responses"][0] if "responses" in latest else latest
                    break
                assert response.status_code == 503 and time.monotonic() < deadline, (
                    response.text + server.debug_logs()
                )
                time.sleep(0.1)
            _, new_state, new_runs = retained_state(latest["remote_snapshot"])
            new_generation = new_state["publication"]["generation"]
            assert new_generation == old_generation, (old_generation, new_generation)
            old_parts = {part["id"] for part in old_state["change_parts"]}
            new_parts = {part["id"] for part in new_state["change_parts"]}
            # Tombstones also occupy retained row groups, even though they
            # do not produce vector runs. Only id=1000's group should change.
            assert len(old_parts - new_parts) == 1, (old_parts, new_parts)
            assert len(new_parts - old_parts) == 1, (old_parts, new_parts)
            assert old_parts & new_parts, (old_parts, new_parts)
            assert old_runs[ord("d")] == new_runs[ord("d")], (old_runs, new_runs)
            assert old_runs[ord("7")]["digest"] != new_runs[ord("7")]["digest"]
            assert (
                next(
                    hit["_source"]["body"]
                    for hit in latest["hits"]["hits"]
                    if hit["_source"]["id"] == 1000
                )
                == "quasar newer recent"
            )
        for restart in (False, True):
            if restart:
                server.restart()
            if hybrid_request:
                hybrid = call("POST", "/tables/wal_windows/query", hybrid_request)
                assert hybrid["hits"] == hybrid_first["hits"]
                for identifier in (2, 1000):
                    selected = call(
                        "POST",
                        "/tables/wal_windows/query",
                        dict(
                            hybrid_request,
                            filter_query={"term": {"path": "/id", "value": identifier}},
                        ),
                    )
                    assert [h["_source"] for h in selected["hits"]["hits"]] == [
                        {"id": identifier}
                    ]
                    assert selected["hits"]["hits"][0].get("_highlights")
                vector_only = dict(hybrid_request)
                vector_only.pop("full_text_search")
                vector_only.pop("highlight")
                vector_only["filter_query"] = {"term": {"path": "/id", "value": 1000}}
                selected = call("POST", "/tables/wal_windows/query", vector_only)
                assert [h["_source"] for h in selected["hits"]["hits"]] == [
                    {"id": 1000}
                ]
                recent_vectors = dict(vector_only, limit=2)
                recent_vectors.pop("filter_query")
                selected = call("POST", "/tables/wal_windows/query", recent_vectors)
                assert {h["_source"]["id"] for h in selected["hits"]["hits"]} == {
                    0,
                    1000,
                }, selected
                # Replacement vectors use a different coordinate, so querying
                # the old sparse coordinate cannot return the archived image.
                stale_vector = dict(
                    vector_only,
                    embeddings={"sparse_native": {"indices": [1], "values": [1]}},
                    filter_query={"term": {"path": "/id", "value": 0}},
                )
                selected = call("POST", "/tables/wal_windows/query", stale_vector)
                assert selected["hits"]["hits"] == [], selected
            repeated = call("POST", "/tables/wal_windows/query", pinned)
            assert repeated["hits"] == first["hits"]
            filtered = call(
                "POST",
                "/tables/wal_windows/query",
                dict(
                    pinned,
                    fields=["id"],
                    filter_query={"term": {"path": "/id", "value": 1000}},
                ),
            )
            assert [h["_source"] for h in filtered["hits"]["hits"]] == [{"id": 1000}]
            assert filtered["hits"]["hits"][0].get("_highlights"), filtered
            archive = call(
                "POST",
                "/tables/wal_windows/query",
                dict(
                    pinned,
                    fields=["id"],
                    filter_query={"term": {"path": "/id", "value": 2}},
                ),
            )
            assert [h["_source"] for h in archive["hits"]["hits"]] == [{"id": 2}]
            recent_only = call(
                "POST",
                "/tables/wal_windows/query",
                dict(
                    pinned,
                    full_text_search={"term": "recent", "field": "body"},
                    fields=["id"],
                ),
            )
            assert [h["_source"] for h in recent_only["hits"]["hits"]] == [{"id": 1000}]
            counted = call("POST", "/tables/wal_windows/query", dict(pinned, limit=0))
            assert counted["hits"]["total"] == {"value": row_count, "relation": "exact"}
        failed = False
    finally:
        server.stop(test_failed=failed)
        intents = lake / "buckets/antfly/objects/metadata/antfly-catalog/intents"
        held_intents = intents.with_name("held-intents")
        if held_intents.exists():
            intents.unlink()
            held_intents.rename(intents)


@pytest.mark.parametrize("file_count", [1, 2, 10])
def test_cold_indexed_iceberg_queries_use_publication_file_navigation(
    tmp_path, file_count
):
    """Cold search and SQL point reads must not reopen data manifests."""
    if not os.environ.get("ANTFLY_E2E_FULL_LAKE"):
        for dependency in ("pyiceberg", "fastavro", "pyarrow"):
            pytest.importorskip(dependency)
    import fastavro
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    catalog = SqlCatalog(
        "cold_navigation",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    catalog.create_namespace("cold_navigation")
    table = catalog.create_table(
        "cold_navigation.items",
        schema=Schema(
            NestedField(1, "amount", LongType(), required=False),
            NestedField(2, "body", StringType(), required=False),
            NestedField(3, "label", StringType(), required=False),
        ),
        properties={"write.parquet.row-group-limit": "16"},
    )
    rows_per_file = 200 // file_count
    for start in range(0, 200, rows_per_file):
        table.append(
            pa.table(
                {
                    "amount": range(start, start + rows_per_file),
                    "body": ["needle common"] * rows_per_file,
                    "label": [
                        "kept" if i == 101 else "other"
                        for i in range(start, start + rows_per_file)
                    ],
                }
            )
        )
    # Real row groups exercise publication enumeration even in a single file.
    import pyarrow.parquet as pq

    tasks = list(table.scan().plan_files())
    assert len(tasks) == file_count
    assert all(
        pq.ParquetFile(Path(task.file.file_path.removeprefix("file://"))).num_row_groups
        > 1
        for task in tasks
    )
    root = tmp_path / "lake"
    _export_table(table, root)
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    hidden = []
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
            "/tables/cold_navigation",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "cold-navigation",
                        "format": "iceberg",
                        "object_mutability": "immutable",
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {"name": "amount_idx", "keys": [{"column": "amount"}]}
                    ],
                },
                "indexes": {"body_text": {"type": "full_text", "field": "body"}},
            },
        )
        deadline = time.monotonic() + 300
        while not call("GET", "/tables/cold_navigation/indexes/body_text")["status"][
            "readiness"
        ]["queryable"]:
            assert time.monotonic() < deadline, server.debug_logs()
            time.sleep(0.1)
        for path in (root / "buckets/antfly/objects/metadata").glob("*.avro"):
            records = list(fastavro.reader(io.BytesIO(path.read_bytes()[84:])))
            if records and "data_file" in records[0]:
                unavailable = path.with_suffix(".avro.unavailable")
                path.rename(unavailable)
                hidden.append((path, unavailable))
        assert hidden, "fixture must contain real data manifests"
        server.restart()
        # Cold ordinary scans enumerate authenticated publication pages; no
        # flat inventory or restored data manifests are prerequisites.
        assert call(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM cold_navigation"}
        )["rows"] == [["200"]]
        assert call(
            "POST",
            "/sql",
            {"statement": "SELECT label FROM cold_navigation WHERE label = 'kept'"},
        )["rows"] == [["kept"]]
        # SUM cannot use the metadata-only COUNT shortcut. Its residual predicate
        # uses row-group workers over the bounded publication view.
        assert call(
            "POST",
            "/sql",
            {
                "statement": "SELECT SUM(amount) FROM cold_navigation WHERE label = 'other'"
            },
        )["rows"] == [["19799"]]
        query = {
            "full_text_search": {"field": "body", "term": "common"},
            "fields": ["amount"],
            "limit": 3,
        }
        first = call("POST", "/tables/cold_navigation/query", query)
        assert len(first["hits"]["hits"]) == 3
        # Retained cuts select an explicit snapshot, while publication was built
        # using the current selector. Both authenticate the same metadata plan.
        token = first["remote_snapshot"]
        repeated = call(
            "POST", "/tables/cold_navigation/query", {**query, "remote_snapshot": token}
        )
        assert repeated["hits"]["hits"] == first["hits"]["hits"]
        residual = call(
            "POST",
            "/tables/cold_navigation/query",
            {
                **query,
                "filter_query": {
                    "conjuncts": [
                        {"range": {"path": "/amount", "gte": 101}},
                        {"prefix": {"path": "/label", "value": "ke"}},
                    ]
                },
                "order_by": [{"field": "_score", "desc": True}],
            },
        )
        assert [hit["_source"] for hit in residual["hits"]["hits"]] == [{"amount": 101}]

        assert call(
            "POST",
            "/sql",
            {"statement": "SELECT amount FROM cold_navigation WHERE amount = 101"},
        )["rows"] == [["101"]]
        # Explicit source discovery remains available when manifests are restored.
        data_manifests = [path for path, _ in hidden]
        for path, unavailable in hidden:
            unavailable.rename(path)
        hidden.clear()
        assert call(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM cold_navigation"}
        )["rows"] == [["200"]]
        assert (
            len(
                call(
                    "POST",
                    "/sql",
                    {"statement": "SELECT label FROM cold_navigation LIMIT 2"},
                )["rows"]
            )
            == 2
        )
        # Repeated full scans retain bounded per-file views and canonical order;
        # neither a new query nor a different projection rediscovers manifests.
        for path in data_manifests:
            unavailable = path.with_suffix(".avro.unavailable")
            path.rename(unavailable)
            hidden.append((path, unavailable))
        assert call(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM cold_navigation"}
        )["rows"] == [["200"]]
        assert (
            len(
                call(
                    "POST",
                    "/sql",
                    {"statement": "SELECT label FROM cold_navigation LIMIT 2"},
                )["rows"]
            )
            == 2
        )
        for path, unavailable in hidden:
            unavailable.rename(path)
        hidden.clear()
        # Index-store admission is optional for automatic source scans. A
        # disposable unavailable store cannot make readable source data fail.
        artifact_root = server.root / "artifacts"
        held_artifacts = server.root / "held-artifacts"
        artifact_root.rename(held_artifacts)
        artifact_root.write_text("unavailable native store")
        try:
            assert (
                len(
                    call(
                        "POST",
                        "/sql",
                        {"statement": "SELECT label FROM cold_navigation LIMIT 2"},
                    )["rows"]
                )
                == 2
            )
            assert call(
                "POST", "/sql", {"statement": "SELECT COUNT(*) FROM cold_navigation"}
            )["rows"] == [["200"]]
        finally:
            artifact_root.unlink()
            held_artifacts.rename(artifact_root)
        failed = False
    finally:
        for path, unavailable in hidden:
            unavailable.rename(path)
        if failed:
            print(server.debug_logs())
        server.stop()


@pytest.mark.parametrize("file_format", ["parquet", "iceberg"])
def test_signed_decimal_statistics_kernels_and_typed_pruning(tmp_path, file_format):
    """Independent writers exercise signed bounds, native kernels and restart."""
    import random
    from decimal import Decimal

    if not os.environ.get("ANTFLY_E2E_FULL_LAKE"):
        pytest.importorskip("pyarrow")
        if file_format == "iceberg":
            pytest.importorskip("pyiceberg")
            pytest.importorskip("fastavro")
    import pyarrow as pa
    import pyarrow.parquet as pq

    rng = random.Random(1112)
    values = [
        (
            i,
            None if i % 7 == 0 else Decimal(rng.randrange(-999999, 999999)).scaleb(-4),
            None
            if i % 11 == 0
            else Decimal(rng.choice([1, 3, 7, 10000001, -11])).scaleb(-2),
        )
        for i in range(75)
    ]
    arrow = pa.table(
        {
            "id": pa.array([v[0] for v in values], type=pa.int64()),
            "n": pa.array([v[1] for v in values], type=pa.decimal128(38, 4)),
            "m": pa.array([v[2] for v in values], type=pa.decimal128(38, 2)),
        }
    )
    root = tmp_path / "lake"
    if file_format == "parquet":
        raw = tmp_path / "data.parquet"
        pq.write_table(
            arrow,
            raw,
            use_dictionary=False,
            row_group_size=19,
            write_statistics=True,
            write_page_index=True,
        )
        payload = raw.read_bytes()
        objects = root / "buckets" / "antfly" / "objects"
        objects.mkdir(parents=True)
        (objects / "data.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
            + payload
        )
    else:
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import DecimalType, LongType, NestedField

        catalog = SqlCatalog(
            "e2e",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=(tmp_path / "warehouse").as_uri(),
        )
        catalog.create_namespace("e2e")
        table = catalog.create_table(
            "e2e.decimals",
            schema=Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(2, "n", DecimalType(38, 4), required=False),
                NestedField(3, "m", DecimalType(38, 2), required=False),
            ),
        )
        table.append(arrow)
        _export_table(table, root)

    def literal(value):
        return (
            "CAST(NULL AS NUMERIC)" if value is None else f"CAST('{value}' AS NUMERIC)"
        )

    rows = ",".join(f"({i},{literal(n)},{literal(m)})" for i, n, m in values)
    reference = f"(VALUES {rows}) v(id,n,m)"
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:

        def request(path, body):
            response = requests.post(
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=120,
            )
            assert response.ok, (
                str(body) + "\n" + response.text + "\n" + server.debug_logs()
            )
            return response.json()

        request(
            "/tables/native_decimals",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "base_source": {
                        "kind": "external",
                        "table_id": "native-decimals",
                        "format": file_format,
                        "object_mutability": "immutable",
                        "uri": root.as_uri(),
                    },
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "id": {"type": "integer"},
                                    "n": {
                                        "type": "number",
                                        "x-antfly-sql-type": "numeric",
                                    },
                                    "m": {
                                        "type": "number",
                                        "x-antfly-sql-type": "numeric",
                                    },
                                },
                            }
                        }
                    },
                },
            },
        )

        def sql(statement):
            return request("/sql", {"statement": statement})["rows"]

        join_values = [n for _, n, _ in values if n is not None][:3]
        statements = [
            f"SELECT {expression} FROM {{table}} ORDER BY id"
            for expression in (
                "n+m",
                "n-m",
                "n*m",
                "n/m",
                "n%m",
                "-(n/m)",
                "(n/m)+m",
                "(n+m)*(n+m)",
                "n/m-(n+m)*(n+m)",
                "(n/m)/m",
                "(n/m) = m",
                "(n/m) < m",
                "((n+m)*(n-m)+(n+m)*(n-m))/m",
            )
        ] + [
            "SELECT COUNT(n), MIN(n), MAX(n), SUM(n) FROM {table}",
            "SELECT id,n FROM {table} WHERE n >= -17.25 AND n < 30.001 ORDER BY id",
            "SELECT id,n FROM {table} WHERE n < 0 ORDER BY id",
            "SELECT id FROM {table} JOIN (VALUES "
            + ",".join(f"({literal(n)})" for n in join_values)
            + ") AS b(bound) ON n = b.bound ORDER BY id",
        ]
        expected = [sql(query.format(table=reference)) for query in statements]
        for cold in (False, True):
            if cold:
                server.restart()
            for query, result in zip(statements, expected):
                assert sql(query.format(table="native_decimals")) == result, query
        failed = False
    finally:
        server.stop(test_failed=failed)
