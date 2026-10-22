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

"""Verified native index relocation against independent Parquet/Iceberg writers."""

import hashlib
import json
import os
import struct
import time

import pytest
import requests
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
)

pytestmark = pytest.mark.fresh_antfly_process


@pytest.mark.parametrize(
    "file_format",
    ["parquet", pytest.param("iceberg", marks=pytest.mark.iceberg_integration)],
)
def test_logical_identity_reuses_reordered_native_indexes(tmp_path, file_format):
    if not os.environ.get("ANTFLY_E2E_FULL_LAKE"):
        pytest.importorskip("pyarrow")
        if file_format == "iceberg":
            pytest.importorskip("pyiceberg")
            pytest.importorskip("fastavro")
    import pyarrow as pa
    import pyarrow.parquet as pq

    count, base = 257, 9007199254740993
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    iceberg = None
    if file_format == "iceberg":
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType
        from test_iceberg_sql import _export_table

        catalog = SqlCatalog(
            "logical",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=(tmp_path / "warehouse").as_uri(),
        )
        catalog.create_namespace("e2e")
        iceberg = catalog.create_table(
            "e2e.logical",
            schema=Schema(
                NestedField(1, "id", LongType(), required=False),
                *[
                    NestedField(i, name, StringType(), required=False)
                    for i, name in enumerate(
                        ("body", "label", "dense_native", "sparse_native"), start=2
                    )
                ],
            ),
        )

    def write(step):
        order = (
            list(range(count)) if step == 0 else [i * 73 % count for i in range(count)]
        )
        if step == 2:
            order.reverse()
        # Separate the top sparse scores despite per-block 8-bit quantization
        # changing scales across physical and logical fragment layouts.
        docs = [
            {
                "id": base + i,
                "body": (
                    "changed word" if step in (3, 6) and i == 17 else "needle same text"
                ),
                "label": f"updated-{i}" if step == 5 else f"row-{i}",
                "dense_native": json.dumps([1, i / count]),
                "sparse_native": json.dumps({"1": (i + 1) ** 2}),
            }
            for i in order
        ]
        if step == 7:
            docs = [doc for doc in docs if doc["id"] != base + 17]
        if step == 4:
            docs = [
                {
                    "id": base + count,
                    "body": "needle same text",
                    "label": f"row-{count}",
                    "dense_native": json.dumps([1, 1]),
                    "sparse_native": json.dumps({"1": (count + 1) ** 2}),
                }
            ]
        if iceberg is not None:
            table = pa.Table.from_pylist(docs)
            if step in (0, 4):
                iceberg.append(table)
            else:
                iceberg.overwrite(table)
            _export_table(iceberg, root)
        else:
            if step != 4:
                for old in objects.glob("*.parquet"):
                    old.unlink()
            for part, begin in enumerate(range(0, len(docs), 129)):
                path = tmp_path / (
                    "added-first.parquet"
                    if step == 4
                    else f"part-{step}-{part}.parquet"
                )
                pq.write_table(
                    pa.Table.from_pylist(docs[begin : begin + 129]),
                    path,
                    row_group_size=17,
                    compression="snappy",
                    use_dictionary=True,
                    write_page_index=True,
                    data_page_version="2.0",
                )
                payload = path.read_bytes()
                (objects / path.name).write_bytes(
                    b"AFOBJ001"
                    + struct.pack("<QI", len(payload), 0)
                    + hashlib.sha256(payload).hexdigest().encode()
                    + payload
                )

    write(0)
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:

        def raw(method, path, body=None):
            return requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )

        def call(method, path, body=None):
            response = raw(method, path, body)
            assert response.ok, response.text + server.debug_logs()
            value = response.json() if response.content else None
            return (
                value.get("responses", [value])[0] if isinstance(value, dict) else value
            )

        def attach(name, logical):
            source = {
                "kind": "external",
                "table_id": "logical-source",
                "format": file_format,
                "uri": root.as_uri(),
            }
            if logical:
                source["row_identity"] = {"key_columns": ["id"]}
            call(
                "POST",
                f"/tables/{name}",
                {
                    "schema": {
                        "storage_mode": "relational",
                        "base_source": source,
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
                                        "label": {"type": "string"},
                                        "dense_native": {"type": "string"},
                                        "sparse_native": {"type": "string"},
                                    },
                                }
                            }
                        },
                        "relational_indexes": [
                            {
                                "name": f"{name}_id_idx",
                                "keys": [{"column": "id"}],
                                "include_columns": ["body", "label"],
                            }
                        ],
                    },
                    "indexes": {
                        "text": {"type": "full_text", "field": "body"},
                        "all_text": {"type": "full_text"},
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
                        "stats": {
                            "type": "algebraic",
                            "derive_from_schema": True,
                            "aggregates": [{"name": "rows", "op": "count"}],
                        },
                    },
                },
            )

        text = {
            "full_text_search": {"match": "needle", "field": "body"},
            "full_text_index": "text",
            "limit": 7,
            "fields": ["id", "label"],
            "highlight": {"fields": ["body"]},
        }

        def ready(name, after=None):
            deadline = time.monotonic() + 90
            while True:
                # Fresh-source query failure schedules publication without a
                # definition change, so it cannot disguise a rebuild as reuse.
                response = raw("POST", f"/tables/{name}/query", text)
                assert response.ok or response.status_code in (409, 503), (
                    response.text + server.debug_logs()
                )
                indexes = call("GET", f"/tables/{name}/indexes")
                assert all(
                    i["status"]["readiness"]["state"] != "failed" for i in indexes
                ), json.dumps(indexes) + server.debug_logs()
                revision = next(
                    i["status"].get("published_revision")
                    for i in indexes
                    if i["config"]["name"] == "text"
                )
                if (
                    response.ok
                    and revision is not None
                    and revision != after
                    and all(i["status"]["readiness"]["queryable"] for i in indexes)
                ):
                    return revision
                assert time.monotonic() < deadline, str(indexes) + server.debug_logs()
                time.sleep(0.1)

        def states():
            result = []
            for path in (server.root / "artifacts").rglob("*"):
                if not path.is_file():
                    continue
                payload = path.read_bytes()
                begin = payload.find(b'{"version":')
                if begin < 0:
                    continue
                try:
                    value = json.loads(payload[begin:])
                except (ValueError, UnicodeDecodeError):
                    continue
                if (
                    isinstance(value, dict)
                    and "values_recipe" in value
                    and "current" in value
                ):
                    if value.get("version") == 2:
                        spaces = {
                            view["id"]: view for view in value["coordinate_spaces"]
                        }
                        value["reused"] = [
                            {
                                "name": binding["name"],
                                **{
                                    k: v
                                    for k, v in spaces[binding["space"]].items()
                                    if k != "id"
                                },
                            }
                            for binding in value["index_bindings"]
                        ]
                    result.append((path.stat().st_mtime_ns, value))
            return [value for _, value in sorted(result, key=lambda item: item[0])]

        attach("logical", True)
        revision = ready("logical")
        first = states()
        assert first, "Logical identity state was not published"
        original = next(root for root in first if root.get("indexes") is None)

        for step in (1, 2, 5, 3, 6, 4, 7):
            write(step)
            revision = ready("logical", revision)
            candidates = states()
            current = candidates[-1]
            reused = {
                entry["name"]: entry["space"] for entry in current.get("reused", [])
            }
            if step in (1, 2):
                text_space = current.get("indexes") or reused.get("text")
                assert text_space is not None, current
                assert current["artifacts"] == original["artifacts"], {
                    "reused": list(reused),
                    "inputs": current["current"].get("input_recipes"),
                    "original_inputs": original["current"].get("input_recipes"),
                }
                assert text_space["snapshot"] == original["current"]["snapshot"], (
                    current
                )
            elif step == 5:
                assert "text" in reused, current
                assert (
                    reused["all_text"]["by_id"] != original["native_space"]["by_id"]
                ), current
                assert current["artifacts"] != original["artifacts"], current
                assert reused["text"]["snapshot"] == original["current"]["snapshot"]
            elif step == 3:
                assert reused["text"]["by_id"] != original["native_space"]["by_id"], (
                    current
                )
                assert (
                    reused["all_text"]["by_id"] != original["native_space"]["by_id"]
                ), current
                assert "dense_native" in reused and "sparse_native" in reused, current
            elif step == 6:
                assert "text" in reused and "dense_native" in reused, current
                assert (
                    reused["text"]["snapshot"] != reused["dense_native"]["snapshot"]
                ), current
            else:
                assert current.get("native_space") is not None, current
                assert current["native_space"]["rows"] == count + (
                    1 if step == 4 else -1
                )
                assert (
                    current["native_space"]["file_count"]
                    == original["native_space"]["file_count"]
                )
                assert {"text", "dense_native", "sparse_native"} <= reused.keys()
                assert current["artifacts"] != original["artifacts"], current
            oracle = f"oracle{step}"
            attach(oracle, False)
            ready(oracle)
            requests_ = [
                text,
                dict(text, filter_query={"term": {"path": "/id", "value": base + 17}}),
                dict(
                    text,
                    filter_query={"range": {"path": "/id", "gte": base + 17}},
                ),
                dict(
                    text,
                    full_text_index="all_text",
                    order_by=[{"field": "id", "desc": True}],
                ),
                {
                    "embeddings": {"dense_native": [1, 0]},
                    "indexes": ["dense_native"],
                    "fields": ["id", "label"],
                    "limit": 7,
                },
                {
                    "embeddings": {"sparse_native": {"indices": [1], "values": [2]}},
                    "indexes": ["sparse_native"],
                    "fields": ["id", "label"],
                    "limit": 7,
                },
            ]
            # Exercise native vector admission in original index coordinates,
            # including include-minus-exclude and a broad ordered predicate.
            for vector in requests_[-2:]:
                requests_.extend(
                    [
                        dict(
                            vector,
                            filter_query={"term": {"path": "/id", "value": base + 17}},
                        ),
                        dict(
                            vector,
                            filter_query={"range": {"path": "/id", "gte": base + 17}},
                            exclusion_query={
                                "term": {"path": "/id", "value": base + 23}
                            },
                        ),
                    ]
                )
            for request in requests_:
                actual = call("POST", "/tables/logical/query", request)
                expected = call("POST", f"/tables/{oracle}/query", request)

                def normalize(response):
                    return [
                        (h["_id"], h["_source"], h.get("_highlights"))
                        for h in response["hits"]["hits"]
                    ]

                assert normalize(actual) == normalize(expected), json.dumps(
                    {
                        "step": step,
                        "request": request,
                        "actual": actual,
                        "expected": expected,
                    }
                )
                # Sparse posting blocks quantize weights to 8 bits. Rebuilding
                # a different physical layout can choose different scales;
                # identity, values and membership must still agree exactly.
                score_error = (
                    2 * (count + 1) ** 2 / 255 + 1e-4
                    if request.get("indexes") == ["sparse_native"]
                    else 0
                )
                assert [h["_score"] for h in actual["hits"]["hits"]] == pytest.approx(
                    [h["_score"] for h in expected["hits"]["hits"]],
                    rel=1e-6,
                    abs=score_error,
                )
            pages = []
            for name in ("logical", oracle):
                ordered = dict(
                    text,
                    order_by=[
                        {"field": "_score", "desc": True},
                        {"field": "_id", "desc": False},
                    ],
                    limit=5,
                )
                first = call("POST", f"/tables/{name}/query", ordered)
                second = call(
                    "POST",
                    f"/tables/{name}/query",
                    dict(
                        ordered,
                        search_after=first["hits"]["hits"][-1]["_sort"],
                        remote_snapshot=first["remote_snapshot"],
                    ),
                )
                previous = call(
                    "POST",
                    f"/tables/{name}/query",
                    dict(
                        ordered,
                        search_before=second["hits"]["hits"][0]["_sort"],
                        remote_snapshot=first["remote_snapshot"],
                    ),
                )
                assert normalize(previous) == normalize(first)
                pages.append([normalize(first), normalize(second)])
            assert pages[0] == pages[1]
            for statement in (
                "SELECT id, label FROM {table} WHERE id >= "
                + str(base + 17)
                + " ORDER BY id DESC LIMIT 9",
                "SELECT COUNT(*) FROM {table}",
            ):
                actual = call(
                    "POST", "/sql", {"statement": statement.format(table="logical")}
                )["rows"]
                expected = call(
                    "POST", "/sql", {"statement": statement.format(table=oracle)}
                )["rows"]
                assert actual == expected
            if step in (2, 6, 7):
                before = call("POST", "/tables/logical/query", text)
                server.restart()
                ready("logical")
                assert normalize(
                    call("POST", "/tables/logical/query", text)
                ) == normalize(before)
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_logical_identity_mixed_parquet_decimal_precision(tmp_path):
    """Optional identity accepts compatible decimal types across physical files."""
    from decimal import Decimal

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    expected = {}
    for part, precision in enumerate((18, 20)):
        ids = list(range(part * 65, (part + 1) * 65))
        price = Decimal("90071992547409.93" if part == 0 else "-123456789012345678.90")
        expected.update(dict.fromkeys(ids, price))
        path = tmp_path / f"part-{part}.parquet"
        pq.write_table(
            pa.table(
                {
                    "id": pa.array(ids, pa.int64()),
                    "body": pa.array(["needle"] * len(ids)),
                    "price": pa.array([price] * len(ids), pa.decimal128(precision, 2)),
                }
            ),
            path,
            row_group_size=17,
            write_page_index=True,
        )
        payload = path.read_bytes()
        (objects / path.name).write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
            + payload
        )
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
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
            assert response.ok, response.text + server.debug_logs()
            return response.json(parse_float=Decimal) if response.content else None

        call(
            "POST",
            "/tables/decimals",
            {
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "decimals",
                        "format": "parquet",
                        "uri": root.as_uri(),
                        "row_identity": {"key_columns": ["id"]},
                    },
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "id": {"type": "integer"},
                                    "body": {"type": "string"},
                                    "price": {
                                        "type": "number",
                                        "x-antfly-sql-type": "numeric",
                                    },
                                },
                            }
                        }
                    },
                    "relational_indexes": [
                        {
                            "name": "decimal_id",
                            "keys": [{"column": "id"}],
                            "include_columns": ["price"],
                        }
                    ],
                },
                "indexes": {"text": {"type": "full_text", "field": "body"}},
            },
        )
        deadline = time.monotonic() + 90
        while True:
            indexes = call("GET", "/tables/decimals/indexes")
            assert all(
                i["status"]["readiness"]["state"] != "failed" for i in indexes
            ), json.dumps(indexes) + server.debug_logs()
            if all(i["status"]["readiness"]["queryable"] for i in indexes):
                break
            assert time.monotonic() < deadline, (
                json.dumps(indexes) + server.debug_logs()
            )
            time.sleep(0.1)
        for restart in (False, True):
            if restart:
                server.restart()
            actual = {}
            # Keep each response below the SQL default of 128 result rows.
            for begin in (0, 65):
                result = call(
                    "POST",
                    "/sql",
                    {
                        "statement": f"SELECT id, price FROM decimals WHERE id >= {begin} AND id < {begin + 65} ORDER BY id LIMIT 65"
                    },
                )
                actual.update(
                    {int(row[0]): Decimal(str(row[1])) for row in result["rows"]}
                )
            assert actual == expected
            result = call(
                "POST",
                "/tables/decimals/query",
                {
                    "full_text_search": {"match": "needle", "field": "body"},
                    "full_text_index": "text",
                    "filter_query": {"term": {"path": "/id", "value": 66}},
                    "fields": ["id", "price"],
                },
            )
            hits = result["responses"][0]["hits"]["hits"]
            assert len(hits) == 1, result
            assert hits[0]["_source"]["id"] == 66
            assert Decimal(str(hits[0]["_source"]["price"])) == expected[66]
        failed = False
    finally:
        server.stop(test_failed=failed)
