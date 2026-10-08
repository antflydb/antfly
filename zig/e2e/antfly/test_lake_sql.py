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

"""Independent Parquet -> public attachment -> HTTP/pgwire -> cold restart.

Run: uv run --extra lake --project e2e/antfly pytest e2e/antfly/test_lake_sql.py
"""

import hashlib
import json
import os
import struct
import time
from pathlib import Path

import pytest
import requests
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
)

pytestmark = pytest.mark.fresh_antfly_process


@pytest.mark.parametrize("dictionary", [False, True])
@pytest.mark.parametrize("page_version", ["1.0", "2.0"])
def test_parquet_attachment_survives_restart_and_streams_over_pgwire(
    tmp_path, dictionary, page_version
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    psycopg = pytest.importorskip("psycopg")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary}")
    # Use a separate writer, actual compression, nulls, and multiple row groups.
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    count = 1200
    pq.write_table(
        pa.table(
            {
                "amount": pa.array(range(count), type=pa.int64()),
                "exact": pa.array(
                    [
                        None if i % 7 == 0 else 9007199254740993 + i % 3
                        for i in range(count)
                    ],
                    type=pa.int64(),
                ),
                "measure": pa.array(
                    [None if i % 7 == 0 else (i % 3) * 0.5 for i in range(count)],
                    type=pa.float64(),
                ),
                "label": pa.array(
                    [None if i % 7 == 0 else f"row-{i}" for i in range(count)]
                ),
            }
        ),
        tmp_path / "input.parquet",
        compression="snappy",
        use_dictionary=dictionary,
        row_group_size=173,
        write_page_index=True,
        data_page_size=256,
        write_batch_size=32,
        data_page_version=page_version,
    )
    # file:// addresses Antfly's filesystem object-store namespace. The object
    # envelope supplies version metadata; its payload is the independent file.
    payload = (tmp_path / "input.parquet").read_bytes()
    envelope = (
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
    )
    (objects / "part.parquet").write_bytes(envelope + payload)
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, pgwire=True)
    failed = True
    try:

        def request(method, path, payload=None):
            try:
                response = requests.request(
                    method,
                    server.api_url + path,
                    json=payload,
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=60,
                )
            except requests.RequestException as exc:
                pytest.fail(f"{exc}\n{server.debug_logs()}")
            assert response.ok, (
                response.text + "\n" + server.log_path.read_text()[-8000:]
            )
            if method == "POST" and path == "/tables/lake_events":
                assert response.status_code == 200, response.text + server.debug_logs()
            return response.json() if response.content else {}

        aggregate_config = {
            "type": "algebraic",
            "derive_from_schema": True,
            "aggregates": [
                {"name": "amount_total", "op": "sum", "measure": "amount"},
                {"name": "exact_min", "op": "min", "measure": "exact"},
                {"name": "row_count", "op": "count"},
                {"name": "non_null_count", "op": "count", "measure": "exact"},
                {"name": "measure_mean", "op": "avg", "measure": "measure"},
                {"name": "counts_by_amount", "op": "count", "group_by": ["amount"]},
            ],
        }
        request(
            "POST",
            "/tables/lake_events",
            {
                "num_shards": 1,
                "indexes": {"exact_stats": aggregate_config},
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "lake-events",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                },
            },
        )
        deadline = time.monotonic() + 30
        while True:
            response = requests.post(
                server.api_url + "/sql",
                json={"statement": "SELECT COUNT(*) FROM lake_events"},
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            if response.ok:
                assert response.json()["rows"] == [[str(count)]]
                break
            assert response.status_code in (404, 409, 503), response.text
            assert time.monotonic() < deadline, response.text
            time.sleep(0.1)
        request(
            "POST",
            "/tables/lake_events/indexes/label_text",
            {"type": "full_text", "field": "label"},
        )

        def await_publication(index_name="label_text"):
            deadline = time.monotonic() + 30
            while True:
                resource = request("GET", f"/tables/lake_events/indexes/{index_name}")
                status = resource["status"]
                if status.get("published_revision") is not None:
                    return status["published_revision"]
                assert status["readiness"]["state"] != "failed", (
                    json.dumps(resource) + "\n" + server.debug_logs()
                )
                assert time.monotonic() < deadline, (
                    str(resource) + "\n" + server.debug_logs()
                )
                time.sleep(0.1)

        published_generation = await_publication()
        artifact_root = server.root / "artifacts" / "buckets" / "native-lake-indexes"
        assert artifact_root.exists()
        aggregate_generation = await_publication("exact_stats")
        stats_resource = request("GET", "/tables/lake_events/indexes/exact_stats")
        assert stats_resource["status"]["readiness"]["queryable"], (
            json.dumps(stats_resource) + "\n" + server.debug_logs()
        )
        listed = request("GET", "/tables/lake_events/indexes")
        assert next(item for item in listed if item["config"]["name"] == "exact_stats")[
            "status"
        ]["readiness"]["queryable"]
        # Adding an index publishes the entire desired definition atomically.
        published_generation = await_publication()
        amount_sum = sum(range(count))

        # Prove actual artifact consumption: a checksum failure after selection
        # aborts an eligible query; a predicate lacking an equivalence proof
        # still scans Parquet. Restore the object before the restart checks.
        aggregate_root = None
        for artifact_path in artifact_root.rglob("*"):
            if not artifact_path.is_file():
                continue
            original = artifact_path.read_bytes()
            marker = original.find(b'"format":"native-sql-aggregate-v')
            if marker >= 0:
                candidate = json.loads(original[marker - 1 :])
                inputs = candidate["recipe"]["inputs"]
                if (
                    not candidate["recipe"]["keys"]
                    and len(inputs) == 1
                    and inputs[0]["spec"]["kind"] == "sum"
                    and inputs[0]["column"]["path"] == "amount"
                ):
                    aggregate_root = candidate
                    break
        assert aggregate_root is not None, "Native exact aggregate root missing"
        checksum = aggregate_root["blocks"][0]["artifact"]["checksum"]
        candidates = list((artifact_root / "objects").rglob(checksum))
        assert candidates, "Native aggregate block missing"
        # Overlapping publication attempts can retain identical blocks under
        # distinct scopes. Damage every copy so selection cannot choose an
        # intact generation, and restore all copies even if an assertion fails.
        originals = []
        try:
            for artifact_path in candidates:
                original = artifact_path.read_bytes()
                originals.append((artifact_path, original))
                marker = original.index(b"NCB\x01")
                damaged = bytearray(original)
                damaged[marker] ^= 1
                artifact_path.write_bytes(damaged)
            for statement in (
                "SELECT SUM(amount) FROM lake_events",
                "SELECT SUM(amount), COUNT(*) FROM lake_events",
            ):
                response = requests.post(
                    server.api_url + "/sql",
                    json={"statement": statement},
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=60,
                )
                assert not response.ok, (
                    "Selected corrupt aggregate silently fell back to scanning"
                )
            assert request(
                "POST",
                "/sql",
                {"statement": "SELECT SUM(amount) FROM lake_events WHERE amount >= 0"},
            )["rows"] == [[str(amount_sum)]]
        finally:
            for artifact_path, original in originals:
                artifact_path.write_bytes(original)
        assert request(
            "POST", "/sql", {"statement": "SELECT SUM(amount) FROM lake_events"}
        )["rows"] == [[str(amount_sum)]]
        assert request(
            "POST",
            "/sql",
            {"statement": "SELECT SUM(amount), COUNT(*), MIN(exact) FROM lake_events"},
        )["rows"] == [[str(amount_sum), str(count), "9007199254740993"]]
        assert request(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM lake_events"}
        )["rows"] == [[str(count)]]
        assert request(
            "POST", "/sql", {"statement": "SELECT MIN(exact) FROM lake_events"}
        )["rows"] == [["9007199254740993"]]
        non_null = [i for i in range(count) if i % 7 != 0]
        assert request(
            "POST", "/sql", {"statement": "SELECT COUNT(exact) FROM lake_events"}
        )["rows"] == [[str(len(non_null))]]
        mean = request(
            "POST", "/sql", {"statement": "SELECT AVG(measure) FROM lake_events"}
        )["rows"][0][0]
        assert float(mean) == pytest.approx(
            sum((i % 3) * 0.5 for i in non_null) / len(non_null)
        )
        assert request(
            "POST",
            "/sql",
            {
                "statement": "SELECT amount, COUNT(*) AS n FROM lake_events GROUP BY amount HAVING amount >= 1197 ORDER BY amount DESC LIMIT 2"
            },
        )["rows"] == [["1199", "1"], ["1198", "1"]]
        # Both independent empty-file layouts must remain valid attachments.
        for row_group in (False, True):
            empty_root = tmp_path / f"empty-{row_group}"
            empty_objects = empty_root / "buckets" / "antfly" / "objects"
            empty_objects.mkdir(parents=True)
            empty_path = tmp_path / f"empty-{row_group}.parquet"
            empty_schema = pa.schema([("amount", pa.int64())])
            if row_group:
                pq.write_table(
                    pa.table({"amount": pa.array([], type=pa.int64())}), empty_path
                )
            else:
                with pq.ParquetWriter(empty_path, empty_schema):
                    pass
            empty = empty_path.read_bytes()
            empty_envelope = (
                b"AFOBJ001"
                + struct.pack("<QI", len(empty), 0)
                + hashlib.sha256(empty).hexdigest().encode()
            )
            (empty_objects / "part.parquet").write_bytes(empty_envelope + empty)
            name = f"empty_lake_{int(row_group)}"
            request(
                "POST",
                f"/tables/{name}",
                {
                    "num_shards": 1,
                    "schema": {
                        "storage_mode": "relational",
                        "base_source": {
                            "kind": "external",
                            "table_id": name,
                            "format": "parquet",
                            "uri": empty_root.as_uri(),
                        },
                    },
                },
            )
            deadline = time.monotonic() + 30
            while True:
                response = requests.post(
                    server.api_url + "/sql",
                    json={"statement": f"SELECT SUM(amount), COUNT(*) FROM {name}"},
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=60,
                )
                if response.ok:
                    assert response.json()["rows"] == [[None, "0"]]
                    break
                assert response.status_code in (404, 409, 503), response.text
                assert time.monotonic() < deadline, response.text
                time.sleep(0.1)
            assert (
                request("POST", "/sql", {"statement": f"SELECT amount FROM {name}"})[
                    "rows"
                ]
                == []
            )
        # A wide physical file crosses ordered-parallel admission even when
        # SQL projects only its narrow integer column. Use an independent
        # writer and verify OFFSET skips a projection error in the prefix.
        large_root = tmp_path / "large-lake"
        large_objects = large_root / "buckets" / "antfly" / "objects"
        large_objects.mkdir(parents=True)
        large_path = tmp_path / "large.parquet"
        large_count = 10_000
        pq.write_table(
            pa.table(
                {
                    "amount": pa.array(range(large_count), type=pa.int64()),
                    "payload": [
                        "".join(
                            hashlib.sha256(f"{i}:{j}".encode()).hexdigest()
                            for j in range(4)
                        )
                        for i in range(large_count)
                    ],
                }
            ),
            large_path,
            compression="snappy",
            use_dictionary=dictionary,
            row_group_size=173,
            write_page_index=True,
            data_page_version="2.0",
        )
        large = large_path.read_bytes()
        assert len(large) >= 2 * 1024 * 1024
        (large_objects / "part.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(large), 0)
            + hashlib.sha256(large).hexdigest().encode()
            + large
        )
        request(
            "POST",
            "/tables/lake_large",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "lake-large",
                        "format": "parquet",
                        "uri": large_root.as_uri(),
                    },
                },
            },
        )
        deadline = time.monotonic() + 30
        while True:
            response = requests.post(
                server.api_url + "/sql",
                json={"statement": "SELECT COUNT(*) FROM lake_large"},
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            if response.ok:
                assert response.json()["rows"] == [[str(large_count)]]
                break
            assert response.status_code in (404, 409, 503), response.text
            assert time.monotonic() < deadline, response.text
            time.sleep(0.1)
        sql = "SELECT amount, label FROM lake_events WHERE amount >= 1197 ORDER BY amount DESC"
        expected = [["1199", "row-1199"], ["1198", "row-1198"], ["1197", None]]
        assert request("POST", "/sql", {"statement": sql})["rows"] == expected
        # Blocking derived tables drain through the statement result cursor.
        for inner in (
            "SELECT amount FROM lake_events ORDER BY amount DESC",
            "SELECT amount, COUNT(*) AS n FROM lake_events GROUP BY amount",
            "SELECT amount, ROW_NUMBER() OVER (ORDER BY amount) AS n FROM lake_events",
        ):
            assert request(
                "POST", "/sql", {"statement": f"SELECT COUNT(*) FROM ({inner}) AS q"}
            )["rows"] == [[str(count)]]
        assert request(
            "POST", "/sql", {"statement": "SELECT COUNT(*), COUNT(*) FROM lake_events"}
        )["rows"] == [[str(count), str(count)]]
        projected = request(
            "POST",
            "/sql",
            {
                "statement": "SELECT amount * 2 AS doubled, label FROM lake_events WHERE amount >= 1197"
            },
        )["rows"]
        assert sorted(projected, key=lambda row: int(row[0])) == [
            ["2394", None],
            ["2396", "row-1198"],
            ["2398", "row-1199"],
        ]
        grouped = request(
            "POST",
            "/sql",
            {
                "statement": "SELECT amount % 11 AS k, COUNT(*) AS c, SUM(amount) AS s FROM lake_events GROUP BY amount % 11 ORDER BY k"
            },
        )["rows"]
        assert grouped == [
            [str(k), str(len(range(k, count, 11))), str(sum(range(k, count, 11)))]
            for k in range(11)
        ]
        windows = request(
            "POST",
            "/sql",
            {
                "statement": "SELECT amount, SUM(amount) OVER (ORDER BY amount ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running FROM lake_events ORDER BY amount DESC LIMIT 3"
            },
        )["rows"]
        assert windows == [["1199", "4790"], ["1198", "4786"], ["1197", "4782"]]
        joined = request(
            "POST",
            "/sql",
            {
                "statement": "SELECT a.amount, b.label FROM (SELECT amount FROM lake_events ORDER BY amount DESC LIMIT 3) a LEFT JOIN lake_events b ON a.amount = b.amount ORDER BY a.amount DESC"
            },
        )["rows"]
        assert joined == expected
        filtered_join = request(
            "POST",
            "/sql",
            {
                "statement": "SELECT p.amount FROM lake_events p JOIN (SELECT amount FROM lake_events WHERE amount >= 1197 LIMIT 3) b ON p.amount = b.amount ORDER BY p.amount DESC"
            },
        )
        assert filtered_join["rows"] == [["1199"], ["1198"], ["1197"]]
        shared_windows = request(
            "POST",
            "/sql",
            {
                "statement": "SELECT amount, rank() OVER (ORDER BY amount % 3), rank() OVER (ORDER BY amount % 3, amount), sum(amount) OVER (ORDER BY amount % 3) FROM lake_events WHERE amount >= 1197 ORDER BY amount"
            },
        )
        assert shared_windows["rows"] == [
            ["1197", "1", "1", "1197"],
            ["1198", "2", "2", "2395"],
            ["1199", "3", "3", "3594"],
        ]
        for restart in (False, True):
            if restart:
                server.restart()
                assert request("POST", "/sql", {"statement": sql})["rows"] == expected
                assert await_publication() == published_generation
                assert await_publication("exact_stats") == aggregate_generation
                assert request(
                    "POST", "/sql", {"statement": "SELECT SUM(amount) FROM lake_events"}
                )["rows"] == [[str(amount_sum)]]
            with (
                psycopg.connect(
                    host="127.0.0.1",
                    port=server.pgwire_port,
                    user="admin",
                    password=AUTH_BOOTSTRAP_PASSWORD,
                    dbname="default",
                    sslmode="disable",
                    autocommit=True,
                ) as connection,
                connection.cursor() as cursor,
            ):
                numeric = list(
                    cursor.stream(
                        "SELECT exact, measure + 0.25 FROM lake_events", size=37
                    )
                )
                assert numeric == [
                    (None, None)
                    if i % 7 == 0
                    else (9007199254740993 + i % 3, (i % 3) * 0.5 + 0.25)
                    for i in range(count)
                ]
                fused_numeric = list(
                    cursor.stream(
                        "SELECT exact, (exact - 9007199254740993) * 3 + 7, "
                        "(exact - 9007199254740993) * 3 + 8 FROM "
                        "(SELECT amount, exact FROM lake_events ORDER BY amount) q",
                        size=29,
                    )
                )
                assert fused_numeric == [
                    (None, None, None)
                    if i % 7 == 0
                    else (9007199254740993 + i % 3, (i % 3) * 3 + 7, (i % 3) * 3 + 8)
                    for i in range(count)
                ]
                # Repeated exact numerics survive a blocking derived relation,
                # downstream predicate evaluation, sorting, and portal slicing.
                retained_numeric = list(
                    cursor.stream(
                        "SELECT exact, measure + 0.25 FROM "
                        "(SELECT amount, exact, measure FROM lake_events ORDER BY amount) q "
                        "WHERE exact = 9007199254740994 ORDER BY amount DESC",
                        size=13,
                    )
                )
                assert retained_numeric == [
                    (9007199254740994, 0.75)
                    for i in reversed(range(count))
                    if i % 3 == 1 and i % 7 != 0
                ]
                cursor.execute(
                    "SELECT exact, COUNT(*), SUM(measure) FROM lake_events "
                    "WHERE exact IS NOT NULL GROUP BY exact ORDER BY exact"
                )
                assert cursor.fetchall() == [
                    (
                        9007199254740993 + k,
                        sum(i % 3 == k and i % 7 != 0 for i in range(count)),
                        sum(
                            (i % 3) * 0.5
                            for i in range(count)
                            if i % 3 == k and i % 7 != 0
                        ),
                    )
                    for k in range(3)
                ]
                # Streaming delivery can return an explicit larger LIMIT
                # while keeping each wire response page bounded.
                large_seen = [
                    row[0]
                    for row in cursor.stream(
                        "SELECT amount FROM lake_large LIMIT 8192 OFFSET 1000", size=137
                    )
                ]
                assert large_seen == list(range(1000, 9192))
                wide_seen = list(
                    cursor.stream(
                        "SELECT amount, payload, amount + 1 FROM lake_large LIMIT 8192 OFFSET 1000",
                        size=137,
                    )
                )
                assert len(wide_seen) == 8192
                for index, (amount, payload, incremented) in enumerate(wide_seen, 1000):
                    assert amount == index and incremented == index + 1
                    assert payload == "".join(
                        hashlib.sha256(f"{index}:{j}".encode()).hexdigest()
                        for j in range(4)
                    )
                grouped = list(
                    cursor.stream(
                        "SELECT amount % 100 AS bucket, COUNT(*), SUM(amount), 100 / (amount % 100) "
                        "FROM lake_large GROUP BY amount % 100 HAVING amount % 100 > 0 ORDER BY bucket",
                        size=17,
                    )
                )
                assert grouped == [
                    (bucket, 100, 495000 + 100 * bucket, 100 // bucket)
                    for bucket in range(1, 100)
                ]
                projected = [
                    row[0]
                    for row in cursor.stream(
                        "SELECT 1 / (amount - 1) FROM lake_large LIMIT 8192 OFFSET 2",
                        size=137,
                    )
                ]
                assert len(projected) == 8192
                assert projected[0] == 1 and projected[-1] == 0
                seen = [
                    row[0]
                    for row in cursor.stream(
                        "SELECT amount FROM lake_events ORDER BY amount DESC",
                        size=37,
                    )
                ]
                assert seen == list(reversed(range(count)))
                # Native execution may evaluate ahead of wire delivery. A later
                # bad lane must follow the valid prefix, including after restart.
                rows = cursor.stream("SELECT 1 / (1 - amount) FROM lake_events", size=1)
                assert next(rows) == (1,)
                with pytest.raises(psycopg.errors.DivisionByZero):
                    next(rows)
                cursor.execute("SELECT COUNT(*) FROM lake_events")
                assert cursor.fetchone() == (count,)

        # LIMIT must not fail because speculative lookahead sees an oversized
        # later page; consuming that page must still enforce the decode budget.
        large_root = tmp_path / "large_pages"
        large_objects = large_root / "buckets" / "antfly" / "objects"
        large_objects.mkdir(parents=True)
        large_input = tmp_path / "large.parquet"
        pq.write_table(
            pa.table({"label": ["first", "x" * (40 * 1024 * 1024)]}),
            large_input,
            compression=None,
            use_dictionary=False,
            data_page_size=1,
            write_batch_size=1,
            write_statistics=False,
            data_page_version="2.0",
        )
        large_payload = large_input.read_bytes()
        large_envelope = (
            b"AFOBJ001"
            + struct.pack("<QI", len(large_payload), 0)
            + hashlib.sha256(large_payload).hexdigest().encode()
        )
        (large_objects / "part.parquet").write_bytes(large_envelope + large_payload)
        del large_payload
        request(
            "POST",
            "/tables/lake_large_pages",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "large-pages",
                        "format": "parquet",
                        "uri": large_root.as_uri(),
                    },
                },
            },
        )
        deadline = time.monotonic() + 30
        while True:
            first = requests.post(
                server.api_url + "/sql",
                json={"statement": "SELECT label FROM lake_large_pages LIMIT 1"},
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            if first.ok:
                assert first.json()["rows"] == [["first"]]
                break
            assert first.status_code in (404, 409, 503), (
                first.text + "\n" + server.log_path.read_text()[-8000:]
            )
            assert time.monotonic() < deadline, first.text
            time.sleep(0.1)
        oversized = requests.post(
            server.api_url + "/sql",
            json={"statement": "SELECT label FROM lake_large_pages LIMIT 1 OFFSET 1"},
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=60,
        )
        assert oversized.status_code >= 400
        response = requests.post(
            server.api_url + "/sql",
            json={"statement": "DELETE FROM lake_events"},
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=60,
        )
        assert response.status_code >= 400
        assert response.json()["code"] == "25006"
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_inline_aggregate_catalog_exceeds_legacy_declaration_limit(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary}")
    lake = tmp_path / "lake"
    objects = lake / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    pq.write_table(pa.table({"amount": [1, 2, 3]}), tmp_path / "input.parquet")
    payload = (tmp_path / "input.parquet").read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
        + payload
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:
        auth = ("admin", AUTH_BOOTSTRAP_PASSWORD)
        indexes = {
            f"stats{i}": {
                "type": "algebraic",
                "derive_from_schema": True,
                "aggregates": [{"name": f"count{j}", "op": "count"} for j in range(64)],
            }
            for i in range(5)
        }
        response = requests.post(
            server.api_url + "/tables/capacity_lake",
            auth=auth,
            timeout=60,
            json={
                "num_shards": 1,
                "indexes": indexes,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "capacity-lake",
                        "format": "parquet",
                        "uri": lake.as_uri(),
                    },
                },
            },
        )
        assert response.status_code == 200, response.text + server.debug_logs()
        deadline = time.monotonic() + 60
        while True:
            response = requests.get(
                server.api_url + "/tables/capacity_lake/indexes", auth=auth, timeout=60
            )
            assert response.ok, response.text
            resources = response.json()
            assert all(
                item["status"]["readiness"]["state"] != "failed" for item in resources
            ), str(resources) + server.debug_logs()
            if len(resources) == 5 and all(
                item["status"]["readiness"]["queryable"] for item in resources
            ):
                break
            assert time.monotonic() < deadline, str(resources) + server.debug_logs()
            time.sleep(0.1)
        response = requests.post(
            server.api_url + "/sql",
            auth=auth,
            timeout=60,
            json={"statement": "SELECT COUNT(*) FROM capacity_lake"},
        )
        assert response.ok, response.text
        assert response.json()["rows"] == [["3"]]

        def contribution_ids(document):
            records = list(document.get("file_contributions", []))
            for page in document.get("contribution_pages", []):
                candidates = (server.root / "artifacts").rglob(page["checksum"])
                for candidate in candidates:
                    if not candidate.is_file():
                        continue
                    payload = candidate.read_bytes()[-page["byte_len"] :]
                    if hashlib.sha256(payload).hexdigest() == page["checksum"]:
                        values = json.loads(payload)
                        assert 0 < len(values) <= 256
                        records.extend(values)
                        break
                else:
                    pytest.fail(f"Missing authenticated contribution page: {page}")

            def contribution_tree(ref):
                digest = bytes(ref["digest"]).hex()
                for candidate in (server.root / "artifacts").rglob(digest):
                    if candidate.is_file():
                        payload = candidate.read_bytes()[-ref["bytes"] :]
                        if hashlib.sha256(payload).hexdigest() == digest:
                            break
                else:
                    pytest.fail(f"Missing authenticated contribution tree page: {ref}")
                assert payload[:8] == b"AFGPT003"
                assert payload[8] == ref["height"]
                count = int.from_bytes(payload[12:16], "little")
                offset = 56
                for _ in range(count):
                    key_len = int.from_bytes(payload[offset : offset + 4], "little")
                    value_len = int.from_bytes(
                        payload[offset + 4 : offset + 8], "little"
                    )
                    offset += 8
                    key = payload[offset : offset + key_len]
                    offset += key_len
                    value = payload[offset : offset + value_len]
                    offset += value_len
                    if ref["height"]:
                        assert len(value) == 61
                        contribution_tree(
                            {
                                "digest": list(value[:32]),
                                "attempt": list(value[45:61]),
                                "bytes": int.from_bytes(value[32:36], "little"),
                                "height": value[36],
                                "records": int.from_bytes(value[37:45], "little"),
                            }
                        )
                    else:
                        assert len(key) == 32
                        records.append(json.loads(value))
                assert offset == len(payload)

            if document.get("contribution_index"):
                contribution_tree(document["contribution_index"])
            return {item["artifact"]["artifact_id"] for item in records}

        directory_found = False
        old_contribution_ids = set()
        for path in (server.root / "artifacts").rglob("*"):
            if not path.is_file():
                continue
            data = path.read_bytes()
            marker = data.find(b'"format":"native-lake-index-directory-v')
            if marker >= 0:
                document = json.loads(data[marker - 1 :])
                if len(document["declarations"]) == 320:
                    directory_found = True
                    old_contribution_ids = contribution_ids(document)
                    break
        assert directory_found, (
            "320 declarations were not published through an immutable directory"
        )
        assert len(old_contribution_ids) == 320
        pq.write_table(pa.table({"amount": [4, 5]}), tmp_path / "append.parquet")
        appended = (tmp_path / "append.parquet").read_bytes()
        (objects / "part2.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(appended), 0)
            + hashlib.sha256(appended).hexdigest().encode()
            + appended
        )
        deadline = time.monotonic() + 60
        while True:
            response = requests.post(
                server.api_url + "/tables/capacity_lake/indexes/extra",
                auth=auth,
                timeout=60,
                json={
                    "type": "algebraic",
                    "derive_from_schema": True,
                    "aggregates": [{"name": "rows", "op": "count"}],
                },
            )
            if response.status_code != 409:
                break
            assert time.monotonic() < deadline, response.text
            time.sleep(0.1)
        assert response.status_code == 201, response.text
        while True:
            response = requests.get(
                server.api_url + "/tables/capacity_lake/indexes", auth=auth, timeout=60
            )
            assert response.ok, response.text
            resources = response.json()
            assert all(
                item["status"]["readiness"]["state"] != "failed" for item in resources
            ), str(resources) + server.debug_logs()
            if len(resources) == 6 and all(
                item["status"]["readiness"]["queryable"] for item in resources
            ):
                break
            assert time.monotonic() < deadline, str(resources) + server.debug_logs()
            time.sleep(0.1)
        response = requests.post(
            server.api_url + "/sql",
            auth=auth,
            timeout=60,
            json={"statement": "SELECT COUNT(*) FROM capacity_lake"},
        )
        assert response.ok, response.text
        assert response.json()["rows"] == [["5"]]
        reused = False
        for path in (server.root / "artifacts").rglob("*"):
            if not path.is_file():
                continue
            data = path.read_bytes()
            marker = data.find(b'"format":"native-lake-index-directory-v')
            if marker >= 0:
                document = json.loads(data[marker - 1 :])
                if len(document["declarations"]) == 321:
                    ids = contribution_ids(document)
                    assert old_contribution_ids <= ids
                    reused = True
                    break
        assert reused, "Append rebuild did not retain unchanged file contributions"
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize(
    "covering", [False, True], ids=["physical-hydration", "covering-blocks"]
)
def test_native_remote_ordered_index_exact_bounds_and_restart(tmp_path, covering):
    """Real Parquet, native CREATE INDEX, ordered seeks and snapshot-bound paging."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    psycopg = pytest.importorskip("psycopg")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    base = 9007199254740993
    amounts = [base + i % 100 for i in reversed(range(1200))]
    pq.write_table(
        pa.table(
            {
                "amount": pa.array(amounts, type=pa.int64()),
                "label": [f"row-{i}" for i in range(1200)],
            }
        ),
        tmp_path / "input.parquet",
        compression="snappy",
        use_dictionary=True,
        row_group_size=137,
        write_page_index=True,
        data_page_size=256,
        write_batch_size=32,
        data_page_version="2.0",
    )
    payload = (tmp_path / "input.parquet").read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
        + payload
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, pgwire=True)
    failed = True
    try:

        def call(method, path, body=None, lines=False):
            response = requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            assert response.ok, response.text + "\n" + server.debug_logs()
            if lines:
                return [json.loads(line) for line in response.text.splitlines() if line]
            return response.json() if response.content else None

        call(
            "POST",
            "/tables/lake_ordered",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "ordered-events",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                },
            },
        )
        call(
            "POST",
            "/sql",
            {
                "statement": "CREATE INDEX amount_idx ON lake_ordered (amount)"
                + (" INCLUDE (label)" if covering else "")
            },
        )
        deadline = time.monotonic() + 60
        while True:
            resource = call("GET", "/tables/lake_ordered/indexes/amount_idx")
            status = resource["status"]
            if status["readiness"]["queryable"]:
                break
            assert status["readiness"]["state"] != "failed", (
                str(resource) + server.debug_logs()
            )
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        listed = call("GET", "/tables/lake_ordered/indexes")
        assert next(i for i in listed if i["config"]["name"] == "amount_idx")["status"][
            "readiness"
        ]["queryable"]
        version = (
            resource["config"]["schema_version"]
            if "schema_version" in resource["config"]
            else None
        )
        primary = call(
            "POST",
            "/tables/lake_ordered/rows/query",
            {"fields": ["amount"], "limit": 1},
            lines=True,
        )
        version = primary[0]["schema_version"]
        body = {
            "index": "amount_idx",
            "schema_version": version,
            "fields": ["amount", "label"],
            "lower": {"values": [str(base + 17)]},
            "upper": {"values": [str(base + 18)], "inclusive": False},
            "limit": 5,
        }
        collected = []
        while True:
            page = call("POST", "/tables/lake_ordered/rows/query", body, lines=True)
            collected.extend(page)
            if not page:
                break
            assert all(int(row["row"]["amount"]) == base + 17 for row in page)
            assert all(row["cursor"] for row in page)
            body["after"] = page[-1]["cursor"]
        assert len(collected) == 12
        assert len({row["_id"] for row in collected}) == 12
        expected = [[str(base + 17), "12"]]
        assert (
            call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT amount, COUNT(*) FROM lake_ordered WHERE amount = {base + 17} GROUP BY amount"
                },
            )["rows"]
            == expected
        )
        with psycopg.connect(
            host="127.0.0.1",
            port=server.pgwire_port,
            user="admin",
            password=AUTH_BOOTSTRAP_PASSWORD,
            dbname="default",
            sslmode="disable",
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT amount, COUNT(*) FROM lake_ordered WHERE amount = {base + 17} GROUP BY amount"
                )
                assert cursor.fetchall() == [(base + 17, 12)]

        def check_filtered_limits():
            # OFFSET crosses an indexed value boundary; covered-column
            # residuals must still run before LIMIT on either access path.
            bounds = f"amount >= {base + 17} AND amount < {base + 19}"
            assert call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT amount FROM lake_ordered WHERE {bounds} ORDER BY amount LIMIT 3 OFFSET 11"
                },
            )["rows"] == [[str(base + 17)], [str(base + 18)], [str(base + 18)]]
            assert call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT amount FROM lake_ordered WHERE {bounds} AND label = 'row-82' ORDER BY amount LIMIT 1"
                },
            )["rows"] == [[str(base + 17)]]

        check_filtered_limits()
        previous_cursor = collected[0]["cursor"]
        server.restart()
        check_filtered_limits()
        body["after"] = previous_cursor
        resumed = call("POST", "/tables/lake_ordered/rows/query", body, lines=True)
        assert resumed and resumed[0]["_id"] == collected[1]["_id"]
        # A source replacement invalidates publication-bound continuation.
        replacement = pa.table(
            {"amount": pa.array([base + 17], type=pa.int64()), "label": ["replacement"]}
        )
        pq.write_table(replacement, tmp_path / "replacement.parquet")
        payload = (tmp_path / "replacement.parquet").read_bytes()
        (objects / "part.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
            + payload
        )
        response = requests.post(
            server.api_url + "/tables/lake_ordered/rows/query",
            json=body,
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=60,
        )
        assert not response.ok, "Stale native row-index continuation was accepted"
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_native_remote_text_corpus_scores_filters_and_restart(tmp_path):
    """Native BM25 across several corpus segments with physical row hydration."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "text-lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    count = 2300
    base = 9007199254740993
    documents = [
        {
            "body": "needle " * (i % 5 + 1)
            if i in (17, 18, 129, 2055)
            else "filler document",
            "amount": base + i,
            "label": f"row-{i}",
            "dense_native": json.dumps([1 if i in (17, 18, 129, 2055) else -1, 0]),
            "sparse_native": json.dumps(
                {"1": {17: 2, 18: 5, 129: 3, 2055: 4}[i]}
                if i in (17, 18, 129, 2055)
                else {"2": 1}
            ),
        }
        for i in range(count)
    ]
    pq.write_table(
        pa.Table.from_pylist(documents),
        tmp_path / "text.parquet",
        compression="snappy",
        row_group_size=137,
        use_dictionary=True,
        write_page_index=True,
        write_batch_size=32,
        data_page_version="2.0",
    )
    payload = (tmp_path / "text.parquet").read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
        + payload
    )
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
            value = response.json() if response.content else None
            return (
                value["responses"][0]
                if isinstance(value, dict) and "responses" in value
                else value
            )

        call(
            "POST",
            "/tables/lake_text",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "text-events",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                },
                "indexes": {
                    "body_text": {"type": "full_text", "field": "body"},
                    "all_text": {"type": "full_text"},
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
        deadline = time.monotonic() + 60
        while True:
            resource = call("GET", "/tables/lake_text/indexes/body_text")
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        request = {
            "full_text_search": {"match": "needle", "field": "body"},
            "full_text_index": "body_text",
            "limit": 10,
        }
        result = call("POST", "/tables/lake_text/query", request)
        hits = result["hits"]["hits"]
        assert len(hits) == 4, result
        assert {hit["_source"]["label"] for hit in hits} == {
            "row-17",
            "row-18",
            "row-129",
            "row-2055",
        }
        assert all(hit["_score"] > 0 for hit in hits)
        # Native doc-value top-N uses public IDs/cursors and hydrates only the
        # selected page. Public scores still use the complete BM25 corpus.
        native_order = dict(request, full_text_index="all_text", fields=["label"],
            order_by=[{"field": "amount", "desc": True}], profile=True, limit=2)
        ordered = call("POST", "/tables/lake_text/query", native_order)
        assert [h["_source"]["label"] for h in ordered["hits"]["hits"]] == ["row-2055", "row-129"], ordered
        assert ordered["profile"]["sort"]["plan"] == "native_doc_values_top_n", ordered
        assert all(h["_id"].startswith("lake1:") for h in ordered["hits"]["hits"]), ordered
        next_ordered = call("POST", "/tables/lake_text/query", dict(native_order,
            search_after=ordered["hits"]["hits"][-1]["_sort"], remote_snapshot=ordered["remote_snapshot"]))
        assert [h["_source"]["label"] for h in next_ordered["hits"]["hits"]] == ["row-18", "row-17"], next_ordered
        previous_ordered = call("POST", "/tables/lake_text/query", dict(native_order,
            search_before=next_ordered["hits"]["hits"][0]["_sort"], remote_snapshot=ordered["remote_snapshot"]))
        assert [h["_id"] for h in previous_ordered["hits"]["hits"]] == [h["_id"] for h in ordered["hits"]["hits"]], previous_ordered
        # Highlight from the pinned original document, including fields omitted
        # from the result projection. Returned source must stay projected.
        highlight_request = dict(
            request, fields=["label"], highlight={"fields": ["body"]}
        )

        def assert_highlights(response, include_source=True):
            highlighted = response["hits"]["hits"]
            assert [hit["_id"] for hit in highlighted] == [
                hit["_id"] for hit in hits
            ], response
            for hit in highlighted:
                if include_source:
                    assert set(hit["_source"]) == {"label"}, hit
                else:
                    assert not hit.get("_source"), hit
                fragments = hit["_highlights"]["body"]
                assert fragments and any(
                    fragment["text"][span["start"] : span["end"]].lower() == "needle"
                    for fragment in fragments
                    for span in fragment["spans"]
                ), hit

        assert_highlights(call("POST", "/tables/lake_text/query", highlight_request))
        assert_highlights(
            call(
                "POST", "/tables/lake_text/query", dict(highlight_request, highlight={})
            )
        )
        assert_highlights(
            call(
                "POST",
                "/tables/lake_text/query",
                dict(highlight_request, highlight={}, fields=[]),
            ),
            include_source=False,
        )
        assert_highlights(
            call(
                "POST",
                "/tables/lake_text/query",
                dict(highlight_request, full_text_index="all_text"),
            )
        )
        assert_highlights(
            call("POST", "/tables/lake_text/query", dict(highlight_request, fields=[])),
            include_source=False,
        )
        all_highlighted = call(
            "POST", "/tables/lake_text/query", dict(request, highlight={})
        )
        assert all(
            hit["_highlights"]["body"] for hit in all_highlighted["hits"]["hits"]
        ), all_highlighted
        prefix_request = dict(
            request, full_text_search={"prefix": "need", "field": "body"}
        )
        prefix_result = call("POST", "/tables/lake_text/query", prefix_request)
        assert {hit["_id"] for hit in prefix_result["hits"]["hits"]} == {
            hit["_id"] for hit in hits
        }
        absent = call(
            "POST",
            "/tables/lake_text/query",
            dict(request, full_text_search={"term": "absent", "field": "body"}),
        )
        assert absent["hits"]["hits"] == [], absent
        default_result = call(
            "POST",
            "/tables/lake_text/query",
            {
                "full_text_search": {"match": "needle", "field": "body"},
                "full_text_index": "all_text",
                "limit": 10,
            },
        )
        assert [
            (hit["_id"], hit["_score"]) for hit in default_result["hits"]["hits"]
        ] == [(hit["_id"], hit["_score"]) for hit in hits], default_result
        first_page = call("POST", "/tables/lake_text/query", dict(request, limit=1))
        assert first_page["hits"]["hits"][0]["_id"] == hits[0]["_id"], first_page
        second_page = call(
            "POST", "/tables/lake_text/query", dict(request, offset=1, limit=3)
        )
        assert [hit["_id"] for hit in second_page["hits"]["hits"]] == [
            hit["_id"] for hit in hits[1:]
        ], second_page
        dense_request = {
            "embeddings": {"dense_native": [1, 0]},
            "indexes": ["dense_native"],
            "limit": 4,
        }
        dense_result = call("POST", "/tables/lake_text/query", dense_request)
        assert {hit["_source"]["label"] for hit in dense_result["hits"]["hits"]} == {
            "row-17",
            "row-18",
            "row-129",
            "row-2055",
        }, dense_result
        assert all(hit["_score"] > 0.99 for hit in dense_result["hits"]["hits"]), (
            dense_result
        )
        sparse_request = {
            "embeddings": {"sparse_native": {"indices": [1], "values": [2]}},
            "indexes": ["sparse_native"],
            "limit": 10,
        }
        sparse_result = call("POST", "/tables/lake_text/query", sparse_request)
        assert [
            (hit["_source"]["label"], hit["_score"])
            for hit in sparse_result["hits"]["hits"]
        ] == [("row-18", 10), ("row-2055", 8), ("row-129", 6), ("row-17", 4)], (
            sparse_result
        )
        sparse_filtered = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                sparse_request,
                filter_query={"term": {"path": "/amount", "value": base + 17}},
            ),
        )
        assert [hit["_source"]["label"] for hit in sparse_filtered["hits"]["hits"]] == [
            "row-17"
        ], sparse_filtered
        mixed_request = dict(
            request,
            embeddings={
                "dense_native": [1, 0],
                "sparse_native": {"indices": [1], "values": [2]},
            },
            indexes=["dense_native", "sparse_native"],
        )
        mixed_result = call("POST", "/tables/lake_text/query", mixed_request)
        assert {hit["_source"]["label"] for hit in mixed_result["hits"]["hits"]} >= {
            "row-17",
            "row-18",
            "row-129",
            "row-2055",
        }, mixed_result
        assert all(hit["_score"] > 0 for hit in mixed_result["hits"]["hits"]), (
            mixed_result
        )
        # Final-page typed hydration must preserve exact source values and
        # keep highlight-only dependencies out of the public projection,
        # including after text/dense/sparse fusion.
        typed_request = dict(
            request, fields=["label", "amount"], highlight={"fields": ["body"]}
        )

        def assert_typed_page(response):
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                assert set(source) == {"label", "amount"}, hit
                assert source["amount"] == base + int(
                    source["label"].removeprefix("row-")
                ), hit
                if source["label"] in {"row-17", "row-18", "row-129", "row-2055"}:
                    assert hit["_highlights"]["body"], hit

        assert_typed_page(call("POST", "/tables/lake_text/query", typed_request))
        assert_typed_page(
            call(
                "POST",
                "/tables/lake_text/query",
                dict(
                    mixed_request,
                    fields=["label", "amount"],
                    highlight={"fields": ["body"]},
                ),
            )
        )
        # Retained column pages must survive subsequent cursor pulls and
        # hydration batches. Dictionary strings and exact integers stay paired
        # with their hit after more than two 256-row batches have advanced.
        broad = call("POST", "/tables/lake_text/query", {
            "full_text_search": {"match": "filler", "field": "body"},
            "full_text_index": "body_text", "fields": ["label", "amount", "body"],
            "highlight": {"fields": ["body"]}, "limit": 600})
        # The native column path must stream the normal JSON envelope. Inspect
        # framing as well as parsed values so a buffered fallback cannot pass.
        streamed = requests.post(server.api_url + "/tables/lake_text/query", json={
            "full_text_search": {"match": "filler", "field": "body"},
            "full_text_index": "body_text", "fields": ["label", "amount", "body"],
            "highlight": {"fields": ["body"]}, "limit": 600},
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD), timeout=60, stream=True)
        with streamed:
            assert streamed.ok, streamed.text
            assert "chunked" in streamed.headers.get("Transfer-Encoding", "").lower(), streamed.headers
            chunks = list(streamed.iter_content(chunk_size=4096))
            assert len(chunks) > 1
            delivered = json.loads(b"".join(chunks))["responses"][0]
            assert delivered["hits"] == broad["hits"]
        assert len(broad["hits"]["hits"]) == 600, broad
        assert len({hit["_id"] for hit in broad["hits"]["hits"]}) == 600
        for hit in broad["hits"]["hits"]:
            source = hit["_source"]
            assert set(source) == {"label", "amount", "body"}, hit
            assert source["amount"] == base + int(source["label"].removeprefix("row-")), hit
            assert source["body"] == "filler document", hit
            assert hit["_highlights"]["body"], hit
        # A rare-term disjunction ranks scattered rows before common rows.
        # Physical hydration must scatter bounded pages back into score order.
        ranked = call(
            "POST", "/tables/lake_text/query",
            {"full_text_search": {"disjuncts": [
                {"match": "needle", "field": "body"},
                {"match": "filler", "field": "body"}]},
             "full_text_index": "body_text", "fields": ["label", "amount"],
             "order_by": [{"field": "_score", "desc": True}], "limit": 600})
        ranked_hits = ranked["hits"]["hits"]
        assert len(ranked_hits) == 600, ranked
        ranked_ids = [hit["_id"] for hit in ranked_hits]
        assert len(set(ranked_ids)) == 600
        assert ranked_ids != sorted(ranked_ids)
        ranked_scores = [hit["_score"] for hit in ranked_hits]
        assert ranked_scores == sorted(ranked_scores, reverse=True)
        for hit in ranked_hits:
            row = int(hit["_source"]["label"].removeprefix("row-"))
            assert hit["_source"]["amount"] == base + row, hit
        ordered_request = dict(request, order_by=[{"field": "_score", "desc": True}])
        ordered_first = call(
            "POST", "/tables/lake_text/query", dict(ordered_request, limit=1)
        )
        # Structured filters resolve against the pinned lake before ranking.
        # They may reference a field omitted from both the text index and source.
        filtered_request = dict(
            ordered_request,
            fields=["label"],
            filter_query={"term": {"path": "/amount", "value": base + 17}},
        )
        ordered_filtered = call("POST", "/tables/lake_text/query", filtered_request)
        assert [
            hit["_source"]["label"] for hit in ordered_filtered["hits"]["hits"]
        ] == ["row-17"]
        assert ordered_filtered["hits"]["total"] == {"value": 1, "relation": "exact"}
        unordered_request = dict(filtered_request)
        unordered_request.pop("order_by")
        unordered_filtered = call("POST", "/tables/lake_text/query", unordered_request)
        assert [hit["_id"] for hit in unordered_filtered["hits"]["hits"]] == [
            hit["_id"] for hit in ordered_filtered["hits"]["hits"]
        ]
        assert all(
            set(hit["_source"]) == {"label"} for hit in ordered_filtered["hits"]["hits"]
        )
        empty_filtered = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                filtered_request,
                filter_query={"term": {"path": "/amount", "value": base - 1}},
            ),
        )
        assert empty_filtered["hits"]["hits"] == []
        excluded = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                ordered_request,
                exclusion_query={"term": {"path": "/amount", "value": base + 17}},
            ),
        )
        assert [hit["_id"] for hit in excluded["hits"]["hits"]] == [
            hit["_id"] for hit in hits if hit["_source"]["label"] != "row-17"
        ]
        subset = dict(
            ordered_request,
            filter_query={
                "disjuncts": [
                    {"term": {"path": "/amount", "value": base + 17}},
                    {"term": {"path": "/amount", "value": base + 18}},
                ]
            },
        )
        subset_hits = call("POST", "/tables/lake_text/query", subset)["hits"]["hits"]
        subset_page = call("POST", "/tables/lake_text/query", dict(subset, limit=1))
        subset_next = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                subset,
                limit=1,
                search_after=subset_page["hits"]["hits"][0]["_sort"],
                remote_snapshot=subset_page["remote_snapshot"],
            ),
        )
        assert [hit["_id"] for hit in subset_next["hits"]["hits"]] == [
            subset_hits[1]["_id"]
        ]
        snapshot_token = ordered_first["remote_snapshot"]
        assert len(snapshot_token) == 64, ordered_first
        sort_tuple = ordered_first["hits"]["hits"][0]["_sort"]
        ordered_next = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                ordered_request, search_after=sort_tuple, remote_snapshot=snapshot_token
            ),
        )
        assert [hit["_id"] for hit in ordered_next["hits"]["hits"]] == [
            hit["_id"] for hit in hits[1:]
        ], ordered_next
        # Pagination and native/residual filters retain column delivery without
        # exposing predicate/highlight dependency fields in the projection.
        for payload, labels in [
            (dict(typed_request, order_by=[{"field": "_score", "desc": True}],
                  search_after=sort_tuple, remote_snapshot=snapshot_token),
             {hit["_source"]["label"] for hit in ordered_next["hits"]["hits"]}),
            (dict(typed_request, order_by=[{"field": "_score", "desc": True}],
                  search_before=ordered_next["hits"]["hits"][0]["_sort"], remote_snapshot=snapshot_token),
             {ordered_first["hits"]["hits"][0]["_source"]["label"]}),
            (dict(typed_request, fields=["label"], filter_query={"term": {"path": "/amount", "value": base + 17}}), {"row-17"}),
            (dict(typed_request, fields=["label"], exclusion_query={"term": {"path": "/amount", "value": base + 17}}), {"row-18", "row-129", "row-2055"}),
        ]:
            with requests.post(server.api_url + "/tables/lake_text/query", json=payload,
                               auth=("admin", AUTH_BOOTSTRAP_PASSWORD), timeout=60, stream=True) as response:
                assert response.ok, response.text
                assert "chunked" in response.headers.get("Transfer-Encoding", "").lower(), response.headers
                page = response.json()["responses"][0]
                for hit in page["hits"]["hits"]:
                    assert set(hit["_source"]) == set(payload["fields"]), hit
                    assert hit["_highlights"]["body"], hit
                    if "amount" in hit["_source"]:
                        assert hit["_source"]["amount"] == base + int(hit["_source"]["label"].removeprefix("row-")), hit
                assert {hit["_source"]["label"] for hit in page["hits"]["hits"]} == labels, page
        unfenced = requests.post(
            server.api_url + "/tables/lake_text/query",
            json=dict(ordered_request, search_after=sort_tuple),
            timeout=60,
        )
        assert unfenced.status_code == 409, unfenced.text
        stale = requests.post(
            server.api_url + "/tables/lake_text/query",
            json=dict(
                ordered_request, search_after=sort_tuple, remote_snapshot="0" * 64
            ),
            timeout=60,
        )
        assert stale.status_code == 409, stale.text
        filtered = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                request, filter_query={"term": {"path": "/amount", "value": base + 17}}
            ),
        )
        assert [hit["_source"]["label"] for hit in filtered["hits"]["hits"]] == [
            "row-17"
        ], filtered
        adjacent = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                request, filter_query={"term": {"path": "/amount", "value": base + 18}}
            ),
        )
        assert [hit["_source"]["label"] for hit in adjacent["hits"]["hits"]] == [
            "row-18"
        ], adjacent
        server.restart()
        dense_reopened = call("POST", "/tables/lake_text/query", dense_request)
        assert [
            (hit["_id"], hit["_score"]) for hit in dense_reopened["hits"]["hits"]
        ] == [(hit["_id"], hit["_score"]) for hit in dense_result["hits"]["hits"]]
        sparse_reopened = call("POST", "/tables/lake_text/query", sparse_request)
        assert [
            (hit["_id"], hit["_score"]) for hit in sparse_reopened["hits"]["hits"]
        ] == [(hit["_id"], hit["_score"]) for hit in sparse_result["hits"]["hits"]]
        assert_highlights(call("POST", "/tables/lake_text/query", highlight_request))
        assert_typed_page(call("POST", "/tables/lake_text/query", typed_request))
        warm = call("POST", "/tables/lake_text/query", request)
        assert [(hit["_id"], hit["_score"]) for hit in warm["hits"]["hits"]] == [
            (hit["_id"], hit["_score"]) for hit in hits
        ]
        prefix_reopened = call("POST", "/tables/lake_text/query", prefix_request)
        assert [
            (hit["_id"], hit["_score"]) for hit in prefix_reopened["hits"]["hits"]
        ] == [(hit["_id"], hit["_score"]) for hit in prefix_result["hits"]["hits"]]
        continued = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                ordered_request, search_after=sort_tuple, remote_snapshot=snapshot_token
            ),
        )
        assert [hit["_id"] for hit in continued["hits"]["hits"]] == [
            hit["_id"] for hit in hits[1:]
        ], continued
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize("covering", [False, True])
def test_native_remote_signed_timestamp_index(tmp_path, covering):
    """Independent Parquet timestamps retain nanoseconds on both sides of 1970."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "signed-timestamp-lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "ts": pa.array([1, -1, 0, -1000000000], type=pa.timestamp("ns")),
                "label": ["positive", "negative", "epoch", "earlier"],
            }
        ),
        tmp_path / "input.parquet",
        data_page_version="2.0",
        use_dictionary=False,
    )
    data = (tmp_path / "input.parquet").read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001"
        + struct.pack("<QI", len(data), 0)
        + hashlib.sha256(data).hexdigest().encode()
        + data
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True

    def call(method, path, body=None, lines=False):
        response = requests.request(
            method,
            server.api_url + path,
            json=body,
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=60,
        )
        assert response.ok, response.text + server.debug_logs()
        return (
            [json.loads(line) for line in response.text.splitlines() if line]
            if lines
            else response.json()
        )

    try:
        call(
            "POST",
            "/tables/lake_epoch",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "epoch",
                        "format": "parquet",
                        "uri": root.as_uri(),
                        "object_mutability": "immutable",
                    },
                },
            },
        )
        statement = "SELECT ts, label FROM lake_epoch ORDER BY ts LIMIT 3"
        expected = call("POST", "/sql", {"statement": statement})["rows"]
        assert [row[1] for row in expected] == ["earlier", "negative", "epoch"]
        call(
            "POST",
            "/sql",
            {
                "statement": "CREATE INDEX ts_idx ON lake_epoch (ts)"
                + (" INCLUDE (label)" if covering else "")
            },
        )
        deadline = time.monotonic() + 60
        while True:
            resource = call("GET", "/tables/lake_epoch/indexes/ts_idx")
            readiness = resource["status"]["readiness"]
            if readiness["queryable"]:
                break
            assert readiness["state"] != "failed", str(resource) + server.debug_logs()
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        assert call("POST", "/sql", {"statement": statement})["rows"] == expected
        primary = call(
            "POST",
            "/tables/lake_epoch/rows/query",
            {"fields": ["ts"], "limit": 1},
            lines=True,
        )
        rows = call(
            "POST",
            "/tables/lake_epoch/rows/query",
            {
                "index": "ts_idx",
                "schema_version": primary[0]["schema_version"],
                "fields": ["ts", "label"],
                "lower": {"values": ["-1"]},
                "upper": {"values": ["0"], "inclusive": False},
                "limit": 5,
            },
            lines=True,
        )
        assert len(rows) == 1 and rows[0]["row"]["label"] == "negative"
        server.restart()
        assert call("POST", "/sql", {"statement": statement})["rows"] == expected
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_native_remote_incremental_generations_keep_public_identity_and_file_artifacts(
    tmp_path,
):
    """Append, replace, remove, and reintroduce real Parquet through all native consumers."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "incremental-lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)

    def put_file(name, amount):
        path = tmp_path / name
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "body": "needle",
                        "amount": amount,
                        "dense_native": "[1,0]",
                        "sparse_native": '{"1":2}',
                    }
                ]
            ),
            path,
            compression="snappy",
            use_dictionary=False,
            data_page_version="2.0",
        )
        payload = path.read_bytes()
        (objects / name).write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
            + payload
        )

    put_file("part.parquet", 1)
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
            value = response.json() if response.content else None
            return (
                value["responses"][0]
                if isinstance(value, dict) and "responses" in value
                else value
            )

        call(
            "POST",
            "/tables/incremental",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "incremental",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {
                            "name": "amount_idx",
                            "keys": [{"column": "amount"}],
                            "include_columns": ["body"],
                        }
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
                    "group_stats": {
                        "type": "algebraic",
                        "derive_from_schema": True,
                        "aggregates": [
                            {"name": "rows", "op": "count", "group_by": ["amount"]},
                            *[
                                {
                                    "name": op,
                                    "op": op,
                                    "measure": "amount",
                                    "group_by": ["amount"],
                                }
                                for op in ("sum", "min", "max")
                            ],
                        ],
                    },
                },
            },
        )

        def wait_ready():
            deadline = time.monotonic() + 90
            while True:
                resources = call("GET", "/tables/incremental/indexes")
                assert all(
                    item["status"]["readiness"]["state"] != "failed"
                    for item in resources
                ), str(resources) + server.debug_logs()
                if all(item["status"]["readiness"]["queryable"] for item in resources):
                    return
                assert time.monotonic() < deadline, str(resources) + server.debug_logs()
                time.sleep(0.1)

        def rebuild(number):
            # A declaration change schedules an authenticated rebuild against the fresh source.
            deadline = time.monotonic() + 90
            while True:
                response = requests.post(
                    server.api_url + f"/tables/incremental/indexes/wake_{number}",
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=60,
                    json={
                        "type": "algebraic",
                        "derive_from_schema": True,
                        "aggregates": [{"name": "rows", "op": "count"}],
                    },
                )
                if response.status_code != 409:
                    assert response.status_code == 201, (
                        response.text + server.debug_logs()
                    )
                    break
                assert time.monotonic() < deadline, response.text
                time.sleep(0.1)
            wait_ready()

        text_request = {
            "full_text_search": {"match": "needle", "field": "body"},
            "full_text_index": "body_text",
            "limit": 20,
            "order_by": [{"field": "_id", "desc": False}],
        }
        dense_request = {
            "embeddings": {"dense_native": [1, 0]},
            "indexes": ["dense_native"],
            "limit": 20,
        }
        sparse_request = {
            "embeddings": {"sparse_native": {"indices": [1], "values": [2]}},
            "indexes": ["sparse_native"],
            "limit": 20,
        }

        def verify(expected):
            results = [
                call("POST", "/tables/incremental/query", request)
                for request in (text_request, dense_request, sparse_request)
            ]
            for result in results:
                hits = result["hits"]["hits"]
                assert {hit["_source"]["amount"] for hit in hits} == set(expected), (
                    result
                )
                assert all(hit["_id"].startswith("lake1:") for hit in hits), result
            assert (
                {hit["_id"] for hit in results[0]["hits"]["hits"]}
                == {hit["_id"] for hit in results[1]["hits"]["hits"]}
                == {hit["_id"] for hit in results[2]["hits"]["hits"]}
            )
            ordered = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount, body FROM incremental ORDER BY amount LIMIT 20"
                },
            )
            assert ordered["rows"] == [
                [str(amount), "needle"] for amount in sorted(expected)
            ], ordered
            grouped = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM incremental GROUP BY amount ORDER BY amount"
                },
            )
            assert grouped["rows"] == [
                [str(amount), "1", str(amount), str(amount), str(amount)]
                for amount in sorted(expected)
            ], grouped
            public_id = results[0]["hits"]["hits"][0]["_id"]
            for request in (text_request, dense_request, sparse_request):
                filtered = call(
                    "POST",
                    "/tables/incremental/query",
                    dict(request, filter_query={"doc_id": [public_id]}),
                )
                assert [hit["_id"] for hit in filtered["hits"]["hits"]] == [
                    public_id
                ], filtered
            return results

        def native_roots():
            roots = []
            for path in (server.root / "artifacts").rglob("*"):
                if not path.is_file():
                    continue
                payload = path.read_bytes()
                begin = payload.find(b'{"version":')
                if begin < 0:
                    continue
                try:
                    document = json.loads(payload[begin:])
                except (ValueError, UnicodeDecodeError):
                    continue
                if "file_groups" in document or "file_states" in document:
                    roots.append(document)
            return roots

        wait_ready()
        initial = verify([1])
        old_roots = native_roots()
        assert len(old_roots) >= 3
        retained = set()
        for root_document in old_roots:
            retained.update(
                segment["artifact_id"] for segment in root_document.get("segments", [])
            )
            for file in root_document.get("file_states", []):
                retained.update(ref["artifact_id"] for ref in file["docs"])
        assert len(retained) >= 2
        put_file("part2.parquet", 2)
        rebuild(1)
        appended = verify([1, 2])
        assert appended[0]["remote_snapshot"] != initial[0]["remote_snapshot"]
        reused = set()
        for root_document in native_roots():
            if (
                len(
                    root_document.get(
                        "file_groups", root_document.get("file_states", [])
                    )
                )
                != 2
            ):
                continue
            reused.update(
                segment["artifact_id"] for segment in root_document.get("segments", [])
            )
            for file in root_document.get("file_states", []):
                reused.update(ref["artifact_id"] for ref in file["docs"])
        assert len(retained & reused) >= 2, (
            "Unchanged text and dense/sparse document-list artifacts were rebuilt"
        )
        put_file("part2.parquet", 3)
        rebuild(2)
        verify([1, 3])
        (objects / "part.parquet").unlink()
        rebuild(3)
        verify([3])
        put_file("part.parquet", 1)
        rebuild(4)
        before_restart = verify([1, 3])
        server.restart()
        after_restart = verify([1, 3])
        assert [
            [hit["_id"] for hit in result["hits"]["hits"]] for result in after_restart
        ] == [
            [hit["_id"] for hit in result["hits"]["hits"]] for result in before_restart
        ]
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize("mode", ["single", "shared", "all"])
def test_native_nullable_parquet_columns_and_projected_hydration(tmp_path, mode):
    """Schema evolution supplies typed NULLs to every native producer."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "nullable-lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    for name, values in [
        (
            "a.parquet",
            {
                "amount": [1],
                "body": ["needle"],
                "dense_native": ["[1,0]"],
                "sparse_native": ['{"1":2}'],
            },
        ),
        ("b.parquet", {"amount": [2]}),
        (
            "c.parquet",
            {
                "amount": [3],
                "body": ["other"],
                "dense_native": ["[0,1]"],
                "sparse_native": ['{"2":3}'],
            },
        ),
    ]:
        path = tmp_path / name
        pq.write_table(
            pa.table(values),
            path,
            compression="snappy",
            use_dictionary=False,
            data_page_version="2.0",
        )
        data = path.read_bytes()
        (objects / name).write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(data), 0)
            + hashlib.sha256(data).hexdigest().encode()
            + data
        )
    indexes = {"body_text": {"type": "full_text", "field": "body"}}
    schema = {
        "storage_mode": "relational",
        "base_source": {
            "kind": "external",
            "table_id": "nullable",
            "format": "parquet",
            "uri": root.as_uri(),
        },
    }
    if mode != "single":
        indexes["all_text"] = {"type": "full_text"}
    if mode == "all":
        indexes.update(
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
        )
        schema["relational_indexes"] = [
            {
                "name": "body_idx",
                "keys": [{"column": "body"}],
                "include_columns": ["amount"],
            }
        ]
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
            value = response.json() if response.content else None
            return (
                value["responses"][0]
                if isinstance(value, dict) and "responses" in value
                else value
            )

        call(
            "POST",
            "/tables/nullable",
            {"num_shards": 1, "schema": schema, "indexes": indexes},
        )
        deadline = time.monotonic() + 60
        while True:
            status = call("GET", "/tables/nullable/indexes/body_text")["status"]
            if status["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(status) + server.debug_logs()
            time.sleep(0.1)
        text = {
            "full_text_search": {"match": "needle", "field": "body"},
            "full_text_index": "body_text",
            "fields": ["amount"],
            "limit": 10,
        }
        response = call("POST", "/tables/nullable/query", text)
        assert [hit["_source"] for hit in response["hits"]["hits"]] == [
            {"amount": 1}
        ], response
        excluded = call("POST", "/tables/nullable/query", dict(text, fields=["-body"]))
        source = excluded["hits"]["hits"][0]["_source"]
        assert (
            source["amount"] == 1 and "dense_native" in source and "body" not in source
        )
        if mode == "all":
            for name, vector in [
                ("dense_native", [1, 0]),
                ("sparse_native", {"indices": [1], "values": [2]}),
            ]:
                response = call(
                    "POST",
                    "/tables/nullable/query",
                    {
                        "embeddings": {name: vector},
                        "indexes": [name],
                        "fields": ["amount"],
                        "limit": 1,
                    },
                )
                assert [hit["_source"] for hit in response["hits"]["hits"]] == [
                    {"amount": 1}
                ], response
        failed = False
    finally:
        if failed:
            print(server.debug_logs())
        server.stop(test_failed=failed)


@pytest.mark.parametrize("dictionary", [False, True])
@pytest.mark.parametrize("page_version", ["1.0", "2.0"])
def test_vector_only_remote_table_supports_index_independent_queries(
    tmp_path, dictionary, page_version
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    path = tmp_path / "input.parquet"
    pq.write_table(
        pa.table({"n": [1, None, 3], "dense_native": ["[1,0]", None, "[0,1]"]}),
        path,
        compression="snappy",
        use_dictionary=dictionary,
        data_page_version=page_version,
    )
    payload = path.read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
        + payload
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:

        def call(method, route, body=None):
            response = requests.request(
                method,
                server.api_url + route,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
            assert response.ok, response.text + "\n" + server.debug_logs()
            value = response.json()
            return value["responses"][0] if "responses" in value else value

        call(
            "POST",
            "/tables/vector_only",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "vector-only",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                },
                "indexes": {
                    "dense_native": {
                        "type": "embeddings",
                        "external": True,
                        "dimension": 2,
                    }
                },
            },
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            status = call("GET", "/tables/vector_only/indexes/dense_native")
            if status.get("status", {}).get("readiness", {}).get("queryable"):
                break
            time.sleep(0.1)
        else:
            pytest.fail(str(status) + "\n" + server.debug_logs())
        empty = call(
            "POST",
            "/tables/vector_only/query",
            {"full_text_search": {"match_none": {}}},
        )
        assert empty["hits"]["hits"] == [], empty
        all_rows = call(
            "POST",
            "/tables/vector_only/query",
            {"full_text_search": {"match_all": {}}, "limit": 10},
        )
        assert len(all_rows["hits"]["hits"]) == 3, all_rows
        assert {hit["_source"].get("n") for hit in all_rows["hits"]["hits"]} == {
            1,
            None,
            3,
        }, all_rows
        vector = call(
            "POST",
            "/tables/vector_only/query",
            {
                "embeddings": {"dense_native": [1, 0]},
                "indexes": ["dense_native"],
                "limit": 1,
            },
        )
        assert vector["hits"]["hits"][0]["_source"]["n"] == 1, vector
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_native_remote_large_hydration_and_residual_filter(tmp_path):
    """Physical selection windows are not a public result/candidate cap."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "large-selection"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    count = 65537
    input_file = tmp_path / "selection.parquet"
    pq.write_table(
        pa.table({"body": ["common"] * count, "amount": range(count)}),
        input_file, row_group_size=4096, use_dictionary=True,
        compression="snappy", write_page_index=True,
    )
    payload = input_file.read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001" + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode() + payload
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:
        def call(method, path, body=None):
            response = requests.request(
                method, server.api_url + path, json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD), timeout=180,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json()

        call("POST", "/tables/large_selection", {
            "num_shards": 1,
            "schema": {"storage_mode": "relational", "base_source": {
                "kind": "external", "table_id": "large-selection",
                "format": "parquet", "uri": root.as_uri(),
            }},
            "indexes": {"body_text": {"type": "full_text", "field": "body"}},
        })
        deadline = time.monotonic() + 180
        while True:
            resource = call("GET", "/tables/large_selection/indexes/body_text")
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        query = {"full_text_search": {"match_all": {}}, "fields": ["amount"], "limit": count}
        result = call("POST", "/tables/large_selection/query", query)["responses"][0]
        hits = result["hits"]["hits"]
        assert len(hits) == count
        assert len({hit["_id"] for hit in hits}) == count
        assert {hit["_source"]["amount"] for hit in hits} == set(range(count))
        # The unindexed numeric field forces complete-source residual filtering
        # over all candidates BEFORE the small final page is selected.
        filtered = call("POST", "/tables/large_selection/query", dict(
            query, limit=10, filter_query={"term": {"path": "/amount", "value": count - 1}},
        ))["responses"][0]
        assert [hit["_source"]["amount"] for hit in filtered["hits"]["hits"]] == [count - 1]
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_native_remote_indexed_metadata_predicates_above_id_list_limit(tmp_path):
    """Bounded index seeks and ordinal maps share exact bitmap semantics."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "indexed-predicates"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    count = 100003
    input_file = tmp_path / "predicates.parquet"
    pq.write_table(pa.table({
        "body": ["common"] * count,
        "sparse_native": ['{"1":1}'] * count,
        "amount": range(count),
        "big_integer": [9007199254740992, 9007199254740993] + [9007199254740994] * (count - 2),
        "time_text": ["2026-01-01T01:00:00+01:00", "2026-01-01T00:30:00Z"] + ["2026-01-01T00:00:00Z"] * (count - 2),
        "category": ["story", "story"] + ["comment"] * (count - 2),
        "label": ["other"] * (count - 1) + ["kept"],
    }), input_file, row_group_size=4096, compression="snappy", write_page_index=True)
    payload = input_file.read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001" + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode() + payload
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:
        def call(method, path, body=None):
            response = requests.request(method, server.api_url + path, json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD), timeout=180)
            assert response.ok, response.text + server.debug_logs()
            value = response.json()
            return value["responses"][0] if "responses" in value else value

        call("POST", "/tables/indexed_predicates", {
            "num_shards": 1,
            "schema": {"storage_mode": "relational", "base_source": {
                "kind": "external", "table_id": "indexed-predicates",
                "format": "parquet", "uri": root.as_uri(),
            }, "relational_indexes": [
                {"name": "amount_idx", "keys": [{"column": "amount"}]},
                {"name": "big_integer_idx", "keys": [{"column": "big_integer"}]},
                {"name": "time_text_idx", "keys": [{"column": "time_text"}]},
                {"name": "category_idx", "keys": [{"column": "category"}]},
            ]},
            "indexes": {
                "body_text": {"type": "full_text", "field": "body"},
                "sparse_native": {"type": "embeddings", "external": True, "sparse": True},
            },
        })
        deadline = time.monotonic() + 180
        while True:
            resource = call("GET", "/tables/indexed_predicates/indexes/body_text")
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)

        def query(predicate, **options):
            return call("POST", "/tables/indexed_predicates/query", dict({
                "full_text_search": {"term": "common", "field": "body"},
                "fields": ["amount"], "limit": 10, "filter_query": predicate,
            }, **options))

        point = {"term": {"path": "/amount", "value": count - 1}}
        assert [h["_source"]["amount"] for h in query(point)["hits"]["hits"]] == [count - 1]
        boolean = query({"bool": {"filter": [point]}})
        assert [h["_source"]["amount"] for h in boolean["hits"]["hits"]] == [count - 1]
        assert query({"term": {"path": "/amount", "value": count}})["hits"]["hits"] == []
        ranged = query({"range": {"path": "/amount", "gte": 10, "lt": 15}})
        assert {h["_source"]["amount"] for h in ranged["hits"]["hits"]} == set(range(10, 15))
        for alias in (
            {"term": {"amount": count - 1}},
            {"term": {"field": "amount", "term": count - 1}},
            {"range": {"amount": {"from": count - 1, "to": count, "include_upper": False}}},
            {"range": {"field": "amount", "min": count - 1, "max": count}},
        ):
            assert [h["_source"]["amount"] for h in query(alias)["hits"]["hits"]] == [count - 1]
        precise = {"range": {"big_integer": {"gt": 9007199254740992}}}
        temporal = {"range": {"time_text": {"gte": "2026-01-01T00:15:00Z"}}}
        for predicate, expected in ((precise, count - 1), (temporal, 1)):
            direct = query(predicate, count=True, fields=[], limit=0)
            # Requiring two should clauses exercises the authoritative fallback.
            fallback = query({"bool": {"should": [predicate, {"match_all": {}}],
                "minimum_should_match": 2}}, count=True, fields=[], limit=0)
            assert direct["hits"]["total"] == fallback["hits"]["total"] == {"value": expected, "relation": "exact"}
        broad = {"term": {"path": "/category", "value": "comment"}}
        hits = query(broad)["hits"]["hits"]
        assert len(hits) == 10 and all(h["_source"]["amount"] >= 2 for h in hits)
        counted = query(broad, count=True, fields=[], limit=0)
        assert counted["hits"]["total"] == {"value": count - 2, "relation": "exact"}, counted
        residual = query({"conjuncts": [point, {"prefix": {"path": "/label", "value": "ke"}}]})
        assert [h["_source"]["amount"] for h in residual["hits"]["hits"]] == [count - 1]
        union = query({"disjuncts": [point, {"term": {"path": "/amount", "value": 0}}]})
        assert {h["_source"]["amount"] for h in union["hits"]["hits"]} == {0, count - 1}
        excluded = call("POST", "/tables/indexed_predicates/query", {
            "full_text_search": {"term": "common", "field": "body"},
            "fields": ["amount"], "limit": 10, "exclusion_query": broad,
        })
        assert {h["_source"]["amount"] for h in excluded["hits"]["hits"]} == {0, 1}
        sparse = call("POST", "/tables/indexed_predicates/query", {
            "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
            "indexes": ["sparse_native"], "fields": ["amount"], "limit": 3,
            "filter_query": broad,
        })
        assert len(sparse["hits"]["hits"]) == 3 and all(h["_source"]["amount"] >= 2 for h in sparse["hits"]["hits"]), sparse
        sparse_point = call("POST", "/tables/indexed_predicates/query", {
            "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
            "indexes": ["sparse_native"], "fields": ["amount"], "limit": 3,
            "filter_query": {"term": {"amount": count - 1}},
        })
        assert [h["_source"]["amount"] for h in sparse_point["hits"]["hits"]] == [count - 1], sparse_point
        failed = False
    finally:
        server.stop(test_failed=failed)
