# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
"""Independent Parquet -> public attachment -> HTTP/pgwire -> cold restart.

Run: uv run --extra lake --project e2e/antfly pytest e2e/antfly/test_lake_sql.py
"""

import hashlib
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
def test_parquet_attachment_survives_restart_and_streams_over_pgwire(
    tmp_path, dictionary
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
                    [None if i % 7 == 0 else 9007199254740993 + i % 3 for i in range(count)],
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
        data_page_version="2.0",
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
            return response.json() if response.content else {}

        request(
            "POST",
            "/tables/lake_events",
            {
                "num_shards": 1,
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
                        sum((i % 3) * 0.5 for i in range(count) if i % 3 == k and i % 7 != 0),
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
