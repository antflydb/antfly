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
                "label": pa.array(
                    [None if i % 7 == 0 else f"row-{i}" for i in range(count)]
                ),
            }
        ),
        tmp_path / "input.parquet",
        compression="snappy",
        use_dictionary=dictionary,
        row_group_size=173,
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
            response = requests.request(
                method,
                server.api_url + path,
                json=payload,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )
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
        sql = "SELECT amount, label FROM lake_events WHERE amount >= 1197 ORDER BY amount DESC"
        expected = [["1199", "row-1199"], ["1198", "row-1198"], ["1197", None]]
        assert request("POST", "/sql", {"statement": sql})["rows"] == expected
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
                seen = [
                    row[0]
                    for row in cursor.stream(
                        "SELECT amount FROM lake_events ORDER BY amount DESC",
                        size=37,
                    )
                ]
                assert seen == list(reversed(range(count)))
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
