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
import shutil
import struct
import time
from decimal import Decimal
from pathlib import Path

import pytest
import requests
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
)
from lake_encoded_fixtures import write_compact_parquet, write_null_compact_parquet

pytestmark = pytest.mark.fresh_antfly_process


def _request_with_idle_ddl_retry(server, method, path, body, *, retry_ddl=False):
    """Replay only an explicitly unadmitted DDL while its catalog publishes."""
    deadline = time.monotonic() + 30
    while True:
        response = requests.request(
            method,
            server.api_url + path,
            json=body,
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=60,
        )
        if response.ok or not retry_ddl or response.status_code != 503:
            return response
        error = response.json()
        if (
            error.get("code") != "53300"
            or error.get("retryable") is not True
            or error.get("transaction_status") != "idle"
        ):
            return response
        assert time.monotonic() < deadline, response.text + server.debug_logs()
        time.sleep(0.1)


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
            deadline = time.monotonic() + 30
            while True:
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
                # Background publication may win the catalog CAS before this
                # index creation is admitted. Replay only that explicit conflict.
                if (
                    method == "POST"
                    and "/indexes/" in path
                    and response.status_code == 409
                    and response.json().get("error") == "table_mutation_conflict"
                    and response.json().get("retryable") is True
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.1)
                    continue
                break
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
        # Pause publication/GC while locating and damaging immutable objects.
        # A prior contribution can carry the same SUM recipe in a different
        # fused cohort, so cover every matching root rather than the first one.
        server.pause()
        checksums = set()
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
                    checksums.update(
                        block["artifact"]["checksum"] for block in candidate["blocks"]
                    )
        assert checksums, "Native exact aggregate blocks missing"
        candidates = [
            path
            for checksum in sorted(checksums)
            for path in (artifact_root / "objects").rglob(checksum)
        ]
        assert candidates, "Native aggregate block missing"
        originals = []
        try:
            for artifact_path in candidates:
                original = artifact_path.read_bytes()
                originals.append((artifact_path, original))
                marker = original.index(b"NCB\x01")
                damaged = bytearray(original)
                damaged[marker] ^= 1
                artifact_path.write_bytes(damaged)
            # COUNT can warm a fused block containing SUM, and verified immutable
            # bytes may also survive on disk. Make source-integrity admission
            # explicitly cold instead of treating a valid cache hit as fallback.
            for cache_root in list(server.root.rglob("lake-ranges")):
                if cache_root.is_dir():
                    shutil.rmtree(cache_root)
            server.resume()
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
                assert response.status_code == 500, (
                    "Selected corrupt aggregate did not fail integrity admission: "
                    f"{response.status_code} {response.text}"
                )
            assert request(
                "POST",
                "/sql",
                {"statement": "SELECT SUM(amount) FROM lake_events WHERE amount >= 0"},
            )["rows"] == [[str(amount_sum)]]
        finally:
            server.pause()
            for artifact_path, original in originals:
                # Do not resurrect obsolete generations collected after resume.
                if artifact_path.exists():
                    artifact_path.write_bytes(original)
            server.resume()
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
        # Cost sampling must replay unique keys exactly once; repeated keys
        # exercise batched typed partial reducers over independent Parquet data.
        unique_join = (
            "SELECT COUNT(*), SUM(l.amount), MIN(r.amount) "
            "FROM lake_large l JOIN lake_large r ON l.amount=r.amount"
        )
        assert request("POST", "/sql", {"statement": unique_join})["rows"] == [
            [str(large_count), str(sum(range(large_count))), "0"]
        ]
        compact_join = (
            "SELECT COUNT(*), SUM(l.amount), MIN(r.amount), MAX(l.amount) "
            "FROM (SELECT amount % 11 AS k, amount FROM lake_events) l "
            "JOIN (SELECT amount % 11 AS k, amount FROM lake_events) r ON l.k=r.k"
        )
        cardinalities = [len(range(k, count, 11)) for k in range(11)]
        expected_compact = [
            [
                str(sum(n * n for n in cardinalities)),
                str(
                    sum(sum(range(k, count, 11)) * cardinalities[k] for k in range(11))
                ),
                "0",
                str(count - 1),
            ]
        ]
        assert (
            request("POST", "/sql", {"statement": compact_join})["rows"]
            == expected_compact
        )
        assert (
            request(
                "POST",
                "/sql",
                {
                    "statement": compact_join
                    + " AND (l.amount IS NOT DISTINCT FROM l.amount)"
                },
            )["rows"]
            == expected_compact
        )
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
                    ids = contribution_ids(document)
                    # Retained immutable objects include checkpoint directories
                    # written before contribution publication completes.
                    if len(ids) != 320:
                        continue
                    directory_found = True
                    old_contribution_ids = ids
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
                    # Both source files need every declaration. The immutable
                    # index may also retain contributions from earlier views.
                    if len(ids) < 2 * 321:
                        continue
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

        def call(method, path, body=None, lines=False, retry_ddl=False):
            response = _request_with_idle_ddl_retry(
                server, method, path, body, retry_ddl=retry_ddl
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
            retry_ddl=True,
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
        version = resource["config"].get("schema_version", None)
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
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "amount": {
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
        native_order = dict(
            request,
            full_text_index="all_text",
            fields=["label"],
            order_by=[{"field": "amount", "desc": True}],
            profile=True,
            limit=2,
        )
        ordered = call("POST", "/tables/lake_text/query", native_order)
        assert [h["_source"]["label"] for h in ordered["hits"]["hits"]] == [
            "row-2055",
            "row-129",
        ], ordered
        assert ordered["profile"]["sort"]["plan"] == "native_doc_values_top_n", ordered
        assert all(h["_id"].startswith("lake3:") for h in ordered["hits"]["hits"]), (
            ordered
        )
        next_ordered = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                native_order,
                search_after=ordered["hits"]["hits"][-1]["_sort"],
                remote_snapshot=ordered["remote_snapshot"],
            ),
        )
        assert [h["_source"]["label"] for h in next_ordered["hits"]["hits"]] == [
            "row-18",
            "row-17",
        ], next_ordered
        previous_ordered = call(
            "POST",
            "/tables/lake_text/query",
            dict(
                native_order,
                search_before=next_ordered["hits"]["hits"][0]["_sort"],
                remote_snapshot=ordered["remote_snapshot"],
            ),
        )
        assert [h["_id"] for h in previous_ordered["hits"]["hits"]] == [
            h["_id"] for h in ordered["hits"]["hits"]
        ], previous_ordered
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
        broad = call(
            "POST",
            "/tables/lake_text/query",
            {
                "full_text_search": {"match": "filler", "field": "body"},
                "full_text_index": "body_text",
                "fields": ["label", "amount", "body"],
                "highlight": {"fields": ["body"]},
                "limit": 600,
            },
        )
        # The native column path must stream the normal JSON envelope. Inspect
        # framing as well as parsed values so a buffered fallback cannot pass.
        streamed = requests.post(
            server.api_url + "/tables/lake_text/query",
            json={
                "full_text_search": {"match": "filler", "field": "body"},
                "full_text_index": "body_text",
                "fields": ["label", "amount", "body"],
                "highlight": {"fields": ["body"]},
                "limit": 600,
            },
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=60,
            stream=True,
        )
        with streamed:
            assert streamed.ok, streamed.text
            assert "chunked" in streamed.headers.get("Transfer-Encoding", "").lower(), (
                streamed.headers
            )
            chunks = list(streamed.iter_content(chunk_size=4096))
            assert len(chunks) > 1
            delivered = json.loads(b"".join(chunks))["responses"][0]
            assert delivered["hits"] == broad["hits"]
        assert len(broad["hits"]["hits"]) == 600, broad
        assert len({hit["_id"] for hit in broad["hits"]["hits"]}) == 600
        for hit in broad["hits"]["hits"]:
            source = hit["_source"]
            assert set(source) == {"label", "amount", "body"}, hit
            assert source["amount"] == base + int(
                source["label"].removeprefix("row-")
            ), hit
            assert source["body"] == "filler document", hit
            assert hit["_highlights"]["body"], hit
        # A rare-term disjunction ranks scattered rows before common rows.
        # Physical hydration must scatter bounded pages back into score order.
        ranked = call(
            "POST",
            "/tables/lake_text/query",
            {
                "full_text_search": {
                    "disjuncts": [
                        {"match": "needle", "field": "body"},
                        {"match": "filler", "field": "body"},
                    ]
                },
                "full_text_index": "body_text",
                "fields": ["label", "amount"],
                "order_by": [{"field": "_score", "desc": True}],
                "limit": 600,
            },
        )
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
        # Retained cut capabilities are opaque; replay below proves their
        # identity and persistence without assuming a digest representation.
        assert isinstance(snapshot_token, str) and snapshot_token, ordered_first
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
            (
                dict(
                    typed_request,
                    order_by=[{"field": "_score", "desc": True}],
                    search_after=sort_tuple,
                    remote_snapshot=snapshot_token,
                ),
                {hit["_source"]["label"] for hit in ordered_next["hits"]["hits"]},
            ),
            (
                dict(
                    typed_request,
                    order_by=[{"field": "_score", "desc": True}],
                    search_before=ordered_next["hits"]["hits"][0]["_sort"],
                    remote_snapshot=snapshot_token,
                ),
                {ordered_first["hits"]["hits"][0]["_source"]["label"]},
            ),
            (
                dict(
                    typed_request,
                    fields=["label"],
                    filter_query={"term": {"path": "/amount", "value": base + 17}},
                ),
                {"row-17"},
            ),
            (
                dict(
                    typed_request,
                    fields=["label"],
                    exclusion_query={"term": {"path": "/amount", "value": base + 17}},
                ),
                {"row-18", "row-129", "row-2055"},
            ),
        ]:
            with requests.post(
                server.api_url + "/tables/lake_text/query",
                json=payload,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
                stream=True,
            ) as response:
                assert response.ok, response.text
                assert (
                    "chunked" in response.headers.get("Transfer-Encoding", "").lower()
                ), response.headers
                page = response.json()["responses"][0]
                for hit in page["hits"]["hits"]:
                    assert set(hit["_source"]) == set(payload["fields"]), hit
                    assert hit["_highlights"]["body"], hit
                    if "amount" in hit["_source"]:
                        assert hit["_source"]["amount"] == base + int(
                            hit["_source"]["label"].removeprefix("row-")
                        ), hit
                assert {
                    hit["_source"]["label"] for hit in page["hits"]["hits"]
                } == labels, page
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

    def call(method, path, body=None, lines=False, retry_ddl=False):
        response = _request_with_idle_ddl_retry(
            server, method, path, body, retry_ddl=retry_ddl
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
            retry_ddl=True,
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
                        "body": f"needle marker{amount}",
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
                assert all(hit["_id"].startswith("lake3:") for hit in hits), result
            assert (
                {hit["_id"] for hit in results[0]["hits"]["hits"]}
                == {hit["_id"] for hit in results[1]["hits"]["hits"]}
                == {hit["_id"] for hit in results[2]["hits"]["hits"]}
            )
            # The unchanged first file stays selected while other files change
            # global DF. A cached physical reader must get fresh scoring state.
            selective_query = {
                "conjuncts": [
                    {"match": "marker1", "field": "body"},
                    {"match": "needle", "field": "body"},
                ]
            }
            selective = call(
                "POST",
                "/tables/incremental/query",
                dict(text_request, full_text_search=selective_query),
            )["hits"]["hits"]
            oracle = call(
                "POST",
                "/tables/incremental/query",
                dict(
                    text_request,
                    full_text_search={
                        "disjuncts": [
                            selective_query,
                            {"prefix": "never_present_in_fixture", "field": "body"},
                        ]
                    },
                ),
            )["hits"]["hits"]
            assert [h["_id"] for h in selective] == [h["_id"] for h in oracle]
            assert len(selective) == int(1 in expected)
            if selective:
                assert selective[0]["_score"] == pytest.approx(
                    oracle[0]["_score"], abs=1e-6
                )
            ordered = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount, body FROM incremental ORDER BY amount LIMIT 20"
                },
            )
            assert ordered["rows"] == [
                [str(amount), f"needle marker{amount}"] for amount in sorted(expected)
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
            filtered_groups = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount, SUM(amount), COUNT(*) FILTER (WHERE amount < 0) FROM incremental WHERE amount >= 0 GROUP BY amount ORDER BY amount"
                },
            )
            assert filtered_groups["rows"] == [
                [str(amount), str(amount), "0"]
                for amount in sorted(expected)
                if amount >= 0
            ], filtered_groups
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
            roots, manifests, pages = [], {}, {}
            for path in (server.root / "artifacts").rglob("*"):
                if not path.is_file():
                    continue
                payload = path.read_bytes()
                begin = payload.find(b"AFGPT003")
                if begin >= 0:
                    encoded = payload[begin:]
                    pages[hashlib.sha256(encoded).digest()] = encoded
                for prefix in (b'{"version":', b'{"file":'):
                    begin = payload.find(prefix)
                    if begin < 0:
                        continue
                    encoded = payload[begin:]
                    try:
                        document = json.loads(encoded)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if "file" in document and "segments" in document:
                        manifests[hashlib.sha256(encoded).hexdigest()] = (
                            document,
                            len(encoded),
                        )
                    elif "file_groups" in document or "file_states" in document:
                        roots.append(document)

            def records(ref, domain):
                page = pages[bytes(ref["digest"])]
                assert len(page) == ref["bytes"]
                assert page[:8] == b"AFGPT003" and page[8] == ref["height"]
                assert page[24:56] == bytes(domain)
                assert struct.unpack_from("<Q", page, 16)[0] == ref["records"]
                count = struct.unpack_from("<I", page, 12)[0]
                offset = 56 + struct.unpack_from("<H", page, 10)[0]
                for _ in range(count):
                    key_size, value_size = struct.unpack_from("<II", page, offset)
                    offset += 8
                    key = page[offset : offset + key_size]
                    offset += key_size
                    value = page[offset : offset + value_size]
                    offset += value_size
                    assert len(key) == key_size and len(value) == value_size
                    if ref["height"]:
                        child = {
                            "digest": list(value[:32]),
                            "bytes": struct.unpack_from("<I", value, 32)[0],
                            "height": value[36],
                            "records": struct.unpack_from("<Q", value, 37)[0],
                        }
                        assert child["height"] + 1 == ref["height"]
                        yield from records(child, domain)
                    else:
                        yield key, value
                assert offset == len(page)

            def manifest(ref):
                group, size = manifests[ref["checksum"].removeprefix("sha256:")]
                assert size == ref["byte_len"]
                return group

            # Resolve both legacy arrays and authenticated paged catalogs.
            # Detailed segment descriptors are not maintenance file manifests.
            for document in roots:
                if document.get("serving"):
                    # The publication carries the prior-snapshot proof used by
                    # append/replacement planning, including after cold restart.
                    assert document.get("inventory_delta_version") == 2, document
                if document.get("inventory_version") == 3:
                    chunks = {}
                    file_root = document["directories"]["files"]
                    for key, value in (
                        records(file_root, document["domain"]) if file_root else ()
                    ):
                        assert len(key) == 36
                        chunk = chunks.setdefault(key[:32], bytearray())
                        assert int.from_bytes(key[32:], "big") == len(chunk)
                        chunk.extend(value)
                    groups = []
                    for identity, encoded in chunks.items():
                        file = json.loads(encoded)
                        assert (
                            hashlib.sha256(file["file"]["id"].encode()).digest()
                            == identity
                        )
                        group = manifest(file["manifest"])
                        assert group["file"]["id"] == file["file"]["id"]
                        assert group["file"]["digest"] == file["file"]["digest"]
                        groups.append(group)
                    assert len(groups) == document["file_count"]
                elif document.get("manifests"):
                    groups = [manifest(ref) for ref in document["manifests"]]
                else:
                    continue
                document["file_groups"] = groups
                document["segments"] = [
                    segment for group in groups for segment in group["segments"]
                ]
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
        verify([1, 3])
        # A previously cached selected first file becomes the full corpus.
        (objects / "part2.parquet").unlink()
        rebuild(5)
        before_restart = verify([1])
        server.restart()
        after_restart = verify([1])
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
        input_file,
        row_group_size=4096,
        use_dictionary=True,
        compression="snappy",
        write_page_index=True,
    )
    payload = input_file.read_bytes()
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
                timeout=180,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json()

        call(
            "POST",
            "/tables/large_selection",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "large-selection",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                },
                "indexes": {"body_text": {"type": "full_text", "field": "body"}},
            },
        )
        deadline = time.monotonic() + 180
        while True:
            resource = call("GET", "/tables/large_selection/indexes/body_text")
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        query = {
            "full_text_search": {"match_all": {}},
            "fields": ["amount"],
            "limit": count,
        }
        result = call("POST", "/tables/large_selection/query", query)["responses"][0]
        hits = result["hits"]["hits"]
        assert len(hits) == count
        assert len({hit["_id"] for hit in hits}) == count
        assert {hit["_source"]["amount"] for hit in hits} == set(range(count))
        # The unindexed numeric field forces complete-source residual filtering
        # over all candidates BEFORE the small final page is selected.
        filtered = call(
            "POST",
            "/tables/large_selection/query",
            dict(
                query,
                limit=10,
                filter_query={"term": {"path": "/amount", "value": count - 1}},
            ),
        )["responses"][0]
        assert [hit["_source"]["amount"] for hit in filtered["hits"]["hits"]] == [
            count - 1
        ]
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
    data_table = pa.table(
        {
            "body": ["common early rarefirst"]
            + ["common early"] * (count - 1001)
            + ["common late"] * 999
            + ["common late rarelast"],
            "sparse_native": ['{"1":1}'] * count,
            "amount": range(count),
            "sort_rank": [0, 1] + list(range(2, count - 1)) + [0],
            "big_integer": [9007199254740992, 9007199254740993]
            + [9007199254740994] * (count - 2),
            "event_time": pa.array(
                [-1, 0, 1] + [2] * (count - 3), type=pa.timestamp("ns", tz="UTC")
            ),
            "time_text": ["2026-01-01T01:00:00+01:00", "2026-01-01T00:30:00Z"]
            + ["2026-01-01T00:00:00Z"] * (count - 2),
            "category": ["story", "story"] + ["comment"] * (count - 2),
            "label": ["other"] * (count - 1) + ["kept"],
        }
    )
    for name, offset, length in (
        ("a", 0, count // 2),
        ("b", count // 2, count - count // 2),
    ):
        pq.write_table(
            data_table.slice(offset, length),
            input_file,
            row_group_size=4096,
            compression="snappy",
            write_page_index=True,
        )
        payload = input_file.read_bytes()
        (objects / f"{name}.parquet").write_bytes(
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
                timeout=180,
            )
            assert response.ok, response.text + server.debug_logs()
            value = response.json()
            return value["responses"][0] if "responses" in value else value

        call(
            "POST",
            "/tables/indexed_predicates",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "body": {
                                        "type": "string",
                                        "x-antfly-field": {"type": "text"},
                                    },
                                    "amount": {
                                        "type": "integer",
                                        "x-antfly-field": {
                                            "type": "number",
                                            "sortable": True,
                                        },
                                    },
                                    "sort_rank": {
                                        "type": "integer",
                                        "x-antfly-field": {
                                            "type": "number",
                                            "sortable": True,
                                        },
                                    },
                                    "big_integer": {"type": "integer"},
                                    "event_time": {
                                        "type": "datetime",
                                        "x-antfly-field": {
                                            "type": "datetime",
                                            "sortable": True,
                                        },
                                    },
                                    "time_text": {"type": "string"},
                                    "category": {"type": "string"},
                                    "label": {"type": "string"},
                                    "sparse_native": {"type": "string"},
                                },
                            }
                        }
                    },
                    "base_source": {
                        "kind": "external",
                        "table_id": "indexed-predicates",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {
                            "name": "amount_idx",
                            "keys": [{"column": "amount", "nulls": "first"}],
                        },
                        {
                            "name": "amount_desc_idx",
                            "keys": [
                                {
                                    "column": "amount",
                                    "direction": "desc",
                                    "nulls": "last",
                                }
                            ],
                        },
                        {
                            "name": "event_time_idx",
                            "keys": [{"column": "event_time", "nulls": "first"}],
                        },
                        {
                            "name": "event_time_desc_idx",
                            "keys": [
                                {
                                    "column": "event_time",
                                    "direction": "desc",
                                    "nulls": "last",
                                }
                            ],
                        },
                        {
                            "name": "big_integer_idx",
                            "keys": [{"column": "big_integer"}],
                        },
                        {"name": "time_text_idx", "keys": [{"column": "time_text"}]},
                        {"name": "category_idx", "keys": [{"column": "category"}]},
                        {
                            "name": "category_amount_idx",
                            "keys": [
                                {"column": "category"},
                                {"column": "amount", "nulls": "first"},
                            ],
                        },
                        {
                            "name": "rank_idx",
                            "keys": [{"column": "sort_rank", "nulls": "first"}],
                        },
                    ],
                },
                "indexes": {
                    "body_text": {"type": "full_text"},
                    "sparse_native": {
                        "type": "embeddings",
                        "external": True,
                        "sparse": True,
                    },
                },
            },
        )
        # This fixture builds several ordered indexes over more than 100,000
        # rows. Allow bounded Debug publication setup before checking indexed
        # query semantics; this is not a publication-latency benchmark.
        deadline = time.monotonic() + 600
        while True:
            resource = call("GET", "/tables/indexed_predicates/indexes/body_text")
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)

        def query(predicate, **options):
            return call(
                "POST",
                "/tables/indexed_predicates/query",
                dict(
                    {
                        "full_text_search": {"term": "common", "field": "body"},
                        "fields": ["amount"],
                        "limit": 10,
                        "filter_query": predicate,
                    },
                    **options,
                ),
            )

        point = {"term": {"path": "/amount", "value": count - 1}}
        assert [h["_source"]["amount"] for h in query(point)["hits"]["hits"]] == [
            count - 1
        ]
        boolean = query({"bool": {"filter": [point]}})
        assert [h["_source"]["amount"] for h in boolean["hits"]["hits"]] == [count - 1]
        assert (
            query({"term": {"path": "/amount", "value": count}})["hits"]["hits"] == []
        )
        ranged = query({"range": {"path": "/amount", "gte": 10, "lt": 15}})
        assert {h["_source"]["amount"] for h in ranged["hits"]["hits"]} == set(
            range(10, 15)
        )
        for alias in (
            {"term": {"field": "amount", "value": count - 1}},
            {
                "range": {
                    "amount": {"from": count - 1, "to": count, "include_upper": False}
                }
            },
            {"range": {"field": "amount", "min": count - 1, "max": count}},
        ):
            assert [h["_source"]["amount"] for h in query(alias)["hits"]["hits"]] == [
                count - 1
            ]
        precise = {"range": {"big_integer": {"gt": 9007199254740992}}}
        temporal = {"range": {"time_text": {"gte": "2026-01-01T00:15:00Z"}}}
        for predicate, expected in ((precise, count - 1), (temporal, 1)):
            direct = query(predicate, count=True, fields=[], limit=0)
            # Requiring two should clauses exercises the authoritative fallback.
            fallback = query(
                {
                    "bool": {
                        "should": [predicate, {"match_all": {}}],
                        "minimum_should_match": 2,
                    }
                },
                count=True,
                fields=[],
                limit=0,
            )
            assert (
                direct["hits"]["total"]
                == fallback["hits"]["total"]
                == {"value": expected, "relation": "exact"}
            )
        # More than 4,096 matching rows can still occupy a small pack range.
        # Compare lazy indexed admission with an equivalent authoritative filter
        # whose required-condition proof deliberately cannot be extracted.
        large_range = {"range": {"path": "/amount", "gte": count - 10000}}
        ranged_native = query(large_range, profile=True)
        assert ranged_native["profile"]["sort"]["selection_reason"] == (
            "indexed_predicate_text_admission"
        ), ranged_native
        ranged_oracle = query(
            {
                "bool": {
                    "should": [large_range, {"match_all": {}}],
                    "minimum_should_match": 2,
                }
            }
        )
        assert [hit["_id"] for hit in ranged_native["hits"]["hits"]] == [
            hit["_id"] for hit in ranged_oracle["hits"]["hits"]
        ]
        for direct_hit, oracle_hit in zip(
            ranged_native["hits"]["hits"], ranged_oracle["hits"]["hits"], strict=True
        ):
            assert direct_hit["_score"] == pytest.approx(oracle_hit["_score"], abs=1e-6)
        assert query(large_range, count=True, fields=[], limit=0)["hits"]["total"] == {
            "value": 10000,
            "relation": "exact",
        }
        broad = {"term": {"path": "/category", "value": "comment"}}
        hits = query(broad)["hits"]["hits"]
        assert len(hits) == 10 and all(h["_source"]["amount"] >= 2 for h in hits)
        counted = query(broad, count=True, fields=[], limit=0)
        assert counted["hits"]["total"] == {"value": count - 2, "relation": "exact"}, (
            counted
        )
        # Broad indexed metadata must refine selective text candidates without
        # changing BM25 statistics, exact counts, exclusions, or segment offsets.
        # These terms live in different files/native segments.
        for term, amount, matches in (
            ("rarefirst", 0, False),
            ("rarelast", count - 1, True),
        ):
            search = {"term": term, "field": "body"}
            unfiltered = query({"match_all": {}}, full_text_search=search)
            assert [h["_source"]["amount"] for h in unfiltered["hits"]["hits"]] == [
                amount
            ]
            # The prefix clause requires complete bounded admission but cannot match.
            # Selective routing must retain global BM25 collection statistics.
            full_corpus = query(
                {"match_all": {}},
                full_text_search={
                    "disjuncts": [
                        search,
                        {"prefix": "never_present_in_fixture", "field": "body"},
                    ]
                },
            )
            assert [h["_id"] for h in full_corpus["hits"]["hits"]] == [
                h["_id"] for h in unfiltered["hits"]["hits"]
            ]
            assert full_corpus["hits"]["hits"][0]["_score"] == pytest.approx(
                unfiltered["hits"]["hits"][0]["_score"], abs=1e-6
            )
            selected = query(broad, full_text_search=search)
            assert [h["_source"]["amount"] for h in selected["hits"]["hits"]] == (
                [amount] if matches else []
            )
            if matches:
                assert selected["hits"]["hits"][0]["_score"] == pytest.approx(
                    unfiltered["hits"]["hits"][0]["_score"], abs=1e-6
                )
            overlapping = query(broad, full_text_search=search, exclusion_query=broad)
            assert overlapping["hits"]["hits"] == [], overlapping
            exact = query(
                broad, full_text_search=search, count=True, fields=[], limit=0
            )
            assert exact["hits"]["total"] == {
                "value": int(matches),
                "relation": "exact",
            }
            excluded = call(
                "POST",
                "/tables/indexed_predicates/query",
                {
                    "full_text_search": search,
                    "exclusion_query": broad,
                    "count": True,
                    "fields": [],
                    "limit": 0,
                },
            )
            assert excluded["hits"]["total"] == {
                "value": int(not matches),
                "relation": "exact",
            }
        # A selective amount index can supply candidates while category remains
        # residual; exact count must not confuse that superset with final matches.
        selective_and = {
            "bool": {
                "filter": [broad, {"range": {"path": "/amount", "gte": count - 3}}]
            }
        }
        impossible_and = {
            "bool": {"filter": [broad, {"term": {"path": "/amount", "value": 0}}]}
        }
        broad_residual = {
            "conjuncts": [broad, {"prefix": {"path": "/label", "value": "ke"}}]
        }
        for predicate, expected in (
            (selective_and, 3),
            (impossible_and, 0),
            (broad_residual, 1),
        ):
            result = query(predicate, count=True, fields=[], limit=0)
            oracle = query(
                {
                    "bool": {
                        "should": [predicate, {"match_all": {}}],
                        "minimum_should_match": 2,
                    }
                },
                count=True,
                fields=[],
                limit=0,
            )
            assert (
                result["hits"]["total"]
                == oracle["hits"]["total"]
                == {"value": expected, "relation": "exact"}
            )
        for predicate, expected in (
            (selective_and, count - 3),
            (impossible_and, count),
            (broad_residual, count - 1),
        ):
            excluded_and = call(
                "POST",
                "/tables/indexed_predicates/query",
                {
                    "full_text_search": {"term": "common", "field": "body"},
                    "exclusion_query": predicate,
                    "count": True,
                    "fields": [],
                    "limit": 0,
                },
            )
            assert excluded_and["hits"]["total"] == {
                "value": expected,
                "relation": "exact",
            }
        kept = query(selective_and)
        assert {h["_source"]["amount"] for h in kept["hits"]["hits"]} == set(
            range(count - 3, count)
        )
        residual = query(
            {"conjuncts": [point, {"prefix": {"path": "/label", "value": "ke"}}]}
        )
        assert [h["_source"]["amount"] for h in residual["hits"]["hits"]] == [count - 1]
        union = query({"disjuncts": [point, {"term": {"path": "/amount", "value": 0}}]})
        assert {h["_source"]["amount"] for h in union["hits"]["hits"]} == {0, count - 1}
        excluded = call(
            "POST",
            "/tables/indexed_predicates/query",
            {
                "full_text_search": {"term": "common", "field": "body"},
                "fields": ["amount"],
                "limit": 10,
                "exclusion_query": broad,
            },
        )
        assert {h["_source"]["amount"] for h in excluded["hits"]["hits"]} == {0, 1}
        sparse = call(
            "POST",
            "/tables/indexed_predicates/query",
            {
                "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
                "indexes": ["sparse_native"],
                "fields": ["amount"],
                "limit": 3,
                "filter_query": broad,
            },
        )
        assert len(sparse["hits"]["hits"]) == 3 and all(
            h["_source"]["amount"] >= 2 for h in sparse["hits"]["hits"]
        ), sparse
        for weight in (1, -1, 0):
            exclusion_only = call(
                "POST",
                "/tables/indexed_predicates/query",
                {
                    "embeddings": {
                        "sparse_native": {"indices": [1], "values": [weight]}
                    },
                    "indexes": ["sparse_native"],
                    "fields": ["amount"],
                    "limit": 3,
                    "exclusion_query": broad,
                },
            )
            hits = exclusion_only["hits"]["hits"]
            assert {h["_source"]["amount"] for h in hits} == {0, 1}, exclusion_only
            assert all(h["_score"] == weight for h in hits), exclusion_only
        sparse_point = call(
            "POST",
            "/tables/indexed_predicates/query",
            {
                "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
                "indexes": ["sparse_native"],
                "fields": ["amount"],
                "limit": 3,
                "filter_query": {"term": {"field": "amount", "value": count - 1}},
            },
        )
        assert [h["_source"]["amount"] for h in sparse_point["hits"]["hits"]] == [
            count - 1
        ], sparse_point
        for weight in (-1, 0):
            signed = call(
                "POST",
                "/tables/indexed_predicates/query",
                {
                    "embeddings": {
                        "sparse_native": {"indices": [1], "values": [weight]}
                    },
                    "indexes": ["sparse_native"],
                    "fields": ["amount"],
                    "limit": 3,
                    "filter_query": {"term": {"field": "amount", "value": count - 1}},
                },
            )
            assert [h["_source"]["amount"] for h in signed["hits"]["hits"]] == [
                count - 1
            ], signed
            assert signed["hits"]["hits"][0]["_score"] == weight, signed
        selective_sort = query(
            point, order_by=[{"field": "amount"}], limit=1, profile=True
        )
        assert [h["_source"]["amount"] for h in selective_sort["hits"]["hits"]] == [
            count - 1
        ], selective_sort
        assert (
            selective_sort["profile"]["sort"]["candidate_source"]
            != "ordered_lake_index"
        ), selective_sort
        assert selective_sort["profile"]["sort"]["ordered_scanned_count"] == 0, (
            selective_sort
        )
        sparse_residual = call(
            "POST",
            "/tables/indexed_predicates/query",
            {
                "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
                "indexes": ["sparse_native"],
                "fields": ["amount"],
                "limit": 3,
                "filter_query": {
                    "conjuncts": [point, {"prefix": {"path": "/label", "value": "ke"}}]
                },
            },
        )
        assert [h["_source"]["amount"] for h in sparse_residual["hits"]["hits"]] == [
            count - 1
        ], sparse_residual
        rejected_residual = call(
            "POST",
            "/tables/indexed_predicates/query",
            {
                "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
                "indexes": ["sparse_native"],
                "fields": ["amount"],
                "limit": 3,
                "filter_query": {
                    "conjuncts": [point, {"prefix": {"path": "/label", "value": "no"}}]
                },
            },
        )
        assert rejected_residual["hits"]["hits"] == [], rejected_residual
        for descending, expected in (
            (False, list(range(7, 10))),
            (True, list(range(count - 8, count - 11, -1))),
        ):
            ordered = query(
                {"match_all": {}},
                order_by=[{"field": "amount", "desc": descending}],
                offset=7,
                limit=3,
                profile=True,
            )
            assert [
                h["_source"]["amount"] for h in ordered["hits"]["hits"]
            ] == expected, ordered
            assert ordered["hits"]["total"] == {"value": count, "relation": "exact"}, (
                ordered
            )
            profile = ordered["profile"]["sort"]
            assert profile["candidate_source"] == "ordered_lake_index", profile
            assert profile["candidate_count"] <= 11, profile
            assert profile["ordered_scanned_count"] <= 11, profile
            continued = query(
                {"match_all": {}},
                order_by=[{"field": "amount", "desc": descending}],
                search_after=ordered["hits"]["hits"][-1]["_sort"],
                remote_snapshot=ordered["remote_snapshot"],
                limit=3,
                profile=True,
            )
            following = (
                list(range(count - 11, count - 14, -1))
                if descending
                else list(range(10, 13))
            )
            assert [
                h["_source"]["amount"] for h in continued["hits"]["hits"]
            ] == following, continued
            assert (
                continued["profile"]["sort"]["candidate_source"] == "ordered_lake_index"
            ), continued
            assert continued["profile"]["sort"]["ordered_scanned_count"] <= 5, continued
            previous = query(
                {"match_all": {}},
                order_by=[{"field": "amount", "desc": descending}],
                search_before=continued["hits"]["hits"][0]["_sort"],
                remote_snapshot=continued["remote_snapshot"],
                limit=3,
                profile=True,
            )
            assert [
                h["_source"]["amount"] for h in previous["hits"]["hits"]
            ] == expected, previous
            assert (
                previous["profile"]["sort"]["candidate_source"] == "ordered_lake_index"
            ), previous
            assert previous["profile"]["sort"]["ordered_scanned_count"] <= 5, previous
        prefixed = query(
            {
                "conjuncts": [
                    {"term": {"field": "category", "value": "comment"}},
                    {"range": {"amount": {"gte": 2}}},
                ]
            },
            order_by=[{"field": "amount"}],
            limit=3,
            profile=True,
        )
        assert [h["_source"]["amount"] for h in prefixed["hits"]["hits"]] == [
            2,
            3,
            4,
        ], prefixed
        assert (
            prefixed["profile"]["sort"]["candidate_source"] == "ordered_lake_index"
        ), prefixed
        continued = query(
            {
                "conjuncts": [
                    {"term": {"field": "category", "value": "comment"}},
                    {"range": {"amount": {"gte": 2}}},
                ]
            },
            order_by=[{"field": "amount"}],
            limit=3,
            profile=True,
            search_after=prefixed["hits"]["hits"][-1]["_sort"],
            remote_snapshot=prefixed["remote_snapshot"],
        )
        assert [h["_source"]["amount"] for h in continued["hits"]["hits"]] == [
            5,
            6,
            7,
        ], continued
        assert (
            continued["profile"]["sort"]["candidate_source"] == "ordered_lake_index"
        ), continued
        assert continued["profile"]["sort"]["ordered_scanned_count"] <= 5, continued
        if count > 10000:
            skewed = call(
                "POST",
                "/tables/indexed_predicates/query",
                {
                    "full_text_search": {"term": "late", "field": "body"},
                    "fields": ["amount"],
                    "order_by": [{"field": "amount"}],
                    "limit": 3,
                    "profile": True,
                },
            )
            assert [h["_source"]["amount"] for h in skewed["hits"]["hits"]] == list(
                range(count - 1000, count - 997)
            ), skewed
            assert skewed["hits"]["total"] == {"value": 1000, "relation": "exact"}, (
                skewed
            )
            assert (
                skewed["profile"]["sort"]["candidate_source"]
                == "ordered_lake_index_then_text_postings"
            ), skewed
            assert skewed["profile"]["sort"]["ordered_scanned_count"] <= 1024, skewed
        tied = query(
            {"match_all": {}}, order_by=[{"field": "sort_rank"}], limit=1, profile=True
        )
        zeros = (
            query({"term": {"field": "amount", "value": 0}})["hits"]["hits"]
            + query(point)["hits"]["hits"]
        )
        assert [h["_source"]["amount"] for h in tied["hits"]["hits"]] == [
            min(zeros, key=lambda h: h["_id"])["_source"]["amount"]
        ], tied
        assert tied["profile"]["sort"]["candidate_source"] == "ordered_lake_index", tied
        assert tied["profile"]["sort"]["candidate_count"] <= 4, tied
        dates = query(
            {"match_all": {}}, order_by=[{"field": "event_time"}], limit=2, profile=True
        )
        assert [h["_source"]["amount"] for h in dates["hits"]["hits"]] == [0, 1], dates
        assert (
            dates["hits"]["hits"][0]["_sort"][0] == "1969-12-31T23:59:59.999999999Z"
        ), dates
        assert dates["profile"]["sort"]["candidate_source"] == "ordered_lake_index", (
            dates
        )
        following_dates = query(
            {"match_all": {}},
            order_by=[{"field": "event_time"}],
            limit=1,
            profile=True,
            search_after=dates["hits"]["hits"][-1]["_sort"],
            remote_snapshot=dates["remote_snapshot"],
        )
        assert [h["_source"]["amount"] for h in following_dates["hits"]["hits"]] == [
            2
        ], following_dates
        prior_dates = query(
            {"match_all": {}},
            order_by=[{"field": "event_time"}],
            limit=2,
            profile=True,
            search_before=following_dates["hits"]["hits"][0]["_sort"],
            remote_snapshot=dates["remote_snapshot"],
        )
        assert [h["_source"]["amount"] for h in prior_dates["hits"]["hits"]] == [
            0,
            1,
        ], prior_dates
        assert prior_dates["profile"]["sort"]["ordered_scanned_count"] <= 4, prior_dates
        # Almost the entire archive shares one timestamp. Complete public-ID
        # seeks must deliver pages without walking that 100,000-row tie group.
        tie_filter = {
            "range": {"event_time": {"gte": "1970-01-01T00:00:00.000000002Z"}}
        }
        for primary_desc in (False, True):
            # The public contract requires an ascending final _id tie-breaker.
            tie_order = [
                {"field": "event_time", "desc": primary_desc},
                {"field": "_id"},
            ]
            first_tie = query(tie_filter, order_by=tie_order, limit=3, profile=True)
            first_ids = [hit["_id"] for hit in first_tie["hits"]["hits"]]
            assert len(first_ids) == 3 and first_ids == sorted(first_ids), first_tie
            assert (
                first_tie["profile"]["sort"]["candidate_source"] == "ordered_lake_index"
            ), first_tie
            assert first_tie["profile"]["sort"]["ordered_scanned_count"] <= 4, first_tie
            next_tie = query(
                tie_filter,
                order_by=tie_order,
                limit=3,
                profile=True,
                search_after=first_tie["hits"]["hits"][-1]["_sort"],
                remote_snapshot=first_tie["remote_snapshot"],
            )
            next_ids = [hit["_id"] for hit in next_tie["hits"]["hits"]]
            assert len(next_ids) == 3 and not set(first_ids) & set(next_ids), next_tie
            assert first_ids + next_ids == sorted(first_ids + next_ids), next_tie
            assert next_tie["profile"]["sort"]["ordered_scanned_count"] <= 4, next_tie
            previous_tie = query(
                tie_filter,
                order_by=tie_order,
                limit=3,
                profile=True,
                search_before=next_tie["hits"]["hits"][0]["_sort"],
                remote_snapshot=next_tie["remote_snapshot"],
            )
            assert [hit["_id"] for hit in previous_tie["hits"]["hits"]] == first_ids, (
                previous_tie
            )
            assert previous_tie["profile"]["sort"]["ordered_scanned_count"] <= 4, (
                previous_tie
            )
        native_dates = call(
            "POST",
            "/tables/indexed_predicates/query",
            {
                "full_text_search": {
                    "field": "event_time",
                    "start": "1969-12-31T23:59:59.999999999Z",
                    "end": "1970-01-01T00:00:00.000000001Z",
                },
                "fields": ["amount"],
                "limit": 10,
            },
        )
        assert {h["_source"]["amount"] for h in native_dates["hits"]["hits"]} == {
            0,
            1,
        }, native_dates
        signed_temporal = {
            "range": {
                "event_time": {
                    "gte": "1970-01-01T00:00:00Z",
                    "lt": "1970-01-01T01:00:00.000000002+01:00",
                }
            }
        }
        for predicate in (
            signed_temporal,
            {
                "bool": {
                    "should": [signed_temporal, {"match_all": {}}],
                    "minimum_should_match": 2,
                }
            },
        ):
            dated = query(predicate, count=True, fields=[], limit=0)
            assert dated["hits"]["total"] == {"value": 2, "relation": "exact"}, dated
        failed = False
    finally:
        server.stop(test_failed=failed)


def _parquet_with_bloom_for_42(payload):
    """Add a standard Bloom to one PyArrow column without changing its data pages.

    PyArrow does not write Blooms. Preserve its compact-Thrift footer verbatim,
    inserting only the two standard ColumnMetaData fields and the Bloom payload.
    The fixed mask is XXH64(seed=0) over little-endian INT64 42, split-block salts.
    """
    footer_len = struct.unpack("<I", payload[-8:-4])[0]
    footer_start = len(payload) - 8 - footer_len
    footer = payload[footer_start:-8]
    position = 0
    insertion = None

    def varint():
        nonlocal position
        value = shift = 0
        while True:
            byte = footer[position]
            position += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
            shift += 7

    def value(kind, path):
        nonlocal position, insertion
        if kind in (1, 2):
            return
        if kind == 3:
            position += 1
        elif kind in (4, 5, 6):
            varint()
        elif kind == 7:
            position += 8
        elif kind == 8:
            length = varint()
            position += length
        elif kind in (9, 10):
            header = footer[position]
            position += 1
            length = varint() if header >> 4 == 15 else header >> 4
            for _ in range(length):
                if header & 15 in (1, 2):
                    position += 1
                else:
                    value(header & 15, path)
        elif kind == 12:
            field = 0
            while footer[position]:
                header = footer[position]
                position += 1
                if header >> 4:
                    field += header >> 4
                else:
                    encoded = varint()
                    field = (encoded >> 1) ^ -(encoded & 1)
                assert path != (4, 1, 3) or field not in (14, 15)
                value(header & 15, (*path, field))
            if path == (4, 1, 3):
                assert insertion is None
                insertion = position
            position += 1
        else:
            raise AssertionError(f"Unexpected PyArrow footer type {kind}")

    value(12, ())
    assert position == len(footer) and insertion is not None

    def encode_varint(number):
        result = bytearray()
        while number >= 128:
            result.append((number & 127) | 128)
            number >>= 7
        result.append(number)
        return bytes(result)

    bloom = bytes.fromhex("15401c1c00001c1c00001c1c000000") + bytes.fromhex(
        "0001000080000000000000400040000000040000008000000000000810000000"
    )
    fields = b"\x06\x1c" + encode_varint(footer_start << 1)
    fields += b"\x05\x1e" + encode_varint(len(bloom) << 1)
    footer = footer[:insertion] + fields + footer[insertion:]
    return (
        payload[:footer_start]
        + bloom
        + footer
        + struct.pack("<I", len(footer))
        + b"PAR1"
    )


def test_parquet_embedded_bloom_skips_unreadable_data_pages(tmp_path):
    """An absent equality succeeds without decoding pages; a present value reads them."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    input_file = tmp_path / "bloom.parquet"
    pq.write_table(
        pa.table({"amount": pa.array([42] * 100, type=pa.int64())}),
        input_file,
        use_dictionary=False,
        compression=None,
        write_statistics=False,
    )
    payload = _parquet_with_bloom_for_42(input_file.read_bytes())
    # Independent reader accepts the extended footer and original data pages.
    input_file.write_bytes(payload)
    assert pq.read_table(input_file)["amount"].to_pylist() == [42] * 100
    footer_start = len(payload) - 8 - struct.unpack("<I", payload[-8:-4])[0]
    bloom_start = footer_start - 47
    corrupt = payload[:4] + b"\xff" * (bloom_start - 4) + payload[bloom_start:]
    for name, data in (("valid", payload), ("unreadable", corrupt)):
        objects = tmp_path / name / "buckets" / "antfly" / "objects"
        objects.mkdir(parents=True)
        (objects / "part.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(data), 0)
            + hashlib.sha256(data).hexdigest().encode()
            + data
        )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:

        def call(path, body):
            return requests.post(
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=60,
            )

        for name in ("valid", "unreadable"):
            attached = call(
                f"/tables/bloom_{name}",
                {
                    "num_shards": 1,
                    "schema": {
                        "storage_mode": "relational",
                        "base_source": {
                            "kind": "external",
                            "table_id": name,
                            "format": "parquet",
                            "uri": (tmp_path / name).as_uri(),
                        },
                    },
                },
            )
            assert attached.ok, attached.text + server.debug_logs()
            absent = call(
                "/sql",
                {"statement": f"SELECT amount FROM bloom_{name} WHERE amount = 43"},
            )
            assert absent.ok, absent.text + server.debug_logs()
            assert absent.json()["rows"] == []
        present = call(
            "/sql",
            {"statement": "SELECT amount FROM bloom_valid WHERE amount = 42 LIMIT 1"},
        )
        assert present.ok and present.json()["rows"] == [["42"]], present.text
        try:
            unreadable = call(
                "/sql",
                {
                    "statement": "SELECT amount FROM bloom_unreadable WHERE amount = 42 LIMIT 1"
                },
            )
            assert not unreadable.ok, unreadable.text
        except requests.exceptions.ConnectionError:
            # A decode error after streaming headers closes the response.
            assert server.proc.poll() is None, server.debug_logs()
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_remote_ordered_scan_budget_counts_rows_without_text_ordinals(tmp_path):
    """Rows omitted from the text corpus still consume the physical probe budget."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = tmp_path / "ordered-physical-budget"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    input_file = tmp_path / "budget.parquet"
    pq.write_table(
        pa.table(
            {
                "body": pa.array([None] * 2048 + ["common"] * 100, type=pa.string()),
                "amount": pa.array([None] * 2048 + list(range(100)), type=pa.int64()),
            }
        ),
        input_file,
        row_group_size=256,
    )
    payload = input_file.read_bytes()
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
                timeout=120,
            )
            assert response.ok, response.text + server.debug_logs()
            value = response.json()
            return value["responses"][0] if "responses" in value else value

        call(
            "POST",
            "/tables/physical_budget",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "body": {
                                        "type": "string",
                                        "x-antfly-field": {"type": "text"},
                                    },
                                    "amount": {
                                        "type": "integer",
                                        "x-antfly-field": {
                                            "type": "number",
                                            "sortable": True,
                                        },
                                    },
                                },
                            }
                        }
                    },
                    "base_source": {
                        "kind": "external",
                        "table_id": "physical-budget",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {
                            "name": "amount_idx",
                            "keys": [{"column": "amount", "nulls": "first"}],
                        }
                    ],
                },
                "indexes": {"body_text": {"type": "full_text"}},
            },
        )
        deadline = time.monotonic() + 120
        while not call("GET", "/tables/physical_budget/indexes/body_text")["status"][
            "readiness"
        ]["queryable"]:
            assert time.monotonic() < deadline, server.debug_logs()
            time.sleep(0.1)
        result = call(
            "POST",
            "/tables/physical_budget/query",
            {
                "full_text_search": {"term": "common", "field": "body"},
                "fields": ["amount"],
                "limit": 3,
                "order_by": [{"field": "amount", "desc": False}],
                "profile": True,
            },
        )
        assert [hit["_source"]["amount"] for hit in result["hits"]["hits"]] == [
            0,
            1,
            2,
        ], result
        profile = result["profile"]["sort"]
        assert profile["candidate_source"] == "ordered_lake_index_then_text_postings", (
            result
        )
        assert profile["ordered_scanned_count"] <= 1024, result
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_native_sparse_metadata_filters_preserve_quantized_scores_and_ranking(tmp_path):
    """Selective index plans share posting weights with unfiltered search."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "source"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    data = tmp_path / "source.parquet"
    pq.write_table(
        pa.table(
            {
                "amount": [0, 1, 2],
                "sparse_native": [
                    json.dumps({"1": w, "2": 1e20, "3": -1e20}) for w in (1, 1.1, 100)
                ],
            }
        ),
        data,
    )
    payload = data.read_bytes()
    (objects / "part.parquet").write_bytes(
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
            r = requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=90,
            )
            assert r.ok, r.text + server.debug_logs()
            v = r.json()
            return v["responses"][0] if "responses" in v else v

        call(
            "POST",
            "/tables/review_sparse",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "amount": {"type": "integer"},
                                    "sparse_native": {"type": "string"},
                                },
                            }
                        }
                    },
                    "base_source": {
                        "kind": "external",
                        "table_id": "review-sparse",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {"name": "amount_idx", "keys": [{"column": "amount"}]}
                    ],
                },
                "indexes": {
                    "sparse_native": {
                        "type": "embeddings",
                        "external": True,
                        "sparse": True,
                    }
                },
            },
        )
        deadline = time.monotonic() + 90
        while not call("GET", "/tables/review_sparse/indexes/sparse_native")["status"][
            "readiness"
        ]["queryable"]:
            assert time.monotonic() < deadline
            time.sleep(0.1)
        base = {
            "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
            "indexes": ["sparse_native"],
            "fields": ["amount"],
            "limit": 3,
        }
        all_hits = call("POST", "/tables/review_sparse/query", base)["hits"]["hits"]
        overflow = requests.post(
            server.api_url + "/tables/review_sparse/query",
            json=dict(
                base,
                embeddings={
                    "sparse_native": {
                        "indices": [2, 3],
                        "values": [1e20, 1e20],
                    }
                },
            ),
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=90,
        )
        assert not overflow.ok, overflow.text
        # A valid query must still work after rejecting finite-input overflow.
        assert (
            call("POST", "/tables/review_sparse/query", base)["hits"]["hits"]
            == all_hits
        )
        selected = call(
            "POST",
            "/tables/review_sparse/query",
            dict(base, filter_query={"term": {"path": "/amount", "value": 1}}),
        )["hits"]["hits"]
        original = next(h for h in all_hits if h["_source"]["amount"] == 1)
        assert original["_score"] == selected[0]["_score"]
        ranged = call(
            "POST",
            "/tables/review_sparse/query",
            dict(base, filter_query={"range": {"path": "/amount", "gte": 0, "lt": 2}}),
        )["hits"]["hits"]
        expected = [h for h in all_hits if h["_source"]["amount"] < 2]
        assert [(h["_id"], h["_score"]) for h in expected] == [
            (h["_id"], h["_score"]) for h in ranged
        ]
        server.restart()
        reopened = call(
            "POST",
            "/tables/review_sparse/query",
            dict(base, filter_query={"range": {"path": "/amount", "gte": 0, "lt": 2}}),
        )["hits"]["hits"]
        assert [(h["_id"], h["_score"]) for h in ranged] == [
            (h["_id"], h["_score"]) for h in reopened
        ]
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_wide_partial_join_keeps_delivery_memory_bounded(tmp_path):
    """Wide reducers stream compact states instead of retaining page-sized arrays."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    # Keep source identity long enough to exercise schema derivation admission
    # independently of the runner's temporary directory name.
    root = tmp_path / ("wide-source-" + "a" * 160) / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    count, width = 8192, 128
    data = tmp_path / "wide.parquet"
    pq.write_table(
        pa.table(
            {
                "k": pa.array([i // 2 for i in range(count)], type=pa.int64()),
                **{
                    f"v{j}": pa.array([i + j for i in range(count)], type=pa.int64())
                    for j in range(width)
                },
            }
        ),
        data,
        compression="snappy",
        row_group_size=1000,
    )
    payload = data.read_bytes()
    (objects / "part.parquet").write_bytes(
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

        def call(path, body):
            response = requests.post(
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=120,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json()

        call(
            "/tables/wide_join",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "wide-join",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                },
            },
        )
        assert call("/sql", {"statement": "SELECT COUNT(*) FROM wide_join"})[
            "rows"
        ] == [[str(count)]]
        query = (
            "SELECT "
            + ",".join(f"SUM(l.v{j})" for j in range(width))
            + " FROM wide_join l JOIN wide_join r ON l.k=r.k"
        )
        # The dedicated COUNT compiler path must consume the same typed partial
        # protocol as a query containing multiple aggregate expressions.
        assert call(
            "/sql",
            {
                "statement": "SELECT COUNT(*) FROM wide_join l JOIN wide_join r ON l.k=r.k"
            },
        )["rows"] == [[str(2 * count)]]
        assert call(
            "/sql",
            {
                "statement": "SELECT COUNT(*) FROM (VALUES (1),(1)) l(k) JOIN (VALUES (1),(1)) r(k) ON l.k=r.k"
            },
        )["rows"] == [["4"]]
        # Each source row has exactly two matches. Python integer arithmetic
        # supplies an independent reference for every reducer and orientation.
        expected = [[str(count * (count - 1) + 2 * count * j) for j in range(width)]]
        assert call("/sql", {"statement": query})["rows"] == expected
        assert (
            call(
                "/sql",
                {"statement": query + " AND (l.v0 IS NOT DISTINCT FROM l.v0)"},
            )["rows"]
            == expected
        )
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_optional_term_directory_refresh_preserves_unchanged_parquet_matches(tmp_path):
    """An unrepresentable optional key must never turn refresh into an incomplete index."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)

    def put(name, body):
        path = tmp_path / name
        pq.write_table(
            pa.table({"body": [body], "amount": [1]}), path, compression="snappy"
        )
        payload = path.read_bytes()
        (objects / name).write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
            + payload
        )

    put("a.parquet", "x" * 262120 + " retainedneedle")
    put("b.parquet", "initial")
    # More than one compaction group ensures refresh also retains an unchanged
    # group when the formerly disabled optional directory becomes admissible.
    for name in "cdefghi":
        put(f"{name}.parquet", "retainedneedle")
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
                timeout=90,
            )
            assert response.ok, response.text + server.debug_logs()
            value = response.json() if response.content else None
            return (
                value["responses"][0]
                if isinstance(value, dict) and "responses" in value
                else value
            )

        def ready():
            deadline = time.monotonic() + 90
            while True:
                resources = call("GET", "/tables/directory/indexes")
                assert all(
                    r["status"]["readiness"]["state"] != "failed" for r in resources
                ), str(resources) + server.debug_logs()
                if resources and all(
                    r["status"]["readiness"]["queryable"] for r in resources
                ):
                    return
                assert time.monotonic() < deadline, str(resources) + server.debug_logs()
                time.sleep(0.1)

        def check():
            ready()
            value = call(
                "POST",
                "/tables/directory/query",
                {
                    "full_text_search": {"match": "retainedneedle", "field": "body"},
                    "full_text_index": "body_text",
                    "fields": ["amount"],
                    "limit": 10,
                },
            )
            assert len(value["hits"]["hits"]) == 8, value

        call(
            "POST",
            "/tables/directory",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "directory",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                },
                "indexes": {"body_text": {"type": "full_text", "field": "body"}},
            },
        )
        check()
        for phase, name, body in (
            (1, "b.parquet", "replacement"),
            (2, "a.parquet", "retainedneedle"),
            (3, "b.parquet", "another replacement"),
        ):
            put(name, body)
            call(
                "POST",
                f"/tables/directory/indexes/wake{phase}",
                {
                    "type": "algebraic",
                    "derive_from_schema": True,
                    "aggregates": [{"name": "rows", "op": "count"}],
                },
            )
            check()
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize("covering", [False, True])
def test_dynamic_join_uses_bounded_native_index_seeks(tmp_path, covering):
    """Sparse build keys drive index probes, including duplicates, NULLs and large fanout."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    path = tmp_path / "probe.parquet"
    count, fanout = 8192, 600
    # Nine real files exercise durable run compaction and final assembly.
    physical_objects = []
    for part, begin in enumerate(range(0, count, 1000)):
        end = min(count, begin + 1000)
        pq.write_table(
            pa.table(
                {
                    "k": pa.array(
                        [0 if i < fanout else i for i in range(begin, end)],
                        type=pa.int64(),
                    ),
                    "v": pa.array(range(begin, end), type=pa.int64()),
                }
            ),
            path,
            compression="snappy",
            use_dictionary=False,
            row_group_size=256,
        )
        payload = path.read_bytes()
        header = (
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
        )
        object_path = objects / f"part-{part}.parquet"
        object_path.write_bytes(header + payload)
        physical_objects.append((object_path, header, payload))
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:

        def call(method, route, body=None):
            response = requests.request(
                method,
                server.api_url + route,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=120,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json() if response.content else None

        call(
            "POST",
            "/tables/join_probe",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "join-probe",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "relational_indexes": [
                        {
                            "name": "k_idx",
                            "keys": [{"column": "k"}],
                            "include_columns": ["v"] if covering else [],
                        }
                    ],
                },
            },
        )
        deadline = time.monotonic() + 90
        while True:
            resources = call("GET", "/tables/join_probe/indexes")
            assert all(
                r["status"]["readiness"]["state"] != "failed" for r in resources
            ), str(resources) + server.debug_logs()
            if resources and all(
                r["status"]["readiness"]["queryable"] for r in resources
            ):
                break
            assert time.monotonic() < deadline, str(resources) + server.debug_logs()
            time.sleep(0.1)
        statement = (
            f"SELECT p.v, b.k FROM join_probe p JOIN (VALUES (-1),(0),({count - 1}),(0),(NULL)) b(k) "
            "ON p.k=b.k ORDER BY p.v, b.k"
        )
        expected = [[str(i), "0"] for i in range(fanout) for _ in range(2)] + [
            [str(count - 1), str(count - 1)]
        ]
        assert (
            call("POST", "/sql", {"statement": statement, "limit": 2048})["rows"]
            == expected
        )
        # Beyond the former 65536-probe cutoff, keys remain a complete bounded
        # stream. Most miss; duplicates retain join multiplicity.
        build_keys = (
            list(range(count - 100, count))
            + list(range(count + 1, count + 70001))
            + [count - 1, None]
        )
        build_root = tmp_path / "build-lake"
        build_objects = build_root / "buckets" / "antfly" / "objects"
        build_objects.mkdir(parents=True)
        pq.write_table(
            pa.table({"k": pa.array(build_keys, type=pa.int64())}),
            path,
            compression="snappy",
            row_group_size=512,
        )
        build_payload = path.read_bytes()
        (build_objects / "part.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(build_payload), 0)
            + hashlib.sha256(build_payload).hexdigest().encode()
            + build_payload
        )
        call(
            "POST",
            "/tables/join_build",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "join-build",
                        "format": "parquet",
                        "uri": build_root.as_uri(),
                    },
                },
            },
        )
        derived_statement = "SELECT p.v,p.join_key FROM (SELECT v,k AS join_key FROM join_probe WHERE v>=0) p JOIN (VALUES(0),(0),(8191),(NULL)) b(k) ON p.join_key=b.k ORDER BY p.v,p.join_key"
        large_statement = "SELECT COUNT(*) FROM join_probe p JOIN (SELECT k FROM join_build) b ON p.k=b.k"
        assert call("POST", "/sql", {"statement": large_statement})["rows"] == [["101"]]
        assert (
            call("POST", "/sql", {"statement": derived_statement, "limit": 2048})[
                "rows"
            ]
            == expected
        )
        server.pause()
        if covering:
            # Preserve object identity/footer while making every physical data
            # page unreadable. The leased covering index must satisfy the join.
            for object_path, header, payload in physical_objects:
                footer = len(payload) - 8 - struct.unpack("<I", payload[-8:-4])[0]
                object_path.write_bytes(
                    header + payload[:4] + b"\xff" * (footer - 4) + payload[footer:]
                )
        for cached in list(server.root.rglob("lake-ranges")):
            if cached.is_dir():
                shutil.rmtree(cached)
        server.resume()
        assert (
            call("POST", "/sql", {"statement": statement, "limit": 2048})["rows"]
            == expected
        )
        assert call("POST", "/sql", {"statement": large_statement})["rows"] == [["101"]]
        assert (
            call("POST", "/sql", {"statement": derived_statement, "limit": 2048})[
                "rows"
            ]
            == expected
        )
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize(
    "physical_type",
    ["integer", "decimal_string", "decimal128", "decimal128_dictionary"],
)
def test_numeric_dynamic_join_uses_exact_covering_index(tmp_path, physical_type):
    """NUMERIC seeks and covering filters agree beyond f64 precision, cold too."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    count = 4096
    if physical_type == "integer":
        keys = [9007199254740993, 9007199254740992] + list(range(2, count))
        targets = ["9007199254740993", "4095"]
        values = pa.array(keys, type=pa.int64())
    else:
        keys = ["9007199254740993.125", "9007199254740993.124"] + [
            f"{i}.125" for i in range(2, count)
        ]
        targets = ["9007199254740993.125", "4095.125"]
        values = (
            pa.array([Decimal(k) for k in keys], type=pa.decimal128(38, 3))
            if physical_type.startswith("decimal128")
            else pa.array(keys, type=pa.string())
        )
    path = tmp_path / "numeric.parquet"
    pq.write_table(
        pa.table({"k": values, "v": pa.array(range(count), type=pa.int64())}),
        path,
        compression="snappy",
        use_dictionary=physical_type.endswith("_dictionary"),
        row_group_size=256,
    )
    payload = path.read_bytes()
    header = (
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
    )
    object_path = objects / "part.parquet"
    object_path.write_bytes(header + payload)
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:

        def call(method, route, body=None):
            response = requests.request(
                method,
                server.api_url + route,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=120,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json() if response.content else None

        call(
            "POST",
            "/tables/numeric_probe",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "base_source": {
                        "kind": "external",
                        "table_id": "numeric-probe",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "k": {
                                        "type": "number",
                                        "x-antfly-sql-type": "numeric",
                                    },
                                    "v": {"type": "integer"},
                                },
                                "additionalProperties": False,
                            }
                        }
                    },
                    "relational_indexes": [
                        {
                            "name": "k_idx",
                            "keys": [{"column": "k"}],
                            "include_columns": ["v"],
                        }
                    ],
                },
            },
        )
        deadline = time.monotonic() + 90
        while True:
            resources = call("GET", "/tables/numeric_probe/indexes")
            assert all(
                r["status"]["readiness"]["state"] != "failed" for r in resources
            ), str(resources) + server.debug_logs()
            if resources and all(
                r["status"]["readiness"]["queryable"] for r in resources
            ):
                break
            assert time.monotonic() < deadline, str(resources) + server.debug_logs()
            time.sleep(0.1)
        terms = ",".join(f"(CAST('{key}' AS NUMERIC))" for key in targets + targets[:1])
        statement = f"SELECT p.v FROM numeric_probe p JOIN (VALUES {terms}) b(k) ON p.k=b.k ORDER BY p.v"
        expected = [["0"], ["0"], [str(count - 1)]]
        predicate = (
            f"SELECT v FROM numeric_probe WHERE k=CAST('{targets[0]}' AS NUMERIC)"
        )
        assert call("POST", "/sql", {"statement": predicate})["rows"] == [["0"]]
        assert call(
            "POST", "/sql", {"statement": f"SELECT CAST('{targets[0]}' AS NUMERIC)"}
        )["rows"] == [[targets[0]]]
        assert call(
            "POST", "/sql", {"statement": "SELECT k FROM numeric_probe WHERE v=0"}
        )["rows"] == [[targets[0]]]
        assert call(
            "POST", "/sql", {"statement": f"SELECT k FROM (VALUES {terms}) b(k)"}
        )["rows"] == [[key] for key in targets + targets[:1]]
        residual = statement.replace("ON p.k=b.k", "ON p.k+CAST('0' AS NUMERIC)=b.k")
        assert call("POST", "/sql", {"statement": residual})["rows"] == expected
        assert call("POST", "/sql", {"statement": statement})["rows"] == expected
        # Filtered three-way joins retain qualified output order after costing
        # and inner-join enumeration; every scan shares the statement snapshot.
        assert call(
            "POST",
            "/sql",
            {
                "statement": (
                    "SELECT p.v,q.v,r.v FROM numeric_probe p "
                    "JOIN numeric_probe q ON p.k=q.k "
                    "JOIN (SELECT k,v FROM numeric_probe WHERE v=0) r "
                    "ON q.k=r.k ORDER BY p.v"
                )
            },
        )["rows"] == [["0", "0", "0"]]
        assert call(
            "POST",
            "/sql",
            {
                "statement": f"SELECT k=CAST('{targets[0]}' AS NUMERIC) FROM numeric_probe WHERE v<2 ORDER BY v"
            },
        )["rows"] == [[True], [False]]
        server.pause()
        # Keep identity/footer intact but remove usable data pages, then drop
        # range caches. The repeated cold query must consume covering values.
        footer = len(payload) - 8 - struct.unpack("<I", payload[-8:-4])[0]
        object_path.write_bytes(
            header + payload[:4] + b"\xff" * (footer - 4) + payload[footer:]
        )
        for cached in server.root.rglob("lake-ranges"):
            if cached.is_dir():
                shutil.rmtree(cached)
        server.resume()
        assert call("POST", "/sql", {"statement": statement})["rows"] == expected
        # Exact standalone equality/range predicates must seek the same native
        # covering index; a residual scan cannot read the damaged data pages.
        assert call("POST", "/sql", {"statement": predicate})["rows"] == [["0"]]
        assert (
            call(
                "POST",
                "/sql",
                {
                    "statement": f"SELECT v FROM numeric_probe WHERE k>CAST('{targets[0]}' AS NUMERIC)"
                },
            )["rows"]
            == []
        )
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize(
    "physical_type",
    ["integer", "decimal_string", "decimal128", "decimal128_dictionary"],
)
def test_numeric_algebraic_materializations_keep_exact_groups_and_inputs(
    tmp_path, physical_type
):
    """Independent Parquet values retain NUMERIC identity through publication and restart."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    keys = (
        ["9007199254740992", "9007199254740993"]
        if physical_type == "integer"
        else ["9007199254740993.124", "9007199254740993.125"]
    )
    values = (
        pa.array([int(k) for k in keys] + [None], type=pa.int64())
        if physical_type == "integer"
        else pa.array([Decimal(k) for k in keys] + [None], type=pa.decimal128(38, 3))
        if physical_type.startswith("decimal128")
        else pa.array(keys + [None])
    )
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    path = tmp_path / "numeric.parquet"
    pq.write_table(
        pa.table({"k": values}),
        path,
        compression="snappy",
        use_dictionary=physical_type.endswith("_dictionary"),
    )
    payload = path.read_bytes()
    header = (
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
    )
    object_path = objects / "part.parquet"
    object_path.write_bytes(header + payload)
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:

        def call(method, route, body=None):
            response = requests.request(
                method,
                server.api_url + route,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=120,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json() if response.content else None

        call(
            "POST",
            "/tables/numeric_groups",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "base_source": {
                        "kind": "external",
                        "table_id": "numeric-groups",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "k": {
                                        "type": "number",
                                        "x-antfly-sql-type": "numeric",
                                    }
                                },
                                "additionalProperties": False,
                            }
                        }
                    },
                },
                "indexes": {
                    "stats": {
                        "type": "algebraic",
                        "derive_from_schema": True,
                        "aggregates": [
                            {"name": "counts", "op": "count", "group_by": ["k"]},
                            *[
                                {
                                    "name": op,
                                    "op": op,
                                    "measure": "k",
                                    "group_by": ["k"],
                                }
                                for op in ("sum", "min", "max")
                            ],
                        ],
                    }
                },
            },
        )
        deadline = time.monotonic() + 90
        while True:
            resource = call("GET", "/tables/numeric_groups/indexes/stats")
            assert resource["status"]["readiness"]["state"] != "failed", (
                str(resource) + server.debug_logs()
            )
            if resource["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        statement = "SELECT k, COUNT(*), SUM(k), MIN(k), MAX(k) FROM numeric_groups GROUP BY k ORDER BY k"
        expected = [[k, "1", k, k, k] for k in keys] + [[None, "1", None, None, None]]
        assert call("POST", "/sql", {"statement": statement})["rows"] == expected
        residual = (
            statement.replace("GROUP BY k", "GROUP BY k+CAST('0' AS NUMERIC)")
            .replace("SELECT k,", "SELECT k+CAST('0' AS NUMERIC),")
            .replace("ORDER BY k", "ORDER BY k+CAST('0' AS NUMERIC)")
        )
        assert call("POST", "/sql", {"statement": residual})["rows"] == expected
        server.pause()
        footer = len(payload) - 8 - struct.unpack("<I", payload[-8:-4])[0]
        object_path.write_bytes(
            header + payload[:4] + b"\xff" * (footer - 4) + payload[footer:]
        )
        for cached in server.root.rglob("lake-ranges"):
            if cached.is_dir():
                shutil.rmtree(cached)
        server.resume()
        assert call("POST", "/sql", {"statement": statement})["rows"] == expected
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_broad_native_text_windows_preserve_scores_ties_counts_and_hydration(
    tmp_path, inference_reranker, monkeypatch
):
    """Paged source spans from 131 independent Parquet files match the full corpus exactly."""
    import conftest

    original_config = conftest._write_remote_content_e2e_config

    def configured(root, **kwargs):
        path = original_config(root, **kwargs)
        config = json.loads(path.read_text())
        config.setdefault("lake_indexes", {}).setdefault("text_cache", {})[
            "admission_window_segments"
        ] = 1
        path.write_text(json.dumps(config))
        return path

    monkeypatch.setattr(conftest, "_write_remote_content_e2e_config", configured)
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    file_count = 131
    row_count = file_count * 3
    for part in range(file_count):
        path = tmp_path / "part.parquet"
        rows = [
            {
                "body": "quasar " * (i % 4 + 1)
                + "common " * (i % 3)
                + ("beta " if i % 2 else "alpha ")
                + ("navquasar " if i in (3, row_count - 2) else "")
                + f"vocab{i} ",
                "amount": i,
                "sparse_native": '{"1":1}',
            }
            for i in range(part * 3, part * 3 + 3)
        ]
        pq.write_table(
            pa.Table.from_pylist(rows), path, compression="snappy", row_group_size=2
        )
        payload = path.read_bytes()
        (objects / f"part-{part:03}.parquet").write_bytes(
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

        def call(method, route, body=None):
            response = requests.request(
                method,
                server.api_url + route,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=120,
            )
            assert response.ok, response.text + server.debug_logs()
            value = response.json() if response.content else None
            return (
                value["responses"][0]
                if isinstance(value, dict) and "responses" in value
                else value
            )

        definition = {
            "num_shards": 1,
            "schema": {
                "storage_mode": "relational",
                "default_type": "doc",
                "document_schemas": {
                    "doc": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "amount": {
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
                                "sparse_native": {"type": "string"},
                            },
                        }
                    }
                },
                "relational_indexes": [
                    {
                        "name": "amount_idx",
                        "keys": [{"column": "amount"}],
                        "include_columns": ["body"],
                    }
                ],
                "base_source": {
                    "kind": "external",
                    "table_id": "paged-text",
                    "format": "parquet",
                    "uri": root.as_uri(),
                },
            },
            "indexes": {
                "body_text": {"type": "full_text", "field": "body"},
                "all_text": {"type": "full_text"},
                "stored_text": {
                    "type": "full_text",
                    "field": "body",
                    "store_source": True,
                },
                "sparse_native": {
                    "type": "embeddings",
                    "external": True,
                    "sparse": True,
                },
            },
        }
        call("POST", "/tables/paged_text", definition)
        # One text index allows direct retrieval, exposing the native profile
        # separately from the named-component fusion profile below.
        profile_definition = json.loads(json.dumps(definition))
        profile_definition["schema"]["base_source"]["table_id"] = "profile-text"
        profile_definition["schema"]["relational_indexes"][0]["name"] = (
            "profile_amount_idx"
        )
        profile_definition["schema"]["relational_indexes"][0]["keys"] = [
            {"column": "amount", "direction": "desc", "nulls": "last"}
        ]
        profile_definition["indexes"] = {
            "body_text": {"type": "full_text", "store_source": True}
        }
        call("POST", "/tables/profile_text", profile_definition)
        deadline = time.monotonic() + 120
        while True:
            resource = call("GET", "/tables/paged_text/indexes/body_text")
            assert resource["status"]["readiness"]["state"] != "failed", (
                str(resource) + server.debug_logs()
            )
            sortable = call("GET", "/tables/paged_text/indexes/all_text")
            assert sortable["status"]["readiness"]["state"] != "failed", (
                str(sortable) + server.debug_logs()
            )
            stored = call("GET", "/tables/paged_text/indexes/stored_text")
            assert stored["config"]["store_source"] is True, stored
            assert stored["status"]["readiness"]["state"] != "failed", (
                str(stored) + server.debug_logs()
            )
            sparse = call("GET", "/tables/paged_text/indexes/sparse_native")
            assert sparse["status"]["readiness"]["state"] != "failed", (
                str(sparse) + server.debug_logs()
            )
            profile_index = call("GET", "/tables/profile_text/indexes/body_text")
            assert profile_index["status"]["readiness"]["state"] != "failed", (
                str(profile_index) + server.debug_logs()
            )
            if all(
                item["status"]["readiness"]["queryable"]
                for item in (resource, sortable, stored, sparse, profile_index)
            ):
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        request = {
            "full_text_search": {"match": "quasar", "field": "body"},
            "full_text_index": "body_text",
            "fields": ["amount"],
            "limit": row_count,
        }

        def full_admission(query):
            # Exercise alternate Boolean lowering without changing scores.
            # The all-row regression below independently verifies global cuts.
            baseline = {
                "bool": {
                    "must_not": [{"term": "neverpresentoracle", "field": "body"}],
                    "boost": 0,
                }
            }
            reference = dict(query, profile=True)
            reference["full_text_search"] = {
                "bool": {"must": [query["full_text_search"]], "should": [baseline]}
            }
            return call("POST", "/tables/paged_text/query", reference)

        oracle = full_admission(request)
        expected = [
            (h["_id"], h["_score"], h["_source"]["amount"])
            for h in oracle["hits"]["hits"]
        ]
        assert len(expected) == row_count

        def check():
            result = call(
                "POST", "/tables/paged_text/query", dict(request, limit=19, offset=61)
            )
            assert [
                (h["_id"], h["_score"], h["_source"]["amount"])
                for h in result["hits"]["hits"]
            ] == expected[61:80]
            # Direct retrieval exposes its native profile. Named components
            # retain the parent's fusion profile instead.
            direct = dict(request, limit=19, offset=61, profile=True)
            del direct["full_text_index"]
            profiled = call("POST", "/tables/profile_text/query", direct)
            assert [
                (h["_score"], h["_source"]["amount"]) for h in profiled["hits"]["hits"]
            ] == [(score, amount) for _, score, amount in expected[61:80]]
            assert profiled["profile"]["sort"]["plan"] == "score_top_k", profiled
            assert profiled["profile"]["sort"]["selection_reason"] == (
                "bounded_global_text_frontier"
            ), profiled
            # Constant-score clauses keep finite bounds in selective Boolean trees.
            for clause in ("must", "should"):
                boolean = {"must": [direct["full_text_search"]]}
                boolean.setdefault(clause, []).append({"match_all": {}})
                mixed = dict(direct, full_text_search={"bool": boolean})
                found = call("POST", "/tables/profile_text/query", mixed)
                complete = full_admission(
                    dict(request, full_text_search=mixed["full_text_search"])
                )
                assert [
                    (h["_score"], h["_source"]["amount"]) for h in found["hits"]["hits"]
                ] == [
                    (h["_score"], h["_source"]["amount"])
                    for h in complete["hits"]["hits"][61:80]
                ]
                assert found["profile"]["sort"]["selection_reason"] == (
                    "bounded_global_text_frontier"
                ), found
            # Pattern admission uses the global vocabulary and routing summaries,
            # with equivalent Boolean lowering for score and result comparisons.
            for pattern in (
                {"prefix": "navquasa", "field": "body"},
                {"wildcard": "navquasa?", "field": "body"},
                {"regexp": "navquasa[a-z]", "field": "body"},
                {"fuzzy": "navquasor", "field": "body", "fuzziness": 1},
            ):
                patterned = dict(direct, full_text_search=pattern, offset=0, limit=7)
                found = call("POST", "/tables/profile_text/query", patterned)
                complete = full_admission(dict(request, full_text_search=pattern))
                assert [
                    (h["_score"], h["_source"]["amount"]) for h in found["hits"]["hits"]
                ] == [
                    (h["_score"], h["_source"]["amount"])
                    for h in complete["hits"]["hits"]
                ]
                assert {h["_source"]["amount"] for h in found["hits"]["hits"]} == {
                    3,
                    row_count - 2,
                }
                assert found["profile"]["sort"]["selection_reason"] == (
                    "bounded_global_text_frontier"
                ), found
            # Constant-score ties retain the independent full-admission order.
            for prefix, offset in (("qu", 0), ("qu", 61), ("vocab", 0), ("vocab", 61)):
                tied = dict(
                    direct,
                    full_text_search={"prefix": prefix, "field": "body"},
                    offset=offset,
                    limit=19,
                )
                found = call("POST", "/tables/profile_text/query", tied)
                complete = full_admission(
                    dict(
                        request,
                        full_text_search=tied["full_text_search"],
                        offset=offset,
                        limit=19,
                    )
                )
                assert [
                    (h["_score"], h["_source"]["amount"]) for h in found["hits"]["hits"]
                ] == [
                    (h["_score"], h["_source"]["amount"])
                    for h in complete["hits"]["hits"]
                ]
            # The global upper bound must include nested negative-only scores.
            # Fetching all rows establishes an independent page oracle without
            # allowing a competitive global cutoff to prune any pack.
            for positive in (
                {"match": "quasar", "field": "body"},
                {"prefix": "vocab", "field": "body"},
                {"match_phrase": "quasar quasar", "field": "body"},
            ):
                nested = {
                    "bool": {
                        "must": [
                            positive,
                            {
                                "bool": {
                                    "must_not": [
                                        {
                                            "term": "neverpresentbaseline",
                                            "field": "body",
                                        }
                                    ],
                                    "boost": 3,
                                }
                            },
                        ]
                    }
                }
                all_rows = call(
                    "POST",
                    "/tables/profile_text/query",
                    dict(direct, full_text_search=nested, offset=0, limit=row_count),
                )["hits"]["hits"]
                for offset in (0, 61):
                    page = call(
                        "POST",
                        "/tables/profile_text/query",
                        dict(direct, full_text_search=nested, offset=offset, limit=19),
                    )["hits"]["hits"]
                    assert page == all_rows[offset : offset + 19]
            filtered_query = dict(
                direct,
                offset=0,
                limit=7,
                filter_query={"range": {"path": "/amount", "gte": 50, "lt": 160}},
            )
            filtered = call("POST", "/tables/profile_text/query", filtered_query)
            complete = full_admission(
                dict(
                    request,
                    filter_query=filtered_query["filter_query"],
                    offset=0,
                    limit=7,
                )
            )
            assert [
                (h["_score"], h["_source"]["amount"]) for h in filtered["hits"]["hits"]
            ] == [
                (h["_score"], h["_source"]["amount"]) for h in complete["hits"]["hits"]
            ]
            highlighted = call(
                "POST",
                "/tables/profile_text/query",
                dict(direct, highlight={"fields": ["body"]}),
            )
            assert highlighted["profile"]["sort"]["selection_reason"] == (
                "bounded_global_text_frontier"
            ), highlighted
            assert all(h["_highlights"]["body"] for h in highlighted["hits"]["hits"])
            metadata_count = call(
                "POST", "/tables/profile_text/query", dict(direct, limit=0, offset=0)
            )
            assert metadata_count["hits"]["total"] == {
                "value": row_count,
                "relation": "exact",
            }
            assert metadata_count["profile"]["sort"]["selection_reason"] == (
                "native_text_metadata_count"
            ), metadata_count
            ordered_query = dict(
                direct, limit=7, offset=0, order_by=[{"field": "amount", "desc": True}]
            )
            ordered = call("POST", "/tables/profile_text/query", ordered_query)
            assert ordered["profile"]["sort"]["selection_reason"] == (
                "bounded_ordered_lake_index"
            ), json.dumps(ordered["profile"], sort_keys=True)
            assert [h["_source"]["amount"] for h in ordered["hits"]["hits"]] == list(
                range(row_count - 1, row_count - 8, -1)
            )
            forward = call(
                "POST",
                "/tables/profile_text/query",
                dict(
                    ordered_query,
                    search_after=ordered["hits"]["hits"][-1]["_sort"],
                    remote_snapshot=ordered["remote_snapshot"],
                ),
            )
            assert [h["_source"]["amount"] for h in forward["hits"]["hits"]] == list(
                range(row_count - 8, row_count - 15, -1)
            )
            backward = call(
                "POST",
                "/tables/profile_text/query",
                dict(
                    ordered_query,
                    search_before=forward["hits"]["hits"][0]["_sort"],
                    remote_snapshot=forward["remote_snapshot"],
                ),
            )
            assert backward["hits"]["hits"] == ordered["hits"]["hits"]
            total = result["hits"]["total"]
            assert total["relation"] in ("exact", "gte")
            assert total["value"] <= row_count
            if total["relation"] == "exact":
                assert total["value"] == row_count
            counted = call("POST", "/tables/paged_text/query", dict(request, limit=0))
            assert counted["hits"]["total"] == {"value": row_count, "relation": "exact"}
            assert counted["hits"]["hits"] == []
            shapes = [
                {
                    "embeddings": {"sparse_native": {"indices": [1], "values": [1]}},
                    "indexes": ["sparse_native"],
                    "highlight": {"fields": ["body"]},
                },
                {
                    "evaluate": {
                        "scope": "candidates",
                        "candidate_count": 31,
                        "compute": {"hidden_body": {"field": "body"}},
                        "order_by": [
                            {"expression": {"field": "amount"}, "descending": True}
                        ],
                    }
                },
                {
                    "evaluate": {
                        "scope": "matches",
                        "max_rows": row_count,
                        "compute": {"hidden_body": {"field": "body"}},
                        "order_by": [
                            {"expression": {"field": "amount"}, "descending": True}
                        ],
                    },
                    "highlight": {"fields": ["body"]},
                },
                {"pruner": {"min_score_ratio": 0.75}},
                {
                    "reranker": {
                        "provider": "antfly",
                        "model": "test-reranker",
                        "url": inference_reranker,
                        "field": "body",
                        "candidate_count": 31,
                    }
                },
                {"highlight": {"fields": ["body"]}},
                {"full_text_index": "stored_text", "highlight": {"fields": ["body"]}},
                {"filter_query": {"range": {"path": "/amount", "gte": 50, "lt": 160}}},
                {
                    "exclusion_query": {
                        "range": {"path": "/amount", "gte": 50, "lt": 160}
                    }
                },
                {
                    "filter_query": {"range": {"path": "/amount", "gte": 50}},
                    "exclusion_query": {"range": {"path": "/amount", "gte": 160}},
                },
                {
                    "order_by": [{"field": "amount", "desc": True}],
                    "full_text_index": "all_text",
                },
                {
                    "filter_query": {"range": {"path": "/amount", "gte": 50}},
                    "order_by": [{"field": "amount"}],
                    "full_text_index": "all_text",
                    "highlight": {"fields": ["body"]},
                },
            ]
            # Score transforms and evaluation must page once after the global
            # candidate pool is selected, including nonzero offsets.
            shapes += [
                dict(shape, offset=3)
                for shape in shapes
                if any(name in shape for name in ("evaluate", "pruner", "reranker"))
            ]
            shapes += [
                dict(shape, fields=[])
                for shape in shapes
                if "reranker" in shape and "offset" not in shape
            ]
            for shape in shapes:
                query = dict(request, **shape, limit=7)
                reference = full_admission(query)
                actual = call("POST", "/tables/paged_text/query", query)
                assert actual["hits"]["hits"] == reference["hits"]["hits"], (
                    actual,
                    reference,
                )
                profiled = call(
                    "POST", "/tables/paged_text/query", dict(query, profile=True)
                )
                assert profiled["hits"]["hits"] == actual["hits"]["hits"]
                if (
                    "reranker" not in shape
                    and "pruner" not in shape
                    and "evaluate" not in shape
                ):
                    counted = call(
                        "POST", "/tables/paged_text/query", dict(query, limit=0)
                    )
                    assert (
                        counted["hits"]["total"]
                        == full_admission(dict(query, limit=0))["hits"]["total"]
                    )
                    assert counted["hits"]["hits"] == []
                assert all(
                    set(h["_source"]) == set(query["fields"])
                    for h in actual["hits"]["hits"]
                )
                if "evaluate" in shape:
                    assert actual["evaluation"]["evaluated_rows"] == shape[
                        "evaluate"
                    ].get("candidate_count", shape["evaluate"].get("max_rows")), actual
                    for hit in actual["hits"]["hits"]:
                        amount = hit["_source"]["amount"]
                        assert hit["_computed"]["hidden_body"] == (
                            "quasar " * (amount % 4 + 1)
                            + "common " * (amount % 3)
                            + ("beta " if amount % 2 else "alpha ")
                            + ("navquasar " if amount in (3, row_count - 2) else "")
                            + f"vocab{amount} "
                        )
                if "reranker" in shape:
                    assert (
                        profiled["profile"]["reranker"]["documents_reranked"] == 31
                    ), profiled
                if "reranker" in shape and query["fields"]:
                    for hit in actual["hits"]["hits"]:
                        assert hit["_score"] == pytest.approx(
                            0.95 if hit["_source"]["amount"] % 2 else 0.6
                        )
                if "highlight" in shape:
                    assert all(h["_highlights"]["body"] for h in actual["hits"]["hits"])
                if "order_by" in shape:
                    cursor = actual["hits"]["hits"][-1]["_sort"]
                    forward = dict(
                        query,
                        search_after=cursor,
                        remote_snapshot=actual["remote_snapshot"],
                    )
                    following = call("POST", "/tables/paged_text/query", forward)
                    assert (
                        following["hits"]["hits"]
                        == full_admission(forward)["hits"]["hits"]
                    )
                    backward = dict(
                        query,
                        search_before=following["hits"]["hits"][0]["_sort"],
                        remote_snapshot=following["remote_snapshot"],
                    )
                    assert (
                        call("POST", "/tables/paged_text/query", backward)["hits"][
                            "hits"
                        ]
                        == actual["hits"]["hits"]
                    )

        check()
        rejected = requests.post(
            server.api_url + "/tables/paged_text/query",
            json=dict(
                request,
                evaluate={
                    "scope": "matches",
                    "max_rows": 31,
                    "compute": {"hidden_body": {"field": "body"}},
                },
            ),
            auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
            timeout=120,
        )
        assert (
            rejected.status_code == 422
            and "decision_limit_exceeded" in rejected.text.lower()
        ), rejected.text + server.debug_logs()
        server.restart()
        check()
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize(
    "physical_type",
    ["integer", "decimal_string", "decimal128", "decimal128_dictionary"],
)
def test_numeric_predicates_use_only_compatible_parquet_pruning(
    tmp_path, physical_type
):
    """Exact integer stats prune; eight-byte decimal strings never become int stats."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    keys = (
        [9007199254740992, 9007199254740993]
        if physical_type == "integer"
        else [10000000, 99999999]
    )
    values = (
        pa.array(keys, type=pa.int64())
        if physical_type == "integer"
        else pa.array([Decimal(k) for k in keys], type=pa.decimal128(38, 0))
        if physical_type.startswith("decimal128")
        else pa.array([str(k) for k in keys])
    )
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    path = tmp_path / "numbers.parquet"
    pq.write_table(
        pa.table({"k": values}),
        path,
        compression=None,
        use_dictionary=physical_type.endswith("_dictionary"),
        write_page_index=True,
        data_page_version="2.0",
    )
    payload = path.read_bytes()
    header = (
        b"AFOBJ001"
        + struct.pack("<QI", len(payload), 0)
        + hashlib.sha256(payload).hexdigest().encode()
    )
    object_path = objects / "part.parquet"
    object_path.write_bytes(header + payload)
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:

        def call(route, body):
            response = requests.post(
                server.api_url + route,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=120,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json()

        call(
            "/tables/numeric_pruning",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "base_source": {
                        "kind": "external",
                        "table_id": "numeric-pruning",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "k": {
                                        "type": "number",
                                        "x-antfly-sql-type": "numeric",
                                    }
                                },
                                "additionalProperties": False,
                            }
                        }
                    },
                },
                "indexes": {},
            },
        )
        for key in keys:
            assert call(
                "/sql",
                {
                    "statement": f"SELECT k FROM numeric_pruning WHERE k=CAST('{key}' AS NUMERIC)"
                },
            )["rows"] == [[str(key)]]
        if physical_type == "integer":
            server.pause()
            # Column/offset indexes lie between data pages and the footer.
            # Keep those authenticated planning structures readable as well.
            damaged = bytearray(payload)
            metadata = pq.ParquetFile(path).metadata
            for group in range(metadata.num_row_groups):
                for column in range(metadata.row_group(group).num_columns):
                    chunk = metadata.row_group(group).column(column)
                    start = (
                        chunk.dictionary_page_offset
                        if chunk.has_dictionary_page
                        else chunk.data_page_offset
                    )
                    end = start + chunk.total_compressed_size
                    damaged[start:end] = b"\xff" * (end - start)
            unreadable_root = tmp_path / "unreadable"
            unreadable_objects = unreadable_root / "buckets" / "antfly" / "objects"
            unreadable_objects.mkdir(parents=True)
            # Authenticate the deliberately unreadable source as a distinct
            # current snapshot, as the independent Bloom regression does.
            unreadable_objects.joinpath("part.parquet").write_bytes(
                b"AFOBJ001"
                + struct.pack("<QI", len(damaged), 0)
                + hashlib.sha256(damaged).hexdigest().encode()
                + damaged
            )
            for cached in server.root.rglob("lake-ranges"):
                if cached.is_dir():
                    shutil.rmtree(cached)
            server.resume()
            call(
                "/tables/numeric_pruning_unreadable",
                {
                    "num_shards": 1,
                    "schema": {
                        "storage_mode": "relational",
                        "default_type": "row",
                        "base_source": {
                            "kind": "external",
                            "table_id": "numeric-pruning-unreadable",
                            "format": "parquet",
                            "uri": unreadable_root.as_uri(),
                        },
                        "document_schemas": {
                            "row": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "k": {
                                            "type": "number",
                                            "x-antfly-sql-type": "numeric",
                                        }
                                    },
                                    "additionalProperties": False,
                                }
                            }
                        },
                    },
                    "indexes": {},
                },
            )
            for op, bound in (
                (">", "9007199254740993.125"),
                ("=", "9007199254740994"),
                ("<", "9007199254740991.875"),
            ):
                assert call(
                    "/sql",
                    {
                        "statement": f"SELECT COUNT(*) FROM numeric_pruning_unreadable WHERE k{op}CAST('{bound}' AS NUMERIC)"
                    },
                )["rows"] == [["0"]]
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_remote_transform_dependencies_bound_wide_candidate_sources(tmp_path):
    """Unrelated wide columns cannot enter the transform candidate pool."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    for part in range(67):
        path = tmp_path / "part.parquet"
        pq.write_table(
            pa.table({"body": ["quasar"] * 3, "unused": ["x" * (512 * 1024)] * 3}),
            path,
            compression="snappy",
            use_dictionary=False,
            write_statistics=False,
            row_group_size=2,
        )
        data = path.read_bytes()
        (objects / f"part-{part:03}.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(data), 0)
            + hashlib.sha256(data).hexdigest().encode()
            + data
        )
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:
        definition = {
            "num_shards": 1,
            "schema": {
                "storage_mode": "relational",
                "default_type": "doc",
                "document_schemas": {
                    "doc": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "body": {
                                    "type": "string",
                                    "x-antfly-field": {"type": "text"},
                                },
                                "unused": {"type": "string"},
                            },
                        }
                    }
                },
                "base_source": {
                    "kind": "external",
                    "table_id": "wide-transform",
                    "format": "parquet",
                    "uri": root.as_uri(),
                },
            },
            "indexes": {"body_text": {"type": "full_text", "field": "body"}},
        }

        def call(method, path, body=None):
            return requests.request(
                method, server.api_url + path, json=body, timeout=120
            )

        created = call("POST", "/tables/wide_transform", definition)
        assert created.ok, created.text + server.debug_logs()
        deadline = time.monotonic() + 120
        while True:
            index = call("GET", "/tables/wide_transform/indexes/body_text").json()
            assert index["status"]["readiness"]["state"] != "failed", index
            if index["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(index) + server.debug_logs()
            time.sleep(0.1)
        query = {
            "full_text_search": {"match": "quasar", "field": "body"},
            "full_text_index": "body_text",
            "fields": ["body"],
            "limit": 1,
            "evaluate": {
                "scope": "candidates",
                "candidate_count": 201,
                "compute": {"tiny": {"field": "body"}},
            },
        }
        for restart in (False, True):
            if restart:
                server.restart()
            response = call("POST", "/tables/wide_transform/query", query)
            assert response.ok, response.text + server.debug_logs()
            value = response.json()
            result = value.get("responses", [value])[0]
            assert result["evaluation"]["evaluated_rows"] == 201
            assert len(result["hits"]["hits"]) == 1
            hit = result["hits"]["hits"][0]
            assert hit["_source"] == {"body": "quasar"}
            assert hit["_computed"]["tiny"] == "quasar"
            # A genuine wide dependency uses the same pre-hydration byte guard.
            wide = dict(
                query,
                evaluate=dict(query["evaluate"], compute={"wide": {"field": "unused"}}),
            )
            rejected = call("POST", "/tables/wide_transform/query", wide)
            assert rejected.status_code == 422, rejected.text + server.debug_logs()
            assert rejected.json()["code"] == "decision_limit_exceeded"
            # Final full source is needed only for the one surviving row.
            complete = dict(query)
            del complete["fields"]
            response = call("POST", "/tables/wide_transform/query", complete)
            assert response.ok, response.text + server.debug_logs()
            value = response.json()
            result = value.get("responses", [value])[0]
            assert len(result["hits"]["hits"][0]["_source"]["unused"]) == 512 * 1024
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_published_parquet_skewed_files_share_row_groups_and_stream_in_order(tmp_path):
    """A 20,000-row file plus tiny files exercises cold rolling scan workers."""
    from decimal import Decimal

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    psycopg = pytest.importorskip("psycopg")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    total = 0
    for part in range(10):
        count = 20000 if part == 4 else 32
        path = tmp_path / "part.parquet"
        pq.write_table(
            pa.table(
                {
                    "amount": pa.array(range(total, total + count), type=pa.int64()),
                    "body": ["common"] * count,
                    # Preserve >2 MiB of source bytes so ordinary SQL delivery
                    # admits its ordered worker path, regardless of compression.
                    "payload": ["x" * 256] * count,
                    "amount_decimal": pa.array(
                        [Decimal(i).scaleb(-2) for i in range(total, total + count)],
                        type=pa.decimal128(20, 2),
                    ),
                    "maybe": [
                        None if i % 7 == 0 else "kept"
                        for i in range(total, total + count)
                    ],
                }
            ),
            path,
            compression=None,
            use_dictionary=False,
            write_page_index=True,
            row_group_size=256,
        )
        data = path.read_bytes()
        (objects / f"part-{part:02}.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(data), 0)
            + hashlib.sha256(data).hexdigest().encode()
            + data
        )
        total += count
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
        pgwire=True,
    )
    failed = True
    try:

        def call(method, path, body=None):
            response = requests.request(
                method,
                server.api_url + path,
                json=body,
                auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                timeout=180,
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json()

        call(
            "POST",
            "/tables/skew_scan",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "skew-scan",
                        "format": "parquet",
                        "object_mutability": "immutable",
                        "uri": root.as_uri(),
                    },
                },
                "indexes": {"body_text": {"type": "full_text", "field": "body"}},
            },
        )
        deadline = time.monotonic() + 180
        while True:
            index = call("GET", "/tables/skew_scan/indexes/body_text")
            assert index["status"]["readiness"]["state"] != "failed", index
            if index["status"]["readiness"]["queryable"]:
                break
            assert time.monotonic() < deadline, str(index) + server.debug_logs()
            time.sleep(0.1)
        expected = list(range(total))
        divisible = [i for i in expected if i % 3 == 0]
        first_order = None
        first_ids = None
        # Inference preserves the existing decimal-as-string read contract.
        # Explicit NUMERIC selects native coefficients for arithmetic execution.
        call(
            "POST",
            "/tables/skew_numeric",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "base_source": {
                        "kind": "external",
                        "table_id": "skew-numeric",
                        "format": "parquet",
                        "object_mutability": "immutable",
                        "uri": root.as_uri(),
                    },
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "amount": {"type": "integer"},
                                    "amount_decimal": {
                                        "type": "number",
                                        "x-antfly-sql-type": "numeric",
                                    },
                                },
                                "additionalProperties": False,
                            }
                        }
                    },
                },
            },
        )
        for _ in range(2):
            server.restart()
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
                # Full delivery uses the streaming transport; the HTTP result
                # cap remains appropriate for the small aggregates below.
                order = [
                    row[0]
                    for row in cursor.stream(
                        f"SELECT amount FROM skew_scan LIMIT {total}", size=137
                    )
                ]
            # Sparse survivor masks precede wide/decimal projection decoding.
            # Check both a surviving NULL and a nonnull value after each restart.
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount, payload, amount_decimal, maybe FROM skew_scan WHERE amount >= 250 AND amount <= 252 ORDER BY amount"
                },
            )["rows"] == [
                ["250", "x" * 256, "2.50", "kept"],
                ["251", "x" * 256, "2.51", "kept"],
                ["252", "x" * 256, "2.52", None],
            ]
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount_decimal = '2.50' FROM skew_scan WHERE amount >= 250 AND amount <= 252 ORDER BY amount"
                },
            )["rows"] == [[True], [False], [False]]
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount_decimal + 1.25, -(amount_decimal * 2.00), amount_decimal * amount, amount_decimal % 2.00, amount_decimal / 2.00 FROM skew_numeric WHERE amount >= 250 AND amount <= 252 ORDER BY amount"
                },
            )["rows"] == [
                ["3.75", "-5.0000", "625.00", "0.50", "1.25000000000000000000"],
                ["3.76", "-5.0200", "630.01", "0.51", "1.25500000000000000000"],
                ["3.77", "-5.0400", "635.04", "0.52", "1.26000000000000000000"],
            ]
            # Exact coefficient expressions must survive derived frames and
            # retained grouped state across cold process restarts.
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT k, SUM(adjusted), MIN(adjusted), MAX(adjusted) FROM (SELECT amount AS k, (amount_decimal + 1.25) * 2.00 AS adjusted FROM skew_numeric WHERE amount >= 250 AND amount <= 252) AS d GROUP BY k ORDER BY k"
                },
            )["rows"] == [
                ["250", "7.5000", "7.5000", "7.5000"],
                ["251", "7.5200", "7.5200", "7.5200"],
                ["252", "7.5400", "7.5400", "7.5400"],
            ]
            delivered = []
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
                pytest.raises(psycopg.errors.InvalidTextRepresentation),
            ):
                delivered.extend(
                    row[0]
                    for row in cursor.stream(
                        f"SELECT CAST(CASE WHEN amount = {order[0]} THEN '1' ELSE 'bad' END AS BIGINT) FROM skew_scan LIMIT 2",
                        size=1,
                    )
                )
            assert delivered == [1]
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT COUNT(*), SUM(total) FROM (SELECT amount, SUM(amount + 1) AS total FROM skew_scan GROUP BY amount) groups"
                },
            )["rows"] == [[str(total), str(sum(expected) + total)]]
            assert sorted(order) == expected
            if first_order is None:
                first_order = order
            else:
                assert order == first_order
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT SUM(amount), COUNT(*) FROM skew_scan WHERE amount % 3 = 0"
                },
            )["rows"] == [[str(sum(divisible)), str(len(divisible))]]
            bounded = [value for value in expected if 4000 <= value < 7000]
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT COUNT(*), SUM(amount) FROM skew_scan WHERE amount >= 4000 AND amount < 7000"
                },
            )["rows"] == [[str(len(bounded)), str(sum(bounded))]]
            identities = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT _id, amount FROM skew_scan WHERE amount >= 4000 AND amount < 7000 ORDER BY amount LIMIT 3"
                },
            )["rows"]
            assert [int(row[1]) for row in identities] == bounded[:3]
            assert len({row[0] for row in identities}) == 3
            if first_ids is None:
                first_ids = identities
            else:
                assert identities == first_ids
            # Mixed FILTER cohorts retain native computed vectors and evaluate
            # invalid expressions only for lanes that survive their filter.
            filtered = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount % 5 AS bucket, SUM(amount + 1) FILTER (WHERE amount % 3 = 0), COUNT(*) FILTER (WHERE amount % 3 = 0), SUM(1 / 0) FILTER (WHERE FALSE), SUM(amount) FROM skew_scan GROUP BY amount % 5 ORDER BY bucket"
                },
            )
            assert filtered["rows"] == [
                [
                    str(bucket),
                    str(sum(i + 1 for i in divisible if i % 5 == bucket)),
                    str(sum(1 for i in divisible if i % 5 == bucket)),
                    None,
                    str(sum(expected[bucket::5])),
                ]
                for bucket in range(5)
            ], filtered
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT "
                    + ", ".join(["COUNT(*)"] * 12)
                    + " FROM skew_scan"
                },
            )["rows"] == [[str(total)] * 12]
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount % 5 AS bucket, SUM(amount), COUNT(*), MIN(amount), MAX(amount) FROM skew_scan GROUP BY amount % 5 ORDER BY bucket"
                },
            )["rows"] == [
                [
                    str(bucket),
                    str(sum(expected[bucket::5])),
                    str(len(expected[bucket::5])),
                    str(min(expected[bucket::5])),
                    str(max(expected[bucket::5])),
                ]
                for bucket in range(5)
            ]
            # Derived expressions retain typed columns through the relation
            # adapter; selected FILTER cohorts reach aggregate kernels directly.
            derived = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT bucket, SUM(v) FILTER (WHERE v % 3 = 1), COUNT(*) FROM (SELECT amount % 5 AS bucket, amount + 1 AS v FROM skew_scan) d GROUP BY bucket ORDER BY bucket"
                },
            )
            assert derived["rows"] == [
                [
                    str(bucket),
                    str(sum(i + 1 for i in expected[bucket::5] if i % 3 == 0)),
                    str(len(expected[bucket::5])),
                ]
                for bucket in range(5)
            ], derived
            # Mixed exact-array/native outputs keep primitive lanes batched;
            # unused derived expressions never fetch or evaluate cold payloads.
            mixed = call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT SUM(v), COUNT(arr) FROM (SELECT amount + 1 AS v, ARRAY[amount, amount+1]::int8[] AS arr, 1/0 AS unused FROM skew_scan) d"
                },
            )
            assert mixed["rows"] == [[str(sum(expected) + total), str(total)]], mixed
            # Thousands of partial groups use hash-owned final reduction. The
            # final typed batches must preserve exact totals across cold starts.
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT amount % 2048 AS bucket, SUM(amount), COUNT(*) "
                    "FROM skew_scan GROUP BY amount % 2048 ORDER BY bucket LIMIT 16"
                },
            )["rows"] == [
                [
                    str(bucket),
                    str(sum(expected[bucket::2048])),
                    str(len(expected[bucket::2048])),
                ]
                for bucket in range(16)
            ]
            # The sealed join filter must reach the probe scan even though
            # only aggregate output is projected; duplicates retain join weight.
            assert call(
                "POST",
                "/sql",
                {
                    "statement": "SELECT SUM(p.amount), COUNT(*) FROM skew_scan p "
                    "JOIN (VALUES (0),(1),(1),(2),(NULL)) b(k) ON p.amount=b.k"
                },
            )["rows"] == [["4", "4"]]
        failed = False
    finally:
        if failed and server.log_path.exists():
            print(server.debug_logs())
        server.stop(test_failed=failed)


@pytest.mark.parametrize("dictionary", [False, True])
def test_real_join_filters_and_source_ids_survive_index_lifecycle(tmp_path, dictionary):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    path = tmp_path / "input.parquet"
    pq.write_table(
        pa.table(
            {
                "id": range(75),
                "n": pa.array([0.1 if i % 2 == 0 else 0.2 for i in range(75)]),
                "label": ["quasar"] * 75,
            }
        ),
        path,
        use_dictionary=dictionary,
        row_group_size=19,
        write_page_index=True,
    )
    data = path.read_bytes()
    (objects / "part.parquet").write_bytes(
        b"AFOBJ001"
        + struct.pack("<QI", len(data), 0)
        + hashlib.sha256(data).hexdigest().encode()
        + data
    )
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:

        def call(method, path, body=None):
            response = _request_with_idle_ddl_retry(
                server, method, path, body, retry_ddl=True
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json() if response.content else None

        def sql(statement):
            return call("POST", "/sql", {"statement": statement})["rows"]

        schema = {
            "storage_mode": "relational",
            "base_source": {
                "kind": "external",
                "table_id": "real_identity",
                "format": "parquet",
                "uri": root.as_uri(),
                "object_mutability": "immutable",
            },
            "default_type": "row",
            "document_schemas": {
                "row": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "n": {"type": "number", "x-antfly-sql-type": "float32"},
                            "label": {"type": "string"},
                        },
                        "additionalProperties": False,
                    }
                }
            },
        }
        created = call("POST", "/tables/real_identity", {"schema": schema})
        assert created["schema"]["base_source"]["row_identity_version"] == 2
        original = sql("SELECT id, _id FROM real_identity ORDER BY id")
        assert len(original) == 75
        assert all(row[1].startswith("lake3:") for row in original)

        def verify():
            assert sql("SELECT id, _id FROM real_identity ORDER BY id") == original
            # Static pushed filters share the same declared REAL boundary as
            # projection and join filters, including DOUBLE-backed dictionaries.
            rounded = "0.10000000149011612"
            for op in ["=", "<>", "<", "<=", ">", ">="]:
                assert sql(
                    f"SELECT id FROM real_identity WHERE n {op} {rounded} ORDER BY id"
                ) == sql(
                    f"SELECT id FROM real_identity WHERE n + 0 {op} {rounded} ORDER BY id"
                )
            assert len(sql(f"SELECT id FROM real_identity WHERE n = {rounded}")) == 38
            expected = sql(
                "SELECT t.id, t.n FROM real_identity t "
                "JOIN (VALUES (0.1::real)) v(k) ON t.n + 0 = v.k ORDER BY t.id"
            )
            assert len(expected) == 38
            for source in ["real_identity", "(SELECT id, n FROM real_identity)"]:
                assert (
                    sql(
                        f"SELECT t.id, t.n FROM {source} t "
                        "JOIN (VALUES (0.1::real)) v(k) ON t.n = v.k ORDER BY t.id"
                    )
                    == expected
                )
            assert sql(
                "SELECT id FROM real_identity WHERE _id = '" + original[0][1] + "'"
            ) == [["0"]]

        verify()
        call(
            "POST",
            "/tables/real_identity/indexes/text",
            {"type": "full_text", "field": "label"},
        )
        deadline = time.monotonic() + 45
        while True:
            resource = call("GET", "/tables/real_identity/indexes/text")
            if resource["status"].get("published_revision") is not None:
                break
            assert time.monotonic() < deadline, str(resource) + server.debug_logs()
            time.sleep(0.1)
        verify()
        call("DELETE", "/tables/real_identity/indexes/text")
        verify()
        server.restart()
        verify()
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_parquet_snapshot_manifest_refresh_and_cold_native_navigation(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "committed"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)

    def put(name, payload):
        digest = hashlib.sha256(payload).hexdigest()
        (objects / name).write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + digest.encode()
            + payload
        )
        return digest

    def chunk(number, ids):
        path = tmp_path / f"part{number}.parquet"
        pq.write_table(pa.table({"id": ids, "body": ["quasar"] * len(ids)}), path)
        data = path.read_bytes()
        name = f"part{number}.parquet"
        etag = put(name, data)
        payload = json.dumps(
            [
                {
                    "file_id": name,
                    "object_uri": f"object://antfly/{name}",
                    "byte_len": len(data),
                    "row_count": len(ids),
                    "etag": etag,
                }
            ],
            separators=(",", ":"),
        ).encode()
        name = hashlib.sha256(payload).hexdigest() + ".json"
        digest = put(name, payload)
        return {
            "uri": f"object://antfly/{name}",
            "sha256": digest,
            "byte_len": len(payload),
            "file_count": 1,
        }, objects / name

    first, first_path = chunk(1, list(range(75)))
    head = "snapshot.parquet-manifest.json"

    def commit(chunks):
        put(
            head,
            json.dumps(
                {
                    "version": 1,
                    "source_id": "committed",
                    "chunks": chunks,
                    "file_count": len(chunks),
                },
                separators=(",", ":"),
            ).encode(),
        )

    commit([first])
    # A prefix listing would admit this invalid object. Commits name membership.
    put("uncommitted.parquet", b"not parquet")
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    held = first_path.with_suffix(".held")
    try:

        def call(method, path, body=None):
            response = _request_with_idle_ddl_retry(
                server, method, path, body, retry_ddl=True
            )
            assert response.ok, response.text + server.debug_logs()
            value = response.json() if response.content else None
            return value.get("responses", [value])[0] if value is not None else None

        def sql(statement):
            return call("POST", "/sql", {"statement": statement})["rows"]

        created = call(
            "POST",
            "/tables/committed",
            {
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "committed",
                        "format": "parquet",
                        "object_mutability": "immutable",
                        "uri": (root / head).as_uri(),
                    },
                }
            },
        )
        assert created["schema"]["base_source"]["row_identity_version"] == 2
        assert sql("SELECT count(*) FROM committed") == [["75"]]
        call(
            "POST",
            "/tables/committed/indexes/text",
            {"type": "full_text", "field": "body"},
        )

        def published(after=None):
            deadline = time.monotonic() + 60
            while True:
                status = call("GET", "/tables/committed/indexes/text")["status"]
                revision = status.get("published_revision")
                if revision is not None and revision != after:
                    return revision
                assert time.monotonic() < deadline, str(status) + server.debug_logs()
                time.sleep(0.1)

        generation = published()
        second, _ = chunk(2, [1000])
        commit([first, second])
        assert sql("SELECT id FROM committed WHERE id = 1000") == [["1000"]]
        published(generation)
        expected = call(
            "POST",
            "/tables/committed/query",
            {"full_text_search": {"match": "quasar", "field": "body"}, "limit": 5},
        )["hits"]["hits"]
        assert len(expected) == 5
        first_path.rename(held)
        server.restart()
        actual = call(
            "POST",
            "/tables/committed/query",
            {"full_text_search": {"match": "quasar", "field": "body"}, "limit": 5},
        )["hits"]["hits"]
        assert [(h["_id"], h["_source"]) for h in actual] == [
            (h["_id"], h["_source"]) for h in expected
        ]
        assert sql("SELECT count(*) FROM committed") == [["76"]]
        held.rename(first_path)
        failed = False
    finally:
        if held.exists():
            held.rename(first_path)
        server.stop(test_failed=failed)


@pytest.mark.parametrize(
    "physical,declared",
    [("float", "float32"), ("double", "float32"), ("double", "float64")],
)
def test_typed_floating_pruning_skips_pages_and_preserves_nan(
    tmp_path, physical, declared
):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "floating.parquet"
    pq.write_table(
        pa.table(
            {
                "n": pa.array(
                    [0.1, float("nan")],
                    type=pa.float32() if physical == "float" else pa.float64(),
                )
            }
        ),
        path,
        use_dictionary=False,
        compression=None,
        write_page_index=True,
    )
    payload = path.read_bytes()
    assert pq.read_table(path).num_rows == 2
    footer = len(payload) - 8 - struct.unpack("<I", payload[-8:-4])[0]
    damaged = payload[:4] + b"\xff" * (footer - 4) + payload[footer:]
    for name, data in (("valid", payload), ("unreadable", damaged)):
        objects = tmp_path / name / "buckets" / "antfly" / "objects"
        objects.mkdir(parents=True)
        (objects / "part.parquet").write_bytes(
            b"AFOBJ001"
            + struct.pack("<QI", len(data), 0)
            + hashlib.sha256(data).hexdigest().encode()
            + data
        )
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:
        for name in ("valid", "unreadable"):
            response = _request_with_idle_ddl_retry(
                server,
                "POST",
                f"/tables/floating_{name}",
                {
                    "num_shards": 1,
                    "indexes": {},
                    "schema": {
                        "storage_mode": "relational",
                        "default_type": "row",
                        "base_source": {
                            "kind": "external",
                            "table_id": f"floating-{name}",
                            "format": "parquet",
                            "uri": (tmp_path / name).as_uri(),
                        },
                        "document_schemas": {
                            "row": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "n": {
                                            "type": "number",
                                            "x-antfly-sql-type": declared,
                                        }
                                    },
                                    "additionalProperties": False,
                                }
                            }
                        },
                    },
                },
                retry_ddl=True,
            )
            assert response.ok, response.text + server.debug_logs()

        def count(name, predicate):
            return _request_with_idle_ddl_retry(
                server,
                "POST",
                "/sql",
                {
                    "statement": f"SELECT COUNT(*) FROM floating_{name} WHERE {predicate}"
                },
            )

        # Footer bounds suffice even though every data and page-index byte is corrupt.
        response = count("unreadable", "n < 0")
        assert response.ok, response.text + server.debug_logs()
        assert response.json()["rows"] == [["0"]]
        if declared == "float32":
            response = count("unreadable", "n = 0.1")
            assert response.ok, response.text + server.debug_logs()
            assert response.json()["rows"] == [["0"]]
        # NaN compares above finite values despite being absent from footer extrema.
        response = count("valid", "n > 2")
        assert response.ok, response.text + server.debug_logs()
        assert response.json()["rows"] == [["1"]]
        assert not count("unreadable", "n > 2").ok
        matching = "0.10000000149011612" if declared == "float32" else "0.1"
        assert not count("unreadable", f"n = {matching}").ok
        failed = False
    finally:
        server.stop(test_failed=failed)


@pytest.mark.parametrize("page_version", [1, 2])
@pytest.mark.parametrize("null_only", [False, True])
def test_compact_parquet_encodings_attach_query_and_restart(
    tmp_path, page_version, null_only
):
    pytest.importorskip("pyarrow")
    path = tmp_path / "encoded.parquet"
    (write_null_compact_parquet if null_only else write_compact_parquet)(
        path, page_version
    )
    payload = path.read_bytes()
    objects = tmp_path / "lake" / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    (objects / "part.parquet").write_bytes(
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
        response = _request_with_idle_ddl_retry(
            server,
            "POST",
            "/tables/compact_encodings",
            {
                "num_shards": 1,
                "indexes": {},
                "schema": {
                    "storage_mode": "relational",
                    "base_source": {
                        "kind": "external",
                        "table_id": "compact-encodings",
                        "format": "parquet",
                        "uri": (tmp_path / "lake").as_uri(),
                    },
                },
            },
            retry_ddl=True,
        )
        assert response.ok, response.text + server.debug_logs()
        expected = [
            [
                str(i - 150),
                str(i * 13 - 1000),
                None if i % 11 == 0 else i + 0.25,
                i + 0.5,
                None if i % 13 == 0 else f"prefix-{i:05d}",
                None if i % 19 == 0 else str(i),
                f"{Decimal(i - 100) / 100:.2f}",
            ]
            for i in range(288, 300)
            if i % 17 != 0
        ]
        for cold in (False, True):
            if cold:
                server.restart()
            for statement, rows in (
                (
                    "SELECT COUNT(*) FROM compact_encodings",
                    [["4" if null_only else "300"]],
                ),
                (
                    'SELECT v,lengths,prefixes FROM compact_encodings WHERE "row" = 3'
                    if null_only
                    else 'SELECT small,bsint,n,"real",label,short,exact FROM compact_encodings WHERE amount >= 288 ORDER BY amount',
                    [[None, None, None]] if null_only else expected,
                ),
            ):
                response = _request_with_idle_ddl_retry(
                    server, "POST", "/sql", {"statement": statement}
                )
                assert response.ok, response.text + server.debug_logs()
                assert response.json()["rows"] == rows
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_native_configuration_cohorts_and_group_directories_survive_restart(tmp_path):
    """Two text configs and distinct aggregate keys publish from real delta pages."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "lake"
    objects = root / "buckets" / "antfly" / "objects"
    objects.mkdir(parents=True)
    base = 9007199254740993
    expected = {k: [0, 0] for k in range(17)}
    originals = []
    for part in range(3):
        ids = list(range(part * 512, (part + 1) * 512))
        path = tmp_path / "part.parquet"
        pq.write_table(
            pa.table(
                {
                    "k": pa.array([i % 17 for i in ids], type=pa.int64()),
                    "amount": pa.array(
                        [None if i % 19 == 0 else base + i % 7 for i in ids],
                        type=pa.int64(),
                    ),
                    "body": [None if i % 13 == 0 else f"quasar item {i}" for i in ids],
                }
            ),
            path,
            compression="snappy",
            use_dictionary=False,
            data_page_version="2.0",
            row_group_size=128,
            column_encoding={
                "k": "DELTA_BINARY_PACKED",
                "amount": "DELTA_BINARY_PACKED",
                "body": "DELTA_BYTE_ARRAY",
            },
        )
        payload = path.read_bytes()
        header = (
            b"AFOBJ001"
            + struct.pack("<QI", len(payload), 0)
            + hashlib.sha256(payload).hexdigest().encode()
        )
        target = objects / f"part-{part}.parquet"
        target.write_bytes(header + payload)
        originals.append((target, header, payload))
        for i in ids:
            expected[i % 17][0] += 1
            expected[i % 17][1] += 0 if i % 19 == 0 else base + i % 7
    server = StandaloneAntflyServer(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))),
        "127.0.0.1",
        0,
    )
    failed = True
    try:

        def call(method, route, body=None, *, ddl=False):
            response = _request_with_idle_ddl_retry(
                server, method, route, body, retry_ddl=ddl
            )
            assert response.ok, response.text + server.debug_logs()
            return response.json() if response.content else None

        call(
            "POST",
            "/tables/native_cohorts",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "base_source": {
                        "kind": "external",
                        "table_id": "native-cohorts",
                        "format": "parquet",
                        "uri": root.as_uri(),
                    },
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "k": {"type": "integer"},
                                    "amount": {"type": "integer"},
                                    "body": {"type": "string"},
                                },
                                "additionalProperties": False,
                            }
                        }
                    },
                },
                "indexes": {
                    **{
                        name: {
                            "type": "full_text",
                            "field": "body",
                            "store_source": True,
                        }
                        for name in ("words", "words_again")
                    },
                    "stats": {
                        "type": "algebraic",
                        "derive_from_schema": True,
                        "aggregates": [
                            {"name": "global_rows", "op": "count"},
                            {"name": "counts", "op": "count", "group_by": ["k"]},
                            {
                                "name": "sums",
                                "op": "sum",
                                "measure": "amount",
                                "group_by": ["k"],
                            },
                        ],
                    },
                },
            },
            ddl=True,
        )
        deadline = time.monotonic() + 90
        for name in ("words", "words_again", "stats"):
            while True:
                resource = call("GET", f"/tables/native_cohorts/indexes/{name}")
                readiness = resource["status"]["readiness"]
                assert readiness["state"] != "failed", (
                    str(resource) + server.debug_logs()
                )
                if readiness["queryable"]:
                    break
                assert time.monotonic() < deadline, str(resource) + server.debug_logs()
                time.sleep(0.1)
        for name in ("words", "words_again"):
            result = call(
                "POST",
                "/tables/native_cohorts/query",
                {
                    "full_text_search": {"match": "quasar", "field": "body"},
                    "full_text_index": name,
                    "limit": 5,
                },
            )["responses"][0]
            assert len(result["hits"]["hits"]) == 5, result
        statement = (
            "SELECT k, COUNT(*), SUM(amount) FROM native_cohorts GROUP BY k ORDER BY k"
        )
        rows = [
            [str(k), str(count), str(total)]
            for k, (count, total) in sorted(expected.items())
        ]
        assert call("POST", "/sql", {"statement": statement})["rows"] == rows
        server.pause()
        for target, header, payload in originals:
            footer = len(payload) - 8 - struct.unpack("<I", payload[-8:-4])[0]
            target.write_bytes(
                header + payload[:4] + b"\xff" * (footer - 4) + payload[footer:]
            )
        for cached in server.root.rglob("lake-ranges"):
            if cached.is_dir():
                shutil.rmtree(cached)
        server.resume()
        # Source payloads cannot satisfy this query: the durable directory and
        # exact shared COUNT/SUM states must remain sufficient after restart.
        assert call("POST", "/sql", {"statement": statement})["rows"] == rows
        assert call(
            "POST", "/sql", {"statement": "SELECT COUNT(*) FROM native_cohorts"}
        )["rows"] == [["1536"]]
        failed = False
    finally:
        server.stop(test_failed=failed)
