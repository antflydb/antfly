# Copyright 2026 Antfly, Inc.
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

from __future__ import annotations

import os
import subprocess
import time
from typing import Any

import pytest
import requests

from helpers import wait_until

pytestmark = pytest.mark.postgres_integration

DEFAULT_PG_DSN = "postgres://localhost:5432/postgres?sslmode=disable"
PSQL_BIN = os.environ.get("ANTFLY_TEST_PSQL_BIN", "/opt/homebrew/opt/postgresql@18/bin/psql")


def _pg_dsn() -> str:
    return os.environ.get("ANTFLY_TEST_PG_DSN") or os.environ.get("PG_DSN") or DEFAULT_PG_DSN


def _pg_available() -> bool:
    try:
        subprocess.run(
            [PSQL_BIN, _pg_dsn(), "-tAc", "select 1"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return False
    return True


def _run_psql(sql: str) -> None:
    subprocess.run(
        [PSQL_BIN, _pg_dsn(), "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )


@pytest.fixture(scope="module")
def pg_customers_table():
    if not _pg_available():
        pytest.skip("local PostgreSQL is unavailable for foreign-source E2E")

    table_name = f"antfly_e2e_pg_customers_{time.time_ns()}"
    _run_psql(
        f"""
        create table {table_name} (
            customer_id text primary key,
            name text not null,
            email text not null,
            tier text not null,
            lifetime_value bigint not null,
            address_id text not null
        );
        insert into {table_name} (customer_id, name, email, tier, lifetime_value, address_id) values
            ('cust-1', 'Alice', 'alice@example.com', 'gold', 1200, 'addr-1'),
            ('cust-2', 'Bob', 'bob@example.com', 'silver', 400, 'addr-2'),
            ('cust-3', 'Charlie', 'charlie@example.com', 'gold', 950, 'addr-3'),
            ('cust-4', 'Diana', 'diana@example.com', 'bronze', 150, 'addr-2'),
            ('cust-5', 'Eve', 'eve@example.com', 'silver', 700, 'addr-1');
        """
    )
    try:
        yield table_name
    finally:
        _run_psql(f"drop table if exists {table_name};")


@pytest.fixture(scope="module")
def pg_addresses_table():
    if not _pg_available():
        pytest.skip("local PostgreSQL is unavailable for foreign-source E2E")

    table_name = f"antfly_e2e_pg_addresses_{time.time_ns()}"
    _run_psql(
        f"""
        create table {table_name} (
            id text primary key,
            city text not null,
            region text not null
        );
        insert into {table_name} (id, city, region) values
            ('addr-1', 'Seattle', 'wa'),
            ('addr-2', 'Portland', 'or'),
            ('addr-3', 'San Francisco', 'ca');
        """
    )
    try:
        yield table_name
    finally:
        _run_psql(f"drop table if exists {table_name};")


@pytest.fixture(scope="module")
def pg_orders_table():
    if not _pg_available():
        pytest.skip("local PostgreSQL is unavailable for foreign-source E2E")

    table_name = f"antfly_e2e_pg_orders_{time.time_ns()}"
    _run_psql(
        f"""
        create table {table_name} (
            order_id text primary key,
            customer_id text not null,
            product text not null
        );
        insert into {table_name} (order_id, customer_id, product) values
            ('order-1', 'cust-1', 'Widget A'),
            ('order-2', 'cust-3', 'Widget B');
        """
    )
    try:
        yield table_name
    finally:
        _run_psql(f"drop table if exists {table_name};")


def _foreign_source(table_name: str) -> dict[str, Any]:
    return {
        "type": "postgres",
        "dsn": _pg_dsn(),
        "postgres_table": table_name,
        "columns": [
            {"name": "customer_id", "type": "text"},
            {"name": "name", "type": "text"},
            {"name": "email", "type": "text"},
            {"name": "tier", "type": "text"},
            {"name": "lifetime_value", "type": "bigint"},
            {"name": "address_id", "type": "text"},
        ],
    }


def _address_foreign_source(table_name: str) -> dict[str, Any]:
    return {
        "type": "postgres",
        "dsn": _pg_dsn(),
        "postgres_table": table_name,
        "columns": [
            {"name": "id", "type": "text"},
            {"name": "city", "type": "text"},
            {"name": "region", "type": "text"},
        ],
    }


def _order_foreign_source(table_name: str) -> dict[str, Any]:
    return {
        "type": "postgres",
        "dsn": _pg_dsn(),
        "postgres_table": table_name,
        "columns": [
            {"name": "order_id", "type": "text"},
            {"name": "customer_id", "type": "text"},
            {"name": "product", "type": "text"},
        ],
    }


def _query_responses(result: dict[str, Any]) -> list[dict[str, Any]]:
    if "responses" in result:
        return result.get("responses", [])
    return [result]


def _first_response(result: dict[str, Any]) -> dict[str, Any]:
    responses = _query_responses(result)
    assert responses
    return responses[0]


def _result_hits(result: dict[str, Any]) -> list[dict[str, Any]]:
    response = _first_response(result)
    hits = response.get("hits", {})
    return hits.get("hits", [])


def _result_total(result: dict[str, Any]) -> int:
    response = _first_response(result)
    hits = response.get("hits", {})
    total = hits.get("total", 0)
    if isinstance(total, dict):
        return int(total.get("value", 0))
    return int(total)


def _result_status(result: dict[str, Any]) -> int:
    return int(_first_response(result).get("status", 200))


def _wait_for_stateful_lookup(table_api, table_name: str, key: str, *, timeout_s: float, interval_s: float) -> dict[str, Any] | None:
    if table_api.backend != "stateful":
        return {}
    lookup = getattr(table_api.raw, "lookup_key", None)
    if lookup is None:
        return {}
    return wait_until(
        lambda: _lookup_stateful_key_if_visible(lookup, table_name, key),
        timeout_s=timeout_s,
        interval_s=interval_s,
    )


def _lookup_stateful_key_if_visible(lookup, table_name: str, key: str) -> dict[str, Any] | None:
    try:
        return lookup(table_name, key)
    except requests.RequestException:
        return None


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_basic_query(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    assert _result_total(result) == 5
    names = {hit.get("_source", {}).get("name") for hit in _result_hits(result)}
    assert "Alice" in names
    assert "Eve" in names


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_filtered_query(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "filter_query": {
                "term": "gold",
                "field": "tier",
            },
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    assert _result_total(result) == 2
    names = {hit.get("_source", {}).get("name") for hit in _result_hits(result)}
    assert names == {"Alice", "Charlie"}


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_range_conjunct_query(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "filter_query": {
                "conjuncts": [
                    {
                        "field": "lifetime_value",
                        "min": 700,
                    },
                    {
                        "term": "gold",
                        "field": "tier",
                    },
                ]
            },
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    assert _result_total(result) == 2
    names = {hit.get("_source", {}).get("name") for hit in _result_hits(result)}
    assert names == {"Alice", "Charlie"}


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_prefix_query(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "filter_query": {
                "prefix": "Ali",
                "field": "name",
            },
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    assert _result_total(result) == 1
    hits = _result_hits(result)
    assert len(hits) == 1
    assert hits[0].get("_source", {}).get("name") == "Alice"


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_query_string_or_query(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "filter_query": {
                "query": "name:Alice OR name:Charlie",
            },
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    assert _result_total(result) == 2
    names = {hit.get("_source", {}).get("name") for hit in _result_hits(result)}
    assert names == {"Alice", "Charlie"}


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_filter_prefix_query(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "filter_prefix": "cust-1",
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    assert _result_total(result) == 1
    hits = _result_hits(result)
    assert len(hits) == 1
    assert hits[0].get("_id") == "cust-1"
    assert hits[0].get("_source", {}).get("name") == "Alice"


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_ordered_paginated_query(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 2,
            "offset": 1,
            "fields": ["customer_id", "name", "lifetime_value"],
            "order_by": [
                {
                    "field": "lifetime_value",
                    "desc": True,
                }
            ],
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    hits = _result_hits(result)
    assert len(hits) == 2
    ordered_names = [hit.get("_source", {}).get("name") for hit in hits]
    assert ordered_names == ["Charlie", "Eve"]
    ordered_values = [hit.get("_source", {}).get("lifetime_value") for hit in hits]
    assert ordered_values == [950, 700]


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_rejects_unsupported_aggregation(table_api, pg_customers_table):
    try:
        result = table_api.query_table(
            "pg_customers",
            {
                "limit": 10,
                "aggregations": {
                    "geo": {
                        "type": "geohash_grid",
                        "field": "tier",
                    }
                },
                "foreign_sources": {
                    "pg_customers": _foreign_source(pg_customers_table),
                },
            },
        )
    except requests.HTTPError as exc:
        assert exc.response is not None
        assert exc.response.status_code == 400
    else:
        assert _result_status(result) == 400


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_aggregations(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "aggregations": {
                "value_stats": {
                    "type": "stats",
                    "field": "lifetime_value",
                },
                "tier_terms": {
                    "type": "terms",
                    "field": "tier",
                    "size": 5,
                },
            },
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    response = _first_response(result)
    assert _result_status(result) == 200
    aggregations = response.get("aggregations") or {}
    assert aggregations

    value_stats = aggregations.get("value_stats") or {}
    assert value_stats.get("count") == 5
    assert value_stats.get("min") == 150
    assert value_stats.get("max") == 1200
    assert value_stats.get("sum") == 3400
    assert value_stats.get("avg") == 680

    tier_terms = aggregations.get("tier_terms") or {}
    buckets = tier_terms.get("buckets") or []
    counts = {bucket.get("key"): bucket.get("doc_count") for bucket in buckets}
    assert counts == {
        "gold": 2,
        "silver": 2,
        "bronze": 1,
    }


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_simple_aggregations(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "aggregations": {
                "doc_count": {
                    "type": "count",
                    "field": "customer_id",
                },
                "value_sum": {
                    "type": "sum",
                    "field": "lifetime_value",
                },
                "value_avg": {
                    "type": "avg",
                    "field": "lifetime_value",
                },
                "value_min": {
                    "type": "min",
                    "field": "lifetime_value",
                },
                "value_max": {
                    "type": "max",
                    "field": "lifetime_value",
                },
            },
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    assert _result_status(result) == 200
    response = _first_response(result)
    aggregations = response.get("aggregations") or {}
    assert aggregations
    assert (aggregations.get("doc_count") or {}).get("value") == 5
    assert (aggregations.get("value_sum") or {}).get("value") == 3400
    assert (aggregations.get("value_avg") or {}).get("value") == 680
    assert (aggregations.get("value_min") or {}).get("value") == 150
    assert (aggregations.get("value_max") or {}).get("value") == 1200


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_filtered_aggregations(table_api, pg_customers_table):
    result = table_api.query_table(
        "pg_customers",
        {
            "limit": 10,
            "filter_query": {
                "term": "gold",
                "field": "tier",
            },
            "aggregations": {
                "value_stats": {
                    "type": "stats",
                    "field": "lifetime_value",
                },
                "tier_terms": {
                    "type": "terms",
                    "field": "tier",
                    "size": 5,
                },
            },
            "foreign_sources": {
                "pg_customers": _foreign_source(pg_customers_table),
            },
        },
    )

    response = _first_response(result)
    assert _result_status(result) == 200
    assert _result_total(result) == 2

    aggregations = response.get("aggregations") or {}
    value_stats = aggregations.get("value_stats") or {}
    assert value_stats.get("count") == 2
    assert value_stats.get("min") == 950
    assert value_stats.get("max") == 1200
    assert value_stats.get("sum") == 2150
    assert value_stats.get("avg") == 1075

    tier_terms = aggregations.get("tier_terms") or {}
    buckets = tier_terms.get("buckets") or []
    assert len(buckets) == 1
    assert buckets[0].get("key") == "gold"
    assert buckets[0].get("doc_count") == 2


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_join_with_antfly(table_api, pg_customers_table):
    table_name = f"foreign_orders_{table_api.backend}_{time.time_ns()}"
    created = table_api.create_table(table_name)
    assert (created.get("name") or created.get("table_name")) == table_name

    batch = table_api.batch_write(
        table_name,
        inserts={
            "order-001": {
                "customer_id": "cust-1",
                "product": "Widget A",
                "amount": 29.99,
                "body": "order widget a",
            },
            "order-002": {
                "customer_id": "cust-3",
                "product": "Widget B",
                "amount": 49.99,
                "body": "order widget b",
            },
            "order-003": {
                "customer_id": "cust-1",
                "product": "Widget C",
                "amount": 19.99,
                "body": "order widget c",
            },
        },
        sync_level="full_text" if table_api.backend == "stateful" else "write",
    )
    assert batch["inserted"] == 3

    if table_api.backend == "serverless":
        assert table_api.publish_table(table_name) is not None

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                table_name,
                    {
                        "limit": 10,
                        "fields": ["customer_id", "product"],
                        "full_text_search": {
                            "query": "body:order",
                        },
                    "join": {
                        "right_table": "pg_customers",
                        "join_type": "left",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "customer_id",
                            "operator": "eq",
                        },
                        "right_fields": ["name", "email", "tier"],
                    },
                    "foreign_sources": {
                        "pg_customers": _foreign_source(pg_customers_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 3:
            return None
        sources = [hit.get("_source", {}) for hit in hits]
        if not any(source.get("pg_customers.name") == "Alice" for source in sources):
            return None
        if not any(source.get("pg_customers.name") == "Charlie" for source in sources):
            return None
        return result

    result = wait_until(joined_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    by_customer = {
        hit.get("_source", {}).get("customer_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_customer["cust-1"]["pg_customers.name"] == "Alice"
    assert by_customer["cust-1"]["pg_customers.tier"] == "gold"
    assert by_customer["cust-3"]["pg_customers.name"] == "Charlie"
    assert by_customer["cust-3"]["pg_customers.tier"] == "gold"


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_direct_foreign_table_join_with_antfly(table_api, pg_customers_table):
    table_name = f"foreign_customer_notes_{table_api.backend}_{time.time_ns()}"
    note_field = f"{table_name}.note"

    created = table_api.create_table(table_name)
    assert (created.get("name") or created.get("table_name")) == table_name

    batch = table_api.batch_write(
        table_name,
        inserts={
            "cust-1": {
                "note": "priority",
                "body": "priority customer note",
            },
            "cust-3": {
                "note": "vip",
                "body": "vip customer note",
            },
        },
        sync_level="full_text" if table_api.backend == "stateful" else "write",
    )
    assert batch["inserted"] == 2
    assert _wait_for_stateful_lookup(table_api, table_name, "cust-1", timeout_s=30.0, interval_s=0.25) is not None
    assert _wait_for_stateful_lookup(table_api, table_name, "cust-3", timeout_s=30.0, interval_s=0.25) is not None

    if table_api.backend == "serverless":
        assert table_api.publish_table(table_name) is not None

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                "pg_customers",
                {
                    "limit": 10,
                    "fields": ["customer_id", "name", "tier"],
                    "join": {
                        "right_table": table_name,
                        "join_type": "left",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "_id",
                            "operator": "eq",
                        },
                        "right_fields": ["note"],
                    },
                    "foreign_sources": {
                        "pg_customers": _foreign_source(pg_customers_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 5:
            return None
        sources = [hit.get("_source", {}) for hit in hits]
        if not any(source.get(note_field) == "priority" for source in sources):
            return None
        if not any(source.get(note_field) == "vip" for source in sources):
            return None
        return result

    result = wait_until(joined_result, timeout_s=60.0, interval_s=0.25)
    assert result is not None

    by_customer = {
        hit.get("_source", {}).get("customer_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_customer["cust-1"]["name"] == "Alice"
    assert by_customer["cust-1"][note_field] == "priority"
    assert by_customer["cust-3"]["name"] == "Charlie"
    assert by_customer["cust-3"][note_field] == "vip"
    assert note_field not in by_customer["cust-2"]


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_direct_foreign_table_right_join_with_antfly(table_api, pg_customers_table):
    table_name = f"foreign_customer_notes_right_{table_api.backend}_{time.time_ns()}"
    note_field = f"{table_name}.note"

    created = table_api.create_table(table_name)
    assert (created.get("name") or created.get("table_name")) == table_name

    batch = table_api.batch_write(
        table_name,
        inserts={
            "cust-1": {
                "note": "priority",
                "body": "priority customer note",
            },
            "cust-999": {
                "note": "unmatched",
                "body": "unmatched customer note",
            },
        },
        sync_level="full_text" if table_api.backend == "stateful" else "write",
    )
    assert batch["inserted"] == 2
    assert _wait_for_stateful_lookup(table_api, table_name, "cust-1", timeout_s=30.0, interval_s=0.25) is not None
    assert _wait_for_stateful_lookup(table_api, table_name, "cust-999", timeout_s=30.0, interval_s=0.25) is not None

    if table_api.backend == "serverless":
        assert table_api.publish_table(table_name) is not None

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                "pg_customers",
                {
                    "limit": 10,
                    "fields": ["customer_id", "name", "tier"],
                    "join": {
                        "right_table": table_name,
                        "join_type": "right",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "_id",
                            "operator": "eq",
                        },
                        "right_fields": ["note"],
                    },
                    "foreign_sources": {
                        "pg_customers": _foreign_source(pg_customers_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 2:
            return None
        sources = [hit.get("_source", {}) for hit in hits]
        if not any(source.get(note_field) == "priority" for source in sources):
            return None
        if not any(source.get(note_field) == "unmatched" for source in sources):
            return None
        return result

    result = wait_until(joined_result, timeout_s=60.0, interval_s=0.25)
    assert result is not None

    rows = [hit.get("_source", {}) for hit in _result_hits(result)]
    matched = next(row for row in rows if row.get(note_field) == "priority")
    unmatched = next(row for row in rows if row.get(note_field) == "unmatched")

    assert matched["customer_id"] == "cust-1"
    assert matched["name"] == "Alice"
    assert matched[note_field] == "priority"
    assert unmatched[note_field] == "unmatched"
    assert unmatched.get("customer_id") in (None, "")
    assert unmatched.get("name") in (None, "")


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_direct_foreign_table_join_with_foreign_rhs(table_api, pg_customers_table, pg_addresses_table):
    city_field = "pg_addresses.city"
    region_field = "pg_addresses.region"

    result = wait_until(
        lambda: _direct_foreign_joined_addresses_result(table_api, pg_customers_table, pg_addresses_table),
        timeout_s=30.0,
        interval_s=0.25,
    )
    assert result is not None

    by_customer = {
        hit.get("_source", {}).get("customer_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_customer["cust-1"]["name"] == "Alice"
    assert by_customer["cust-1"][city_field] == "Seattle"
    assert by_customer["cust-1"][region_field] == "wa"
    assert by_customer["cust-2"][city_field] == "Portland"
    assert by_customer["cust-2"][region_field] == "or"
    assert by_customer["cust-3"][city_field] == "San Francisco"
    assert by_customer["cust-3"][region_field] == "ca"


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_nested_foreign_leaf_join_with_antfly(table_api, pg_addresses_table):
    customers_table = f"foreign_join_customers_{table_api.backend}_{time.time_ns()}"
    docs_table = f"foreign_join_docs_{table_api.backend}_{time.time_ns()}"
    customer_name_field = f"{customers_table}.name"
    customer_city_field = f"{customers_table}.pg_addresses.city"
    customer_region_field = f"{customers_table}.pg_addresses.region"

    created_customers = table_api.create_table(customers_table)
    assert (created_customers.get("name") or created_customers.get("table_name")) == customers_table
    created_docs = table_api.create_table(docs_table)
    assert (created_docs.get("name") or created_docs.get("table_name")) == docs_table

    customer_batch = table_api.batch_write(
        customers_table,
        inserts={
            "cust-1": {"name": "Alice", "address_id": "addr-1"},
            "cust-2": {"name": "Bob", "address_id": "addr-2"},
            "cust-3": {"name": "Charlie", "address_id": "addr-3"},
        },
        sync_level="full_text" if table_api.backend == "stateful" else "write",
    )
    assert customer_batch["inserted"] == 3

    docs_batch = table_api.batch_write(
        docs_table,
        inserts={
            "doc-001": {
                "customer_id": "cust-1",
                "title": "Order A",
                "body": "customer order a",
            },
            "doc-002": {
                "customer_id": "cust-3",
                "title": "Order B",
                "body": "customer order b",
            },
        },
        sync_level="full_text" if table_api.backend == "stateful" else "write",
    )
    assert docs_batch["inserted"] == 2

    if table_api.backend == "serverless":
        assert table_api.publish_table(customers_table) is not None
        assert table_api.publish_table(docs_table) is not None

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                docs_table,
                {
                    "limit": 10,
                    "fields": ["customer_id", "title"],
                    "full_text_search": {
                        "query": "body:customer",
                    },
                    "join": {
                        "right_table": customers_table,
                        "join_type": "left",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "_id",
                            "operator": "eq",
                        },
                        "right_fields": ["name", "pg_addresses.city", "pg_addresses.region"],
                            "nested_join": {
                                "right_table": "pg_addresses",
                                "join_type": "left",
                                "on": {
                                    "left_field": "address_id",
                                    "right_field": "id",
                                    "operator": "eq",
                                },
                                "right_fields": ["city", "region"],
                            },
                    },
                    "foreign_sources": {
                        "pg_addresses": _address_foreign_source(pg_addresses_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 2:
            return None
        sources = [hit.get("_source", {}) for hit in hits]
        if not any(source.get(customer_city_field) == "Seattle" for source in sources):
            return None
        if not any(source.get(customer_city_field) == "San Francisco" for source in sources):
            return None
        return result

    result = wait_until(joined_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    by_customer = {
        hit.get("_source", {}).get("customer_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_customer["cust-1"][customer_name_field] == "Alice"
    assert by_customer["cust-1"][customer_city_field] == "Seattle"
    assert by_customer["cust-1"][customer_region_field] == "wa"
    assert by_customer["cust-3"][customer_name_field] == "Charlie"
    assert by_customer["cust-3"][customer_city_field] == "San Francisco"
    assert by_customer["cust-3"][customer_region_field] == "ca"


def _direct_foreign_joined_addresses_result(
    table_api,
    pg_customers_table: str,
    pg_addresses_table: str,
) -> dict[str, Any] | None:
    try:
        result = table_api.query_table(
            "pg_customers",
            {
                "limit": 10,
                "fields": ["customer_id", "name", "address_id"],
                "join": {
                    "right_table": "pg_addresses",
                    "join_type": "left",
                    "on": {
                        "left_field": "address_id",
                        "right_field": "id",
                        "operator": "eq",
                    },
                    "right_fields": ["city", "region"],
                },
                "foreign_sources": {
                    "pg_customers": _foreign_source(pg_customers_table),
                    "pg_addresses": _address_foreign_source(pg_addresses_table),
                },
            },
        )
    except requests.RequestException:
        return None
    hits = _result_hits(result)
    if len(hits) < 5:
        return None
    sources = [hit.get("_source", {}) for hit in hits]
    if not any(source.get("pg_addresses.city") == "Seattle" for source in sources):
        return None
    if not any(source.get("pg_addresses.city") == "San Francisco" for source in sources):
        return None
    return result


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_foreign_table_nested_foreign_leaf_join(table_api, pg_customers_table, pg_addresses_table):
    table_name = f"foreign_orders_nested_{table_api.backend}_{time.time_ns()}"
    customer_city_field = "pg_customers.pg_addresses.city"
    customer_region_field = "pg_customers.pg_addresses.region"

    created = table_api.create_table(table_name)
    assert (created.get("name") or created.get("table_name")) == table_name

    batch = table_api.batch_write(
        table_name,
        inserts={
            "order-001": {
                "customer_id": "cust-1",
                "product": "Widget A",
                "body": "order widget a",
            },
            "order-002": {
                "customer_id": "cust-3",
                "product": "Widget B",
                "body": "order widget b",
            },
        },
        sync_level="full_text" if table_api.backend == "stateful" else "write",
    )
    assert batch["inserted"] == 2

    if table_api.backend == "serverless":
        assert table_api.publish_table(table_name) is not None

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                table_name,
                {
                    "limit": 10,
                    "fields": ["customer_id", "product"],
                    "full_text_search": {
                        "query": "body:order",
                    },
                    "join": {
                        "right_table": "pg_customers",
                        "join_type": "left",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "customer_id",
                            "operator": "eq",
                        },
                        "right_fields": ["name", "tier", "pg_addresses.city", "pg_addresses.region"],
                        "nested_join": {
                            "right_table": "pg_addresses",
                            "join_type": "left",
                            "on": {
                                "left_field": "address_id",
                                "right_field": "id",
                                "operator": "eq",
                            },
                            "right_fields": ["city", "region"],
                        },
                    },
                    "foreign_sources": {
                        "pg_customers": _foreign_source(pg_customers_table),
                        "pg_addresses": _address_foreign_source(pg_addresses_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 2:
            return None
        sources = [hit.get("_source", {}) for hit in hits]
        if not any(source.get(customer_city_field) == "Seattle" for source in sources):
            return None
        if not any(source.get(customer_city_field) == "San Francisco" for source in sources):
            return None
        return result

    result = wait_until(joined_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    by_customer = {
        hit.get("_source", {}).get("customer_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_customer["cust-1"]["pg_customers.name"] == "Alice"
    assert by_customer["cust-1"][customer_city_field] == "Seattle"
    assert by_customer["cust-1"][customer_region_field] == "wa"
    assert by_customer["cust-3"]["pg_customers.name"] == "Charlie"
    assert by_customer["cust-3"][customer_city_field] == "San Francisco"
    assert by_customer["cust-3"][customer_region_field] == "ca"


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_direct_foreign_table_nested_foreign_leaf_join(
    table_api,
    pg_orders_table,
    pg_customers_table,
    pg_addresses_table,
):
    customer_name_field = "pg_customers.name"
    customer_city_field = "pg_customers.pg_addresses.city"
    customer_region_field = "pg_customers.pg_addresses.region"

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                "pg_orders",
                {
                    "limit": 10,
                    "fields": ["order_id", "customer_id", "product"],
                    "join": {
                        "right_table": "pg_customers",
                        "join_type": "left",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "customer_id",
                            "operator": "eq",
                        },
                        "right_fields": ["name", "pg_addresses.city", "pg_addresses.region"],
                        "nested_join": {
                            "right_table": "pg_addresses",
                            "join_type": "left",
                            "on": {
                                "left_field": "address_id",
                                "right_field": "id",
                                "operator": "eq",
                            },
                            "right_fields": ["city", "region"],
                        },
                    },
                    "foreign_sources": {
                        "pg_orders": _order_foreign_source(pg_orders_table),
                        "pg_customers": _foreign_source(pg_customers_table),
                        "pg_addresses": _address_foreign_source(pg_addresses_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 2:
            return None
        sources = [hit.get("_source", {}) for hit in hits]
        if not any(source.get(customer_city_field) == "Seattle" for source in sources):
            return None
        if not any(source.get(customer_city_field) == "San Francisco" for source in sources):
            return None
        return result

    result = wait_until(joined_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    by_order = {
        hit.get("_source", {}).get("order_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_order["order-1"][customer_name_field] == "Alice"
    assert by_order["order-1"][customer_city_field] == "Seattle"
    assert by_order["order-1"][customer_region_field] == "wa"
    assert by_order["order-2"][customer_name_field] == "Charlie"
    assert by_order["order-2"][customer_city_field] == "San Francisco"
    assert by_order["order-2"][customer_region_field] == "ca"


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_direct_foreign_table_join_with_right_filters(table_api, pg_orders_table, pg_customers_table):
    customer_name_field = "pg_customers.name"
    customer_tier_field = "pg_customers.tier"

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                "pg_orders",
                {
                    "limit": 10,
                    "fields": ["order_id", "customer_id", "product"],
                    "join": {
                        "right_table": "pg_customers",
                        "join_type": "left",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "customer_id",
                            "operator": "eq",
                        },
                        "right_fields": ["name", "tier"],
                        "right_filters": {
                            "filter_query": {
                                "query": "name:Alice",
                            }
                        },
                    },
                    "foreign_sources": {
                        "pg_orders": _order_foreign_source(pg_orders_table),
                        "pg_customers": _foreign_source(pg_customers_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 2:
            return None
        by_order = {
            hit.get("_source", {}).get("order_id"): hit.get("_source", {})
            for hit in hits
        }
        if by_order.get("order-1", {}).get(customer_name_field) != "Alice":
            return None
        if "order-2" not in by_order:
            return None
        return result

    result = wait_until(joined_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    by_order = {
        hit.get("_source", {}).get("order_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_order["order-1"][customer_name_field] == "Alice"
    assert by_order["order-1"][customer_tier_field] == "gold"
    assert customer_name_field not in by_order["order-2"]
    assert customer_tier_field not in by_order["order-2"]


@pytest.mark.parametrize("table_api", ["stateful", "serverless"], indirect=True)
def test_direct_foreign_table_join_with_right_filter_prefix(table_api, pg_orders_table, pg_customers_table):
    customer_name_field = "pg_customers.name"

    def joined_result() -> dict[str, Any] | None:
        try:
            result = table_api.query_table(
                "pg_orders",
                {
                    "limit": 10,
                    "fields": ["order_id", "customer_id", "product"],
                    "join": {
                        "right_table": "pg_customers",
                        "join_type": "left",
                        "on": {
                            "left_field": "customer_id",
                            "right_field": "customer_id",
                            "operator": "eq",
                        },
                        "right_fields": ["name"],
                        "right_filters": {
                            "filter_prefix": "cust-1",
                        },
                    },
                    "foreign_sources": {
                        "pg_orders": _order_foreign_source(pg_orders_table),
                        "pg_customers": _foreign_source(pg_customers_table),
                    },
                },
            )
        except requests.RequestException:
            return None
        hits = _result_hits(result)
        if len(hits) < 2:
            return None
        by_order = {
            hit.get("_source", {}).get("order_id"): hit.get("_source", {})
            for hit in hits
        }
        if by_order.get("order-1", {}).get(customer_name_field) != "Alice":
            return None
        if "order-2" not in by_order:
            return None
        return result

    result = wait_until(joined_result, timeout_s=30.0, interval_s=0.25)
    assert result is not None

    by_order = {
        hit.get("_source", {}).get("order_id"): hit.get("_source", {})
        for hit in _result_hits(result)
    }
    assert by_order["order-1"][customer_name_field] == "Alice"
    assert customer_name_field not in by_order["order-2"]
