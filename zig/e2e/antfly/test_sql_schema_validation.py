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

"""Production runtime archives must share native schema publication authority."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests
from conftest import (
    DEFAULT_ANTFLY_BIN,
    REPO_ROOT,
    StandaloneAntflyServer,
    resolve_binary_path,
)


def test_native_sql_validation_preserves_uuid_rows_through_failure_and_restart():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    assert Path(binary).is_file(), f"Build the production Antfly binary first: {binary}"
    original = next(
        entry["sql"]
        for entry in json.loads(
            (
                REPO_ROOT
                / "pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
            ).read_text()
        )["entries"]
        if entry["id"] == "sql-0744"
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:

        def sql(statement, expected_error=None):
            response = requests.post(
                server.api_url + "/sql", json={"statement": statement}, timeout=30
            )
            result = response.json()
            if response.status_code >= 400:
                assert expected_error is not None, (
                    statement,
                    response.status_code,
                    result,
                    server.debug_logs(),
                )
                assert result.get("code") == expected_error, (
                    result,
                    server.debug_logs(),
                )
                return None
            token = result.get("ddl_receipt", {}).get("schema_validation_token")
            if token:
                deadline = time.monotonic() + 45
                while True:
                    observed = requests.get(
                        server.api_url + "/tables/usage_records/schema/publication",
                        headers={"X-Antfly-Schema-Validation-Token": token},
                        timeout=10,
                    )
                    assert observed.status_code == 200, (
                        observed.status_code,
                        observed.text,
                        server.debug_logs(),
                    )
                    progress = observed.json()
                    assert progress["schema_validation_token"] == token
                    if progress["state"] == "succeeded":
                        assert (
                            progress["owner_count"]
                            == progress["ready"]
                            == progress["installed"]
                            == progress["released"]
                        ), progress
                        break
                    if progress["state"] == "failed":
                        assert (
                            expected_error == "23502"
                            and progress["failure"] == "not_null_violation"
                        ), progress
                        return None
                    assert progress["state"] in ("pending", "admission_unknown"), (
                        progress
                    )
                    assert time.monotonic() < deadline, (progress, server.debug_logs())
                    time.sleep(0.05)
            assert expected_error is None, (statement, expected_error, result)
            return result

        sql(
            "CREATE TABLE usage_records(id UUID PRIMARY KEY DEFAULT uuidv7(),status TEXT DEFAULT 'open',status_upper TEXT GENERATED ALWAYS AS (upper(status)) STORED,amount NUMERIC CHECK(amount >= 0))"
        )
        sql(
            "INSERT INTO usage_records(id,status,amount) VALUES ('00000000-0000-0000-0000-000000000001',NULL,1),('00000000-0000-0000-0000-000000000002','open',2)"
        )
        read = "SELECT id::text,status,status_upper,amount::text FROM usage_records ORDER BY id"
        before = sql(read)["rows"]
        sql(original, "23502")
        assert sql(read)["rows"] == before
        sql(
            "UPDATE usage_records SET status='ready' WHERE id='00000000-0000-0000-0000-000000000001'"
        )
        repaired = sql(read)["rows"]
        first = sql(original)
        assert sql(read)["rows"] == repaired

        def schema_version():
            response = requests.get(
                server.api_url + "/tables/usage_records", timeout=10
            )
            assert response.status_code == 200, (response.status_code, response.text)
            return response.json()["schema"]["version"]

        committed_version = schema_version()
        assert committed_version == first["ddl_receipt"]["schema_version"]
        again = sql(original)
        assert again["command_tag"] == "ALTER TABLE"
        # A no-op need not create a new recovery receipt. Inspect the actual
        # public catalog epoch rather than assuming another job was admitted.
        assert schema_version() == committed_version
        # The generated UUID is materialized once and survives schema publication
        # and cold reopen; defaults/generated/CHECK/PK enforcement all remain live.
        inserted = sql(
            "INSERT INTO usage_records(amount) VALUES(3) RETURNING id::text,status,status_upper"
        )["rows"][0]
        assert inserted[0][14] == "7" and inserted[1:] == ["open", "OPEN"]
        sql("INSERT INTO usage_records(amount) VALUES(4)")
        sql("INSERT INTO usage_records(status,amount) VALUES(NULL,3)", "23502")
        sql("INSERT INTO usage_records(amount) VALUES(-1)", "23514")
        sql(
            "INSERT INTO usage_records(id) VALUES('00000000-0000-0000-0000-000000000001')",
            "23505",
        )
        committed = sql(read)["rows"]
        for _ in range(2):
            server.restart()
            assert sql(read)["rows"] == committed
            sql("INSERT INTO usage_records(status) VALUES(NULL)", "23502")
        sql("ALTER TABLE usage_records ALTER COLUMN status DROP NOT NULL")
        sql(
            "UPDATE usage_records SET status=NULL WHERE id='00000000-0000-0000-0000-000000000002'"
        )
        nullable = sql(read)["rows"]
        sql(original, "23502")
        assert sql(read)["rows"] == nullable
        failed = False
    finally:
        server.stop(test_failed=failed)
