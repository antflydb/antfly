# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Database.sql_json: statements run against the embedded table, and a failed
statement raises an AntflyError whose `body` carries the SQL diagnostics.
Mirrors rs/crates/embedded/tests/sql.rs.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import antfly_embedded
from antfly_embedded import errors

pytestmark = pytest.mark.usefixtures("require_native")

DOCUMENT_SCHEMA = {
    "version": 1,
    "default_type": "row",
    "document_schemas": {
        "row": {
            "schema": {
                "type": "object",
                "properties": {"n": {"type": "integer"}},
                "additionalProperties": True,
            }
        }
    },
}

ENFORCED_SCHEMA = {
    "version": 1,
    "enforce_types": True,
    "default_type": "row",
    "document_schemas": {
        "row": {
            "schema": {
                "type": "object",
                "properties": {"n": {"type": "integer", "minimum": 0}},
                "required": ["n"],
                "additionalProperties": True,
            }
        }
    },
}


def test_sql_json_reads_and_mutates_documents(tmp_path: Path) -> None:
    with antfly_embedded.create(tmp_path / "rw.aflite", no_sync=True) as db:
        db.set_schema(DOCUMENT_SCHEMA)
        db.batch_json({"inserts": {"a": {"n": 1, "extra": True}}})

        selected = db.sql_json("items", {"statement": "SELECT n FROM items WHERE _id='a'"})
        assert selected["rows"] == [["1"]]

        db.sql_json("items", {"statement": "INSERT INTO items (_id,n) VALUES ('b',2) RETURNING n"})
        db.sql_json("items", {"statement": "UPDATE items SET n=n+10 WHERE _id='a' RETURNING n"})

        assert db.lookup("a") == {"n": 11, "extra": True}
        assert db.lookup("b") == {"n": 2}
        count = db.sql_json("items", {"statement": "SELECT COUNT(*) FROM items"}, raw=True)
        assert isinstance(count, bytes)
        assert b'"2"' in count


def test_sql_json_failures_carry_diagnostics(tmp_path: Path) -> None:
    db = antfly_embedded.create(tmp_path / "err.aflite", no_sync=True)
    try:
        db.set_schema(ENFORCED_SCHEMA)

        with pytest.raises(errors.AntflyError) as rejected:
            db.sql_json(
                "items",
                {"statement": "INSERT INTO items (_id,n) VALUES ('valid',1),('invalid',-1)"},
            )
        assert rejected.value.body, "rejected statement should carry SQL diagnostics"
        assert rejected.value.body in str(rejected.value)
        count = db.sql_json("items", {"statement": "SELECT COUNT(*) FROM items"})
        assert count["rows"] == [["0"]], "the whole statement is rejected"

        with pytest.raises(errors.AntflyError) as ddl:
            db.sql_json("items", {"statement": "CREATE TABLE other (id INT)"})
        assert not isinstance(ddl.value, errors.NotFoundError)
    finally:
        db.close()

    with pytest.raises(errors.AntflyError) as closed:
        db.sql_json("items", {"statement": "SELECT 1"})
    assert closed.value.body == ""
