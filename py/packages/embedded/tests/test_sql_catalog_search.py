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

"""Catalog lifecycle and retained search statements through the public ABI."""

import json
from contextlib import closing
from pathlib import Path

import pytest

import antfly_embedded as af
from antfly_embedded._sql import SQLStateError


def test_sql_index_schema_idempotence_retirement_and_reopen(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (id TEXT, n BIGINT, label TEXT)")
        db.sql("INSERT INTO entries (_id,id,n,label) VALUES ('a','a',1,'Alpha')")
        db.sql("CREATE INDEX ordered ON entries (n DESC NULLS LAST,id) INCLUDE (label)")
        db.sql("CREATE INDEX expression_key ON entries (lower(label)) WHERE n > 0")
        with db.open_table("entries") as table:
            schema = table.get_schema()
        indexes = {index["name"]: index for index in schema["relational_indexes"]}
        assert indexes["ordered"]["keys"][0] == {"column": "n", "direction": "desc", "nulls": "last"}
        assert indexes["ordered"]["include_columns"] == ["label"]
        assert indexes["expression_key"]["where"]
        assert "expression" in indexes["expression_key"]["keys"][0]
        db.sql("CREATE INDEX IF NOT EXISTS ordered ON entries (id)")
        with db.open_table("entries") as table:
            assert table.get_schema() == schema
        db.sql("CREATE UNIQUE INDEX identity_key ON entries (id)")
        with pytest.raises(SQLStateError) as duplicate:
            db.sql("INSERT INTO entries (_id,id,n) VALUES ('duplicate','a',2)")
        assert duplicate.value.sqlstate == "23505"
        db.sql("DROP INDEX identity_key")
        db.sql("INSERT INTO entries (_id,id,n) VALUES ('duplicate','a',2)")
        db.sql("DROP INDEX expression_key")
        db.sql("DROP INDEX ordered")
        db.sql("DROP INDEX IF EXISTS ordered")
    with af.open(aflite_path) as db:
        assert db.sql("SELECT n FROM entries ORDER BY n")["rows"] == [["1"], ["2"]]
        with db.open_table("entries") as table:
            schema = table.get_schema()
            assert not schema["relational_indexes"]
            assert not schema["unique_constraints"]


def test_sql_drop_index_ambiguity_fk_dependencies_and_transactions(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents (id BIGINT)")
        db.sql("CREATE TABLE other (id BIGINT)")
        db.sql("CREATE UNIQUE INDEX parent_key ON parents (id)")
        db.sql("CREATE TABLE children (id BIGINT, parent_id BIGINT REFERENCES parents(id))")
        with pytest.raises(SQLStateError) as dependency:
            db.sql("DROP INDEX parent_key")
        assert dependency.value.sqlstate == "2BP01"
        db.sql("CREATE INDEX same_name ON parents (id)")
        db.sql("CREATE INDEX same_name ON other (id)")
        with pytest.raises(SQLStateError) as ambiguity:
            db.sql("DROP INDEX IF EXISTS same_name")
        assert ambiguity.value.sqlstate == "42725"
        db.sql("DROP INDEX same_name ON other")
        db.sql("DROP INDEX same_name")
        with closing(db.sql_session()) as session:
            session.execute("BEGIN")
            with pytest.raises(SQLStateError) as ddl:
                session.execute("CREATE INDEX forbidden ON parents (id)")
            assert ddl.value.sqlstate == "0A000"
            session.execute("ROLLBACK")


def test_sql_validate_retries_failed_coverage_without_changing_schema(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE checked (n BIGINT)")
        db.sql("INSERT INTO checked (_id,n) VALUES ('a',-1)")
        failed = db.sql("ALTER TABLE checked ADD CONSTRAINT positive CHECK (n > 0)")
        assert failed["ddl_receipt"]["state"] == "invalid"
        with db.open_table("checked") as table:
            schema = table.get_schema()
        again = db.sql("ALTER TABLE checked VALIDATE CONSTRAINT positive")
        assert again["ddl_receipt"]["state"] == "invalid"
        db.sql("UPDATE checked SET n=1 WHERE _id='a'")
        for _ in range(2):
            validated = db.sql("ALTER TABLE checked VALIDATE CONSTRAINT positive")
            assert validated["ddl_receipt"]["state"] == "ready"
            assert validated["ddl_receipt"]["schema_version"] == schema["version"]
            with db.open_table("checked") as table:
                assert table.get_schema() == schema
        with pytest.raises(SQLStateError) as violation:
            db.sql("INSERT INTO checked (_id,n) VALUES ('bad',-1)")
        assert violation.value.sqlstate == "23514"
    with af.open(aflite_path) as db:
        assert db.sql("ALTER TABLE checked VALIDATE CONSTRAINT positive")["ddl_receipt"]["state"] == "ready"
        assert db.sql("SELECT n FROM checked")["rows"] == [["1"]]


def test_sql_drop_unused_unique_preserves_the_foreign_keys_selected_generation(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents (id BIGINT)")
        db.sql("CREATE UNIQUE INDEX selected ON parents (id)")
        db.sql("CREATE UNIQUE INDEX redundant ON parents (id)")
        db.sql("CREATE TABLE children (parent_id BIGINT REFERENCES parents(id))")
        db.sql("INSERT INTO parents (_id,id) VALUES ('p',1)")
        db.sql("INSERT INTO children (_id,parent_id) VALUES ('c',1)")
        dropped = db.sql("DROP INDEX redundant")
        assert dropped["ddl_receipt"]["state"] == "ready"
        with pytest.raises(SQLStateError) as dependency:
            db.sql("DROP INDEX selected")
        assert dependency.value.sqlstate == "2BP01"
    with af.open(aflite_path) as db:
        db.sql("INSERT INTO children (_id,parent_id) VALUES ('c2',1)")
        with pytest.raises(SQLStateError) as missing:
            db.sql("INSERT INTO children (_id,parent_id) VALUES ('bad',2)")
        assert missing.value.sqlstate == "23503"


@pytest.mark.parametrize("session", [False, True])
@pytest.mark.parametrize("repair", ["DELETE FROM items WHERE _id='a'", "UPDATE items SET n=2 WHERE _id='b'"])
def test_sql_repairs_failed_unique_coverage(require_native, aflite_path, session, repair):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE items(n BIGINT,CONSTRAINT positive CHECK(n > 0))")
        db.sql("INSERT INTO items(_id,n) VALUES ('a',1),('b',1)")
        assert db.sql("CREATE UNIQUE INDEX u ON items(n)")["ddl_receipt"]["state"] == "invalid"
        with pytest.raises(SQLStateError) as invalid_repair:
            db.sql("UPDATE items SET n=-1 WHERE _id='b'")
        assert invalid_repair.value.sqlstate == "23514"
        if session:
            with closing(db.sql_session()) as connection:
                connection.execute("BEGIN")
                connection.execute(repair)
                connection.execute("COMMIT")
        else:
            db.sql(repair)
        with pytest.raises(SQLStateError) as rejected:
            db.sql("ALTER TABLE items VALIDATE CONSTRAINT u")
        assert rejected.value.sqlstate == "42704"
        db.sql("DROP INDEX u")
        assert db.sql("CREATE UNIQUE INDEX u ON items(n)")["ddl_receipt"]["state"] == "ready"
        with pytest.raises(SQLStateError) as duplicate:
            db.sql("INSERT INTO items(_id,n) VALUES ('bad',1)")
        assert duplicate.value.sqlstate == "23505"
    with af.open(aflite_path) as db:
        with pytest.raises(SQLStateError) as rejected:
            db.sql("ALTER TABLE items VALIDATE CONSTRAINT u")
        assert rejected.value.sqlstate == "42704"
        with pytest.raises(SQLStateError) as duplicate:
            db.sql("INSERT INTO items(_id,n) VALUES ('still_bad',1)")
        assert duplicate.value.sqlstate == "23505"


def test_sql_add_drop_foreign_key_publishes_parent_generations(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY)")
        db.sql("CREATE TABLE children(n BIGINT)")
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1)")
        db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
        for _ in range(2):
            added = db.sql("ALTER TABLE children ADD CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n)")
            assert added["ddl_receipt"]["state"] == "ready"
            with pytest.raises(SQLStateError) as missing:
                db.sql("INSERT INTO children(_id,n) VALUES ('bad',2)")
            assert missing.value.sqlstate == "23503"
            dropped = db.sql("ALTER TABLE children DROP CONSTRAINT fk")
            assert dropped["ddl_receipt"]["state"] == "ready"
        db.sql("DELETE FROM parents")
        assert db.sql("SELECT n FROM children")["rows"] == [["1"]]
    with af.open(aflite_path) as db:
        db.sql("INSERT INTO children(_id,n) VALUES ('free',2)")


def test_sql_drop_child_retires_references_and_recreate_registers_new_generation(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY)")
        for _ in range(2):
            db.sql("CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n))")
            db.sql("INSERT INTO parents(_id,n) VALUES ('p',1)")
            db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
            db.sql("DROP TABLE children")
            db.sql("DELETE FROM parents")
    with af.open(aflite_path) as db:
        db.sql("CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n))")
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1)")
        db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
        db.sql("DROP TABLE children")
        db.sql("DELETE FROM parents")


def test_sql_self_foreign_key_generation_publication(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql(
            "CREATE TABLE nodes(id BIGINT PRIMARY KEY,parent BIGINT,CONSTRAINT fk FOREIGN KEY(parent) REFERENCES nodes(id))"
        )
        db.sql("INSERT INTO nodes(_id,id,parent) VALUES ('a',1,1)")
        assert db.sql("ALTER TABLE nodes DROP CONSTRAINT fk")["ddl_receipt"]["state"] == "ready"
        assert (
            db.sql("ALTER TABLE nodes ADD CONSTRAINT fk FOREIGN KEY(parent) REFERENCES nodes(id)")["ddl_receipt"][
                "state"
            ]
            == "ready"
        )
        db.sql("DROP TABLE nodes")


def test_sql_drop_child_retires_every_parent_owner(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        for parent in ("parents", "other"):
            db.sql(f"CREATE TABLE {parent}(n BIGINT PRIMARY KEY)")
            db.sql(f"INSERT INTO {parent}(_id,n) VALUES ('p',1)")
        db.sql(
            "CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n),"
            "CONSTRAINT other_fk FOREIGN KEY(n) REFERENCES other(n))"
        )
        db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
        db.sql("DROP TABLE children")
        for parent in ("parents", "other"):
            db.sql(f"DELETE FROM {parent}")
            assert db.sql(f"SELECT COUNT(*) FROM {parent}")["rows"] == [["0"]]


def test_sql_failed_foreign_key_allows_parent_repair_after_reopen(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY)")
        db.sql("CREATE TABLE children(n BIGINT)")
        db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
        added = db.sql("ALTER TABLE children ADD CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n)")
        assert added["ddl_receipt"]["state"] == "invalid"
        assert db.sql("SELECT n FROM children")["rows"] == [["1"]]
    with af.open(aflite_path) as db:
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1)")
        assert db.sql("ALTER TABLE children VALIDATE CONSTRAINT fk")["ddl_receipt"]["state"] == "ready"
        with pytest.raises(SQLStateError) as missing:
            db.sql("INSERT INTO children(_id,n) VALUES ('bad',2)")
        assert missing.value.sqlstate == "23503"


def test_sql_search_is_a_source_for_update_and_delete(require_native, aflite_path):
    fixture = json.loads(
        (
            Path(__file__).resolve().parents[4] / "zig/pkg/antfly-embedded/capi-conformance/sql/search-fixture.json"
        ).read_text()
    )
    with af.create(aflite_path, no_sync=True) as db:
        db.create_table("notes", fixture["history"])
        db.sql("INSERT INTO notes (_id,body) VALUES ('a','alpha'),('b','beta')")
        db.sql("CREATE TABLE ranked (id TEXT, rank DOUBLE PRECISION)")
        db.sql("INSERT INTO ranked (_id,id,rank) VALUES ('a','a',0),('b','b',0)")
        result = db.sql(
            "UPDATE ranked SET rank=s.score FROM antfly_search('notes',$1) s WHERE ranked.id=s._id", ["body:alpha"]
        )
        assert result["rows_affected"] == 1
        assert db.sql("SELECT id,rank > 0 FROM ranked ORDER BY id")["rows"] == [["a", True], ["b", False]]
        # Binding the read source must not grant durable range protection to
        # an embedded runtime that cannot execute MERGE under that contract.
        with pytest.raises(SQLStateError) as unsupported:
            db.sql(
                "MERGE INTO ranked r USING antfly_search('notes',$1) s ON r.id=s._id "
                "WHEN MATCHED THEN UPDATE SET rank=s.score",
                ["body:beta"],
            )
        assert unsupported.value.sqlstate == "0A000"
        assert "owner-fenced range protection" in str(unsupported.value)
        db.sql("UPDATE ranked SET rank=s.score FROM antfly_search('notes',$1) s WHERE ranked.id=s._id", ["body:beta"])
        assert db.sql("SELECT id,rank > 0 FROM ranked ORDER BY id")["rows"] == [["a", True], ["b", True]]
        db.sql("DELETE FROM ranked USING antfly_search('notes',$1) s WHERE ranked.id=s._id", ["body:alpha"])
        assert db.sql("SELECT id FROM ranked")["rows"] == [["b"]]


def test_sql_search_retains_hits_and_join_snapshot_between_cursor_pages(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE threads (id TEXT, label TEXT)")
        fixture = json.loads(
            (
                Path(__file__).resolve().parents[4] / "zig/pkg/antfly-embedded/capi-conformance/sql/search-fixture.json"
            ).read_text()
        )
        db.create_table("history", fixture["history"])
        db.sql("INSERT INTO threads (_id,id,label) VALUES ('t','t','before')")
        db.sql("INSERT INTO history (_id,thread_id,body) VALUES ('h1','t','alpha'),('h2','t','alpha')")
        query = "SELECT s._id,t.label FROM antfly_search('history',$1,50) s JOIN threads t ON t.id=s.thread_id"
        request = json.dumps({"full_text_search": {"match": {"field": "body", "text": "alpha"}}})
        with closing(db.sql_cursor(query, [request])) as cursor:
            first = cursor.fetch(1)["result"]["rows"]
            assert len(first) == 1 and first[0][1] == "before"
            db.sql("UPDATE threads SET label='after'")
            db.sql("DELETE FROM history")
            rest = cursor.fetch(10)["result"]["rows"]
            assert len(rest) == 1 and rest[0][1] == "before"
            assert {first[0][0], rest[0][0]} == {"h1", "h2"}
        assert db.sql(query, [request])["rows"] == []
        with closing(db.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("INSERT INTO history (_id,body) VALUES ('staged','alpha')")
            with pytest.raises(SQLStateError) as uncommitted:
                session.execute("SELECT _id FROM antfly_search('history',$1)", [request])
            assert uncommitted.value.sqlstate == "0A000"
            session.execute("ROLLBACK")


def test_sql_search_highlights_match_public_json_and_do_not_use_stored_metadata(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.create_table(
            "notes",
            {
                "version": 1,
                "default_type": "doc",
                "document_schemas": {"doc": {"schema": {"type": "object", "properties": {"body": {"type": "string"}}}}},
            },
        )
        request = {"full_text_search": {"match": "alpha", "field": "body"}}
        with db.open_table("notes") as table:
            table.batch_json({"inserts": {"n1": {"body": "alpha memory", "_highlights": {"spoof": []}}}})
            table.run_until_idle()
            bare = db.sql("SELECT _highlights FROM antfly_search('notes',$1)", [json.dumps(request)])
            assert bare["rows"] == [[None]]
            assert bare["sql_nulls"] == [[True]]
            request["highlight"] = {"fields": ["body"], "fragment_size": 32, "max_fragments": 1}
            public = table.search(request)["responses"][0]["hits"]["hits"][0]["_highlights"]
            result = db.sql("SELECT _highlights FROM antfly_search('notes',$1)", [json.dumps(request)])
            assert result["rows"] == [[public]]
            assert public["body"][0]["spans"]


def test_sql_semantic_search_uses_the_named_tables_configured_embedder(require_native, aflite_path):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    requests = []

    class Embedder(BaseHTTPRequestHandler):
        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(request)
            inputs = request.get("input", [])
            if isinstance(inputs, str):
                inputs = [inputs]
            response = json.dumps(
                {"data": [{"index": i, "embedding": [1.0, 0.0]} for i in range(len(inputs))]}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Embedder)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with af.create(aflite_path, no_sync=True) as db:
            db.create_table(
                "memories",
                {
                    "version": 1,
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "properties": {"body": {"type": "string"}, "thread_id": {"type": "string"}},
                            }
                        }
                    },
                },
            )
            with db.open_table("memories") as table:
                table.add_index(
                    {
                        "name": "semantic",
                        "kind": "dense_vector",
                        "config_json": json.dumps(
                            {
                                "type": "embeddings",
                                "field": "body",
                                "dimension": 2,
                                "distance_metric": "cosine",
                                "embedder": {
                                    "provider": "openai",
                                    "model": "test",
                                    "api_key": "test",
                                    "url": f"http://127.0.0.1:{server.server_port}/v1",
                                },
                            }
                        ),
                    }
                )
                table.batch_json(
                    {"inserts": {"m1": {"body": "alpha memory", "thread_id": "t1"}}, "sync_level": "write"}
                )
                table.run_until_idle()
            db.sql("CREATE TABLE threads (id TEXT, title TEXT)")
            db.sql("INSERT INTO threads (_id,id,title) VALUES ('t1','t1','First')")
            request = json.dumps({"semantic_search": "remember alpha", "indexes": ["semantic"], "limit": 5})
            result = db.sql(
                "SELECT t.title,s._id FROM antfly_search('memories',$1) s JOIN threads t ON t.id=s.thread_id", [request]
            )
            assert result["rows"] == [["First", "m1"]]
            assert any("remember alpha" in json.dumps(request) for request in requests)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_sql_pending_fk_dependency_allows_parent_repair_and_validation(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT)")
        db.sql("CREATE TABLE children(n BIGINT)")
        db.sql("INSERT INTO parents(_id,n) VALUES ('a',1),('b',1)")
        db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
        assert db.sql("ALTER TABLE parents ADD CONSTRAINT u UNIQUE(n)")["ddl_receipt"]["state"] == "invalid"
        added = db.sql("ALTER TABLE children ADD CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n)")
        assert added["ddl_receipt"]["state"] == "pending"
        assert db.sql("SELECT COUNT(*) FROM parents")["rows"] == [["2"]]
    with af.open(aflite_path) as db:
        db.sql("UPDATE parents SET n=2 WHERE _id='b'")
        assert db.sql("ALTER TABLE parents VALIDATE CONSTRAINT u")["ddl_receipt"]["state"] == "ready"
        assert db.sql("ALTER TABLE children VALIDATE CONSTRAINT fk")["ddl_receipt"]["state"] == "ready"
        with pytest.raises(SQLStateError) as missing:
            db.sql("INSERT INTO children(_id,n) VALUES ('bad',3)")
        assert missing.value.sqlstate == "23503"
        db.sql("DROP TABLE children")
        db.sql("DELETE FROM parents")


@pytest.mark.parametrize("index", [False, True])
def test_sql_retire_unrelated_unique_on_fk_child(require_native, aflite_path, index):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY)")
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1)")
        db.sql("CREATE TABLE children(n BIGINT,m BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n))")
        db.sql("CREATE UNIQUE INDEX u ON children(m)" if index else "ALTER TABLE children ADD CONSTRAINT u UNIQUE(m)")
        db.sql("INSERT INTO children(_id,n,m) VALUES ('c',1,2)")
        result = db.sql("DROP INDEX u ON children" if index else "ALTER TABLE children DROP CONSTRAINT u")
        assert result["ddl_receipt"]["state"] == "ready"
        db.sql("INSERT INTO children(_id,n,m) VALUES ('d',1,2)")
        with pytest.raises(SQLStateError) as missing:
            db.sql("INSERT INTO children(_id,n,m) VALUES ('bad',3,2)")
        assert missing.value.sqlstate == "23503"
    with af.open(aflite_path) as db:
        assert db.sql("SELECT COUNT(*) FROM children")["rows"] == [["2"]]


@pytest.mark.parametrize("action", ["CASCADE", "SET NULL"])
def test_sql_session_repair_retains_cascades_and_other_participants(require_native, aflite_path, action):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY,m BIGINT)")
        db.sql(
            "CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n) ON DELETE "
            + action
            + ")"
        )
        db.sql("CREATE TABLE audit(n BIGINT PRIMARY KEY)")
        db.sql("INSERT INTO parents(_id,n,m) VALUES ('a',1,1),('b',2,1)")
        db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
        assert db.sql("ALTER TABLE parents ADD CONSTRAINT u UNIQUE(m)")["ddl_receipt"]["state"] == "invalid"
        with closing(db.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("DELETE FROM parents WHERE _id='a'")
            expected = [] if action == "CASCADE" else [[None]]
            assert session.execute("SELECT n FROM children")["rows"] == expected
            session.execute("INSERT INTO audit(_id,n) VALUES ('a',1)")
            session.execute("COMMIT")
        assert db.sql("SELECT n FROM children")["rows"] == expected
        assert db.sql("SELECT COUNT(*) FROM audit")["rows"] == [["1"]]
        assert db.sql("ALTER TABLE parents VALIDATE CONSTRAINT u")["ddl_receipt"]["state"] == "ready"


def test_sql_session_repairs_multiple_failed_tables_atomically(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        for table in ("first", "second"):
            db.sql(f"CREATE TABLE {table}(n BIGINT,CONSTRAINT positive CHECK(n>0))")
            db.sql(f"INSERT INTO {table}(_id,n) VALUES ('a',1),('b',1)")
            assert db.sql(f"ALTER TABLE {table} ADD CONSTRAINT u UNIQUE(n)")["ddl_receipt"]["state"] == "invalid"
        with closing(db.sql_session()) as session:
            session.execute("BEGIN")
            for table in ("first", "second"):
                session.execute(f"UPDATE {table} SET n=2 WHERE _id='b'")
            session.execute("COMMIT")
        for table in ("first", "second"):
            assert db.sql(f"ALTER TABLE {table} VALIDATE CONSTRAINT u")["ddl_receipt"]["state"] == "ready"
            assert db.sql(f"SELECT n FROM {table} ORDER BY n")["rows"] == [["1"], ["2"]]
        with closing(db.sql_session()) as session:
            session.execute("BEGIN")
            with pytest.raises(SQLStateError) as failed:
                session.execute("UPDATE first SET n=-1 WHERE _id='b'")
            assert failed.value.sqlstate == "23514"
            session.execute("ROLLBACK")


@pytest.mark.parametrize("session_mode", [False, True])
@pytest.mark.parametrize("parent", ["p", "q"])
def test_sql_cascade_discovers_failed_child_repair(require_native, aflite_path, session_mode, parent):
    with af.create(aflite_path, no_sync=True) as db:
        for statement in (
            "CREATE TABLE parents(n BIGINT PRIMARY KEY)",
            "CREATE TABLE children(n BIGINT,m BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n) ON DELETE CASCADE)",
            "INSERT INTO parents(_id,n) VALUES ('p',1),('q',2)",
            "INSERT INTO children(_id,n,m) VALUES ('a',1,1),('b',2,1)",
        ):
            db.sql(statement)
        assert db.sql("ALTER TABLE children ADD CONSTRAINT u UNIQUE(m)")["ddl_receipt"]["state"] == "invalid"
        delete = f"DELETE FROM parents WHERE _id='{parent}'"
        if session_mode:
            with closing(db.sql_session()) as session:
                session.execute("BEGIN")
                session.execute(delete)
                assert session.execute("SELECT COUNT(*) FROM children")["rows"] == [["1"]]
                session.execute("COMMIT")
        else:
            db.sql(delete)
        assert db.sql("ALTER TABLE children VALIDATE CONSTRAINT u")["ddl_receipt"]["state"] == "ready"
    with af.open(aflite_path) as db:
        assert db.sql("SELECT COUNT(*) FROM parents")["rows"] == [["1"]]
        assert db.sql("SELECT COUNT(*) FROM children")["rows"] == [["1"]]


@pytest.mark.parametrize("session_mode", [False, True])
def test_sql_set_null_discovers_failed_child_repair(require_native, aflite_path, session_mode):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY)")
        db.sql("CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n) ON DELETE SET NULL)")
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1)")
        db.sql("INSERT INTO children(_id,n) VALUES ('a',1),('b',1)")
        assert db.sql("ALTER TABLE children ADD CONSTRAINT u UNIQUE(n)")["ddl_receipt"]["state"] == "invalid"
        if session_mode:
            with closing(db.sql_session()) as session:
                session.execute("BEGIN")
                session.execute("DELETE FROM parents")
                assert session.execute("SELECT n FROM children")["rows"] == [[None], [None]]
                session.execute("COMMIT")
        else:
            db.sql("DELETE FROM parents")
        assert db.sql("SELECT n FROM children")["rows"] == [[None], [None]]
        assert db.sql("ALTER TABLE children VALIDATE CONSTRAINT u")["ddl_receipt"]["state"] == "ready"


@pytest.mark.parametrize("session_mode", [False, True])
def test_sql_cascade_discovers_failed_repairs_across_multiple_hops(require_native, aflite_path, session_mode):
    with af.create(aflite_path, no_sync=True) as db:
        for statement in (
            "CREATE TABLE parents(n BIGINT PRIMARY KEY)",
            "CREATE TABLE children(n BIGINT PRIMARY KEY,m BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n) ON DELETE CASCADE)",
            "CREATE TABLE leaves(n BIGINT,m BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES children(n) ON DELETE CASCADE)",
            "INSERT INTO parents(_id,n) VALUES ('p',1),('q',2)",
            "INSERT INTO children(_id,n,m) VALUES ('a',1,1),('b',2,1)",
            "INSERT INTO leaves(_id,n,m) VALUES ('a',1,1),('b',2,1)",
        ):
            db.sql(statement)
        for table in ("children", "leaves"):
            assert db.sql(f"ALTER TABLE {table} ADD CONSTRAINT u UNIQUE(m)")["ddl_receipt"]["state"] == "invalid"
        if session_mode:
            with closing(db.sql_session()) as session:
                session.execute("BEGIN")
                session.execute("DELETE FROM parents WHERE _id='q'")
                assert session.execute("SELECT n FROM leaves")["rows"] == [["1"]]
                session.execute("COMMIT")
        else:
            db.sql("DELETE FROM parents WHERE _id='q'")
        for table in ("children", "leaves"):
            assert db.sql(f"SELECT n FROM {table}")["rows"] == [["1"]]
            assert db.sql(f"ALTER TABLE {table} VALIDATE CONSTRAINT u")["ddl_receipt"]["state"] == "ready"


@pytest.mark.parametrize("session_mode", [False, True])
def test_sql_cascaded_repair_checks_new_values_and_rolls_back_all_participants(
    require_native, aflite_path, session_mode
):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY)")
        db.sql("CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n) ON DELETE SET NULL)")
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1)")
        db.sql("INSERT INTO children(_id,n) VALUES ('a',1),('b',1)")
        assert db.sql("ALTER TABLE children ADD CONSTRAINT u UNIQUE(n)")["ddl_receipt"]["state"] == "invalid"
        db.sql("ALTER TABLE children ADD CONSTRAINT required CHECK(n IS NOT NULL)")
        with pytest.raises(SQLStateError) as rejected:
            if session_mode:
                with closing(db.sql_session()) as session:
                    session.execute("BEGIN")
                    session.execute("DELETE FROM parents")
                    session.execute("COMMIT")
            else:
                db.sql("DELETE FROM parents")
        assert rejected.value.sqlstate == "23514"
        assert db.sql("SELECT n FROM parents")["rows"] == [["1"]]
        assert db.sql("SELECT n FROM children")["rows"] == [["1"], ["1"]]


@pytest.mark.parametrize("session_mode", [False, True])
def test_sql_repair_discovery_keeps_read_only_parent_coverage(require_native, aflite_path, session_mode):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY,m BIGINT)")
        db.sql("CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n))")
        db.sql("INSERT INTO parents(_id,n,m) VALUES ('p',1,1),('q',2,1)")
        assert db.sql("ALTER TABLE parents ADD CONSTRAINT u UNIQUE(m)")["ddl_receipt"]["state"] == "invalid"
        with pytest.raises(SQLStateError) as rejected:
            if session_mode:
                with closing(db.sql_session()) as session:
                    session.execute("BEGIN")
                    session.execute("INSERT INTO children(_id,n) VALUES ('c',1)")
                    session.execute("COMMIT")
            else:
                db.sql("INSERT INTO children(_id,n) VALUES ('c',1)")
        assert rejected.value.sqlstate == "55006"
        assert db.sql("SELECT COUNT(*) FROM children")["rows"] == [["0"]]
        assert db.sql("SELECT COUNT(*) FROM parents")["rows"] == [["2"]]


@pytest.mark.parametrize("session_mode", [False, True])
def test_sql_cascade_repairs_failed_child_check_coverage(require_native, aflite_path, session_mode):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT PRIMARY KEY)")
        db.sql(
            "CREATE TABLE children(n BIGINT,m BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n) ON DELETE CASCADE)"
        )
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1),('q',2)")
        db.sql("INSERT INTO children(_id,n,m) VALUES ('a',1,-1),('b',2,1)")
        assert db.sql("ALTER TABLE children ADD CONSTRAINT positive CHECK(m>0)")["ddl_receipt"]["state"] == "invalid"
        if session_mode:
            with closing(db.sql_session()) as session:
                session.execute("BEGIN")
                session.execute("DELETE FROM parents WHERE _id='p'")
                session.execute("COMMIT")
        else:
            db.sql("DELETE FROM parents WHERE _id='p'")
        assert db.sql("SELECT m FROM children")["rows"] == [["1"]]
        assert db.sql("ALTER TABLE children VALIDATE CONSTRAINT positive")["ddl_receipt"]["state"] == "ready"


@pytest.mark.parametrize("session_mode", [False, True])
@pytest.mark.parametrize("alter", [False, True])
def test_sql_partial_match_uses_ready_support_indexes_and_repairs_cascades(
    require_native, aflite_path, session_mode, alter
):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(a BIGINT,b BIGINT,PRIMARY KEY(a,b))")
        db.sql("INSERT INTO parents(_id,a,b) VALUES ('p',1,10),('q',2,20)")
        for name, column in (("by_a", "a"), ("by_b", "b")):
            result = db.sql(f"CREATE INDEX {name} ON parents({column})")
            assert result["ddl_receipt"]["state"] == "ready"
        definition = "CONSTRAINT fk FOREIGN KEY(a,b) REFERENCES parents(a,b) MATCH PARTIAL ON DELETE CASCADE"
        if alter:
            db.sql("CREATE TABLE children(a BIGINT,b BIGINT,m BIGINT)")
            assert db.sql("ALTER TABLE children ADD " + definition)["ddl_receipt"]["state"] == "ready"
        else:
            db.sql("CREATE TABLE children(a BIGINT,b BIGINT,m BIGINT," + definition + ")")
        db.sql("INSERT INTO children(_id,a,b,m) VALUES ('a',1,NULL,1),('b',2,NULL,1)")
        assert db.sql("ALTER TABLE children ADD CONSTRAINT u UNIQUE(m)")["ddl_receipt"]["state"] == "invalid"
        if session_mode:
            with closing(db.sql_session()) as session:
                session.execute("BEGIN")
                session.execute("DELETE FROM parents WHERE _id='q'")
                assert session.execute("SELECT a FROM children")["rows"] == [["1"]]
                session.execute("COMMIT")
        else:
            db.sql("DELETE FROM parents WHERE _id='q'")
        assert db.sql("ALTER TABLE children VALIDATE CONSTRAINT u")["ddl_receipt"]["state"] == "ready"
    with af.open(aflite_path) as db:
        assert db.sql("SELECT a FROM children")["rows"] == [["1"]]
        db.sql("INSERT INTO children(_id,a,b,m) VALUES ('c',1,NULL,2)")


@pytest.mark.parametrize("drain", [False, True])
def test_sql_partial_match_advances_existing_pending_parent_support(require_native, aflite_path, drain):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(a BIGINT,b BIGINT,PRIMARY KEY(a,b))")
        db.sql("INSERT INTO parents(_id,a,b) VALUES ('p',1,10)")
        with db.open_table("parents") as parents:
            schema = parents.get_schema()
            schema["version"] = schema.get("version", 0) + 1
            schema["relational_indexes"] = [
                {"name": "by_a", "keys": [{"column": "a"}]},
                {"name": "by_b", "keys": [{"column": "b"}]},
            ]
            parents.set_schema(schema)
            if drain:
                parents.run_until_idle()
        db.sql(
            "CREATE TABLE children(a BIGINT,b BIGINT,"
            "CONSTRAINT fk FOREIGN KEY(a,b) REFERENCES parents(a,b) MATCH PARTIAL ON DELETE CASCADE)"
        )
        db.sql("INSERT INTO children(_id,a,b) VALUES ('c',1,NULL)")
        assert db.sql("SELECT a FROM children")["rows"] == [["1"]]
        db.sql("DELETE FROM parents")
        assert db.sql("SELECT COUNT(*) FROM children")["rows"] == [["0"]]


def test_sql_partial_match_admission_ignores_unrelated_pending_builds(require_native, aflite_path):
    # Hosted mode keeps unrelated builds pending until explicitly advanced.
    with af.create(aflite_path, no_sync=True, profile=af.Profile.HOSTED) as db:
        db.sql("CREATE TABLE parents(a BIGINT,b BIGINT,z BIGINT,PRIMARY KEY(a,b))")
        db.sql("CREATE INDEX by_a ON parents(a)")
        db.sql("CREATE INDEX by_b ON parents(b)")
        for start in range(0, 2000, 200):
            values = ",".join(f"('{i}',{i},{i},1)" for i in range(start, start + 200))
            db.sql("INSERT INTO parents(_id,a,b,z) VALUES " + values)
        with db.open_table("parents") as parents:
            schema = parents.get_schema()
            schema["version"] += 1
            schema["relational_indexes"] += [{"name": f"unrelated_{i}", "keys": [{"column": "z"}]} for i in range(200)]
            parents.set_schema(schema)
        # The required witness indexes are already ready. A full catalog
        # drain exhausted the admission deadline on these unrelated builds.
        db.sql(
            "CREATE TABLE children(a BIGINT,b BIGINT,"
            "CONSTRAINT fk FOREIGN KEY(a,b) REFERENCES parents(a,b) MATCH PARTIAL)"
        )
        db.sql("INSERT INTO children(_id,a,b) VALUES ('c',1,NULL)")
        with pytest.raises(SQLStateError) as missing:
            db.sql("INSERT INTO children(_id,a,b) VALUES ('bad',2001,NULL)")
        assert missing.value.sqlstate == "23503"


@pytest.mark.parametrize("storage_mode", ["relational", "document"])
def test_sql_search_preserves_native_nulls_and_json_numbers(require_native, aflite_path, storage_mode):
    with af.create(aflite_path, no_sync=True, profile=af.Profile.HOSTED) as db:
        if storage_mode == "relational":
            db.sql("CREATE TABLE values_source(body TEXT,payload JSONB)")
        else:
            db.create_table(
                "values_source",
                {
                    "version": 1,
                    "default_type": "doc",
                    "document_schemas": {
                        "doc": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "body": {"type": "string"},
                                    "payload": {"type": "object", "nullable": True},
                                },
                            }
                        }
                    },
                },
            )
        db.sql(
            "INSERT INTO values_source(_id,body,payload) VALUES "
            "('a','alpha',NULL),('b','alpha',CAST('null' AS JSONB)),"
            "('c','alpha',CAST('{\"amount\":9007199254740993.0,"
            '"fraction":0.123456789012345678901}\' AS JSONB))'
        )
        with db.open_table("values_source") as table:
            table.run_until_idle()
        projection = "_id,payload IS NULL,payload ->> 'amount',payload ->> 'fraction'"
        expected = db.sql(f"SELECT {projection} FROM values_source ORDER BY _id")["rows"]
        assert expected[0][1] is True
        assert expected[1][1] is False
        assert expected[2][3] == "0.123456789012345678901"
        source = "antfly_search('values_source','body:alpha')"
        assert db.sql(f"SELECT {projection} FROM {source} ORDER BY _id")["rows"] == expected
        assert db.sql(f"SELECT _id FROM {source} WHERE payload IS NULL")["rows"] == [["a"]]
        assert db.sql(f"SELECT _id,score IS NOT NULL,_highlights IS NULL FROM {source} ORDER BY _id")["rows"] == [
            ["a", True, True],
            ["b", True, True],
            ["c", True, True],
        ]
        if storage_mode == "relational":
            with db.open_table("values_source") as table:
                schema = table.get_schema()
                schema["version"] = schema.get("version", 0) + 1
                schema["document_schemas"][schema["default_type"]]["schema"]["properties"]["added"] = {
                    "type": "json",
                    "nullable": True,
                }
                table.set_schema(schema)
            assert db.sql(f"SELECT _id,added IS NULL,payload IS NULL FROM {source} ORDER BY _id")["rows"] == [
                ["a", True, True],
                ["b", True, False],
                ["c", True, False],
            ]
        db.sql("CREATE TABLE copied(body TEXT,payload JSONB)")
        db.sql(f"INSERT INTO copied(_id,body,payload) SELECT _id,body,payload FROM {source}")
        copied = db.sql(f"SELECT {projection} FROM copied ORDER BY _id")["rows"]
        assert [row[:2] + row[3:] for row in copied] == [row[:2] + row[3:] for row in expected]
        assert copied[2][2] == "9007199254740993"
        # Materialized ranked rows retain typed nulls and exact JSON after
        # writers change the source between cursor pages.
        with closing(db.sql_cursor(f"SELECT {projection} FROM {source}")) as cursor:
            first = cursor.fetch(1)["result"]["rows"]
            db.sql("UPDATE values_source SET payload=CAST('{}' AS JSONB)")
            remaining = cursor.fetch(10)["result"]["rows"]
            assert sorted(first + remaining, key=lambda row: row[0]) == expected


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize(
    "predicate,selected",
    [
        ("archived", (True,)),
        ("NOT archived", (False,)),
        ("NOT NOT archived", (True,)),
        ("archived IS TRUE", (True,)),
        ("archived IS FALSE", (False,)),
        ("archived IS NOT TRUE", (False, None)),
        ("archived IS NOT FALSE", (True, None)),
        ("NOT (archived = true)", (False,)),
        ("NOT (archived = false)", (True,)),
        ("NOT (archived IS FALSE)", (True, None)),
        ("NOT (archived IS NOT TRUE)", (True,)),
        ("archived AND label <> ''", (True,)),
        ("NOT (NOT archived OR label = '')", (True,)),
    ],
)
def test_sql_boolean_partial_index_membership_and_unique_claims(
    require_native, aflite_path, nullable, predicate, selected
):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql(f"CREATE TABLE threads (n BIGINT, label TEXT, archived BOOLEAN {'NULL' if nullable else 'NOT NULL'})")
        states = [True, False, None] if nullable else [True, False]
        for n, state in enumerate(states):
            db.sql("INSERT INTO threads(_id,n,label,archived) VALUES ($1,$2,$3,$4)", [f"row{n}", n, f"label{n}", state])
        receipt = db.sql(f"CREATE UNIQUE INDEX visible ON threads(label) INCLUDE(n) WHERE {predicate}")
        assert receipt["ddl_receipt"]["state"] == "ready"
        with db.open_table("threads") as table:
            schema = table.get_schema()
        index = schema["relational_indexes"][0]
        assert index["where"] == schema["unique_constraints"][0]["where"]
        if predicate in ("archived IS NOT TRUE", "archived IS NOT FALSE", "NOT (archived IS FALSE)"):
            assert index["where"][0]["op"] == "is_distinct"
        for n, state in enumerate(states):
            statement = "INSERT INTO threads(_id,n,label,archived) VALUES ($1,$2,$3,$4)"
            parameters = [f"duplicate{n}", n + 10, f"label{n}", state]
            if state in selected:
                with pytest.raises(SQLStateError) as duplicate:
                    db.sql(statement, parameters)
                assert duplicate.value.sqlstate == "23505"
            else:
                db.sql(statement, parameters)
        expected = [[str(n)] for n, state in enumerate(states) if state in selected]
        assert db.sql(f"SELECT n FROM threads WHERE {predicate} ORDER BY n")["rows"] == expected
        # Updates must consult the same native partial-unique membership as
        # index construction: entering coverage now conflicts with row0/row1.
        if predicate == "NOT archived":
            db.sql("INSERT INTO threads(_id,n,label,archived) VALUES ('outside',99,'label1',true)")
            with pytest.raises(SQLStateError) as duplicate:
                db.sql("UPDATE threads SET archived=false WHERE _id='outside'")
            assert duplicate.value.sqlstate == "23505"


def test_sql_boolean_partial_index_updates_and_reopen(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE threads(n BIGINT, archived BOOLEAN)")
        db.sql("INSERT INTO threads(_id,n,archived) VALUES ('false',1,false),('true',2,true),('null',3,NULL)")
        db.sql("CREATE INDEX visible ON threads(n DESC) WHERE NOT archived")
        db.sql("CREATE INDEX not_true ON threads(n DESC) WHERE archived IS NOT TRUE")
        assert db.sql("SELECT n FROM threads WHERE n >= 0 AND archived = false ORDER BY n DESC")["rows"] == [["1"]]
        assert db.sql("SELECT n FROM threads WHERE n >= 0 AND archived IS NOT TRUE ORDER BY n DESC")["rows"] == [
            ["3"],
            ["1"],
        ]
        db.sql("UPDATE threads SET archived=false WHERE _id='true'")
        db.sql("UPDATE threads SET archived=true WHERE _id='false'")
    with af.open(aflite_path) as db:
        assert db.sql("SELECT n FROM threads WHERE n >= 0 AND archived = false ORDER BY n DESC")["rows"] == [["2"]]
        assert db.sql("SELECT n FROM threads WHERE n >= 0 AND archived IS NOT TRUE ORDER BY n DESC")["rows"] == [
            ["3"],
            ["2"],
        ]


@pytest.mark.parametrize(
    "predicate,selected",
    [
        ("flag IS TRUE", (True,)),
        ("flag IS FALSE", (False,)),
        ("flag IS NOT TRUE", (False, None)),
        ("flag IS NOT FALSE", (True, None)),
    ],
)
def test_sql_check_boolean_truth_tests_preserve_null_semantics(require_native, aflite_path, predicate, selected):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql(f"CREATE TABLE truth_tests(flag BOOLEAN, CONSTRAINT truth_test CHECK ({predicate}))")
        for n, state in enumerate([True, False, None]):
            if state in selected:
                db.sql("INSERT INTO truth_tests(_id,flag) VALUES ($1,$2)", [f"row{n}", state])
            else:
                with pytest.raises(SQLStateError) as violation:
                    db.sql("INSERT INTO truth_tests(_id,flag) VALUES ($1,$2)", [f"row{n}", state])
                assert violation.value.sqlstate == "23514"


@pytest.mark.parametrize(
    "predicate,sqlstate",
    [
        ("label", "22023"),
        ("NOT label", "22023"),
        ("label IS FALSE", "22023"),
        ("archived OR label = ''", "0A000"),
        ("NOT (archived AND label = '')", "0A000"),
    ],
)
def test_sql_partial_index_rejects_invalid_predicates_without_schema_changes(
    require_native, aflite_path, predicate, sqlstate
):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE threads(n BIGINT, label TEXT, archived BOOLEAN)")
        with db.open_table("threads") as table:
            before = table.get_schema()
        with pytest.raises(SQLStateError) as rejected:
            db.sql(f"CREATE INDEX invalid ON threads(n) WHERE {predicate}")
        assert rejected.value.sqlstate == sqlstate
        with db.open_table("threads") as table:
            assert table.get_schema() == before


@pytest.mark.parametrize(
    "predicate,value",
    [
        ("flag IS NOT TRUE", True),
        ("flag IS NOT FALSE", False),
        ("NOT (flag IS TRUE)", True),
        ("NOT (flag IS FALSE)", False),
    ],
)
def test_sql_partial_index_null_safe_historical_rows(require_native, aflite_path, predicate, value):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE historical(n BIGINT)")
        db.sql("INSERT INTO historical(_id,n) VALUES ('old',1)")
        db.sql("ALTER TABLE historical ADD COLUMN flag BOOLEAN NOT NULL DEFAULT false")
        db.sql(f"CREATE INDEX visible ON historical(n) WHERE {predicate}")
        with db.open_table("historical") as table:
            assert table.get_schema()["relational_indexes"][0]["where"] == [
                {"column": "flag", "op": "is_distinct", "value": value}
            ]
        assert db.sql(f"SELECT _id,flag FROM historical WHERE n>=0 AND {predicate}")["rows"] == [["old", None]]
    with af.open(aflite_path) as db:
        assert db.sql(f"SELECT _id,flag FROM historical WHERE n>=0 AND {predicate}")["rows"] == [["old", None]]


@pytest.mark.parametrize(
    "predicate,allowed",
    [
        ("x IN (1, 2)", [1, 2, None]),
        ("x NOT IN (1, 2)", [3, None]),
        ("x IN (1, NULL)", [1, 2, 3, None]),
        ("x NOT IN (1, NULL)", [2, 3, None]),
        ("x IN (y, 2)", [1, 2, None]),
        ("x IN (1, 1 / y)", [1, None]),
        ("NULL IN (NULL)", [1, 2, 3, None]),
        ("x IN (NULL)", [1, 2, 3, None]),
    ],
)
def test_sql_check_in_list_enforces_three_valued_membership(require_native, aflite_path, predicate, allowed):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql(f"CREATE TABLE membership(x BIGINT, y BIGINT, CONSTRAINT allowed CHECK({predicate}))")
        for index, value in enumerate([1, 2, 3, None]):
            if value in allowed:
                db.sql(
                    "INSERT INTO membership(_id,x,y) VALUES ($1,$2,$3)",
                    [str(index), value, 0 if "/ y" in predicate and value is not None else 1],
                )
            else:
                with pytest.raises(SQLStateError) as rejected:
                    db.sql("INSERT INTO membership(_id,x,y) VALUES ($1,$2,1)", [str(index), value])
                assert rejected.value.sqlstate == "23514"
    with af.open(aflite_path) as db:
        assert len(db.sql("SELECT x FROM membership")["rows"]) == len(allowed)
        if 3 not in allowed:
            with pytest.raises(SQLStateError) as rejected:
                db.sql("UPDATE membership SET x=3,y=1 WHERE x=1")
            assert rejected.value.sqlstate == "23514"


def test_sql_check_in_list_activation_and_exact_integer_membership(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE checked(x BIGINT)")
        db.sql("INSERT INTO checked(_id,x) VALUES ('bad',9007199254740992)")
        receipt = db.sql("ALTER TABLE checked ADD CONSTRAINT exact CHECK(x IN (9007199254740993, NULL))")
        assert receipt["ddl_receipt"]["state"] == "ready"
        assert (
            db.sql("ALTER TABLE checked ADD CONSTRAINT strict CHECK(x IN (9007199254740993))")["ddl_receipt"]["state"]
            == "invalid"
        )
        with pytest.raises(SQLStateError) as rejected:
            db.sql("INSERT INTO checked(_id,x) VALUES ('newbad',9007199254740992)")
        assert rejected.value.sqlstate == "23514"
        db.sql("UPDATE checked SET x=9007199254740993 WHERE _id='bad'")
        assert db.sql("ALTER TABLE checked VALIDATE CONSTRAINT strict")["ddl_receipt"]["state"] == "ready"
        db.sql("INSERT INTO checked(_id,x) VALUES ('newgood',9007199254740993)")


def test_sql_strpos_parameters_unicode_and_thread_filter(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE titles(title TEXT)")
        db.sql("INSERT INTO titles(_id,title) VALUES ('a','é🍎needle'),('b','absent'),('c',NULL),('d','needleneedle')")
        assert db.sql("SELECT _id, strpos(title,$1) FROM titles WHERE strpos(title,$1)>0 ORDER BY _id", ["needle"])[
            "rows"
        ] == [["a", "3"], ["d", "1"]]
        assert db.sql("SELECT strpos($1,$2),strpos($3,$2),strpos($1,$4)", ["é🍎needle", "needle", None, ""])[
            "rows"
        ] == [["3", None, "1"]]
        for statement, parameters in [
            ("SELECT strpos(1,'x')", []),
            ("SELECT strpos('x',1)", []),
            ("SELECT strpos('x')", []),
        ]:
            with pytest.raises(SQLStateError):
                db.sql(statement, parameters)


def test_sql_check_in_list_large_bounded_lists(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        allowed = ",".join(str(n) for n in range(100))
        db.sql(f"CREATE TABLE bounded(x BIGINT CHECK(x IN ({allowed})))")
        db.sql("INSERT INTO bounded(_id,x) VALUES ('last',99)")
        with pytest.raises(SQLStateError) as rejected:
            db.sql("INSERT INTO bounded(_id,x) VALUES ('absent',100)")
        assert rejected.value.sqlstate == "23514"


def test_sql_explicit_like_escape_parameters_and_validation(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE escaped(title TEXT)")
        db.sql("INSERT INTO escaped(_id,title) VALUES ('literal','a_b'),('wildcard','axb'),('null',NULL)")
        assert db.sql("SELECT _id FROM escaped WHERE title LIKE $1 ESCAPE $2 ORDER BY _id", ["%é_%", "é"])["rows"] == [
            ["literal"]
        ]
        assert db.sql("SELECT _id FROM escaped WHERE title NOT ILIKE $1 ESCAPE $2 ORDER BY _id", ["A!_B", "!"])[
            "rows"
        ] == [["wildcard"]]
        assert db.sql("SELECT 'a_b' LIKE $1 ESCAPE $2", ["a_b", ""])["rows"] == [[True]]
        assert db.sql("SELECT 'a_b' LIKE $1 ESCAPE $2", ["a_b", None])["rows"] == [[None]]
        for pattern, escape in [("a", "xx"), ("x!", "!"), ("!", "!")]:
            with pytest.raises(SQLStateError) as rejected:
                db.sql("SELECT 'a' LIKE $1 ESCAPE $2", [pattern, escape])
            assert rejected.value.sqlstate == "22025"


def test_sql_correlated_projection_and_insert_select_not_exists(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE parents(n BIGINT)")
        db.sql("CREATE TABLE children(parent_id TEXT, label TEXT)")
        db.sql("CREATE TABLE copied(n BIGINT)")
        db.sql("INSERT INTO parents(_id,n) VALUES ('p',1),('empty',2)")
        db.sql("INSERT INTO children(_id,parent_id,label) VALUES ('c','p','child')")
        assert db.sql(
            "SELECT p._id,(SELECT c.label FROM children c WHERE c.parent_id=p._id) AS label FROM parents p ORDER BY p._id"
        )["rows"] == [["empty", None], ["p", "child"]]
        assert db.sql("INSERT INTO copied(_id,n) SELECT p._id,p.n FROM parents p")["rows_affected"] == 2
        db.sql("DELETE FROM copied WHERE _id='p'")
        statement = "INSERT INTO copied(_id,n) SELECT p._id,p.n FROM parents p WHERE NOT EXISTS (SELECT 1 FROM copied c WHERE c._id=p._id)"
        assert db.sql(statement)["rows_affected"] == 1
        assert db.sql(statement)["rows_affected"] == 0
        assert db.sql("SELECT _id,n FROM copied ORDER BY _id")["rows"] == [["empty", "2"], ["p", "1"]]


@pytest.mark.parametrize(
    "eviction",
    [
        "DELETE FROM entries WHERE id IN (SELECT id FROM entries ORDER BY updated DESC,id OFFSET 2)",
        "DELETE FROM entries WHERE id IN (SELECT id FROM (SELECT id,ROW_NUMBER() OVER "
        "(ORDER BY updated DESC,id) AS rank FROM entries) ranked WHERE rank>2)",
        "WITH ranked AS (SELECT id,ROW_NUMBER() OVER (ORDER BY updated DESC,id) AS rank FROM entries) "
        "DELETE FROM entries WHERE id IN (SELECT id FROM ranked WHERE rank>2)",
        "WITH ranked AS (SELECT id,ROW_NUMBER() OVER (ORDER BY updated DESC,id) AS rank FROM entries) "
        "DELETE FROM entries USING ranked WHERE entries.id=ranked.id AND ranked.rank>2",
    ],
)
def test_sql_eviction_subqueries_and_window_derived_tables(require_native, aflite_path, eviction):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (id TEXT, updated BIGINT)")
        db.sql("INSERT INTO entries (_id,id,updated) VALUES ('a','a',1),('b','b',2),('c','c',2),('d','d',3)")
        assert db.sql(
            "SELECT id FROM (SELECT id,ROW_NUMBER() OVER (ORDER BY updated DESC,id) AS rank "
            "FROM entries) ranked WHERE rank>2 ORDER BY rank"
        )["rows"] == [["c"], ["a"]]
        assert db.sql(eviction)["rows_affected"] == 2
        assert db.sql("SELECT id FROM entries ORDER BY updated DESC,id")["rows"] == [["d"], ["b"]]
    with af.open(aflite_path) as db:
        assert db.sql("SELECT id FROM entries ORDER BY id")["rows"] == [["b"], ["d"]]


def test_sql_partitioned_window_eviction(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (id TEXT, category TEXT, updated BIGINT)")
        db.sql(
            "INSERT INTO entries (_id,id,category,updated) VALUES "
            "('a','a','x',1),('b','b','x',2),('c','c','y',1),('d','d','y',2)"
        )
        assert (
            db.sql(
                "DELETE FROM entries WHERE id IN (SELECT id FROM (SELECT id,ROW_NUMBER() OVER "
                "(PARTITION BY category ORDER BY updated DESC,id) AS rank FROM entries) ranked WHERE rank>1)"
            )["rows_affected"]
            == 2
        )
        assert db.sql("SELECT id FROM entries ORDER BY id")["rows"] == [["b"], ["d"]]


def test_sql_large_values_requests_and_transactions(require_native, aflite_path):
    payload = "é" * 1_100_000
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (id TEXT, body TEXT)")
        db.sql("CREATE TABLE other (id TEXT, body TEXT)")
        # Exercise data admission independently of the SQL source/AST limits.
        assert db.sql("SELECT CAST($1 AS TEXT) AS body", [payload])["rows"] == [[payload]]
        assert db.sql("SELECT '" + payload + "' AS body")["rows"] == [[payload]]
        with closing(db.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("INSERT INTO entries (_id,id,body) VALUES ('a','a',$1)", [payload])
            session.execute("INSERT INTO entries (_id,id,body) VALUES ('c','c',$1)", [payload])
            session.execute("INSERT INTO other (_id,id,body) VALUES ('b','b',$1)", [payload])
            session.execute("COMMIT")
            session.execute("BEGIN")
            session.execute("UPDATE entries SET body=$1", [payload + "rollback"])
            session.execute("ROLLBACK")
        assert db.sql("SELECT body FROM entries")["rows"] == [[payload], [payload]]
        assert db.sql("SELECT body FROM other")["rows"] == [[payload]]
        assert db.sql("SELECT body FROM (SELECT body FROM entries ORDER BY body) sorted")["rows"] == [
            [payload],
            [payload],
        ]
        with closing(db.sql_cursor("SELECT body FROM entries WHERE body=$1", [payload])) as cursor:
            assert cursor.fetch(2)["result"]["rows"] == [[payload], [payload]]
    with af.open(aflite_path) as db:
        assert db.sql("SELECT body FROM entries")["rows"] == [[payload], [payload]]
        assert db.sql("SELECT body FROM other")["rows"] == [[payload]]


def test_sql_large_search_values_fall_back_from_index_bounds(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (body TEXT)")
        db.sql("INSERT INTO entries (_id,body) VALUES ('a','short')")
        db.sql("CREATE INDEX by_body ON entries (body)")
        payload = "z" * 1_100_000
        assert db.sql("SELECT body FROM entries WHERE body<$1", [payload])["rows"] == [["short"]]
        with closing(db.sql_cursor("SELECT body FROM entries WHERE body=$1", [payload])) as cursor:
            assert cursor.fetch(10)["result"]["rows"] == []
        assert db.sql("SELECT CAST($1 AS JSONB) AS data", [{"body": payload}])["rows"] == [[{"body": payload}]]


@pytest.mark.parametrize("predicate", ["n IN (1, 2.0)", "n NOT IN (0, 3.5)", "n IN (NULL, 1, 2.0)"])
def test_sql_check_mixed_numeric_membership(require_native, aflite_path, predicate):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql(f"CREATE TABLE entries (n BIGINT CHECK ({predicate}))")
        db.sql("INSERT INTO entries (_id,n) VALUES ('one',1),('two',2),('null',NULL)")
        # UNKNOWN passes a CHECK, while a definite nonmember fails.
        if "NULL," not in predicate:
            with pytest.raises(SQLStateError) as failure:
                db.sql("INSERT INTO entries (_id,n) VALUES ('bad',0)")
            assert failure.value.sqlstate == "23514"
    with af.open(aflite_path) as db:
        db.sql("INSERT INTO entries (_id,n) VALUES ('reopened',2)")


def test_sql_check_numeric_membership_keeps_large_integers_exact(require_native, aflite_path):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (n BIGINT CHECK (n NOT IN (9007199254740992.0)))")
        db.sql("INSERT INTO entries (_id,n) VALUES ('exact',9007199254740993)")
        with pytest.raises(SQLStateError) as failure:
            db.sql("INSERT INTO entries (_id,n) VALUES ('rounded',9007199254740992)")
        assert failure.value.sqlstate == "23514"
        db.sql("CREATE TABLE numbers (n DOUBLE PRECISION CHECK (n IN (1, 2.5)))")
        db.sql("INSERT INTO numbers (_id,n) VALUES ('integer',1),('fraction',2.5)")
        with pytest.raises(SQLStateError) as failure:
            db.sql("INSERT INTO numbers (_id,n) VALUES ('bad',2)")
        assert failure.value.sqlstate == "23514"


def test_sql_describe_enforces_preparation_budget(require_native, aflite_path):
    import ctypes

    from antfly_embedded import _ffi

    with af.create(aflite_path, no_sync=True) as db:
        # Describe is exposed by the C ABI; Python does not wrap it yet.
        describe = db._lib.antfly_db_sql_describe_json
        describe.argtypes = [ctypes.c_void_p, _ffi.AntflySlice, ctypes.POINTER(_ffi.AntflyBuffer)]
        describe.restype = ctypes.c_int

        def request(statement):
            source, keepalive = _ffi.make_slice(json.dumps({"statement": statement}).encode())
            output = _ffi.AntflyBuffer()
            status = describe(db._handle, source, ctypes.byref(output))
            try:
                result = json.loads(ctypes.string_at(output.ptr, output.len))
            finally:
                db._lib.antfly_buffer_free(ctypes.byref(output))
            assert keepalive is not None
            return status, result

        # Legal request size, but its compilation exceeds the preparation working set.
        status, result = request("SELECT '" + "x" * (56 * 1024 * 1024) + "' AS value")
        assert status != 0
        assert result["error"]["code"] == "54000"
        # Rejection must release allocations and leave the handle usable.
        status, result = request("SELECT '" + "x" * (2 * 1024 * 1024) + "' AS value")
        assert status == 0
        assert result == {"columns": [{"name": "value", "type": "string"}], "parameter_types": []}
        assert db.sql("SELECT 7 AS n")["rows"] == [["7"]]


@pytest.mark.parametrize("phase", ["parse", "compile"])
def test_sql_cursor_preparation_budget_rejection(require_native, aflite_path, phase):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (body TEXT)")
        db.sql("INSERT INTO entries (_id,body) VALUES ('ok','usable')")
        if phase == "parse":
            statement = "SELECT body FROM entries WHERE body=$1"
            parameters = ["x" * (40 * 1024 * 1024)]
        else:
            statement = "SELECT body FROM entries WHERE body='" + "x" * (56 * 1024 * 1024) + "'"
            parameters = []
        with pytest.raises(SQLStateError) as failure:
            db.sql_cursor(statement, parameters)
        assert failure.value.sqlstate == "54000"
        # Failed preparation must release its allocations and cursor slot.
        with closing(db.sql_cursor("SELECT body FROM entries WHERE body=$1", ["usable"])) as cursor:
            assert cursor.fetch(1)["result"]["rows"] == [["usable"]]
        assert db.sql("SELECT 7 AS n")["rows"] == [["7"]]


@pytest.mark.parametrize("api", ["execute", "cursor"])
@pytest.mark.parametrize("phase", ["parse", "compile"])
@pytest.mark.parametrize("recovery", ["rollback", "savepoint"])
def test_sql_preparation_failure_aborts_session(require_native, aflite_path, api, phase, recovery):
    with af.create(aflite_path, no_sync=True) as db:
        db.sql("CREATE TABLE entries (body TEXT)")
        with closing(db.sql_session()) as session, closing(db.sql_session()) as other:
            session.execute("BEGIN")
            session.execute("INSERT INTO entries (_id,body) VALUES ('before','pending')")
            session.execute("SAVEPOINT checkpoint")
            session.execute("INSERT INTO entries (_id,body) VALUES ('after','pending')")
            if phase == "parse":
                statement = "SELECT body FROM entries WHERE body=$1"
                parameters = ["x" * (40 * 1024 * 1024)]
            else:
                statement = "SELECT body FROM entries WHERE body='" + "x" * (56 * 1024 * 1024) + "'"
                parameters = []
            with pytest.raises(SQLStateError) as failure:
                if api == "execute":
                    session.execute(statement, parameters)
                else:
                    session.open_cursor(statement, parameters)
            assert failure.value.sqlstate == "54000"
            for statement in ["SELECT body FROM entries", "COMMIT"]:
                with pytest.raises(SQLStateError) as aborted:
                    session.execute(statement)
                assert aborted.value.sqlstate == "25P02"
            assert other.execute("SELECT _id FROM entries")["rows"] == []
            if recovery == "savepoint":
                session.execute("ROLLBACK TO SAVEPOINT checkpoint")
                expected = [["before"], ["recovered"]]
            else:
                session.execute("ROLLBACK")
                session.execute("BEGIN")
                expected = [["recovered"]]
            session.execute("INSERT INTO entries (_id,body) VALUES ('recovered','usable')")
            session.execute("COMMIT")
            assert db.sql("SELECT _id FROM entries ORDER BY _id")["rows"] == expected
