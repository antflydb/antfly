# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

import antfly_embedded as af
from antfly_embedded import dbapi

CASES = Path(__file__).resolve().parents[4] / "zig/pkg/antfly-embedded/capi-conformance/sql/cases.json"


def canonical(rows):
    return [
        [str(value) if isinstance(value, int) and not isinstance(value, bool) else value for value in row]
        for row in rows
    ]


def test_sql_conformance(require_native, aflite_path):
    with closing(dbapi.connect(aflite_path, autocommit=True, no_sync=True)) as connection:
        cursor = connection.cursor()
        for case in json.loads(CASES.read_text()):
            if "sqlstate" in case:
                with pytest.raises(dbapi.DatabaseError) as raised:
                    cursor.execute(case["statement"], case.get("parameters", ()))
                assert raised.value.sqlstate == case["sqlstate"]
            else:
                cursor.execute(case["statement"], case.get("parameters", ()))
                if "rows" in case:
                    assert canonical(cursor.fetchall()) == case["rows"]
        connection.close()


def test_numeric_parameters_and_type_categories(require_native, aflite_path):
    with closing(dbapi.connect(aflite_path, autocommit=True, no_sync=True)) as connection:
        c = connection.cursor()
        c.execute("SELECT CAST(:1 AS BIGINT) AS n, ':2' AS literal -- :3\n", (9007199254740993,))
        assert c.fetchall() == [(9007199254740993, ":2")]
        assert c.description[0][1] == dbapi.NUMBER
        assert c.description[1][1] == dbapi.STRING
        with pytest.raises(dbapi.DataError):
            c.execute("SELECT :1", (float("nan"),))
        with pytest.raises(dbapi.DataError):
            c.execute("SELECT :1", (1 << 63,))


def test_sessions_streaming_and_reopen(require_native, aflite_path):
    a = dbapi.connect(aflite_path, no_sync=True)
    b = dbapi.connect(aflite_path, no_sync=True)
    try:
        c = a.cursor()
        c.execute("CREATE TABLE numbers (n BIGINT)")
        c.execute("INSERT INTO numbers (_id,n) VALUES ($1,$2)", ("keep", 1))
        c.execute("SAVEPOINT point")
        c.execute("INSERT INTO numbers (_id,n) VALUES ($1,$2)", ("discard", 2))
        c.execute("ROLLBACK TO SAVEPOINT point")
        assert c.execute("SELECT n FROM numbers").fetchall() == [(1,)]
        assert b.cursor().execute("SELECT n FROM numbers").fetchall() == []
        b.rollback()
        a.commit()
        assert b.cursor().execute("SELECT n FROM numbers").fetchall() == [(1,)]
        b.rollback()
        c.executemany("INSERT INTO numbers (_id,n) VALUES ($1,$2)", [(f"row:{i:04}", i) for i in range(300)])
        a.commit()
        assert len(c.execute("SELECT n FROM numbers ORDER BY _id").fetchall()) == 301
        a.rollback()
        c.execute("INSERT INTO numbers (_id,n) VALUES ('uncommitted',99)")
    finally:
        a.close()
        b.close()
    connection = dbapi.connect(aflite_path, autocommit=True, no_sync=True)
    try:
        assert len(connection.cursor().execute("SELECT n FROM numbers").fetchall()) == 301
        connection.cursor().execute("DROP TABLE numbers")
        connection.cursor().execute("CREATE TABLE numbers (n BIGINT)")
        assert connection.cursor().execute("SELECT n FROM numbers").fetchall() == []
    finally:
        connection.close()


def test_tables_document_handles(require_native, aflite_path):
    with af.create_with_options(aflite_path, af.OpenOptions(no_sync=True)) as db:
        db.create_table("one", {})
        db.create_table("two", {})
        one = db.open_table("one")
        two = db.open_table("two")
        try:
            one.batch_json({"inserts": {"same": {"name": "one"}}})
            two.batch_json({"inserts": {"same": {"name": "two"}}})
            assert one.lookup("same")["name"] == "one"
            assert two.lookup("same")["name"] == "two"
            assert db.list_tables() == ["default", "one", "two"]
        finally:
            one.close()
            two.close()
        db.drop_table("one")
        db.create_table("one", {})
        with db.open_table("one") as recreated:
            with pytest.raises(af.NotFoundError):
                recreated.lookup("same")


def test_multi_table_stable_snapshot(require_native, aflite_path, tmp_path):
    snapshot = tmp_path / "snapshot.aflite"
    with af.create_with_options(aflite_path, af.OpenOptions(no_sync=True)) as database:
        for name, value in (("one", 1), ("two", 2)):
            database.create_table(name, {})
            with database.open_table(name) as table:
                table.batch_json({"inserts": {"same": {"value": value}}})
                with pytest.raises(af.InvalidArgumentError):
                    table.backup()
        backup = database.backup()
        database.copy_stable_snapshot(str(snapshot))
    with af.open_with_options(snapshot, af.OpenOptions(no_sync=True)) as database:
        assert database.list_tables() == ["default", "one", "two"]
        for name, value in (("one", 1), ("two", 2)):
            with database.open_table(name) as table:
                assert table.lookup("same") == {"value": value}

    for storage, suffix in ((af.Storage.LITE, ".aflite"), (af.Storage.DIRECTORY, "")):
        path = tmp_path / f"portable{suffix}"
        af.restore(path, backup, storage=storage)
        with af.open_with_options(path, af.OpenOptions(storage=storage, no_sync=True)) as database:
            assert database.list_tables() == ["default", "one", "two"]
            for name, value in (("one", 1), ("two", 2)):
                with database.open_table(name) as table:
                    assert table.lookup("same") == {"value": value}


def test_cross_table_foreign_keys_and_failed_transaction(require_native, aflite_path):
    connection = dbapi.connect(aflite_path, autocommit=True, no_sync=True)
    try:
        c = connection.cursor()
        c.execute("CREATE TABLE parents (id BIGINT PRIMARY KEY, name TEXT)")
        c.execute(
            "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES parents(id) ON DELETE CASCADE)"
        )
        c.execute("BEGIN")
        c.execute("INSERT INTO parents (_id,id,name) VALUES ('parent',1,'name')")
        c.execute("INSERT INTO children (_id,id,parent_id) VALUES ('child',1,1)")
        c.execute("COMMIT")
        assert c.execute("SELECT p.name,c.id FROM parents p JOIN children c ON p.id=c.parent_id").fetchall() == [
            ("name", 1)
        ]
        c.execute("BEGIN")
        c.execute("SAVEPOINT before_delete")
        c.execute("DELETE FROM parents WHERE id=1")
        assert c.execute("SELECT id FROM children").fetchall() == []
        c.execute("ROLLBACK TO SAVEPOINT before_delete")
        assert c.execute("SELECT id FROM children").fetchall() == [(1,)]
        c.execute("DELETE FROM parents WHERE id=1")
        c.execute("COMMIT")
        with pytest.raises(dbapi.IntegrityError) as raised:
            c.execute("INSERT INTO children (_id,id,parent_id) VALUES ('orphan',2,42)")
        assert raised.value.sqlstate == "23503"
        c.execute("BEGIN")
        with pytest.raises(dbapi.ProgrammingError):
            c.execute("SELECT n FROM nonexistent")
        with pytest.raises(dbapi.DatabaseError) as raised:
            c.execute("SELECT id FROM children")
        assert raised.value.sqlstate == "25P02"
        c.execute("ROLLBACK")
    finally:
        connection.close()


def test_immediate_unique_constraints_and_upsert(require_native, aflite_path):
    with closing(dbapi.connect(aflite_path, autocommit=True, no_sync=True)) as connection:
        c = connection.cursor()
        c.execute("CREATE TABLE accounts (email TEXT UNIQUE, visits BIGINT)")
        c.execute("BEGIN")
        c.execute("INSERT INTO accounts (_id,email,visits) VALUES ('one','one@example.com',1)")
        c.execute(
            "INSERT INTO accounts (_id,email,visits) VALUES ('two','one@example.com',2) "
            "ON CONFLICT (email) DO UPDATE SET visits=excluded.visits"
        )
        assert c.execute("SELECT visits FROM accounts").fetchall() == [(2,)]

        c.execute("SAVEPOINT valid")
        with pytest.raises(dbapi.IntegrityError) as raised:
            c.execute("INSERT INTO accounts (_id,email,visits) VALUES ('three','one@example.com',3)")
        assert raised.value.sqlstate == "23505"
        c.execute("ROLLBACK TO SAVEPOINT valid")
        c.execute("COMMIT")
        assert c.execute("SELECT visits FROM accounts").fetchall() == [(2,)]


def test_concurrent_updates_report_serialization_failure(require_native, aflite_path):
    first = dbapi.connect(aflite_path, autocommit=True, no_sync=True)
    second = dbapi.connect(aflite_path, autocommit=True, no_sync=True)
    try:
        a, b = first.cursor(), second.cursor()
        a.execute("CREATE TABLE counters (n BIGINT)")
        a.execute("INSERT INTO counters (_id,n) VALUES ('counter',0)")
        a.execute("BEGIN")
        b.execute("BEGIN")
        a.execute("UPDATE counters SET n=n+1 WHERE _id='counter'")
        b.execute("UPDATE counters SET n=n+1 WHERE _id='counter'")
        a.execute("COMMIT")
        with pytest.raises(dbapi.OperationalError) as raised:
            b.execute("COMMIT")
        assert raised.value.sqlstate == "40001"
        b.execute("ROLLBACK")
        assert b.execute("SELECT n FROM counters").fetchall() == [(1,)]
    finally:
        first.close()
        second.close()


def test_directory_database_transactions(require_native, tmp_path):
    path = tmp_path / "directory"
    options = af.OpenOptions(storage=af.Storage.DIRECTORY, no_sync=True)
    with af.open_with_options(path, options) as database:
        database.sql("CREATE TABLE first_table (n BIGINT)")
        database.sql("CREATE TABLE second_table (n BIGINT)")
        session = database.sql_session()
        try:
            session.execute("BEGIN")
            session.execute("INSERT INTO first_table (_id,n) VALUES ('first',1)")
            session.execute("INSERT INTO second_table (_id,n) VALUES ('second',2)")
            session.execute("COMMIT")
        finally:
            session.close()
    with af.open_with_options(path, options) as database:
        assert database.sql("SELECT n FROM first_table")["rows"] == [["1"]]
        assert database.sql("SELECT n FROM second_table")["rows"] == [["2"]]


def test_interrupted_multi_table_commit_recovers_atomically(require_native, tmp_path):
    # Kill separate writers across the prepare/decision window. Reopening must
    # abort an undecided preparation or finish every decided participant.
    child = """
import sys
from antfly_embedded import dbapi
c = dbapi.connect(sys.argv[1], autocommit=True)
q = c.cursor()
q.execute('BEGIN')
for table in ('first_table', 'second_table'):
    q.executemany('INSERT INTO '+table+' (_id,n) VALUES ($1,$2)',
                  [(str(i),i) for i in range(200)])
print('prepared', flush=True)
q.execute('COMMIT')
c.close()
"""
    for attempt, delay in enumerate((0, 0.002, 0.02, 0.1)):
        path = tmp_path / f"recovery-{attempt}.aflite"
        with closing(dbapi.connect(path, autocommit=True)) as connection:
            c = connection.cursor()
            c.execute("CREATE TABLE first_table (n BIGINT)")
            c.execute("CREATE TABLE second_table (n BIGINT)")
        process = subprocess.Popen(
            [sys.executable, "-c", child, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert process.stdout.readline().strip() == "prepared", process.stderr.read()
            try:
                process.communicate(timeout=delay)
                assert process.returncode == 0
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=10)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=10)
        with closing(dbapi.connect(path, autocommit=True)) as connection:
            c = connection.cursor()
            first = c.execute("SELECT n FROM first_table").fetchall()
            second = c.execute("SELECT n FROM second_table").fetchall()
            assert len(first) in (0, 200)
            assert len(second) == len(first)


@pytest.mark.parametrize("source_storage", [af.Storage.LITE, af.Storage.DIRECTORY])
@pytest.mark.parametrize("destination_storage", [af.Storage.LITE, af.Storage.DIRECTORY])
def test_database_backup_preserves_catalog_constraints_and_atomic_restore(
    require_native, tmp_path, source_storage, destination_storage
):
    from antfly_embedded._sql import SQLStateError

    source_path = tmp_path / ("source.aflite" if source_storage == af.Storage.LITE else "source")
    destination_path = tmp_path / ("restored.aflite" if destination_storage == af.Storage.LITE else "restored")
    options = af.OpenOptions(storage=source_storage, no_sync=True)
    opener = af.create_with_options if source_storage == af.Storage.LITE else af.open_with_options
    with opener(source_path, options) as source:
        source.sql("CREATE TABLE parents (id BIGINT PRIMARY KEY, name TEXT UNIQUE)")
        source.sql(
            "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES parents(id) ON DELETE CASCADE)"
        )
        source.sql("CREATE TABLE discarded (n BIGINT)")
        source.drop_table("discarded")
        source.sql("INSERT INTO parents (_id,id,name) VALUES ('parent',1,'first')")
        source.sql("INSERT INTO children (_id,id,parent_id) VALUES ('child',1,1)")
        with closing(source.sql_session()) as session:
            session.execute("BEGIN")
            session.execute("INSERT INTO parents (_id,id,name) VALUES ('pending',2,'pending')")
            backup = source.backup()
            session.execute("ROLLBACK")
        backup_file = tmp_path / "database.afb"
        source.backup_to_file(backup_file)
    af.restore_file(destination_path, backup_file, storage=destination_storage)
    destination_options = af.OpenOptions(storage=destination_storage, no_sync=True)
    readonly_options = af.OpenOptions(storage=destination_storage, mode=af.OpenMode.READONLY)
    with af.open_with_options(destination_path, readonly_options) as readonly:
        assert readonly.list_tables() == ["children", "default", "parents"]
        assert readonly.sql("SELECT id FROM parents")["rows"] == [["1"]]
    with af.open_with_options(destination_path, destination_options) as restored:
        assert restored.list_tables() == ["children", "default", "parents"]
        assert restored.sql("SELECT p.name,c.id FROM parents p JOIN children c ON p.id=c.parent_id")["rows"] == [
            ["first", "1"]
        ]
        with pytest.raises(SQLStateError) as duplicate:
            restored.sql("INSERT INTO parents (_id,id,name) VALUES ('duplicate',3,'first')")
        assert duplicate.value.sqlstate == "23505"
        with pytest.raises(SQLStateError) as orphan:
            restored.sql("INSERT INTO children (_id,id,parent_id) VALUES ('orphan',2,99)")
        assert orphan.value.sqlstate == "23503"
        restored.sql("DELETE FROM parents WHERE id=1")
        assert restored.sql("SELECT id FROM children")["rows"] == []
        restored.sql("CREATE TABLE discarded (n BIGINT)")
        assert restored.sql("SELECT n FROM discarded")["rows"] == []
    # Corruption must leave the previously published database untouched.
    with pytest.raises(af.InvalidArgumentError):
        af.restore(destination_path, backup[:-1] + bytes([backup[-1] ^ 1]), storage=destination_storage, replace=True)
    with af.open_with_options(destination_path, destination_options) as restored:
        assert "discarded" in restored.list_tables()
    empty_path = tmp_path / ("imported.aflite" if destination_storage == af.Storage.LITE else "imported")
    opener = af.create_with_options if destination_storage == af.Storage.LITE else af.open_with_options
    with opener(empty_path, destination_options) as imported:
        imported.import_backup(backup)
        assert imported.list_tables() == ["children", "default", "parents"]
        assert imported.sql("SELECT name FROM parents")["rows"] == [["first"]]


@pytest.mark.parametrize("destination_storage", [af.Storage.LITE, af.Storage.DIRECTORY])
def test_database_backup_rebuilds_indexes_and_enrichments_before_readonly_open(
    require_native, tmp_path, destination_storage
):
    source_path = tmp_path / "indexed.aflite"
    backup_path = tmp_path / "indexed.afb"
    with af.create(source_path, no_sync=True) as source:
        source.create_table("named", {})
        for name in ("default", "named"):
            with source.open_table(name) as table:
                table.batch([af.WriteIntent(key="initial", value=b'{"title":"initial"}')], timestamp=1)
                table.set_schema(
                    {
                        "version": 1,
                        "default_type": "doc",
                        "document_schemas": {"doc": {"schema": {"type": "object", "required": ["title"]}}},
                    }
                )
                table.add_enrichment(
                    {"name": "chunks", "kind": "chunk", "field": "body", "chunk_size": 8, "chunk_overlap": 2}
                )
                for index, kind, config in (
                    ("ft", "full_text", {}),
                    ("dv", "dense_vector", {"field": "embedding", "dims": 2, "metric": "l2_squared", "external": True}),
                    ("sv", "sparse_vector", {"field": "sparse_embedding", "external": True}),
                    ("gr", "graph", {}),
                ):
                    table.add_index({"name": index, "kind": kind, "config_json": json.dumps(config)})
                document = {
                    "title": name,
                    "body": "full text search hybrid alpha",
                    "_embeddings": {"dv": [1.0, 0.0], "sv": {"indices": [7, 42], "values": [1.5, 0.5]}},
                    "_edges": {"gr": {"links": [{"target": "related", "weight": 1.0}]}},
                }
                table.batch(
                    [
                        af.WriteIntent(key="indexed", value=json.dumps(document).encode()),
                        af.WriteIntent(key="related", value=b'{"title":"related"}'),
                    ],
                    timestamp=2,
                )
                table.run_until_idle()
        txn_id = b"0123456789abcdef"
        source.begin_transaction(txn_id, 3)
        source.write_transaction(txn_id, [af.WriteIntent(key="pending", value=b'{"title":"pending"}')])
        source.backup_to_file(backup_path)
        source.resolve_transaction(txn_id, af.TxnStatus.ABORTED, 0)
    destination = tmp_path / ("indexed-copy.aflite" if destination_storage == af.Storage.LITE else "indexed-copy")
    af.restore_file(destination, backup_path, storage=destination_storage)
    with af.open_with_options(
        destination, af.OpenOptions(storage=destination_storage, mode=af.OpenMode.READONLY)
    ) as restored:
        for name in ("default", "named"):
            with restored.open_table(name) as table:
                assert table.lookup("indexed")["title"] == name
                with pytest.raises(af.NotFoundError):
                    table.lookup("pending")
                assert "chunks" in json.dumps(table.list_enrichments())
                for index, query in (("dv", [1.0, 0.0]), ("sv", {"indices": [7, 42], "values": [1.5, 0.5]})):
                    result = table.search({"embeddings": {index: query}, "indexes": [index], "limit": 1})
                    assert result["responses"][0]["hits"]["hits"][0]["_id"] == "indexed"
                result = table.search(
                    {
                        "mode": "full_text",
                        "index_name": "ft",
                        "text_query_type": "match",
                        "field": "body",
                        "text": "hybrid alpha",
                        "limit": 5,
                    }
                )
                assert result["total_hits"] >= 1
