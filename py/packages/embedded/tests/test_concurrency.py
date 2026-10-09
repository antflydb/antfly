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

"""Concurrency and threading-contract tests, mirroring
go/pkg/embedded/concurrency_cgo_test.go.

libantfly is ANTFLY_THREADING_SERIALIZED: any thread may call any function
on a handle concurrently. Writes on one handle queue behind each other
instead of failing with Busy; Close waits for in-flight calls; calls after
Close raise InvalidArgumentError.
"""

from __future__ import annotations

import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import antfly_embedded
from antfly_embedded._sql import SQLStateError

pytestmark = pytest.mark.usefixtures("require_native")


def test_threading_mode_is_serialized(tmp_path: Path) -> None:
    assert antfly_embedded.threading_mode() == antfly_embedded.THREADING_SERIALIZED
    with antfly_embedded.create(tmp_path / "threading.aflite", no_sync=True) as db:
        caps = db.capabilities()
        assert caps["threading"] == "serialized"


def test_concurrent_calls_on_one_handle(tmp_path: Path) -> None:
    db = antfly_embedded.create(tmp_path / "concurrent.aflite", no_sync=True)
    try:
        writers, writes_per_writer, readers = 4, 25, 4
        errors_q: queue.Queue[str] = queue.Queue()
        stop = threading.Event()
        counter_lock = threading.Lock()
        counter = {"value": 0}

        def next_timestamp() -> int:
            with counter_lock:
                counter["value"] += 1
                return counter["value"]

        def run_writer(w: int) -> None:
            for i in range(writes_per_writer):
                key = f"doc:w{w}:{i}"
                value = f'{{"body":"concurrent writer {w} item {i}"}}'.encode()
                try:
                    db.batch([antfly_embedded.WriteIntent(key=key, value=value)], next_timestamp())
                except Exception as exc:  # noqa: BLE001
                    errors_q.put(f"batch {key}: {exc!r}")
                    return

        search_request = {"full_text_search": {"match": {"field": "body", "text": "concurrent writer"}}, "limit": 5}
        scan_request = {"from": "doc:w", "to": "doc:x", "include_documents": True, "limit": 20}

        def run_reader() -> None:
            while not stop.is_set():
                try:
                    db.search(search_request)
                    db.stats()
                    db.scan(scan_request)
                    try:
                        db.lookup("doc:w0:0")
                    except antfly_embedded.NotFoundError:
                        pass
                except Exception as exc:  # noqa: BLE001
                    errors_q.put(f"reader: {exc!r}")
                    return

        def run_maintainer() -> None:
            while not stop.is_set():
                try:
                    db.run_until_idle()
                    db.delete_index("no_such_index")
                except Exception as exc:  # noqa: BLE001
                    errors_q.put(f"maintainer: {exc!r}")
                    return

        writer_threads = [threading.Thread(target=run_writer, args=(w,)) for w in range(writers)]
        reader_threads = [threading.Thread(target=run_reader) for _ in range(readers)]
        maintainer_thread = threading.Thread(target=run_maintainer)

        for t in writer_threads:
            t.start()
        for t in reader_threads:
            t.start()
        maintainer_thread.start()

        for t in writer_threads:
            t.join()
        stop.set()
        for t in reader_threads:
            t.join()
        maintainer_thread.join()

        collected = []
        while not errors_q.empty():
            collected.append(errors_q.get())
        assert not collected, "\n".join(collected)

        db.run_until_idle()
        for w in range(writers):
            for i in range(writes_per_writer):
                key = f"doc:w{w}:{i}"
                got = db.lookup(key, raw=True)
                assert f"item {i}".encode() in got, f"lookup {key} = {got!r}"
    finally:
        db.close()


def test_close_races_in_flight_calls(tmp_path: Path) -> None:
    db = antfly_embedded.create(tmp_path / "close-race.aflite", no_sync=True)
    db.batch([antfly_embedded.WriteIntent(key="doc:close", value=b'{"body":"close race"}')], 1)

    closed_seen = {"count": 0}
    seen_lock = threading.Lock()
    unexpected: list[str] = []

    def run_reader() -> None:
        while True:
            try:
                db.lookup("doc:close")
            except antfly_embedded.InvalidArgumentError:
                with seen_lock:
                    closed_seen["count"] += 1
                return
            except Exception as exc:  # noqa: BLE001
                unexpected.append(repr(exc))
                return

    reader_threads = [threading.Thread(target=run_reader) for _ in range(8)]
    for t in reader_threads:
        t.start()
    time.sleep(0.02)

    closer_threads = [threading.Thread(target=db.close) for _ in range(3)]
    for t in closer_threads:
        t.start()
    for t in closer_threads:
        t.join()
    for t in reader_threads:
        t.join()

    assert not unexpected, "\n".join(unexpected)
    assert closed_seen["count"] == 8, f"{closed_seen['count']} of 8 readers observed the closed handle"
    with pytest.raises(antfly_embedded.InvalidArgumentError):
        db.stats()


@pytest.mark.parametrize("no_sync", [False, True])
def test_independent_writable_connections(tmp_path: Path, no_sync: bool) -> None:
    path = tmp_path / "connections.aflite"
    with antfly_embedded.create(path, no_sync=no_sync) as first:
        with antfly_embedded.open(path, no_sync=no_sync) as second:
            first.batch_json({"inserts": {"first": {"body": "first"}}})
            assert second.lookup("first")["body"] == "first"
            second.batch_json({"inserts": {"second": {"body": "second"}}})
            assert first.lookup("second")["body"] == "second"
            first.close()
            second.batch_json({"inserts": {"after": {"body": "still open"}}})
            assert second.lookup("first")["body"] == "first"


def test_readonly_connection_observes_new_commits(tmp_path: Path) -> None:
    path = tmp_path / "readonly.aflite"
    with antfly_embedded.create(path, no_sync=True) as writer:
        writer.batch_json({"inserts": {"item": {"body": "before"}}})
        with antfly_embedded.open(path, mode=antfly_embedded.OpenMode.READONLY) as reader:
            assert reader.lookup("item")["body"] == "before"
            writer.batch_json({"inserts": {"item": {"body": "after"}}})
            assert reader.lookup("item")["body"] == "after"
            writer.create_table("new_table", {})
            assert "new_table" in reader.list_tables()


def test_readonly_snapshot_without_sidecar_in_readonly_directory(tmp_path: Path) -> None:
    source = tmp_path / "source.aflite"
    with antfly_embedded.create(source, no_sync=True) as db:
        db.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
        db.sql("INSERT INTO items (id,name) VALUES (1, 'before')")
        db.run_until_idle()
    directory = tmp_path / "readonly"
    directory.mkdir()
    snapshot = directory / "snapshot.aflite"
    shutil.copyfile(source, snapshot)
    directory.chmod(0o555)
    try:
        with antfly_embedded.open(snapshot, mode=antfly_embedded.OpenMode.READONLY) as reader:
            assert reader.sql("SELECT name FROM items")["rows"] == [["before"]]
            assert list(directory.iterdir()) == [snapshot]
            cursor = reader.sql_cursor("SELECT name FROM items")
            try:
                # Installing a sidecar must respect existing inode readers
                # when the snapshot subsequently becomes writable.
                directory.chmod(0o755)
                with antfly_embedded.open(snapshot, no_sync=True) as writer:
                    with pytest.raises(antfly_embedded.BusyError):
                        writer.sql("UPDATE items SET name = 'after' WHERE id = 1")
                    # A second reader must not switch lock authorities while
                    # the first reader still owns the fallback inode fence.
                    with antfly_embedded.open(snapshot, mode=antfly_embedded.OpenMode.READONLY) as second:
                        assert second.sql("SELECT name FROM items")["rows"] == [["before"]]
                        assert list(directory.iterdir()) == [snapshot]
                        with pytest.raises(antfly_embedded.BusyError):
                            writer.sql("UPDATE items SET name = 'after' WHERE id = 1")
                    assert cursor.fetch(10)["result"]["rows"] == [["before"]]
                    cursor.close()
                    reader.close()
                    writer.sql("UPDATE items SET name = 'after' WHERE id = 1")
                    with antfly_embedded.open(snapshot, mode=antfly_embedded.OpenMode.READONLY) as reopened:
                        assert reopened.sql("SELECT name FROM items")["rows"] == [["after"]]
            finally:
                cursor.close()
    finally:
        directory.chmod(0o755)


def test_table_handle_rebinds_and_rejects_recreated_table(tmp_path: Path) -> None:
    path = tmp_path / "table-connections.aflite"
    with antfly_embedded.create(path, no_sync=True) as first:
        first.create_table("items", {})
        with first.open_table("items") as table:
            with antfly_embedded.open(path, no_sync=True) as second:
                with second.open_table("items") as other:
                    other.batch_json({"inserts": {"item": {"body": "external"}}})
                assert table.lookup("item")["body"] == "external"
                table.batch_json({"inserts": {"local": {"body": "local"}}})
                with second.open_table("items") as other:
                    assert other.lookup("local")["body"] == "local"
                second.drop_table("items")
                second.create_table("items", {})
                with pytest.raises(antfly_embedded.InvalidArgumentError):
                    table.lookup("item")


def test_stale_table_handle_does_not_block_unrelated_drop(tmp_path: Path) -> None:
    path = tmp_path / "table-drop-identity.aflite"
    # Manual maintenance keeps refreshes driven by the calls below.
    options = antfly_embedded.OpenOptions(no_sync=True, profile=antfly_embedded.Profile.HOSTED)
    with antfly_embedded.create_with_options(path, options) as first:
        first.create_table("foo", {})
        with first.open_table("foo") as foo:
            with antfly_embedded.open_with_options(path, options) as second:
                second.create_table("bar", {})
                first.list_tables()
                second.batch_json({"inserts": {"touch": {"version": 1}}})
                first.list_tables()
                # Do not call foo before DROP: its cached DB pointer still
                # names the original generation, which has now been retired.
                first.drop_table("bar")
                assert first.list_tables() == ["default", "foo"]
                with pytest.raises(SQLStateError) as error:
                    first.sql("DROP TABLE foo")
                assert error.value.sqlstate == "53300"
                foo.stats()
        first.drop_table("foo")
        assert first.list_tables() == ["default"]


def test_idle_connections_allow_vacuum_and_refresh_replacement(tmp_path: Path) -> None:
    path = tmp_path / "vacuum-connections.aflite"
    with antfly_embedded.create(path, no_sync=True) as first:
        first.batch_json({"inserts": {"item": {"body": "before"}}})
        with antfly_embedded.open(path, no_sync=True) as second:
            with antfly_embedded.open(path, mode=antfly_embedded.OpenMode.READONLY) as reader:
                assert reader.lookup("item")["body"] == "before"
                first.vacuum()
                assert second.lookup("item")["body"] == "before"
                second.batch_json({"inserts": {"item": {"body": "after"}}})
                assert first.lookup("item")["body"] == "after"
                assert reader.lookup("item")["body"] == "after"


def _child_environment() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parent.parent / "src")
    return env


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX kernel lock fixture")
def test_busy_timeout_applies_to_operations_not_open(tmp_path: Path) -> None:
    path = tmp_path / "busy-timeout.aflite"
    with antfly_embedded.create(path, no_sync=True):
        pass
    locker = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            "import fcntl,sys; f=open(sys.argv[1], 'r+'); "
            "fcntl.flock(f, fcntl.LOCK_EX); print('locked', flush=True); sys.stdin.readline()",
            str(path) + ".lock",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert locker.stdout.readline().strip() == "locked"
        with antfly_embedded.open(path, no_sync=True) as immediate:
            with pytest.raises(antfly_embedded.BusyError):
                immediate.batch_json({"inserts": {"blocked": {"body": "blocked"}}})
        with antfly_embedded.open(path, no_sync=True, busy_timeout=0.15) as timed:
            start = time.monotonic()
            with pytest.raises(antfly_embedded.BusyError):
                timed.batch_json({"inserts": {"blocked": {"body": "blocked"}}})
            assert time.monotonic() - start >= 0.14
        with antfly_embedded.open(path, no_sync=True, busy_timeout=5.0) as waiting:

            def release() -> None:
                time.sleep(0.1)
                locker.stdin.write("release\n")
                locker.stdin.flush()

            release_thread = threading.Thread(target=release)
            release_thread.start()
            waiting.batch_json({"inserts": {"after": {"body": "committed"}}})
            release_thread.join()
            assert waiting.lookup("after")["body"] == "committed"
    finally:
        if locker.poll() is None:
            locker.stdin.close()
        locker.wait(timeout=5)


def test_cross_process_connections_refresh_documents_and_search(tmp_path: Path) -> None:
    path = tmp_path / "processes.aflite"
    with antfly_embedded.create(path, no_sync=True, busy_timeout=5.0) as parent:
        parent.batch_json({"inserts": {"parent": {"body": "parent initial"}}})
        script = (
            "import antfly_embedded,sys; "
            "db=antfly_embedded.open(sys.argv[1], no_sync=True, busy_timeout=5.0); "
            "assert db.lookup('parent')['body']=='parent initial'; "
            "db.batch_json({'inserts':{'child':{'body':'child publication'}}}); "
            "db.run_until_idle(); db.close()"
        )
        subprocess.run([sys.executable, "-c", script, str(path)], env=_child_environment(), check=True, timeout=60)
        assert parent.lookup("child")["body"] == "child publication"
        parent.batch_json({"inserts": {"after": {"body": "parent after child"}}})
        parent.run_until_idle()
        assert parent.lookup("parent")["body"] == "parent initial"
        assert parent.lookup("child")["body"] == "child publication"
        result = parent.search({"full_text_search": {"match": {"field": "body", "text": "publication"}}, "limit": 10})
        assert "child" in str(result)


@pytest.mark.parametrize("table_name", ["default", "named"])
@pytest.mark.parametrize("open_kind", ["cold", "refresh", "deferred"])
def test_background_enrichment_resumes_durable_work(tmp_path: Path, table_name: str, open_kind: str) -> None:
    path = tmp_path / "enrichment-recovery.aflite"
    options = antfly_embedded.OpenOptions(no_sync=True, local_runtime_configured=True, busy_timeout=5.0)
    with antfly_embedded.create_with_options(path, options) as db:
        if table_name != "default":
            db.create_table(table_name, {})
    # Keep an existing generation for the refresh case. Otherwise reopen only
    # after the producer exits, exercising recovery on a cold connection.
    parent = antfly_embedded.open_with_options(path, options) if open_kind == "refresh" else None
    locker = None
    script = """
import antfly_embedded as af, json, os, sys
db = af.open_with_options(sys.argv[1], af.OpenOptions(no_sync=True, local_runtime_configured=True, busy_timeout=5.0))
table = db if sys.argv[2] == 'default' else db.open_table(sys.argv[2])
table.add_index({'name': 'semantic', 'kind': 'dense_vector', 'config_json': json.dumps({
    'field': 'embedding', 'dims': 3, 'metric': 'l2_squared',
    'generator': {'kind': 'dense_embedding', 'source_field': 'body', 'embedding_name': 'dense_v1'},
})})
table.batch_json({'inserts': {'doc': {'body': 'durable generated work'}}, 'sync_level': 'write'})
# Preserve the durable journal without relying on graceful drain/close.
os._exit(0)
"""
    try:
        subprocess.run(
            [sys.executable, "-c", script, str(path), table_name], env=_child_environment(), check=True, timeout=60
        )
        if open_kind == "deferred":
            locker = subprocess.Popen(
                [
                    sys.executable,
                    "-u",
                    "-c",
                    "import fcntl,sys; f=open(sys.argv[1], 'r+'); "
                    "fcntl.flock(f, fcntl.LOCK_EX); print('locked', flush=True); sys.stdin.readline()",
                    str(path) + ".lock",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            assert locker.stdout.readline().strip() == "locked"
            parent = antfly_embedded.open_with_options(path, options)
            locker.stdin.close()
            locker.wait(timeout=5)
        elif parent is None:
            parent = antfly_embedded.open_with_options(path, options)
        # Observe through a read-only connection: calling the parent would
        # initialize a deferred generation and hide an idle worker regression.
        observer_options = antfly_embedded.OpenOptions(
            no_sync=True, local_runtime_configured=True, mode=antfly_embedded.OpenMode.READONLY, busy_timeout=5.0
        )
        with antfly_embedded.open_with_options(path, observer_options) as observer:
            deadline = time.monotonic() + 10
            while True:
                table = observer if table_name == "default" else observer.open_table(table_name)
                try:
                    enrichment = table.stats()["enrichment"]
                finally:
                    if table is not observer:
                        table.close()
                if enrichment["applied_sequence"] >= 1:
                    break
                assert time.monotonic() < deadline, enrichment
                time.sleep(0.05)
            assert enrichment["applied_sequence"] >= enrichment["target_sequence"]
    finally:
        if locker is not None:
            if locker.poll() is None:
                locker.stdin.close()
            locker.wait(timeout=5)
        if parent is not None:
            parent.close()


def test_cross_process_dense_checkpoints_refresh_search(tmp_path: Path) -> None:
    path = tmp_path / "dense-processes.aflite"
    with antfly_embedded.create(path, no_sync=True, busy_timeout=5.0) as parent:
        parent.add_index(
            {
                "name": "dv",
                "kind": "dense_vector",
                "config_json": '{"field":"embedding","dims":2,"metric":"l2_squared","external":true}',
            }
        )
        parent.batch(
            [antfly_embedded.WriteIntent(key="east", value=b'{"title":"east","_embeddings":{"dv":[1,0]}}')],
            timestamp=1,
        )
        parent.run_until_idle()
        script = (
            "import antfly_embedded,sys; "
            "db=antfly_embedded.open(sys.argv[1], no_sync=True, busy_timeout=5.0); "
            "db.batch([antfly_embedded.WriteIntent(key='north', "
            'value=b\'{"title":"north","_embeddings":{"dv":[0,1]}}\')],timestamp=2); '
            "db.run_until_idle(); db.close()"
        )
        subprocess.run([sys.executable, "-c", script, str(path)], env=_child_environment(), check=True, timeout=60)
        result = parent.search({"embeddings": {"dv": [0.1, 0.9]}, "indexes": ["dv"], "limit": 1})
        assert "north" in str(result)
        parent.batch(
            [antfly_embedded.WriteIntent(key="middle", value=b'{"title":"middle","_embeddings":{"dv":[0.5,0.5]}}')],
            timestamp=3,
        )
        parent.run_until_idle()
        result = parent.search({"embeddings": {"dv": [0.5, 0.5]}, "indexes": ["dv"], "limit": 3})
        assert all(key in str(result) for key in ("east", "north", "middle"))


def test_cursors_retain_snapshots_across_external_commits_and_refresh(tmp_path: Path) -> None:
    path = tmp_path / "cursor.aflite"
    with antfly_embedded.create(path, no_sync=True, busy_timeout=5.0) as first:
        with antfly_embedded.open(path, no_sync=True, busy_timeout=5.0) as second:
            first.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
            first.sql("INSERT INTO items (id,name) VALUES (1, 'before'), (2, 'second')")
            cursor = first.sql_cursor("SELECT id, name FROM items")
            try:
                second.sql("UPDATE items SET name = 'after' WHERE id = 1")
                assert first.sql("SELECT name FROM items WHERE id = 1")["rows"] == [["after"]]
                # first has reopened, but this cursor borrows its old runtime.
                old_rows = cursor.fetch(1)["result"]["rows"]
                assert len(old_rows) == 1
                second.sql("INSERT INTO items (id,name) VALUES (3, 'new')")
                old_rows.extend(cursor.fetch(10)["result"]["rows"])
                assert sorted(old_rows) == [["1", "before"], ["2", "second"]]
            finally:
                cursor.close()
            assert len(first.sql("SELECT id FROM items")["rows"]) == 3


def test_sql_sessions_keep_uncommitted_rows_private_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "transactions.aflite"
    with antfly_embedded.create(path, no_sync=True, busy_timeout=5.0) as first:
        with antfly_embedded.open(path, no_sync=True, busy_timeout=5.0) as second:
            first.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
            session = first.sql_session()
            try:
                session.execute("BEGIN")
                session.execute("INSERT INTO items (id,name) VALUES (1, 'pending')")
                assert second.sql("SELECT id FROM items")["rows"] == []
                second.sql("INSERT INTO items (id,name) VALUES (2, 'other')")
                # Reopening first's cache must preserve its session postimages.
                assert len(session.execute("SELECT id FROM items")["rows"]) == 2
                session.execute("COMMIT")
                assert len(second.sql("SELECT id FROM items")["rows"]) == 2
                session.execute("BEGIN")
                session.execute("INSERT INTO items (id,name) VALUES (3, 'aborted')")
                session.execute("ROLLBACK")
                assert len(second.sql("SELECT id FROM items")["rows"]) == 2
            finally:
                session.close()


def test_session_cannot_commit_into_recreated_table(tmp_path: Path) -> None:
    path = tmp_path / "session-identity.aflite"
    with antfly_embedded.create(path, no_sync=True) as first:
        with antfly_embedded.open(path, no_sync=True) as second:
            first.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
            session = first.sql_session()
            try:
                session.execute("BEGIN")
                session.execute("INSERT INTO items (id,name) VALUES (1, 'old table')")
                second.sql("DROP TABLE items")
                second.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
                with pytest.raises(SQLStateError) as error:
                    session.execute("COMMIT")
                assert error.value.sqlstate == "40001"
                session.execute("ROLLBACK")
                assert second.sql("SELECT id FROM items")["rows"] == []
            finally:
                session.close()


@pytest.mark.parametrize("streaming", [False, True])
def test_session_cannot_read_staged_rows_into_recreated_table(tmp_path: Path, streaming: bool) -> None:
    path = tmp_path / "session-read-identity.aflite"
    with antfly_embedded.create(path, no_sync=True) as first:
        with antfly_embedded.open(path, no_sync=True) as second:
            first.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
            session = first.sql_session()
            try:
                session.execute("BEGIN")
                session.execute("INSERT INTO items (id,name) VALUES (1, 'old table')")
                second.sql("DROP TABLE items")
                second.sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)")
                with pytest.raises(SQLStateError) as error:
                    if streaming:
                        cursor = session.open_cursor("SELECT id, name FROM items")
                        try:
                            cursor.fetch(10)
                        finally:
                            cursor.close()
                    else:
                        session.execute("SELECT id, name FROM items")
                assert error.value.sqlstate == "40001"
                session.execute("ROLLBACK")
                assert session.execute("SELECT id, name FROM items")["rows"] == []
            finally:
                session.close()


def test_cross_process_writers_do_not_lose_each_others_rows(tmp_path: Path) -> None:
    path = tmp_path / "writers.aflite"
    with antfly_embedded.create(path, no_sync=True, busy_timeout=10.0) as parent:
        script = """
import antfly_embedded, sys
with antfly_embedded.open(sys.argv[1], no_sync=True, busy_timeout=10.0) as db:
    for i in range(8):
        db.batch_json({'inserts': {f'{sys.argv[2]}:{i}': {'body': f'writer {sys.argv[2]} item {i}'}}})
"""
        workers = [
            subprocess.Popen([sys.executable, "-c", script, str(path), str(i)], env=_child_environment())
            for i in range(2)
        ]
        try:
            for worker in workers:
                assert worker.wait(timeout=90) == 0
            for writer in range(2):
                for i in range(8):
                    assert parent.lookup(f"{writer}:{i}")["body"] == f"writer {writer} item {i}"
        finally:
            for worker in workers:
                if worker.poll() is None:
                    worker.terminate()
                    worker.wait(timeout=5)
