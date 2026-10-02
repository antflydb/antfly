#!/usr/bin/env python3
"""Exercise Loadscape SQL regressions against a disposable standalone server.

Usage: python3 scripts/ci/test_sql_loadscape_regressions.py --antfly zig/zig-out/bin/antfly
Add --engine lite to exercise served Lite. Uses only the Python standard library.
"""

import argparse
import json
import pathlib
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


class Server:
    def __init__(self, binary, engine, directory):
        self.engine = engine
        self.directory = pathlib.Path(directory)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/db/v1/sql"
        self.command = [
            str(binary),
            "standalone",
            "--data-dir",
            str(self.directory),
            "--port",
            str(self.port),
            "--health=false",
        ]
        if engine == "lite":
            self.command += [
                "--storage-engine=lite",
                "--storage-path",
                str(self.directory / "store.aflite"),
            ]
        self.process = None
        self.log = None

    def start(self):
        self.log = open(self.directory / "server.log", "a")
        self.process = subprocess.Popen(self.command, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(
                    f"server exited: {(self.directory / 'server.log').read_text()}"
                )
            try:
                self.sql("SELECT 1")
                return
            except (OSError, urllib.error.URLError):
                time.sleep(0.1)
        raise AssertionError("server readiness timed out")

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None

    def sql(self, statement, *, session=None, code=None, parameters=()):
        body = {"statement": statement, "parameters": list(parameters)}
        if session:
            body["session_id"] = session
        request = urllib.request.Request(
            self.url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                status, result = response.status, json.load(response)
        except urllib.error.HTTPError as error:
            status, result = error.code, json.load(error)
        if code:
            assert status >= 400 and result.get("code") == code, (
                statement,
                status,
                result,
            )
        else:
            assert status < 400, (statement, status, result)
        return result


def exercise(server):
    sql = server.sql
    sql("CREATE TABLE regression (n BIGINT, t TIMESTAMPTZ, j JSONB, u UUID)")
    uuid = "550e8400-e29b-41d4-a716-446655440000"
    sql(
        "INSERT INTO regression (_id,n,t,j,u) VALUES "
        f"('a',1,'2026-01-01T00:00:00Z',CAST('null' AS JSONB),CAST('{uuid}' AS UUID)),"
        "('b',2,NULL,NULL,NULL)"
    )
    sql(
        "INSERT INTO regression (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET n=EXCLUDED.n"
    )
    for statement in (
        "WITH q AS (SELECT n FROM regression) SELECT n FROM q ORDER BY n",
        "SELECT n FROM (SELECT n FROM regression) q ORDER BY n",
        "SELECT a.n FROM regression a JOIN regression b ON a._id=b._id ORDER BY a.n",
        "SELECT n FROM regression INTERSECT SELECT n FROM regression ORDER BY n",
    ):
        assert sql(statement)["rows"] == [["2"], ["9"]], statement
    assert sql("SELECT n FROM regression WHERE j IS NULL")["rows"] == [["2"]]
    assert sql("SELECT n FROM regression WHERE j IS NOT NULL")["rows"] == [["9"]]
    assert sql(f"SELECT n FROM regression WHERE u=CAST('{uuid}' AS UUID)")["rows"] == [
        ["9"]
    ]
    sql("UPDATE regression SET n=b.n FROM regression b WHERE regression._id=b._id")
    sql(
        "DELETE FROM regression USING regression b WHERE regression._id=b._id AND b.n=99"
    )
    for isolation in (
        "",
        " ISOLATION LEVEL READ COMMITTED",
        " ISOLATION LEVEL REPEATABLE READ",
        " ISOLATION LEVEL SERIALIZABLE",
    ):
        session = sql("BEGIN" + isolation)["session_id"]
        sql("UPDATE regression SET n=n+1 WHERE _id='a'", session=session)
        sql("COMMIT", session=session)
    sql("INSERT INTO regression (_id,n) VALUES ('a',1)", code="23505")
    session = sql("BEGIN")["session_id"]
    sql("DELETE FROM regression WHERE _id='b'", session=session)
    sql("INSERT INTO regression (_id,n) VALUES ('b',2)", session=session)
    sql("COMMIT", session=session)
    sql("CREATE TABLE unique_rows (label TEXT)")
    sql("INSERT INTO unique_rows (_id,label) VALUES ('first','same')")
    sql("CREATE UNIQUE INDEX unique_label ON unique_rows(label)")
    deadline = time.monotonic() + 60
    while True:
        with urllib.request.urlopen(
            server.url.removesuffix("/sql") + "/tables/unique_rows/constraints/status",
            timeout=90,
        ) as response:
            coverage = json.load(response)
        assert coverage["state"] != "invalid", coverage
        if coverage["state"] == "enforced":
            break
        assert time.monotonic() < deadline, coverage
        time.sleep(0.2)
    sql("INSERT INTO unique_rows (_id,label) VALUES ('second','same')", code="23505")
    assert sql("SELECT _id FROM unique_rows")["rows"] == [["first"]]
    sql("CREATE TABLE checked (n BIGINT CHECK (n>0))")
    sql("INSERT INTO checked (_id,n) VALUES ('bad',-1)", code="23514")
    sql("SELECT 1 +", code="42601")
    sql("SELECT * FROM absent", code="42P01")
    sql("DROP TABLE checked")
    sql("SELECT * FROM checked", code="42P01")
    if server.engine == "local":
        # Assert values after publication; an admission receipt alone is insufficient.
        # Exercise multiple historical layouts and a changed preexisting
        # DEFAULT while a fresh-generation rewrite is being admitted.
        sql("CREATE TABLE defaults_history (n BIGINT)")
        sql("INSERT INTO defaults_history (_id,n) VALUES ('oldest',1)")
        sql("ALTER TABLE defaults_history ADD COLUMN old_col BIGINT")
        sql("ALTER TABLE defaults_history ALTER COLUMN old_col SET DEFAULT 7")
        sql("INSERT INTO defaults_history (_id,n,old_col) VALUES ('newer',2,NULL)")
        assert sql("SELECT old_col FROM defaults_history ORDER BY _id")[
            "sql_nulls"
        ] == [[True], [True]]
        sql("ALTER TABLE defaults_history ADD COLUMN new_col BIGINT DEFAULT 9")
        deadline = time.monotonic() + 60
        while True:
            try:
                history = sql(
                    "SELECT old_col,new_col FROM defaults_history ORDER BY _id"
                )
                assert history["sql_nulls"] == [[True, False], [True, False]], history
                assert [row[1] for row in history["rows"]] == ["9", "9"], history
                break
            except AssertionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        sql("INSERT INTO defaults_history (_id,n) VALUES ('future',3)")
        assert sql("SELECT old_col,new_col FROM defaults_history WHERE _id='future'")[
            "rows"
        ] == [["7", "9"]]
        sql("ALTER TABLE regression ADD COLUMN flag BIGINT DEFAULT 3")
        deadline = time.monotonic() + 60
        while True:
            try:
                result = sql("SELECT flag FROM regression ORDER BY _id")
                assert result["rows"] == [["3"], ["3"]], result
                break
            except AssertionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
    before = sql("SELECT t,j FROM regression ORDER BY _id")
    assert before["sql_nulls"] == [[False, False], [True, True]], before
    request = urllib.request.Request(
        server.url.removesuffix("/sql") + "/tables/regression/documents",
        data=b'{"limit":100}',
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        assert response.status == 200
    server.stop()
    server.start()
    if server.engine == "local":
        history = sql(
            "SELECT old_col,new_col FROM defaults_history WHERE _id <> 'future' ORDER BY _id"
        )
        assert history["sql_nulls"] == [[True, False], [True, False]], history
        assert [row[1] for row in history["rows"]] == ["9", "9"], history
    assert sql("SELECT t,j FROM regression ORDER BY _id") == before
    print("SQL Loadscape regression checks passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly", type=pathlib.Path, required=True)
    parser.add_argument("--engine", choices=("local", "lite"), default="local")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="antfly-sql-regressions-") as directory:
        server = Server(args.antfly.resolve(), args.engine, directory)
        try:
            server.start()
            exercise(server)
        finally:
            server.stop()


if __name__ == "__main__":
    main()
