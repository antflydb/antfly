# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Native cursor qualification against the actual standalone query owners."""

import json
import os
from pathlib import Path
import socket
import shutil
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest


@pytest.mark.parametrize("vector_storage", ["primary_lsm", "vector_store"])
def test_mutable_native_cursor_survives_updates_deletes_and_restart(
    tmp_path, vector_storage
):
    binary = os.environ.get("ANTFLY_NATIVE_BINARY")
    if not binary:
        pytest.skip("set ANTFLY_NATIVE_BINARY for native HTTP qualification")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}/db/v1"
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "storage": {
                    "engine": "local",
                    "local": {"base_dir": str(tmp_path / "data")},
                }
            }
        )
    )
    log_path = tmp_path / "server.log"
    log = log_path.open("w")
    process = None

    def call(method, path, body=None):
        request = Request(
            endpoint + path,
            None if body is None else json.dumps(body).encode(),
            {"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urlopen(request, timeout=30) as response:
                return None if response.status == 204 else json.load(response)
        except HTTPError as error:
            error.add_note(error.read().decode(errors="replace"))
            raise

    def start():
        nonlocal process
        process = subprocess.Popen(
            [
                str(Path(binary).resolve()),
                "standalone",
                "--config",
                str(config),
                "--data-dir",
                str(tmp_path / "data"),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--health",
                "false",
                "--auth",
                "false",
                "--models-dir",
                str(tmp_path / "models"),
            ],
            stdout=log,
            stderr=log,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            assert process.poll() is None, log_path.read_text()[-6000:]
            try:
                call("GET", "/tables")
                return
            except (URLError, HTTPError):
                time.sleep(0.1)
        pytest.fail("daemon did not start")

    def stop():
        process.terminate()
        try:
            process.wait(20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def response(body):
        return call("POST", "/tables/current/query", body)["responses"][0]

    try:
        start()
        call(
            "POST",
            "/tables/current",
            {
                "storage": {"dense_embeddings": vector_storage},
                "schema": {
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "amount": {
                                        "type": "integer",
                                        "x-antfly-field": {
                                            "type": "numeric",
                                            "sortable": True,
                                        },
                                    },
                                    "body": {"type": "string"},
                                },
                            }
                        }
                    },
                },
                "indexes": {"body_text": {"type": "full_text"}},
            },
        )
        call(
            "POST",
            "/tables/current/batch",
            {
                "inserts": {
                    f"doc-{i}": {"amount": i, "body": "original searchable"}
                    for i in range(1, 5)
                },
                "sync_level": "full_index",
            },
        )
        query = {
            "full_text_search": {"match": "original", "field": "body"},
            "order_by": [{"field": "amount"}],
            "limit": 1,
        }
        first = response(query)
        assert first["hits"]["hits"][0]["_source"]["amount"] == 1, first
        assert first["remote_snapshot"].startswith("native2:"), first
        page = dict(
            query,
            limit=10,
            remote_snapshot=first["remote_snapshot"],
            search_after=first["hits"]["hits"][0]["_sort"],
        )
        score_query = dict(query, order_by=[{"field": "_score", "desc": True}])
        score_first = response(score_query)
        assert score_first["hits"]["hits"][0]["_source"]["amount"] == 1
        score_page = dict(
            score_query,
            limit=10,
            remote_snapshot=score_first["remote_snapshot"],
            search_after=score_first["hits"]["hits"][0]["_sort"],
        )
        call(
            "POST",
            "/tables/history",
            {
                "storage": {"dense_embeddings": vector_storage},
                "schema": {
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "amount": {
                                        "type": "integer",
                                        "x-antfly-field": {
                                            "type": "numeric",
                                            "sortable": True,
                                        },
                                    },
                                    "body": {"type": "string"},
                                },
                            }
                        }
                    },
                },
                "indexes": {"body_text": {"type": "full_text"}},
            },
        )
        call(
            "POST",
            "/tables/history/batch",
            {
                "inserts": {"history": {"amount": 0, "body": "original historical"}},
                "sync_level": "full_index",
            },
        )
        composed_query = dict(
            query, source={"union": [{"table": "history"}, {"table": "current"}]}
        )
        composed_first = call("POST", "/query", composed_query)["responses"][0]
        assert composed_first["hits"]["hits"][0]["_source"]["amount"] == 0, (
            composed_first
        )
        composed_page = dict(
            composed_query, limit=10, source_cursor=composed_first["next_source_cursor"]
        )
        call(
            "POST",
            "/tables/current/batch",
            {
                "inserts": {
                    "doc-2": {"amount": 20, "body": "changed"},
                    "doc-5": {"amount": 5, "body": "original searchable"},
                },
                "deletes": ["doc-3"],
                "sync_level": "full_index",
            },
        )
        resumed = response(page)
        assert [hit["_source"]["amount"] for hit in resumed["hits"]["hits"]] == [
            2,
            3,
            4,
        ], resumed
        stop()
        start()
        restarted = response(page)
        assert [hit["_source"]["amount"] for hit in restarted["hits"]["hits"]] == [
            2,
            3,
            4,
        ], restarted
        score_restarted = response(score_page)
        assert [
            hit["_source"]["amount"] for hit in score_restarted["hits"]["hits"]
        ] == [2, 3, 4], score_restarted
        assert all(
            hit["_score"] == pytest.approx(score_first["hits"]["hits"][0]["_score"])
            for hit in score_restarted["hits"]["hits"]
        ), score_restarted
        composed_restarted = call("POST", "/query", composed_page)["responses"][0]
        assert [
            hit["_source"]["amount"] for hit in composed_restarted["hits"]["hits"]
        ] == [1, 2, 3, 4], composed_restarted
        assert composed_restarted["hits"]["total"]["value"] == 5, composed_restarted
        fresh = response(dict(query, limit=10))
        assert [hit["_source"]["amount"] for hit in fresh["hits"]["hits"]] == [
            1,
            4,
            5,
        ], fresh
        with pytest.raises(HTTPError) as stale:
            response(
                dict(
                    page,
                    remote_snapshot=first["remote_snapshot"].rsplit(":", 1)[0]
                    + ":"
                    + str(int(first["remote_snapshot"].rsplit(":", 1)[1]) + 1),
                )
            )
        assert stale.value.code in (400, 409), stale.value.__notes__
        with pytest.raises(HTTPError) as private:
            response(dict(query, _native_cut={"id": "a" * 64}))
        assert private.value.code == 400, private.value.__notes__
        with pytest.raises(HTTPError) as unpinned:
            response(dict(query, search_after=page["search_after"]))
        assert unpinned.value.code == 409, unpinned.value.__notes__
        with pytest.raises(HTTPError) as implicit_unpinned:
            response(
                dict(
                    full_text_search=query["full_text_search"],
                    search_after=["doc-1"],
                )
            )
        assert implicit_unpinned.value.code == 409, implicit_unpinned.value.__notes__
        # Losing an owner's retained files must never recreate the old cut
        # from live rows, even though the server capability remains valid.
        retained_files = list((tmp_path / "data").rglob("query-cut.json"))
        assert retained_files
        for manifest in retained_files:
            shutil.rmtree(manifest.parent)
        with pytest.raises(HTTPError) as missing:
            response(page)
        assert missing.value.code == 409, missing.value.__notes__
        assert response(query)["remote_snapshot"].startswith("native2:")
    finally:
        if process is not None and process.poll() is None:
            stop()
        log.close()
