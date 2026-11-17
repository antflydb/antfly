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

"""Public CLI transfer boundaries must agree on catalogs, identities and paths."""

import json
import os
import subprocess

import requests
import pytest
from conftest import (
    AUTH_BOOTSTRAP_PASSWORD,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    resolve_binary_path,
)
from helpers import wait_until

pytestmark = pytest.mark.e2e_resource("antfly_process")


@pytest.mark.parametrize(
    "schema_version,direct_sparse", [(0, False), (3, False), (0, True)]
)
def test_lite_server_backup_roundtrip_under_connection_root(
    tmp_path, schema_version, direct_sparse
):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    root = (tmp_path / "backups").resolve()
    root.mkdir()
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "connections": {
                    "bk": {
                        "kind": "external_io",
                        "capabilities": ["backup.write", "restore.read"],
                        "external_io": {"protocol": "filesystem", "root": str(root)},
                    }
                }
            }
        )
    )
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0, config_path=config)
    env = {
        **os.environ,
        "ANTFLY_USERNAME": "admin",
        "ANTFLY_PASSWORD": AUTH_BOOTSTRAP_PASSWORD,
    }
    env.pop("ANTFLY_TOKEN", None)

    def cli(*args, check=True):
        result = subprocess.run(
            [binary, *([] if args[0] == "lite" else ["--no-agent"]), *map(str, args)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert not check or result.returncode == 0, (
            result.stdout
            + result.stderr
            + "\nSERVER:\n"
            + server.log_path.read_text()[-20000:]
        )
        return result

    def expect_docs(table):
        last = {}

        def ready():
            response = requests.post(
                server.api_url + f"/tables/{table}/query",
                json={"full_text_search": {"match_all": {}}},
                timeout=10,
            )
            last["full_text"] = (response.status_code, response.text)
            if response.status_code in (404, 503):
                return False
            assert response.ok, response.text
            if response.json()["responses"][0]["hits"]["total"]["value"] != 2:
                return False
            vector = requests.post(
                server.api_url + f"/tables/{table}/query",
                json={
                    "embeddings": {"vectors": [1, 0]},
                    "indexes": ["vectors"],
                    "limit": 1,
                },
                timeout=10,
            )
            last["dense"] = (vector.status_code, vector.text)
            if vector.status_code == 503:
                return False
            assert vector.ok, vector.text
            hits = vector.json()["responses"][0]["hits"]["hits"]
            if not hits:
                return False
            assert hits[0]["_id"] == "a", vector.text
            sparse = requests.post(
                server.api_url + f"/tables/{table}/query",
                json={
                    "embeddings": {
                        "sparse_vectors": {"indices": [10], "values": [1.0]}
                    },
                    "indexes": ["sparse_vectors"],
                    "limit": 1,
                },
                timeout=10,
            )
            last["sparse"] = (sparse.status_code, sparse.text)
            if sparse.status_code == 503:
                return False
            assert sparse.ok, sparse.text
            hits = sparse.json()["responses"][0]["hits"]["hits"]
            if not hits:
                return False
            assert hits[0]["_id"] == "a", sparse.text
            return True

        assert wait_until(ready, timeout_s=60, interval_s=0.1), (
            f"{table}: {json.dumps(last)}"
        )
        definition = requests.get(server.api_url + f"/tables/{table}", timeout=10)
        assert definition.ok, definition.text
        assert definition.json()["schema"]["version"] == schema_version, definition.text
        assert definition.json()["indexes"]["sparse_vectors"]["external"] is True, (
            definition.text
        )

    failed = True
    try:
        seed = tmp_path / "seed.aflite"
        batch = tmp_path / "batch.json"
        batch.write_text(
            json.dumps(
                {
                    "inserts": {
                        "a": {
                            "title": "alpha",
                            "_embeddings": {
                                "vectors": [1, 0],
                                "sparse_vectors": {"indices": [10], "values": [2.5]},
                            },
                        },
                        "b": {
                            "title": "beta",
                            "_embeddings": {
                                "vectors": [0, 1],
                                "sparse_vectors": {"indices": [20], "values": [3.0]},
                            },
                        },
                    }
                }
            )
        )
        cli("lite", "init", seed)
        schema = {
            "version": schema_version,
            "default_type": "doc",
            "enforce_types": False,
            "document_schemas": {
                "doc": {
                    "schema": {
                        "type": "object",
                        "additionalProperties": True,
                        "x-antfly-dynamic-indexing": {"mode": "infer_types"},
                    }
                }
            },
        }
        if schema_version:
            schema_file = tmp_path / "schema.json"
            schema_file.write_text(json.dumps(schema))
            cli("lite", "schema", "set", seed, "--file", schema_file)
        index = tmp_path / "vectors.json"
        index.write_text(
            json.dumps(
                {
                    "name": "vectors",
                    "kind": "dense_vector",
                    "config_json": json.dumps(
                        {
                            "field": "embedding",
                            "dims": 2,
                            "metric": "l2_squared",
                            "external": True,
                        }
                    ),
                }
            )
        )
        cli("lite", "index", "create", seed, "--file", index)
        index.write_text(
            json.dumps(
                {
                    "name": "sparse_vectors",
                    "kind": "sparse_vector",
                    "config_json": json.dumps(
                        {
                            "field": "embedding",
                            **({} if direct_sparse else {"external": True}),
                        }
                    ),
                }
            )
        )
        cli("lite", "index", "create", seed, "--file", index)
        cli("lite", "batch", seed, "--file", batch)
        cli("lite", "run-until-idle", seed)
        sparse_query = tmp_path / "sparse-query.json"
        sparse_query.write_text(
            json.dumps(
                {
                    "embeddings": {
                        "sparse_vectors": {"indices": [10], "values": [1.0]}
                    },
                    "indexes": ["sparse_vectors"],
                    "limit": 1,
                }
            )
        )
        initial = json.loads(cli("lite", "query", seed, "--file", sparse_query).stdout)
        assert initial["responses"][0]["hits"]["hits"][0]["_id"] == "a", initial
        portable = tmp_path / "seed.afb"
        cli("lite", "backup", seed, "--out", portable)
        if direct_sparse:
            result = cli(
                "restore",
                "--input",
                portable,
                "--table",
                "direct_sparse",
                "--connection",
                "bk",
                "--location",
                root.as_uri(),
                "--url",
                server.url,
                "--wait",
                check=False,
            )
            assert result.returncode != 0
            assert "UnsupportedEmbeddedLiteIndex" in result.stderr
            assert "index without a server catalog equivalent" in result.stderr
            assert (
                requests.get(
                    server.api_url + "/tables/direct_sparse", timeout=10
                ).status_code
                == 404
            )
            failed = False
            return
        for name, source in [("from_file", seed), ("from_archive", portable)]:
            location = root / name
            cli(
                "restore",
                "--input",
                source,
                "--table",
                name,
                "--connection",
                "bk",
                "--location",
                location.as_uri(),
                "--url",
                server.url,
                "--wait",
            )
            expect_docs(name)
        cli(
            "lite",
            "promote",
            seed,
            "--target",
            server.url,
            "--table",
            "promoted",
            "--connection",
            "bk",
            "--location",
            (root / "promoted").as_uri(),
        )
        expect_docs("promoted")
        for format in ("portable", "native"):
            if format == "native":
                alias = requests.put(
                    server.api_url + "/tables/from_file/index-aliases/live",
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    json={
                        "index": "sparse_vectors",
                        "idempotency_key": "restore-alias",
                        "require": "queryable",
                    },
                    timeout=10,
                )
                assert alias.ok, alias.text
                source_alias = requests.get(
                    server.api_url + "/tables/from_file/index-aliases/live",
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=10,
                )
                assert source_alias.ok, source_alias.text
                source_etag = source_alias.headers["ETag"]
            output = tmp_path / f"server-{format}.afb"
            cli(
                "backup",
                "--table",
                "from_file",
                "--backup-id",
                f"export-{format}",
                "--connection",
                "bk",
                "--location",
                root.as_uri(),
                "--format",
                format,
                "--out",
                output,
                "--url",
                server.url,
            )
            assert output.stat().st_size > 0
            assert (root / f"export-{format}-metadata.json").exists()
            cli(
                "restore",
                "--input",
                output,
                "--table",
                f"returned_{format}",
                "--connection",
                "bk",
                "--location",
                (root / f"returned_{format}").as_uri(),
                "--url",
                server.url,
                "--wait",
            )
            expect_docs(f"returned_{format}")
            if format == "native":
                restored_alias = requests.get(
                    server.api_url + "/tables/returned_native/index-aliases/live",
                    auth=("admin", AUTH_BOOTSTRAP_PASSWORD),
                    timeout=10,
                )
                assert restored_alias.ok, restored_alias.text
                assert restored_alias.headers["ETag"] != source_etag
            if format == "portable":
                restored = tmp_path / "returned.aflite"
                cli("lite", "restore", output, "--out", restored)
                query = tmp_path / "query.json"
                query.write_text(json.dumps({"full_text_search": {"match_all": {}}}))
                result = cli("lite", "query", restored, "--file", query)
                assert (
                    json.loads(result.stdout)["responses"][0]["hits"]["total"]["value"]
                    == 2
                )
                query.write_text(
                    json.dumps(
                        {
                            "embeddings": {"vectors": [1, 0]},
                            "indexes": ["vectors"],
                            "limit": 1,
                        }
                    )
                )
                result = cli("lite", "query", restored, "--file", query)
                assert (
                    json.loads(result.stdout)["responses"][0]["hits"]["hits"][0]["_id"]
                    == "a"
                )
        cli("lite", "backup", restored, "--out", tmp_path / "reexport.afb")
        cli(
            "restore",
            "--input",
            tmp_path / "reexport.afb",
            "--table",
            "reexported",
            "--connection",
            "bk",
            "--location",
            (root / "reexported").as_uri(),
            "--url",
            server.url,
            "--wait",
        )
        expect_docs("reexported")
        failed = False
    finally:
        server.stop(test_failed=failed)


def test_foreign_source_failure_preserves_safe_cause():
    from port_reservations import LoopbackPortReservations

    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    server = StandaloneAntflyServer(binary, "127.0.0.1", 0)
    failed = True
    try:
        with LoopbackPortReservations() as ports:
            port = ports.reserve()
            response = requests.post(
                server.api_url + "/tables/unreachable_source/query",
                json={
                    "limit": 10,
                    "foreign_sources": {
                        "unreachable_source": {
                            "type": "postgres",
                            "dsn": f"postgres://user:never-return-this-password@127.0.0.1:{port}/shop",
                            "postgres_table": "customers",
                        }
                    },
                },
                timeout=15,
            )
        assert response.status_code == 503, response.text
        body = response.json()
        assert body["code"] in (
            "foreign_source_unavailable",
            "foreign_source_driver_unavailable",
        )
        assert body["source"] == "unreachable_source"
        assert body["retryable"] is (body["code"] == "foreign_source_unavailable")
        assert "never-return-this-password" not in response.text
        failed = False
    finally:
        server.stop(test_failed=failed)
