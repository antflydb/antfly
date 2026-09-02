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

"""E2E tests for the antfly CLI client commands.

These tests start an antfly standalone server, then exercise the CLI binary
(table, insert, lookup, query, delete, internal) by shelling out via
subprocess and verifying stdout JSON and exit codes.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from conftest import (
    DEFAULT_ANTFLY_BIN,
    InferenceEmbeddingServer,
    InferenceGeneratorServer,
    InferenceRerankerServer,
    StandaloneAntflyServer,
    resolve_binary_path,
)
from helpers import wait_until
from port_reservations import find_free_port


class TinyImageServer:
    """Local HTTP origin used to exercise remoteMedia's real transport path."""

    _PNG = bytes.fromhex(
        "89504e470d0a1a0a0000000d4948445200000001000000010804000000b51c0c02"
        "0000000b4944415478da63fcff1f0002eb01f58f59952f0000000049454e44ae426082"
    )

    def __init__(self, host: str = "127.0.0.1"):
        self.request_count = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path != "/tiny.png":
                    self.send_error(404)
                    return
                outer.request_count += 1
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(outer._PNG)))
                self.end_headers()
                self.wfile.write(outer._PNG)

            def log_message(self, format: str, *args: object) -> None:
                _ = format
                _ = args
        # Bind port zero atomically; probing and then reopening a selected port
        # leaves an avoidable race with parallel E2E workers.
        self._server = ThreadingHTTPServer((host, 0), Handler)
        port = self._server.server_address[1]
        self.url = f"http://{host}:{port}/tiny.png"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@pytest.fixture(scope="module")
def cli_inference_servers():
    embedder = InferenceEmbeddingServer(response_delay_s=30.0)
    generator = InferenceGeneratorServer()
    reranker = InferenceRerankerServer()
    yield {
        "embedder": embedder.url,
        "embedder_server": embedder,
        "generator": generator.url,
        "reranker": reranker.url,
    }
    reranker.stop()
    generator.stop()
    embedder.stop()


@pytest.fixture(scope="module")
def cli_media_server():
    server = TinyImageServer()
    yield server
    server.stop()


@pytest.fixture(scope="module")
def cli_server(cli_inference_servers):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"antfly binary not found: {binary}")

    port = find_free_port()
    server = StandaloneAntflyServer(binary, "127.0.0.1", port)
    server.cli_inference_urls = cli_inference_servers
    yield server
    server.stop()


@pytest.fixture(scope="module")
def cli(cli_server):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    env = os.environ.copy()
    env["ANTFLY_URL"] = cli_server.url

    def run_cli(
        *args: str, check: bool = True, timeout_s: float = 30.0
    ) -> subprocess.CompletedProcess[str]:
        cmd = [binary] + list(args)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
            check=False,
        )
        if check and result.returncode != 0:
            raise AssertionError(
                f"CLI failed (exit {result.returncode}): {' '.join(cmd)}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}\n"
                f"server logs:\n{cli_server.debug_logs()[-2000:]}"
            )
        return result

    return run_cli


def parse_json(output: str) -> dict | list:
    return json.loads(output.strip())


def assert_no_unexpected_semantic_warning(
    result: subprocess.CompletedProcess[str], index_name: str
) -> None:
    warning_lines = [
        line.lower()
        for line in result.stderr.splitlines()
        if "warning:" in line.lower()
    ]
    if not warning_lines:
        return

    assert len(warning_lines) == 1, result.stderr
    warning = warning_lines[0]
    assert f"warning: semantic index {index_name} is queryable_partial" in warning
    assert "results may be incomplete" in warning
    assert "--until complete" in warning


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------


def test_table_create_list_get_drop(cli):
    table = f"cli_test_{int(time.time() * 1000)}"

    # create
    cli("table", "create", "--table", table, "--shards", "1")

    # list defaults to a compact summary rather than dumping full metadata
    result = cli("table", "list")
    lines = result.stdout.strip().splitlines()
    assert lines[0] == "NAME\tSHARDS\tINDEXES\tSTORAGE"
    assert any(line.startswith(f"{table}\t1\t") for line in lines[1:])

    # detailed machine-readable output remains available explicitly
    result = cli("table", "list", "--output", "json")
    tables = parse_json(result.stdout)
    assert isinstance(tables, list)
    names = [t["name"] for t in tables]
    assert table in names

    # get
    result = cli("table", "get", "--table", table)
    info = parse_json(result.stdout)
    assert info["name"] == table

    # drop
    cli("table", "drop", "--table", table)

    # list again — should be gone (eventually)
    def table_gone() -> bool:
        r = cli("table", "list", "--output", "json")
        tbl_list = parse_json(r.stdout)
        return table not in [t["name"] for t in tbl_list]

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if table_gone():
            break
        time.sleep(0.25)
    else:
        pytest.fail(f"table {table} still present after drop")


def test_cli_inline_create_load_wait_query_image_and_rag_pipeline(
    cli, cli_server, cli_inference_servers, cli_media_server, tmp_path
):
    """Exercise the documented CLI path across parsing, readiness, and retrieval."""
    table = f"cli_quickstart_{time.time_ns()}"
    embedder_url = cli_server.cli_inference_urls["embedder"]
    generator_url = cli_server.cli_inference_urls["generator"]
    reranker_url = cli_server.cli_inference_urls["reranker"]
    embedder_server = cli_inference_servers["embedder_server"]
    inline_index = json.dumps(
        {
            "name": "title_body",
            "type": "embeddings",
            "template": "{{title}} {{body}}",
            "dimension": 3,
            "embedder": {
                "provider": "antfly",
                "model": "antfly-embed-v1",
                "api_url": embedder_url,
            },
            "chunker": {
                "provider": "antfly",
                "text": {
                    "target_tokens": 200,
                    "overlap_tokens": 25,
                },
            },
        }
    )
    tiny_png = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZlS8AAAAASUVORK5CYII="
    records = tmp_path / "quickstart.jsonl"
    records.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "id": "doc:alpha",
                        "title": "Alpha",
                        "body": "alpha concept overview",
                        "thumbnail_url": cli_media_server.url,
                    }
                ),
                json.dumps(
                    {
                        "id": "doc:beta",
                        "title": "Beta",
                        "body": "beta unrelated notes",
                    }
                ),
            ]
        )
        + "\n"
    )

    later_wait: subprocess.Popen[str] | None = None

    try:
        cli(
            "table",
            "create",
            "--table",
            table,
            "--shards",
            "1",
            "--index",
            inline_index,
        )
        # Hold the first generated batch open. Full-text publication is already
        # complete and must remain independently readable while the resident
        # embeddings owner is waiting on its provider.
        embedder_server.arm_delay()
        cli(
            "load",
            "--table",
            table,
            "--file",
            str(records),
            "--id-field",
            "id",
            "--sync-level",
            "full_text",
            "--no-checkpoint",
        )
        assert embedder_server.wait_for_embedding_request(10.0)

        full_text_started = time.monotonic()
        full_text_query = cli(
            "query",
            "--table",
            table,
            "--full-text-search",
            "body:alpha",
            "--fields",
            "title,body",
            "--limit",
            "2",
            timeout_s=5.0,
        )
        assert time.monotonic() - full_text_started < 3.0, (
            "a ready full-text read queued behind generated-index backfill"
        )
        full_text_hits = parse_json(full_text_query.stdout)["responses"][0]["hits"][
            "hits"
        ]
        assert full_text_hits[0]["_id"] == "doc:alpha"
        embedder_server.release_delay()

        # `searchable-artifacts` is intentionally index-type neutral. Exercise
        # it against the real default full-text index so the CLI cannot regress
        # to requiring an embeddings-only incarnation projection.
        listed_indexes = parse_json(
            cli("index", "list", "--table", table, "--output", "json").stdout
        )
        full_text_index = next(
            item for item in listed_indexes if item["config"]["type"] == "full_text"
        )
        full_text_name = full_text_index["config"]["name"]
        full_text_incarnation = full_text_index["status"]["incarnation"]
        assert full_text_incarnation
        assert (
            full_text_index["status"]["readiness"]["incarnation"]
            == full_text_incarnation
        )
        full_text_wait = cli(
            "index",
            "wait",
            "--table",
            table,
            "--index",
            full_text_name,
            "--until",
            "searchable-artifacts=1",
            "--timeout",
            "20s",
            "--poll-interval",
            "25ms",
            timeout_s=30.0,
        )
        assert (
            f"Index {full_text_name} (full_text) reached searchable-artifacts=1:"
            in full_text_wait.stdout
        )

        text_wait = cli(
            "index",
            "wait",
            "--table",
            table,
            "--index",
            "title_body",
            "--until",
            "searchable-artifacts=1",
            "--timeout",
            "20s",
            "--poll-interval",
            "25ms",
            timeout_s=30.0,
        )
        assert (
            "Index title_body (embeddings) reached searchable-artifacts=1:"
            in text_wait.stdout
        )
        assert "source_coverage=" in text_wait.stdout
        assert "searchable_vectors=" in text_wait.stdout
        assert "pending_reasons=" in text_wait.stdout
        assert "complete_blockers=" in text_wait.stdout

        text_query = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha concept",
            "--indexes",
            "title_body",
            "--limit",
            "2",
        )
        assert_no_unexpected_semantic_warning(text_query, "title_body")
        text_hits = parse_json(text_query.stdout)["responses"][0]["hits"]["hits"]
        assert text_hits[0]["_id"] == "doc:alpha"

        # Reproduce the documented live-add path while another managed index
        # is actively awaiting inference. Activation/control work must bypass
        # the corpus lane and acknowledge the new incarnation without restart.
        embedder_server.arm_delay()
        cli(
            "insert",
            "--table",
            table,
            "--key",
            "doc:concurrent",
            "--document",
            json.dumps(
                {
                    "title": "Concurrent",
                    "body": "embedding build remains active during image activation",
                }
            ),
        )
        assert embedder_server.wait_for_embedding_request(10.0)
        image_create_started = time.monotonic()
        cli(
            "index",
            "create",
            "--table",
            table,
            "--index",
            "thumbnail",
            "--type",
            "embeddings",
            "--coverage-policy",
            "partial",
            "--template",
            "{{#if thumbnail_url}}{{remoteMedia url=thumbnail_url}}{{/if}}",
            "--embedder",
            json.dumps(
                {
                    "provider": "antfly",
                    "model": "antflydb/clipclap",
                    "api_url": embedder_url,
                }
            ),
            timeout_s=8.0,
        )
        assert time.monotonic() - image_create_started < 5.0, (
            "second-index activation queued behind provider execution"
        )

        def thumbnail_is_owned() -> dict | None:
            index = parse_json(
                cli(
                    "index",
                    "get",
                    "--table",
                    table,
                    "--index",
                    "thumbnail",
                ).stdout
            )
            assert index["config"]["dimension"] == 3
            status = index["status"]
            if (
                status["incarnation"]
                and status["readiness"]["state"] != "runtime_unavailable"
            ):
                return status
            return None

        image_activation = wait_until(
            thumbnail_is_owned, timeout_s=5.0, interval_s=0.025
        )
        assert image_activation is not None, cli(
            "index", "get", "--table", table, "--index", "thumbnail"
        ).stdout

        def generated_coverage_is_observed() -> dict | None:
            statuses = {
                name: parse_json(
                    cli("index", "get", "--table", table, "--index", name).stdout
                )["status"]
                for name in ("title_body", "thumbnail")
            }
            if all(
                status["source_coverage"]["observation_complete"]
                for status in statuses.values()
            ):
                return statuses
            return None

        observed_coverage = wait_until(
            generated_coverage_is_observed, timeout_s=5.0, interval_s=0.025
        )
        assert observed_coverage is not None, cli(
            "index", "list", "--table", table, "--output", "json"
        ).stdout

        # An incomplete generated corpus is owned by ordinary initial-build
        # work. Keep the provider blocked across multiple periodic startup
        # observations and prove they neither synthesize repair debt, invalidate
        # runtime observations, nor overwrite the exact repair owner's bounded
        # wake with a hot immediate loop. This is the live quickstart
        # regression: a second index is activated while its sibling already has
        # pending generated coverage.
        rebuild_log_counts = {
            name: cli_server.debug_logs().count(
                f"dense startup artifact rebuild planned index={name} "
            )
            for name in ("title_body", "thumbnail")
        }
        repair_pass_log = "provisioned index repair begin group="
        repair_passes_before = sum(
            1
            for line in cli_server.debug_logs().splitlines()
            if repair_pass_log in line and f"table={table} " in line
        )
        # Span two complete five-second audit periods. A level observation of
        # known debt must not erase owner backoff or turn the queue into a hot
        # immediate-repair loop after the first audit fires.
        status_stability_deadline = time.monotonic() + 10.5
        while time.monotonic() < status_stability_deadline:
            for name in ("title_body", "thumbnail"):
                status = parse_json(
                    cli("index", "get", "--table", table, "--index", name).stdout
                )["status"]
                assert status["readiness"]["state"] != "runtime_unavailable", (
                    json.dumps(status, indent=2)
                )
                source_coverage = status["source_coverage"]
                if source_coverage["observation_complete"]:
                    assert source_coverage["pending"] > 0, json.dumps(status, indent=2)
                else:
                    # A concurrent runtime-status publication may temporarily
                    # make the census freshness unknown. That is explicit and
                    # must never be represented as authoritative zero work.
                    assert source_coverage["pending"] is None
                    assert source_coverage["observation_incomplete_reasons"]
                assert source_coverage["failed"] == 0
                assert status.get("repair") is None, json.dumps(status, indent=2)
                assert status["enrichment_runtime"]["worker_failed"] is False
            time.sleep(0.2)
        logs_while_building = cli_server.debug_logs()
        repair_passes_after = sum(
            1
            for line in logs_while_building.splitlines()
            if repair_pass_log in line and f"table={table} " in line
        )
        # One bounded pass per five-second audit period (plus a boundary race)
        # is expected; the regression produced hundreds of immediate passes.
        assert repair_passes_after - repair_passes_before <= 3, (
            "periodic startup observation repeatedly promoted known repair "
            "debt to an immediate wake"
        )
        for name, before_count in rebuild_log_counts.items():
            assert (
                logs_while_building.count(
                    f"dense startup artifact rebuild planned index={name} "
                )
                == before_count
            ), (
                f"ordinary generated coverage for {name} was misclassified as "
                "artifact repair"
            )
        embedder_server.release_delay()
        text_status_after_index_create = parse_json(
            cli("index", "get", "--table", table, "--index", "title_body").stdout
        )
        assert (
            text_status_after_index_create["status"]["readiness"]["queryable"] is True
        ), json.dumps(text_status_after_index_create, indent=2)
        image_wait = cli(
            "index",
            "wait",
            "--table",
            table,
            "--index",
            "thumbnail",
            "--until",
            "searchable-artifacts=1",
            "--timeout",
            "20s",
            "--poll-interval",
            "25ms",
            timeout_s=30.0,
            check=False,
        )
        assert image_wait.returncode == 0, (
            f"{image_wait.stderr}\nindex diagnostics:\n"
            f"{cli('index', 'list', '--table', table, '--output', 'json').stdout}"
        )
        assert (
            "Index thumbnail (embeddings) reached searchable-artifacts=1:"
            in image_wait.stdout
        )
        image_status = parse_json(
            cli("index", "get", "--table", table, "--index", "thumbnail").stdout
        )
        image_coverage = image_status["status"]["source_coverage"]
        assert image_coverage["policy"] == "partial"
        assert image_coverage["total"] == 3
        assert image_coverage["covered"] == 1
        assert image_coverage["skipped"] == 2
        assert image_coverage["failed"] == 0
        assert image_coverage["complete"] is True, json.dumps(image_status, indent=2)
        assert image_coverage["healthy"] is True
        assert cli_media_server.request_count >= 1, (
            "the image quickstart path did not exercise remoteMedia over HTTP"
        )

        image_query = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "map of a country",
            "--indexes",
            "thumbnail",
            "--limit",
            "2",
        )
        assert "warning:" not in image_query.stderr.lower()
        image_hits = parse_json(image_query.stdout)["responses"][0]["hits"]["hits"]
        assert image_hits[0]["_id"] == "doc:alpha"

        # A retry-exhausted ClipClap request is a per-source terminal outcome,
        # not a table-wide worker failure. This is the production quickstart
        # edge: the text and image indexes share one resident enrichment owner
        # while the thumbnail provider repeatedly returns 503.
        embedder_server.arm_transient_embedding_failures("antflydb/clipclap")
        insert_started = time.monotonic()
        cli(
            "insert",
            "--table",
            table,
            "--key",
            "doc:image-broken",
            "--document",
            json.dumps(
                {
                    "title": "Broken image",
                    "body": "source-specific image failure",
                    "thumbnail_url": tiny_png,
                }
            ),
        )
        assert time.monotonic() - insert_started < 5.0, (
            "the default point-mutation barrier waited for generated indexing"
        )

        def isolated_failure_is_settled() -> dict | None:
            status = parse_json(
                cli("index", "get", "--table", table, "--index", "thumbnail").stdout
            )["status"]
            if status["coverage"]["terminal_failed"] >= 1:
                return status
            return None

        settled_failure = wait_until(
            isolated_failure_is_settled, timeout_s=30.0, interval_s=0.05
        )
        assert settled_failure is not None, cli(
            "index", "get", "--table", table, "--index", "thumbnail"
        ).stdout
        embedder_server.release_transient_embedding_failures()
        assert embedder_server.transient_embedding_requests > 1
        assert settled_failure["enrichment_runtime"]["worker_failed"] is False
        text_after_media_failure = parse_json(
            cli("index", "get", "--table", table, "--index", "title_body").stdout
        )["status"]
        assert text_after_media_failure["readiness"]["queryable"] is True
        assert text_after_media_failure["enrichment_runtime"]["worker_failed"] is False

        # Hold a later healthy request open so the CLI observes and waits
        # through the exact degraded-but-progressing boundary.
        embedder_server.arm_delay()
        insert_started = time.monotonic()
        cli(
            "insert",
            "--table",
            table,
            "--key",
            "doc:image-later",
            "--document",
            json.dumps(
                {
                    "title": "Later image",
                    "body": "later usable image",
                    "thumbnail_url": tiny_png,
                }
            ),
        )
        assert time.monotonic() - insert_started < 5.0, (
            "the default point-mutation barrier inherited provider latency"
        )
        assert embedder_server.wait_for_embedding_request(10.0)

        def isolated_failure_is_pending() -> dict | None:
            status = parse_json(
                cli("index", "get", "--table", table, "--index", "thumbnail").stdout
            )["status"]
            coverage = status["coverage"]
            if (
                coverage["terminal_failed"] >= 1
                and coverage["pending"] >= 1
                and status["readiness"]["state"] == "queryable_partial"
                and status["searchable_vectors"] == 1
            ):
                return status
            return None

        pending_status = wait_until(
            isolated_failure_is_pending, timeout_s=10.0, interval_s=0.025
        )
        assert pending_status is not None, cli(
            "index", "get", "--table", table, "--index", "thumbnail"
        ).stdout

        binary = resolve_binary_path(
            os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))
        )
        wait_env = os.environ.copy()
        wait_env["ANTFLY_URL"] = cli_server.url
        later_wait = subprocess.Popen(
            [
                binary,
                "index",
                "wait",
                "--table",
                table,
                "--index",
                "thumbnail",
                "--until",
                "searchable-artifacts=2",
                "--timeout",
                "20s",
                "--poll-interval",
                "25ms",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=wait_env,
        )
        time.sleep(0.15)
        assert later_wait.poll() is None, (
            "index wait treated an isolated source failure as terminal while "
            f"later work remained: {later_wait.communicate(timeout=1.0)}"
        )
        embedder_server.release_delay()
        later_stdout, later_stderr = later_wait.communicate(timeout=30.0)
        assert later_wait.returncode == 0, (
            f"stdout: {later_stdout}\nstderr: {later_stderr}\n"
            f"pending status: {json.dumps(pending_status, indent=2)}"
        )
        assert (
            "Index thumbnail (embeddings) reached searchable-artifacts=2:"
            in later_stdout
        )

        # A partial published generation is the restart availability unit. The
        # same exact incarnations must be admitted promptly after reopen while
        # any remaining source coverage continues in the background.
        before_restart = {
            name: parse_json(
                cli("index", "get", "--table", table, "--index", name).stdout
            )["status"]["incarnation"]
            for name in ("title_body", "thumbnail")
        }
        cli_server.restart()

        def same_incarnations_are_queryable() -> dict | None:
            statuses = {
                name: parse_json(
                    cli("index", "get", "--table", table, "--index", name).stdout
                )["status"]
                for name in before_restart
            }
            if all(
                status["incarnation"] == before_restart[name]
                and status["readiness"]["queryable"] is True
                for name, status in statuses.items()
            ):
                return statuses
            return None

        restarted = wait_until(
            same_incarnations_are_queryable, timeout_s=8.0, interval_s=0.05
        )
        if restarted is None:
            restart_diagnostics = {}
            for name, expected_incarnation in before_restart.items():
                status = parse_json(
                    cli("index", "get", "--table", table, "--index", name).stdout
                )["status"]
                restart_diagnostics[name] = {
                    "expected_incarnation": expected_incarnation,
                    "incarnation": status.get("incarnation"),
                    "readiness": status.get("readiness"),
                    "source_coverage": status.get("source_coverage"),
                    "publication": status.get("publication"),
                    "searchable_vectors": status.get("searchable_vectors"),
                    "runtime_present": status.get("runtime_present"),
                    "runtime_fresh": status.get("runtime_fresh"),
                    "repair": status.get("repair"),
                }
            pytest.fail(json.dumps(restart_diagnostics, indent=2, sort_keys=True))

        rag = cli(
            "agents",
            "retrieval",
            "--table",
            table,
            "--semantic-search",
            "alpha concept",
            "--indexes",
            "title_body",
            "--prompt",
            "Summarize the alpha document",
            "--fields",
            "title,body",
            "--limit",
            "1",
            "--reranker",
            json.dumps(
                {
                    "provider": "antfly",
                    "model": "test-reranker",
                    "url": reranker_url,
                    "field": "body",
                    "top_n": 1,
                }
            ),
            "--pruner",
            json.dumps({"min_score_ratio": 0.01}),
            "--generator",
            json.dumps(
                {
                    "provider": "antfly",
                    "model": "local-generator",
                    "api_url": generator_url,
                    "api_key": "test-key",
                }
            ),
            "--generate",
            "--no-streaming",
            timeout_s=60.0,
        )
        assert_no_unexpected_semantic_warning(rag, "title_body")
        rag_result = parse_json(rag.stdout)
        assert rag_result["status"] == "completed"
        assert rag_result["generation"]
        assert rag_result["hits"][0]["_id"] == "doc:alpha"
    finally:
        embedder_server.release_delay()
        if later_wait is not None and later_wait.poll() is None:
            later_wait.terminate()
            try:
                later_wait.communicate(timeout=5.0)
            except subprocess.TimeoutExpired:
                later_wait.kill()
                later_wait.communicate(timeout=5.0)
        cli("table", "drop", "--table", table, check=False)


# ---------------------------------------------------------------------------
# Insert + Lookup + Delete
# ---------------------------------------------------------------------------


def test_insert_lookup_delete(cli):
    table = f"cli_crud_{int(time.time() * 1000)}"

    cli("table", "create", "--table", table, "--shards", "1")

    # insert
    doc = json.dumps({"title": "Hello", "body": "world"})
    cli("insert", "--table", table, "--key", "doc1", "--document", doc)

    # lookup
    def lookup_succeeds() -> dict | None:
        r = cli("lookup", "--table", table, "--key", "doc1", check=False)
        if r.returncode != 0:
            return None
        try:
            data = parse_json(r.stdout)
        except json.JSONDecodeError:
            return None
        if not data:
            return None
        return data

    result = wait_until(lookup_succeeds, timeout_s=10.0, interval_s=0.25)
    assert result is not None
    assert result["title"] == "Hello"
    assert result["body"] == "world"

    # delete
    cli("delete", "--table", table, "--key", "doc1")

    # verify gone
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        r = cli("lookup", "--table", table, "--key", "doc1", check=False)
        if r.returncode != 0 or not r.stdout.strip():
            break
        time.sleep(0.25)

    # cleanup
    cli("table", "drop", "--table", table)


@pytest.mark.parametrize(
    "args",
    [
        (
            "insert",
            "--table",
            "docs",
            "--key",
            "doc:a",
            "--document",
            "{}",
            "--typo",
            "value",
        ),
        ("delete", "--table", "docs", "--key", "doc:a", "--typo", "value"),
        ("lookup", "--table", "docs", "--key", "doc:a", "--typo", "value"),
        ("artifact", "list", "--table", "docs", "--typo", "value"),
        (
            "agents",
            "query-builder",
            "--intent",
            "find documents",
            "--generator",
            '{"provider":"openai","model":"test"}',
            "--typo",
            "value",
        ),
    ],
)
def test_client_commands_reject_unknown_options_before_network_work(cli, args):
    result = cli(*args, check=False)
    assert result.returncode != 0
    assert "unknown" in result.stderr.lower()


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (
            ("load", "--table", "docs", "--file", "missing.jsonl", "--checkpoint"),
            "--checkpoint requires a value",
        ),
        (
            ("load", "--table", "docs", "-t", "other", "--file", "missing.jsonl"),
            "-t may only be provided once",
        ),
        (
            (
                "load",
                "--table",
                "docs",
                "--file",
                "missing.jsonl",
                "--checkpoint",
                "state",
                "--no-checkpoint",
            ),
            "--checkpoint cannot be used with --no-checkpoint",
        ),
    ],
)
def test_load_rejects_missing_duplicate_and_conflicting_options_before_io(
    cli, args, message
):
    result = cli(*args, check=False)
    assert result.returncode != 0
    assert message in result.stderr


def test_semantic_query_requires_table_before_network_work(cli):
    result = cli("query", "--semantic-search", "alpha", check=False)
    assert result.returncode != 0
    assert "--table is required" in result.stderr


def test_semantic_preflight_reports_missing_selected_index(cli):
    table = f"cli_missing_semantic_{time.time_ns()}"
    try:
        cli("table", "create", "--table", table, "--shards", "1")
        missing = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha",
            "--indexes",
            "missing_dense",
            check=False,
        )
        assert missing.returncode != 0
        assert (
            f"selected semantic index missing_dense was not found on table {table}"
            in missing.stderr
        )

        wrong_type = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha",
            "--indexes",
            "full_text_index_v0",
            check=False,
        )
        assert wrong_type.returncode != 0
        assert (
            "selected semantic index full_text_index_v0 is type full_text, not embeddings"
            in wrong_type.stderr
        )

        none_available = cli(
            "query",
            "--table",
            table,
            "--semantic-search",
            "alpha",
            check=False,
        )
        assert none_available.returncode != 0
        assert (
            f"table {table} has no embeddings indexes available for semantic search"
            in none_available.stderr
        )
    finally:
        cli("table", "drop", "--table", table, check=False)


# ---------------------------------------------------------------------------
# Query (full-text search via CLI)
# ---------------------------------------------------------------------------


def test_query_full_text_search(cli):
    table = f"cli_query_{int(time.time() * 1000)}"

    cli("table", "create", "--table", table, "--shards", "1")

    for key, body in [
        ("alpha", json.dumps({"content": "alpha retrieval architecture"})),
        ("beta", json.dumps({"content": "beta unrelated noise"})),
    ]:
        cli("insert", "--table", table, "--key", key, "--value", body)

    def query_hits() -> dict | None:
        r = cli(
            "query",
            "--table",
            table,
            "--full-text-search",
            "content:alpha",
            "--limit",
            "5",
            check=False,
        )
        if r.returncode != 0:
            return None
        try:
            data = parse_json(r.stdout)
        except json.JSONDecodeError:
            return None
        responses = data.get("responses", [])
        if not responses:
            return None
        hits = responses[0].get("hits", {}).get("hits", [])
        if not hits:
            return None
        return data

    result = wait_until(query_hits, timeout_s=15.0, interval_s=0.5)
    assert result is not None
    responses = result["responses"]
    hits = responses[0]["hits"]["hits"]
    assert hits[0]["_id"] == "alpha"

    cli("table", "drop", "--table", table)


# ---------------------------------------------------------------------------
# Internal metadata status
# ---------------------------------------------------------------------------


def test_internal_metadata_status(cli):
    result = cli("internal", "metadata", "status")
    status = parse_json(result.stdout)
    assert isinstance(status, dict)
