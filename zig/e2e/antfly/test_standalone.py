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

"""Live standalone integration tests using antfly standalone with embedded inference."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import tempfile
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
import requests

from conftest import antfly_public_api_url, inference_public_api_url
from helpers import wait_until
from port_reservations import LoopbackPortReservations


pytestmark = pytest.mark.standalone_integration

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ANTFLY_BIN = REPO_ROOT / "zig-out" / "bin" / "antfly"
DEFAULT_INFERENCE_MODELS_DIR = Path("~/.antfly/inference/models").expanduser()
DEFAULT_INFERENCE_MODEL_NAME = "ggml-org/gemma-4-e2b-it-gguf"
DEFAULT_INFERENCE_STANDALONE_HOST_BUDGET_MB = 0
DEFAULT_INFERENCE_STANDALONE_BACKEND_BUDGET_MB = 12288
DEFAULT_INFERENCE_STANDALONE_COMBINED_BUDGET_MB = 16384
DEFAULT_INFERENCE_STANDALONE_KV_BUDGET_MB = 0
DEFAULT_INFERENCE_STANDALONE_SCRATCH_BUDGET_MB = 0
DEFAULT_INFERENCE_STANDALONE_PROCESS_MEMORY_BUDGET_MB = 0


def _integration_enabled(env_name: str) -> bool:
    value = os.environ.get(env_name, "")
    return value != "" and value not in {"0", "false", "False"}


def _resolve_binary_path(binary: str) -> str:
    return str(Path(binary).expanduser().resolve())


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def _wait_for_server(url: str, timeout_s: float = 30.0, path: str = "/status") -> bool:
    deadline = time.monotonic() + timeout_s
    consecutive_successes = 0
    while time.monotonic() < deadline:
        try:
            response = requests.get(f"{url}{path}", timeout=2)
            if response.ok:
                consecutive_successes += 1
                if consecutive_successes >= 2:
                    return True
            else:
                consecutive_successes = 0
        except requests.RequestException:
            consecutive_successes = 0
        time.sleep(0.25)
    return False


def _read_log_tail(path: Path, *, limit: int = 20000) -> str:
    if not path.exists():
        return ""
    data = path.read_text(errors="replace")
    if len(data) <= limit:
        return data
    return data[-limit:]


def _parse_sse_events(body: str) -> list[tuple[str, object]]:
    events: list[tuple[str, object]] = []
    for chunk in body.strip().split("\n\n"):
        if not chunk:
            continue
        event_name = None
        data = None
        for line in chunk.splitlines():
            if line.startswith("event: "):
                event_name = line[len("event: ") :]
            elif line.startswith("data: "):
                payload = line[len("data: ") :]
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    data = payload
        if event_name is not None and data is not None:
            events.append((event_name, data))
    return events


def _hit_ids(result: dict) -> list[str]:
    return [hit["_id"] for hit in result.get("hits", [])]


def _normalize_model_ref_for_path(model_name: str) -> str:
    normalized = model_name[3:] if model_name.startswith("hf:") else model_name
    if ":" in normalized:
        normalized = normalized.split(":", 1)[0]
    return normalized


def _candidate_model_dirs(models_dir: Path, model_name: str) -> list[Path]:
    normalized = _normalize_model_ref_for_path(model_name)
    legacy_candidates = [
        models_dir / normalized,
        models_dir / "generators" / normalized,
    ]
    candidates = list(legacy_candidates)
    for legacy in legacy_candidates:
        parent = legacy.parent
        if not parent.is_dir():
            continue
        prefix = f"{legacy.name}--antfly-"
        # Explicit variants are atomically published beside the legacy model
        # directory using a 16-character SHA-256 prefix. Enumerating only that
        # namespace keeps this preflight bounded and avoids accepting temporary
        # download directories as complete models.
        for child in sorted(parent.iterdir()):
            if not child.is_dir() or not child.name.startswith(prefix):
                continue
            variant_hash = child.name[len(prefix) :]
            if len(variant_hash) == 16 and all(
                char in "0123456789abcdef" for char in variant_hash
            ):
                candidates.append(child)
    return candidates


def _model_exists(models_dir: Path, model_name: str) -> bool:
    return any(path.is_dir() for path in _candidate_model_dirs(models_dir, model_name))


def test_standalone_model_preflight_recognizes_atomic_variant_publication(
    tmp_path: Path,
):
    models_dir = tmp_path / "models"
    published = models_dir / "ggml-org" / "gemma-4-e2b-it-gguf--antfly-0123456789abcdef"
    published.mkdir(parents=True)

    assert _model_exists(models_dir, "ggml-org/gemma-4-e2b-it-gguf")


def test_standalone_model_preflight_rejects_incomplete_variant_names(tmp_path: Path):
    models_dir = tmp_path / "models"
    incomplete = (
        models_dir
        / "generators"
        / "ggml-org"
        / "gemma-4-e2b-it-gguf--antfly-0123456789abcdef.tmp"
    )
    incomplete.mkdir(parents=True)

    assert not _model_exists(models_dir, "ggml-org/gemma-4-e2b-it-gguf")


class EmbeddedInferenceStandaloneServer:
    def __init__(
        self,
        binary: str,
        models_dir: Path,
        model_name: str,
        *,
        inference_budget_mb: dict[str, int] | None = None,
        process_memory_budget_mb: int = 0,
        host: str = "127.0.0.1",
    ):
        self.binary = binary
        self.models_dir = models_dir
        self.model_name = model_name
        self.host = host
        self.inference_budget_mb = inference_budget_mb or {}
        self.process_memory_budget_mb = process_memory_budget_mb
        self.forced_kill = False
        self.returncode: int | None = None
        self.final_logs = ""
        with ExitStack() as setup:
            self.port_reservations = LoopbackPortReservations(host)
            setup.callback(self.port_reservations.close)
            self.public_port, self.health_port = self.port_reservations.reserve_many(2)
            self.public_url = f"http://{host}:{self.public_port}"
            self.url = antfly_public_api_url(self.public_url)
            self.health_url = f"http://{host}:{self.health_port}"
            self.inference_api_url = inference_public_api_url(self.public_url)
            self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-standalone-e2e-")
            setup.callback(self.tempdir.cleanup)
            self.root = Path(self.tempdir.name)
            self.log_path = self.root / "server.log"
            self.log_file = setup.enter_context(self.log_path.open("w"))
            self.proc: subprocess.Popen[str] | None = None
            setup.pop_all()
        try:
            self._start()
        except BaseException:
            if not self.log_file.closed:
                self.stop()
            raise

    def _start(self) -> None:
        command = [
            self.binary,
            "standalone",
            "--host",
            self.host,
            "--port",
            str(self.public_port),
            "--health-port",
            str(self.health_port),
            "--control-tick-ms",
            "5",
            "--models-dir",
            str(self.models_dir),
            "--data-dir",
            str(self.root),
            "--replica-root-dir",
            str(self.root / "replicas"),
            "--replica-catalog-path",
            str(self.root / "catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / "snapshots"),
        ]
        for flag_name, value in (
            ("--inference-host-budget-mb", self.inference_budget_mb.get("host", 0)),
            ("--inference-backend-budget-mb", self.inference_budget_mb.get("backend", 0)),
            ("--inference-combined-budget-mb", self.inference_budget_mb.get("combined", 0)),
            ("--inference-kv-budget-mb", self.inference_budget_mb.get("kv", 0)),
            ("--inference-scratch-budget-mb", self.inference_budget_mb.get("scratch", 0)),
        ):
            if value > 0:
                command.extend([flag_name, str(value)])
        if self.process_memory_budget_mb > 0:
            command.extend(["--process-memory-budget-mb", str(self.process_memory_budget_mb)])
        self.proc = self.port_reservations.handoff_to(
            (self.public_port, self.health_port),
            lambda: subprocess.Popen(
                command,
                stdout=self.log_file,
                stderr=subprocess.STDOUT,
                cwd=REPO_ROOT,
            ),
        )
        if not _wait_for_server(self.url):
            logs = _read_log_tail(self.log_path)
            self.stop()
            raise RuntimeError(f"Standalone API server failed to start at {self.url}\n{logs}")
        if not _wait_for_server(self.public_url, path="/readyz"):
            logs = _read_log_tail(self.log_path)
            self.stop()
            raise RuntimeError(f"Standalone runtime failed readiness at {self.public_url}\n{logs}")

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def stop(self) -> None:
        self.port_reservations.close()
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=45)
            except subprocess.TimeoutExpired:
                self.forced_kill = True
                self.proc.kill()
                self.proc.wait()
        if self.proc is not None:
            self.returncode = self.proc.returncode
        self.proc = None
        self.final_logs = self.debug_logs()
        self.log_file.close()
        self.tempdir.cleanup()


def _warm_inference_generator(api_url: str, model_name: str) -> None:
    response = requests.post(
        f"{api_url}/generate",
        json={
            "model": model_name,
            "messages": [{"role": "user", "content": "Reply with ok."}],
            "max_tokens": 8,
            "temperature": 0,
        },
        timeout=300,
    )
    if response.status_code >= 400:
        raise AssertionError(
            f"generator warmup failed: {response.status_code} {response.text}"
        )


@pytest.fixture(scope="session")
def embedded_standalone_runtime():
    if not _integration_enabled("ANTFLY_INFERENCE_STANDALONE_TESTS"):
        pytest.skip("Set ANTFLY_INFERENCE_STANDALONE_TESTS=1 to run live inference standalone tests")

    binary = _resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.fail(
            f"Standalone inference was explicitly enabled but the Antfly binary was not found: {binary}"
        )

    models_dir = Path(
        os.environ.get("ANTFLY_INFERENCE_STANDALONE_MODELS_DIR", str(DEFAULT_INFERENCE_MODELS_DIR))
    ).expanduser().resolve()
    if not models_dir.exists():
        pytest.fail(
            "Standalone inference was explicitly enabled but its models directory was not found: "
            f"{models_dir}"
        )

    model_name = os.environ.get("ANTFLY_INFERENCE_STANDALONE_MODEL_NAME", DEFAULT_INFERENCE_MODEL_NAME)
    if not _model_exists(models_dir, model_name):
        pytest.fail(
            "Standalone inference was explicitly enabled but its generator model was not found under "
            f"{models_dir}. Pull it with: antfly inference pull hf:{_normalize_model_ref_for_path(model_name)}"
        )

    inference_budget_mb = {
        "host": _env_int("ANTFLY_INFERENCE_STANDALONE_HOST_BUDGET_MB", DEFAULT_INFERENCE_STANDALONE_HOST_BUDGET_MB),
        "backend": _env_int(
            "ANTFLY_INFERENCE_STANDALONE_BACKEND_BUDGET_MB", DEFAULT_INFERENCE_STANDALONE_BACKEND_BUDGET_MB
        ),
        "combined": _env_int(
            "ANTFLY_INFERENCE_STANDALONE_COMBINED_BUDGET_MB", DEFAULT_INFERENCE_STANDALONE_COMBINED_BUDGET_MB
        ),
        "kv": _env_int("ANTFLY_INFERENCE_STANDALONE_KV_BUDGET_MB", DEFAULT_INFERENCE_STANDALONE_KV_BUDGET_MB),
        "scratch": _env_int(
            "ANTFLY_INFERENCE_STANDALONE_SCRATCH_BUDGET_MB", DEFAULT_INFERENCE_STANDALONE_SCRATCH_BUDGET_MB
        ),
    }
    process_memory_budget_mb = _env_int(
        "ANTFLY_INFERENCE_STANDALONE_PROCESS_MEMORY_BUDGET_MB",
        DEFAULT_INFERENCE_STANDALONE_PROCESS_MEMORY_BUDGET_MB,
    )

    server = EmbeddedInferenceStandaloneServer(
        binary,
        models_dir,
        model_name,
        inference_budget_mb=inference_budget_mb,
        process_memory_budget_mb=process_memory_budget_mb,
    )
    warmup_performed = False
    try:
        if not _integration_enabled("ANTFLY_INFERENCE_STANDALONE_SKIP_GENERATOR_WARMUP"):
            try:
                _warm_inference_generator(server.inference_api_url, model_name)
            except Exception as exc:
                raise AssertionError(
                    f"standalone inference generator warmup failed for {model_name}: {exc}\n"
                    f"server logs:\n{server.debug_logs()}"
                ) from exc
            warmup_performed = True
        yield {
            "base_url": server.url,
            "public_url": server.public_url,
            "health_url": server.health_url,
            "inference_api_url": server.inference_api_url,
            "model": model_name,
            "models_dir": str(models_dir),
            "inference_budget_mb": inference_budget_mb,
            "process_memory_budget_mb": process_memory_budget_mb,
            "warmup_performed": warmup_performed,
            "logs": server.debug_logs,
        }
    finally:
        server.stop()
        if server.forced_kill or server.returncode != 0:
            pytest.fail(
                "standalone inference did not shut down cleanly "
                f"(forced_kill={server.forced_kill}, returncode={server.returncode})\n"
                f"last logs:\n{server.final_logs[-8000:]}"
            )


@pytest.fixture(scope="function")
def embedded_standalone_api(embedded_standalone_runtime):
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"
    base_url = embedded_standalone_runtime["base_url"]
    log_fn = embedded_standalone_runtime["logs"]

    class Api:
        def __init__(self, session: requests.Session, base_url: str):
            self.s = session
            self.url = base_url.rstrip("/")
            self._request_lock = threading.Lock()

        def _check(self, response: requests.Response) -> Any:
            if response.status_code >= 400:
                body = response.text.strip()
                logs = log_fn().strip()
                if body and logs:
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url} body={body}\nserver logs:\n{logs}",
                        response=response,
                    )
                if body:
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url} body={body}",
                        response=response,
                    )
                if logs:
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url}\nserver logs:\n{logs}",
                        response=response,
                    )
                response.raise_for_status()
            if not response.content:
                return {}
            return response.json()

        def create_table(self, table_name: str, *, num_shards: int = 1) -> dict:
            with self._request_lock:
                response = self.s.post(
                    f"{self.url}/tables/{table_name}",
                    json={"num_shards": num_shards},
                    timeout=30,
                )
            return self._check(response)

        def list_tables(self) -> list[dict]:
            with self._request_lock:
                response = self.s.get(f"{self.url}/tables", timeout=30)
            return self._check(response)

        def create_index(self, table_name: str, index_name: str, config: dict[str, object]) -> dict:
            with self._request_lock:
                response = self.s.post(
                    f"{self.url}/tables/{table_name}/indexes/{index_name}",
                    json=config,
                    timeout=30,
                )
            return self._check(response)

        def get_index(self, table_name: str, index_name: str) -> dict:
            with self._request_lock:
                response = self.s.get(
                    f"{self.url}/tables/{table_name}/indexes/{index_name}",
                    timeout=30,
                )
            return self._check(response)

        def delete_table(self, table_name: str) -> dict:
            with self._request_lock:
                response = self.s.delete(f"{self.url}/tables/{table_name}", timeout=30)
            return self._check(response)

        def lookup_key(self, table_name: str, key: str) -> dict:
            with self._request_lock:
                response = self.s.get(
                    f"{self.url}/tables/{table_name}/documents/{quote(key, safe='')}",
                    timeout=30,
                )
            return self._check(response)

        def batch_write(
            self,
            table_name: str,
            *,
            inserts: dict[str, dict] | None = None,
            deletes: list[str] | None = None,
            transforms: list[dict[str, object]] | None = None,
            sync_level: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {}
            if inserts:
                payload["inserts"] = inserts
            if deletes:
                payload["deletes"] = deletes
            if transforms:
                payload["transforms"] = transforms
            if sync_level is not None:
                payload["sync_level"] = sync_level
            with self._request_lock:
                response = self.s.post(f"{self.url}/tables/{table_name}/batch", json=payload, timeout=30)
            return self._check(response)

    yield Api(session, base_url)
    session.close()


@pytest.fixture(scope="function")
def embedded_standalone_cli(embedded_standalone_runtime):
    binary = _resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    env = os.environ.copy()
    env["ANTFLY_URL"] = embedded_standalone_runtime["public_url"]

    def run_cli(*args: str, check: bool = True, timeout_s: float = 180.0) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [binary] + list(args),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
            cwd=REPO_ROOT,
        )
        if check and result.returncode != 0:
            raise AssertionError(
                f"CLI failed (exit {result.returncode}): {' '.join([binary, *args])}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}\n"
                f"server logs:\n{embedded_standalone_runtime['logs']()}"
            )
        return result

    return run_cli


def _post_json_with_timeout(api, path: str, payload: dict, *, timeout_s: float) -> dict:
    response = api.s.post(f"{api.url}{path}", json=payload, timeout=timeout_s)
    return api._check(response)


def _parse_cli_json(stdout: str) -> dict | None:
    text = stdout.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def test_standalone_inference_process_envelope_composition(embedded_standalone_runtime):
    budget_mb = embedded_standalone_runtime["process_memory_budget_mb"]
    if budget_mb <= 0:
        pytest.skip("Set ANTFLY_INFERENCE_STANDALONE_PROCESS_MEMORY_BUDGET_MB to validate composition")

    assert embedded_standalone_runtime["warmup_performed"], "real generator warmup must complete"
    expected_bytes = budget_mb * 1024 * 1024
    logs = embedded_standalone_runtime["logs"]()
    assert "effective_source=explicit" in logs
    assert f"configured_limit_bytes={expected_bytes}" in logs
    assert f"effective_limit_bytes={expected_bytes}" in logs
    assert "inference resource policy ownership=external_required" in logs
    assert "process_memory_limit_provenance=explicit" in logs


def test_standalone_health_endpoints(embedded_standalone_runtime):
    health_url = embedded_standalone_runtime["health_url"]
    logs = embedded_standalone_runtime["logs"]

    # The dedicated health server may take a brief moment after the public
    # API comes up. Retry /healthz briefly before asserting.
    def healthz_ok() -> bool:
        try:
            r = requests.get(f"{health_url}/healthz", timeout=2)
        except requests.RequestException:
            return False
        return r.status_code == 200

    if not wait_until(lambda: True if healthz_ok() else None, timeout_s=15.0, interval_s=0.25):
        raise AssertionError(f"standalone health server did not come up at {health_url}\nlogs:\n{logs()}")

    healthz = requests.get(f"{health_url}/healthz", timeout=5)
    assert healthz.status_code == 200
    assert healthz.headers["Content-Type"].startswith("application/json")
    assert healthz.json() == {"status": "ok"}

    readyz = requests.get(f"{health_url}/readyz", timeout=5)
    assert readyz.status_code in (200, 503)
    assert readyz.headers["Content-Type"].startswith("application/json")
    assert readyz.json().get("status") in {"ready", "not_ready"}

    metrics = requests.get(f"{health_url}/metrics", timeout=5)
    assert metrics.status_code == 200
    assert metrics.headers["Content-Type"].startswith("text/plain")
    body = metrics.text
    # Core raft host metrics written by StandaloneHealthSource.
    assert "antfly_raft_hosted_groups" in body
    assert "antfly_raft_reconcile_rounds_total" in body
    # Managed service metrics.
    assert "antfly_service_queued_updates" in body
    assert "antfly_service_applied_updates_total" in body
    # Prometheus exposition format sanity.
    assert "# HELP antfly_raft_hosted_groups" in body
    assert "# TYPE antfly_raft_hosted_groups gauge" in body

    unknown = requests.get(f"{health_url}/does-not-exist", timeout=5)
    assert unknown.status_code == 404


def test_standalone_retired_recognize_route_returns_not_found(backup_api):
    """The unified public server must match the dedicated inference 404 contract."""
    public_url = backup_api.url.removesuffix("/db/v1")
    response = backup_api.s.post(
        f"{public_url}/ai/v1/recognize",
        json={"model": "antflydb/gliner2-base-v1", "texts": ["John works at Antfly."]},
        timeout=10,
    )

    assert response.status_code == 404
    assert response.json() == {"error": "NOT_FOUND", "message": "resource not found"}


def test_standalone_drop_tables_with_pending_embedded_embeddings(
    embedded_standalone_api,
    embedded_standalone_runtime,
):
    hot_tables = [f"standalone_drop_hot_{time.time_ns()}_{i}" for i in range(6)]
    survivor = f"standalone_drop_survivor_{time.time_ns()}"
    created_tables: set[str] = set()
    try:
        for table_name in [*hot_tables, survivor]:
            created = embedded_standalone_api.create_table(table_name, num_shards=1)
            created_tables.add(table_name)
            assert created["name"] == table_name
            assert (
                embedded_standalone_api.create_index(
                    table_name,
                    "semantic_idx",
                    {
                        "name": "semantic_idx",
                        "type": "embeddings",
                        "template": "{{title}}",
                        "dimension": 384,
                        "embedder": {
                            "provider": "antfly",
                            "model": "BAAI/bge-small-en-v1.5",
                        },
                    },
                )
                == {}
            )

        docs = {
            f"doc-{i:02d}": {
                "title": (
                    f"Document {i} has enough distinct words to keep the embedded "
                    "inference queue active while its table is retired."
                )
            }
            for i in range(50)
        }
        for table_name in hot_tables:
            batch = embedded_standalone_api.batch_write(table_name, inserts=docs)
            assert batch["inserted"] == len(docs)

        latest_index_statuses: dict[str, dict] = {}

        def observe_pending_embedding_work() -> dict | None:
            for table_name in hot_tables:
                try:
                    detail = embedded_standalone_api.get_index(table_name, "semantic_idx")
                except (requests.RequestException, ValueError):
                    continue
                latest_index_statuses[table_name] = detail
                status = detail.get("status", {})
                coverage = status.get("coverage", {})
                replay_applied = int(status.get("replay_applied_sequence", 0))
                replay_target = int(status.get("replay_target_sequence", 0))
                if (
                    int(coverage.get("pending", 0)) > 0
                    or replay_applied < replay_target
                    or status.get("catch_up_active") is True
                    or status.get("backfill_active") is True
                ):
                    return {"table": table_name, "status": detail}
            return None

        pending = wait_until(
            observe_pending_embedding_work,
            timeout_s=15.0,
            interval_s=0.05,
        )
        if pending is None:
            raise AssertionError(
                "standalone table-drop workload never exposed pending embedding work\n"
                f"last index statuses:\n{json.dumps(latest_index_statuses, indent=2, sort_keys=True)}\n"
                f"server logs:\n{embedded_standalone_runtime['logs']()}"
            )

        for table_name in hot_tables:
            embedded_standalone_api.delete_table(table_name)

        def dropped_tables_are_absent() -> bool:
            try:
                names = {table["name"] for table in embedded_standalone_api.list_tables()}
            except (requests.RequestException, ValueError):
                return False
            return not names.intersection(hot_tables)

        if not wait_until(
            lambda: True if dropped_tables_are_absent() else None,
            timeout_s=30.0,
            interval_s=0.1,
        ):
            raise AssertionError(
                "dropped standalone tables remained catalog-visible\n"
                f"server logs:\n{embedded_standalone_runtime['logs']()}"
            )

        survivor_batch = embedded_standalone_api.batch_write(
            survivor,
            inserts={"doc:survivor": {"title": "surviving table remains writable"}},
            sync_level="write",
        )
        assert survivor_batch["inserted"] == 1
        survivor_doc = embedded_standalone_api.lookup_key(survivor, "doc:survivor")
        assert survivor_doc["title"] == "surviving table remains writable"

        response = requests.get(f"{embedded_standalone_runtime['base_url']}/status", timeout=30)
        response.raise_for_status()
    finally:
        for table_name in sorted(created_tables):
            try:
                embedded_standalone_api.delete_table(table_name)
            except (requests.RequestException, ValueError):
                pass


def test_standalone_retrieval_generation_with_live_inference(embedded_standalone_api, embedded_standalone_runtime):
    table_name = f"standalone_generation_{time.time_ns()}"
    created = embedded_standalone_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_standalone_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "retrieval agents combine keyword search with generated answers",
            },
            "doc:b": {
                "title": "beta",
                "body": "secondary document",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    payload = {
        "query": "Summarize the retrieval document",
        "stream": False,
        "generator": {
            "provider": "antfly",
            "model": embedded_standalone_runtime["model"],
            "max_tokens": 32,
            "temperature": 0,
        },
        "steps": {
            "generation": {
                "enabled": True,
            }
        },
        "queries": [
            {
                "table": table_name,
                "full_text_search": {"query": "body:retrieval"},
                "limit": 5,
            }
        ],
    }

    result = wait_until(
        lambda: (
            response
            if (
                (response := _post_json_with_timeout(
                    embedded_standalone_api,
                    "/agents/retrieval",
                    payload,
                    timeout_s=180,
                )).get("hits")
                and response.get("generation")
            )
            else None
        ),
        timeout_s=240.0,
        interval_s=1.0,
    )
    assert result is not None
    assert result["status"] == "completed"
    assert result["strategy_used"] == "bm25"
    assert _hit_ids(result) == ["doc:a"]
    assert result["model"] == embedded_standalone_runtime["model"]
    assert isinstance(result["generation"], str)
    assert result["generation"].strip()
    assert result["steps"][-1]["name"] == "generation"


def test_standalone_retrieval_streaming_with_live_inference(embedded_standalone_api, embedded_standalone_runtime):
    table_name = f"standalone_streaming_{time.time_ns()}"
    created = embedded_standalone_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_standalone_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "alpha",
                "body": "retrieval systems can stream search hits and generated answers",
            },
            "doc:b": {
                "title": "beta",
                "body": "secondary document",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    response = embedded_standalone_api.s.post(
        f"{embedded_standalone_api.url}/agents/retrieval",
        json={
            "query": "Explain the retrieval system",
            "stream": True,
            "generator": {
                "provider": "antfly",
                "model": embedded_standalone_runtime["model"],
                "max_tokens": 64,
                "temperature": 0,
            },
            "steps": {
                "generation": {
                    "enabled": True,
                }
            },
            "queries": [
                {
                    "table": table_name,
                    "full_text_search": {"query": "body:retrieval"},
                    "limit": 5,
                }
            ],
        },
        timeout=180,
    )
    if response.status_code >= 400:
        logs = embedded_standalone_runtime["logs"]()
        raise requests.HTTPError(
            f"{response.status_code} {response.reason} body={response.text}\nserver logs:\n{logs}",
            response=response,
        )

    assert response.headers["Content-Type"].startswith("text/event-stream")
    assert '"_id":"doc:a"' in response.text
    assert "event: hit" in response.text
    assert "event: generation" in response.text
    assert "event: done" in response.text

    events = _parse_sse_events(response.text)
    generation_chunks = [data for event, data in events if event == "generation"]
    assert generation_chunks
    assert any(isinstance(chunk, str) and chunk.strip() for chunk in generation_chunks)


def test_standalone_cli_retrieval_non_streaming_with_live_inference(embedded_standalone_api, embedded_standalone_cli, embedded_standalone_runtime):
    table_name = f"standalone_cli_generation_{time.time_ns()}"
    created = embedded_standalone_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_standalone_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "Korean history overview",
                "body": "Korean history includes the Three Kingdoms period and the Joseon dynasty.",
            },
            "doc:b": {
                "title": "noise",
                "body": "secondary document",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    generator_json = json.dumps(
        {
            "provider": "antfly",
            "model": embedded_standalone_runtime["model"],
            "max_tokens": 96,
            "temperature": 0,
        }
    )

    result = wait_until(
        lambda: (
            parsed
            if (
                (completed := embedded_standalone_cli(
                    "agents",
                    "retrieval",
                    "--table",
                    table_name,
                    "--full-text-search",
                    "body:Korean",
                    "--prompt",
                    "What are the major events in Korean history?",
                    "--generator",
                    generator_json,
                    "--classify",
                    "--reasoning",
                    "--generate",
                    "--followup",
                    "--confidence",
                    "--no-streaming",
                    check=False,
                    timeout_s=240.0,
                )).returncode
                == 0
                and (parsed := _parse_cli_json(completed.stdout))
                and parsed.get("generation")
            )
            else None
        ),
        timeout_s=240.0,
        interval_s=1.0,
    )
    assert result is not None
    assert result["status"] == "completed"
    assert result["strategy_used"] == "bm25"
    assert result["model"] == embedded_standalone_runtime["model"]
    assert _hit_ids(result) == ["doc:a"]
    assert isinstance(result["generation"], str)
    assert result["generation"].strip()
    assert result["classification"]["route_type"]
    assert result["classification"]["strategy"]
    assert result["classification"]["reasoning"]
    assert result["generation_confidence"] > 0
    assert result["context_relevance"] > 0
    assert result["followup_questions"]


def test_standalone_cli_retrieval_streaming_with_live_inference(embedded_standalone_api, embedded_standalone_cli, embedded_standalone_runtime):
    table_name = f"standalone_cli_streaming_{time.time_ns()}"
    created = embedded_standalone_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_standalone_api.batch_write(
        table_name,
        inserts={
            "doc:a": {
                "title": "retrieval",
                "body": "retrieval systems can classify questions and stream generated answers",
            },
            "doc:b": {
                "title": "noise",
                "body": "secondary document",
            },
        },
        sync_level="full_index",
    )
    assert batch["inserted"] == 2

    generator_json = json.dumps(
        {
            "provider": "antfly",
            "model": embedded_standalone_runtime["model"],
            "max_tokens": 96,
            "temperature": 0,
        }
    )

    result = embedded_standalone_cli(
        "agents",
        "retrieval",
        "--table",
        table_name,
        "--full-text-search",
        "body:retrieval",
        "--prompt",
        "Explain the retrieval system",
        "--generator",
        generator_json,
        "--classify",
        "--reasoning",
        "--generate",
        "--followup",
        timeout_s=240.0,
    )

    assert result.stdout
    assert "event: hit" in result.stdout
    assert "event: reasoning" in result.stdout
    assert "event: generation" in result.stdout
    assert "event: followup" in result.stdout
    assert "event: done" in result.stdout

    events = _parse_sse_events(result.stdout)
    assert any(event == "hit" for event, _ in events)
    assert any(event == "reasoning" and isinstance(data, str) and data.strip() for event, data in events)
    assert any(event == "generation" and isinstance(data, str) and data.strip() for event, data in events)
    assert any(event == "followup" and isinstance(data, str) and data.strip() for event, data in events)
