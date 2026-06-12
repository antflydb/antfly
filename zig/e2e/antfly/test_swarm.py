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

"""Live swarm integration tests using antfly swarm with embedded inference."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import requests

from conftest import antfly_public_api_url, inference_public_api_url
from helpers import wait_until


pytestmark = pytest.mark.swarm_integration

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ANTFLY_BIN = REPO_ROOT / "zig-out" / "bin" / "antfly"
DEFAULT_INFERENCE_MODELS_DIR = Path("~/.antfly/inference/models").expanduser()
DEFAULT_INFERENCE_MODEL_NAME = "ggml-org/gemma-4-e2b-it-gguf"
DEFAULT_INFERENCE_SWARM_HOST_BUDGET_MB = 0
DEFAULT_INFERENCE_SWARM_BACKEND_BUDGET_MB = 12288
DEFAULT_INFERENCE_SWARM_COMBINED_BUDGET_MB = 16384
DEFAULT_INFERENCE_SWARM_KV_BUDGET_MB = 0
DEFAULT_INFERENCE_SWARM_SCRATCH_BUDGET_MB = 0


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


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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
    return [
        models_dir / normalized,
        models_dir / "generators" / normalized,
    ]


def _model_exists(models_dir: Path, model_name: str) -> bool:
    return any(path.exists() for path in _candidate_model_dirs(models_dir, model_name))


class EmbeddedInferenceSwarmServer:
    def __init__(
        self,
        binary: str,
        models_dir: Path,
        model_name: str,
        *,
        inference_budget_mb: dict[str, int] | None = None,
        host: str = "127.0.0.1",
    ):
        self.binary = binary
        self.models_dir = models_dir
        self.model_name = model_name
        self.host = host
        self.inference_budget_mb = inference_budget_mb or {}
        self.public_port = _find_free_port()
        self.health_port = _find_free_port()
        self.public_url = f"http://{host}:{self.public_port}"
        self.url = antfly_public_api_url(self.public_url)
        self.health_url = f"http://{host}:{self.health_port}"
        self.inference_api_url = inference_public_api_url(self.public_url)
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-swarm-e2e-")
        self.root = Path(self.tempdir.name)
        self.log_path = self.root / "server.log"
        self.log_file = self.log_path.open("w")
        self.proc: subprocess.Popen[str] | None = None
        self._start()

    def _start(self) -> None:
        command = [
            self.binary,
            "swarm",
            "--host",
            self.host,
            "--port",
            str(self.public_port),
            "--health-port",
            str(self.health_port),
            "--tick-ms",
            "5",
            "--models-dir",
            str(self.models_dir),
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
        self.proc = subprocess.Popen(
            command,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
        if not _wait_for_server(self.url):
            self.stop()
            logs = _read_log_tail(self.log_path)
            raise RuntimeError(f"Swarm API server failed to start at {self.url}\n{logs}")
        if not _wait_for_server(self.inference_api_url, timeout_s=120.0, path="/models"):
            self.stop()
            logs = _read_log_tail(self.log_path)
            raise RuntimeError(f"Embedded inference server failed to start at {self.inference_api_url}\n{logs}")

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None
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
    response.raise_for_status()


@pytest.fixture(scope="session")
def embedded_swarm_runtime():
    if not _integration_enabled("ANTFLY_INFERENCE_SWARM_TESTS"):
        pytest.skip("Set ANTFLY_INFERENCE_SWARM_TESTS=1 to run live inference swarm tests")

    binary = _resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary}")

    models_dir = Path(
        os.environ.get("ANTFLY_INFERENCE_SWARM_MODELS_DIR", str(DEFAULT_INFERENCE_MODELS_DIR))
    ).expanduser().resolve()
    if not models_dir.exists():
        pytest.skip(f"Antfly inference models directory not found: {models_dir}")

    model_name = os.environ.get("ANTFLY_INFERENCE_SWARM_MODEL_NAME", DEFAULT_INFERENCE_MODEL_NAME)
    if not _model_exists(models_dir, model_name):
        pytest.skip(
            "Antfly inference generator model not found under "
            f"{models_dir}. Pull it with: antfly inference pull hf:{_normalize_model_ref_for_path(model_name)}"
        )

    inference_budget_mb = {
        "host": _env_int("ANTFLY_INFERENCE_SWARM_HOST_BUDGET_MB", DEFAULT_INFERENCE_SWARM_HOST_BUDGET_MB),
        "backend": _env_int(
            "ANTFLY_INFERENCE_SWARM_BACKEND_BUDGET_MB", DEFAULT_INFERENCE_SWARM_BACKEND_BUDGET_MB
        ),
        "combined": _env_int(
            "ANTFLY_INFERENCE_SWARM_COMBINED_BUDGET_MB", DEFAULT_INFERENCE_SWARM_COMBINED_BUDGET_MB
        ),
        "kv": _env_int("ANTFLY_INFERENCE_SWARM_KV_BUDGET_MB", DEFAULT_INFERENCE_SWARM_KV_BUDGET_MB),
        "scratch": _env_int(
            "ANTFLY_INFERENCE_SWARM_SCRATCH_BUDGET_MB", DEFAULT_INFERENCE_SWARM_SCRATCH_BUDGET_MB
        ),
    }

    server = EmbeddedInferenceSwarmServer(
        binary,
        models_dir,
        model_name,
        inference_budget_mb=inference_budget_mb,
    )
    _warm_inference_generator(server.inference_api_url, model_name)
    yield {
        "base_url": server.url,
        "public_url": server.public_url,
        "health_url": server.health_url,
        "inference_api_url": server.inference_api_url,
        "model": model_name,
        "models_dir": str(models_dir),
        "inference_budget_mb": inference_budget_mb,
        "logs": server.debug_logs,
    }
    server.stop()


@pytest.fixture(scope="function")
def embedded_swarm_api(embedded_swarm_runtime):
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"
    base_url = embedded_swarm_runtime["base_url"]
    log_fn = embedded_swarm_runtime["logs"]

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
def embedded_swarm_cli(embedded_swarm_runtime):
    binary = _resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    env = os.environ.copy()
    env["ANTFLY_URL"] = embedded_swarm_runtime["public_url"]

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
                f"server logs:\n{embedded_swarm_runtime['logs']()}"
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


def test_swarm_health_endpoints(embedded_swarm_runtime):
    health_url = embedded_swarm_runtime["health_url"]
    logs = embedded_swarm_runtime["logs"]

    # The dedicated health server may take a brief moment after the public
    # API comes up. Retry /healthz briefly before asserting.
    def healthz_ok() -> bool:
        try:
            r = requests.get(f"{health_url}/healthz", timeout=2)
        except requests.RequestException:
            return False
        return r.status_code == 200

    if not wait_until(lambda: True if healthz_ok() else None, timeout_s=15.0, interval_s=0.25):
        raise AssertionError(f"swarm health server did not come up at {health_url}\nlogs:\n{logs()}")

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
    # Core raft host metrics written by SwarmHealthSource.
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


def test_swarm_retrieval_generation_with_live_inference(embedded_swarm_api, embedded_swarm_runtime):
    table_name = f"swarm_generation_{time.time_ns()}"
    created = embedded_swarm_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_swarm_api.batch_write(
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
            "model": embedded_swarm_runtime["model"],
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
                    embedded_swarm_api,
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
    assert result["model"] == embedded_swarm_runtime["model"]
    assert isinstance(result["generation"], str)
    assert result["generation"].strip()
    assert result["steps"][-1]["name"] == "generation"


def test_swarm_retrieval_streaming_with_live_inference(embedded_swarm_api, embedded_swarm_runtime):
    table_name = f"swarm_streaming_{time.time_ns()}"
    created = embedded_swarm_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_swarm_api.batch_write(
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

    response = embedded_swarm_api.s.post(
        f"{embedded_swarm_api.url}/agents/retrieval",
        json={
            "query": "Explain the retrieval system",
            "stream": True,
            "generator": {
                "provider": "antfly",
                "model": embedded_swarm_runtime["model"],
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
        logs = embedded_swarm_runtime["logs"]()
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


def test_swarm_cli_retrieval_non_streaming_with_live_inference(embedded_swarm_api, embedded_swarm_cli, embedded_swarm_runtime):
    table_name = f"swarm_cli_generation_{time.time_ns()}"
    created = embedded_swarm_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_swarm_api.batch_write(
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
            "model": embedded_swarm_runtime["model"],
            "max_tokens": 96,
            "temperature": 0,
        }
    )

    result = wait_until(
        lambda: (
            parsed
            if (
                (completed := embedded_swarm_cli(
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
    assert result["model"] == embedded_swarm_runtime["model"]
    assert _hit_ids(result) == ["doc:a"]
    assert isinstance(result["generation"], str)
    assert result["generation"].strip()
    assert result["classification"]["route_type"]
    assert result["classification"]["strategy"]
    assert result["classification"]["reasoning"]
    assert result["generation_confidence"] > 0
    assert result["context_relevance"] > 0
    assert result["followup_questions"]


def test_swarm_cli_retrieval_streaming_with_live_inference(embedded_swarm_api, embedded_swarm_cli, embedded_swarm_runtime):
    table_name = f"swarm_cli_streaming_{time.time_ns()}"
    created = embedded_swarm_api.create_table(table_name, num_shards=1)
    assert created["name"] == table_name

    batch = embedded_swarm_api.batch_write(
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
            "model": embedded_swarm_runtime["model"],
            "max_tokens": 96,
            "temperature": 0,
        }
    )

    result = embedded_swarm_cli(
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
