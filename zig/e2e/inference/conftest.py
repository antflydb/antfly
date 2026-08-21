# Copyright 2026 Antfly, Inc.
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

"""Shared fixtures for inference E2E tests.

Usage:
    # Against a running server:
    ANTFLY_INFERENCE_URL=http://localhost:8080 uv run --project e2e/inference pytest e2e/inference

    # Start server automatically:
    ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/inference pytest e2e/inference

    # Custom AI and ML directories:
    ANTFLY_INFERENCE_MODELS_DIR=/path/to/models uv run --project e2e/inference pytest e2e/inference
    ANTFLY_INFERENCE_ML_DIR=/path/to/ml uv run --project e2e/inference pytest e2e/inference

    # Optional inference server model-cache limit:
    ANTFLY_INFERENCE_MAX_LOADED_MODELS=1 uv run --project e2e/inference pytest e2e/inference

    # Lazily pull missing models with a local antfly binary (opt-in):
    ANTFLY_INFERENCE_DOWNLOAD=1 uv run --project e2e/inference pytest e2e/inference

    # Against a hosted inference endpoint:
    ANTFLY_INFERENCE_URL=https://inference.example.com ANTFLY_INFERENCE_TOKEN=... uv run --project e2e/inference pytest e2e/inference
"""

import math
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

import pytest
import requests

from .models import (
    DEFAULT_EXTRACTOR_MODEL,
    bootstrap_models_for_listing,
    inference_command,
    maybe_pull_missing_model,
    ml_dir,
    models_dir,
)

API_PREFIX = "/ai/v1"
ML_API_PREFIX = "/ml/v1"
DEFAULT_REQUEST_TIMEOUT = float(os.environ.get("ANTFLY_INFERENCE_REQUEST_TIMEOUT", "30"))
SERVER_OUTPUT_LIMIT = 64 * 1024
CAPACITY_RETRY_TIMEOUT = float(
    os.environ.get("ANTFLY_INFERENCE_CAPACITY_RETRY_TIMEOUT", "30")
)
CAPACITY_RETRY_MAX_ATTEMPTS = int(
    os.environ.get("ANTFLY_INFERENCE_CAPACITY_RETRY_MAX_ATTEMPTS", "5")
)
MIN_CAPACITY_RETRY_DELAY = 0.05
MAX_CAPACITY_RETRY_DELAY = 5.0
_LOCAL_SERVERS_BY_URL: dict[str, "InferenceServer"] = {}


def env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def api_path(path: str) -> str:
    """Resolve bare API paths against the current antfly prefix."""

    if path.startswith(ML_API_PREFIX + "/") or path == ML_API_PREFIX:
        return path
    if path.startswith(API_PREFIX + "/") or path == API_PREFIX:
        return path
    if path == "/predict" or path.startswith("/predict/"):
        return ML_API_PREFIX + path
    if path.startswith("/"):
        return API_PREFIX + path
    return API_PREFIX + "/" + path


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(url: str, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{url}/healthz", timeout=2)
            if r.ok:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.5)
    return False


def _response_json(response) -> dict:
    if not response.headers.get("content-type", "").startswith("application/json"):
        return {}
    try:
        value = response.json()
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def capacity_retry_delay(response, fallback_delay: float) -> float | None:
    """Return the bounded retry delay for an admission rejection, if any."""

    if response.status_code != 503:
        return None
    body = _response_json(response)
    if body.get("error") != "MODEL_RESOURCE_BUSY" or body.get("retryable") is not True:
        return None
    retry_after_ms = body.get("retry_after_ms")
    if type(retry_after_ms) in (int, float):
        delay = float(retry_after_ms) / 1000
    else:
        try:
            delay = float(response.headers["Retry-After"])
        except (KeyError, TypeError, ValueError):
            delay = fallback_delay
    if not math.isfinite(delay) or delay < 0:
        delay = fallback_delay
    return min(max(delay, MIN_CAPACITY_RETRY_DELAY), MAX_CAPACITY_RETRY_DELAY)


def retry_transient_capacity(
    response,
    send,
    timeout: float = CAPACITY_RETRY_TIMEOUT,
    max_attempts: int = CAPACITY_RETRY_MAX_ATTEMPTS,
    *,
    clock=time.monotonic,
    sleeper=time.sleep,
):
    """Retry requests rejected before execution by transient admission pressure."""

    deadline = clock() + max(0.0, timeout)
    fallback_delay = 0.25
    attempts = 0
    while (delay := capacity_retry_delay(response, fallback_delay)) is not None:
        if attempts >= max(0, max_attempts):
            break
        now = clock()
        if now + delay > deadline:
            break
        response.close()
        sleeper(delay)
        response = send()
        attempts += 1
        fallback_delay = min(fallback_delay * 2, MAX_CAPACITY_RETRY_DELAY)
    return response


class InferenceServer:
    """Manages a local inference server process."""

    def __init__(
        self,
        command_prefix: list[str],
        models_path: str,
        ml_path: str,
        host: str,
        port: int,
        max_loaded_models: str | None = None,
        extra_env: dict[str, str] | None = None,
    ):
        self.url = f"http://{host}:{port}"
        self.failure_reported = False
        self.http_failure_reported = False
        self._diagnostic_lock = threading.Lock()
        self.output = tempfile.TemporaryFile(mode="w+b")
        self.proc = subprocess.Popen(
            [
                *command_prefix,
                "run",
                "--host",
                host,
                "--port",
                str(port),
                "--models-dir",
                models_path,
                "--ml-dir",
                ml_path,
                *(
                    ["--max-loaded-models", max_loaded_models]
                    if max_loaded_models is not None
                    else []
                ),
            ],
            stdout=self.output,
            stderr=subprocess.STDOUT,
            env={**os.environ, **extra_env} if extra_env else None,
        )
        if not wait_for_server(self.url):
            self.stop(close_output=False)
            out = self.read_output()
            self.output.close()
            raise RuntimeError(f"Server failed to start at {self.url}\n{out}")

    def read_output(self) -> str:
        self.output.flush()
        descriptor = self.output.fileno()
        size = os.fstat(descriptor).st_size
        offset = max(0, size - SERVER_OUTPUT_LIMIT)
        if hasattr(os, "pread"):
            data = os.pread(descriptor, SERVER_OUTPUT_LIMIT, offset)
        elif self.proc.poll() is not None:  # pragma: no cover - Windows fallback after exit.
            position = self.output.tell()
            try:
                self.output.seek(offset)
                data = self.output.read(SERVER_OUTPUT_LIMIT)
            finally:
                self.output.seek(position)
        else:  # pragma: no cover - never move a live child's shared file offset.
            return "<output tail unavailable while this platform's server is running>"
        return data.decode(errors="replace")

    def failure_diagnostic(self) -> str:
        returncode = self.proc.poll()
        if returncode is None:
            status = "still running"
        elif returncode < 0:
            try:
                signal_name = signal.Signals(-returncode).name
            except ValueError:
                signal_name = "unknown signal"
            status = f"terminated by {signal_name} ({returncode})"
        else:
            status = f"exited with status {returncode}"
        return f"Local inference server {status}. Output tail:\n{self.read_output()}"

    def report_http_failure_once(self) -> None:
        """Expose backend logs for a live local server without moving its write offset."""

        with self._diagnostic_lock:
            if self.http_failure_reported:
                return
            self.http_failure_reported = True
            print(self.failure_diagnostic(), file=sys.stderr, flush=True)

    def stop(self, close_output: bool = True):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        if close_output:
            self.output.close()


@pytest.fixture(scope="session")
def base_url():
    """Return the base URL of the inference server under test.

    If ANTFLY_INFERENCE_URL is set, use it directly (external server).
    Otherwise, start a local server from ANTFLY_BIN.
    """
    url = env_first("ANTFLY_INFERENCE_URL")
    if url:
        url = url.rstrip("/")
        if not wait_for_server(url, timeout=10):
            pytest.skip(f"Server at {url} is not reachable")
        yield url
        return

    try:
        command_prefix = inference_command()
    except RuntimeError:
        pytest.skip("Set ANTFLY_INFERENCE_URL or ANTFLY_BIN to run E2E tests")

    models_path = str(models_dir())
    ml_path = str(ml_dir())

    port = find_free_port()
    server = InferenceServer(
        command_prefix,
        models_path,
        ml_path,
        "127.0.0.1",
        port,
        max_loaded_models=env_first("ANTFLY_INFERENCE_MAX_LOADED_MODELS"),
    )
    _LOCAL_SERVERS_BY_URL[server.url] = server
    yield server.url
    _LOCAL_SERVERS_BY_URL.pop(server.url, None)
    unexpected_exit = server.proc.poll() is not None
    diagnostic = server.failure_diagnostic() if unexpected_exit else None
    server.stop(close_output=False)
    server.output.close()
    if unexpected_exit and not server.failure_reported:
        pytest.fail(diagnostic, pytrace=False)


@pytest.fixture(scope="session")
def api(base_url):
    """Return a requests.Session configured for the inference API."""
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    token = env_first("ANTFLY_INFERENCE_TOKEN")
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    def _check(r):
        """Raise for status, skipping only explicit model-unavailable errors."""
        if r.status_code == 404:
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            if body.get("error") == "MODEL_NOT_FOUND":
                pytest.skip(f"Model unavailable: {body.get('message', 'model not found')}")
        if r.status_code == 400:
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            err = body.get("error", "")
            if "INVALID_MODEL" in err or "MODEL_NOT_FOUND" in err:
                pytest.skip(f"Model unavailable: {body.get('message', err)}")
        try:
            r.raise_for_status()
        except requests.HTTPError as exc:
            raise AssertionError(f"{exc}\nresponse body: {r.text[:2000]}") from exc

    class Api:
        def __init__(self, session, base_url):
            self.s = session
            self.url = base_url

        def _request(
            self,
            method: str,
            path: str,
            *,
            json=None,
            retry_on_missing_model: bool = True,
            **kwargs,
        ):
            request = getattr(self.s, method)
            normalized_path = api_path(path)
            kwargs.setdefault("timeout", DEFAULT_REQUEST_TIMEOUT)
            local_server = _LOCAL_SERVERS_BY_URL.get(self.url)

            def send():
                try:
                    return request(f"{self.url}{normalized_path}", json=json, **kwargs)
                except requests.RequestException as exc:
                    if (
                        local_server is not None
                        and local_server.proc.poll() is not None
                    ):
                        local_server.failure_reported = True
                        raise AssertionError(local_server.failure_diagnostic()) from exc
                    raise

            response = send()
            if retry_on_missing_model and maybe_pull_missing_model(
                normalized_path, json, response
            ):
                response.close()
                response = send()
            response = retry_transient_capacity(response, send)
            if response.status_code >= 500 and local_server is not None:
                local_server.report_http_failure_once()
            return response

        def post(self, path: str, json=None, **kwargs):
            return self._request("post", path, json=json, **kwargs)

        def get(self, path: str, **kwargs):
            return self._request("get", path, **kwargs)

        def embed(self, input, model: str = "BAAI/bge-small-en-v1.5"):
            r = self.post("/embed", json={"model": model, "input": input})
            _check(r)
            return r.json()

        def rerank(self, query: str, documents: list[str], model: str = ""):
            r = self.post("/rerank", json={"model": model, "query": query, "prompts": documents})
            _check(r)
            return r.json()

        def chunk(self, text: str, model: str = "", **kwargs):
            config = {}
            if model:
                config["model"] = model
            config.update(kwargs)
            body: dict = {"input": text}
            if config:
                body["config"] = config
            r = self.post("/chunk", json=body)
            _check(r)
            return r.json()

        def generate(self, messages: list[dict], model: str = "", stream: bool = False, **kwargs):
            body = {"model": model, "messages": messages, "stream": stream, **kwargs}
            if stream:
                r = self.post("/generate", json=body, stream=True)
                _check(r)
                return r
            r = self.post("/generate", json=body)
            _check(r)
            return r.json()

        def chat(self, messages: list[dict], model: str = "", stream: bool = False, **kwargs):
            body = {"model": model, "messages": messages, "stream": stream, **kwargs}
            if stream:
                r = self.post("/chat/completions", json=body, stream=True)
                _check(r)
                return r
            r = self.post("/chat/completions", json=body)
            _check(r)
            return r.json()

        def classify(self, text: list[str], labels: list[str], model: str = "", **kwargs):
            multi_label = bool(kwargs.pop("multi_label", False))
            hypothesis_template = kwargs.pop("hypothesis_template", None)
            top_k = kwargs.pop("top_k", None)
            threshold = kwargs.pop("threshold", None)
            include_confidence = bool(kwargs.pop("include_confidence", True))
            if kwargs:
                raise TypeError(f"unsupported classify options: {sorted(kwargs)}")
            classification_schema = {
                "name": "classification",
                "labels": labels,
                "multi_label": multi_label,
            }
            if hypothesis_template is not None:
                classification_schema["hypothesis_template"] = hypothesis_template
            if top_k is not None:
                classification_schema["top_k"] = top_k
            body = {
                "model": model or "cross-encoder/nli-distilroberta-base",
                "inputs": [
                    {"id": f"input-{index}", "content": value}
                    for index, value in enumerate(text)
                ],
                "schema": {"classifications": [classification_schema]},
                "options": {"include_confidence": include_confidence},
            }
            if threshold is not None:
                body["options"]["threshold"] = threshold
            r = self.post("/extract", json=body)
            _check(r)
            return r.json()

        def extract_entities(self, text: list[str], model: str = "", labels: list[str] | None = None, relations: list[str | dict] | None = None, **kwargs):
            body: dict = {
                "model": model or DEFAULT_EXTRACTOR_MODEL,
                "inputs": [{"content": value} for value in text],
                "schema": {},
            }
            if labels is not None:
                body["schema"]["entities"] = labels
            if relations is not None:
                body["schema"]["relations"] = [
                    value if isinstance(value, dict) else {"type": value}
                    for value in relations
                ]
            if kwargs:
                body["options"] = kwargs
            r = self.post("/extract", json=body)
            _check(r)
            return r.json()

        def rewrite(self, text: list[str], model: str = "", **kwargs):
            body = {"model": model, "inputs": text, **kwargs}
            r = self.post("/rewrite", json=body)
            _check(r)
            return r.json()

        def read(self, images: list[str], model: str = "antflydb/florence-2-base", prompt: str = "", **kwargs):
            # Convert plain URL strings to ImageURL objects {url: "..."}
            image_objs = [{"url": img} if isinstance(img, str) else img for img in images]
            body = {"model": model, "images": image_objs, "prompt": prompt, **kwargs}
            r = self.post("/read", json=body)
            _check(r)
            return r.json()

        def transcribe(self, audio: str, model: str = "", **kwargs):
            body = {"model": model, "audio": audio, **kwargs}
            r = self.post("/transcribe", json=body)
            _check(r)
            return r.json()

        def extract(self, texts: list[str] | None = None, images: list[str] | None = None, schema: dict | None = None, model: str = "", **kwargs):
            structures = {}
            for structure_name, field_defs in (schema or {}).items():
                fields = {}
                for field_def in field_defs:
                    parts = field_def.split("::")
                    field_name = parts[0]
                    field_type = "list" if "list" in parts[1:] or "array" in parts[1:] else "str"
                    fields[field_name] = field_type
                structures[structure_name] = {"fields": fields}
            inputs = []
            if texts is not None:
                inputs.extend({"content": text} for text in texts)
            if images is not None:
                inputs.extend({"content": [{"type": "image_url", "image_url": {"url": image}}]} for image in images)
            body = {
                "model": model,
                "inputs": inputs,
                "schema": {"structures": structures},
            }
            if kwargs:
                body["options"] = kwargs
            r = self.post("/extract", json=body)
            _check(r)
            return r.json()

        def models(self):
            r = self.get("/models", retry_on_missing_model=False)
            _check(r)
            payload = r.json()
            if bootstrap_models_for_listing(payload):
                r = self.get("/models", retry_on_missing_model=False)
                _check(r)
                payload = r.json()
            return payload

        def readyz(self):
            r = requests.get(f"{self.url}/readyz", timeout=10)
            _check(r)
            return r.json()

    yield Api(session, base_url)
    session.close()


@pytest.fixture(scope="session")
def openai_client(base_url):
    openai = pytest.importorskip("openai")
    return openai.OpenAI(base_url=f"{base_url}{API_PREFIX}", api_key="unused")


def pytest_configure(config):
    """Model downloads are handled lazily by the request helpers when enabled."""
    if os.environ.get("ANTFLY_INFERENCE_DOWNLOAD") == "1":
        print("ANTFLY_INFERENCE_DOWNLOAD=1: missing E2E models will be fetched with `antfly inference pull` on demand")
