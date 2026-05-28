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

    # Custom models directory:
    ANTFLY_INFERENCE_MODELS_DIR=/path/to/models uv run --project e2e/inference pytest e2e/inference

    # Lazily pull missing models with a local antfly binary (opt-in):
    ANTFLY_INFERENCE_DOWNLOAD=1 uv run --project e2e/inference pytest e2e/inference

    # Against a hosted inference endpoint:
    ANTFLY_INFERENCE_URL=https://inference.example.com ANTFLY_INFERENCE_TOKEN=... uv run --project e2e/inference pytest e2e/inference
"""

import os
import signal
import socket
import subprocess
import time

import pytest
import requests

from .models import bootstrap_models_for_listing, inference_command, maybe_pull_missing_model, models_dir

API_PREFIX = "/ml/v1"


def env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def api_path(path: str) -> str:
    """Resolve bare API paths against the current antfly prefix."""

    if path.startswith(API_PREFIX + "/") or path == API_PREFIX:
        return path
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
            r = requests.get(f"{url}/readyz", timeout=2)
            if r.ok:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    return False


class InferenceServer:
    """Manages a local inference server process."""

    def __init__(self, command_prefix: list[str], models_path: str, host: str, port: int):
        self.url = f"http://{host}:{port}"
        self.proc = subprocess.Popen(
            [*command_prefix, "run", "--host", host, "--port", str(port), "--models-dir", models_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if not wait_for_server(self.url):
            # Capture output for debugging
            out = ""
            if self.proc.stdout:
                out = self.proc.stdout.read().decode(errors="replace")[:2000]
            self.stop()
            raise RuntimeError(f"Server failed to start at {self.url}\n{out}")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


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

    port = find_free_port()
    server = InferenceServer(command_prefix, models_path, "127.0.0.1", port)
    yield server.url
    server.stop()


@pytest.fixture(scope="session")
def api(base_url):
    """Return a requests.Session configured for the inference API."""
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    token = env_first("ANTFLY_INFERENCE_TOKEN")
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    def _check(r):
        """Raise for status, but skip test on 404 or model-unavailable errors."""
        if r.status_code == 404:
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            msg = body.get("error", "model not found")
            pytest.skip(f"Model unavailable: {msg}")
        if r.status_code == 400:
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            err = body.get("error", "")
            if "INVALID_MODEL" in err or "MODEL_NOT_FOUND" in err:
                pytest.skip(f"Model unavailable: {body.get('message', err)}")
        r.raise_for_status()

    class Api:
        def __init__(self, session, base_url):
            self.s = session
            self.url = base_url

        def _request(self, method: str, path: str, *, json=None, retry_on_missing_model: bool = True, **kwargs):
            request = getattr(self.s, method)
            normalized_path = api_path(path)
            response = request(f"{self.url}{normalized_path}", json=json, **kwargs)
            if retry_on_missing_model and maybe_pull_missing_model(normalized_path, json, response):
                response.close()
                response = request(f"{self.url}{normalized_path}", json=json, **kwargs)
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
            body: dict = {"text": text}
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
            body = {"model": model, "texts": text, "labels": labels, **kwargs}
            r = self.post("/classify", json=body)
            _check(r)
            return r.json()

        def recognize(self, text: list[str], model: str = "", labels: list[str] | None = None, **kwargs):
            body: dict = {"model": model, "texts": text}
            if labels is not None:
                body["labels"] = labels
            body.update(kwargs)
            r = self.post("/recognize", json=body)
            _check(r)
            return r.json()

        def rewrite(self, text: list[str], model: str = "", **kwargs):
            body = {"model": model, "inputs": text, **kwargs}
            r = self.post("/rewrite", json=body)
            _check(r)
            return r.json()

        def read(self, images: list[str], model: str = "", prompt: str = "", **kwargs):
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
            body = {"model": model, "schema": schema or {}, **kwargs}
            if texts is not None:
                body["texts"] = texts
            if images is not None:
                body["images"] = [{"url": img} if isinstance(img, str) else img for img in images]
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
            r = requests.get(f"{self.base_url}/readyz", timeout=10)
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
