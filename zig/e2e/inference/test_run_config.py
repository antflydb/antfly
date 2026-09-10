"""Execute config-only inference and CLI precedence against the candidate binary."""

import json
import math
import os
import signal
import socket
import subprocess
import time

import pytest
import requests

from .models import ensure_model_by_name, inference_command, models_dir

pytestmark = pytest.mark.model_integration
MODEL = "BAAI/bge-small-en-v1.5"


@pytest.fixture(scope="module")
def run_config_models():
    assert ensure_model_by_name(MODEL, "embedders") is not None
    return models_dir().resolve()


@pytest.mark.parametrize("layout", ["flat", "nested", "cli-before", "cli-after"])
def test_run_config_model_settings(tmp_path, run_config_models, layout):
    settings = {
        "models_dir": str(run_config_models),
        "ml_dir": str(tmp_path / "ml"),
        "max_loaded_models": 0,
        "preload": [{"kind": "embedder", "name": MODEL, "backend": "native"}],
    }
    model_flags = []
    if layout.startswith("cli-"):
        # A bad config model/path must not be loaded in addition to CLI models.
        settings["models_dir"] = str(tmp_path / "wrong-models")
        settings["preload"] = [{"kind": "embedder", "name": "missing/config-model"}]
        settings["max_loaded_models"] = 1
        model_flags = [
            "--models-dir",
            str(run_config_models),
            "--max-loaded-models",
            "0",
            "--preload-model",
            f"embedder:native:{MODEL}",
        ]
    config = {"inference": settings} if layout != "flat" else settings
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config_flags = ["--config", str(config_path)]
    command = [*inference_command(), "run", "--host", "127.0.0.1", "--port", str(port)]
    command += (
        model_flags + config_flags
        if layout == "cli-before"
        else config_flags + model_flags
    )
    # Do not inherit credentials or model-path defaults that could hide a
    # missing config handoff. Retain only executable/library lookup settings.
    env = {
        key: os.environ[key]
        for key in ("PATH", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH")
        if key in os.environ
    }
    env.update(
        {
            "HOME": str(tmp_path),
            "XDG_CACHE_HOME": str(tmp_path),
            "ANTFLY_INFERENCE_PREFERRED_BACKEND": "native",
            "ANTFLY_INFERENCE_REQUIRED_BACKEND": "native",
        }
    )
    log_path = tmp_path / "runtime.log"
    with log_path.open("w") as output:
        process = subprocess.Popen(
            command,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            base = f"http://127.0.0.1:{port}"
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                assert process.poll() is None, log_path.read_text()
                try:
                    response = requests.get(base + "/readyz", timeout=1)
                    ready = response.status_code == 200
                    response.close()
                    if ready:
                        break
                except requests.RequestException:
                    pass
                time.sleep(0.1)
            else:
                pytest.fail(
                    "config-only inference never became ready:\n" + log_path.read_text()
                )
            # Check warming before the first inference request can load a model.
            assert f"warmed inference embedder model={MODEL}" in log_path.read_text()
            with requests.post(
                base + "/ai/v1/embed",
                json={"model": MODEL, "input": "runtime config contract"},
                timeout=30,
            ) as response:
                assert response.status_code == 200, (
                    response.text + "\n" + log_path.read_text()
                )
                embedding = response.json()["data"][0]["embedding"]
                assert len(embedding) == 384
                assert all(math.isfinite(value) for value in embedding)
                assert sum(value * value for value in embedding) > 0
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
