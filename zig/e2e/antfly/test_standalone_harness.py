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

"""Fast regression tests for the standalone inference test harness."""

import pytest

import test_standalone as standalone


def test_model_preflight_recognizes_atomic_variant_publication(tmp_path):
    models_dir = tmp_path / "models"
    published = models_dir / "ggml-org" / "gemma-4-e2b-it-gguf--antfly-0123456789abcdef"
    published.mkdir(parents=True)

    assert standalone._model_exists(models_dir, "ggml-org/gemma-4-e2b-it-gguf")


def test_model_preflight_rejects_incomplete_variant_names(tmp_path):
    models_dir = tmp_path / "models"
    incomplete = (
        models_dir
        / "generators"
        / "ggml-org"
        / "gemma-4-e2b-it-gguf--antfly-0123456789abcdef.tmp"
    )
    incomplete.mkdir(parents=True)

    assert not standalone._model_exists(models_dir, "ggml-org/gemma-4-e2b-it-gguf")


def test_warmup_uses_configured_first_use_deadline(monkeypatch):
    observed: dict[str, object] = {}

    class Response:
        status_code = 200
        text = ""

    def post(url: str, **kwargs):
        observed["url"] = url
        observed["timeout"] = kwargs["timeout"]
        return Response()

    monkeypatch.setattr(standalone.requests, "post", post)

    standalone._warm_inference_generator(
        "http://127.0.0.1:8080/ai/v1",
        "test/model",
        request_timeout=1800.0,
    )

    assert observed == {
        "url": "http://127.0.0.1:8080/ai/v1/generate",
        "timeout": 1800.0,
    }


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "invalid"])
def test_first_use_deadline_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("ANTFLY_INFERENCE_FIRST_USE_REQUEST_TIMEOUT", value)

    with pytest.raises(
        ValueError,
        match="ANTFLY_INFERENCE_FIRST_USE_REQUEST_TIMEOUT must be a positive finite number",
    ):
        standalone._positive_timeout(
            "ANTFLY_INFERENCE_FIRST_USE_REQUEST_TIMEOUT",
            standalone.DEFAULT_INFERENCE_STANDALONE_FIRST_USE_REQUEST_TIMEOUT,
        )


class _FakeStandaloneServer:
    def __init__(
        self,
        *,
        forced_kill: bool = False,
        returncode: int = 0,
        stop_failure: Exception | None = None,
    ):
        self.forced_kill = forced_kill
        self.returncode = returncode
        self.stop_failure = stop_failure
        self.final_logs = "standalone diagnostic logs"
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_failure is not None:
            raise self.stop_failure


def test_cleanup_preserves_primary_failure():
    server = _FakeStandaloneServer(returncode=1)
    primary_failure = RuntimeError("warmup timed out")

    standalone._finish_standalone_server(server, primary_failure)

    assert server.stop_calls == 1
    assert primary_failure.__notes__ == [
        "standalone inference did not shut down cleanly "
        "(forced_kill=False, returncode=1)\n"
        "last logs:\nstandalone diagnostic logs"
    ]


def test_cleanup_exception_is_attached_to_primary_failure():
    server = _FakeStandaloneServer(stop_failure=OSError("cleanup failed"))
    primary_failure = RuntimeError("warmup timed out")

    standalone._finish_standalone_server(server, primary_failure)

    assert server.stop_calls == 1
    assert primary_failure.__notes__ == [
        "standalone inference cleanup raised OSError: cleanup failed"
    ]


def test_cleanup_failure_is_primary_after_successful_test():
    server = _FakeStandaloneServer(forced_kill=True, returncode=-9)

    with pytest.raises(AssertionError, match="did not shut down cleanly"):
        standalone._finish_standalone_server(server, None)

    assert server.stop_calls == 1
