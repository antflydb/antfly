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

"""End-to-end parity tests for the tabular predictor stack.

Covers:
  - Built-in iris classifier returns correct softmax predictions.
  - CLI URL pull + predict round-trip for a hand-built tabular_model.json.
  - Name-allowlist rejects path traversal.
  - Oversized batches return 413.
  - Non-existent model returns 404.
  - XGBoost / LightGBM / ONNX-ML converter-to-predictor round trips.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import partial
import hashlib
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import subprocess
import threading
from urllib.parse import parse_qs, unquote, urlsplit

import numpy as np
import pytest
import requests

from .conftest import DEFAULT_REQUEST_TIMEOUT, api_path
from .models import REPO_ROOT, inference_command, ml_dir


IRIS_SAMPLE_SETOSA = [5.1, 3.5, 1.4, 0.2]
IRIS_SAMPLE_VERSICOLOR = [6.5, 2.8, 4.6, 1.5]


def _predict(api, model, rows):
    return api.post(
        "/predict",
        json={"model": model, "input": rows},
        retry_on_missing_model=False,
    )


# ---------------------------------------------------------------------------
# Built-in iris classifier — seeded on first server start.
# ---------------------------------------------------------------------------


def test_builtin_iris_returns_softmax(api):
    r = _predict(api, "iris-classifier", [IRIS_SAMPLE_SETOSA])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == "iris-classifier"
    assert body["task"] == "multiclass"
    probs = body["predictions"][0]
    assert len(probs) == 3
    assert abs(sum(probs) - 1.0) < 1e-4
    assert np.argmax(probs) == 0, probs
    assert probs[0] > 0.9, probs


def test_builtin_iris_batched(api):
    r = _predict(api, "iris-classifier", [IRIS_SAMPLE_SETOSA, IRIS_SAMPLE_VERSICOLOR])
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["predictions"]) == 2
    assert np.argmax(body["predictions"][0]) == 0
    assert np.argmax(body["predictions"][1]) == 1


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


def test_nonexistent_model_returns_404(api):
    r = _predict(api, "does-not-exist", [[1, 2, 3, 4]])
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["error"] == "MODEL_NOT_FOUND"


def test_feature_count_mismatch_returns_400(api):
    r = _predict(api, "iris-classifier", [[1, 2, 3]])
    assert r.status_code == 400, r.text


def test_oversized_batch_returns_413(api):
    r = _predict(api, "iris-classifier", [IRIS_SAMPLE_SETOSA] * 10_001)
    assert r.status_code == 413, r.text
    body = r.json()
    assert body["error"] == "BATCH_TOO_LARGE"


# ---------------------------------------------------------------------------
# CLI pull + predict round-trip
# ---------------------------------------------------------------------------

STUMP_IR = {
    "schema_version": 1,
    "metadata": {
        "name": "stump",
        "source_framework": "test",
        "task": "regression",
        "num_features": 1,
    },
    "output": {"activation": "identity", "num_outputs": 1},
    "pipeline": [
        {
            "type": "tree_ensemble",
            "tree_ensemble": {
                "objective": "reg:squarederror",
                "base_score": 0.0,
                "num_trees": 1,
                "num_features": 1,
                "max_depth": 1,
                "nodes": {
                    "feature_index": [0, -1, -1],
                    "threshold": [0.5, 0.0, 0.0],
                    "left_child": [1, -1, -1],
                    "right_child": [2, -1, -1],
                    "leaf_value": [0.0, -1.0, 1.0],
                    "default_left": [True, False, False],
                    "tree_starts": [0],
                },
            },
        }
    ],
}


XGBOOST_STUMP_JSON = {
    "learner": {
        "learner_model_param": {
            "num_feature": "1",
            "num_class": "0",
            "base_score": "0",
        },
        "objective": {"name": "reg:squarederror"},
        "gradient_booster": {
            "name": "gbtree",
            "model": {
                "trees": [
                    {
                        "split_indices": [0, 0, 0],
                        "split_conditions": [0.5, 0.0, 0.0],
                        "left_children": [1, -1, -1],
                        "right_children": [2, -1, -1],
                        "base_weights": [0.0, -1.0, 1.0],
                        "default_left": [True, True, True],
                    }
                ]
            },
        },
    }
}


LIGHTGBM_STUMP_TEXT = """tree
version=v4
num_class=1
objective=regression
max_feature_idx=0

Tree=0
num_leaves=2
num_cat=0
split_feature=0
threshold=0.5
decision_type=2
left_child=-1
right_child=-2
leaf_value=-1 1
end of trees
"""


class QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass


@contextmanager
def _serve_directory(directory):
    handler = partial(QuietSimpleHTTPRequestHandler, directory=str(directory))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def _serve_fake_hf(files, owner="acme", repo="stump-model"):
    payloads = {
        name: data if isinstance(data, bytes) else data.encode("utf-8")
        for name, data in files.items()
    }

    class FakeHuggingFaceHandler(QuietSimpleHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            request_url = urlsplit(self.path)
            path = unquote(request_url.path)
            api_path = f"/api/models/{owner}/{repo}"
            resolve_prefix = f"/{owner}/{repo}/resolve/main/"
            if path == api_path:
                if parse_qs(request_url.query).get("blobs") != ["true"]:
                    self.send_error(400, "blobs=true is required")
                    return
                body = json.dumps(
                    {
                        "siblings": [
                            {
                                "rfilename": name,
                                "size": len(data),
                                "blobId": hashlib.sha1(
                                    f"blob {len(data)}\0".encode() + data,
                                    usedforsecurity=False,
                                ).hexdigest(),
                            }
                            for name, data in payloads.items()
                        ]
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path.startswith(resolve_prefix):
                filename = path[len(resolve_prefix) :]
                data = payloads.get(filename)
                if data is not None:
                    self.send_response(200)
                    self.send_header("content-type", "application/octet-stream")
                    self.send_header("content-length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
            self.send_error(404)

    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeHuggingFaceHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", owner, repo
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _local_cli_models():
    if os.environ.get("ANTFLY_INFERENCE_URL"):
        pytest.skip(
            "CLI model pull tests require the local e2e server models directory"
        )
    try:
        command = inference_command()
    except RuntimeError as exc:
        pytest.skip(str(exc))
    return command, ml_dir()


def _write_hosted_ir(tmp_path, name, payload):
    hosted_dir = tmp_path / name
    hosted_dir.mkdir()
    model = json.loads(json.dumps(payload))
    model.setdefault("metadata", {})["name"] = name
    (hosted_dir / "tabular_model.json").write_text(json.dumps(model), encoding="utf-8")
    return hosted_dir


def _write_onnx_linear_regressor(model_path):
    onnx = pytest.importorskip("onnx")
    from onnx import TensorProto, helper

    node = helper.make_node(
        "LinearRegressor",
        ["X"],
        ["Y"],
        domain="ai.onnx.ml",
        coefficients=[1.0, 2.0],
        intercepts=[0.5],
        post_transform="NONE",
    )
    graph = helper.make_graph(
        [node],
        "linear-regressor",
        [helper.make_tensor_value_info("X", TensorProto.FLOAT, [None, 2])],
        [helper.make_tensor_value_info("Y", TensorProto.FLOAT, [None, 1])],
    )
    model = helper.make_model(
        graph,
        opset_imports=[
            helper.make_opsetid("", 18),
            helper.make_opsetid("ai.onnx.ml", 3),
        ],
    )
    onnx.save(model, model_path)


def test_pull_url_then_predict(api, tmp_path):
    command, model_root = _local_cli_models()
    model_name = "stump-pull-e2e"
    hosted_dir = _write_hosted_ir(tmp_path, model_name, STUMP_IR)

    with _serve_directory(hosted_dir) as origin:
        subprocess.run(
            [
                *command,
                "pull",
                f"{origin}/tabular_model.json",
                "--name",
                model_name,
                "--ml-dir",
                str(model_root),
            ],
            cwd=REPO_ROOT,
            check=True,
        )

    pr = _predict(api, model_name, [[0.1], [0.9]])
    assert pr.status_code == 200, pr.text
    preds = pr.json()["predictions"]
    assert abs(preds[0][0] - (-1.0)) < 1e-4
    assert abs(preds[1][0] - 1.0) < 1e-4


def test_pull_hf_tabular_ir_then_predict(api, tmp_path):
    command, model_root = _local_cli_models()
    model = json.loads(json.dumps(STUMP_IR))
    model["metadata"]["name"] = "ignored-source-name"

    with _serve_fake_hf({"tabular_model.json": json.dumps(model)}) as (
        origin,
        owner,
        repo,
    ):
        env = {**os.environ, "ANTFLY_INFERENCE_HF_BASE_URL": origin}
        subprocess.run(
            [
                *command,
                "pull",
                f"hf:{owner}/{repo}",
                "--type",
                "predictor",
                "--ml-dir",
                str(model_root),
            ],
            cwd=REPO_ROOT,
            env=env,
            check=True,
        )

    pr = _predict(api, repo, [[0.1], [0.9]])
    assert pr.status_code == 200, pr.text
    preds = pr.json()["predictions"]
    assert abs(preds[0][0] - (-1.0)) < 1e-4
    assert abs(preds[1][0] - 1.0) < 1e-4


@pytest.mark.parametrize(
    ("repo", "filename", "payload"),
    [
        ("xgb-stump", "model.json", lambda: json.dumps(XGBOOST_STUMP_JSON)),
        ("lgb-stump", "model.txt", lambda: LIGHTGBM_STUMP_TEXT),
    ],
)
def test_pull_hf_tree_model_then_predict(api, repo, filename, payload):
    command, model_root = _local_cli_models()

    with _serve_fake_hf({filename: payload()}, repo=repo) as (origin, owner, repo):
        env = {**os.environ, "ANTFLY_INFERENCE_HF_BASE_URL": origin}
        subprocess.run(
            [
                *command,
                "pull",
                f"hf:{owner}/{repo}",
                "--type",
                "predictor",
                "--ml-dir",
                str(model_root),
            ],
            cwd=REPO_ROOT,
            env=env,
            check=True,
        )

    pr = _predict(api, repo, [[0.1], [0.9]])
    assert pr.status_code == 200, pr.text
    preds = pr.json()["predictions"]
    assert abs(preds[0][0] - (-1.0)) < 1e-4
    assert abs(preds[1][0] - 1.0) < 1e-4


def test_pull_hf_onnx_ml_then_predict(api, tmp_path):
    command, model_root = _local_cli_models()
    repo = "onnx-linear"
    model_path = tmp_path / "linear.onnx"
    _write_onnx_linear_regressor(model_path)

    with _serve_fake_hf({"model.onnx": model_path.read_bytes()}, repo=repo) as (
        origin,
        owner,
        repo,
    ):
        env = {**os.environ, "ANTFLY_INFERENCE_HF_BASE_URL": origin}
        subprocess.run(
            [
                *command,
                "pull",
                f"hf:{owner}/{repo}",
                "--type",
                "predictor",
                "--ml-dir",
                str(model_root),
            ],
            cwd=REPO_ROOT,
            env=env,
            check=True,
        )

    pr = _predict(api, repo, [[2.0, 3.0], [-1.0, 4.0]])
    assert pr.status_code == 200, pr.text
    preds = pr.json()["predictions"]
    assert abs(preds[0][0] - 8.5) < 1e-4
    assert abs(preds[1][0] - 7.5) < 1e-4


def test_pull_hf_pickle_only_rejected(tmp_path):
    command, model_root = _local_cli_models()
    repo = "pickle-only"

    with _serve_fake_hf({"model.pkl": b"\x80\x04not safe"}, repo=repo) as (
        origin,
        owner,
        repo,
    ):
        env = {**os.environ, "ANTFLY_INFERENCE_HF_BASE_URL": origin}
        result = subprocess.run(
            [
                *command,
                "pull",
                f"hf:{owner}/{repo}",
                "--type",
                "predictor",
                "--ml-dir",
                str(model_root),
            ],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert "UnsupportedArtifact" in combined
    assert "Pickle" in combined or "pickle" in combined
    assert not (model_root / repo / "tabular_model.json").exists()


def test_pull_url_does_not_accept_models_dir_alias(tmp_path):
    command, model_root = _local_cli_models()
    ai_root = tmp_path / "ai-models"
    model_name = f"stump-wrong-root-{os.getpid()}"
    hosted_dir = _write_hosted_ir(tmp_path, model_name, STUMP_IR)

    with _serve_directory(hosted_dir) as origin:
        result = subprocess.run(
            [
                *command,
                "pull",
                f"{origin}/tabular_model.json",
                "--name",
                model_name,
                "--models-dir",
                str(ai_root),
            ],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

    combined = result.stdout + result.stderr
    assert "unexpected arg '--models-dir'" in combined
    assert not (ai_root / model_name / "tabular_model.json").exists()
    assert not (model_root / model_name / "tabular_model.json").exists()


def test_upload_and_convert_routes_removed(base_url):
    body = json.dumps(STUMP_IR).encode()
    for path in ("/predict/upload", "/predict/convert"):
        r = requests.post(
            f"{base_url}{api_path(path)}?name=removed",
            data=body,
            headers={"content-type": "application/octet-stream"},
            timeout=DEFAULT_REQUEST_TIMEOUT,
        )
        assert r.status_code == 404, f"{path} should not be exposed over HTTP"

    r = requests.post(
        f"{base_url}/ai/v1/predict",
        json={"model": "iris-classifier", "input": [IRIS_SAMPLE_SETOSA]},
        timeout=DEFAULT_REQUEST_TIMEOUT,
    )
    assert r.status_code == 404, "/ai/v1/predict should move to /ml/v1/predict"


def test_pull_rejects_unsafe_name(tmp_path):
    command, model_root = _local_cli_models()
    hosted_dir = _write_hosted_ir(tmp_path, "unsafe-source", STUMP_IR)
    for unsafe in ("../etc/passwd", ".hidden", "a/b", "local/iris", "a b"):
        with _serve_directory(hosted_dir) as origin:
            result = subprocess.run(
                [
                    *command,
                    "pull",
                    f"{origin}/tabular_model.json",
                    "--name",
                    unsafe,
                    "--ml-dir",
                    str(model_root),
                ],
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
        assert "InvalidName" in result.stderr or "InvalidName" in result.stdout
        assert not (model_root / unsafe / "tabular_model.json").exists()


def test_discovery_skips_unsafe_metadata_name(api):
    _, model_root = _local_cli_models()
    model_name = f"unsafe-metadata-name-{os.getpid()}"
    model_dir = model_root / model_name
    model_dir.mkdir(parents=True, exist_ok=True)
    model = json.loads(json.dumps(STUMP_IR))
    model["metadata"]["name"] = f"../{model_name}"
    (model_dir / "tabular_model.json").write_text(json.dumps(model), encoding="utf-8")

    r = api.get("/ml/v1/models")
    assert r.status_code == 200, r.text
    predictors = r.json()["predictors"]
    assert f"../{model_name}" not in predictors
    assert model_name not in predictors


# ---------------------------------------------------------------------------
# Convert + predict round-trip.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("framework", ["xgboost", "lightgbm"])
def test_convert_tree_model_predicts(api, framework, tmp_path):
    command, model_root = _local_cli_models()
    if framework == "xgboost":
        model_path = tmp_path / "xgb.json"
        model_path.write_text(json.dumps(XGBOOST_STUMP_JSON), encoding="utf-8")
    else:
        model_path = tmp_path / "lgb.txt"
        model_path.write_text(LIGHTGBM_STUMP_TEXT, encoding="utf-8")

    model_name = f"e2e-{framework}"
    subprocess.run(
        [
            *command,
            "convert",
            str(model_path),
            "-o",
            str(model_root / model_name),
            "--framework",
            framework,
        ],
        cwd=REPO_ROOT,
        check=True,
    )

    pr = _predict(api, model_name, [[0.1], [0.9]])
    assert pr.status_code == 200, pr.text
    preds = pr.json()["predictions"]
    assert abs(preds[0][0] - (-1.0)) < 1e-4
    assert abs(preds[1][0] - 1.0) < 1e-4


def test_convert_onnx_linear_regressor_predicts(api, tmp_path):
    command, model_root = _local_cli_models()
    model_path = tmp_path / "linear.onnx"
    _write_onnx_linear_regressor(model_path)

    model_name = "e2e-onnx-linear"
    subprocess.run(
        [
            *command,
            "convert",
            str(model_path),
            "-o",
            str(model_root / model_name),
            "--framework",
            "onnx",
        ],
        cwd=REPO_ROOT,
        check=True,
    )

    pr = _predict(api, model_name, [[2.0, 3.0], [-1.0, 4.0]])
    assert pr.status_code == 200, pr.text
    preds = pr.json()["predictions"]
    assert abs(preds[0][0] - 8.5) < 1e-4
    assert abs(preds[1][0] - 7.5) < 1e-4


def test_convert_rejects_unknown_framework(tmp_path):
    command, model_root = _local_cli_models()
    model_path = tmp_path / "bad.pkl"
    model_path.write_bytes(b"\x80\x04not a real pickle")
    result = subprocess.run(
        [
            *command,
            "convert",
            str(model_path),
            "-o",
            str(model_root / "sk"),
            "--framework",
            "sklearn",
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert (
        "unknown framework 'sklearn'" in result.stderr
        or "unknown framework 'sklearn'" in result.stdout
    )


def test_convert_malformed_xgboost_does_not_crash_server(base_url, tmp_path):
    command, model_root = _local_cli_models()
    bad_xgb = {
        "learner": {
            "learner_model_param": {
                "num_feature": "4294967296",
                "num_class": "0",
                "base_score": "0.5",
            },
            "objective": {"name": "reg:squarederror"},
            "gradient_booster": {"model": {"trees": []}},
        }
    }
    model_path = tmp_path / "bad-xgb.json"
    model_path.write_text(json.dumps(bad_xgb), encoding="utf-8")
    result = subprocess.run(
        [
            *command,
            "convert",
            str(model_path),
            "-o",
            str(model_root / "bad-xgb"),
            "--framework",
            "xgboost",
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert "XgboostFailed" in result.stderr or "XgboostFailed" in result.stdout

    follow = requests.post(
        f"{base_url}{api_path('/predict')}",
        json={"model": "iris-classifier", "input": [IRIS_SAMPLE_SETOSA]},
        timeout=DEFAULT_REQUEST_TIMEOUT,
    )
    assert follow.status_code == 200, follow.text


# ---------------------------------------------------------------------------
# Regression coverage for production safety and API contract behavior.
# ---------------------------------------------------------------------------


def test_ragged_batch_rejected(api):
    """Every row in a batch must match the model feature count."""
    r = api.post(
        "/predict",
        json={"model": "iris-classifier", "input": [[5.1, 3.5, 1.4, 0.2], [5.1]]},
        retry_on_missing_model=False,
    )
    assert r.status_code == 400, r.text


def test_ml_models_lists_predictors(api):
    """/ml/v1/models is the Traditional ML predictor catalog."""
    r = api.get("/ml/v1/models")
    assert r.status_code == 200, r.text
    body = r.json()
    assert "predictors" in body, body
    assert "iris-classifier" in body["predictors"], body["predictors"]
    iris = body["predictors"]["iris-classifier"]
    assert iris["task"] == "multiclass"
    assert iris["num_features"] == 4
    assert iris["num_outputs"] == 3
    assert iris["feature_names"] == [
        "sepal_length",
        "sepal_width",
        "petal_length",
        "petal_width",
    ]


def test_ai_models_excludes_predictors(api):
    """/ai/v1/models remains the AI model catalog."""
    r = api.get("/models")
    assert r.status_code == 200, r.text
    assert "predictors" not in r.json()


def test_hostile_int_does_not_crash_loader(base_url, tmp_path):
    """Out-of-range integer fields are rejected as invalid model input."""
    command, model_root = _local_cli_models()
    malicious = {
        "schema_version": 1,
        "metadata": {"name": "x", "num_features": 1, "task": "regression"},
        "output": {"activation": "identity", "num_outputs": 1},
        "pipeline": [
            {
                "type": "tree_ensemble",
                "tree_ensemble": {
                    "objective": "x",
                    "num_trees": 2147483648,  # > i32 max
                    "num_features": 1,
                    "max_depth": 1,
                    "nodes": {
                        "feature_index": [-1],
                        "threshold": [0],
                        "left_child": [-1],
                        "right_child": [-1],
                        "leaf_value": [1.0],
                        "default_left": [False],
                        "tree_starts": [0],
                    },
                },
            }
        ],
    }
    hosted_dir = tmp_path / "hostile"
    hosted_dir.mkdir()
    (hosted_dir / "tabular_model.json").write_text(
        json.dumps(malicious), encoding="utf-8"
    )
    with _serve_directory(hosted_dir) as origin:
        result = subprocess.run(
            [
                *command,
                "pull",
                f"{origin}/tabular_model.json",
                "--name",
                "hostile",
                "--ml-dir",
                str(model_root),
            ],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
    assert "InvalidModel" in result.stderr or "InvalidModel" in result.stdout
    # Subsequent requests must still work.
    follow = requests.post(
        f"{base_url}{api_path('/predict')}",
        json={"model": "iris-classifier", "input": [IRIS_SAMPLE_SETOSA]},
        timeout=DEFAULT_REQUEST_TIMEOUT,
    )
    assert follow.status_code == 200, follow.text
