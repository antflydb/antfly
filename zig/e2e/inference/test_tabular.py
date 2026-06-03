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
  - Upload + predict round-trip for a hand-built tabular_model.json.
  - Name-allowlist rejects path traversal.
  - Oversized batches return 413.
  - Non-existent model returns 404.
  - XGBoost / LightGBM converter parity (skipped if packages absent).
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import requests

from .conftest import api_path


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
# Upload + predict round-trip
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


def test_upload_then_predict(api, base_url):
    body = json.dumps(STUMP_IR).encode()
    r = requests.post(
        f"{base_url}{api_path('/predict/upload')}?name=stump-e2e",
        data=body,
        headers={"content-type": "application/octet-stream"},
    )
    assert r.status_code == 201, r.text
    info = r.json()
    assert info["name"] == "stump-e2e"
    assert info["task"] == "regression"

    pr = _predict(api, "stump-e2e", [[0.1], [0.9]])
    assert pr.status_code == 200, pr.text
    preds = pr.json()["predictions"]
    assert abs(preds[0][0] - (-1.0)) < 1e-4
    assert abs(preds[1][0] - 1.0) < 1e-4


def test_upload_rejects_unsafe_name(base_url):
    body = json.dumps(STUMP_IR).encode()
    for unsafe in ("../etc/passwd", ".hidden", "a/b", "a b"):
        r = requests.post(
            f"{base_url}{api_path('/predict/upload')}?name={unsafe}",
            data=body,
            headers={"content-type": "application/octet-stream"},
        )
        assert r.status_code == 400, f"unsafe name {unsafe!r} should be rejected, got {r.status_code}"


# ---------------------------------------------------------------------------
# Convert + predict round-trip — requires xgboost / lightgbm to be installed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("framework", ["xgboost", "lightgbm"])
def test_convert_real_model_predicts(api, base_url, framework, tmp_path):
    if framework == "xgboost":
        xgb = pytest.importorskip("xgboost")
        sklearn_ds = pytest.importorskip("sklearn.datasets")
        data = sklearn_ds.load_iris()
        booster = xgb.XGBClassifier(
            objective="multi:softprob",
            num_class=3,
            n_estimators=10,
            max_depth=3,
            random_state=0,
        ).fit(data.data, data.target)
        model_path = tmp_path / "xgb.json"
        booster.save_model(str(model_path))
    else:
        lgb = pytest.importorskip("lightgbm")
        sklearn_ds = pytest.importorskip("sklearn.datasets")
        data = sklearn_ds.load_iris()
        booster = lgb.LGBMClassifier(
            objective="multiclass",
            num_class=3,
            n_estimators=10,
            max_depth=3,
            random_state=0,
        ).fit(data.data, data.target)
        model_path = tmp_path / "lgb.txt"
        booster.booster_.save_model(str(model_path))

    body = model_path.read_bytes()
    r = requests.post(
        f"{base_url}{api_path('/predict/convert')}?name=e2e-{framework}&framework={framework}",
        data=body,
        headers={"content-type": "application/octet-stream"},
    )
    assert r.status_code == 201, r.text

    sample = data.data[0].tolist()
    pr = _predict(api, f"e2e-{framework}", [sample])
    assert pr.status_code == 200, pr.text
    probs = pr.json()["predictions"][0]
    assert len(probs) == 3
    assert abs(sum(probs) - 1.0) < 1e-3, probs
    assert np.argmax(probs) == 0, probs  # setosa


def test_convert_sklearn_returns_415(base_url):
    r = requests.post(
        f"{base_url}{api_path('/predict/convert')}?name=sk&framework=sklearn",
        data=b"\x80\x04not a real pickle",
        headers={"content-type": "application/octet-stream"},
    )
    assert r.status_code == 415, r.text
    body = r.json()
    assert "termite-convert" in body.get("message", "")
