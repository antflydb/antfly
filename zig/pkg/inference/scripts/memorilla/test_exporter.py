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

"""Artifact corruption, architecture admission, and bitwise conversion gates."""

import json
from pathlib import Path

import pytest
import torch
from safetensors.torch import load_file, save_file

from exporter import (
    RELEASE_CONFIG,
    expected_shapes,
    export_checkpoint,
    load_bundle,
    read_json,
    sha256,
    validate_config,
)


CONFIG = {**RELEASE_CONFIG, "embedding_dim": 8, "output_dim": 12, "num_heads": 2}


def write_checkpoint(path: Path, *, dtype=torch.bfloat16):
    path.mkdir()
    (path / "config.json").write_text(json.dumps(CONFIG))
    rng = torch.Generator().manual_seed(11)
    state = {
        name: torch.randn(shape, generator=rng).to(dtype)
        for name, shape in expected_shapes(CONFIG, split=False).items()
    }
    # Distinguish signed-zero payloads in the round trip.
    state["memory_queries"][0, 0, 0] = -0.0
    torch.save(state, path / "memory.pt")
    return state


@pytest.mark.parametrize("dtype", (torch.bfloat16, torch.float16, torch.float32))
def test_round_trip_preserves_bits_and_projection_order(tmp_path, dtype):
    checkpoint, output = tmp_path / "checkpoint", tmp_path / "output"
    state = write_checkpoint(checkpoint, dtype=dtype)
    manifest = export_checkpoint(checkpoint, output, release=False)
    config, restored, loaded = load_bundle(output)
    assert loaded == manifest
    assert config == CONFIG
    assert manifest["release"] is False
    assert manifest["base_models"] is None
    assert manifest["parameter_count"] == sum(t.numel() for t in state.values())
    for prefix in ("query_cond_attn", "cross_attn_layers.0", "cross_attn_layers.1"):
        for kind in ("weight", "bias"):
            for index, part in enumerate(("q", "k", "v")):
                expected = state[f"{prefix}.in_proj_{kind}"].chunk(3, dim=0)[index]
                actual = restored[f"{prefix}.{part}_proj.{kind}"]
                assert torch.equal(
                    actual.view(torch.uint8), expected.contiguous().view(torch.uint8)
                )
    assert torch.signbit(restored["memory_queries"][0, 0, 0]).item()


def test_release_rejects_synthetic_architecture(tmp_path):
    checkpoint = tmp_path / "checkpoint"
    write_checkpoint(checkpoint)
    with pytest.raises(ValueError, match="released Qwen architecture"):
        export_checkpoint(checkpoint, tmp_path / "output")


def test_mismatched_source_hash_rejected_before_conversion(tmp_path):
    checkpoint = tmp_path / "checkpoint"
    write_checkpoint(checkpoint)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        export_checkpoint(
            checkpoint, tmp_path / "output", release=False, expected_sha256="0" * 64
        )
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize(
    "mutation", ("missing", "extra", "shape", "nan", "integer", "mixed")
)
def test_invalid_checkpoint_rejected_without_output(tmp_path, mutation):
    checkpoint, output = tmp_path / "checkpoint", tmp_path / "output"
    state = write_checkpoint(checkpoint)
    key = "query_cond_attn.in_proj_bias"
    if mutation == "missing":
        del state[key]
    elif mutation == "extra":
        state["unexpected.weight"] = torch.zeros(1, dtype=torch.bfloat16)
    elif mutation == "shape":
        state[key] = state[key][:-1]
    elif mutation == "nan":
        state[key][0] = float("nan")
    elif mutation == "integer":
        state[key] = state[key].to(torch.int32)
    else:
        state[key] = state[key].to(torch.float32)
    torch.save(state, checkpoint / "memory.pt")
    with pytest.raises(ValueError):
        export_checkpoint(checkpoint, output, release=False)
    assert not output.exists()
    assert not list(tmp_path.glob(".output-*"))


def test_export_never_overwrites_existing_directory(tmp_path):
    checkpoint, output = tmp_path / "checkpoint", tmp_path / "output"
    write_checkpoint(checkpoint)
    output.mkdir()
    (output / "keep").write_text("existing")
    with pytest.raises(ValueError, match="already exists"):
        export_checkpoint(checkpoint, output, release=False)
    assert (output / "keep").read_text() == "existing"


@pytest.mark.parametrize("filename", ("config.json", "memory.safetensors"))
def test_changed_bundle_file_rejected(tmp_path, filename):
    checkpoint, output = tmp_path / "checkpoint", tmp_path / "output"
    write_checkpoint(checkpoint)
    export_checkpoint(checkpoint, output, release=False)
    with (output / filename).open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(ValueError, match="checksum mismatch"):
        load_bundle(output)


def test_checksum_alone_does_not_authorize_wrong_tensor_layout(tmp_path):
    checkpoint, output = tmp_path / "checkpoint", tmp_path / "output"
    write_checkpoint(checkpoint)
    export_checkpoint(checkpoint, output, release=False)
    weights = load_file(str(output / "memory.safetensors"))
    weights["memory_projection.weight"] = (
        weights["memory_projection.weight"].t().contiguous()
    )
    save_file(weights, str(output / "memory.safetensors"))
    manifest = read_json(output / "manifest.json")
    info = manifest["files"]["memory.safetensors"]
    info.update(
        sha256=sha256(output / "memory.safetensors"),
        size_bytes=(output / "memory.safetensors").stat().st_size,
    )
    (output / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="shape/layout"):
        load_bundle(output)


@pytest.mark.parametrize(
    "key,value",
    [
        ("embedding_dim", True),
        ("num_heads", 3),
        ("dropout", float("nan")),
        ("retrieval_init", 0),
        ("num_self_attn_layers", 1),
        ("num_cross_attn_layers", 9),
        ("unexpected", 1),
    ],
)
def test_unsupported_config_is_explicit(key, value):
    with pytest.raises(ValueError):
        validate_config({**CONFIG, key: value}, release=False)


def test_duplicate_json_fields_rejected(tmp_path):
    path = tmp_path / "config.json"
    path.write_text('{"num_heads": 8, "num_heads": 4}')
    with pytest.raises(ValueError, match="duplicate JSON key"):
        read_json(path)


def test_release_parameter_inventory_matches_published_count():
    shapes = expected_shapes(RELEASE_CONFIG)
    assert sum(torch.Size(shape).numel() for shape in shapes.values()) == 194_123_776


@pytest.mark.parametrize("checkpoint", [None, "not-a-released-checkpoint", ["stage3"]])
def test_release_provenance_rejects_unknown_checkpoint_even_with_missing_digest(
    tmp_path, monkeypatch, checkpoint
):
    import exporter

    source, output = tmp_path / "source", tmp_path / "bundle"
    write_checkpoint(source)
    export_checkpoint(source, output, release=False)
    # Reach release provenance checks with a tiny valid BF16 inventory. The
    # real released geometry is independently covered by the count test above.
    monkeypatch.setattr(exporter, "RELEASE_CONFIG", CONFIG)
    manifest = read_json(output / "manifest.json")
    manifest["release"] = True
    manifest["base_models"] = exporter.BASE_MODELS
    manifest["source"].update(
        model=exporter.MODEL,
        revision=exporter.REVISION,
        checkpoint=checkpoint,
        sha256=None,
    )
    (output / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="unrecognized release provenance"):
        load_bundle(output)


@pytest.mark.parametrize("field", ("repository", "revision", "memory_py_sha256"))
def test_release_rejects_changed_implementation_provenance(
    tmp_path, monkeypatch, field
):
    import exporter

    source, output = tmp_path / "source", tmp_path / "bundle"
    write_checkpoint(source)
    export_checkpoint(source, output, release=False)
    monkeypatch.setattr(exporter, "RELEASE_CONFIG", CONFIG)
    manifest = read_json(output / "manifest.json")
    checkpoint = next(iter(exporter.CHECKPOINT_SHA256))
    manifest["release"] = True
    manifest["base_models"] = exporter.BASE_MODELS
    manifest["source"].update(
        model=exporter.MODEL,
        revision=exporter.REVISION,
        checkpoint=checkpoint,
        sha256=exporter.CHECKPOINT_SHA256[checkpoint],
    )
    path = output / "manifest.json"
    path.write_text(json.dumps(manifest))
    monkeypatch.setitem(
        exporter.CONVERTED_SHA256,
        checkpoint,
        manifest["files"]["memory.safetensors"]["sha256"],
    )
    # Establish that the tiny release otherwise satisfies every admission gate.
    load_bundle(output)
    manifest["implementation"][field] = "unrecognized-implementation"
    path.write_text(json.dumps(manifest))
    with pytest.raises(
        ValueError, match="unsupported memory implementation provenance"
    ):
        load_bundle(output)


def test_recomputed_manifest_cannot_substitute_converted_release_weights(
    tmp_path, monkeypatch
):
    import exporter

    source, output = tmp_path / "source", tmp_path / "bundle"
    write_checkpoint(source)
    export_checkpoint(source, output, release=False)
    monkeypatch.setattr(exporter, "RELEASE_CONFIG", CONFIG)
    manifest = read_json(output / "manifest.json")
    stage = "stage1"
    manifest.update(release=True, base_models=exporter.BASE_MODELS)
    manifest["source"].update(
        model=exporter.MODEL,
        revision=exporter.REVISION,
        checkpoint=stage,
        sha256=exporter.CHECKPOINT_SHA256[stage],
    )
    monkeypatch.setitem(
        exporter.CONVERTED_SHA256,
        stage,
        manifest["files"]["memory.safetensors"]["sha256"],
    )
    path = output / "manifest.json"
    path.write_text(json.dumps(manifest))
    load_bundle(output)
    weights_path = output / "memory.safetensors"
    weights = load_file(str(weights_path))
    weights["memory_queries"][0, 0, 1] += 1
    save_file(weights, str(weights_path), metadata={"format": exporter.SCHEMA})
    manifest["files"]["memory.safetensors"].update(
        sha256=sha256(weights_path), size_bytes=weights_path.stat().st_size
    )
    path.write_text(json.dumps(manifest))
    with pytest.raises(
        ValueError, match="converted weights differ from the pinned release"
    ):
        load_bundle(output)
