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

"""Synthetic gates for the encoder-LoRA native export: adapter merge bytes,
non-release reader bundles and pipeline fixture round trips."""

import hashlib
import json

import pytest
import torch
from safetensors import safe_open
from safetensors.torch import load_file, save_file

import encoder_lora_export as export
import exporter
from encoder_lora import SCHEMA as CANDIDATE_SCHEMA, SYSTEM
from exporter import (
    EXECUTION,
    PER_TOKEN_DUAL_LAYER_POOLING,
    RELEASE_CONFIG,
    execution_contract,
    expected_shapes,
    load_bundle,
    validate_execution,
)

CONFIG = {**RELEASE_CONFIG, "embedding_dim": 8, "output_dim": 12, "num_heads": 2}
WIDTH, Q_OUT, V_OUT, RANK, ALPHA = 6, 10, 4, 2, 4


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_candidate(path, *, layers=2, reader_config=CONFIG):
    """A tiny research candidate: adapters for ``layers`` q/v pairs, a packed
    reader for ``reader_config`` and a copy head, all hashed in candidate.json."""
    path.mkdir()
    rng = torch.Generator().manual_seed(5)
    adapter = {}
    for layer in range(layers):
        for leaf, out in (("q_proj", Q_OUT), ("v_proj", V_OUT)):
            base = f"layers.{layer}.self_attn.{leaf}"
            adapter[base + ".A"] = torch.randn(RANK, WIDTH, generator=rng)
            adapter[base + ".B"] = torch.randn(out, RANK, generator=rng) * 0.1
    save_file(adapter, str(path / "encoder-lora.safetensors"))
    reader = {
        name: torch.randn(shape, generator=rng)
        for name, shape in expected_shapes(reader_config, split=False).items()
    }
    save_file(reader, str(path / "reader.safetensors"))
    save_file(
        {
            "query.weight": torch.eye(4),
            "gate.weight": torch.zeros(1, 4),
            "gate.bias": torch.full((1,), -2.0),
        },
        str(path / "copy-head.safetensors"),
    )
    (path / "base-proofs.json").write_text("{}\n")
    protocol = {
        "schema": CANDIDATE_SCHEMA,
        "arm": "encoder-lora",
        "memory_slots": 16,
        "rank": RANK,
        "alpha": ALPHA,
        "target_modules": ["q_proj", "v_proj"],
        "system_prompt": SYSTEM,
        "rollout_qualified": False,
        "document_features": "tokens-dual",
        "encoder_layer": None,
        "dual_layer": 1,
        "copy_head": True,
        "copy_head_temperature": 0.05,
        "document_space_prefix": True,
        "document_self_context": True,
        "document_pooled_context": False,
        "encoder_special_rows": True,
        "entity_tags": "none",
        "copy_restrict_by_type": False,
        "reader_loops": 1,
    }
    (path / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    candidate = {
        "schema": CANDIDATE_SCHEMA,
        "rollout_qualified": False,
        "experimental_only": True,
        "memory_slots": 16,
        "base_proofs_sha256": exporter.sha256(path / "base-proofs.json"),
        "reader_sha256": exporter.sha256(path / "reader.safetensors"),
        "encoder_lora_sha256": exporter.sha256(path / "encoder-lora.safetensors"),
        "protocol_sha256": exporter.sha256(path / "protocol.json"),
        "copy_head_sha256": exporter.sha256(path / "copy-head.safetensors"),
    }
    (path / "candidate.json").write_text(json.dumps(candidate, indent=2) + "\n")
    return adapter, reader


BASE = export.encoder_base()


def write_encoder(
    path, *, layers=2, prefix="", manifest=export.ENCODER_MANIFEST, revision=None
):
    """Two BF16 shards with an HF index, tokenizer-like side files, a managed
    receipt naming ``revision`` and the catalog's canonical manifest."""
    revision = BASE["revision"] if revision is None else revision
    path.mkdir()
    rng = torch.Generator().manual_seed(9)
    tensors = {
        f"{prefix}embed_tokens.weight": torch.randn(5, WIDTH, generator=rng),
        f"{prefix}norm.weight": torch.ones(WIDTH),
    }
    for layer in range(layers):
        base = f"{prefix}layers.{layer}.self_attn."
        tensors[base + "q_proj.weight"] = torch.randn(Q_OUT, WIDTH, generator=rng)
        tensors[base + "k_proj.weight"] = torch.randn(V_OUT, WIDTH, generator=rng)
        tensors[base + "v_proj.weight"] = torch.randn(V_OUT, WIDTH, generator=rng)
        tensors[f"{prefix}layers.{layer}.mlp.up_proj.weight"] = torch.randn(
            3, WIDTH, generator=rng
        )
    tensors = {k: v.to(torch.bfloat16) for k, v in tensors.items()}
    names = sorted(tensors)
    shards = {
        "model-00001-of-00002.safetensors": names[: len(names) // 2],
        "model-00002-of-00002.safetensors": names[len(names) // 2 :],
    }
    weight_map = {}
    for shard, members in shards.items():
        save_file(
            {n: tensors[n] for n in members},
            str(path / shard),
            metadata={"format": "pt"},
        )
        weight_map.update({n: shard for n in members})
    (path / "model.safetensors.index.json").write_text(
        json.dumps(
            {
                "metadata": {
                    "total_size": sum(2 * t.numel() for t in tensors.values())
                },
                "weight_map": weight_map,
            },
            indent=2,
        )
    )
    (path / "config.json").write_text('{"model_type": "qwen3"}\n')
    (path / ".antfly-download-complete.json").write_text(
        json.dumps(
            {
                "version": 1,
                "source": {
                    "owner": "Qwen",
                    "name": "Qwen3-Embedding-4B",
                    "variant": "memorilla-f32-safetensors-bundle-v1",
                    "resolved_revision": revision,
                },
                "artifacts": [],
            }
        )
    )
    (path / "tokenizer.json").write_text('{"version": "1.0"}\n')
    (path / "1_Pooling").mkdir()
    (path / "1_Pooling" / "config.json").write_text(
        '{"pooling_mode_lasttoken": true}\n'
    )
    (path / "model_manifest.json").write_bytes(manifest.encode())
    return tensors, weight_map


@pytest.mark.parametrize("prefix", ("", "model."))
def test_merge_encoder_adds_scaled_lora_and_copies_everything_else(tmp_path, prefix):
    candidate, encoder, output = (tmp_path / "cand", tmp_path / "enc", tmp_path / "out")
    adapter, _ = write_candidate(candidate)
    tensors, weight_map = write_encoder(encoder, prefix=prefix)
    receipt = export.merge_encoder(candidate, encoder, output)
    index = json.loads((output / "model.safetensors.index.json").read_text())
    assert index["weight_map"] == weight_map
    merged_total = 0
    for name, shard in weight_map.items():
        with safe_open(str(output / shard), "pt") as handle:
            assert handle.metadata() == {"format": "pt"}
            stored = handle.get_tensor(name)
        module = name.removeprefix("model.").removesuffix(".weight")
        source = tensors[name]
        if module + ".A" in adapter:
            delta = (adapter[module + ".B"] @ adapter[module + ".A"]) * (ALPHA / RANK)
            assert stored.dtype == torch.float32
            assert torch.allclose(stored, source.float() + delta, rtol=0, atol=1e-6)
            assert receipt["tensors"][name]["max_abs_delta"] == pytest.approx(
                float(delta.abs().max())
            )
            assert receipt["tensors"][name]["shard"] == shard
        else:
            assert stored.dtype == torch.bfloat16
            assert torch.equal(stored.view(torch.int16), source.view(torch.int16))
        merged_total += stored.numel() * stored.element_size()
    assert index["metadata"]["total_size"] == merged_total
    assert receipt["counts"] == {
        "tensors": len(tensors),
        "merged": 4,
        "copied": len(tensors) - 4,
    }
    assert receipt["scale"] == 2.0
    assert receipt["adapter_sha256"] == exporter.sha256(
        candidate / "encoder-lora.safetensors"
    )
    assert receipt["candidate_sha256"]["candidate_json"] == exporter.sha256(
        candidate / "candidate.json"
    )
    assert receipt["copied_files"] == ["1_Pooling", "config.json", "tokenizer.json"]
    assert (output / "1_Pooling" / "config.json").read_text() == (
        encoder / "1_Pooling" / "config.json"
    ).read_text()
    assert receipt["skipped_files"] == [
        ".antfly-download-complete.json",
        "model_manifest.json",
    ]
    assert receipt["sidecar_files"] == list(export.SIDECAR_NAMES)
    assert receipt["base"] == BASE
    assert sorted(p.name for p in output.iterdir()) == sorted(
        receipt["copied_files"]
        + sorted(set(weight_map.values()))
        + [
            ".antfly-download-complete.json",
            "memorilla_lock.json",
            "merge.json",
            "model.safetensors.index.json",
            "model_manifest.json",
        ]
    )
    assert (output / "model_manifest.json").read_bytes() == (
        export.RESEARCH_ENCODER_MANIFEST.encode()
    )
    assert receipt["model_manifest"] == {
        "memorilla_profile": export.RESEARCH_ENCODER_PROFILE,
        "source_sha256": export.ENCODER_MANIFEST_SHA256,
        "written_sha256": export.RESEARCH_ENCODER_MANIFEST_SHA256,
    }
    assert json.loads((output / "merge.json").read_text()) == receipt
    assert not list(tmp_path.glob(".out-*"))


def pins(directory, names, size_key):
    return [
        {
            "path": name,
            size_key: (directory / name).stat().st_size,
            "sha256": exporter.sha256(directory / name),
        }
        for name in names
    ]


def test_merge_encoder_writes_managed_research_sidecars(tmp_path):
    candidate, encoder, output = (tmp_path / "cand", tmp_path / "enc", tmp_path / "out")
    write_candidate(candidate)
    _, weight_map = write_encoder(encoder)
    receipt = export.merge_encoder(candidate, encoder, output)
    candidate_sha256 = exporter.sha256(candidate / "candidate.json")
    shards = sorted(set(weight_map.values()))
    locked = sorted(
        [
            "1_Pooling/config.json",
            "config.json",
            "merge.json",
            "model.safetensors.index.json",
            "tokenizer.json",
            *shards,
        ]
    )
    lock_bytes = (output / "memorilla_lock.json").read_bytes()
    lock = json.loads(lock_bytes)
    assert list(lock) == ["schema", "candidate_sha256", "base", "files"]
    assert lock["schema"] == export.RESEARCH_LOCK_SCHEMA
    assert lock["candidate_sha256"] == candidate_sha256
    assert lock["base"] == BASE
    assert lock["files"] == pins(output, locked, "size_bytes")
    assert lock_bytes.endswith(b"\n")
    assert all(p["size_bytes"] > 0 for p in lock["files"])

    receipt_bytes = (output / ".antfly-download-complete.json").read_bytes()
    managed = json.loads(receipt_bytes)
    assert list(managed) == ["version", "source", "artifacts"]
    assert managed["version"] == 1
    source = managed["source"]
    assert {k: source[k] for k in ("owner", "name", "variant")} == {
        "owner": "Qwen",
        "name": "Qwen3-Embedding-4B",
        "variant": export.RESEARCH_VARIANT,
    }
    assert (
        source["requested_revision"] == source["resolved_revision"] == BASE["revision"]
    )
    assert source["selected_format"] == export.RESEARCH_VARIANT
    assert source["merge"]["candidate_sha256"] == candidate_sha256
    assert source["merge"]["adapter_sha256"] == receipt["adapter_sha256"]
    assert source["merge"]["merge_json_sha256"] == exporter.sha256(
        output / "merge.json"
    )
    assert source["merge"]["lock_sha256"] == sha256_bytes(lock_bytes)
    assert source["merge"]["memorilla_profile"] == export.RESEARCH_ENCODER_PROFILE
    assert managed["artifacts"] == pins(
        output, sorted(locked + ["memorilla_lock.json", "model_manifest.json"]), "size"
    )
    assert [a["path"] for a in managed["artifacts"]] == sorted(
        a["path"] for a in managed["artifacts"]
    )
    assert all(
        "/" not in a["path"] or a["path"].startswith("1_Pooling/")
        for a in managed["artifacts"]
    )
    assert any(a["path"].endswith(".safetensors") for a in managed["artifacts"])
    manifest_pin = next(
        a for a in managed["artifacts"] if a["path"] == "model_manifest.json"
    )
    assert manifest_pin["sha256"] == export.RESEARCH_ENCODER_MANIFEST_SHA256
    assert manifest_pin["size"] == len(export.RESEARCH_ENCODER_MANIFEST)
    assert receipt_bytes.endswith(b"\n") and b"\n" not in receipt_bytes[:-1]
    assert not (output / ".antfly-download-in-progress").exists()
    assert not (output / ".antfly-download-plan.json").exists()


def test_sidecars_only_is_idempotent_and_tracks_merge_json(tmp_path):
    candidate, encoder, output = (tmp_path / "cand", tmp_path / "enc", tmp_path / "out")
    write_candidate(candidate)
    write_encoder(encoder)
    export.merge_encoder(candidate, encoder, output)
    before = {name: (output / name).read_bytes() for name in export.SIDECAR_NAMES}
    shard_stats = {p.name: p.stat().st_mtime_ns for p in output.glob("*.safetensors")}
    written = export.refresh_sidecars(output, candidate)
    assert written["lock"] == json.loads(before["memorilla_lock.json"])
    assert {
        name: (output / name).read_bytes() for name in export.SIDECAR_NAMES
    } == before
    assert {
        p.name: p.stat().st_mtime_ns for p in output.glob("*.safetensors")
    } == shard_stats
    assert (
        export.main(["merge-encoder", "--output", str(output), "--sidecars-only"])
        is None
    )
    assert {
        name: (output / name).read_bytes() for name in export.SIDECAR_NAMES
    } == before
    # A refreshed merge.json changes the lock and receipt, nothing else.
    merge = json.loads((output / "merge.json").read_text())
    merge["torch_version"] = "other"
    export.save_json(output / "merge.json", merge)
    export.refresh_sidecars(output)
    after = {name: (output / name).read_bytes() for name in export.SIDECAR_NAMES}
    assert after["model_manifest.json"] == before["model_manifest.json"]
    assert after["memorilla_lock.json"] != before["memorilla_lock.json"]
    lock = json.loads(after["memorilla_lock.json"])
    assert (
        next(f for f in lock["files"] if f["path"] == "merge.json")
        == pins(output, ["merge.json"], "size_bytes")[0]
    )
    assert (
        after[".antfly-download-complete.json"]
        != before[".antfly-download-complete.json"]
    )
    # Tampered shards and in-progress markers fail closed.
    (output / ".antfly-download-plan.json").write_text("{}")
    with pytest.raises(ValueError, match="in progress"):
        export.refresh_sidecars(output)
    (output / ".antfly-download-plan.json").unlink()
    other = tmp_path / "other"
    other.mkdir()
    (other / "candidate.json").write_text("{}\n")
    with pytest.raises(ValueError, match="differs from the merged encoder"):
        export.refresh_sidecars(output, other)
    shard = next(output.glob("*.safetensors"))
    shard.write_bytes(shard.read_bytes() + b"\0")
    with pytest.raises(ValueError, match="shard differs"):
        export.refresh_sidecars(output)


def test_merge_encoder_requires_the_catalog_encoder(tmp_path):
    candidate = tmp_path / "cand"
    write_candidate(candidate)
    plain = export.ENCODER_MANIFEST.replace(
        f'"memorilla_profile":"{export.RELEASE_ENCODER_PROFILE}",', ""
    )
    write_encoder(tmp_path / "enc1", manifest=plain)
    with pytest.raises(ValueError, match="catalog encoder manifest"):
        export.merge_encoder(candidate, tmp_path / "enc1", tmp_path / "out1")
    write_encoder(tmp_path / "enc2", revision="0" * 40)
    with pytest.raises(ValueError, match="revision differs"):
        export.merge_encoder(candidate, tmp_path / "enc2", tmp_path / "out2")
    assert not (tmp_path / "out1").exists() and not (tmp_path / "out2").exists()


def test_merge_encoder_bfloat16_rounds_the_merge(tmp_path):
    candidate, encoder, output = (tmp_path / "cand", tmp_path / "enc", tmp_path / "out")
    adapter, _ = write_candidate(candidate)
    tensors, weight_map = write_encoder(encoder)
    receipt = export.merge_encoder(candidate, encoder, output, merged_dtype="bfloat16")
    name = "layers.0.self_attn.q_proj.weight"
    with safe_open(str(output / weight_map[name]), "pt") as handle:
        stored = handle.get_tensor(name)
    delta = (
        adapter["layers.0.self_attn.q_proj.B"] @ adapter["layers.0.self_attn.q_proj.A"]
    ) * 2.0
    assert stored.dtype == torch.bfloat16
    assert torch.equal(stored, (tensors[name].float() + delta).to(torch.bfloat16))
    assert receipt["tensors"][name]["max_abs_rounding"] > 0


def test_merge_encoder_refuses_changed_artifacts_and_missing_bases(tmp_path):
    candidate, encoder = tmp_path / "cand", tmp_path / "enc"
    write_candidate(candidate)
    write_encoder(encoder, layers=1)
    with pytest.raises(ValueError, match="without a base weight"):
        export.merge_encoder(candidate, encoder, tmp_path / "out1")
    assert not (tmp_path / "out1").exists()
    write_encoder(tmp_path / "enc2")
    (candidate / "protocol.json").write_text(
        (candidate / "protocol.json").read_text().replace('"alpha": 4', '"alpha": 8')
    )
    with pytest.raises(ValueError, match="artifact changed"):
        export.merge_encoder(candidate, tmp_path / "enc2", tmp_path / "out2")


def test_export_reader_writes_non_release_dual_layer_bundle(tmp_path):
    candidate = tmp_path / "cand"
    _, reader = write_candidate(candidate)
    output = tmp_path / "bundle"
    manifest = export.export_reader(candidate, output, config=CONFIG)
    config, weights, loaded = load_bundle(output)
    assert loaded == manifest
    assert config == CONFIG
    assert manifest["release"] is False
    assert manifest["base_models"] is None
    assert manifest["execution"] == {
        **EXECUTION,
        "encoder_pooling": PER_TOKEN_DUAL_LAYER_POOLING,
    }
    assert manifest["dtype"] == "float32"
    assert manifest["source"]["sha256"] == exporter.sha256(
        candidate / "reader.safetensors"
    )
    assert manifest["source"]["candidate_sha256"] == exporter.sha256(
        candidate / "candidate.json"
    )
    assert manifest["source"]["checkpoint"] is None
    assert set(manifest["files"]) == {
        "config.json",
        "memory.safetensors",
        "candidate.json",
        "copy-head.safetensors",
        "protocol.json",
    }
    for name in export.BUNDLE_EXTRA_FILES:
        assert (output / name).read_bytes() == (candidate / name).read_bytes()
        assert manifest["files"][name] == {
            "sha256": exporter.sha256(candidate / name),
            "size_bytes": (candidate / name).stat().st_size,
        }
    assert (
        manifest["files"]["candidate.json"]["sha256"]
        == (manifest["source"]["candidate_sha256"])
    )
    assert sorted(p.name for p in output.iterdir()) == sorted(
        ["manifest.json", *manifest["files"]]
    )
    # Pinned extras are verified on load like the config and weights.
    (output / "protocol.json").write_text("{}\n")
    with pytest.raises(ValueError, match="checksum mismatch: protocol.json"):
        load_bundle(output)
    (output / "protocol.json").write_bytes((candidate / "protocol.json").read_bytes())
    load_bundle(output)
    release_weights = {
        name: torch.randn(shape).to(torch.bfloat16)
        for name, shape in expected_shapes(RELEASE_CONFIG, split=True).items()
    }
    with pytest.raises(ValueError, match="only their config and weights"):
        exporter.write_bundle(
            release_weights,
            RELEASE_CONFIG,
            tmp_path / "release",
            release=True,
            source={},
            extra_files={"protocol.json": candidate / "protocol.json"},
        )
    assert not (tmp_path / "release").exists()
    assert set(weights) == set(expected_shapes(CONFIG, split=True))
    for prefix in ("query_cond_attn", "cross_attn_layers.0", "cross_attn_layers.1"):
        for kind in ("weight", "bias"):
            parts = reader[f"{prefix}.in_proj_{kind}"].chunk(3, dim=0)
            for part, name in zip(parts, ("q", "k", "v"), strict=True):
                assert torch.equal(weights[f"{prefix}.{name}_proj.{kind}"], part)
    assert manifest["parameter_count"] == sum(t.numel() for t in reader.values())


def test_execution_contract_accepts_dual_layer_only_off_release():
    assert execution_contract() == EXECUTION
    assert execution_contract("last-token-l2-normalized") == EXECUTION
    alternate = execution_contract(PER_TOKEN_DUAL_LAYER_POOLING)
    assert alternate != EXECUTION
    assert validate_execution(alternate, release=False) == alternate
    assert validate_execution(EXECUTION, release=True) == EXECUTION
    with pytest.raises(ValueError, match="execution contract"):
        validate_execution(alternate, release=True)
    with pytest.raises(ValueError, match="execution contract"):
        validate_execution({**EXECUTION, "encoder_pooling": "mean"}, release=False)
    with pytest.raises(ValueError, match="execution contract"):
        validate_execution({**alternate, "gelu": "tanh"}, release=False)
    with pytest.raises(ValueError, match="encoder pooling"):
        execution_contract("mean")


def test_write_bundle_refuses_alternate_pooling_for_release(tmp_path):
    reader = {
        name: torch.randn(shape).to(torch.bfloat16)
        for name, shape in expected_shapes(RELEASE_CONFIG, split=True).items()
    }
    with pytest.raises(ValueError, match="released execution contract"):
        exporter.write_bundle(
            reader,
            RELEASE_CONFIG,
            tmp_path / "bundle",
            release=True,
            source={},
            encoder_pooling=PER_TOKEN_DUAL_LAYER_POOLING,
        )
    assert not (tmp_path / "bundle").exists()


def test_reference_fixture_round_trip(tmp_path):
    cases = [
        {
            "name": "dev-0000-code-a",
            "documents": [" The brass key.", " Another."],
            "question": "What is the code?",
            "expected_answer": "Kg9-009369",
            "copy_ids": [1, 2, 3, 4, 5],
            "spans": [[0, 3, 1]],
            "snapped_ids": list(range(16)),
            "completed_ids": list(range(16)),
            "prompt_ids": [7, 8, 9],
            "output_ids": [10, 151645],
            "answer": "Kg9-009369",
        }
    ]
    tensors = {
        "dev-0000-code-a.docs": torch.randn(12, 8),
        "dev-0000-code-a.query": torch.randn(8),
        "dev-0000-code-a.memories": torch.randn(16, 4),
        "dev-0000-code-a.copy_output": torch.randn(16, 4),
        "dev-0000-code-a.gate": torch.rand(16),
        "dev-0000-code-a.logits": torch.randn(16, 5),
    }
    header = {
        "dual_layer": 24,
        "system_prompt": SYSTEM,
        "pad_id": 220,
        "eos_ids": [151645, 151643],
    }
    receipt = export.write_reference(tmp_path / "fixtures", header, cases, tensors)
    written = json.loads((tmp_path / "fixtures" / "reference.json").read_text())
    assert written == receipt
    assert written["schema"] == export.REFERENCE_SCHEMA
    assert written["cases"] == cases
    assert written["dual_layer"] == 24
    assert written["tensors"]["sha256"] == exporter.sha256(
        tmp_path / "fixtures" / "reference.safetensors"
    )
    restored = load_file(str(tmp_path / "fixtures" / "reference.safetensors"))
    assert set(restored) == set(tensors)
    for name, tensor in tensors.items():
        assert torch.equal(restored[name], tensor)
        assert written["tensors"]["inventory"][name] == {
            "shape": list(tensor.shape),
            "dtype": "float32",
        }
    with pytest.raises(ValueError, match="already exists"):
        export.write_reference(tmp_path / "fixtures", header, cases, tensors)
    with pytest.raises(ValueError, match="no '.'"):
        export.write_reference(
            tmp_path / "other", header, [dict(cases[0], name="a.b")], {}
        )
    with pytest.raises(ValueError, match="FP32 case boundary"):
        export.write_reference(
            tmp_path / "other",
            header,
            cases,
            {"dev-0000-code-a.gate": torch.rand(16).to(torch.bfloat16)},
        )


def test_case_files_round_trip_and_reject_dotted_names(tmp_path):
    rows = [
        {
            "id": "dev-0000-code-a",
            "group": "dev-0000",
            "category": "code",
            "cohort": "original-development",
            "documents": ["a", "b"],
            "question": "q?",
            "answer": "A",
        },
        {"id": "x.y", "documents": ["a"], "question": "q?", "answer": "B"},
    ]
    cases = [export.case_entry(row, "dev") for row in rows]
    assert cases[0]["name"] == "dev-0000-code-a"
    assert cases[0]["cohort"] == "original-development"
    assert cases[1]["name"] == "x_y"
    assert cases[1]["cohort"] == "dev"
    export.write_cases(tmp_path / "cases.json", cases, split="dev")
    loaded = export.read_cases(tmp_path / "cases.json")
    assert [c["name"] for c in loaded] == ["dev-0000-code-a", "x_y"]
    (tmp_path / "plain.json").write_text(
        json.dumps(
            [{"name": "n.1", "documents": ["a"], "question": "q", "answer": "a"}]
        )
    )
    with pytest.raises(ValueError, match="without '.'"):
        export.read_cases(tmp_path / "plain.json")
    with pytest.raises(ValueError, match="duplicate"):
        export.write_cases(tmp_path / "dup.json", cases + cases[:1], split="dev")


def test_export_reader_can_pin_encoder_lora_for_run_time_adapters(tmp_path):
    candidate = tmp_path / "cand"
    write_candidate(candidate)
    output = tmp_path / "bundle"
    manifest = export.export_reader(
        candidate, output, config=CONFIG, include_encoder_lora=True
    )
    _, _, loaded = load_bundle(output)
    assert loaded == manifest
    lora = candidate / "encoder-lora.safetensors"
    assert manifest["files"]["encoder-lora.safetensors"] == {
        "sha256": exporter.sha256(lora),
        "size_bytes": lora.stat().st_size,
    }
    assert (output / "encoder-lora.safetensors").read_bytes() == lora.read_bytes()
    candidate_json = json.loads((output / "candidate.json").read_text())
    assert (
        candidate_json["encoder_lora_sha256"]
        == manifest["files"]["encoder-lora.safetensors"]["sha256"]
    )
    default = export.export_reader(candidate, tmp_path / "plain", config=CONFIG)
    assert "encoder-lora.safetensors" not in default["files"]


def test_export_reader_pins_a_span_head_only_for_span_window_candidates(tmp_path):
    candidate = tmp_path / "cand"
    write_candidate(candidate)
    with pytest.raises(ValueError, match="span-window"):
        export.export_reader(candidate, tmp_path / "x", config=CONFIG, include_span_head=True)
    head = candidate / "span-head.safetensors"
    head.write_bytes(b"span head bytes")
    protocol = json.loads((candidate / "protocol.json").read_text())
    protocol["record_kind"] = "span-window"
    (candidate / "protocol.json").write_text(json.dumps(protocol))
    pinned = json.loads((candidate / "candidate.json").read_text())
    pinned["protocol_sha256"] = exporter.sha256(candidate / "protocol.json")
    pinned["span_head_sha256"] = "0" * 64
    (candidate / "candidate.json").write_text(json.dumps(pinned))
    with pytest.raises(ValueError, match="span-head"):
        export.export_reader(candidate, tmp_path / "y", config=CONFIG, include_span_head=True)
    pinned["span_head_sha256"] = exporter.sha256(head)
    (candidate / "candidate.json").write_text(json.dumps(pinned))
    output = tmp_path / "bundle"
    manifest = export.export_reader(
        candidate, output, config=CONFIG, include_encoder_lora=True, include_span_head=True
    )
    assert manifest["files"]["span-head.safetensors"]["sha256"] == exporter.sha256(head)
    assert (output / "span-head.safetensors").read_bytes() == head.read_bytes()
