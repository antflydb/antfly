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

import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

import native_attention_matching as native
import native_repeat_attention_matching as repeat_native


class NativeHeadInputTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.build = patch.object(native, "validate_build", return_value={"receipt_sha256": "6" * 64, "binary_sha256": "5" * 64})
        self.mock_build = self.build.start()
        self.addCleanup(self.build.stop)
        self.document = {"id": "synthetic", "sha256": "8" * 64, "bucket": "fixture"}
        self.keys = struct.pack("<2050e", *([0] * 2050))
        self.values = struct.pack("<2050e", *([1] * 2050))
        self.q = struct.pack("<64f", *([0] * 64))
        (self.root / "keys.f16").write_bytes(self.keys)
        (self.root / "values.f16").write_bytes(self.values)
        (self.root / "queries.f32").write_bytes(self.q)
        self.strata = [{"consumer_layer": 4, "query_head": i, "kv_head": 0} for i in range(4)]
        self.query = {
            "schema_version": 2, "purpose": "calibration",
            "document": self.document,
            "model": {"sha256": "1" * 64, "tokenizer_sha256": "2" * 64,
                      "template_sha256": "3" * 64, "layout_sha256": "4" * 64},
            "capture": {"owner_layer": 4, "heads": 1, "dim": 2, "rows": 512, "block_start": 1,
                        "query_origin": "multi_branch_v2", "query_sampling": "source_uniform_partition_v3",
                        "training_rows": 24, "validation_rows": 8, "evaluation_rows": 0, "scale": 0.5,
                        "strata": [dict(s, training_start=i * 6, training_count=6,
                            validation_start=i * 2, validation_count=2,
                            query_source_ranges={"training": [{"source_id": "fit"}],
                                                 "validation": [{"source_id": "gate"}]})
                                   for i, s in enumerate(self.strata)]},
            "tensors": {
                "keys": {"bytes": 2048, "sha256": native.sha(self.keys[4:2052])},
                "values": {"bytes": 2048, "sha256": native.sha(self.values[4:2052])},
                "queries": {"bytes": len(self.q), "path": "queries.f32", "sha256": native.sha(self.q)},
            },
        }
        self.head = {
            "prefix_storage": "packed_exact", "tensor_dtype": "f16_le",
            "tensor_order": "logical_row_kv_head_head_dim", "owner_layer": 4,
            "heads": 1, "dim": 2, "rows": 1025, "block_start": 0, "scale": 0.5,
            "prompt": {"tokens": 1025, "sha256": "7" * 64}, "document": self.document,
            "loaded_identity": {"load_generation": 1, "artifact_sha256": "1" * 64,
                "tokenizer_sha256": "2" * 64, "template_sha256": "3" * 64, "layout_sha256": "4" * 64},
            "provenance": {"binary_sha256": "5" * 64, "source_build_sha256": "6" * 64},
            "strata": self.strata,
            "keys": {"path": "keys.f16", "sha256": native.sha(self.keys)},
            "values": {"path": "values.f16", "sha256": native.sha(self.values)},
        }
        self.authority = {"training_source_authority": True, "held_tensors_loaded": False, "release_authority": False}
        self.kwargs = self.seal_metadata()

    def json_file(self, name, value):
        data = json.dumps(value, sort_keys=True).encode()
        path = self.root / name
        path.write_bytes(data)
        return path, native.sha(data)

    def seal_metadata(self):
        query_path, query_sha = self.json_file("query.json", self.query)
        head_path, head_sha = self.json_file("head.json", self.head)
        self.authority["cells"] = [{"owner_layer": 4, "manifest": {"sha256": query_sha}}]
        auth_path, auth_sha = self.json_file("authority.json", self.authority)
        index = {"schema": "antfly.context.native.full-head-source.v2", "diagnostic_only": True,
            "serving_qualified": False, "prompt_tokens": 1025, "prompt_tokens_sha256": "7" * 64,
            "artifact_sha256": "1" * 64, "load_generation": 1, "binary_sha256": "5" * 64,
            "source_build_sha256": self.head["provenance"]["source_build_sha256"], "eligible_block_starts": [1],
            "descriptors": [{"owner_layer": 4, "block_start": 0, "path": head_path.name, "sha256": head_sha}]}
        index_path, index_sha = self.json_file("index.json", index)
        return dict(source_index=index_path, source_sha256=index_sha,
            query_manifest=query_path, query_sha256=query_sha,
            training_authority=auth_path, authority_sha256=auth_sha,
            source_build_receipt=self.root / "build.json",
            min_recent_rows=512,
            owner=4, output=self.root / "input.bin")

    def configure_two_heads(self):
        self.strata = [{"consumer_layer": 4, "query_head": i, "kv_head": i // 4}
                       for i in range(8)]
        capture_strata = []
        local = [0, 0]
        for s in self.strata:
            index = local[s["kv_head"]]
            local[s["kv_head"]] += 1
            capture_strata.append(dict(s, training_start=index * 6, training_count=6,
                validation_start=index * 2, validation_count=2,
                query_source_ranges={"training": [{"source_id": "fit"}],
                                     "validation": [{"source_id": "gate"}]}))
        key_values = []
        value_values = []
        for _ in range(1025):
            key_values.extend((1, 2, 11, 12))
            value_values.extend((3, 4, 13, 14))
        self.keys = struct.pack("<4100e", *key_values)
        self.values = struct.pack("<4100e", *value_values)
        (self.root / "keys.f16").write_bytes(self.keys)
        (self.root / "values.f16").write_bytes(self.values)
        plane = 64
        self.q = struct.pack("<128f", *([5] * plane + [15] * plane))
        (self.root / "queries.f32").write_bytes(self.q)
        self.head.update(heads=2, strata=self.strata)
        self.head["keys"] = {"path": "keys.f16", "sha256": native.sha(self.keys)}
        self.head["values"] = {"path": "values.f16", "sha256": native.sha(self.values)}
        self.query["capture"].update(heads=2, strata=capture_strata)
        row_bytes = 2 * 2 * 2
        start = row_bytes
        self.query["tensors"]["keys"] = {"bytes": 512 * row_bytes,
            "sha256": native.sha(self.keys[start:start + 512 * row_bytes])}
        self.query["tensors"]["values"] = {"bytes": 512 * row_bytes,
            "sha256": native.sha(self.values[start:start + 512 * row_bytes])}
        self.query["tensors"]["queries"] = {"bytes": len(self.q), "path": "queries.f32",
            "sha256": native.sha(self.q)}

    def test_seals_complete_head_and_explicit_development_role(self):
        manifest = native.prepare(**self.kwargs)
        data = self.kwargs["output"].read_bytes()
        header = native.HEADER.unpack_from(data)
        self.assertEqual(header[:8], (b"AMHEAD02", 1025, 2, 24, 8, 2, 1, 512))
        self.assertEqual(manifest["input"]["sha256"], native.sha(data))
        self.assertEqual(data[64:], self.keys + self.values + self.q)
        self.assertEqual(manifest["evaluation_role"], "internal_development")
        self.assertFalse(manifest["serving_qualified"])
        self.assertFalse(manifest["fresh_unseen_qualified"])
        self.assertEqual(manifest["kv_witness_dtype"], "f16_le")
        self.assertEqual(manifest["preparer_sha256"], native.sha(Path(native.__file__).read_bytes()))
        with self.assertRaisesRegex(ValueError, "already exists"):
            native.prepare(**self.kwargs)

    def test_two_head_input_selects_exact_interleaved_kv_and_head_major_queries(self):
        self.configure_two_heads()
        kwargs = self.seal_metadata()
        kwargs.update(kv_head=1, output=self.root / "head1.bin")
        manifest = native.prepare(**kwargs)
        payload = kwargs["output"].read_bytes()[native.HEADER.size:]
        expected_keys = struct.pack("<2050e", *([11, 12] * 1025))
        expected_values = struct.pack("<2050e", *([13, 14] * 1025))
        expected_queries = struct.pack("<64f", *([15] * 64))
        self.assertEqual(payload, expected_keys + expected_values + expected_queries)
        self.assertEqual((manifest["source_kv_heads"], manifest["kv_head"],
                          manifest["consumer_groups"]), (2, 1, 4))
        self.assertEqual(manifest["source_tensor_order"], "logical_row_kv_head_head_dim")

    def test_two_head_input_rejects_global_instead_of_head_local_starts_before_query_read(self):
        self.configure_two_heads()
        self.query["capture"]["strata"][4]["training_start"] = 24
        kwargs = self.seal_metadata()
        kwargs.update(kv_head=1, output=self.root / "head1.bin")
        (self.root / "queries.f32").unlink()
        with self.assertRaisesRegex(ValueError, "head-local contiguous"):
            native.prepare(**kwargs)

    def test_two_head_input_rejects_reordered_strata_before_tensor_read(self):
        self.configure_two_heads()
        self.query["capture"]["strata"][0], self.query["capture"]["strata"][1] = (
            self.query["capture"]["strata"][1], self.query["capture"]["strata"][0])
        kwargs = self.seal_metadata()
        kwargs.update(kv_head=1, output=self.root / "head1.bin")
        (self.root / "keys.f16").unlink()
        with self.assertRaisesRegex(ValueError, "reordered physical consumer"):
            native.prepare(**kwargs)

    def test_role_rejection_precedes_any_tensor_read(self):
        self.authority["training_source_authority"] = False
        self.kwargs = self.seal_metadata()
        (self.root / "keys.f16").unlink()
        with self.assertRaisesRegex(ValueError, "no training authority"):
            native.prepare(**self.kwargs)
        self.assertFalse(self.kwargs["output"].exists())

    def test_rejects_missing_consumer_before_tensor_read(self):
        self.query["capture"]["strata"].pop()
        self.kwargs = self.seal_metadata()
        (self.root / "queries.f32").unlink()
        with self.assertRaisesRegex(ValueError, "physical consumer"):
            native.prepare(**self.kwargs)

    def test_rejects_overlapping_training_validation_sources(self):
        self.query["capture"]["strata"][0]["query_source_ranges"]["validation"][0]["source_id"] = "fit"
        self.kwargs = self.seal_metadata()
        with self.assertRaisesRegex(ValueError, "sources overlap"):
            native.prepare(**self.kwargs)

    def test_rejects_changed_kv_witness_before_query_read(self):
        changed = self.keys[:4] + struct.pack("<e", 1) + self.keys[6:]
        (self.root / "keys.f16").write_bytes(changed)
        self.head["keys"]["sha256"] = native.sha(changed)
        self.kwargs = self.seal_metadata()
        (self.root / "queries.f32").unlink()
        with self.assertRaisesRegex(ValueError, "KV witness differ"):
            native.prepare(**self.kwargs)

    def test_rejects_f32_witness_before_query_read(self):
        self.query["tensors"]["keys"]["bytes"] *= 2
        self.kwargs = self.seal_metadata()
        (self.root / "queries.f32").unlink()
        with self.assertRaisesRegex(ValueError, "KV witness differ"):
            native.prepare(**self.kwargs)

    def test_rejects_tensor_symlink(self):
        p = self.root / "keys.f16"
        p.unlink()
        p.symlink_to(self.root / "values.f16")
        with self.assertRaises(OSError):
            native.prepare(**self.kwargs)

    def test_rejects_build_provenance_before_tensor_read(self):
        self.mock_build.return_value["binary_sha256"] = "9" * 64
        (self.root / "queries.f32").unlink()
        with self.assertRaisesRegex(ValueError, "build provenance mismatch"):
            native.prepare(**self.kwargs)

    def test_recent_window_cannot_be_shortened_by_capture_eligibility(self):
        self.kwargs["min_recent_rows"] = 513
        with self.assertRaisesRegex(ValueError, "protected recent window"):
            native.prepare(**self.kwargs)

    def repeat_fixture(self, multihead=False, selected_head=0):
        receipt = b"synthetic build receipt"
        (self.root / "build.json").write_bytes(receipt)
        self.mock_build.return_value["receipt_sha256"] = native.sha(receipt)
        self.mock_build.return_value["binary_path"] = self.root / "context-tests"
        self.head["provenance"]["source_build_sha256"] = native.sha(receipt)
        kwargs = self.seal_metadata()
        index_path = self.root / "fixture-native-head-index.json"
        index_path.write_bytes(kwargs["source_index"].read_bytes())
        kwargs["source_index"] = index_path
        prompt = struct.pack("<1025q", *([0] * 1025))
        (self.root / "prompt.i64").write_bytes(prompt)
        inputs, inputs_sha = self.json_file("capture-inputs.json", {
            "schema_version": 1, "stage": "context_numerical_capture_inputs", "completed": True,
            "loaded_identity": self.head["loaded_identity"], "provenance": self.head["provenance"],
            "prompt": {"path": "prompt.i64", "sha256": native.sha(prompt), "count": 1025}})
        # Bind the synthetic prompt to the existing complete-head fixture.
        self.head["prompt"]["sha256"] = native.sha(prompt)
        head_path, head_sha = self.json_file("head.json", self.head)
        index = json.loads(index_path.read_text())
        index["prompt_tokens_sha256"] = native.sha(prompt)
        index["descriptors"][0]["sha256"] = head_sha
        index_path, index_sha = self.json_file(index_path.name, index)
        kwargs.update(source_index=index_path, source_sha256=index_sha)
        owner_rows = []
        source_heads = 2 if multihead else 1
        for physical_head in range(source_heads):
            refs = {}
            head_strata = [s for s in self.strata if s["kv_head"] == physical_head]
            for name, held, rows, group_rows in (("training", False, 24, 6), ("validation", True, 8, 2)):
                q = struct.pack("<" + str(rows * 2) + "f",
                                *([physical_head * 10 + (1 if name == "training" else 2)] * rows * 2))
                suffix = "-head" + str(physical_head) if multihead else ""
                path = self.root / (name + suffix + ".f32")
                path.write_bytes(q)
                refs[name] = {"path": path.name, "sha256": native.sha(q), "bytes": len(q)}
                positions = repeat_native.expected_positions(1025, 512, held, group_rows) * len(head_strata)
                raw = struct.pack("<" + str(rows) + "I", *positions)
                path = self.root / (name + suffix + "-positions.u32")
                path.write_bytes(raw)
                refs[name + "_positions"] = {"path": path.name, "sha256": native.sha(raw), "bytes": len(raw)}
            row = {"owner_layer": 4, "head_dim": 2, "strata": head_strata,
                   "training_rows": 24, "validation_rows": 8, "training_group_rows": 6,
                   "validation_group_rows": 2, **refs}
            if multihead:
                row.update(source_heads=source_heads, kv_head=physical_head)
            owner_rows.append(row)
        tokens = struct.pack("<512q", *([0] * 512))
        (self.root / "repeat.i64").write_bytes(tokens)
        self.repeat_meta = {"schema": "antfly.context.native.repeat-source.v2" if multihead else "antfly.context.native.repeat-source.v1", "diagnostic_only": True,
            "evaluation_role": "internal_development", "serving_qualified": False, "fresh_unseen_qualified": False,
            "query_sampling": "representative_repeat_partition_v1", "selection": "history_only_cuda_study_v1",
            "position_partition": "hashed_quartet_v1", "source_kv_readback_bytes": 0,
            "repeat_start": 1025, "repeat_rows": 512, "load_generation": 1,
            "binary_sha256": "5" * 64, "source_build_sha256": native.sha(receipt),
            "capture_inputs_manifest_sha256": inputs_sha,
            "full_head_source_index": {"path": index_path.name, "sha256": index_sha},
            "repeat_tokens": {"path": "repeat.i64", "sha256": native.sha(tokens), "bytes": len(tokens)},
            "owners": owner_rows}
        command = ["/usr/bin/env", "MTL_DEBUG_LAYER=1", "ANTFLY_CONTEXT_CAMPAIGN_CAPTURE=1",
            "ANTFLY_CONTEXT_CAPTURE_NATIVE_FULL_HEADS=1", "ANTFLY_CONTEXT_CAPTURE_NATIVE_REPEAT=1",
            "ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES=8589934592",
            "ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR=" + str(self.root),
            "ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST=" + inputs.name,
            "ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256=" + inputs_sha,
            "ANTFLY_CONTEXT_CAMPAIGN_CAPTURE_STEM=fixture", "ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT=build.json",
            str(self.root / "context-tests"), "--test-filter", "context Metal installed Gemma production query campaign capture"]
        self.repeat_resource = {"pass": True, "returncode": 0, "violation": None,
            "swapout_growth_mib": 0, "remaining_child_group_live_processes": 0, "command": command,
            "limits": {"max_rss_mib": 8192, "max_swap_growth_mib": 0, "min_free_percent": 15, "min_disk_free_mib": 4096}}
        (self.root / "capture-resource.stdout").write_bytes(b"")
        (self.root / "capture-resource.stderr").write_bytes(b"Metal API Validation Enabled\ncontext_native_repeat_capture: repeat_tokens=512 original_unchanged=true source_kv_readback_bytes=0 serving_qualified=false\n1 selected; 1 passed; 0 skipped.\n")
        kwargs.update(capture_inputs=inputs, output=self.root / "repeat-input.bin",
                      kv_head=selected_head)
        return self.seal_repeat(kwargs)

    def seal_repeat(self, kwargs):
        path, digest = self.json_file("fixture-native-repeat-index.json", self.repeat_meta)
        stderr_path = self.root / "capture-resource.stderr"
        base = b"\n".join(line for line in stderr_path.read_bytes().splitlines()
                           if not line.startswith(b"context_native_repeat_artifact:")) + b"\n"
        stderr_path.write_bytes(base + ("context_native_repeat_artifact: index=" + path.name +
            " sha256=" + digest + "\n").encode())
        resource, resource_sha = self.json_file("capture-resource.json", self.repeat_resource)
        return dict(kwargs, repeat_source=path, repeat_sha256=digest,
                    capture_resource=resource, resource_sha256=resource_sha)

    def test_repeat_seals_disjoint_history_queries_without_reading_old_validation(self):
        kwargs = self.repeat_fixture()
        (self.root / "queries.f32").unlink()
        manifest = repeat_native.prepare(**kwargs)
        self.assertEqual(manifest["query_sampling"], "representative_repeat_partition_v1")
        self.assertFalse(manifest["old_validation_queries_used_for_fit"])
        self.assertFalse(manifest["fresh_unseen_qualified"])
        self.assertEqual(native.HEADER.unpack_from(kwargs["output"].read_bytes())[:6],
                         (b"AMHEAD02", 1025, 2, 24, 8, 2))

    def test_repeat_v2_selects_distinct_second_head_bytes_and_strata(self):
        self.configure_two_heads()
        kwargs = self.repeat_fixture(multihead=True, selected_head=1)
        (self.root / "queries.f32").unlink()
        manifest = repeat_native.prepare(**kwargs)
        payload = kwargs["output"].read_bytes()[native.HEADER.size:]
        expected_keys = struct.pack("<2050e", *([11, 12] * 1025))
        expected_values = struct.pack("<2050e", *([13, 14] * 1025))
        training = struct.pack("<48f", *([11] * 48))
        validation = struct.pack("<16f", *([12] * 16))
        self.assertEqual(payload, expected_keys + expected_values + training + validation)
        self.assertEqual((manifest["source_kv_heads"], manifest["kv_head"],
                          manifest["consumer_groups"]), (2, 1, 4))

    def test_repeat_v2_rejects_missing_selected_head_before_query_read(self):
        self.configure_two_heads()
        kwargs = self.repeat_fixture(multihead=True, selected_head=1)
        self.repeat_meta["owners"].pop()
        (self.root / "training-head1.f32").unlink()
        with self.assertRaisesRegex(ValueError, "missing or duplicate repeat owner"):
            repeat_native.prepare(**self.seal_repeat(kwargs))

    def test_repeat_v2_rejects_wrong_source_head_count_before_query_read(self):
        self.configure_two_heads()
        kwargs = self.repeat_fixture(multihead=True, selected_head=1)
        self.repeat_meta["owners"][1]["source_heads"] = 1
        (self.root / "training-head1.f32").unlink()
        with self.assertRaisesRegex(ValueError, "consumer coverage"):
            repeat_native.prepare(**self.seal_repeat(kwargs))

    def test_repeat_rejects_unbound_artifact_record_before_query_read(self):
        kwargs = self.repeat_fixture()
        stderr = self.root / "capture-resource.stderr"
        stderr.write_bytes(stderr.read_bytes().replace(kwargs["repeat_sha256"].encode(), b"0" * 64))
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "completion evidence missing"):
            repeat_native.prepare(**kwargs)

    def test_repeat_rejects_duplicate_artifact_record_before_query_read(self):
        kwargs = self.repeat_fixture()
        stderr = self.root / "capture-resource.stderr"
        record = next(line for line in stderr.read_bytes().splitlines()
                      if line.startswith(b"context_native_repeat_artifact:"))
        (self.root / "capture-resource.stdout").write_bytes(record + b"\n")
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "completion evidence missing"):
            repeat_native.prepare(**kwargs)

    def test_repeat_rejects_noncanonical_index_name_before_query_read(self):
        kwargs = self.repeat_fixture()
        renamed = self.root / "renamed-repeat.json"
        renamed.write_bytes(kwargs["repeat_source"].read_bytes())
        kwargs.update(repeat_source=renamed)
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "output binding mismatch"):
            repeat_native.prepare(**kwargs)

    def test_repeat_rejects_cross_partition_positions_before_query_read(self):
        kwargs = self.repeat_fixture()
        path = self.root / "validation-positions.u32"
        raw = bytearray(path.read_bytes())
        raw[:4] = (self.root / "training-positions.u32").read_bytes()[:4]
        path.write_bytes(raw)
        self.repeat_meta["owners"][0]["validation_positions"]["sha256"] = native.sha(raw)
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "disjoint uniform partition"):
            repeat_native.prepare(**self.seal_repeat(kwargs))

    def test_repeat_rejects_incomplete_consumers_before_query_read(self):
        kwargs = self.repeat_fixture()
        self.repeat_meta["owners"][0]["strata"] = self.strata[:-1]
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "consumer coverage"):
            repeat_native.prepare(**self.seal_repeat(kwargs))

    def test_repeat_rejects_failed_capture_before_query_read(self):
        kwargs = self.repeat_fixture()
        self.repeat_resource["pass"] = False
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "clean resource guards"):
            repeat_native.prepare(**self.seal_repeat(kwargs))

    def test_repeat_rejects_nonhistory_tokens_before_query_read(self):
        kwargs = self.repeat_fixture()
        path = self.root / "repeat.i64"
        raw = bytearray(path.read_bytes())
        raw[:8] = struct.pack("<q", 17)
        path.write_bytes(raw)
        self.repeat_meta["repeat_tokens"]["sha256"] = native.sha(raw)
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "history subsequence"):
            repeat_native.prepare(**self.seal_repeat(kwargs))

    def test_repeat_rejects_old_campaign_capture_before_query_read(self):
        kwargs = self.repeat_fixture()
        self.repeat_resource["command"].remove("ANTFLY_CONTEXT_CAPTURE_NATIVE_REPEAT=1")
        (self.root / "training.f32").unlink()
        with self.assertRaisesRegex(ValueError, "not a native representative replay"):
            repeat_native.prepare(**self.seal_repeat(kwargs))


if __name__ == "__main__":
    unittest.main()
