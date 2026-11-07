#!/usr/bin/env python3
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

import math
import io
import sys
from pathlib import Path
from unittest import mock
import copy
import unittest
import json
import tempfile
import types

if __package__:
    from . import compare
else:
    import compare


class ComparisonContracts(unittest.TestCase):
    def test_text_prepare_never_reads_or_hashes_media(self):
        tokenizer = mock.Mock()
        tokenizer.encode.side_effect = lambda text: types.SimpleNamespace(ids=list(range(len(text.split()))))
        package = types.SimpleNamespace(Tokenizer=mock.Mock())
        package.Tokenizer.from_file.return_value = tokenizer
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "text-suite.json"
            # No artifacts directory, media oracle, image or audio exists.
            with mock.patch.dict(sys.modules, {"tokenizers": package}), mock.patch.object(compare.reference, "sha256", side_effect=AssertionError("media hash attempted")):
                compare.prepare(root, None, output, text_only=True)
            suite = json.loads(output.read_text())
        self.assertEqual(suite["evaluation_scope"], "text-only")
        self.assertEqual(suite["assets"], {})
        self.assertEqual(suite["case_selection"], [case["name"] for case in suite["cases"]])
        self.assertTrue(all("text" in case and "oracle" not in case for case in suite["cases"]))
        self.assertEqual([len(case["token_ids"]) for case in suite["cases"][2:5]], [128, 512, 8192])
        self.assertIn("decision", suite)
        self.assertIn("retrieval", suite)

    def test_prepare_cli_allows_absent_artifacts_only_with_explicit_text_scope(self):
        argv = ["compare.py", "--backend", "prepare", "--model-dir", "model", "--output", "suite.json"]
        with mock.patch.object(sys, "argv", argv + ["--text-only"]), mock.patch.object(compare, "prepare") as prepared:
            compare.main()
            prepared.assert_called_once_with(Path("model"), None, Path("suite.json"), True)
        with mock.patch.object(sys, "argv", argv), mock.patch.object(compare, "prepare") as prepared:
            with self.assertRaises(SystemExit):
                compare.main()
            prepared.assert_not_called()

    def test_full_prepare_still_requires_real_media_oracle(self):
        tokenizer = mock.Mock()
        tokenizer.encode.side_effect = lambda text: types.SimpleNamespace(ids=list(range(len(text.split()))))
        package = types.SimpleNamespace(Tokenizer=mock.Mock())
        package.Tokenizer.from_file.return_value = tokenizer
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(sys.modules, {"tokenizers": package}):
            root = Path(directory)
            with self.assertRaises(FileNotFoundError):
                compare.prepare(root, root / "missing-media", root / "suite.json")
            self.assertFalse((root / "suite.json").exists())

    def test_text_scope_retains_unknown_duplicate_and_media_selection_errors(self):
        text = {"name": "query", "text": "x", "token_ids": [1]}
        media = {"name": "image", "token_ids": [2]}
        suite = {"cases": [text, media]}
        self.assertEqual(compare.select_cases(suite, None, text_only=True)["cases"], [text])
        self.assertEqual(compare.select_cases(suite, ["query"], text_only=True)["cases"], [text])
        for names in (["missing"], ["query", "query"], ["image"], ["image", "query"]):
            with self.subTest(names=names), self.assertRaises(ValueError):
                compare.select_cases(suite, names, text_only=True)
        with self.assertRaises(ValueError):
            compare.select_cases({"cases": [media]}, None, text_only=True)
        # Default full mode still selects actual media.
        self.assertEqual(compare.select_cases(suite, ["image"])["cases"], [media])

    def test_text_torch_media_boundary_imports_nothing_optional(self):
        with mock.patch("builtins.__import__", side_effect=AssertionError("optional media import")):
            self.assertEqual(compare.load_torch_media(Path("model"), {"cases": [{"name": "text", "text": "x"}]}), (None, None, None))
            with self.assertRaises(ValueError):
                compare.load_torch_media(Path("model"), {"cases": [{"name": "unknown"}]})

    def test_image_only_media_boundary_does_not_import_or_open_audio(self):
        image = mock.Mock()
        image.open.return_value.convert.return_value = "actual-image"
        processor = mock.Mock()
        processor.from_pretrained.return_value = "actual-processor"
        original_import = __import__
        def import_guard(name, *args, **kwargs):
            if name in ("wave", "numpy"):
                raise AssertionError("unselected audio dependency")
            return original_import(name, *args, **kwargs)
        suite = {"cases": [{"name": "image"}], "assets": {"image.png": {"path": "image-path"}}}
        with mock.patch.dict(sys.modules, {"PIL": types.SimpleNamespace(Image=image), "transformers": types.SimpleNamespace(AutoProcessor=processor)}), mock.patch("builtins.__import__", side_effect=import_guard):
            self.assertEqual(compare.load_torch_media(Path("model"), suite), ("actual-processor", "actual-image", None))
        image.open.assert_called_once_with("image-path")
        processor.from_pretrained.assert_called_once_with(Path("model"), local_files_only=True)

    def test_selected_media_still_fails_without_requested_asset(self):
        processor = mock.Mock()
        with mock.patch.dict(sys.modules, {"PIL": types.SimpleNamespace(Image=mock.Mock()), "transformers": types.SimpleNamespace(AutoProcessor=processor)}):
            with self.assertRaises(KeyError):
                compare.load_torch_media(Path("model"), {"cases": [{"name": "image"}], "assets": {}})

    def test_post_declares_bounded_inference_budget_and_transport_timeout(self):
        with mock.patch.object(
            compare.urllib.request, "urlopen", return_value=io.BytesIO(b'{"ok":true}')
        ) as opened:
            self.assertEqual(
                compare.post(
                    "http://localhost", "/embed", {"input": ["x"]}, timeout=6.3278
                ),
                {"ok": True},
            )
        request = opened.call_args.args[0]
        self.assertEqual(request.get_header("X-antfly-inference-timeout-ms"), "6327")
        self.assertEqual(opened.call_args.kwargs["timeout"], 6.3278)
        self.assertEqual(request.get_header("Content-type"), "application/json")

    def test_post_rejects_invalid_budget_before_network_io(self):
        with mock.patch.object(compare.urllib.request, "urlopen") as opened:
            for budget in (0, -1, math.inf, math.nan, 3600.001):
                with self.subTest(budget=budget):
                    with self.assertRaises(ValueError):
                        compare.post("http://localhost", "/embed", {}, timeout=budget)
            opened.assert_not_called()

    def test_deadline_probe_rejects_unverified_worker_disconnect(self):
        with mock.patch("sys.path", [str(Path(__file__).resolve().parent), *sys.path]):
            import video_deadline_qualification as probe

        self.assertEqual(
            probe.restart_generation(
                "inference worker stopped unexpectedly; restarting generation=2 delay_ms=100 term=.{ .exited = 86 }"
            ),
            2,
        )
        for text in (
            "",
            "restarting generation=2 exited = 86",
            "inference worker stopped unexpectedly; restarting generation=2 exited = 1",
            "inference worker stopped unexpectedly; restarting generation=1 exited = 86",
        ):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    probe.restart_generation(text)

    def test_case_selection_and_long_sample_controls(self):
        short = {"name": "short", "text": "x", "token_ids": [1] * 512}
        long = {"name": "long", "text": "x", "token_ids": [1] * 8192}
        media = {"name": "image", "token_ids": [1]}
        suite = {"cases": [short, long, media]}
        self.assertEqual(
            compare.select_cases(suite, None, True)["cases"], [short, long]
        )
        self.assertEqual(compare.select_cases(suite, ["long"], True)["cases"], [long])
        for names in (["missing"], ["short", "short"], ["image"]):
            with self.assertRaises(ValueError):
                compare.select_cases(suite, names, True)
        self.assertEqual(compare.iterations(long, 20), 1)
        self.assertEqual(compare.iterations(long, 20, 5), 5)
        self.assertEqual(compare.iterations(short, 20, 5), 20)
        self.assertEqual(compare.case_warmups(long, 2), 2)
        self.assertEqual(compare.case_warmups(long, None), 0)

    def test_strict_vector_gate_and_encoder_scope(self):
        actual = [math.sqrt(1 - 2e-5**2), 2e-5]
        compare.assert_agreement(actual, [1.0, 0.0])
        with self.assertRaises(ValueError):
            compare.assert_agreement(actual, [1.0, 0.0], 1e-5)
        result = {
            "suite_sha256": "s",
            "checkpoint_receipt_sha256": "w",
            "precision": "float32",
            "status": "pass",
            "measurement_mode": "encoder",
            "cases": {"x": {"vector": [1.0, 0.0]}},
            "decision": {"status": "not_run"},
        }
        self.assertEqual(
            compare.compare_reference(result, result, 1e-5)["decision_status"],
            "not_run",
        )
        bad = copy.deepcopy(result)
        bad["measurement_mode"] = "default"
        with self.assertRaises(ValueError):
            compare.compare_reference(result, bad)

    def test_percentiles_retain_sample_count_and_nearest_rank_tail(self):
        result = compare.timing([4.0, 1.0, 3.0, 2.0])
        self.assertEqual(result["count"], 4)
        self.assertEqual(result["p50_seconds"], 2.5)
        self.assertEqual(result["p95_seconds"], 4.0)

    def test_bad_timings_cannot_be_reported_as_measurements(self):
        for values in ([], [0.0], [-1.0], [math.inf], [math.nan]):
            with self.assertRaises(ValueError):
                compare.timing(values)

    def test_numerical_gate_checks_error_norm_and_direction(self):
        compare.assert_agreement([1.0, 0.0], [1.0, 0.0])
        for actual in ([2.0, 0.0], [-1.0, 0.0], [1.0, 0.1], [math.nan, 0.0]):
            with self.assertRaises(ValueError):
                compare.assert_agreement(actual, [1.0, 0.0])

    def test_shape_and_zero_vectors_fail_closed(self):
        for actual, expected in (([], []), ([1.0], [1.0, 0.0]), ([0.0], [1.0])):
            with self.assertRaises(ValueError):
                compare.agreement(actual, expected)

    def test_retrieval_preserves_full_rankings_and_cosine_scores(self):
        suite = {
            "retrieval": {
                "queries": ["q"],
                "documents": ["b", "a", "c"],
                "purpose": "parity",
            }
        }
        cases = {
            "q": {"vector": [1.0, 0.0]},
            "a": {"vector": [1.0, 0.0]},
            "b": {"vector": [0.0, 1.0]},
            "c": {"vector": [-1.0, 0.0]},
        }
        result = compare.retrieval_result(suite, cases)
        self.assertEqual(result["rankings"]["q"], ["a", "b", "c"])
        self.assertEqual(result["scores"]["q"], {"a": 1.0, "b": 0.0, "c": -1.0})
        self.assertIsNone(compare.retrieval_result({}, cases))

    def test_matched_reference_rejects_identity_vector_rank_and_route_changes(self):
        expected = {
            "suite_sha256": "suite",
            "checkpoint_receipt_sha256": "weights",
            "precision": "float32",
            "status": "pass",
            "cases": {"a": {"vector": [1.0, 0.0]}},
            "decision": {
                "choice": "a",
                "similarities": {"a": 0.8, "b": 0.2},
                "margin": 0.6,
            },
            "retrieval": {
                "rankings": {"q": ["a", "b"]},
                "scores": {"q": {"a": 0.8, "b": 0.2}},
            },
        }
        actual = copy.deepcopy(expected)
        actual["decision"] = {"answer": actual["decision"]}
        self.assertEqual(
            compare.compare_reference(actual, expected)["max_retrieval_score_error"], 0
        )
        for field, value in (
            ("suite_sha256", "other"),
            ("checkpoint_receipt_sha256", "other"),
            ("precision", "float16"),
        ):
            bad = copy.deepcopy(actual)
            bad[field] = value
            with self.assertRaises(ValueError):
                compare.compare_reference(bad, expected)
        bads = [copy.deepcopy(actual) for _ in range(4)]
        bads[0]["cases"]["a"]["vector"] = [-1.0, 0.0]
        bads[1]["decision"]["answer"]["choice"] = "b"
        bads[2]["decision"]["answer"]["similarities"]["a"] = 0.9
        bads[3]["retrieval"]["rankings"]["q"] = ["b", "a"]
        for bad in bads:
            with self.assertRaises(ValueError):
                compare.compare_reference(bad, expected)


class CudaRouteTests(unittest.TestCase):
    def test_qualification_requires_executed_routes(self):
        routes = compare.cuda_routes(
            "extended_flash_launches_total=3 extended_flash_launches_total=9 embedding_graph_replays_total=4 native_vision_launches_total=2 media_batches_total=1 fused_backbone_launches_total=96 packed_ple_groups_total=2 packed_head_launches_total=24 packed_gelu_launches_total=24 tuned_gemm_calls_total=100 vision_residual_launches_total=32 vision_graph_replays_total=2 spatial_pool_launches_total=4 short_cudnn_attention_launches_total=2 audio_constant_hits_total=12 patch_rows_written_total=3600"
        )
        self.assertEqual(routes["extended_flash"], 9)
        with self.assertRaisesRegex(ValueError, "tuned_gemm"):
            compare.require_cuda_routes(
                compare.cuda_routes(
                    "gemm_tuning_declines_total=9 cublaslt_matmuls_total=9"
                ),
                ["tuned_gemm"],
            )
        compare.require_cuda_routes(
            routes,
            [
                "extended_flash",
                "graph_replay",
                "native_vision",
                "media_batch",
                "fused_backbone",
                "packed_ple",
                "packed_heads",
                "packed_gelu",
                "tuned_gemm",
                "vision_residual",
                "vision_graph",
                "spatial_pool",
                "short_attention",
                "audio_constants",
                "patch_into",
            ],
        )
        with self.assertRaisesRegex(ValueError, "fused_vision"):
            compare.require_cuda_routes(routes, ["fused_vision"])
        self.assertEqual(
            compare.cuda_routes("extended_flash_launches_total=9garbage")[
                "extended_flash"
            ],
            0,
        )


class PrecisionTests(unittest.TestCase):
    def test_bf16_reference_requires_declared_pooling(self):
        result = {
            "suite_sha256": "suite",
            "checkpoint_receipt_sha256": "weights",
            "precision": "bfloat16",
            "status": "pass",
            "cases": {},
            "measurement_mode": "encoder",
            "decision": {"status": "not_run"},
        }
        with self.assertRaisesRegex(ValueError, "F32 pooling"):
            compare.compare_reference(result, result)
        result["pooling_precision"] = "float32"
        self.assertEqual(
            compare.compare_reference(result, result)["decision_status"], "not_run"
        )

    def test_backend_precision_is_explicit_and_cannot_cross_recipes(self):
        for backend in ("cuda", "torch-cuda"):
            self.assertEqual(compare.execution_precision(backend), "bfloat16")
            with self.assertRaises(ValueError):
                compare.execution_precision(backend, "float32")
        for backend in ("native", "metal", "cpu", "mps"):
            self.assertEqual(compare.execution_precision(backend), "float32")
            with self.assertRaises(ValueError):
                compare.execution_precision(backend, "bfloat16")



class PlatformObservationContracts(unittest.TestCase):
    def test_linux_swap_counters_have_two_real_fields(self):
        with mock.patch.object(compare.sys, "platform", "linux"), mock.patch.object(
            compare.Path, "read_text", return_value="pswpin 7\npswpout 11\npgpgout 99\n"
        ), mock.patch.object(compare.subprocess, "check_output") as commands:
            self.assertEqual(compare.vm_counter_order(), ["pswpin", "pswpout"])
            self.assertEqual(compare.vm_counters(), [7, 11])
            commands.assert_not_called()
        with mock.patch.object(compare.sys, "platform", "linux"), mock.patch.object(
            compare.Path, "read_text", return_value="pswpin 7\n"
        ), self.assertRaises(ValueError):
            compare.vm_counters()

    def test_darwin_counter_order_and_commands_are_preserved(self):
        with mock.patch.object(compare.sys, "platform", "darwin"), mock.patch.object(
            compare.subprocess, "check_output", return_value="Pageouts: 2.\nSwapins: 3.\nSwapouts: 4.\n"
        ) as command:
            self.assertEqual(compare.vm_counter_order(), ["pageouts", "swapins", "swapouts"])
            self.assertEqual(compare.vm_counters(), [2, 3, 4])
            self.assertEqual(command.call_args.args[0], ["vm_stat"])

    def test_linux_conditions_observe_host_swap_affinity_and_nested_quota(self):
        files = {
            "/proc/vmstat": "pswpin 7\npswpout 11\n",
            "/proc/meminfo": "MemTotal: 1000 kB\nSwapTotal: 200 kB\nSwapFree: 150 kB\n",
            "/proc/cpuinfo": "processor : 0\nmodel name : fixture CPU\n",
            "/proc/sys/kernel/random/boot_id": "fixture-boot",
            "/proc/self/cgroup": "0::/fixture\n",
            "/sys/fs/cgroup/cpu.max": "max 100000",
            "/sys/fs/cgroup/fixture/cpu.max": "200000 100000",
        }
        with mock.patch.object(compare.sys, "platform", "linux"), mock.patch.object(
            compare.Path, "read_text", autospec=True, side_effect=lambda path: files[str(path)]
        ), mock.patch.object(compare.Path, "is_file", autospec=True, side_effect=lambda path: str(path) in files), mock.patch.object(
            compare.os, "sched_getaffinity", create=True, return_value={1, 3}
        ), mock.patch.object(compare.subprocess, "check_output") as commands:
            result = compare.conditions("native")
        commands.assert_not_called()
        self.assertEqual(result["swap"], {"total_bytes": 200 * 1024, "free_bytes": 150 * 1024})
        self.assertEqual(result["host_identity"]["boot_id"], "fixture-boot")
        self.assertEqual(result["thread_budget"]["affinity"], [1, 3])
        self.assertEqual(len(result["thread_budget"]["cgroup_cpu_max"]), 2)
        self.assertEqual(result["device_identity"], {"status": "observed", "kind": "cpu"})

    def test_cuda_requires_observed_sole_device_driver_and_visibility(self):
        row = "GPU-fixture, Example GPU, 580.1, 0000:01:00.0, 1000\n"
        with mock.patch.dict(compare.os.environ, {}, clear=True), mock.patch.object(
            compare.subprocess, "check_output", return_value=row
        ) as command:
            result = compare.cuda_identity()
            self.assertEqual(result["status"], "observed")
            self.assertEqual(result["driver_version"], "580.1")
            self.assertEqual(command.call_args.kwargs["timeout"], 10)
        for missing in ("N/A", "[N/A]"):
            with mock.patch.dict(compare.os.environ, {}, clear=True), mock.patch.object(
                compare.subprocess, "check_output", return_value=row.replace("1000", missing)
            ):
                observed = compare.cuda_identity()
                self.assertEqual(observed["status"], "observed")
                self.assertIsNone(observed["memory_mib"])
                self.assertEqual(observed["memory_capacity_status"], "unavailable")
                self.assertIn(missing, observed["query_output"])
        for output in ("", row + row, "GPU-fixture, name, , pci, N/A\n", row.replace("1000", "unknown"), row.replace("1000", "0")):
            with mock.patch.object(compare.subprocess, "check_output", return_value=output):
                self.assertEqual(compare.cuda_identity()["status"], "unavailable")
        with mock.patch.object(compare.subprocess, "check_output", side_effect=FileNotFoundError()):
            self.assertEqual(compare.cuda_identity()["status"], "unavailable")
        with mock.patch.dict(compare.os.environ, {"CUDA_VISIBLE_DEVICES": "1"}), mock.patch.object(
            compare.subprocess, "check_output", return_value=row
        ):
            self.assertEqual(compare.cuda_identity()["status"], "unavailable")

    def test_metal_requires_actual_single_gpu_inventory(self):
        with mock.patch.object(compare.subprocess, "check_output", return_value='{"SPDisplaysDataType":[{"sppci_model":"Apple fixture","spdisplays_metal":"supported"}]}'):
            self.assertEqual(compare.metal_identity()["status"], "observed")
        with mock.patch.object(compare.subprocess, "check_output", return_value='{"SPDisplaysDataType":[]}'):
            self.assertEqual(compare.metal_identity()["status"], "unavailable")

if __name__ == "__main__":
    unittest.main()
