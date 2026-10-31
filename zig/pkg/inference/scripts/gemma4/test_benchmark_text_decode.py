# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Prevent shared GPU activity from being reported as isolated performance."""
import unittest
import argparse
import contextlib
import io
import json
from pathlib import Path
import signal
import tempfile
from unittest.mock import Mock, patch

import benchmark_text_decode as benchmark
from benchmark_text_decode import audit_gpu_activity, request_options, server_command, stop_owned_process


class GpuAuditTests(unittest.TestCase):
    samples = [{"started_at_unix": 1793295796.2, "finished_at_unix": 1793295798.8}]

    def test_own_supervisor_worker_and_graphics_do_not_count_as_competitors(self):
        log = """
#Date Time gpu pid type sm mem enc dec jpg ofa fb ccpm command
20261029 17:43:16 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:17 0 101 C 90 10 - - - - 5000 0 server
20261029 17:43:17 0 200 G 15 10 - - - - 100 0 Xorg
"""
        report = audit_gpu_activity(log, {100, 101}, self.samples)
        self.assertTrue(report["quiet"])
        self.assertEqual(report["observed_seconds"], 2)

    def test_other_compute_activity_rejects_run(self):
        log = """
20261029 17:43:16 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:17 0 200 C 3 0 - - - - 100 0 competitor
"""
        report = audit_gpu_activity(log, {100}, self.samples)
        self.assertFalse(report["quiet"])
        self.assertEqual(report["competing_activity"][0]["pid"], 200)

    def test_unknown_compute_activity_and_missing_telemetry_reject_run(self):
        log = """
20261029 17:43:16 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:17 0 200 C - 0 - - - - 100 0 competitor
"""
        self.assertFalse(audit_gpu_activity(log, {100}, self.samples)["quiet"])
        self.assertFalse(audit_gpu_activity("Not supported", {100}, self.samples)["quiet"])

    def test_idle_service_is_allowed_and_activity_outside_window_is_excluded(self):
        log = """
20261029 17:43:10 0 200 C 99 10 - - - - 5000 0 competitor
20261029 17:43:16 0 200 C 0 0 - - - - 5000 0 competitor
20261029 17:43:17 0 100 C 99 10 - - - - 5000 0 worker
20261029 17:43:25 0 200 C 99 10 - - - - 5000 0 competitor
"""
        self.assertTrue(audit_gpu_activity(log, {100}, self.samples)["quiet"])


class EngineAdapterTests(unittest.TestCase):
    def test_reference_manifest_embeds_metadata_and_rejects_changed_runtime_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary) / "loader.py"
            artifact.write_text("qualified implementation")
            receipt = Path(temporary) / "reference.json"
            receipt.write_text(json.dumps({"version": "qualified", "verified_files": {str(artifact): benchmark.q.file_sha(artifact)}}))
            captured = benchmark.reference_manifest(receipt)
            self.assertEqual(captured["metadata"]["version"], "qualified")
            self.assertEqual(captured["sha256"], benchmark.q.file_sha(receipt))
            artifact.write_text("changed implementation")
            with self.assertRaisesRegex(ValueError, "artifact changed"):
                benchmark.reference_manifest(receipt)

    def args(self, **overrides):
        values = dict(engine="native", binary=Path("server"), models_dir=Path("models"), model="google/gemma",
                      process_memory_budget_mb=None, model_file=None, request_json='{"cache_dtype":"f16"}',
                      threads=None, vllm_model=None, context=8192, gpu_memory_utilization=0.15,
                      tokenizer=None, server_arg=[], base_url=None, token_preflight=False)
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_native_resource_envelope_and_extra_arguments_are_exact_argv(self):
        command = server_command(self.args(process_memory_budget_mb=24576, server_arg=["--keep-alive", "5"]), 1234)
        self.assertIn("--process-memory-budget-mb", command)
        self.assertEqual(command[-4:], ["--process-memory-budget-mb", "24576", "--keep-alive", "5"])
        self.assertIn("generator:cuda:google/gemma", command)

    def test_native_binding_follows_exact_root_precedence_and_checks_selected_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "generators/google/gemma"
            legacy.mkdir(parents=True)
            selected = legacy / "model.gguf"
            selected.write_bytes(b"same weights")
            args = self.args(models_dir=root, model_file=selected)
            result = benchmark.native_checkpoint_binding(args)
            self.assertTrue(result["verified"])
            self.assertEqual(result["checkpoint"], str(selected))
            direct = root / "google/gemma"
            direct.mkdir(parents=True)
            actual = direct / "model.gguf"
            actual.write_bytes(b"different weights")
            with self.assertRaisesRegex(ValueError, "differs from"):
                benchmark.native_checkpoint_binding(self.args(models_dir=root, model_file=selected))
            result = benchmark.native_checkpoint_binding(self.args(models_dir=root, model_file=actual))
            self.assertEqual(result["checkpoint"], str(actual))

    def test_native_binding_accepts_hardlinks_and_rejects_unverifiable_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model_dir = root / "generators/google/gemma"
            model_dir.mkdir(parents=True)
            actual = model_dir / "model.gguf"
            actual.write_bytes(b"same weights")
            alias = root / "comparison.gguf"
            alias.hardlink_to(actual)
            args = self.args(models_dir=root, model_file=alias)
            benchmark.native_checkpoint_binding(args)
            self.assertEqual(args.model_file, actual)
            for model in ("gemma4-e4b", "google/gemma:q4", "../gemma", "/google/gemma"):
                with self.subTest(model=model), self.assertRaisesRegex(ValueError, "exact owner/name"):
                    benchmark.native_checkpoint_binding(self.args(models_dir=root, model=model))
            for arguments in (["--models-dir", "elsewhere"], ["--config=other.json"], ["--preload-model", "other"]):
                with self.subTest(arguments=arguments), self.assertRaisesRegex(ValueError, "model-affecting"):
                    benchmark.native_checkpoint_binding(self.args(models_dir=root, server_arg=arguments))
            for options in ({"model_file": actual}, {"token_preflight": True}):
                with self.subTest(options=options), self.assertRaisesRegex(ValueError, "external native"):
                    benchmark.native_checkpoint_binding(self.args(base_url="http://localhost", **options))

    def test_native_model_mismatch_rejects_before_starting_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model_dir = root / "models/generators/google/gemma"
            model_dir.mkdir(parents=True)
            (model_dir / "model.gguf").write_bytes(b"actual weights")
            wrong = root / "wrong.gguf"
            wrong.write_bytes(b"other weights")
            output = root / "results"
            argv = ["benchmark", "--binary", str(root / "server"), "--models-dir", str(root / "models"),
                    "--model", "google/gemma", "--model-file", str(wrong), "--output", str(output)]
            with patch("sys.argv", argv), patch.object(benchmark.q, "gpu_snapshot") as snapshot, \
                 patch.object(benchmark.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(ValueError, "differs from"):
                    benchmark.main()
            snapshot.assert_not_called()
            launch.assert_not_called()
            self.assertFalse(output.exists())

    def test_native_cli_capture_finishes_before_monitor_and_server_start(self):
        sample = {"usage": {"prompt_tokens": 46, "completion_tokens": 4}, "text": "same result",
                  "decode_tokens_per_second": 10.0, "end_to_end_tokens_per_second": 8.0, "ttft_seconds": 0.05, "seconds": 0.5}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model_dir = root / "models/generators/google/gemma"
            model_dir.mkdir(parents=True)
            model = model_dir / "model.gguf"
            model.write_bytes(b"actual weights")
            binary = root / "server"
            binary.write_bytes(b"server")
            output = root / "results"
            argv = ["benchmark", "--binary", str(binary), "--models-dir", str(root / "models"),
                    "--model", "google/gemma", "--model-file", str(model), "--output", str(output),
                    "--tokens", "4", "--warmups", "0", "--repeats", "1", "--monitor-gpu", "--token-preflight",
                    "--native-cli", str(root / "cli"), "--native-cli-backend", "cuda",
                    "--native-cli-source-root", str(root), "--native-server-source-root", str(root)]
            events = []
            def capture(command, **kwargs):
                events.append("capture")
                request = json.loads((output / "report.json").read_text())
                proof = {"passed": True, "logical_tokens": 46, "prompt_identity_sha256": request["prompt_identity_sha256"]}
                (output / "preflight.json").write_text(json.dumps(proof))
                events.append("capture-finished")
                return Mock(returncode=0)
            def start(command, **kwargs):
                events.append("monitor" if command[0] == "nvidia-smi" else "server")
                return Mock(pid=123, poll=Mock(return_value=None if command[0] != "nvidia-smi" else 0))
            with patch("sys.argv", argv), patch.object(benchmark.subprocess, "run", side_effect=capture), \
                 patch.object(benchmark.subprocess, "Popen", side_effect=start), \
                 patch.object(benchmark.q, "sse_generate", return_value=sample), patch.object(benchmark.q, "gpu_snapshot", return_value={}), \
                 patch.object(benchmark.urllib.request, "urlopen"), patch.object(benchmark, "process_tree", return_value={123}), \
                 patch.object(benchmark, "stop_owned_process"), patch.object(benchmark.time, "sleep"), \
                 patch.object(benchmark.socket, "socket") as sockets, contextlib.redirect_stdout(io.StringIO()):
                sockets.return_value.__enter__.return_value.getsockname.return_value = ("127.0.0.1", 1234)
                benchmark.main()
            self.assertEqual(events, ["capture", "capture-finished", "monitor", "server"])
            report = json.loads((output / "report.json").read_text())
            self.assertTrue(report["native_model_binding"]["verified"])
            self.assertEqual(report["model_sha256"], benchmark.q.file_sha(model))

    def test_llama_uses_selected_checkpoint_precision_and_threads(self):
        command = server_command(self.args(engine="llama", threads=2, model_file=Path("selected.gguf")), 1234)
        self.assertEqual(command[command.index("-m") + 1], str(Path("selected.gguf").resolve()))
        self.assertEqual(command[command.index("-t") + 1], "2")
        self.assertEqual(command[command.index("-ctk") + 1], "f16")
        self.assertEqual(command[command.index("-ctv") + 1], "f16")

    def test_vllm_disables_prefix_reuse_and_matches_f16_kv_without_eager_mode(self):
        command = server_command(self.args(engine="vllm", model_file=Path("selected.gguf"), tokenizer="google/tokenizer"), 1234)
        self.assertEqual(command[1:3], ["serve", str(Path("selected.gguf").resolve())])
        self.assertIn("--no-enable-prefix-caching", command)
        self.assertEqual(command[command.index("--max-num-seqs") + 1], "1")
        self.assertEqual(command[command.index("--dtype") + 1], "half")
        self.assertEqual(command[command.index("--kv-cache-dtype") + 1], "auto")
        self.assertNotIn("--enforce-eager", command)

    def test_request_policy_does_not_mutate_supplied_options_or_send_private_cache_flags_to_vllm(self):
        supplied = {"cache_dtype": "f16"}
        self.assertEqual(request_options("llama", supplied), {"cache_prompt": False})
        self.assertEqual(supplied, {"cache_dtype": "f16"})
        self.assertFalse(request_options("native", {})["prompt_cache"])
        self.assertEqual(request_options("vllm", {}), {})
        for key in ("cache_dtype", "prompt_cache", "cache_prompt", "backend"):
            with self.assertRaises(ValueError):
                request_options("vllm", {key: False})

    def test_request_policy_preserves_target_and_cuda_backend_without_dropping_other_options(self):
        supplied = {"model": "google/gemma", "backend": "cuda", "cache_dtype": "f16", "temperature": 0, "speculative_k": 2}
        self.assertEqual(request_options("native", supplied, model="google/gemma"), {**supplied, "prompt_cache": False})
        self.assertNotIn("prompt_cache", supplied)
        for engine in ("native", "llama", "vllm"):
            with self.subTest(engine=engine), self.assertRaisesRegex(ValueError, "selected --model"):
                request_options(engine, {"model": "other"}, model="google/gemma")
        for backend in ("native", "cpu", None, False):
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, "backend=cuda"):
                request_options("native", {"backend": backend}, model="google/gemma")

    def test_request_target_bypass_rejects_before_binding_preflight_or_processes(self):
        for supplied in ({"model": "other"}, {"model": None}, {"backend": "cpu"}, {"backend": None}):
            with self.subTest(supplied=supplied), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "results"
                argv = ["benchmark", "--binary", "unused-server", "--models-dir", "unused-models", "--model", "google/gemma",
                        "--output", str(output), "--request-json", json.dumps(supplied), "--token-preflight",
                        "--model-file", "unused.gguf", "--native-cli", "unused-cli",
                        "--native-cli-source-root", "unused-source", "--native-server-source-root", "unused-source"]
                with patch("sys.argv", argv), patch.object(benchmark, "native_checkpoint_binding") as binding, \
                     patch.object(benchmark, "run_token_preflight") as preflight, \
                     patch.object(benchmark.subprocess, "Popen") as launch, patch.object(benchmark.q, "gpu_snapshot") as snapshot:
                    with self.assertRaisesRegex(ValueError, "override"):
                        benchmark.main()
                binding.assert_not_called()
                preflight.assert_not_called()
                launch.assert_not_called()
                snapshot.assert_not_called()
                self.assertFalse(output.exists())

    @patch("benchmark_text_decode.os.killpg")
    def test_external_service_has_no_owned_process_to_stop(self, kill):
        stop_owned_process(None)
        kill.assert_not_called()

    @patch("benchmark_text_decode.os.killpg")
    def test_owned_supervisor_and_gpu_workers_share_cleanup_group(self, kill):
        process = Mock(pid=123)
        stop_owned_process(process)
        self.assertEqual(kill.call_args_list[0].args, (123, signal.SIGTERM))
        self.assertEqual(kill.call_args_list[-1].args, (123, signal.SIGKILL))
        process.wait.assert_called_once_with(timeout=20)

    def test_external_vllm_needs_no_binary_and_preserves_authoritative_usage(self):
        sample = {"usage": {"prompt_tokens": 46, "completion_tokens": 4}, "text": "same result",
                  "decode_tokens_per_second": 10.0, "end_to_end_tokens_per_second": 8.0, "ttft_seconds": 0.05, "seconds": 0.5}
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "results"
            argv = ["benchmark", "--engine", "vllm", "--base-url", "http://127.0.0.1:8000/",
                    "--output", str(output), "--tokens", "4", "--repeats", "2"]
            with patch("sys.argv", argv), patch.object(benchmark.q, "sse_generate", side_effect=lambda *a, **kw: json.loads(json.dumps(sample))) as generate, \
                 patch.object(benchmark.q, "gpu_snapshot", return_value={}), patch.object(benchmark.urllib.request, "urlopen"), \
                 patch.object(benchmark.subprocess, "Popen") as start, contextlib.redirect_stdout(io.StringIO()):
                benchmark.main()
            report = json.loads((output / "report.json").read_text())
            self.assertTrue(report["external_server"])
            self.assertIsNone(report["command"])
            self.assertIsNone(report["binary_sha256"])
            self.assertEqual(report["summary"]["completion_tokens"], [4, 4])
            self.assertEqual(report["summary"]["prompt_tokens"], 46)
            self.assertEqual(report["summary"]["seconds"], 0.5)
            self.assertEqual(generate.call_count, 3)
            self.assertIsNone(generate.call_args.kwargs["backend"])
            self.assertEqual(generate.call_args.kwargs["route"], "/v1/chat/completions")
            start.assert_not_called()

    def test_reference_request_omits_antfly_backend_and_uses_terminal_usage(self):
        class Response(io.BytesIO):
            headers = {}
        events = b'data: {"choices":[{"delta":{"content":"text"}}]}\n\n'
        events += b'data: {"choices":[],"usage":{"prompt_tokens":46,"completion_tokens":4}}\n\n'
        events += b'data: [DONE]\n\n'
        with patch.object(benchmark.q.urllib.request, "urlopen", return_value=Response(events)) as request, \
             patch.object(benchmark.q.time, "perf_counter", side_effect=[0.0, 1.0, 4.0, 4.1]):
            result = benchmark.q.sse_generate("http://localhost", "model", 4, backend=None)
        self.assertNotIn("backend", json.loads(request.call_args.args[0].data))
        self.assertEqual(result["decode_tokens_per_second"], 1.0)
        self.assertEqual(result["usage"]["completion_tokens"], 4)


if __name__ == "__main__":
    unittest.main()
