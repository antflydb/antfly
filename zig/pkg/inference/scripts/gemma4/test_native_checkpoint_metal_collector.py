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
import signal
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

import qualify_native_checkpoint_http as qualification


class Process:
    def __init__(self, pid):
        self.pid = pid

    def poll(self):
        return None


class OwnedProcess:
    def __init__(self, pid=51001, status=None):
        self.pid = pid
        self.status = status
        self.terminate_calls = 0

    def poll(self):
        return self.status

    def terminate(self):
        self.terminate_calls += 1


class Response:
    def __init__(self, backend="metal", pid=41):
        self.headers = [
            ("X-Antfly-Inference-Backend", backend),
            ("X-Antfly-Inference-Pid", str(pid)),
        ]

    def getheaders(self):
        return self.headers

    def __enter__(self):
        self.status = 200
        return self

    def __exit__(self, *args):
        pass


class MetalCollectorTests(unittest.TestCase):
    def args(self, **changes):
        values = {
            "backend": "metal",
            "server_kind": "metal-test",
            "encoding": "f16",
            "experimental_rotated4": False,
            "qualification_minimum_savings": None,
        }
        values.update(changes)
        return SimpleNamespace(**values)

    def test_backend_contract_is_explicit_and_rejects_unsupported_codecs(self):
        qualification.validate_backend_configuration(self.args())
        qualification.validate_backend_configuration(self.args(encoding="fp8"))
        for args in (
            self.args(backend="cuda"),
            self.args(server_kind="standalone"),
            self.args(encoding="f32"),
            self.args(encoding="rotated4", experimental_rotated4=True),
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                qualification.validate_backend_configuration(args)
        # Existing CUDA modes remain valid for the unchanged default path.
        qualification.validate_backend_configuration(
            self.args(backend="cuda", server_kind="standalone", encoding="rotated4")
        )

    def test_test_only_savings_header_is_metal_scoped_and_separate(self):
        args = self.args(qualification_minimum_savings=4.0)
        qualification.validate_backend_configuration(args)
        headers = qualification.prepare_headers(args)
        self.assertEqual(
            headers[qualification.METAL_TEST_MINIMUM_SAVINGS_HEADER], "4.0"
        )
        self.assertEqual(headers["X-Antfly-KV-Encoding"], "f16")
        for args in (
            self.args(qualification_minimum_savings=1),
            self.args(qualification_minimum_savings=float("inf")),
            self.args(qualification_minimum_savings=4.01),
            self.args(
                backend="cuda",
                server_kind="standalone",
                qualification_minimum_savings=2,
            ),
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                qualification.validate_backend_configuration(args)
        cuda = self.args(backend="cuda", server_kind="standalone")
        self.assertNotIn(
            qualification.METAL_TEST_MINIMUM_SAVINGS_HEADER,
            qualification.prepare_headers(cuda),
        )

    def test_metal_request_normalization_is_eager_and_fail_closed(self):
        source = {"model": qualification.METAL_TEST_MODEL, "messages": []}
        normalized = qualification.normalize_request_body(self.args(), source)
        self.assertEqual(normalized["mode"], "eager")
        self.assertNotIn("mode", source)
        explicit = {**source, "mode": "eager"}
        self.assertIs(
            qualification.normalize_request_body(self.args(), explicit), explicit
        )
        for mode in ("compiled", "whole-model"):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                qualification.normalize_request_body(
                    self.args(), {**source, "mode": mode}
                )
        cuda = self.args(backend="cuda", server_kind="standalone")
        compiled = {**source, "mode": "compiled"}
        self.assertIs(qualification.normalize_request_body(cuda, compiled), compiled)

    def test_prepare_body_disables_stream_without_mutating_measurement_body(self):
        source = {
            "model": qualification.METAL_TEST_MODEL,
            "messages": [{"role": "user", "content": "capacity fixture"}],
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        prepared = qualification.prepare_request_body(self.args(), source)
        self.assertIsNot(prepared, source)
        self.assertTrue(source["stream"])
        self.assertNotIn("mode", source)
        self.assertFalse(prepared["stream"])
        self.assertEqual(prepared["mode"], "eager")
        self.assertEqual(prepared["stream_options"], {"include_usage": True})

    def test_owner_ledger_wrapper_requires_attributed_prepare_source(self):
        committed = {
            "request": 7,
            "route": "checkpoint_prepare",
            "owner_id": 9,
            "binding": False,
            "binding_representation": "none",
            "binding_allocated_bytes": 0,
            "binding_fp8_heads": 0,
            "binding_fp8_rows": 0,
            "binding_exact_resident_bytes": 0,
            "binding_encoded_resident_bytes": 0,
            "binding_metadata_resident_bytes": 0,
        }
        source = {
            "schema": "antfly.metal-checkpoint-phase.v1",
            "phase": "checkpoint_source",
            **committed,
            "binding": False,
            "binding_representation": "none",
        }
        proof = {"committed": committed, "release": {"released": True}}
        record = {"log_path": "/unused", "owner_ledgers": []}
        with (
            mock.patch(
                "metal_checkpoint_qualification_ledger.wait_ledger",
                return_value=[source],
            ),
            mock.patch(
                "metal_checkpoint_qualification_ledger.generation_contract",
                return_value=proof,
            ),
        ):
            observed = qualification.metal_ledger_finish(
                self.args(),
                record,
                0,
                "checkpoint_prepare",
                require_binding=False,
                required_phase="checkpoint_source",
            )
            self.assertIs(observed, proof)
            self.assertEqual(record["owner_ledgers"], [proof])
            bad = dict(source, owner_id=10)
            with mock.patch(
                "metal_checkpoint_qualification_ledger.wait_ledger",
                return_value=[bad],
            ), self.assertRaises(AssertionError):
                qualification.metal_ledger_finish(
                    self.args(),
                    record,
                    0,
                    "checkpoint_prepare",
                    require_binding=False,
                    required_phase="checkpoint_source",
                )

    def test_ready_attestation_binds_process_backend_build_and_identity(self):
        with mock.patch.object(qualification.urllib.request, "urlopen", return_value=Response()):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ready = root / "ready.json"
                log = root / "server.log"
                log.write_text("")
                pid = 41
                model = "google/gemma-4-E4B-it-qat-q4_0-gguf"
                ready.write_text(json.dumps({
                    "schema": qualification.METAL_TEST_SERVER_SCHEMA,
                    "backend": "metal",
                    "metal_enabled": True,
                    "cuda_enabled": False,
                    "pid": pid,
                    "port": 8500,
                    "model": model,
                    "load_generation": 7,
                    "loaded_identity": {
                        name: "a" * 64
                        for name in qualification.PORTABLE_IDENTITY_FIELDS
                    },
                }))
                args = SimpleNamespace(timeout=2, port=8500, model=model)
                observed = qualification._wait_metal_test_server(Process(pid), ready, log, args)
                self.assertEqual(observed["load_generation"], 7)
                changed = json.loads(ready.read_text())
                changed["cuda_enabled"] = True
                ready.write_text(json.dumps(changed))
                with self.assertRaises(RuntimeError):
                    qualification._wait_metal_test_server(Process(pid), ready, log, args)

    def test_headers_require_exact_metal_worker(self):
        args = self.args()
        record = {"worker_pid": 41}
        qualification.response_headers(Response(), record, args)
        for response in (Response("cuda"), Response(pid=42)):
            with self.assertRaises(AssertionError):
                qualification.response_headers(response, record, args)

    def test_loader_headroom_requires_two_fresh_samples_and_recovers(self):
        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

            def wall_time(self):
                return 1_000.0 + self.now

            def sleep(self, seconds):
                self.asserted_sleeps.append(seconds)
                self.now += seconds

            asserted_sleeps = []

        clock = Clock()
        receipt = {}
        free = iter((75, 76, 76))
        swap = iter((400, 400, 400, 400))
        observed = qualification._wait_metal_loader_headroom(
            30,
            receipt,
            memory_probe=lambda: next(free),
            swap_probe=lambda: next(swap),
            monotonic=clock.monotonic,
            wall_time=clock.wall_time,
            sleeper=clock.sleep,
        )
        self.assertIs(observed, receipt)
        self.assertTrue(receipt["passed"])
        self.assertEqual(clock.asserted_sleeps, [10, 10])
        self.assertEqual(
            [sample["free_memory_percent"] for sample in receipt["samples"]],
            [75, 76, 76],
        )
        self.assertEqual(
            [sample["consecutive_passing_samples"] for sample in receipt["samples"]],
            [0, 1, 2],
        )
        self.assertEqual(receipt["elapsed_seconds"], 20)
        self.assertEqual(receipt["swapout_growth_bytes"], 0)

    def test_loader_headroom_timeout_and_swap_growth_leave_failure_receipts(self):
        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

            def sleep(self, seconds):
                self.now += seconds

        for name, memory_probe, swap_values, error in (
            ("timeout", lambda: 75, (10, 10, 10), TimeoutError),
            ("swap", lambda: 76, (10, 11), RuntimeError),
        ):
            clock = Clock()
            receipt = {}
            swap = iter(swap_values)
            with self.subTest(name=name), self.assertRaises(error):
                qualification._wait_metal_loader_headroom(
                    10,
                    receipt,
                    memory_probe=memory_probe,
                    swap_probe=lambda: next(swap),
                    monotonic=clock.monotonic,
                    wall_time=lambda: 1_000 + clock.now,
                    sleeper=clock.sleep,
                )
            self.assertFalse(receipt["passed"])
            self.assertIn(error.__name__, receipt["error"])
            self.assertGreaterEqual(len(receipt["samples"]), 1)

    def test_importer_loader_headroom_failure_prevents_process_start(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(
                server_kind="metal-test",
                inference_admission_capacity=1,
                models_dir=root,
                port=8500,
                output=root / "receipt.json",
                server=root / "context-tests",
                timeout=20,
            )
            evidence = {}

            def fail_gate(timeout, receipt):
                self.assertEqual(timeout, 20)
                receipt.update(
                    passed=False,
                    samples=[{"free_memory_percent": 75}],
                    error="TimeoutError: loader capacity remained below 76 percent",
                )
                raise TimeoutError("loader capacity remained below 76 percent")

            with (
                mock.patch.object(
                    qualification,
                    "_wait_metal_loader_headroom",
                    side_effect=fail_gate,
                ),
                mock.patch.object(qualification.subprocess, "Popen") as popen,
            ):
                with self.assertRaisesRegex(TimeoutError, "below 76 percent"):
                    with qualification.server(args, "importer", evidence):
                        self.fail("headroom failure cannot start importer")
            popen.assert_not_called()
            self.assertFalse(evidence["importer"]["loader_headroom"]["passed"])
            self.assertIn(
                "below 76 percent",
                evidence["importer"]["loader_headroom"]["error"],
            )

    def test_startup_failure_is_not_masked_by_failed_graceful_stop(self):
        class ExitedProcess:
            pid = 51001
            returncode = 1

            @staticmethod
            def poll():
                return 1

        class Sampler:
            fatal_error = None

            def __init__(self, *args, **kwargs):
                pass

            def start(self):
                pass

            def finish(self):
                return {"samples": 0, "errors": []}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(
                server_kind="metal-test",
                inference_admission_capacity=1,
                models_dir=root,
                port=8500,
                output=root / "receipt.json",
                server=root / "context-tests",
                timeout=1,
            )
            evidence = {}
            with (
                mock.patch.object(qualification.subprocess, "Popen", return_value=ExitedProcess()),
                mock.patch.object(
                    qualification,
                    "_metal_server_topology",
                    return_value={
                        "qualifier_pid": 50000,
                        "pgid": None,
                        "shared_pgid": False,
                        "start_new_session": True,
                    },
                ),
                mock.patch.object(qualification.os, "getpgid", return_value=51001),
                mock.patch.object(qualification, "MetalProcessSampler", Sampler),
                mock.patch.object(
                    qualification,
                    "_wait_metal_loader_headroom",
                    side_effect=lambda timeout, receipt: receipt.update(passed=True),
                ),
                mock.patch.object(
                    qualification,
                    "_wait_metal_test_server",
                    side_effect=RuntimeError("primary startup admission failed"),
                ),
                mock.patch.object(
                    qualification,
                    "_remaining_metal_server_group_members",
                    return_value=[],
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "primary startup admission failed"):
                    with qualification.server(args, "worker", evidence):
                        self.fail("exited server cannot yield")
            record = evidence["worker"]
            self.assertEqual(record["startup_returncode"], 1)
            self.assertEqual(record["returncode"], 1)
            self.assertFalse(record["graceful_stop"])
            self.assertTrue(record["stopped"])
            self.assertEqual(
                record["cleanup_errors"],
                ["Metal qualification server did not drain cleanly"],
            )
            self.assertTrue(record["loader_headroom"]["passed"])

    def test_unified_memory_receipt_has_no_cuda_claims_and_is_fail_closed(self):
        good = {
            "scope": "unified_process_group",
            "max_combined_rss_bytes": qualification.METAL_COMBINED_RSS_LIMIT_BYTES,
            "samples": 2,
            "errors": [],
            "peak_group_rss_bytes": 1024,
            "minimum_free_memory_percent": 64,
            "minimum_disk_free_mib": 8192,
            "swapout_growth_bytes": 0,
        }
        qualification.validate_metal_memory(good)
        for field, value in (
            ("samples", 1),
            ("peak_group_rss_bytes", 9 * 1024**3),
            ("minimum_free_memory_percent", 14),
            ("minimum_disk_free_mib", 4095),
            ("swapout_growth_bytes", 1),
        ):
            changed = dict(good)
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                qualification.validate_metal_memory(changed)

    def test_live_resource_guard_terminates_owned_group_on_every_violation(self):
        cases = {
            "rss": {"rss_mib": 8192, "free": 64, "disk_mib": 8192, "swap": [0, 0]},
            "free": {"rss_mib": 1, "free": 14, "disk_mib": 8192, "swap": [0, 0]},
            "disk": {"rss_mib": 1, "free": 64, "disk_mib": 4095, "swap": [0, 0]},
            "swap": {"rss_mib": 1, "free": 64, "disk_mib": 8192, "swap": [0, 1]},
        }
        for name, values in cases.items():
            process = OwnedProcess()
            with (
                self.subTest(name=name),
                mock.patch.object(
                    qualification, "swapout_bytes", side_effect=values["swap"]
                ),
                mock.patch.object(
                    qualification, "group_rss_mib", return_value=values["rss_mib"]
                ),
                mock.patch.object(
                    qualification.subprocess, "check_output", return_value="1024\n"
                ),
                mock.patch.object(
                    qualification, "memory_free_percent", return_value=values["free"]
                ),
                mock.patch.object(
                    qualification.shutil,
                    "disk_usage",
                    return_value=SimpleNamespace(free=values["disk_mib"] * 1048576),
                ),
                mock.patch.object(qualification.os, "getpgid", return_value=process.pid),
                mock.patch.object(qualification.os, "killpg") as killpg,
            ):
                sampler = qualification.MetalProcessSampler(
                    process,
                    Path("/guard-test"),
                    pgid=process.pid,
                    shared_pgid=False,
                    interval_s=60,
                )
                with self.assertRaisesRegex(RuntimeError, "resource guard failed"):
                    sampler.start()
                receipt = sampler.finish()
            killpg.assert_called_once_with(process.pid, signal.SIGTERM)
            self.assertTrue(receipt["termination_signaled"])
            self.assertIsNotNone(receipt["fatal_error"])
            self.assertEqual(receipt["samples"], 1)
            self.assertEqual(len(receipt["observations"]), 1)

    def test_live_resource_guard_probe_failure_is_fatal_and_preserved(self):
        process = OwnedProcess()
        with (
            mock.patch.object(qualification, "swapout_bytes", return_value=0),
            mock.patch.object(
                qualification, "group_rss_mib", side_effect=OSError("probe broke")
            ),
            mock.patch.object(qualification.os, "getpgid", return_value=process.pid),
            mock.patch.object(qualification.os, "killpg") as killpg,
        ):
            sampler = qualification.MetalProcessSampler(
                process,
                Path("/guard-test"),
                pgid=process.pid,
                shared_pgid=False,
            )
            with self.assertRaisesRegex(RuntimeError, "resource probe failed"):
                sampler.start()
            receipt = sampler.finish()
        killpg.assert_called_once_with(process.pid, signal.SIGTERM)
        self.assertIn("OSError: probe broke", receipt["errors"])
        self.assertIn("probe failed", receipt["fatal_error"])
        self.assertEqual(receipt["samples"], 0)

    def test_live_resource_guard_healthy_samples_do_not_signal(self):
        process = OwnedProcess()
        with (
            mock.patch.object(qualification, "swapout_bytes", return_value=0),
            mock.patch.object(qualification, "group_rss_mib", return_value=1),
            mock.patch.object(
                qualification.subprocess, "check_output", return_value="1024\n"
            ),
            mock.patch.object(qualification, "memory_free_percent", return_value=64),
            mock.patch.object(
                qualification.shutil,
                "disk_usage",
                return_value=SimpleNamespace(free=8192 * 1048576),
            ),
            mock.patch.object(qualification.os, "getpgid", return_value=process.pid),
            mock.patch.object(qualification.os, "killpg") as killpg,
        ):
            sampler = qualification.MetalProcessSampler(
                process,
                Path("/guard-test"),
                pgid=process.pid,
                shared_pgid=False,
                interval_s=60,
            )
            sampler.start()
            receipt = sampler.finish()
        killpg.assert_not_called()
        self.assertIsNone(receipt["fatal_error"])
        self.assertFalse(receipt["termination_signaled"])
        qualification.validate_metal_memory(receipt)

    def test_live_resource_guard_never_signals_exited_or_mismatched_group(self):
        for name, process, actual_pgid in (
            ("exited", OwnedProcess(status=0), 51001),
            ("mismatched", OwnedProcess(), 61001),
        ):
            with (
                self.subTest(name=name),
                mock.patch.object(qualification, "swapout_bytes", side_effect=[0, 0]),
                mock.patch.object(qualification, "group_rss_mib", return_value=8192),
                mock.patch.object(
                    qualification.subprocess, "check_output", return_value="1024\n"
                ),
                mock.patch.object(qualification, "memory_free_percent", return_value=64),
                mock.patch.object(
                    qualification.shutil,
                    "disk_usage",
                    return_value=SimpleNamespace(free=8192 * 1048576),
                ),
                mock.patch.object(qualification.os, "getpgid", return_value=actual_pgid),
                mock.patch.object(qualification.os, "killpg") as killpg,
            ):
                sampler = qualification.MetalProcessSampler(
                    process,
                    Path("/guard-test"),
                    pgid=process.pid,
                    shared_pgid=False,
                )
                with self.assertRaises(RuntimeError):
                    sampler.start()
                receipt = sampler.finish()
            killpg.assert_not_called()
            self.assertIsNotNone(receipt["fatal_error"])
            self.assertFalse(receipt["termination_signaled"])

    def test_outer_guard_group_is_inherited_and_not_double_counted(self):
        with (
            mock.patch.object(qualification.os, "getpid", return_value=700),
            mock.patch.object(qualification.os, "getpgrp", return_value=700),
        ):
            topology = qualification._metal_server_topology()
        self.assertEqual(
            topology,
            {
                "qualifier_pid": 700,
                "pgid": 700,
                "shared_pgid": True,
                "start_new_session": False,
            },
        )
        process = OwnedProcess(pid=701)
        with (
            mock.patch.object(qualification, "swapout_bytes", side_effect=[0, 0]),
            mock.patch.object(qualification, "group_rss_mib", return_value=8193),
            mock.patch.object(
                qualification.subprocess,
                "check_output",
                side_effect=AssertionError("collector RSS must not be added twice"),
            ),
            mock.patch.object(qualification, "memory_free_percent", return_value=64),
            mock.patch.object(
                qualification.shutil,
                "disk_usage",
                return_value=SimpleNamespace(free=8192 * 1048576),
            ),
            mock.patch.object(qualification.os, "getpgid", return_value=700),
            mock.patch.object(qualification.os, "killpg") as killpg,
        ):
            sampler = qualification.MetalProcessSampler(
                process,
                Path("/guard-test"),
                pgid=700,
                shared_pgid=True,
            )
            with self.assertRaisesRegex(RuntimeError, "resource guard failed"):
                sampler.start()
            receipt = sampler.finish()
        killpg.assert_not_called()
        self.assertEqual(process.terminate_calls, 1)
        self.assertEqual(receipt["observations"][0]["collector_rss_bytes"], 0)
        self.assertTrue(receipt["shared_qualifier_pgid"])

    def test_inherited_group_drain_excludes_only_live_qualifier(self):
        with (
            mock.patch.object(qualification.os, "getpid", return_value=700),
            mock.patch.object(
                qualification,
                "_portable_active_group_members",
                side_effect=([700, 701], [700]),
            ),
        ):
            self.assertEqual(
                qualification._remaining_metal_server_group_members(700, True),
                [701],
            )
            self.assertEqual(
                qualification._remaining_metal_server_group_members(700, True), []
            )

    def test_process_group_probe_excludes_only_its_known_pid(self):
        class Probe:
            pid = 702
            returncode = 0
            args = ["/bin/ps"]

            def communicate(self, timeout=None):
                self.timeout = timeout
                return (
                    "700 700 Ss\n701 700 S\n702 700 R\n703 999 S\n704 700 Z\n",
                    "",
                )

        probe = Probe()
        with mock.patch.object(
            qualification.subprocess, "Popen", return_value=probe
        ):
            self.assertEqual(
                qualification._portable_active_group_members(700), [700, 701]
            )
        self.assertEqual(probe.timeout, 10)

    def test_manual_cli_keeps_dedicated_server_group(self):
        with (
            mock.patch.object(qualification.os, "getpid", return_value=701),
            mock.patch.object(qualification.os, "getpgrp", return_value=700),
        ):
            topology = qualification._metal_server_topology()
        self.assertFalse(topology["shared_pgid"])
        self.assertTrue(topology["start_new_session"])
        self.assertIsNone(topology["pgid"])


if __name__ == "__main__":
    unittest.main()
