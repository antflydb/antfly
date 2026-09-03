#!/usr/bin/env python3

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import a4b_metal_server_acceptance as acceptance


GOOD_TELEMETRY = (
    "metal_a4b_server_request: scope=process_delta model=gemma-a4b "
    "route_select_attempts=120 route_select_successes=120 route_select_fallbacks=0 "
    "slot_lookup_attempts=120 slot_route_hits=880 slot_route_misses=80 "
    "slot_all_hit_layers=70 slot_map_publications=30 slot_map_publish_failures=0 "
    "slot_arena_attempts=30 slot_arena_successes=30 slot_arena_failures=0 "
    "slot_upload_batches=30 slot_uploads=240 "
    "mapped_attempts=8 mapped_fallbacks=0 mapped_failures=0 "
    "linear_attempts=0 linear_successes=0 linear_fallbacks=0 "
    "pair_attempts=0 pair_successes=0 pair_fallbacks=0 "
    "scatter_attempts=0 scatter_successes=0 scatter_fallbacks=0 "
    "frame_begins=667 frame_submits=457 compute_encoders=2207 "
    "blit_encoders=30 bootstrap_misses=0\n"
    "metal_a4b_server_selected_page: scope=process_delta model=gemma-a4b "
    "attempts=0 successes=0 failures=0 logical_bytes=0 mapped_bytes=0 allocations=0"
)
SELECTED_PAGE_TELEMETRY = (
    GOOD_TELEMETRY.replace("attempts=0 successes=0", "attempts=120 successes=120")
    .replace("logical_bytes=0", "logical_bytes=3211591680")
    .replace("mapped_bytes=0", "mapped_bytes=3232235520")
    .replace("allocations=0", "allocations=1920")
)
RUNTIME = (
    "gemma4_a4b_runtime: mode=streamed budget_mb=2048 kv_mb=512 "
    "safety_mb=64 expert_slots=8 model=/models/gemma.gguf"
)
GOOD_TIMING = (
    "generate_timing_ms: prompt_format=0 tokenize=0 runtime_prepare=1 "
    "prefill=2100 decode=1000 text_decode=1 total=3101"
)
SELECTED_PAGE_MOE = (
    "metal_a4b_selected_page_moe: enabled=1 logical_bytes=26763264 "
    "mapped_page_bytes=26935296 allocations=16 persistent_cache=0 "
    "weight_copies=0 rollback=TERMITE_METAL_DISABLE_A4B_SELECTED_EXPERT_PAGE_MOE"
)


class ResourceParserTests(unittest.TestCase):
    def test_parses_memory_pressure(self) -> None:
        output = (
            "The system has 17179869184 bytes.\n"
            "System-wide memory free percentage: 69%\n"
        )
        self.assertEqual(69, acceptance.parse_memory_pressure_free_percent(output))

    def test_memory_pressure_fails_closed_without_percentage(self) -> None:
        with self.assertRaisesRegex(acceptance.AcceptanceError, "did not report"):
            acceptance.parse_memory_pressure_free_percent("unknown")

    def test_parses_vm_stat_swapouts_using_reported_page_size(self) -> None:
        output = (
            "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
            "Pages free: 123.\n"
            "Swapouts: 42.\n"
        )
        self.assertEqual(42 * 16384, acceptance.parse_vm_stat_swapout_bytes(output))

    def test_process_rss_requires_numeric_kib(self) -> None:
        self.assertEqual(12345, acceptance.parse_process_rss_kib(" 12345\n"))
        with self.assertRaisesRegex(acceptance.AcceptanceError, "numeric RSS"):
            acceptance.parse_process_rss_kib("-")


class ResourceMonitorTests(unittest.TestCase):
    class RunningProcess:
        pid = 4242

        @staticmethod
        def poll() -> None:
            return None

    def wait_for(self, predicate: Callable[[], bool]) -> None:
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.001)
        self.fail("resource monitor did not publish a result")

    @mock.patch.object(acceptance, "_process_rss_mib", return_value=512.0)
    @mock.patch.object(acceptance, "_swapout_bytes", return_value=1024)
    @mock.patch.object(acceptance, "_memory_free_percent", return_value=70)
    def test_thread_samples_and_stops_cleanly(self, *_: object) -> None:
        monitor = acceptance.ResourceMonitor(
            self.RunningProcess(),
            baseline_swapout_bytes=1024,
            min_free_percent=10,
            max_rss_mib=2048,
            max_swapout_growth_mib=0,
            interval_seconds=0.001,
        )
        monitor.start()
        self.wait_for(lambda: bool(monitor.samples))
        monitor.stop()
        monitor.check()
        self.assertGreaterEqual(monitor.summary()["sample_count"], 1)

    @mock.patch.object(acceptance.os, "killpg")
    @mock.patch.object(acceptance, "_process_rss_mib", return_value=512.0)
    @mock.patch.object(acceptance, "_swapout_bytes", return_value=1024)
    @mock.patch.object(acceptance, "_memory_free_percent", return_value=4)
    def test_violation_terminates_server_and_fails_closed(
        self,
        _free: object,
        _swap: object,
        _rss: object,
        killpg: mock.Mock,
    ) -> None:
        monitor = acceptance.ResourceMonitor(
            self.RunningProcess(),
            baseline_swapout_bytes=1024,
            min_free_percent=5,
            max_rss_mib=2048,
            max_swapout_growth_mib=0,
            interval_seconds=0.001,
        )
        monitor.start()
        self.wait_for(lambda: monitor.violation is not None)
        monitor.stop()
        with self.assertRaisesRegex(acceptance.AcceptanceError, "fell below"):
            monitor.check()
        killpg.assert_called_once_with(
            self.RunningProcess.pid, acceptance.signal.SIGTERM
        )

    @mock.patch.object(acceptance.os, "killpg")
    @mock.patch.object(
        acceptance,
        "_memory_free_percent",
        side_effect=acceptance.AcceptanceError("probe unavailable"),
    )
    def test_probe_failure_terminates_server_and_fails_closed(
        self,
        _free: object,
        killpg: mock.Mock,
    ) -> None:
        monitor = acceptance.ResourceMonitor(
            self.RunningProcess(),
            baseline_swapout_bytes=0,
            min_free_percent=5,
            max_rss_mib=2048,
            max_swapout_growth_mib=0,
            interval_seconds=0.001,
        )
        monitor.start()
        self.wait_for(lambda: monitor.probe_failure is not None)
        monitor.stop()
        with self.assertRaisesRegex(acceptance.AcceptanceError, "failed closed"):
            monitor.check()
        killpg.assert_called_once_with(
            self.RunningProcess.pid, acceptance.signal.SIGTERM
        )


class ContractTests(unittest.TestCase):
    def test_short_serial_oracle_never_self_promotes_to_release_ready(self) -> None:
        qualification = acceptance.release_qualification(semantic_oracle_attested=True)
        self.assertFalse(qualification["release_ready"])
        self.assertTrue(qualification["semantic_oracle_attested"])
        self.assertEqual("short_serial_acceptance", qualification["scope"])

    def test_defaults_keep_streamed_residency_bounded_with_aggregate_headroom(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            binary = tmp / "antfly-inference"
            binary.touch()
            model_dir = tmp / "model"
            model_dir.mkdir()
            with mock.patch.object(
                acceptance.platform, "system", return_value="Darwin"
            ):
                args = acceptance.parse_args(
                    [
                        "--antfly-bin",
                        str(binary),
                        "--model-dir",
                        str(model_dir),
                    ]
                )
        self.assertEqual(2048, args.memory_budget_mb)
        self.assertEqual(2304, args.backend_budget_mb)
        self.assertEqual(4096, args.combined_budget_mb)

    def test_cautious_gate_accepts_100_output_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            binary = tmp / "antfly-inference"
            binary.touch()
            model_dir = tmp / "model"
            model_dir.mkdir()
            with mock.patch.object(
                acceptance.platform, "system", return_value="Darwin"
            ):
                args = acceptance.parse_args(
                    [
                        "--antfly-bin",
                        str(binary),
                        "--model-dir",
                        str(model_dir),
                        "--output-tokens",
                        "100",
                    ]
                )
        self.assertEqual(100, args.output_tokens)
        self.assertEqual(6500, args.max_frame_submits)

    def test_server_environment_enables_only_requested_diagnostics(self) -> None:
        plain = acceptance.effective_server_environment()
        debug = acceptance.effective_server_environment(debug_metal_timing=True)
        deferred = acceptance.effective_server_environment(
            active_paged_slot_attention_deferred=True
        )
        disabled = acceptance.effective_server_environment(
            disable_active_paged_slot_attention_deferred=True
        )
        host_routes = acceptance.effective_server_environment(
            disable_device_moe_route_select=True
        )
        selected_pages = acceptance.effective_server_environment(
            selected_expert_page_moe=True
        )
        self.assertEqual("1", plain["TERMITE_SERVER_A4B_TELEMETRY"])
        self.assertEqual("1", plain["TERMITE_SERVER_GENERATE_TIMING"])
        self.assertNotIn("TERMITE_DEBUG_METAL_TIMING", plain)
        self.assertEqual("1", debug["TERMITE_DEBUG_METAL_TIMING"])
        self.assertNotIn(
            "TERMITE_METAL_ENABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED", plain
        )
        self.assertEqual(
            "1",
            deferred["TERMITE_METAL_ENABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED"],
        )
        self.assertEqual(
            "1",
            disabled["TERMITE_METAL_DISABLE_ACTIVE_PAGED_SLOT_ATTENTION_DEFERRED"],
        )
        self.assertEqual(
            "1", host_routes["TERMITE_METAL_DISABLE_DEVICE_MOE_ROUTE_SELECT"]
        )
        self.assertNotIn("TERMITE_METAL_ENABLE_A4B_SELECTED_EXPERT_PAGE_MOE", plain)
        self.assertEqual(
            "1",
            selected_pages["TERMITE_METAL_ENABLE_A4B_SELECTED_EXPERT_PAGE_MOE"],
        )

    def test_server_environment_rejects_conflicting_attention_overrides(self) -> None:
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            acceptance.effective_server_environment(
                active_paged_slot_attention_deferred=True,
                disable_active_paged_slot_attention_deferred=True,
            )

    def test_server_config_is_serial_bounded_and_cache_cold(self) -> None:
        config = acceptance.build_server_config(
            models_dir=Path("/models"),
            model_id="gemma-a4b",
            residency_mode="streamed",
            memory_budget_mb=2048,
        )
        self.assertEqual(1, config["admission"]["inference"]["max_concurrent_requests"])
        self.assertEqual("off", config["generation_batching"]["mode"])
        self.assertFalse(config["prompt_cache"]["enabled"])
        self.assertEqual(
            {
                "kind": "generator",
                "name": "gemma-a4b",
                "backend": "metal",
                "residency_mode": "streamed",
                "memory_budget_mb": 2048,
            },
            config["preload"][0],
        )

    def test_request_is_deterministic_metal_without_prompt_cache(self) -> None:
        request = acceptance.generation_request(
            model_id="gemma-a4b", prompt="hello", output_tokens=8
        )
        self.assertEqual("metal", request["backend"])
        self.assertEqual("compiled", request["mode"])
        self.assertEqual("whole-model", request["compiled_target"])
        self.assertEqual(0, request["temperature"])
        self.assertEqual(1, request["top_k"])
        self.assertTrue(request["ignore_eos"])
        self.assertFalse(request["prompt_cache"])
        self.assertFalse(request["stream"])

        natural = acceptance.generation_request(
            model_id="gemma-a4b",
            prompt="hello",
            output_tokens=8,
            ignore_eos=False,
        )
        self.assertFalse(natural["ignore_eos"])
        thinking = acceptance.generation_request(
            model_id="gemma-a4b",
            prompt="hello",
            output_tokens=8,
            enable_thinking=True,
        )
        self.assertTrue(thinking["chat_template_kwargs"]["enable_thinking"])

    def test_response_contract_requires_semantics_length_and_usage(self) -> None:
        response = {
            "choices": [
                {
                    "message": {
                        "content": "Paris.",
                        "reasoning_content": "France's capital is Paris.",
                    },
                    "finish_reason": "length",
                }
            ],
            "usage": {"prompt_tokens": 12, "completion_tokens": 8},
        }
        summary = acceptance.summarize_response(
            response, expected_tokens=8, expected_substring="paris"
        )
        self.assertEqual("Paris.", summary["content"])
        self.assertEqual("France's capital is Paris.", summary["reasoning_content"])
        with self.assertRaisesRegex(acceptance.AcceptanceError, "semantic substring"):
            acceptance.summarize_response(
                response, expected_tokens=8, expected_substring="London"
            )

    def test_natural_eos_response_accepts_short_stop_and_rejects_length(self) -> None:
        response = {
            "choices": [{"message": {"content": "Paris"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 12, "completion_tokens": 2},
        }
        summary = acceptance.summarize_response(
            response,
            expected_tokens=8,
            expected_substring="Paris",
            fixed_length=False,
        )
        self.assertEqual(2, summary["completion_tokens"])
        response["choices"][0]["finish_reason"] = "length"
        with self.assertRaisesRegex(acceptance.AcceptanceError, "natural EOS"):
            acceptance.summarize_response(
                response,
                expected_tokens=8,
                expected_substring="Paris",
                fixed_length=False,
            )

    def test_good_runtime_and_request_telemetry_pass(self) -> None:
        result = acceptance.validate_server_log(
            RUNTIME
            + "\n"
            + GOOD_TIMING
            + "\n"
            + GOOD_TELEMETRY
            + "\n"
            + GOOD_TIMING
            + "\n"
            + GOOD_TELEMETRY,
            residency_mode="streamed",
            memory_budget_mb=2048,
            output_tokens=8,
            request_count=2,
            max_frame_submits=520,
        )
        self.assertEqual(2, len(result["measured_events"]))
        self.assertEqual(457, result["measured_events"][0]["frame_submits"])
        self.assertEqual(7.0, result["generation_timings_ms"][0]["decode_tok_s"])

    def test_selected_page_moe_requires_bounded_no_copy_marker(self) -> None:
        result = acceptance.validate_server_log(
            RUNTIME
            + "\n"
            + SELECTED_PAGE_MOE
            + "\n"
            + GOOD_TIMING
            + "\n"
            + SELECTED_PAGE_TELEMETRY,
            residency_mode="streamed",
            memory_budget_mb=2048,
            output_tokens=8,
            request_count=1,
            max_frame_submits=520,
            require_selected_page_moe=True,
        )
        self.assertEqual(16, result["selected_page_moe"]["allocations"])
        self.assertEqual(0, result["selected_page_moe"]["weight_copies"])
        self.assertEqual(120, result["measured_events"][0]["selected_page_successes"])

    def test_selected_page_moe_requires_request_scoped_success(self) -> None:
        with self.assertRaisesRegex(
            acceptance.AcceptanceError, "selected-expert page MoE telemetry"
        ):
            acceptance.validate_server_log(
                RUNTIME
                + "\n"
                + SELECTED_PAGE_MOE
                + "\n"
                + GOOD_TIMING
                + "\n"
                + GOOD_TELEMETRY,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
                require_selected_page_moe=True,
            )

    def test_selected_page_moe_missing_request_telemetry_fails_closed(self) -> None:
        base_telemetry = GOOD_TELEMETRY.splitlines()[0]
        with self.assertRaisesRegex(
            acceptance.AcceptanceError,
            "measured selected-expert page MoE telemetry events",
        ):
            acceptance.validate_server_log(
                RUNTIME
                + "\n"
                + SELECTED_PAGE_MOE
                + "\n"
                + GOOD_TIMING
                + "\n"
                + base_telemetry,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
                require_selected_page_moe=True,
            )

    def test_selected_page_moe_rejects_request_accounting_mismatch(self) -> None:
        bad = SELECTED_PAGE_TELEMETRY.replace("allocations=1920", "allocations=1919")
        with self.assertRaisesRegex(
            acceptance.AcceptanceError, "selected-expert page MoE telemetry"
        ):
            acceptance.validate_server_log(
                RUNTIME + "\n" + SELECTED_PAGE_MOE + "\n" + GOOD_TIMING + "\n" + bad,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
                require_selected_page_moe=True,
            )

    def test_unrequested_selected_page_request_telemetry_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            acceptance.AcceptanceError, "explicit harness request"
        ):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + SELECTED_PAGE_TELEMETRY,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
            )

    def test_selected_page_moe_missing_marker_fails_closed(self) -> None:
        with self.assertRaisesRegex(acceptance.AcceptanceError, "marker is missing"):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + GOOD_TELEMETRY,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
                require_selected_page_moe=True,
            )

    def test_selected_page_moe_unrequested_marker_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            acceptance.AcceptanceError, "explicit harness request"
        ):
            acceptance.validate_server_log(
                RUNTIME
                + "\n"
                + SELECTED_PAGE_MOE
                + "\n"
                + GOOD_TIMING
                + "\n"
                + GOOD_TELEMETRY,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
            )

    def test_any_route_fallback_is_rejected(self) -> None:
        bad = GOOD_TELEMETRY.replace("linear_fallbacks=0", "linear_fallbacks=1")
        with self.assertRaisesRegex(acceptance.AcceptanceError, "forbidden"):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + bad,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
            )

    def test_unattributed_blit_is_rejected(self) -> None:
        bad = GOOD_TELEMETRY.replace("blit_encoders=30", "blit_encoders=31")
        with self.assertRaisesRegex(acceptance.AcceptanceError, "unattributed"):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + bad,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
            )

    def test_attributed_slot_upload_blits_are_accepted(self) -> None:
        result = acceptance.validate_server_log(
            RUNTIME + "\n" + GOOD_TIMING + "\n" + GOOD_TELEMETRY,
            residency_mode="streamed",
            memory_budget_mb=2048,
            output_tokens=8,
            request_count=1,
            max_frame_submits=520,
        )
        self.assertEqual(30, result["measured_events"][0]["slot_upload_batches"])

    def test_host_route_selection_rollback_is_attested(self) -> None:
        host_routes = GOOD_TELEMETRY.replace(
            "route_select_successes=120", "route_select_successes=0"
        ).replace("route_select_fallbacks=0", "route_select_fallbacks=120")
        result = acceptance.validate_server_log(
            RUNTIME + "\n" + GOOD_TIMING + "\n" + host_routes,
            residency_mode="streamed",
            memory_budget_mb=2048,
            output_tokens=8,
            request_count=1,
            max_frame_submits=520,
            require_device_route_select=False,
        )
        self.assertEqual(120, result["measured_events"][0]["route_select_fallbacks"])

    def test_eager_route_counters_are_rejected(self) -> None:
        bad = GOOD_TELEMETRY.replace("linear_attempts=0", "linear_attempts=1")
        with self.assertRaisesRegex(acceptance.AcceptanceError, "escaped compiled"):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + bad,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
            )

    def test_packed_moe_contract_requires_successful_fused_ops(self) -> None:
        packed = (
            GOOD_TELEMETRY.replace("linear_attempts=0", "linear_attempts=90")
            .replace("linear_successes=0", "linear_successes=90")
            .replace("pair_attempts=0", "pair_attempts=30")
            .replace("pair_successes=0", "pair_successes=30")
            .replace("scatter_attempts=0", "scatter_attempts=30")
            .replace("scatter_successes=0", "scatter_successes=30")
        )
        result = acceptance.validate_server_log(
            RUNTIME + "\n" + GOOD_TIMING + "\n" + packed,
            residency_mode="streamed",
            memory_budget_mb=2048,
            output_tokens=8,
            request_count=1,
            max_frame_submits=520,
            require_packed_moe=True,
        )
        self.assertEqual(30, result["measured_events"][0]["pair_successes"])

    def test_packed_moe_contract_rejects_baseline_zero_counters(self) -> None:
        with self.assertRaisesRegex(acceptance.AcceptanceError, "packed A4B MoE"):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + GOOD_TELEMETRY,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
                require_packed_moe=True,
            )

    def test_old_per_layer_submission_count_is_rejected(self) -> None:
        bad = GOOD_TELEMETRY.replace("frame_submits=457", "frame_submits=660")
        with self.assertRaisesRegex(acceptance.AcceptanceError, "exceeds 520"):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + bad,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
            )

    def test_wrong_immutable_load_policy_is_rejected(self) -> None:
        resident = RUNTIME.replace("mode=streamed", "mode=resident")
        with self.assertRaisesRegex(acceptance.AcceptanceError, "different A4B"):
            acceptance.validate_server_log(
                resident + "\n" + GOOD_TIMING + "\n" + GOOD_TELEMETRY,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
            )

    def test_decode_throughput_floor_is_enforced(self) -> None:
        with self.assertRaisesRegex(acceptance.AcceptanceError, "decode throughput"):
            acceptance.validate_server_log(
                RUNTIME + "\n" + GOOD_TIMING + "\n" + GOOD_TELEMETRY,
                residency_mode="streamed",
                memory_budget_mb=2048,
                output_tokens=8,
                request_count=1,
                max_frame_submits=520,
                min_decode_tok_s=7.1,
            )

    def test_generation_timing_uses_actual_natural_eos_token_count(self) -> None:
        result = acceptance.validate_server_log(
            RUNTIME + "\n" + GOOD_TIMING + "\n" + GOOD_TELEMETRY,
            residency_mode="streamed",
            memory_budget_mb=2048,
            output_tokens=8,
            request_count=1,
            max_frame_submits=520,
            completion_token_counts=[3],
        )
        self.assertEqual(2.0, result["generation_timings_ms"][0]["decode_tok_s"])


if __name__ == "__main__":
    unittest.main()
