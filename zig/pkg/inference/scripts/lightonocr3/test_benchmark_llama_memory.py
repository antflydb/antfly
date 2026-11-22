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

"""Quality gates for the llama.cpp OCR memory benchmark."""

from http.client import IncompleteRead
from pathlib import Path
import signal
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

from benchmark_llama_memory import (
    Profile,
    active_slot_tasks,
    grounding_quality_passes,
    memory_measurement_valid,
    memory_occupancy_report,
    missing_memory_phases,
    quality_passes,
    read_json,
    record_phase_sample,
    run_pass,
    run_profile,
    shutdown_pass,
    score_case,
)


class QualificationTest(unittest.TestCase):
    def test_ocr_with_every_sentinel_but_high_cer_fails(self):
        case = {
            "expected": "Invoice heading, description, and balance USD 42.00",
            "required": ["42.00"],
        }
        row = {"finish": "stop", "text": "42.00"}
        score_case(row, case)
        self.assertEqual(row["missing"], [])
        self.assertFalse(quality_passes(row))

    def test_changing_one_table_row_fails_even_at_low_cer(self):
        rows = [
            ["Item", "Reference", "Quantity"],
            ["Replacement part", "SKU-1201", "2"],
            ["Replacement part", "SKU-1202", "3"],
        ]
        case = {
            "expected": "\n".join(" ".join(row) for row in rows),
            "required": ["SKU-1201", "SKU-1202"],
            "expected_table_rows": rows,
        }
        table = "<table><tr><td>Item</td><td>Reference</td><td>Quantity</td></tr><tr><td>Replacement part</td><td>SKU-1201</td><td>2</td></tr><tr><td>Replacement part</td><td>SKU-1202</td><td>3</td></tr></table>"
        row = {"finish": "stop", "text": table}
        score_case(row, case)
        self.assertTrue(quality_passes(row))
        row["text"] = case["expected"]
        score_case(row, case)
        self.assertEqual(row["cer"], 0)
        self.assertFalse(quality_passes(row))
        row["text"] = table.replace(
            "<td>2</td></tr><tr><td>Replacement part</td>",
            "</tr><tr><td>2</td><td>Replacement part</td>",
        )
        score_case(row, case)
        self.assertEqual(row["cer"], 0)
        self.assertFalse(quality_passes(row))
        row["text"] = (
            "| Item | Reference | Quantity |\n| --- | --- | --- |\n| Replacement part | SKU-1201 | 2 |\n| Replacement part | SKU-1202 | 3 |"
        )
        score_case(row, case)
        self.assertTrue(quality_passes(row))
        row["text"] = row["text"].replace(
            "| Replacement part | SKU-1202", "|| Replacement part | SKU-1202"
        )
        score_case(row, case)
        self.assertFalse(quality_passes(row))
        row["text"] = table.replace(">3<", ">8<")
        score_case(row, case)
        self.assertLess(row["cer"], 0.05)
        self.assertEqual(row["missing"], [])
        self.assertFalse(quality_passes(row))

    def test_setup_failure_records_a_result_row(self):
        profile = Profile("bf16")
        args = SimpleNamespace(models_dir=Path("."), grounding_only=False, rounds=1)
        for failure in (FileNotFoundError("weights"), OSError("address in use")):
            with (
                self.subTest(failure=failure),
                patch("benchmark_llama_memory.measure_profile", side_effect=failure),
            ):
                row = run_profile(args, [], None, "0.8B", profile, 4)
            self.assertFalse(row["qualified"])
            self.assertEqual(row["error"], str(failure))
            self.assertEqual(row["results"], [])
            self.assertEqual(row["missing_memory_phases"], ["serve"])
            self.assertEqual(row["parallel"], 4)

    def test_memory_sample_crossing_a_phase_transition_is_discarded(self):
        peaks = {}
        phase = ("idle", 1)
        metrics = {"rss_bytes": 100, "physical_footprint_bytes": 70}

        def read_across_transition():
            nonlocal phase
            phase = ("idle", 3)
            return metrics

        record_phase_sample(
            peaks, {}, lambda: phase, read_across_transition, lambda: set()
        )
        self.assertEqual(peaks, {})
        record_phase_sample(peaks, {}, lambda: phase, lambda: metrics, lambda: set())
        self.assertEqual(peaks, {"idle": metrics})

    def test_active_sample_requires_same_task_on_both_sides(self):
        metrics = {"rss_bytes": 100, "physical_footprint_bytes": 70}
        for slots, expected in (
            ([set()], {}),
            ([{(0, 4)}, set()], {}),
            ([{(0, 4)}, {(0, 5)}], {}),
            ([{(0, 4)}, {(0, 4), (1, 5)}], {"grounding": metrics}),
        ):
            with self.subTest(slots=slots):
                peaks = {}
                read_slots = iter(slots).__next__
                record_phase_sample(
                    peaks, {}, lambda: ("grounding", 1), lambda: metrics, read_slots
                )
                self.assertEqual(peaks, expected)

    def test_verified_occupancy_uses_only_tasks_bracketing_the_sample(self):
        metrics = {"rss_bytes": 100, "physical_footprint_bytes": 70}
        task_peaks = {}
        slots = iter([{(0, 4), (1, 5)}, {(0, 4), (1, 6)}])
        record_phase_sample(
            {}, task_peaks, lambda: ("serve", 1), lambda: metrics, slots.__next__
        )
        report = memory_occupancy_report(task_peaks, {"serve": 2})
        self.assertEqual(report["max_verified_tasks_by_phase"], {"serve": 1})
        self.assertEqual(report["missing_memory_occupancy_phases"], ["serve"])
        self.assertEqual(
            report["sampled_peak_bytes_by_phase_and_minimum_verified_occupancy"],
            {"serve": {1: metrics}},
        )

    def test_truncated_http_response_is_a_reportable_io_error(self):
        with patch("benchmark_llama_memory.urlopen") as open_request:
            response = open_request.return_value.__enter__.return_value
            response.read.side_effect = IncompleteRead(b"{", 10)
            with self.assertRaisesRegex(OSError, "HTTP protocol error for /slots"):
                read_json("http://localhost/slots")

    def test_slots_schema_validates_width_and_active_task_ids(self):
        slots = [{"id": 0, "is_processing": False}]
        with patch("benchmark_llama_memory.read_json", return_value=slots):
            self.assertEqual(active_slot_tasks("http://localhost", 1), set())
            with self.assertRaisesRegex(ValueError, "expected 2"):
                active_slot_tasks("http://localhost", 2)
            slots[0]["is_processing"] = True
            with self.assertRaisesRegex(ValueError, "expected 1"):
                active_slot_tasks("http://localhost", 1)
            slots[0]["id_task"] = 7
            self.assertEqual(active_slot_tasks("http://localhost", 1), {(0, 7)})

    def test_run_pass_disables_timing_probes_and_reports_memory_preflight_errors(self):
        case = {"name": "receipt"}
        args = SimpleNamespace(grounding_only=True)
        with (
            patch("benchmark_llama_memory.subprocess.Popen") as spawn,
            patch("benchmark_llama_memory.wait_ready"),
            patch("benchmark_llama_memory.child_pid", return_value=None),
            patch("benchmark_llama_memory.os.kill"),
            patch("benchmark_llama_memory.os.killpg"),
            patch(
                "benchmark_llama_memory.request_case", return_value={"finish": "stop"}
            ) as request,
            patch(
                "benchmark_llama_memory.active_slot_tasks",
                side_effect=OSError("HTTP 404"),
            ) as probe,
        ):
            argv = (args, [case], case, 1, ["llama-server"], "http://localhost")
            timing = run_pass(*argv, record_memory=False)
            timing_command = spawn.call_args.args[0]
            self.assertEqual(timing_command[:2], ["llama-server", "--alias"])
            self.assertEqual(timing_command[-1], "--no-slots")
            self.assertTrue(spawn.call_args.kwargs["start_new_session"])
            self.assertIsNone(timing.error)
            self.assertIsNone(request.call_args.kwargs["on_submit"])
            probe.assert_not_called()

            memory = run_pass(*argv, record_memory=True)
            memory_command = spawn.call_args.args[0]
            self.assertEqual(
                memory_command[:4], ["/usr/bin/time", "-l", "llama-server", "--alias"]
            )
            self.assertEqual(memory_command[-1], "--slots")
            self.assertNotEqual(timing_command[-2], memory_command[-2])
            self.assertEqual(
                memory.error, "llama.cpp /slots preflight failed: HTTP 404"
            )
            self.assertEqual(
                memory.slot_probe_errors["preflight"],
                {"count": 1, "first_error": "HTTP 404"},
            )
            self.assertIsNone(memory.grounding)

    def test_shutdown_reaps_group_when_wrapper_has_no_unique_child(self):
        proc = Mock(pid=29)
        with (
            patch("benchmark_llama_memory.child_pid", return_value=None),
            patch("benchmark_llama_memory.os.killpg") as kill_group,
        ):
            shutdown_pass(proc, record_memory=True)
        proc.wait.assert_called_once_with(timeout=20)
        self.assertEqual(
            kill_group.call_args_list,
            [call(29, signal.SIGTERM), call(29, signal.SIGKILL)],
        )

    def test_shutdown_sweeps_group_even_if_known_child_exits(self):
        proc = Mock(pid=29)
        with (
            patch("benchmark_llama_memory.child_pid", return_value=30),
            patch("benchmark_llama_memory.os.kill") as kill_process,
            patch("benchmark_llama_memory.os.killpg") as kill_group,
        ):
            shutdown_pass(proc, record_memory=True)
        kill_process.assert_called_once_with(30, signal.SIGTERM)
        proc.wait.assert_called_once_with(timeout=20)
        kill_group.assert_called_once_with(29, signal.SIGKILL)

    def test_shutdown_escalates_to_group_if_known_child_does_not_exit(self):
        proc = Mock(pid=29)
        proc.wait.side_effect = [subprocess.TimeoutExpired("time", 20), 0]
        with (
            patch("benchmark_llama_memory.child_pid", return_value=30),
            patch("benchmark_llama_memory.os.kill") as kill_process,
            patch("benchmark_llama_memory.os.killpg") as kill_group,
        ):
            shutdown_pass(proc, record_memory=True)
        kill_process.assert_called_once_with(30, signal.SIGTERM)
        kill_group.assert_called_once_with(29, signal.SIGKILL)
        self.assertEqual(proc.wait.call_args_list, [call(timeout=20), call(timeout=10)])

    def test_memory_process_launch_failure_is_scoped_to_that_pass(self):
        with patch(
            "benchmark_llama_memory.subprocess.Popen",
            side_effect=OSError("spawn failed"),
        ):
            memory = run_pass(
                SimpleNamespace(),
                [],
                None,
                1,
                ["llama-server"],
                "http://localhost",
                record_memory=True,
            )
        self.assertEqual(memory.error, "spawn failed")
        self.assertEqual(memory.task_peaks, {})
        self.assertEqual(memory.rows, [])

    def test_memory_phase_starts_at_http_submission(self):
        calls = []

        def open_request(request, **kwargs):
            calls.append("open")
            self.assertEqual(request.data, b'{"image": "encoded"}')
            raise OSError("request failed")

        with patch("benchmark_llama_memory.urlopen", side_effect=open_request):
            with self.assertRaises(OSError):
                read_json(
                    "http://localhost",
                    {"image": "encoded"},
                    on_submit=lambda: calls.append("submit"),
                )
        self.assertEqual(calls, ["submit", "open"])

    def test_reports_missing_active_phase_and_incomplete_counters(self):
        peaks = {"serve": {"rss_bytes": 100}, "grounding": {}}
        self.assertEqual(
            missing_memory_phases(peaks, ["serve", "grounding"]),
            ["serve", "grounding"],
        )

    def test_memory_qualification_requires_lifetime_and_active_phase_counters(self):
        metrics = {"rss_bytes": 100, "physical_footprint_bytes": 70}
        peaks = {"serve": metrics, "grounding": metrics}
        self.assertTrue(
            memory_measurement_valid(peaks, ["serve", "grounding"], 120, 80)
        )
        self.assertFalse(memory_measurement_valid(peaks, ["serve"], 120, None))
        self.assertFalse(
            memory_measurement_valid(
                {"serve": metrics}, ["serve", "grounding"], 120, 80
            )
        )
        self.assertFalse(
            memory_measurement_valid({"serve": {"rss_bytes": 100}}, ["serve"], 120, 80)
        )

    def test_layout_requires_distinct_boxes_at_fixture_coverage(self):
        case = {
            "expected": "Invoice 12345",
            "required": ["12345"],
            "min_grounding_boxes": 3,
        }
        box = "![title](1,2,3,4)"
        row = {"finish": "stop", "text": box * 3 + "Invoice 12345"}
        score_case(row, case)
        self.assertFalse(grounding_quality_passes(row, case))
        row["text"] = box + "![text](5,6,7,8)![table](9,10,11,12)Invoice 12345"
        score_case(row, case)
        self.assertTrue(grounding_quality_passes(row, case))


if __name__ == "__main__":
    unittest.main()
