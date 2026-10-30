"""Reject false-positive oracle results without loading a model or a GPU."""
import copy
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import qualify_metal_http as gate


class OracleComparison(unittest.TestCase):
    def setUp(self):
        self.guard = {
            "id": "guard", "heads": [
                {"task": "first", "labels": ["safe", "unsafe"],
                 "probabilities": {"values": [0.1, 0.9]}},
                {"task": "second", "labels": ["a", "b"],
                 "probabilities": {"values": [0.8, 0.7]}},
            ], "presented_output": {"second": ["a", "b"], "first": "unsafe"},
        }
        self.guard_row = {"classifications": [
            {"name": "first", "label": "unsafe", "score": 0.9},
            {"name": "second", "label": "a", "score": 0.8},
            {"name": "second", "label": "b", "score": 0.7},
        ]}
        self.pii = {"id": "unicode", "labels": ["person"], "text": "José José",
                    "presented_output": {"entities": {"person": [
                        {"text": "José", "start": 0, "end": 4, "confidence": 0.9},
                        {"text": "José", "start": 5, "end": 9, "confidence": 0.8},
                    ]}}}
        self.pii_row = {"entities": [
            {"label": "person", "text": "José", "start": 0, "end": 4, "score": 0.9},
            {"label": "person", "text": "José", "start": 5, "end": 9, "score": 0.8},
        ]}

    def test_capture_object_key_sorting_does_not_define_api_task_order(self):
        self.assertEqual(0, gate.compare_row("gliguard", self.guard, self.guard_row))
        self.guard_row["classifications"].reverse()
        with self.assertRaisesRegex(RuntimeError, "order"):
            gate.compare_row("gliguard", self.guard, self.guard_row)

    def test_captured_selection_is_independent_of_probability_argmax(self):
        self.guard["presented_output"]["first"] = "safe"
        with self.assertRaisesRegex(RuntimeError, "selection/order"):
            gate.compare_row("gliguard", self.guard, self.guard_row)

    def test_duplicate_or_unknown_capture_labels_fail_closed(self):
        for labels in (["a", "a"], ["unknown"]):
            self.guard["presented_output"]["second"] = labels
            with self.assertRaisesRegex(RuntimeError, "invalid captured"):
                gate.compare_row("gliguard", self.guard, self.guard_row)

    def test_repeated_unicode_entities_keep_distinct_coordinates(self):
        self.assertEqual(0, gate.compare_row("pii_multi", self.pii, self.pii_row))
        self.pii_row["entities"][1]["start"] = 0
        with self.assertRaisesRegex(RuntimeError, "identity/order"):
            gate.compare_row("pii_multi", self.pii, self.pii_row)

    def test_default_offsets_are_utf8_bytes(self):
        row = copy.deepcopy(self.pii_row)
        row["entities"][0]["end"] = 5
        row["entities"][1].update(start=6, end=11)
        self.assertEqual(0, gate.compare_row("pii_multi", self.pii, row, byte_offsets=True))
        with self.assertRaises(RuntimeError):
            gate.compare_row("pii_multi", self.pii, self.pii_row, byte_offsets=True)

    def test_explicit_allow_preserves_confidence_order_not_source_order(self):
        self.pii["request_options"] = {"overlap": "allow"}
        self.pii["presented_output"]["entities"]["person"].reverse()
        self.pii["presented_output"]["entities"]["person"][0]["confidence"] = 0.95
        self.pii_row["entities"].reverse()
        self.pii_row["entities"][0]["score"] = 0.95
        self.assertEqual(0, gate.compare_row("pii_multi", self.pii, self.pii_row))
        self.pii_row["entities"].reverse()
        with self.assertRaisesRegex(RuntimeError, "identity/order"):
            gate.compare_row("pii_multi", self.pii, self.pii_row)

    def test_nonfinite_output_never_passes_a_tolerance_check(self):
        self.guard_row["classifications"][0]["score"] = float("nan")
        with self.assertRaisesRegex(RuntimeError, "non-finite"):
            gate.compare_row("gliguard", self.guard, self.guard_row)

    def test_ragged_batch_retains_required_shared_schema(self):
        case = {"id": "one", "text": "hello", "heads": [
            {"task": "safety", "labels": ["safe", "unsafe"], "multi_label": False, "threshold": 0.5}
        ]}
        body = gate.ragged_guard_body([case])
        self.assertEqual(gate.guard_schema(case), body["schema"])
        self.assertEqual(9, len(body["inputs"]))
        self.assertTrue(all("schema" in row for row in body["inputs"]))

    def test_entity_thresholds_use_public_entity_definitions(self):
        schema = gate.pii_schema({"labels": ["person", "email"],
                                  "entity_thresholds": {"person": 0.9}})
        self.assertEqual(["person", "email"], schema["entities"])
        self.assertEqual({"person": {"threshold": 0.9}}, schema["entity_definitions"])

    def test_boundary_uses_observed_prepared_usage_and_checks_the_next_token(self):
        probes = []

        def endpoint(_url, _path, body):
            tokens = 40 + len(body["inputs"][0]["content"].split())
            probes.append(tokens)
            if tokens > 512:
                return 413, {"error": "EXTRACTION_LIMIT_EXCEEDED"}, 0.001
            return 200, {"usage": {"prompt_tokens": tokens}, "data": [{}]}, 0.001

        with patch.object(gate, "http", side_effect=endpoint):
            result = gate.prepared_boundary("http://unused", "pii_multi", self.pii)
        self.assertEqual(512, result["accepted_prepared_tokens"])
        self.assertEqual([511, 512, 513], probes[-3:])

    def test_boundary_composes_exact_limits_without_a_single_piece_word(self):
        probes = []
        costs = {"a": 2, "token": 3, "hello": 2}

        def endpoint(_url, _path, body):
            tokens = 40 + sum(costs[word] for word in body["inputs"][0]["content"].split())
            probes.append(tokens)
            if tokens > 512:
                return 413, {"error": "EXTRACTION_LIMIT_EXCEEDED"}, 0.001
            return 200, {"usage": {"prompt_tokens": tokens}, "data": [{}]}, 0.001

        with patch.object(gate, "http", side_effect=endpoint):
            result = gate.prepared_boundary("http://unused", "pii_multi", self.pii)
        self.assertEqual(costs, result["measured_word_piece_costs"])
        self.assertEqual([511, 512, 513], probes[-3:])

    def test_live_catalog_rejects_extra_capabilities_and_contract_drift(self):
        catalog = {"extractors": {}}
        for profile, name in gate.MODELS.items():
            catalog["extractors"][name] = {
                "capabilities": ["extraction"] if profile == "pii_multi" else
                    ["classification", "zero_shot", "multi_label"],
                "inputs": ["text"], "inference_capabilities": {
                    "task": "extract", "input_modalities": ["text"],
                    "batch": {"max_items": 128}, "result_cardinality": "one_per_item"}}
        gate.verify_catalog(catalog)
        for mutate in (
                lambda info: info["capabilities"].append("embedding"),
                lambda info: info["inference_capabilities"]["batch"].update(max_items=1),
                lambda info: info["inference_capabilities"]["input_modalities"].append("image"),
                lambda info: info["inference_capabilities"].update(result_cardinality="one_per_request")):
            changed = copy.deepcopy(catalog)
            mutate(changed["extractors"][gate.MODELS["pii_multi"]])
            with self.assertRaises(RuntimeError):
                gate.verify_catalog(changed)

    def concurrent_responses(self):
        row = copy.deepcopy(self.pii_row)
        row["id"] = self.pii["id"]
        success = (200, {"data": [row]}, 0.01, {})
        busy = (503, {"error": "MODEL_RESOURCE_BUSY",
                      "message": "insufficient inference capacity is currently available",
                      "reason": "inference_capacity", "retryable": True, "retry_after_ms": 1000},
                0.001, {"retry-after": "1"})
        return success, busy

    def test_concurrent_backpressure_retries_only_after_both_initial_calls_drain(self):
        success, busy = self.concurrent_responses()
        lock = threading.Lock()
        busy_sent = threading.Event()
        allow_success = threading.Event()
        retry_started = threading.Event()
        completed = 0

        def endpoint(*_args):
            nonlocal completed
            with lock:
                completed += 1
                number = completed
            if number == 1:
                self.assertTrue(allow_success.wait(3))
                return success
            if number == 2:
                busy_sent.set()
                return busy
            retry_started.set()
            self.assertTrue(allow_success.is_set(), "retry raced the undrained success")
            return success

        with patch.object(gate, "http_with_headers", side_effect=endpoint) as raw:
            with gate.ThreadPoolExecutor(max_workers=1) as driver:
                pending = driver.submit(gate.concurrent_requests, "http://unused", "pii_multi", self.pii)
                try:
                    self.assertTrue(busy_sent.wait(3))
                    self.assertFalse(retry_started.wait(0.1))
                finally:
                    allow_success.set()
                report = pending.result(timeout=3)
        self.assertEqual((1, 1, 1), (report["initial_successes"], report["initial_busy"], report["retry_successes"]))
        self.assertEqual(3, raw.call_count)

    def test_concurrent_rejections_cannot_hide_failure_or_stalled_progress(self):
        success, busy = self.concurrent_responses()
        invalid = [
            (429, busy[1], busy[2], busy[3]),
            (503, dict(busy[1], retryable=False), busy[2], busy[3]),
            (503, busy[1], busy[2], {"retry-after": "2"}),
            (503, dict(busy[1], retry_after_ms=1000.0), busy[2], busy[3]),
        ]
        for bad in invalid:
            with self.subTest(response=bad), patch.object(gate, "http_with_headers", side_effect=[success, bad]) as raw:
                with self.assertRaisesRegex(RuntimeError, "unexpected concurrent rejection"):
                    gate.concurrent_requests("http://unused", "pii_multi", self.pii)
                self.assertEqual(2, raw.call_count)
        with patch.object(gate, "http_with_headers", return_value=busy):
            with self.assertRaisesRegex(RuntimeError, "no progress"):
                gate.concurrent_requests("http://unused", "pii_multi", self.pii)
        with patch.object(gate, "http_with_headers", side_effect=[success, busy, busy]):
            with self.assertRaisesRegex(RuntimeError, "post-drain retry failed"):
                gate.concurrent_requests("http://unused", "pii_multi", self.pii)

    def test_concurrent_transport_errors_propagate(self):
        success, _ = self.concurrent_responses()
        with patch.object(gate, "http_with_headers", side_effect=[OSError("lost socket"), success]):
            with self.assertRaisesRegex(OSError, "lost socket"):
                gate.concurrent_requests("http://unused", "pii_multi", self.pii)

    def test_late_resource_failure_or_server_exit_cannot_pass(self):
        for failure in ("resource", "crash"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                output = Path(directory)
                inspecting_log = threading.Event()
                terminated = threading.Event()
                first_sample = threading.Event()
                rss_calls = 0

                class Process:
                    pid = 123456
                    returncode = None

                    def poll(self):
                        return self.returncode

                process = Process()
                original_read = Path.read_text

                def terminate(_process):
                    process.returncode = -15
                    terminated.set()

                def rss(_group):
                    nonlocal rss_calls
                    rss_calls += 1
                    if rss_calls >= 2:
                        first_sample.set()
                    if process.poll() is not None:
                        return 0
                    return 8193 if inspecting_log.is_set() else 100

                def exercise(*_args):
                    self.assertTrue(first_sample.wait(3))
                    return {"cases": [], "gates": {}}

                def read_log(path, *args, **kwargs):
                    if path.name != "default-server.log":
                        return original_read(path, *args, **kwargs)
                    if failure == "resource":
                        # Trigger the real monitor after the last successful
                        # request/health check, during final log inspection.
                        inspecting_log.set()
                        self.assertTrue(terminated.wait(3))
                    else:
                        process.returncode = 3
                    return "\n".join("selected backend metal for /models/" + name
                                     for name in gate.MODELS.values())

                with (patch.object(gate.socket, "socket") as sockets,
                      patch.object(gate, "ProcessGroupPhysicalMemorySampler") as physical,
                      patch.object(gate.subprocess, "Popen", return_value=process),
                      patch.object(gate, "group_rss_mib", side_effect=rss),
                      patch.object(gate, "memory_free_percent", return_value=80),
                      patch.object(gate, "swapout_bytes", return_value=0),
                      patch.object(gate, "terminate_group", side_effect=terminate),
                      patch.object(gate, "http", return_value=(200, {}, 0.001)),
                      patch.object(gate, "verify_catalog"),
                      patch.object(gate, "exercise", side_effect=exercise),
                      patch.object(Path, "read_text", read_log)):
                    sockets.return_value.__enter__.return_value.getsockname.return_value = ("127.0.0.1", 18090)
                    physical.return_value.snapshot.return_value = {
                        "live_processes": 2, "observed_process_identities": 2,
                        "conservative_summed_identity_peak_bound_bytes": 100 * 1024**2,
                    }
                    with self.assertRaisesRegex(RuntimeError, "production cleanup failed"):
                        gate.run_lane(Path("/unused/server"), output, output, {}, "default", 25)
                receipt = json.loads((output / "default-receipt.json").read_text())
                self.assertFalse(receipt["passed"])
                self.assertTrue(receipt["cleanup_errors"])
                self.assertEqual(0, receipt["cleanup_group_rss_mib"])
                self.assertTrue(terminated.is_set())


if __name__ == "__main__":
    unittest.main()
