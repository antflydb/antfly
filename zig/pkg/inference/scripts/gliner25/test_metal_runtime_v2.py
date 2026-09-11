from __future__ import annotations

import copy
import unittest

import metal_runtime_contract_v2 as contract
import metal_python_worker_v2 as worker
from test_benchmark_metal import ready as old_ready, contract as fixture_contract


def ready(policy="optimized_v2", *, capacity=0):
    fixture = fixture_contract()
    event = old_ready(contract.v1.NATIVE, fixture)
    event.update(scope=contract.SCOPE, runtime_contract_version=2, execution_policy=policy,
                 runtime_policy=copy.deepcopy(contract.POLICIES[policy]),
                 model_live_bytes=4096 if policy == "optimized_v2" else 0,
                 workspace_capacity_bytes=capacity)
    return event, fixture


def admitted(policy="optimized_v2", *, capacity=0):
    event, fixture = ready(policy, capacity=capacity)
    return contract.native_ready(event, fixture["bundle"], fixture["model"], fixture["case_path"], policy)


def response(policy="optimized_v2", *, wb=0, wa=0, generation=0):
    fixture = fixture_contract()
    model = 4096 if policy == "optimized_v2" else 0
    snapshot = {key: 0 for key in (
        "device_owned_live_bytes", "host_mirror_live_bytes", "device_owned_buffers_created",
        "device_owned_buffers_released", "device_owned_bytes_created", "device_owned_bytes_released",
        "host_mirror_allocations", "host_mirror_frees")}
    before, after = dict(snapshot), dict(snapshot)
    before.update(device_owned_live_bytes=model + wb, device_owned_bytes_created=model + wb)
    after.update(device_owned_live_bytes=model + wa, device_owned_bytes_created=model + wa)
    event = {
        "event": "result", "runtime_contract_version": 2, "execution_policy": policy,
        "duration_ns": 100, "gpu_work_submitted": True, "external_frame": False,
        "request_stats": {"encoder": {"device_dispatches": 12}},
        "output": copy.deepcopy(fixture["expected"][fixture["cases"][0]]), "input_ids": [1, 2, 3],
        "host_fallback_evidence": {"strict_device_dispatch": True, "host_mirror_allocations_delta": 0,
                                   "host_mirror_download_bytes_delta": 0, "to_host_device_calls_delta": 0},
        "owned_memory": {"before": before, "after": after,
            "model": {"before_live_bytes": model, "after_live_bytes": model},
            "transient": {"before_live_bytes": 0, "after_live_bytes": 0, "pending_after_bytes": 0},
            "workspace": {"before_live_bytes": wb, "after_live_bytes": wa, "generation": generation}},
        "runtime_stats": {"actual_weight_upload_bytes": 0, "scope_submissions": 1,
                          "peak_pending_device_bytes": 1024, "pending_device_bytes": 0,
                          "workspace_live_bytes": wa, "workspace_pending_bytes": 0,
                          "workspace_peak_pending_bytes": 0},
    }
    return event, fixture["expected"][fixture["cases"][0]]


class RuntimeTests(unittest.TestCase):
    def test_phase_instrumentation_is_validated_and_excluded_from_acceptance(self):
        event, expected = response()
        event["runtime_stats"]["phase_timings_ns"] = {name: 1 for name in contract.PHASE_TIMINGS}
        self.assertEqual(expected, admitted().result(event, expected, validation=True, phase="validation"))
        self.assertEqual(expected, admitted().result(event, expected, validation=False, phase="diagnostic"))
        for phase in ("warmup", "measurement"):
            with self.subTest(phase=phase), self.assertRaises(contract.v1.BenchmarkError):
                admitted().result(event, expected, validation=False, phase=phase)
        event["runtime_stats"]["phase_timings_ns"]["device"] = -1
        with self.assertRaises(contract.v1.BenchmarkError):
            admitted().result(event, expected, validation=True)

    def test_legacy_requires_explicit_reference_and_cannot_downgrade_optimized(self):
        fixture = fixture_contract()
        event = old_ready(contract.v1.NATIVE, fixture)
        args = fixture["bundle"], fixture["model"], fixture["case_path"]
        self.assertTrue(contract.native_ready(event, *args, "reference_v1").legacy)
        with self.assertRaises(contract.v1.BenchmarkError):
            contract.native_ready(event, *args, "optimized_v2")
        event["runtime_contract_version"] = 2
        with self.assertRaises(contract.v1.BenchmarkError):
            contract.native_ready(event, *args, "reference_v1")

    def test_ready_policy_identity_is_closed_but_diagnostics_can_be_added(self):
        event, fixture = ready()
        event["runtime_policy"]["diagnostic_note"] = "extra"
        args = fixture["bundle"], fixture["model"], fixture["case_path"], "optimized_v2"
        self.assertFalse(contract.native_ready(event, *args).legacy)
        for key, value in (("execution_policy", "reference_v1"), ("runtime_contract_version", True),
                           ("model_live_bytes", 0), ("scope", contract.v1.SCOPE)):
            with self.subTest(key=key), self.assertRaises(contract.v1.BenchmarkError):
                contract.native_ready({**event, key: value}, *args)
        event["runtime_policy"]["relative_projection_residency"] = "request"
        with self.assertRaises(contract.v1.BenchmarkError):
            contract.native_ready(event, *args)

    def test_model_bytes_stable_and_optional_timings_not_invented(self):
        event, expected = response()
        event["runtime_stats"].update(scope_wait_nanos=None, extra_diagnostic=7)
        self.assertEqual(expected, admitted().result(event, expected, validation=True))
        del event["owned_memory"]["workspace"]
        del event["runtime_stats"]["workspace_live_bytes"]
        self.assertEqual(expected, admitted().result(event, expected, validation=True))

    def test_model_leak_transient_leak_pending_and_upload_are_rejected(self):
        mutations = (
            lambda e: e["owned_memory"]["model"].update(after_live_bytes=8192),
            lambda e: e["owned_memory"]["transient"].update(after_live_bytes=32),
            lambda e: e["owned_memory"]["transient"].update(pending_after_bytes=32),
            lambda e: e["runtime_stats"].update(actual_weight_upload_bytes=32),
            lambda e: e["runtime_stats"].update(pending_device_bytes=32),
            lambda e: e["runtime_stats"].update(scope_submissions=0),
            lambda e: e["owned_memory"]["after"].update(device_owned_live_bytes=5000),
            lambda e: e["owned_memory"]["after"].update(device_owned_bytes_released=1),
            lambda e: e["host_fallback_evidence"].update(to_host_device_calls_delta=1),
        )
        for mutate in mutations:
            event, expected = response()
            mutate(event)
            with self.subTest(mutation=mutate), self.assertRaises(contract.v1.BenchmarkError):
                admitted().result(event, expected, validation=True)

    def test_counter_types_are_exact_and_optional_timings_finite(self):
        for value in (True, -1, 1.0, "0", 1 << 64):
            event, expected = response()
            event["runtime_stats"]["scope_gpu_nanos"] = value
            with self.subTest(value=value), self.assertRaises(contract.v1.BenchmarkError):
                admitted().result(event, expected, validation=True)

    def test_workspace_growth_is_bounded_versioned_and_sealed_after_all_warmups(self):
        state = admitted(capacity=2048)
        event, expected = response(wa=1024, generation=1)
        state.result(event, expected, validation=True)
        event, expected = response(wb=1024, wa=2048, generation=2)
        state.result(event, expected, validation=False, phase="warmup")
        state.seal_workspace()
        event, expected = response(wb=2048, wa=2048, generation=2)
        state.result(event, expected, validation=False, phase="measurement")
        event["owned_memory"]["workspace"]["generation"] = 3
        with self.assertRaises(contract.v1.BenchmarkError):
            state.result(event, expected, validation=False, phase="measurement")

    def test_workspace_growth_cannot_hide_in_measurement_or_skip_a_generation(self):
        for values, phase in (((0, 1024, 0), "warmup"), ((0, 4096, 1), "warmup"),
                              ((1024, 1024, 1), "warmup"), ((0, 1024, 1), "measurement")):
            event, expected = response(wb=values[0], wa=values[1], generation=values[2])
            with self.subTest(values=values, phase=phase), self.assertRaises(contract.v1.BenchmarkError):
                admitted(capacity=2048).result(event, expected, validation=False, phase=phase)

    def test_rejected_case_preserves_valid_owner_baseline_for_next_case(self):
        for malformed in (False, True):
            state = admitted(capacity=2048)
            event, expected = response(wa=1024, generation=1)
            if malformed:
                del event["output"]["relations"]
            else:
                event["output"]["classifications"].append({"name": "unexpected", "labels": []})
            with self.subTest(malformed=malformed), self.assertRaises((contract.v1.cpu.BenchmarkError, KeyError)):
                state.result(event, expected, validation=True)
            self.assertEqual((1024, 1), (state.workspace_live_bytes, state.workspace_generation))
            valid, expected = response(wb=1024, wa=1024, generation=1)
            self.assertEqual(expected, state.result(valid, expected, validation=True))

    def test_invalid_owner_is_fatal_before_parity_and_never_advances_baseline(self):
        state = admitted(capacity=2048)
        event, expected = response(wa=1024, generation=1)
        event["output"]["classifications"].append({"name": "unexpected", "labels": []})
        event["runtime_stats"]["pending_device_bytes"] = 4
        with self.assertRaisesRegex(contract.v1.BenchmarkError, "pending device bytes"):
            state.result(event, expected, validation=True)
        self.assertEqual((0, 0), (state.workspace_live_bytes, state.workspace_generation))

    def test_stop_requires_actual_model_and_pending_cleanup(self):
        event = {"event": "stopped", "runtime_contract_version": 2, "execution_policy": "optimized_v2",
                 "model_live_bytes": 0, "transient_live_bytes": 0, "pending_device_bytes": 0}
        self.assertTrue(admitted().stopped(event)["final_owned_cleanup_proved"])
        for key in ("model_live_bytes", "transient_live_bytes", "pending_device_bytes"):
            with self.subTest(key=key), self.assertRaises(contract.v1.BenchmarkError):
                admitted().stopped({**event, key: 1})
        with self.assertRaises(contract.v1.BenchmarkError):
            admitted().stopped({"event": "stopped"})

    def test_reference_wire_adapter_preserves_outputs_and_timing(self):
        event = {"event": "result", "duration_ns": 17, "output": {"anything": [1, 2]}}
        self.assertEqual({**event, "scope": contract.SCOPE}, worker.event_v2(event))
        self.assertNotIn("scope", event)
        with self.assertRaises(worker.reference.WorkerError):
            worker.event_v2({"scope": "foreign"})

    def test_per_case_gate_does_not_average_away_one_bad_repetition(self):
        passed = {"lower_95": 5 / 6, "median": 1, "upper_95": 1.1}
        self.assertTrue(contract.competitiveness([passed] * 3, numerator="python_over_metal")["passed"])
        bad = {**passed, "lower_95": .83}
        self.assertFalse(contract.competitiveness([passed, bad, passed], numerator="python_over_metal")["passed"])
        self.assertFalse(contract.competitiveness([passed] * 2, numerator="python_over_metal")["passed"])
        cpu = {"lower_95": .7, "median": .8, "upper_95": .9}
        self.assertTrue(contract.competitiveness([cpu] * 3, numerator="native_over_python")["passed"])
        self.assertFalse(contract.competitiveness([cpu, cpu, {**cpu, "upper_95": 1}], numerator="native_over_python")["passed"])


if __name__ == "__main__":
    unittest.main()
