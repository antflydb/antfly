"""Qualification gates must fail closed when measurements or identities differ."""

import copy
import unittest
from unittest.mock import patch
from compare_receipts import compare
from limits import check_container
from load_clients import (
    percentile,
    operation_kind,
    workload_breakdown,
    admission_diagnostics,
    workload_snapshot,
    waiting_evidence_failures,
)
from run_matrix import variant_order
from pressure_clients import PressureClients


class QualificationTests(unittest.TestCase):
    def test_retained_pressure_rejects_disconnected_upload_and_closes_on_failure(self):
        import socket
        import urllib.parse

        pressure = PressureClients(
            urllib.parse.urlsplit("http://127.0.0.1:8080/db/v1/status"),
            5432,
            dict(
                fixture_rows=2,
                retained_pressure=dict(slow_uploads=1, upload_bytes=4096),
            ),
        )
        upload, remote = socket.socketpair()
        pressure.uploads.append(upload)
        remote.close()
        try:
            with self.assertRaisesRegex(RuntimeError, "rejected or closed"):
                pressure.verify()
        finally:
            pressure.close()
        self.assertEqual(-1, upload.fileno())
        self.assertEqual([], pressure.uploads)

    def test_retained_pressure_rejects_impossible_cursor_and_body_ownership(self):
        for config in (
            dict(held_cursors=2, pgwire_connections=1),
            dict(slow_uploads=1, upload_bytes=1),
            dict(http_connections=-1),
        ):
            with self.assertRaises(ValueError):
                PressureClients(
                    None, 5432, dict(fixture_rows=2, retained_pressure=config)
                )

    def test_pressure_profile_requires_measured_waiting(self):
        for diagnostics in (None, {}, {"wait_completed_total": 0}):
            self.assertEqual(
                ["fixed waiting was not exercised"],
                waiting_evidence_failures(diagnostics, 1),
            )
        self.assertEqual([], waiting_evidence_failures({"wait_completed_total": 2}, 1))
        self.assertEqual([], waiting_evidence_failures(None, 0))

    def test_fixed_policy_rejects_ineffective_waiting_after_sql_work(self):
        with patch.dict("os.environ", ANTFLY_QUALIFICATION_POLICY="fixed"):
            for query in ({}, {"sql_waiting_enabled": False}):
                with patch(
                    "load_clients.operator_request",
                    return_value=(200, {"query": query}),
                ):
                    # Before the first SQL request the pool may still be lazy.
                    workload_snapshot(None)
                    with self.assertRaisesRegex(RuntimeError, "enable SQL waiting"):
                        workload_snapshot(None, require_waiting=True)
            with patch(
                "load_clients.operator_request",
                return_value=(200, {"query": {"sql_waiting_enabled": True}}),
            ):
                self.assertTrue(
                    workload_snapshot(None, require_waiting=True)["query"][
                        "sql_waiting_enabled"
                    ]
                )
        with patch.dict("os.environ", ANTFLY_QUALIFICATION_POLICY="failfast"):
            with patch(
                "load_clients.operator_request",
                return_value=(200, {"query": {"sql_waiting_enabled": False}}),
            ):
                workload_snapshot(None, require_waiting=True)

    def test_admission_diagnostics_keep_queue_wait_and_lease_ownership_separate(self):
        before = {
            "query": {
                key: 0
                for key in (
                    "wait_ns_total",
                    "wait_completed_total",
                    "lease_hold_ns_total",
                    "lease_completed_total",
                    "rejected_total",
                    "expired_total",
                    "cancelled_total",
                )
            }
        }
        after = copy.deepcopy(before)
        after["query"].update(
            wait_ns_total=20_000_000,
            wait_completed_total=2,
            lease_hold_ns_total=12_000_000,
            lease_completed_total=4,
        )
        result = admission_diagnostics(before, after)
        self.assertEqual(0.01, result["mean_wait_seconds"])
        self.assertEqual(0.003, result["mean_completed_lease_hold_seconds"])
        self.assertIsNone(admission_diagnostics(None, None))
        with self.assertRaises(RuntimeError):
            admission_diagnostics(after, before)

    def test_repeated_matrix_rotates_every_variant_through_every_position(self):
        variants = ["main", "fixed", "failfast"]
        orders = [variant_order(variants, i) for i in range(3)]
        for position in range(3):
            self.assertEqual(set(variants), {order[position] for order in orders})
        self.assertEqual(variants, variant_order(variants, 3))
        self.assertEqual(["main", "fixed", "failfast"], variants)

    def receipt(self):
        return dict(
            passed=True,
            server_exit_code=0,
            binary_sha256="candidate",
            manifest_sha256="manifest",
            configuration={"admission": {}, "pgwire": {}},
            process_limits={"max_fds": 512},
            enforced_limits={"platform": "Linux", "memory_bytes": 2147483648},
            manifest={
                "comparison_thresholds": {
                    "low_load_p95_ratio_max": 1.25,
                    "throughput_ratio_min": 0.9,
                },
                "phases": [{"name": "low-load"}, {"name": "sustained-overload"}],
            },
            phases=[
                dict(
                    name=name,
                    offered=100,
                    retries=0,
                    success_p95_seconds=0.1,
                    successful_per_second=20,
                )
                for name in ("low-load", "sustained-overload")
            ],
        )

    def test_equal_successful_receipts_do_not_qualify_automatic_defaults(self):
        result = compare(self.receipt(), self.receipt())
        self.assertTrue(result["passed"])
        self.assertFalse(result["automatic_defaults_qualified"])

    def test_resource_and_manifest_identity_mismatch_fail(self):
        baseline = self.receipt()
        for field in ("manifest_sha256", "process_limits", "enforced_limits"):
            candidate = self.receipt()
            candidate[field] = "different"
            self.assertFalse(compare(baseline, candidate)["passed"])

    def test_failed_shutdown_missing_phase_and_retries_fail(self):
        for change in ("shutdown", "phase", "retries", "qualification", "zero-success"):
            candidate = self.receipt()
            if change == "shutdown":
                candidate["server_exit_code"] = 1
            elif change == "phase":
                candidate["phases"].pop()
            elif change == "retries":
                candidate["phases"][0]["retries"] = 1
            elif change == "qualification":
                candidate["passed"] = False
            else:
                candidate["phases"][0]["success_p95_seconds"] = None
            self.assertFalse(compare(self.receipt(), candidate)["passed"])

    def test_latency_and_throughput_regressions_fail(self):
        candidate = self.receipt()
        candidate["phases"][0]["success_p95_seconds"] = 0.126
        self.assertFalse(compare(self.receipt(), candidate)["passed"])
        candidate = self.receipt()
        candidate["phases"][1]["successful_per_second"] = 17.9
        self.assertFalse(compare(self.receipt(), candidate)["passed"])

    def test_linux_manifest_requires_actual_equal_container_limits(self):
        manifest = {
            "container_limits": {
                "cpu_count": 2,
                "memory_bytes": 2147483648,
                "pids": 512,
            }
        }
        measured = dict(platform="Linux", **manifest["container_limits"])
        self.assertFalse(check_container(manifest, measured))
        for field in ("cpu_count", "memory_bytes", "pids"):
            unavailable = copy.deepcopy(measured)
            unavailable[field] = None
            self.assertTrue(check_container(manifest, unavailable))
        self.assertTrue(check_container(manifest, dict(measured, platform="Darwin")))

    def test_same_binary_comparison_requires_equal_nonwaiting_configuration(self):
        baseline, candidate = self.receipt(), self.receipt()
        candidate["configuration"]["admission"]["max_request_memory_bytes"] = 64
        self.assertFalse(compare(baseline, candidate)["passed"])
        baseline["configuration"]["admission"]["max_request_memory_bytes"] = 64
        candidate["configuration"]["admission"]["query"] = {
            "waiting": {"max_wait_ms": 250}
        }
        baseline["configuration"]["admission"]["query"] = {
            "waiting": {"max_wait_ms": 0}
        }
        self.assertTrue(compare(baseline, candidate)["passed"])

    def test_workload_breakdown_preserves_drops_failures_and_successes(self):
        outcomes = [
            {"kind": "cheap", "ok": True, "status": 200, "end_to_end_seconds": 0.1},
            {"kind": "cheap", "ok": False, "status": 503, "end_to_end_seconds": 0.2},
            {
                "kind": "slow_reader",
                "ok": True,
                "status": 200,
                "end_to_end_seconds": 0.4,
            },
        ]
        measured = workload_breakdown(
            outcomes, {"cheap": 4, "slow_reader": 1}, {"cheap": 2}, 2
        )
        self.assertEqual(sum(k["offered"] for k in measured.values()), 5)
        self.assertEqual(
            sum(k["completed"] + k["generator_dropped"] for k in measured.values()), 5
        )
        self.assertEqual(measured["cheap"]["successful_per_second"], 0.5)
        self.assertEqual(measured["cheap"]["status_counts"], {"200": 1, "503": 1})
        self.assertEqual(
            [operation_kind(i, True) for i in (3, 7, 11, 15)],
            ["expensive", "write", "expensive", "slow_reader"],
        )
        self.assertTrue(all(operation_kind(i, False) == "cheap" for i in range(16)))

    def test_empty_latency_sample_has_no_successful_percentile(self):
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(percentile([0.3, 0.1, 0.2], 0.5), 0.2)


class RunnableQualificationTests(unittest.TestCase):
    def manifest(self):
        return dict(
            configuration={"admission": {"query": {"max_concurrent_requests": 1}}},
            candidate_configuration={
                "admission": {
                    "query": {
                        "max_concurrent_requests": 1,
                        "waiting": {"max_wait_ms": 250},
                    },
                    "max_request_memory_bytes": 67108864,
                }
            },
            runnable_policy={"slots": 4, "cleanup_slots": 1},
        )

    def test_scheduler_matrix_selects_and_compares_each_variant(self):
        import pathlib
        import tempfile
        from types import SimpleNamespace
        import run_matrix

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifest = root / "profile.json"
            manifest.write_text(
                __import__("json").dumps(dict(self.manifest(), kind="load"))
            )
            calls = []

            def run(argv):
                calls.append(argv)
                return SimpleNamespace(returncode=0)

            with (
                patch(
                    "sys.argv",
                    [
                        "run_matrix",
                        "--candidate",
                        "/candidate",
                        "--baseline",
                        "/main",
                        "--manifest",
                        str(manifest),
                        "--repetitions",
                        "4",
                        "--output-dir",
                        str(root / "output"),
                    ],
                ),
                patch("run_matrix.subprocess.run", side_effect=run),
            ):
                self.assertFalse(run_matrix.main())
            launches = [call for call in calls if call[1].endswith("run_local.py")]
            orders = [
                [
                    call[call.index("--policy") + 1]
                    for call in launches[offset : offset + 4]
                ]
                for offset in range(0, 16, 4)
            ]
            for position in range(4):
                self.assertEqual(
                    {"baseline", "fixed", "failfast", "runnable"},
                    {order[position] for order in orders},
                )
            comparisons = [
                call for call in calls if call[1].endswith("compare_receipts.py")
            ]
            self.assertEqual(20, len(comparisons))
            self.assertEqual(
                12,
                sum(
                    "candidate-runnable" in call[call.index("--candidate") + 1]
                    for call in comparisons
                ),
            )
            for repetition in range(1, 5):
                prefix = "profile-r%02d-" % repetition
                pairs = {
                    (
                        pathlib.Path(call[call.index("--baseline") + 1]).stem,
                        pathlib.Path(call[call.index("--candidate") + 1]).stem,
                    )
                    for call in comparisons
                }
                self.assertIn(
                    (prefix + "candidate-failfast", prefix + "candidate-runnable"),
                    pairs,
                )

    def test_variant_preserves_admission_and_memory_configuration(self):
        from run_local import configuration_for_policy

        manifest = self.manifest()
        before = copy.deepcopy(manifest)
        fixed = configuration_for_policy(manifest, "fixed")
        runnable = configuration_for_policy(manifest, "runnable")
        self.assertEqual(
            manifest["runnable_policy"], runnable["admission"].pop("runnable")
        )
        self.assertEqual(fixed, runnable)
        self.assertEqual(before, manifest)
        self.assertNotIn(
            "runnable", configuration_for_policy(manifest, "baseline")["admission"]
        )
        with self.assertRaises(ValueError):
            configuration_for_policy({}, "runnable")

    def measured(self):
        return dict(
            workload_after={
                "runnable": {
                    "enabled": True,
                    "capacity_slots": 4,
                    "control_capacity_slots": 2,
                    "operation_capacity": 128,
                    "metadata_capacity_bytes": 8388608,
                    "control_metadata_capacity_bytes": 65536,
                    "max_wait_ms": 250,
                    "quantum_ms": 2,
                    "checkpoint_budget": 64,
                }
            },
            runnable_diagnostics={"grants": [10, 2, 0, 0, 1], "yields": 1},
            by_kind={"cheap": {"successful": 5}, "write": {"successful": 1}},
        )

    def test_success_requires_matching_capacity_and_real_class_grants(self):
        from load_clients import runnable_evidence_failures

        self.assertEqual(
            [],
            runnable_evidence_failures(
                self.measured(), self.manifest()["runnable_policy"], 1
            ),
        )
        for change in ("disabled", "capacity", "query", "write", "yield", "missing"):
            measured = self.measured()
            if change == "disabled":
                measured["workload_after"]["runnable"]["enabled"] = False
            elif change == "capacity":
                measured["workload_after"]["runnable"]["capacity_slots"] = 8
            elif change == "query":
                measured["runnable_diagnostics"]["grants"][0] = 0
            elif change == "write":
                measured["runnable_diagnostics"]["grants"][1] = 0
            elif change == "yield":
                measured["runnable_diagnostics"]["yields"] = 0
            else:
                measured.pop("runnable_diagnostics")
            self.assertTrue(
                runnable_evidence_failures(
                    measured, self.manifest()["runnable_policy"], 1
                ),
                change,
            )

    def test_runnable_deltas_reject_policy_changes_counter_reset_and_missing_classes(
        self,
    ):
        from load_clients import runnable_diagnostics

        stats = dict(
            self.measured()["workload_after"]["runnable"],
            grants=[10, 2, 0, 0, 1],
            yields=1,
            backfills=0,
            rejected=0,
            wait_ns=10,
        )
        with patch.dict("os.environ", ANTFLY_QUALIFICATION_POLICY="runnable"):
            for field, value in (
                ("capacity_slots", 8),
                ("grants", [9, 2, 0, 0, 1]),
                ("grants", [10, 2]),
                ("wait_ns", 0),
            ):
                after = dict(stats, **{field: value})
                with self.assertRaises(RuntimeError):
                    runnable_diagnostics({"runnable": stats}, {"runnable": after})
            after = dict(stats, grants=[11, 3, 0, 0, 1], yields=2, wait_ns=20)
            delta = runnable_diagnostics({"runnable": stats}, {"runnable": after})
            self.assertEqual([1, 1, 0, 0, 0], delta["grants"])
            self.assertEqual(1, delta["yields"])
            self.assertEqual({"available": False}, delta["service"])
            stats["service"] = self.service_stats()
            after["service"] = self.service_stats()
            after["service"]["observed_slot_ns"][4] += 12
            delta = runnable_diagnostics({"runnable": stats}, {"runnable": after})
            self.assertEqual(
                [0, 0, 0, 0, 12, 0], delta["service"]["counters"]["observed_slot_ns"]
            )

    @staticmethod
    def service_stats():
        from load_clients import SERVICE_COUNTERS

        return dict(
            version=1,
            struct_size=408,
            measurement_model=1,
            available=True,
            quantum_ns=1_000_000,
            **{field: [100] * 6 for field in SERVICE_COUNTERS},
            debt_slot_ns=[0] * 6,
            rebate_slot_ns=[0] * 6,
        )

    def test_service_deltas_preserve_classes_and_allow_correction_gauges_to_fall(self):
        from load_clients import SERVICE_CLASSES, SERVICE_COUNTERS, service_diagnostics

        before = self.service_stats()
        before["debt_slot_ns"][0] = 2_000_000
        after = self.service_stats()
        for field in SERVICE_COUNTERS:
            after[field] = [101 + index for index in range(6)]
        after["rebate_slot_ns"][0] = 500_000
        delta = service_diagnostics({"service": before}, {"service": after})
        self.assertEqual(list(SERVICE_CLASSES), delta["classes"])
        for field in SERVICE_COUNTERS:
            self.assertEqual([1, 2, 3, 4, 5, 6], delta["counters"][field])
        self.assertEqual(0, delta["debt_slot_ns"][0])
        self.assertEqual(500_000, delta["rebate_slot_ns"][0])

    def test_service_deltas_reject_resets_malformed_contracts_and_policy_changes(self):
        from load_clients import SERVICE_COUNTERS, service_diagnostics

        mutations = [
            ("version", 2),
            ("struct_size", 400),
            ("measurement_model", 2),
            ("available", False),
            ("available", 1),
            ("quantum_ns", 2_000_000),
            ("quantum_ns", 0),
            ("quantum_ns", 100_000_001),
            ("debt_slot_ns", [64_000_001] + [0] * 5),
            ("rebate_slot_ns", [True] + [0] * 5),
        ]
        for field in SERVICE_COUNTERS:
            mutations.extend(
                (
                    (field, [99] + [100] * 5),
                    (field, [100] * 5),
                    (field, [-1] * 6),
                    (field, [2**64] * 6),
                )
            )
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                before = self.service_stats()
                after = dict(before, **{field: value})
                with self.assertRaises(RuntimeError):
                    service_diagnostics({"service": before}, {"service": after})
        after = self.service_stats()
        after["debt_slot_ns"][0] = after["rebate_slot_ns"][0] = 1
        with self.assertRaises(RuntimeError):
            service_diagnostics({"service": self.service_stats()}, {"service": after})
        with self.assertRaises(RuntimeError):
            service_diagnostics({"service": self.service_stats()}, {})

    def test_absent_service_is_explicit_and_ignores_unavailable_payload(self):
        from load_clients import service_diagnostics

        self.assertEqual({"available": False}, service_diagnostics({}, {}))
        unavailable = dict(self.service_stats(), available=False, observed_slot_ns=None)
        self.assertEqual(
            {"available": False},
            service_diagnostics({"service": unavailable}, {"service": unavailable}),
        )

    def test_service_gate_requires_observation_not_grants_or_estimates(self):
        from load_clients import runnable_evidence_failures, service_diagnostics

        measured = self.measured()
        policy = self.manifest()["runnable_policy"]
        self.assertIn(
            "runnable service observations were not measured",
            runnable_evidence_failures(measured, policy, 1, 10),
        )
        before, after = self.service_stats(), self.service_stats()
        after["estimated_slot_ns"][0] += 1000
        delta = service_diagnostics({"service": before}, {"service": after})
        measured["runnable_diagnostics"]["service"] = delta
        self.assertIn(
            "runnable observed service was below the declared minimum",
            runnable_evidence_failures(measured, policy, 1, 10),
        )
        after["observed_slot_ns"][0] += 10
        measured["runnable_diagnostics"]["service"] = service_diagnostics(
            {"service": before},
            {"service": after},
        )
        self.assertEqual([], runnable_evidence_failures(measured, policy, 1, 10))

    def test_comparison_recomputes_declared_service_gate_from_snapshots(self):
        from load_clients import service_diagnostics

        baseline = QualificationTests().receipt()
        baseline["binary_sha256"] = "baseline"
        candidate = copy.deepcopy(baseline)
        candidate.update(binary_sha256="candidate", policy="runnable")
        for phase in candidate["manifest"]["phases"]:
            phase["thresholds"] = {"runnable_observed_slot_ns_min": 1}
        baseline["manifest"] = copy.deepcopy(candidate["manifest"])
        for phase in candidate["phases"]:
            before, after = self.service_stats(), self.service_stats()
            after["observed_slot_ns"][0] += 1
            phase["workload_before"] = {"runnable": {"service": before}}
            phase["workload_after"] = {"runnable": {"service": after}}
            phase["runnable_diagnostics"] = {
                "service": service_diagnostics(
                    phase["workload_before"]["runnable"],
                    phase["workload_after"]["runnable"],
                )
            }
        self.assertTrue(compare(baseline, candidate)["passed"])
        for change in ("missing", "unavailable", "reset", "zero", "forged"):
            changed = copy.deepcopy(candidate)
            phase = changed["phases"][0]
            if change == "missing":
                phase.pop("workload_before")
            elif change == "unavailable":
                phase["workload_before"]["runnable"]["service"]["available"] = False
                phase["workload_after"]["runnable"]["service"]["available"] = False
            elif change == "reset":
                phase["workload_after"]["runnable"]["service"]["observed_slot_ns"][
                    0
                ] = 99
            elif change == "zero":
                phase["workload_after"] = copy.deepcopy(phase["workload_before"])
            else:
                phase["runnable_diagnostics"]["service"]["counters"][
                    "observed_slot_ns"
                ][0] = 999
            with self.subTest(change=change):
                self.assertFalse(compare(baseline, changed)["passed"])

    def test_runnable_snapshot_rejects_disabled_and_missing_scheduler(self):
        with patch.dict("os.environ", ANTFLY_QUALIFICATION_POLICY="runnable"):
            for runnable in (
                {},
                {"enabled": False},
                {"enabled": True, "capacity_slots": 0},
            ):
                with patch(
                    "load_clients.operator_request",
                    return_value=(
                        200,
                        {"query": {"sql_waiting_enabled": True}, "runnable": runnable},
                    ),
                ):
                    with self.assertRaisesRegex(RuntimeError, "telemetry"):
                        workload_snapshot(None, require_waiting=True)

    def test_comparison_rejects_changes_to_admission_and_memory(self):
        from run_local import configuration_for_policy

        baseline, candidate = (
            QualificationTests().receipt(),
            QualificationTests().receipt(),
        )
        manifest = dict(candidate["manifest"], **self.manifest())
        for receipt, policy in ((baseline, "fixed"), (candidate, "runnable")):
            receipt["policy"] = policy
            receipt["manifest"] = manifest
            receipt["configuration"] = configuration_for_policy(manifest, policy)
            receipt["configuration"]["pgwire"] = {
                "enabled": True,
                "bind_port": 5432,
                "max_connections": 32,
            }
        self.assertTrue(compare(baseline, candidate)["passed"])
        for field, value in (
            ("max_request_memory_bytes", 1),
            ("query", {"max_concurrent_requests": 5}),
        ):
            changed = copy.deepcopy(candidate)
            changed["configuration"]["admission"][field] = value
            self.assertFalse(compare(baseline, changed)["passed"])

    def test_failfast_runnable_comparison_verifies_both_complete_policies(self):
        from run_local import configuration_for_policy

        baseline, candidate = (
            QualificationTests().receipt(),
            QualificationTests().receipt(),
        )
        manifest = dict(candidate["manifest"], **self.manifest())
        for receipt, policy in ((baseline, "failfast"), (candidate, "runnable")):
            receipt["policy"] = policy
            receipt["manifest"] = manifest
            receipt["configuration"] = configuration_for_policy(manifest, policy)
            receipt["configuration"]["pgwire"] = {
                "enabled": True,
                "bind_port": 5432,
                "max_connections": 32,
            }
        self.assertTrue(compare(baseline, candidate)["passed"])
        for index, original in enumerate((baseline, candidate)):
            for change in ("waiting", "memory", "capacity"):
                changed = copy.deepcopy(original)
                admission = changed["configuration"]["admission"]
                if change == "waiting":
                    admission["query"]["waiting"] = {"max_wait_ms": 999}
                elif change == "memory":
                    admission["max_request_memory_bytes"] = 1
                else:
                    admission["query"]["max_concurrent_requests"] = 99
                pair = (changed, candidate) if index == 0 else (baseline, changed)
                self.assertFalse(compare(*pair)["passed"], (index, change))


class ConcurrencySweepTests(unittest.TestCase):
    def test_sweep_preserves_limits_and_declares_per_worker_pressure(self):
        import json
        import pathlib
        from run_sweep import sweep_manifest, CONCURRENCIES

        manifest = json.loads(
            pathlib.Path(__file__).with_name("linux-runnable-pressure.json").read_text()
        )
        original = copy.deepcopy(manifest)
        self.assertEqual((1, 5, 10, 20, 30, 40, 60, 80, 160), CONCURRENCIES)
        for concurrency in CONCURRENCIES:
            point = sweep_manifest(manifest, concurrency)
            self.assertEqual(concurrency, point["client_workers"])
            for field in (
                "process_limits",
                "container_limits",
                "max_pending_clients",
                "retained_pressure",
                "configuration",
                "candidate_configuration",
                "runnable_policy",
            ):
                self.assertEqual(manifest[field], point[field])
            for before, after in zip(manifest["phases"], point["phases"]):
                self.assertEqual(before["thresholds"], after["thresholds"])
                ratio = (
                    1
                    if before.get("workload") == "cheap"
                    else concurrency / manifest["client_workers"]
                )
                self.assertEqual(
                    before["offered_per_second"] * ratio, after["offered_per_second"]
                )
        self.assertEqual(original, manifest)
        for concurrency in (0, -1, 257, True):
            with self.assertRaises(ValueError):
                sweep_manifest(manifest, concurrency)

    def test_offered_write_starvation_cannot_pass_with_query_success(self):
        from load_clients import runnable_evidence_failures

        measured = RunnableQualificationTests().measured()
        measured["by_kind"]["write"].update(offered=10, successful=0)
        measured["runnable_diagnostics"]["grants"][1] = 0
        failures = runnable_evidence_failures(
            measured, {"slots": 4, "cleanup_slots": 1}
        )
        self.assertIn("offered write work completed no successful operation", failures)

    def test_client_activity_records_peak_and_exact_retirement(self):
        from load_clients import ClientActivity
        import threading

        activity = ClientActivity()
        ready = threading.Barrier(5)
        release = threading.Event()

        def worker():
            activity.start()
            ready.wait()
            release.wait()
            activity.finish()

        workers = [threading.Thread(target=worker) for _ in range(4)]
        for worker in workers:
            worker.start()
        ready.wait(timeout=5)
        try:
            self.assertEqual((4, 4), activity.snapshot())
        finally:
            release.set()
            for worker in workers:
                worker.join(timeout=5)
        self.assertEqual((0, 4), activity.snapshot())
        with self.assertRaises(RuntimeError):
            activity.finish()

    def test_preparation_records_every_point_without_claiming_measurement(self):
        import json
        import pathlib
        import tempfile
        import run_sweep

        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "sweep"
            manifest = pathlib.Path(__file__).with_name("linux-runnable-pressure.json")
            with patch(
                "sys.argv",
                [
                    "run_sweep",
                    "--manifest",
                    str(manifest),
                    "--output-dir",
                    str(output),
                    "--prepare-only",
                ],
            ):
                self.assertEqual(0, run_sweep.main())
            summary = json.loads((output / "sweep.json").read_text())
            self.assertEqual(9, len(summary["points"]))
            self.assertFalse(summary["passed"])
            self.assertFalse(summary["measured"])
            self.assertFalse(summary["automatic_defaults_qualified"])
            self.assertTrue(
                all(point["state"] == "prepared" for point in summary["points"])
            )

    def test_missing_or_failed_matrix_cannot_pass_and_does_not_skip_later_points(self):
        import json
        import pathlib
        import tempfile
        import run_sweep
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "sweep"
            manifest = pathlib.Path(__file__).with_name("linux-runnable-pressure.json")
            calls = []

            def run(argv):
                calls.append(argv)
                target = pathlib.Path(argv[argv.index("--output-dir") + 1])
                if len(calls) != 1:
                    target.mkdir()
                    (target / "matrix.json").write_text(
                        json.dumps({"passed": len(calls) != 2})
                    )
                return SimpleNamespace(returncode=0)

            with (
                patch(
                    "sys.argv",
                    [
                        "run_sweep",
                        "--manifest",
                        str(manifest),
                        "--output-dir",
                        str(output),
                        "--candidate",
                        "/candidate",
                        "--baseline",
                        "/main",
                    ],
                ),
                patch("run_sweep.subprocess.run", side_effect=run),
            ):
                self.assertEqual(1, run_sweep.main())
            summary = json.loads((output / "sweep.json").read_text())
            self.assertEqual(9, len(calls))
            self.assertFalse(summary["passed"])
            self.assertFalse(summary["measured"])
            self.assertFalse(summary["automatic_defaults_qualified"])
            self.assertTrue(
                all(point["state"] == "terminal" for point in summary["points"])
            )
            self.assertFalse(summary["points"][0]["passed"])
            self.assertFalse(summary["points"][1]["passed"])
            self.assertTrue(summary["points"][-1]["passed"])


if __name__ == "__main__":
    unittest.main()
