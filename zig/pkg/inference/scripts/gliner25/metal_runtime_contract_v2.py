"""Additive runtime receipts for the GLiNER2.5 FP32 comparison.

No model or GPU imports. A legacy receipt is accepted only by an explicitly
selected reference policy; optimized execution cannot downgrade its proof.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any

import benchmark_metal as v1


SCOPE = "gliner25_direct_core_metal_comparison_fp32_v2"
VERSION = 2
POLICIES = {
    "reference_v1": {
        "weight_residency": "request",
        "relative_projection_residency": "request",
        "submission": "synchronous_unframed",
        "required_outputs": "all_enabled",
    },
    "optimized_v2": {
        "weight_residency": "model",
        "relative_projection_residency": "model",
        "submission": "request_owned",
        "required_outputs": "schema_required",
    },
}
COUNTERS = (
    "actual_weight_upload_bytes", "scope_submissions",
    "peak_pending_device_bytes", "pending_device_bytes",
)
OPTIONAL_TIMINGS = ("scope_wait_nanos", "scope_gpu_nanos")
PHASE_TIMINGS = ("schema", "processor", "backend_setup", "device", "backend_cleanup")
MAX_COUNTER = (1 << 64) - 1


def integer(value: Any, where: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_COUNTER:
        raise v1.BenchmarkError(f"{where}: expected a nonnegative u64 counter")
    return value


def mapping(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise v1.BenchmarkError(f"{where}: expected an object")
    return value


def policy_matches(value: Any, execution_policy: str) -> bool:
    return isinstance(value, dict) and all(value.get(key) == expected
        for key, expected in POLICIES[execution_policy].items())


@dataclass
class NativeReceipt:
    execution_policy: str
    runtime_contract_version: int
    model_live_bytes: int | None
    workspace_capacity_bytes: int = 0
    workspace_live_bytes: int = 0
    workspace_generation: int = 0
    workspace_sealed: bool = False

    @property
    def legacy(self) -> bool:
        return self.runtime_contract_version == 1

    def check_identity(self, event: dict[str, Any]) -> None:
        if self.legacy:
            if any(key in event for key in ("runtime_contract_version", "execution_policy", "runtime_policy")):
                raise v1.BenchmarkError("legacy runtime changed receipt policy after ready")
            return
        if (type(event.get("runtime_contract_version")) is not int
                or event["runtime_contract_version"] != VERSION
                or event.get("execution_policy") != self.execution_policy):
            raise v1.BenchmarkError("native runtime receipt changed after ready")
        if "runtime_policy" in event and not policy_matches(event["runtime_policy"], self.execution_policy):
            raise v1.BenchmarkError("native semantic policy changed after ready")

    def seal_workspace(self) -> None:
        self.workspace_sealed = True

    def result(self, response: dict[str, Any], expected: dict[str, Any], *, validation: bool,
               phase: str | None = None) -> dict[str, Any]:
        if self.legacy:
            self.check_identity(response)
            return v1.checked_result(response, expected, v1.NATIVE, validation=validation)
        self.observe_ownership(response, validation=validation, phase=phase)
        actual = v1.cpu.canonical_result(response["output"])
        v1.cpu.require_equal(expected, actual, v1.NATIVE)
        return actual

    def observe_ownership(self, response: dict[str, Any], *, validation: bool,
                          phase: str | None = None) -> None:
        """Advance valid owner receipts even when a case's output is rejected.

        Invalid ownership never advances the tracker. Numerical validation is
        separate so a recoverable case failure cannot stale the next request's
        workspace baseline.
        """
        self.check_identity(response)
        if self.legacy:
            raise v1.BenchmarkError("legacy requests have no v2 ownership receipt")
        phase = phase or ("validation" if validation else "measurement")
        if phase not in ("validation", "warmup", "measurement", "diagnostic"):
            raise v1.BenchmarkError("unknown request phase")
        if response.get("event") == "error":
            raise v1.BenchmarkError(f"worker error: {response.get('message', '')}")
        if integer(response.get("duration_ns"), "duration_ns") == 0:
            raise v1.BenchmarkError("native request has no completed duration")
        if response.get("gpu_work_submitted") is not True or response.get("external_frame") is not False:
            raise v1.BenchmarkError("native request lacks strict owned GPU execution")
        dispatches = mapping(mapping(response.get("request_stats"), "request_stats").get("encoder"), "encoder")
        if integer(dispatches.get("device_dispatches"), "encoder.device_dispatches") == 0:
            raise v1.BenchmarkError("native request lacks encoder dispatches")
        fallback = mapping(response.get("host_fallback_evidence"), "host_fallback_evidence")
        if fallback.get("strict_device_dispatch") is not True:
            raise v1.BenchmarkError("native request lacks strict device dispatch")
        for key in ("host_mirror_allocations_delta", "host_mirror_download_bytes_delta", "to_host_device_calls_delta"):
            if integer(fallback.get(key), key) != 0:
                raise v1.BenchmarkError("native request used a host tensor fallback")
        if validation:
            ids = response.get("input_ids")
            if (not isinstance(ids, list) or not 1 <= len(ids) <= v1.oracle.MAX_ENCODED_TOKENS
                    or any(type(token) is not int or not 0 <= token <= (1 << 32) - 1 for token in ids)):
                raise v1.BenchmarkError("native validation has invalid encoder tokens")
        memory = mapping(response.get("owned_memory"), "owned_memory")
        model = mapping(memory.get("model"), "owned_memory.model")
        transient = mapping(memory.get("transient"), "owned_memory.transient")
        for position in ("before_live_bytes", "after_live_bytes"):
            if integer(model.get(position), f"model.{position}") != self.model_live_bytes:
                raise v1.BenchmarkError("model-owned residency changed after common preparation")
            if integer(transient.get(position), f"transient.{position}") != 0:
                raise v1.BenchmarkError("request boundary retains transient device memory")
        if integer(transient.get("pending_after_bytes"), "transient.pending_after_bytes") != 0:
            raise v1.BenchmarkError("request returned before pending device ownership completed")
        workspace = mapping(memory.get("workspace", {
            "before_live_bytes": 0, "after_live_bytes": 0, "generation": 0}), "owned_memory.workspace")
        wb = integer(workspace.get("before_live_bytes"), "workspace.before_live_bytes")
        wa = integer(workspace.get("after_live_bytes"), "workspace.after_live_bytes")
        generation = integer(workspace.get("generation"), "workspace.generation")
        if (wb != self.workspace_live_bytes or not wb <= wa <= self.workspace_capacity_bytes
                or generation < self.workspace_generation
                or (wa != wb and generation == self.workspace_generation)):
            raise v1.BenchmarkError("workspace growth is discontinuous, unversioned or over its declared bound")
        if self.workspace_sealed or phase == "measurement":
            if wa != wb or generation != self.workspace_generation:
                raise v1.BenchmarkError("workspace changed after the full warm matrix")
        # The aggregate physical counter must include both owners. Otherwise an
        # invented zero transient receipt could hide an unclassified allocation.
        for position, workspace_bytes in (("before", wb), ("after", wa)):
            snapshot = mapping(memory.get(position), f"owned_memory.{position}")
            if integer(snapshot.get("device_owned_live_bytes"), f"{position}.device_owned_live_bytes") != self.model_live_bytes + workspace_bytes:
                raise v1.BenchmarkError("aggregate device residency disagrees with owner receipts")
            if integer(snapshot.get("host_mirror_live_bytes"), f"{position}.host_mirror_live_bytes") != 0:
                raise v1.BenchmarkError("native request retained a host mirror")
        before, after = memory["before"], memory["after"]
        for created, released, net in (
            ("device_owned_bytes_created", "device_owned_bytes_released", wa - wb),
            ("device_owned_buffers_created", "device_owned_buffers_released", None),
            ("host_mirror_allocations", "host_mirror_frees", 0),
        ):
            deltas = []
            for key in (created, released):
                delta = integer(after.get(key), f"after.{key}") - integer(before.get(key), f"before.{key}")
                if delta < 0:
                    raise v1.BenchmarkError("native cumulative allocation counters moved backwards")
                deltas.append(delta)
            observed = deltas[0] - deltas[1]
            if (net is not None and observed != net) or (net is None and wa == wb and observed != 0):
                raise v1.BenchmarkError("native allocation counters do not reconcile with owner changes")
        stats = mapping(response.get("runtime_stats"), "runtime_stats")
        checked = {key: integer(stats.get(key), f"runtime_stats.{key}") for key in COUNTERS}
        for key in OPTIONAL_TIMINGS:
            if key in stats and stats[key] is not None:
                integer(stats[key], f"runtime_stats.{key}")
        if stats.get("phase_timings_ns") is not None:
            timings = mapping(stats["phase_timings_ns"], "runtime_stats.phase_timings_ns")
            for key in PHASE_TIMINGS:
                integer(timings.get(key), f"runtime_stats.phase_timings_ns.{key}")
            if phase in ("warmup", "measurement"):
                raise v1.BenchmarkError("instrumented phases cannot enter latency acceptance")
        if checked["pending_device_bytes"] != 0:
            raise v1.BenchmarkError("runtime has pending device bytes after extraction")
        workspace_new = self.execution_policy == "optimized_v2" and self.workspace_capacity_bytes > 0
        if integer(stats.get("workspace_pending_bytes", None if workspace_new else 0), "runtime_stats.workspace_pending_bytes") != 0:
            raise v1.BenchmarkError("runtime has pending workspace bytes after extraction")
        if "workspace_peak_pending_bytes" in stats or workspace_new:
            peak = integer(stats.get("workspace_peak_pending_bytes"), "runtime_stats.workspace_peak_pending_bytes")
            if peak > self.workspace_capacity_bytes:
                raise v1.BenchmarkError("workspace pending peak exceeded its admitted capacity")
        if integer(stats.get("workspace_live_bytes", 0), "runtime_stats.workspace_live_bytes") != wa:
            raise v1.BenchmarkError("workspace runtime counter disagrees with owned memory")
        if self.execution_policy == "optimized_v2":
            if checked["actual_weight_upload_bytes"] != 0:
                raise v1.BenchmarkError("optimized request uploaded immutable weights")
            if checked["scope_submissions"] == 0:
                raise v1.BenchmarkError("optimized request has no owned submission evidence")
        self.workspace_live_bytes, self.workspace_generation = wa, generation

    def stopped(self, event: dict[str, Any]) -> dict[str, Any]:
        if event.get("event") != "stopped":
            raise v1.BenchmarkError("native cleanup did not acknowledge stop")
        self.check_identity(event)
        if self.legacy:
            return {"runtime_contract_version": 1, "final_owned_cleanup_proved": False}
        for key in ("model_live_bytes", "transient_live_bytes", "pending_device_bytes"):
            if integer(event.get(key), f"stopped.{key}") != 0:
                raise v1.BenchmarkError("native stopped before all model/request/device ownership was released")
        if integer(event.get("workspace_live_bytes", 0), "stopped.workspace_live_bytes") != 0:
            raise v1.BenchmarkError("native stopped before its warm workspace was released")
        return {"runtime_contract_version": VERSION, "final_owned_cleanup_proved": True}


def native_ready(ready: dict[str, Any], bundle: dict[str, Any], variant: str,
                 case_path: Path, execution_policy: str) -> NativeReceipt:
    if execution_policy not in POLICIES:
        raise v1.BenchmarkError("unknown native execution policy")
    scope, version = ready.get("scope"), ready.get("runtime_contract_version")
    if scope == v1.SCOPE and version is None:
        if execution_policy != "reference_v1":
            raise v1.BenchmarkError("optimized execution cannot accept a v1 runtime receipt")
        if "execution_policy" in ready or "runtime_policy" in ready:
            raise v1.BenchmarkError("legacy readiness contains ambiguous v2 policy fields")
        v1.checked_ready(v1.NATIVE, ready, bundle, variant, case_path)
        return NativeReceipt(execution_policy, 1, None)
    if (scope != SCOPE or type(version) is not int or version != VERSION
            or ready.get("execution_policy") != execution_policy
            or not policy_matches(ready.get("runtime_policy"), execution_policy)):
        raise v1.BenchmarkError("native v2 readiness policy/version differs")
    # Normalize only the wire envelope for the frozen v1 semantic checks.
    v1.checked_ready(v1.NATIVE, {**ready, "scope": v1.SCOPE}, bundle, variant, case_path)
    model_bytes = integer(ready.get("model_live_bytes"), "ready.model_live_bytes")
    if (execution_policy == "reference_v1" and model_bytes != 0) or (
            execution_policy == "optimized_v2" and model_bytes == 0):
        raise v1.BenchmarkError("ready residency disagrees with the selected execution policy")
    capacity = integer(ready.get("workspace_capacity_bytes", 0), "ready.workspace_capacity_bytes")
    workspace = integer(ready.get("workspace_live_bytes", 0), "ready.workspace_live_bytes")
    generation = integer(ready.get("workspace_generation", 0), "ready.workspace_generation")
    if workspace > capacity or (execution_policy == "reference_v1" and (capacity or workspace or generation)):
        raise v1.BenchmarkError("ready workspace differs from the execution policy or cap")
    return NativeReceipt(execution_policy, VERSION, model_bytes, capacity, workspace, generation)


def reference_ready(arm: str, ready: dict[str, Any], bundle: dict[str, Any],
                    variant: str, case_path: Path, execution_policy: str) -> None:
    if ready.get("scope") != SCOPE and not (execution_policy == "reference_v1" and ready.get("scope") == v1.SCOPE):
        raise v1.BenchmarkError("Python reference uses a different comparison scope")
    v1.checked_ready(arm, {**ready, "scope": v1.SCOPE}, bundle, variant, case_path)


def competitiveness(intervals: list[dict[str, Any]], *, numerator: str) -> dict[str, Any]:
    """Fixed per-repetition acceptance; no pooling independent host sessions."""
    if numerator not in ("python_over_metal", "native_over_python"):
        raise v1.BenchmarkError("unknown latency ratio direction")
    for interval in intervals:
        values = [interval.get(key) for key in ("lower_95", "median", "upper_95")]
        if (any(isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0 for value in values)
                or not values[0] <= values[1] <= values[2]):
            raise v1.BenchmarkError("invalid paired confidence interval")
    complete = len(intervals) == 3
    passed = complete and all(
        interval["lower_95"] >= 5 / 6 if numerator == "python_over_metal" else interval["upper_95"] < 1
        for interval in intervals)
    return {"complete_three_repetitions": complete, "passed": passed,
            "criterion": ("each_lower_95_python_over_metal_at_least_5_over_6" if numerator == "python_over_metal"
                          else "each_upper_95_native_over_python_strictly_below_1"),
            "serving_qualified": False, "performance_release_qualified": False}
