// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dependency-free workload-profile schema and weighted tuning-policy replay.
//!
//! The same verifier runs before canonical package export and while verifying
//! or importing a package. Coverage is therefore derived from the captured
//! workload and exact qualified signatures; a serialized boolean is never
//! trusted as evidence by itself.

const std = @import("std");

pub const schema_name = "antfly.workload-profile.v2";
pub const maximum_profile_entries: usize = 256;
pub const tuning_regime_count: usize = 5;
pub const maximum_attempts_per_regime: usize = 5;
pub const maximum_tuning_signatures: u8 = maximum_attempts_per_regime * tuning_regime_count;
pub const selection_target_percent: u64 = 90;
pub const selection_minimum_percent: u64 = 2;

pub const ProfileEntry = struct {
    regime: []const u8,
    quant_format: []const u8,
    dispatch: []const u8,
    implementation: []const u8,
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    input_encoding: []const u8,
    output_encoding: []const u8,
    epilogue: []const u8,
    fusion: []const u8,
    layout: []const u8,
    call_count: u64,
    logical_bytes: u64,
    macs: u64,
    estimated_gpu_nanos: ?u64 = null,
    calibrated: bool = false,
};

pub const QualifiedKernel = struct {
    artifact_key: []const u8,
    regime: []const u8,
    quant_format: []const u8,
    dispatch: []const u8,
    implementation: []const u8,
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    input_encoding: []const u8,
    output_encoding: []const u8,
    epilogue: []const u8,
    fusion: []const u8,
    layout: []const u8,
    candidate_index: u8,
    threads_per_threadgroup: u16,
    rows_per_threadgroup: u8 = 1,
    cols_per_threadgroup: u8,
    reduction: []const u8,
    measured_speedup: f64,
    minimum_repeat_speedup: f64,
};

pub const ProfileReport = struct {
    schema: []const u8 = schema_name,
    complete: bool,
    dropped_signatures: u64 = 0,
    dropped_dispatches: u64 = 0,
    device_registry_id: u64,
    whole_frame_gpu_nanos: u64 = 0,
    profiled_frame_count: u64 = 0,
    compiler_fingerprint: []const u8,
    model_fingerprint: []const u8,
    requested_tokens: ?u64 = null,
    effective_tokens: ?u64 = null,
    batch: ?u32 = null,
    mtp_k: ?u32 = null,
    entries: []const ProfileEntry,
    qualified_kernels: []const QualifiedKernel = &.{},
    /// Number attempted by the deterministic weighted policy.
    tuning_selected_signatures: u8 = 0,
    /// Attempted signatures with a persisted eligible winner.
    tuning_qualified_signatures: u8 = 0,
    /// Attempted signatures rejected by correctness or performance gates.
    tuning_failed_signatures: u8 = 0,
    tuning_coverage_complete: bool = false,
};

const regimes = [_][]const u8{
    "encoder",
    "prefill",
    "decode",
    "speculative_draft",
    "speculative_verify",
};

fn saturatingAdd(lhs: u64, rhs: u64) u64 {
    return if (std.math.maxInt(u64) - lhs < rhs) std.math.maxInt(u64) else lhs + rhs;
}

fn percentageCeil(total: u64, percent: u64) u64 {
    return @intCast((@as(u128, total) * percent + 99) / 100);
}

fn knownRegime(name: []const u8) bool {
    for (regimes) |regime| {
        if (std.mem.eql(u8, name, regime)) return true;
    }
    return false;
}

fn formatRank(name: []const u8) ?u8 {
    if (std.mem.eql(u8, name, "q4_0")) return 6;
    if (std.mem.eql(u8, name, "q4_k")) return 8;
    if (std.mem.eql(u8, name, "q6_k")) return 12;
    return null;
}

fn dispatchRank(name: []const u8) ?u8 {
    if (std.mem.eql(u8, name, "scalar")) return 0;
    if (std.mem.eql(u8, name, "small_batch")) return 2;
    return null;
}

fn eligible(entry: ProfileEntry) bool {
    const format = formatRank(entry.quant_format) orelse return false;
    const dispatch = dispatchRank(entry.dispatch) orelse return false;
    const matches_dispatch = (format == 6 and (dispatch == 0 or dispatch == 2)) or
        ((format == 8 or format == 12) and dispatch == 0);
    const rows_supported = if (format == 6)
        entry.rows >= 2 and entry.rows <= 8
    else
        entry.rows >= 2 and entry.rows <= 64;
    return knownRegime(entry.regime) and
        matches_dispatch and
        std.mem.eql(u8, entry.implementation, "bundled") and
        rows_supported and
        std.mem.eql(u8, entry.input_encoding, "f32") and
        std.mem.eql(u8, entry.output_encoding, "f32") and
        std.mem.eql(u8, entry.epilogue, "none") and
        std.mem.eql(u8, entry.fusion, "none") and
        std.mem.eql(u8, entry.layout, "row_major") and
        entry.calibrated and
        (entry.estimated_gpu_nanos orelse 0) != 0;
}

pub fn hasEligibleTuningWork(report: ProfileReport) bool {
    for (report.entries) |entry| {
        if (eligible(entry)) return true;
    }
    return false;
}

fn sameEntrySignature(lhs: ProfileEntry, rhs: ProfileEntry) bool {
    return std.mem.eql(u8, lhs.regime, rhs.regime) and
        std.mem.eql(u8, lhs.quant_format, rhs.quant_format) and
        std.mem.eql(u8, lhs.dispatch, rhs.dispatch) and
        std.mem.eql(u8, lhs.implementation, rhs.implementation) and
        lhs.rows == rhs.rows and
        lhs.in_dim == rhs.in_dim and
        lhs.out_dim == rhs.out_dim and
        std.mem.eql(u8, lhs.input_encoding, rhs.input_encoding) and
        std.mem.eql(u8, lhs.output_encoding, rhs.output_encoding) and
        std.mem.eql(u8, lhs.epilogue, rhs.epilogue) and
        std.mem.eql(u8, lhs.fusion, rhs.fusion) and
        std.mem.eql(u8, lhs.layout, rhs.layout);
}

fn qualifiedMatchesEntry(qualified: QualifiedKernel, entry: ProfileEntry) bool {
    return std.mem.eql(u8, qualified.regime, entry.regime) and
        std.mem.eql(u8, qualified.quant_format, entry.quant_format) and
        std.mem.eql(u8, qualified.dispatch, entry.dispatch) and
        std.mem.eql(u8, qualified.implementation, entry.implementation) and
        qualified.rows == entry.rows and
        qualified.in_dim == entry.in_dim and
        qualified.out_dim == entry.out_dim and
        std.mem.eql(u8, qualified.input_encoding, entry.input_encoding) and
        std.mem.eql(u8, qualified.output_encoding, entry.output_encoding) and
        std.mem.eql(u8, qualified.epilogue, entry.epilogue) and
        std.mem.eql(u8, qualified.fusion, entry.fusion) and
        std.mem.eql(u8, qualified.layout, entry.layout);
}

fn sameQualifiedSignature(lhs: QualifiedKernel, rhs: QualifiedKernel) bool {
    return std.mem.eql(u8, lhs.regime, rhs.regime) and
        std.mem.eql(u8, lhs.quant_format, rhs.quant_format) and
        std.mem.eql(u8, lhs.dispatch, rhs.dispatch) and
        std.mem.eql(u8, lhs.implementation, rhs.implementation) and
        lhs.rows == rhs.rows and
        lhs.in_dim == rhs.in_dim and
        lhs.out_dim == rhs.out_dim and
        std.mem.eql(u8, lhs.input_encoding, rhs.input_encoding) and
        std.mem.eql(u8, lhs.output_encoding, rhs.output_encoding) and
        std.mem.eql(u8, lhs.epilogue, rhs.epilogue) and
        std.mem.eql(u8, lhs.fusion, rhs.fusion) and
        std.mem.eql(u8, lhs.layout, rhs.layout);
}

fn validArtifactKey(value: []const u8) bool {
    if (value.len != 64) return false;
    for (value) |byte| switch (byte) {
        '0'...'9', 'a'...'f' => {},
        else => return false,
    };
    return true;
}

/// Full profile structure shared by canonical export and package
/// verify/import. Backend activation should never be the first place malformed
/// entry, schedule, or evidence metadata is discovered.
pub fn validateStructure(
    report: ProfileReport,
    minimum_speedup: f64,
    maximum_candidates: usize,
) !void {
    if (!std.mem.eql(u8, report.schema, schema_name)) return error.InvalidProfileSchema;
    if (report.complete and (report.dropped_signatures != 0 or report.dropped_dispatches != 0)) {
        return error.InconsistentProfileCompleteness;
    }
    if ((report.whole_frame_gpu_nanos == 0) != (report.profiled_frame_count == 0)) {
        return error.InvalidProfileFrameTiming;
    }
    if (report.batch == 0 or report.mtp_k == 0) return error.InvalidProfileMetadata;
    if (report.entries.len > maximum_profile_entries) return error.TooManyProfileEntries;
    for (report.entries, 0..) |entry, index| {
        if (entry.regime.len == 0 or
            entry.quant_format.len == 0 or
            entry.dispatch.len == 0 or
            entry.implementation.len == 0 or
            entry.input_encoding.len == 0 or
            entry.output_encoding.len == 0 or
            entry.epilogue.len == 0 or
            entry.fusion.len == 0 or
            entry.layout.len == 0 or
            entry.rows == 0 or
            entry.in_dim == 0 or
            entry.out_dim == 0 or
            entry.call_count == 0)
        {
            return error.InvalidProfileEntry;
        }
        if (entry.calibrated and (entry.estimated_gpu_nanos orelse 0) == 0) {
            return error.InvalidProfileCalibration;
        }
        for (report.entries[0..index]) |previous| {
            if (sameEntrySignature(previous, entry)) return error.DuplicateProfileEntry;
        }
    }
    for (report.qualified_kernels, 0..) |kernel, index| {
        if (!validArtifactKey(kernel.artifact_key) or
            kernel.regime.len == 0 or
            kernel.quant_format.len == 0 or
            kernel.dispatch.len == 0 or
            kernel.implementation.len == 0 or
            kernel.rows < 2 or kernel.rows > 64 or
            kernel.in_dim == 0 or
            kernel.out_dim == 0 or
            kernel.input_encoding.len == 0 or
            kernel.output_encoding.len == 0 or
            kernel.epilogue.len == 0 or
            kernel.fusion.len == 0 or
            kernel.layout.len == 0 or
            kernel.candidate_index >= maximum_candidates or
            kernel.threads_per_threadgroup == 0 or
            (kernel.rows_per_threadgroup != 1 and kernel.rows_per_threadgroup != 2 and
                kernel.rows_per_threadgroup != 4 and kernel.rows_per_threadgroup != 8 and
                kernel.rows_per_threadgroup != 40) or
            kernel.cols_per_threadgroup == 0 or
            kernel.reduction.len == 0 or
            !std.math.isFinite(kernel.measured_speedup) or
            !std.math.isFinite(kernel.minimum_repeat_speedup) or
            kernel.measured_speedup < minimum_speedup or
            kernel.minimum_repeat_speedup < minimum_speedup)
        {
            return error.InvalidQualifiedKernel;
        }
        for (report.qualified_kernels[0..index]) |previous| {
            if (std.mem.eql(u8, previous.artifact_key, kernel.artifact_key) or
                sameQualifiedSignature(previous, kernel))
            {
                return error.DuplicateQualifiedKernel;
            }
        }
    }
}

/// Mirrors the Metal collector's numeric format/dispatch ordering. String
/// ordering would put `small_batch` before `scalar` and silently change which
/// tied signature is inside the five-attempt prefix.
fn comesBefore(lhs: ProfileEntry, rhs: ProfileEntry) bool {
    const lhs_cost = lhs.estimated_gpu_nanos.?;
    const rhs_cost = rhs.estimated_gpu_nanos.?;
    if (lhs_cost != rhs_cost) return lhs_cost > rhs_cost;
    const lhs_format = formatRank(lhs.quant_format).?;
    const rhs_format = formatRank(rhs.quant_format).?;
    if (lhs_format != rhs_format) return lhs_format < rhs_format;
    const lhs_dispatch = dispatchRank(lhs.dispatch).?;
    const rhs_dispatch = dispatchRank(rhs.dispatch).?;
    if (lhs_dispatch != rhs_dispatch) return lhs_dispatch < rhs_dispatch;
    if (lhs.rows != rhs.rows) return lhs.rows < rhs.rows;
    if (lhs.in_dim != rhs.in_dim) return lhs.in_dim < rhs.in_dim;
    if (lhs.out_dim != rhs.out_dim) return lhs.out_dim < rhs.out_dim;
    return false;
}

fn qualifiedIndexForEntry(report: ProfileReport, entry: ProfileEntry) !?usize {
    var match: ?usize = null;
    for (report.qualified_kernels, 0..) |qualified, index| {
        if (!qualifiedMatchesEntry(qualified, entry)) continue;
        if (match != null) return error.DuplicateQualifiedKernelSignature;
        match = index;
    }
    return match;
}

/// Replays the complete deterministic attempt policy. Qualified signatures are
/// treated as successful attempts; every other member of the ranked prefix is
/// a rejection. The replay stops immediately at 90% qualified weight or after
/// five attempts, so out-of-prefix and post-target winners are rejected.
pub fn validateWeightedCoverage(report: ProfileReport) !void {
    if (!std.mem.eql(u8, report.schema, schema_name)) return error.InvalidProfileSchema;
    if (report.entries.len > maximum_profile_entries or
        report.qualified_kernels.len > maximum_tuning_signatures or
        report.tuning_selected_signatures > maximum_tuning_signatures or
        report.qualified_kernels.len != report.tuning_qualified_signatures or
        @as(u16, report.tuning_qualified_signatures) + report.tuning_failed_signatures !=
            report.tuning_selected_signatures)
    {
        return error.InvalidProfileTuningSummary;
    }

    for (report.entries, 0..) |entry, index| {
        for (report.entries[0..index]) |previous| {
            if (sameEntrySignature(previous, entry)) return error.DuplicateProfileEntry;
        }
    }
    for (report.qualified_kernels, 0..) |qualified, index| {
        for (report.qualified_kernels[0..index]) |previous| {
            if (std.mem.eql(u8, previous.artifact_key, qualified.artifact_key) or
                sameQualifiedSignature(previous, qualified))
            {
                return error.DuplicateQualifiedKernel;
            }
        }
    }

    const zero_tuning = report.tuning_selected_signatures == 0 and
        report.tuning_qualified_signatures == 0 and
        report.tuning_failed_signatures == 0 and
        report.qualified_kernels.len == 0;
    if (zero_tuning) {
        if (report.tuning_coverage_complete) return error.InvalidProfileTuningCoverage;
        return;
    }

    var matched: [maximum_tuning_signatures]bool = @splat(false);
    var derived_selected: u16 = 0;
    var derived_qualified: u16 = 0;
    var derived_failed: u16 = 0;
    var all_regimes_reached_target = true;
    var saw_eligible_regime = false;

    for (regimes) |regime| {
        var ordered: [maximum_profile_entries]u16 = undefined;
        var ordered_count: usize = 0;
        var eligible_nanos: u64 = 0;
        for (report.entries, 0..) |entry, index| {
            if (!eligible(entry) or !std.mem.eql(u8, entry.regime, regime)) continue;
            eligible_nanos = saturatingAdd(eligible_nanos, entry.estimated_gpu_nanos.?);
            var insertion = ordered_count;
            while (insertion > 0 and comesBefore(entry, report.entries[ordered[insertion - 1]])) : (insertion -= 1) {
                ordered[insertion] = ordered[insertion - 1];
            }
            ordered[insertion] = @intCast(index);
            ordered_count += 1;
        }
        if (eligible_nanos == 0) continue;
        saw_eligible_regime = true;

        const minimum_nanos = percentageCeil(eligible_nanos, selection_minimum_percent);
        const target_nanos = percentageCeil(eligible_nanos, selection_target_percent);
        var qualified_nanos: u64 = 0;
        var attempts: usize = 0;
        for (ordered[0..ordered_count]) |entry_index| {
            if (attempts == maximum_attempts_per_regime or qualified_nanos >= target_nanos) break;
            const entry = report.entries[entry_index];
            if (entry.estimated_gpu_nanos.? < minimum_nanos) continue;
            attempts += 1;
            derived_selected += 1;
            if (try qualifiedIndexForEntry(report, entry)) |qualified_index| {
                if (matched[qualified_index]) return error.DuplicateQualifiedKernelSignature;
                matched[qualified_index] = true;
                derived_qualified += 1;
                qualified_nanos = saturatingAdd(qualified_nanos, entry.estimated_gpu_nanos.?);
            } else {
                derived_failed += 1;
            }
        }
        if (qualified_nanos < target_nanos) all_regimes_reached_target = false;
    }

    for (matched[0..report.qualified_kernels.len]) |was_matched| {
        if (!was_matched) return error.InvalidQualifiedKernelSelection;
    }
    if (derived_selected != report.tuning_selected_signatures or
        derived_qualified != report.tuning_qualified_signatures or
        derived_failed != report.tuning_failed_signatures)
    {
        return error.InvalidProfileTuningSummary;
    }
    const coverage_complete = saw_eligible_regime and all_regimes_reached_target;
    if (coverage_complete != report.tuning_coverage_complete) {
        return error.InvalidProfileTuningCoverage;
    }
}

/// Package/canonical-profile validation is stricter than a census report: an
/// all-zero tuning summary may represent an empty bundle component, but it may
/// not discard calibrated eligible work.
pub fn validateQualifiedCoverage(report: ProfileReport) !void {
    try validateWeightedCoverage(report);
    const zero_tuning = report.tuning_selected_signatures == 0 and
        report.tuning_qualified_signatures == 0 and
        report.tuning_failed_signatures == 0 and
        report.qualified_kernels.len == 0;
    if (zero_tuning and hasEligibleTuningWork(report)) return error.MissingProfileTuningEvidence;
}

fn testEntry(cost: u64, out_dim: u32) ProfileEntry {
    return .{
        .regime = "encoder",
        .quant_format = "q4_0",
        .dispatch = "small_batch",
        .implementation = "bundled",
        .rows = 2,
        .in_dim = 256,
        .out_dim = out_dim,
        .input_encoding = "f32",
        .output_encoding = "f32",
        .epilogue = "none",
        .fusion = "none",
        .layout = "row_major",
        .call_count = 1,
        .logical_bytes = 1,
        .macs = 1,
        .estimated_gpu_nanos = cost,
        .calibrated = true,
    };
}

fn testQualified(entry: ProfileEntry, artifact_key: []const u8) QualifiedKernel {
    return .{
        .artifact_key = artifact_key,
        .regime = entry.regime,
        .quant_format = entry.quant_format,
        .dispatch = entry.dispatch,
        .implementation = entry.implementation,
        .rows = entry.rows,
        .in_dim = entry.in_dim,
        .out_dim = entry.out_dim,
        .input_encoding = entry.input_encoding,
        .output_encoding = entry.output_encoding,
        .epilogue = entry.epilogue,
        .fusion = entry.fusion,
        .layout = entry.layout,
        .candidate_index = 0,
        .threads_per_threadgroup = 32,
        .cols_per_threadgroup = 1,
        .reduction = "simd_sum",
        .measured_speedup = 1.03,
        .minimum_repeat_speedup = 1.02,
    };
}

test "weighted coverage accepts a fifth-attempt backfill after rank four rejects" {
    const entries = [_]ProfileEntry{
        testEntry(55, 500),
        testEntry(25, 501),
        testEntry(8, 502),
        testEntry(5, 503),
        testEntry(4, 504),
        testEntry(3, 505),
    };
    const qualified = [_]QualifiedKernel{
        testQualified(entries[0], "01"),
        testQualified(entries[1], "02"),
        testQualified(entries[2], "03"),
        testQualified(entries[4], "05"),
    };
    try validateWeightedCoverage(.{
        .complete = true,
        .device_registry_id = 1,
        .compiler_fingerprint = "compiler",
        .model_fingerprint = "model",
        .entries = &entries,
        .qualified_kernels = &qualified,
        .tuning_selected_signatures = 5,
        .tuning_qualified_signatures = 4,
        .tuning_failed_signatures = 1,
        .tuning_coverage_complete = true,
    });
}

test "weighted coverage validates incomplete diagnostic reports but rejects forged completion" {
    const entries = [_]ProfileEntry{
        testEntry(50, 500),
        testEntry(20, 501),
        testEntry(9, 502),
        testEntry(8, 503),
        testEntry(7, 504),
        testEntry(6, 505),
    };
    const qualified = [_]QualifiedKernel{
        testQualified(entries[0], "01"),
        testQualified(entries[1], "02"),
        testQualified(entries[2], "03"),
    };
    var report = ProfileReport{
        .complete = true,
        .device_registry_id = 1,
        .compiler_fingerprint = "compiler",
        .model_fingerprint = "model",
        .entries = &entries,
        .qualified_kernels = &qualified,
        .tuning_selected_signatures = 5,
        .tuning_qualified_signatures = 3,
        .tuning_failed_signatures = 2,
        .tuning_coverage_complete = false,
    };
    try validateWeightedCoverage(report);
    report.tuning_coverage_complete = true;
    try std.testing.expectError(error.InvalidProfileTuningCoverage, validateWeightedCoverage(report));
}

test "weighted coverage requires every eligible regime to reach its target" {
    var verify_entries = [_]ProfileEntry{
        testEntry(50, 500),
        testEntry(20, 501),
        testEntry(9, 502),
        testEntry(8, 503),
        testEntry(7, 504),
        testEntry(6, 505),
    };
    for (&verify_entries) |*entry| entry.regime = "speculative_verify";
    const encoder = testEntry(100, 600);
    const entries = [_]ProfileEntry{
        encoder,
        verify_entries[0],
        verify_entries[1],
        verify_entries[2],
        verify_entries[3],
        verify_entries[4],
        verify_entries[5],
    };
    const qualified = [_]QualifiedKernel{
        testQualified(entries[0], "00"),
        testQualified(entries[1], "01"),
        testQualified(entries[2], "02"),
        testQualified(entries[3], "03"),
    };
    var report = ProfileReport{
        .complete = true,
        .device_registry_id = 1,
        .compiler_fingerprint = "compiler",
        .model_fingerprint = "model",
        .entries = &entries,
        .qualified_kernels = &qualified,
        .tuning_selected_signatures = 6,
        .tuning_qualified_signatures = 4,
        .tuning_failed_signatures = 2,
        .tuning_coverage_complete = false,
    };
    try validateWeightedCoverage(report);
    report.tuning_coverage_complete = true;
    try std.testing.expectError(error.InvalidProfileTuningCoverage, validateWeightedCoverage(report));
}

test "weighted coverage rounds the ninety percent target upward" {
    const entries = [_]ProfileEntry{
        testEntry(80, 500),
        testEntry(21, 501),
    };
    const qualified = [_]QualifiedKernel{testQualified(entries[0], "01")};
    var report = ProfileReport{
        .complete = true,
        .device_registry_id = 1,
        .compiler_fingerprint = "compiler",
        .model_fingerprint = "model",
        .entries = &entries,
        .qualified_kernels = &qualified,
        .tuning_selected_signatures = 2,
        .tuning_qualified_signatures = 1,
        .tuning_failed_signatures = 1,
        .tuning_coverage_complete = false,
    };
    try validateWeightedCoverage(report);
    report.tuning_coverage_complete = true;
    try std.testing.expectError(error.InvalidProfileTuningCoverage, validateWeightedCoverage(report));
}

test "weighted coverage tie order uses Metal format and dispatch ids" {
    var q4_0_scalar = testEntry(10, 500);
    q4_0_scalar.dispatch = "scalar";
    var q4_0_small_batch = q4_0_scalar;
    q4_0_small_batch.dispatch = "small_batch";
    var q4_k_scalar = q4_0_scalar;
    q4_k_scalar.quant_format = "q4_k";
    try std.testing.expect(comesBefore(q4_0_scalar, q4_0_small_batch));
    try std.testing.expect(comesBefore(q4_0_small_batch, q4_k_scalar));
}

test "weighted coverage rejects post-target and legacy claims" {
    const entries = [_]ProfileEntry{
        testEntry(90, 500),
        testEntry(10, 501),
    };
    const qualified = [_]QualifiedKernel{
        testQualified(entries[0], "01"),
        testQualified(entries[1], "02"),
    };
    var report = ProfileReport{
        .complete = true,
        .device_registry_id = 1,
        .compiler_fingerprint = "compiler",
        .model_fingerprint = "model",
        .entries = &entries,
        .qualified_kernels = &qualified,
        .tuning_selected_signatures = 2,
        .tuning_qualified_signatures = 2,
        .tuning_coverage_complete = true,
    };
    try std.testing.expectError(error.InvalidQualifiedKernelSelection, validateWeightedCoverage(report));
    report.schema = "antfly.workload-profile.v1";
    try std.testing.expectError(error.InvalidProfileSchema, validateWeightedCoverage(report));
}

test "weighted coverage permits a census but strict qualification requires tuning" {
    const entries = [_]ProfileEntry{testEntry(100, 500)};
    const report = ProfileReport{
        .complete = true,
        .device_registry_id = 1,
        .compiler_fingerprint = "compiler",
        .model_fingerprint = "model",
        .entries = &entries,
    };
    try validateWeightedCoverage(report);
    try std.testing.expectError(error.MissingProfileTuningEvidence, validateQualifiedCoverage(report));

    var noneligible = entries[0];
    noneligible.calibrated = false;
    noneligible.estimated_gpu_nanos = null;
    const empty_entries = [_]ProfileEntry{noneligible};
    var empty = report;
    empty.entries = &empty_entries;
    try validateQualifiedCoverage(empty);
}
