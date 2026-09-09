// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Automatic continuous, recovery, and final harness-health evidence.

const std = @import("std");
const ids = @import("id.zig");
const property = @import("property.zig");

pub const Phase = enum {
    /// Safety and resource evidence sampled while ordinary work or faults run.
    continuous,
    /// Faults have stopped or the scenario reports quiescence and recovery is
    /// expected to converge.
    recovery,
    /// The scenario reached its terminal state; leak and cleanup checks use
    /// this snapshot.
    final,
};

pub const Snapshot = struct {
    progress_expected: bool = false,
    progress_units: u64 = 0,
    active_tasks: ?u64 = null,
    expected_active_tasks: u64 = 0,
    open_descriptors: ?u64 = null,
    expected_open_descriptors: u64 = 0,
    allocator_exhausted: ?bool = null,
    storage_exhausted: ?bool = null,
    unexpected_crash: ?bool = null,
    recovery_expected: bool = false,
    recovery_complete: ?bool = null,
    consistency_valid: ?bool = null,
    cleanup_complete: ?bool = null,
};

/// Pointer-free aggregate retained on an in-memory Trace as diagnostic data.
/// It is deliberately excluded from canonical trace serialization so adding a
/// health adapter cannot change choices, replay, or failure fingerprints.
pub const Evidence = struct {
    samples: u64 = 0,
    continuous_samples: u64 = 0,
    recovery_samples: u64 = 0,
    final_samples: u64 = 0,
    progress_encountered: bool = false,
    no_progress_ok: bool = true,
    longest_progress_stall: u64 = 0,
    unexpected_crash_encountered: bool = false,
    no_unexpected_crash: bool = true,
    recovery_expected: bool = false,
    recovery_complete: bool = false,
    final_snapshot: ?Snapshot = null,
};

pub const Recorder = struct {
    /// Consecutive samples allowed while progress is expected. Scenarios with
    /// longer legitimate waits can override this value or set
    /// `progress_expected = false` while parked.
    no_progress_sample_budget: u64 = 8,
    evidence_value: Evidence = .{},
    last_progress_units: ?u64 = null,
    stalled_samples: u64 = 0,

    pub fn record(self: *Recorder, phase: Phase, snapshot: Snapshot) void {
        self.evidence_value.samples +|= 1;
        switch (phase) {
            .continuous => self.evidence_value.continuous_samples +|= 1,
            .recovery => self.evidence_value.recovery_samples +|= 1,
            .final => {
                self.evidence_value.final_samples +|= 1;
                self.evidence_value.final_snapshot = snapshot;
            },
        }

        if (snapshot.progress_expected) {
            self.evidence_value.progress_encountered = true;
            if (self.last_progress_units) |previous| {
                if (snapshot.progress_units > previous) {
                    self.stalled_samples = 0;
                } else if (phase != .final) {
                    self.stalled_samples +|= 1;
                    self.evidence_value.longest_progress_stall = @max(
                        self.evidence_value.longest_progress_stall,
                        self.stalled_samples,
                    );
                    if (self.stalled_samples > self.no_progress_sample_budget) {
                        self.evidence_value.no_progress_ok = false;
                    }
                }
            }
            self.last_progress_units = snapshot.progress_units;
        } else {
            self.stalled_samples = 0;
        }

        if (snapshot.unexpected_crash) |crashed| {
            self.evidence_value.unexpected_crash_encountered = true;
            self.evidence_value.no_unexpected_crash = self.evidence_value.no_unexpected_crash and !crashed;
        }
        if (snapshot.recovery_expected) self.evidence_value.recovery_expected = true;
        if (snapshot.recovery_complete == true) self.evidence_value.recovery_complete = true;
    }

    pub fn evidence(self: *const Recorder) Evidence {
        return self.evidence_value;
    }
};

pub const Check = struct {
    property_id: ids.StableId,
    name: []const u8,
    kind: property.Kind = .always_or_unreachable,
    encountered: bool,
    condition: bool,
    details: []const u8,
};

pub const check_count = 11;

/// Evaluate an explicitly supplied final snapshot when no sampled evidence is
/// available. This is a canonical input mode, not a legacy format adapter.
pub fn evaluate(history: anytype, snapshot: ?Snapshot) [check_count]Check {
    var evidence = Evidence{ .final_snapshot = snapshot };
    if (snapshot) |state| {
        evidence.samples = 1;
        evidence.final_samples = 1;
        evidence.progress_encountered = state.progress_expected;
        evidence.no_progress_ok = !state.progress_expected or state.progress_units > 0;
        evidence.unexpected_crash_encountered = state.unexpected_crash != null;
        evidence.no_unexpected_crash = state.unexpected_crash == null or !state.unexpected_crash.?;
        evidence.recovery_expected = state.recovery_expected;
        evidence.recovery_complete = state.recovery_complete == true;
    }
    return evaluateEvidence(history, evidence);
}

pub fn evaluateEvidence(history: anytype, evidence: Evidence) [check_count]Check {
    const state = evidence.final_snapshot orelse Snapshot{};
    var replay_ok = true;
    var harness_ok = true;
    var allocator_ok = state.allocator_exhausted == null or !state.allocator_exhausted.?;
    var allocator_encountered = state.allocator_exhausted != null;
    var crash_ok = evidence.no_unexpected_crash;
    var crash_encountered = evidence.unexpected_crash_encountered;
    for (history.failures.items) |failure| switch (failure.class) {
        .replay_divergence => replay_ok = false,
        .harness => harness_ok = false,
        .allocator => {
            allocator_encountered = true;
            allocator_ok = false;
        },
        .panic, .process => {
            crash_encountered = true;
            crash_ok = false;
        },
        else => {},
    };
    return .{
        check("vopr.health.no_progress", evidence.progress_encountered, evidence.no_progress_ok, "expected progress stalled beyond the configured sample budget"),
        check("vopr.health.unexpected_crash", crash_encountered, crash_ok, "scenario reported an unexpected crash"),
        check("vopr.health.task_leaks", state.active_tasks != null, state.active_tasks == null or state.active_tasks.? <= state.expected_active_tasks, "simulated tasks remained after cleanup"),
        check("vopr.health.descriptor_leaks", state.open_descriptors != null, state.open_descriptors == null or state.open_descriptors.? <= state.expected_open_descriptors, "descriptors remained after cleanup"),
        check("vopr.health.allocator_exhaustion", allocator_encountered, allocator_ok, "allocator budget was exhausted"),
        check("vopr.health.storage_exhaustion", state.storage_exhausted != null, state.storage_exhausted == null or !state.storage_exhausted.?, "storage budget was exhausted"),
        check("vopr.health.eventual_recovery", evidence.recovery_expected, !evidence.recovery_expected or evidence.recovery_complete, "recovery did not converge after faults stopped"),
        check("vopr.health.consistency", state.consistency_valid != null, state.consistency_valid == null or state.consistency_valid.?, "final scenario consistency check failed"),
        check("vopr.health.cleanup", state.cleanup_complete != null, state.cleanup_complete == null or state.cleanup_complete.?, "scenario cleanup did not complete"),
        check("vopr.health.replay_divergence", true, replay_ok, "history contains replay divergence"),
        check("vopr.health.harness_error", true, harness_ok, "history contains a harness error"),
    };
}

pub fn failureCount(checks: []const Check) usize {
    var count: usize = 0;
    for (checks) |item| count += @intFromBool(item.encountered and !item.condition);
    return count;
}

fn check(name: []const u8, encountered: bool, condition: bool, details: []const u8) Check {
    return .{
        .property_id = ids.stable("property", name),
        .name = name,
        .encountered = encountered,
        .condition = condition,
        .details = if (condition) "" else details,
    };
}

test "default harness health reports leaks exhaustion and replay state" {
    const FailureClass = enum { replay_divergence, harness, allocator, panic, process, other };
    const Failure = struct { class: FailureClass };
    const History = struct { failures: struct { items: []const Failure } };
    const history = History{ .failures = .{ .items = &.{} } };
    const checks = evaluate(&history, .{
        .progress_expected = true,
        .active_tasks = 2,
        .open_descriptors = 0,
        .allocator_exhausted = false,
        .storage_exhausted = true,
        .cleanup_complete = false,
    });
    try std.testing.expectEqual(@as(usize, 4), failureCount(&checks));
    try std.testing.expect(!checks[0].condition);
    try std.testing.expect(!checks[2].condition);
    try std.testing.expect(!checks[5].condition);
    try std.testing.expect(!checks[8].condition);
}

test "health recorder separates continuous recovery and final evidence" {
    var recorder = Recorder{ .no_progress_sample_budget = 1 };
    recorder.record(.continuous, .{ .progress_expected = true, .progress_units = 0 });
    recorder.record(.continuous, .{ .progress_expected = true, .progress_units = 0, .recovery_expected = true });
    recorder.record(.recovery, .{ .progress_expected = true, .progress_units = 0, .recovery_expected = true });
    recorder.record(.recovery, .{ .progress_expected = true, .progress_units = 1, .recovery_expected = true, .recovery_complete = true });
    recorder.record(.final, .{ .progress_units = 1, .active_tasks = 0, .cleanup_complete = true });
    const evidence = recorder.evidence();
    try std.testing.expectEqual(@as(u64, 2), evidence.continuous_samples);
    try std.testing.expectEqual(@as(u64, 2), evidence.recovery_samples);
    try std.testing.expectEqual(@as(u64, 1), evidence.final_samples);
    try std.testing.expect(!evidence.no_progress_ok);
    try std.testing.expect(evidence.recovery_complete);
}
