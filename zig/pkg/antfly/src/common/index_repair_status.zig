// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

/// Stable, compact repair state that is safe to carry across runtime-status
/// process and persistence boundaries. Detailed repair diagnostics remain
/// local; this state is only the application/operator-facing lifecycle.
pub const IndexRepairStatus = enum(u8) {
    rebuilding = 1,
    waiting = 2,
    paused = 3,
    failed = 4,
};

/// Query availability and desired-incarnation convergence are separate repair
/// effects. A proven active generation may remain safe to query while its
/// replacement needs operator action; that repair still prevents the desired
/// incarnation from being complete.
pub const LifecycleProjection = struct {
    present: bool,
    action_required: bool,
    active_generation_serviceable: bool,
    blocks_queryable: bool,
    blocks_complete: bool,
};

/// Project the compact repair lifecycle into the two public milestones. The
/// serviceability input must come from the incarnation/version-scoped admission
/// proof owned by storage; callers must not infer it from a repair phase.
pub fn projectLifecycle(
    status: ?IndexRepairStatus,
    action_required: bool,
    active_generation_serviceable: bool,
) LifecycleProjection {
    const present = status != null;
    const normalized_action_required = action_required or if (status) |state|
        state == .paused or state == .failed
    else
        false;
    const serving = present and active_generation_serviceable;
    const runnable = status != null and status.? == .rebuilding;
    return .{
        .present = present,
        .action_required = present and normalized_action_required,
        .active_generation_serviceable = serving,
        .blocks_queryable = present and !serving,
        // Only a runnable replacement may be redundant after its active
        // generation is proven. Waiting/paused/failed debt remains incomplete
        // even when a retained generation can still answer queries.
        .blocks_complete = present and (!serving or normalized_action_required or !runnable),
    };
}

/// Collapse durable repair diagnostics without requiring an intent ID for
/// corrupt terminal state. Callers must not infer serviceability from this
/// lifecycle; that is a separate bounded proof.
pub fn summarize(
    has_intent: bool,
    automation: []const u8,
    phase: []const u8,
    wait_reason: []const u8,
    action_required: bool,
) ?IndexRepairStatus {
    if (std.mem.eql(u8, automation, "paused")) return .paused;
    // Some terminal durable phases are scheduler checkpoints whose source
    // coverage can make them runnable again. Keep those in the waiting class;
    // `failed` is reserved for a genuinely operator-actionable terminal state.
    if (std.mem.eql(u8, phase, "terminal")) return if (action_required) .failed else .waiting;
    if (!has_intent) return null;
    if (!std.mem.eql(u8, wait_reason, "none")) return .waiting;
    return .rebuilding;
}

test "compact index repair status keeps corrupt terminal state actionable" {
    try std.testing.expectEqual(IndexRepairStatus.failed, summarize(false, "none", "terminal", "terminal", true).?);
    try std.testing.expectEqual(IndexRepairStatus.waiting, summarize(true, "enabled", "terminal", "terminal", false).?);
    try std.testing.expectEqual(IndexRepairStatus.paused, summarize(true, "paused", "detected", "paused", true).?);
    try std.testing.expectEqual(IndexRepairStatus.waiting, summarize(true, "enabled", "building", "backoff", false).?);
    try std.testing.expectEqual(IndexRepairStatus.rebuilding, summarize(true, "enabled", "building", "none", false).?);
    try std.testing.expect(summarize(false, "none", "none", "none", false) == null);
}

test "repair lifecycle separates serving availability from convergence" {
    const rebuilding = projectLifecycle(.rebuilding, false, true);
    try std.testing.expect(!rebuilding.blocks_queryable);
    try std.testing.expect(!rebuilding.blocks_complete);

    const retained_failure = projectLifecycle(.failed, true, true);
    try std.testing.expect(retained_failure.action_required);
    try std.testing.expect(!retained_failure.blocks_queryable);
    try std.testing.expect(retained_failure.blocks_complete);

    const retained_wait = projectLifecycle(.waiting, false, true);
    try std.testing.expect(!retained_wait.blocks_queryable);
    try std.testing.expect(retained_wait.blocks_complete);

    const blocking_failure = projectLifecycle(.failed, true, false);
    try std.testing.expect(blocking_failure.blocks_queryable);
    try std.testing.expect(blocking_failure.blocks_complete);

    const healthy = projectLifecycle(null, false, true);
    try std.testing.expect(!healthy.present);
    try std.testing.expect(!healthy.active_generation_serviceable);
}
