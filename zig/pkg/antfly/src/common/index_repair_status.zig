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

/// Collapse durable repair diagnostics without requiring an intent ID for
/// corrupt terminal state. Callers must not infer serviceability from this
/// lifecycle; that is a separate bounded proof.
pub fn summarize(
    has_intent: bool,
    automation: []const u8,
    phase: []const u8,
    wait_reason: []const u8,
) ?IndexRepairStatus {
    if (std.mem.eql(u8, automation, "paused")) return .paused;
    if (std.mem.eql(u8, phase, "terminal")) return .failed;
    if (!has_intent) return null;
    if (!std.mem.eql(u8, wait_reason, "none")) return .waiting;
    return .rebuilding;
}

test "compact index repair status keeps corrupt terminal state actionable" {
    try std.testing.expectEqual(IndexRepairStatus.failed, summarize(false, "none", "terminal", "terminal").?);
    try std.testing.expectEqual(IndexRepairStatus.paused, summarize(true, "paused", "detected", "paused").?);
    try std.testing.expectEqual(IndexRepairStatus.waiting, summarize(true, "enabled", "building", "backoff").?);
    try std.testing.expectEqual(IndexRepairStatus.rebuilding, summarize(true, "enabled", "building", "none").?);
    try std.testing.expect(summarize(false, "none", "none", "none") == null);
}
