// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Standby read freshness decisions.
//!
//! The concrete query executor plugs in below this layer. This module owns the
//! HA read routing/freshness contract: standbys may serve reads at or below
//! `safe_read_lsn`, read-after-write callers can wait for `at_least_lsn`, and
//! callers that require primary consistency are routed away from the standby.

const std = @import("std");
const standby_mod = @import("standby.zig");

pub const Consistency = enum {
    stale_ok,
    at_least_lsn,
    primary,
};

pub const Action = enum {
    serve_standby,
    wait_for_apply,
    route_to_primary,
};

pub const Request = struct {
    consistency: Consistency = .stale_ok,
    required_lsn: ?u64 = null,
};

pub const Decision = struct {
    action: Action,
    consistency: Consistency,
    required_lsn: ?u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    serve_lsn: ?u64,
    missing_lsn_count: u64,

    pub fn canServeStandby(self: Decision) bool {
        return self.action == .serve_standby;
    }

    pub fn shouldWait(self: Decision) bool {
        return self.action == .wait_for_apply;
    }
};

pub fn evaluateStandby(standby: *const standby_mod.Standby, request: Request) !Decision {
    return try evaluateProgress(standby.currentProgress(), request);
}

pub fn evaluateProgress(progress: standby_mod.Progress, request: Request) !Decision {
    try validateProgress(progress);

    switch (request.consistency) {
        .primary => {
            return .{
                .action = .route_to_primary,
                .consistency = .primary,
                .required_lsn = request.required_lsn,
                .received_lsn = progress.received_lsn,
                .applied_lsn = progress.applied_lsn,
                .safe_read_lsn = progress.safe_read_lsn,
                .serve_lsn = null,
                .missing_lsn_count = 0,
            };
        },
        .stale_ok => {
            return .{
                .action = .serve_standby,
                .consistency = .stale_ok,
                .required_lsn = request.required_lsn,
                .received_lsn = progress.received_lsn,
                .applied_lsn = progress.applied_lsn,
                .safe_read_lsn = progress.safe_read_lsn,
                .serve_lsn = progress.safe_read_lsn,
                .missing_lsn_count = 0,
            };
        },
        .at_least_lsn => {
            const required_lsn = request.required_lsn orelse return error.RequiredLsnMissing;
            const satisfied = progress.safe_read_lsn >= required_lsn;
            return .{
                .action = if (satisfied) .serve_standby else .wait_for_apply,
                .consistency = .at_least_lsn,
                .required_lsn = required_lsn,
                .received_lsn = progress.received_lsn,
                .applied_lsn = progress.applied_lsn,
                .safe_read_lsn = progress.safe_read_lsn,
                .serve_lsn = if (satisfied) progress.safe_read_lsn else null,
                .missing_lsn_count = required_lsn -| progress.safe_read_lsn,
            };
        },
    }
}

fn validateProgress(progress: standby_mod.Progress) !void {
    if (progress.applied_lsn > progress.received_lsn) return error.AppliedAheadOfReceived;
    if (progress.safe_read_lsn > progress.applied_lsn) return error.SafeReadAheadOfApplied;
}

test "storage.ha read gate serves stale reads at safe read lsn" {
    const decision = try evaluateProgress(.{
        .received_lsn = 7,
        .applied_lsn = 5,
        .safe_read_lsn = 5,
    }, .{ .consistency = .stale_ok });

    try std.testing.expectEqual(Action.serve_standby, decision.action);
    try std.testing.expect(decision.canServeStandby());
    try std.testing.expectEqual(@as(?u64, 5), decision.serve_lsn);
    try std.testing.expectEqual(@as(u64, 0), decision.missing_lsn_count);
}

test "storage.ha read gate waits for read after write freshness" {
    const waiting = try evaluateProgress(.{
        .received_lsn = 7,
        .applied_lsn = 5,
        .safe_read_lsn = 5,
    }, .{
        .consistency = .at_least_lsn,
        .required_lsn = 6,
    });
    try std.testing.expectEqual(Action.wait_for_apply, waiting.action);
    try std.testing.expect(waiting.shouldWait());
    try std.testing.expectEqual(@as(?u64, null), waiting.serve_lsn);
    try std.testing.expectEqual(@as(u64, 1), waiting.missing_lsn_count);

    const ready = try evaluateProgress(.{
        .received_lsn = 7,
        .applied_lsn = 6,
        .safe_read_lsn = 6,
    }, .{
        .consistency = .at_least_lsn,
        .required_lsn = 6,
    });
    try std.testing.expectEqual(Action.serve_standby, ready.action);
    try std.testing.expectEqual(@as(?u64, 6), ready.serve_lsn);
    try std.testing.expectEqual(@as(u64, 0), ready.missing_lsn_count);
}

test "storage.ha read gate routes primary consistency away from standby" {
    const decision = try evaluateProgress(.{
        .received_lsn = 10,
        .applied_lsn = 9,
        .safe_read_lsn = 9,
    }, .{
        .consistency = .primary,
        .required_lsn = 10,
    });

    try std.testing.expectEqual(Action.route_to_primary, decision.action);
    try std.testing.expectEqual(@as(?u64, null), decision.serve_lsn);
    try std.testing.expectEqual(@as(u64, 0), decision.missing_lsn_count);
}

test "storage.ha read gate rejects invalid progress or missing freshness target" {
    try std.testing.expectError(error.RequiredLsnMissing, evaluateProgress(.{
        .received_lsn = 5,
        .applied_lsn = 5,
        .safe_read_lsn = 5,
    }, .{ .consistency = .at_least_lsn }));

    try std.testing.expectError(error.AppliedAheadOfReceived, evaluateProgress(.{
        .received_lsn = 4,
        .applied_lsn = 5,
        .safe_read_lsn = 5,
    }, .{ .consistency = .stale_ok }));

    try std.testing.expectError(error.SafeReadAheadOfApplied, evaluateProgress(.{
        .received_lsn = 5,
        .applied_lsn = 4,
        .safe_read_lsn = 5,
    }, .{ .consistency = .stale_ok }));
}
