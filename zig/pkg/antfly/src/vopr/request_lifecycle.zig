// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Adapter from Antfly's production request-lifecycle seam to stable VoprIo
//! safepoints. This module is harness-only; the API kernel imports no VOPR
//! types.

const std = @import("std");
const vopr = @import("vopr");
const http_server = @import("../api/http_server.zig");

pub const Hook = struct {
    vopr_io: *vopr.vopr_io.VoprIo,

    pub fn lifecycle(self: *Hook) http_server.RequestLifecycleHook {
        return .{ .ptr = self, .reach_fn = reach };
    }

    pub fn stableId(event: http_server.RequestLifecycleEvent) vopr.id.StableId {
        const phase_name = switch (event.phase) {
            .ingress => "ingress",
            .admission_acquired => "admission-acquired",
            .response_ready => "response-ready",
        };
        return vopr.id.derive(
            "antfly-request-lifecycle",
            vopr.id.stable("request-lifecycle-phase", phase_name),
            if (event.operation_id) |operation_id| vopr.id.digest(operation_id) else 0,
        );
    }

    fn reach(ptr: *anyopaque, event: http_server.RequestLifecycleEvent) !void {
        const self: *Hook = @ptrCast(@alignCast(ptr));
        try self.vopr_io.safepoint(stableId(event));
    }
};

test "request lifecycle adapter records stable VoprIo safepoints" {
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        .instrumentation = .{ .enabled = false, .map_digest = 0x524551 },
    });
    defer vopr_io.deinit();
    var adapter = Hook{ .vopr_io = &vopr_io };
    const hook = adapter.lifecycle();

    const ingress = http_server.RequestLifecycleEvent{ .phase = .ingress };
    const admission = http_server.RequestLifecycleEvent{
        .phase = .admission_acquired,
        .operation_id = "queryTable",
    };
    const response = http_server.RequestLifecycleEvent{ .phase = .response_ready };
    try hook.reach(ingress);
    try hook.reach(admission);
    try hook.reach(response);

    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(Hook.stableId(ingress)));
    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(Hook.stableId(admission)));
    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(Hook.stableId(response)));
    try std.testing.expect(Hook.stableId(admission) != Hook.stableId(.{
        .phase = .admission_acquired,
        .operation_id = "batchWrite",
    }));
    try vopr_io.ensureNoCapabilityViolation();
}
