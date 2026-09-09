// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Adapter from Antfly's production request-lifecycle seam to stable VoprIo
//! safepoints. This module is harness-only; the API kernel imports no VOPR
//! types.

const std = @import("std");
const vopr = @import("vopr");
const http_server = @import("../api/http_server.zig");
const data_runtime = @import("../data/runtime.zig");

pub const Hook = struct {
    vopr_io: *vopr.vopr_io.VoprIo,
    /// Optional harness-owned gate used to hold an admitted request at the
    /// ingress boundary while the external VOPR driver schedules a fault or
    /// deadline. Production code sees only the existing lifecycle interface.
    ingress_release: ?*std.Io.Event = null,

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
        if (event.phase == .ingress) if (self.ingress_release) |release|
            release.waitUncancelable(self.vopr_io.io());
    }
};

pub const DataHook = struct {
    vopr_io: *vopr.vopr_io.VoprIo,

    pub fn lifecycle(self: *DataHook) data_runtime.DataRequestLifecycleHook {
        return .{ .ptr = self, .reach_fn = reach };
    }

    pub fn stableId(event: data_runtime.DataRequestLifecycleEvent) vopr.id.StableId {
        var result = vopr.id.stable("data-request-lifecycle-phase", @tagName(event.phase));
        result = vopr.id.derive("data-request-lifecycle.group", result, event.group_id);
        result = vopr.id.derive("data-request-lifecycle.related-group", result, event.related_group_id);
        result = vopr.id.derive("data-request-lifecycle.target-node", result, event.target_node_id);
        result = vopr.id.derive("data-request-lifecycle.log-index", result, event.log_index);
        result = vopr.id.derive("data-request-lifecycle.transition", result, event.transition_id);
        result = vopr.id.derive("data-request-lifecycle.operation", result, vopr.id.digest(event.operation_id));
        result = vopr.id.derive("data-request-lifecycle.table", result, vopr.id.digest(event.table_name));
        return vopr.id.derive("data-request-lifecycle.response-bytes", result, event.response_bytes);
    }

    fn reach(ptr: *anyopaque, event: data_runtime.DataRequestLifecycleEvent) !void {
        const self: *DataHook = @ptrCast(@alignCast(ptr));
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

test "data request lifecycle adapter records stable semantic VoprIo safepoints" {
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        .instrumentation = .{ .enabled = false, .map_digest = 0x445251 },
    });
    defer vopr_io.deinit();
    var adapter = DataHook{ .vopr_io = &vopr_io };
    const hook = adapter.lifecycle();

    const routed = data_runtime.DataRequestLifecycleEvent{
        .phase = .routing_started,
        .group_id = 7,
        .table_name = "docs",
    };
    const accepted = data_runtime.DataRequestLifecycleEvent{
        .phase = .proposal_accepted,
        .group_id = 7,
        .log_index = 11,
        .table_name = "docs",
    };
    const cutover = data_runtime.DataRequestLifecycleEvent{
        .phase = .split_cutover_completed,
        .group_id = 7,
        .related_group_id = 8,
        .transition_id = 3,
        .table_name = "docs",
    };
    const query = data_runtime.DataRequestLifecycleEvent{
        .phase = .query_result_assembled,
        .operation_id = "public.table.query",
        .table_name = "docs",
        .response_bytes = 128,
    };
    try hook.reach(routed);
    try hook.reach(accepted);
    try hook.reach(cutover);
    try hook.reach(query);

    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(DataHook.stableId(routed)));
    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(DataHook.stableId(accepted)));
    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(DataHook.stableId(cutover)));
    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(DataHook.stableId(query)));
    try std.testing.expect(DataHook.stableId(accepted) != DataHook.stableId(.{
        .phase = .proposal_accepted,
        .group_id = 7,
        .log_index = 12,
        .table_name = "docs",
    }));
    try std.testing.expect(DataHook.stableId(query) != DataHook.stableId(.{
        .phase = .query_result_assembled,
        .operation_id = "public.global.multi_query",
        .table_name = "docs",
        .response_bytes = 128,
    }));
    try vopr_io.ensureNoCapabilityViolation();
}
