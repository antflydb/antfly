// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional outer readers must acquire their own target metadata authority.
//! In particular Metal's managed token does not authorize indexed SDK reads.
const ops = @import("../ops/ops.zig");
pub fn snapshot(backend: *const ops.ComputeBackend) ?ops.BackendDebugTimingSnapshot {
    const operation = @import("backend_operation.zig");
    // Deliberately do not pass operationToken(): managed->metadata borrowing
    // would skip Metal's actual slot-index lock acquisition.
    var scope = operation.enter(backend.*, null, null, .metadata) catch return null;
    defer scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    if (scope.token() == null) return null;
    const value = backend.debugTimingSnapshot();
    scope.finish() catch return null;
    return value;
}

test "outer metadata samples decline busy shared providers and release before returning" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const std = @import("std");
    const shared = try @import("../backends/compute_context.zig").SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    const backend = shared.stableBackend();
    const operation = @import("backend_operation.zig");
    var busy = try operation.enter(backend.*, null, null, .managed);
    defer busy.deinitChecked() catch unreachable;
    try std.testing.expect(snapshot(backend) == null);
    try busy.finish();
    try std.testing.expect(snapshot(backend) != null);
    // A second acquisition proves no metadata exclusion escaped the helper.
    try std.testing.expect(snapshot(backend) != null);
}
