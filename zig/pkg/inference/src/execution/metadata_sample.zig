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

/// The actual synchronous owner supplies its live token explicitly. A target
/// without this bounded metadata service declines; it never lends an ambient
/// operation or turns numerical exclusion into metadata/index authority.
pub fn snapshotOwned(backend: *const ops.ComputeBackend, token: @import("backend_operation.zig").Borrowed) ?ops.BackendDebugTimingSnapshot {
    const services = backend.vtable.operation_services orelse return null;
    if (!services.accepts(backend.vtable)) return null; // BEFORE any context cast
    const sample = services.snapshot_owned_metadata orelse return null;
    return sample(backend, token) catch null;
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

test "owned metadata sampling declines unsupported and copied actual Native tables before invalid token context" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const std = @import("std");
    const operation = @import("backend_operation.zig");
    const shared = try @import("../backends/compute_context.zig").SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    const backend = shared.stableBackend();
    const invalid = operation.Borrowed{ .context = @ptrFromInt(0x1000), .nonce = 1, .mode = .managed, .vtable = @ptrFromInt(0x2000) };
    // No supported service means unavailable, never token validation or an
    // implicit fallback acquisition of another provider's metadata scope.
    try std.testing.expect(snapshotOwned(backend, invalid) == null);
    var copied = backend.vtable.*;
    var wrapped = backend.*;
    wrapped.vtable = &copied;
    wrapped.ptr = @ptrFromInt(0x1000);
    try std.testing.expect(snapshotOwned(&wrapped, invalid) == null);
    var scope = try operation.enter(backend.*, null, null, .managed);
    defer scope.deinitChecked() catch @panic("owned metadata Native scope");
    try std.testing.expect(snapshotOwned(backend, scope.token().?) == null);
    try scope.finish();
    try std.testing.expect(snapshot(backend) != null);
}
