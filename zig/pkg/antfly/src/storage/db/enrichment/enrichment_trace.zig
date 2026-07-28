// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const build_options = @import("build_options");
const tracing = @import("../../../tracing/mod.zig");
const lease_mod = @import("../lease.zig");
const ownership_mod = @import("../ownership.zig");

fn persistedLeaseValid(alloc: std.mem.Allocator, ownership: *ownership_mod.State, now_ms: u64) bool {
    if (!ownership.lease_owned) return true;
    var record = (ownership.lease.load(alloc) catch return false) orelse return false;
    defer lease_mod.deinitRecord(alloc, &record);
    return record.expires_at_ms > now_ms and
        std.mem.eql(u8, record.owner_id, ownership.owner_id);
}

pub fn event(
    runtime: anytype,
    name: []const u8,
    pending_work: ?u64,
    sequence: ?u64,
    reason: ?[]const u8,
) void {
    if (comptime !build_options.with_tla) return;
    const io_impl = runtime.io_impl orelse return;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    const applied_sequence = runtime.applied_sequence;
    const target_sequence = runtime.target_sequence;
    const lease_epoch = runtime.ownership.acquisition_count;
    const retrying = runtime.retrying;
    const worker_failed = runtime.worker_failed;
    runtime.mutex.unlock(io);

    const valid_lease = persistedLeaseValid(
        runtime.alloc,
        &runtime.ownership,
        runtime.config.clock.nowRealtimeMs(),
    );
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "enrichment-lease",
        .trace_id = @intFromPtr(runtime),
        .name = name,
        .facts = .{
            .owner_id = runtime.ownership.owner_id,
            .reason = reason,
            .applied_sequence = applied_sequence,
            .target_sequence = target_sequence,
            .last_sequence = sequence,
            .pending_work = pending_work,
            .lease_epoch = lease_epoch,
            .lease_valid = valid_lease,
            .retrying = retrying,
            .worker_failed = worker_failed,
        },
    });
}

test "persisted lease validity detects takeover before cached ownership does" {
    const mem_backend = @import("../../mem_backend.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "trace-lease" });
    defer store.deinit();

    var owner_a = try ownership_mod.State.init(alloc, store, "trace-lease", .{
        .lease_owned = true,
        .owner_id = "worker-a",
        .lease_ttl_ms = 250,
    });
    defer owner_a.deinit(alloc);
    var owner_b = try ownership_mod.State.init(alloc, store, "trace-lease", .{
        .lease_owned = true,
        .owner_id = "worker-b",
        .lease_ttl_ms = 250,
    });
    defer owner_b.deinit(alloc);

    try std.testing.expect(try owner_a.ensureLease(1_000));
    try std.testing.expect(try owner_b.ensureLease(1_300));
    try std.testing.expect(owner_a.has_lease);
    try std.testing.expect(!persistedLeaseValid(alloc, &owner_a, 1_320));
    try std.testing.expect(persistedLeaseValid(alloc, &owner_b, 1_320));
}
