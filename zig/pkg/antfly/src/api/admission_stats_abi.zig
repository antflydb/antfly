// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Scalar-only admission snapshots shared by the API kernel and its host.
//! Keep vocabulary sizes explicit: changing this layout requires an API ABI
//! version bump. Conversion fails to compile if the controller grows new fields.
const std = @import("std");

pub const Stats = extern struct {
    capacity: usize,
    in_flight: usize,
    peak_in_flight: usize,
    rejected_total: u64,
    queued: usize,
    queued_bytes: usize,
    retained_bytes: usize,
    waited_total: u64,
    wait_ns_total: u64,
    wait_completed_total: u64,
    wait_buckets: [11]u64,
    expired_total: u64,
    cancelled_total: u64,
    draining: u8,
    max_queued_requests: usize,
    max_queued_bytes: usize,
    max_retained_bytes: usize,
    max_wait_ms: u32,
    policy_generation: u64,
    rejection_reasons: [9]u64,
    allocation_denials: [3]u64,

    pub fn fromNative(value: anytype) Stats {
        comptime validateNative(@TypeOf(value));
        var out: Stats = undefined;
        inline for (std.meta.fields(Stats)) |field| {
            @field(out, field.name) = if (comptime std.mem.eql(u8, field.name, "draining"))
                @intFromBool(value.draining)
            else
                @field(value, field.name);
        }
        return out;
    }

    pub fn toNative(self: Stats, comptime T: type) T {
        comptime validateNative(T);
        var out: T = undefined;
        inline for (std.meta.fields(Stats)) |field| {
            @field(out, field.name) = if (comptime std.mem.eql(u8, field.name, "draining"))
                self.draining != 0
            else
                @field(self, field.name);
        }
        return out;
    }

    fn validateNative(comptime T: type) void {
        if (std.meta.fields(T).len != std.meta.fields(Stats).len)
            @compileError("admission stats changed: update the wire snapshot and API ABI version");
        for (std.meta.fields(Stats)) |field| {
            const expected = if (std.mem.eql(u8, field.name, "draining")) bool else field.type;
            if (@FieldType(T, field.name) != expected)
                @compileError("admission stats field changed: " ++ field.name);
        }
    }
};

pub const HandlerStats = extern struct {
    query: Stats,
    write: Stats,
    inference: Stats,
    query_body: Stats,
    recovery: RecoveryStats = .{},
};

/// Journal bytes are logical durable credits, not an RSS measurement. A failed
/// journal read is unavailable evidence and must never be published as zero.
pub const AttemptStats = extern struct {
    enabled: u8 = 0,
    available: u8 = 0,
    attempts: u64 = 0,
    logical_bytes: u64 = 0,
    uncertain: u64 = 0,
};

pub const RecoveryStats = extern struct {
    ingress_enabled: u8 = 0,
    ingress_capacity: u64 = 0,
    ingress_in_flight: u64 = 0,
    ingress_retained_bytes: u64 = 0,
    ingress_capacity_bytes: u64 = 0,
    worker: AttemptStats = .{},
    coordinator: AttemptStats = .{},

    pub fn collect(server: anytype) RecoveryStats {
        const ingress = server.ingress_admission.recovery.stats();
        var result: RecoveryStats = .{
            .ingress_enabled = @intFromBool(server.cfg.ingress_admission.recovery_requests != 0),
            .ingress_capacity = ingress.capacity,
            .ingress_in_flight = ingress.in_flight,
            .ingress_retained_bytes = ingress.retained_bytes,
            .ingress_capacity_bytes = ingress.max_retained_bytes,
        };
        if (server.remote_attempt_worker) |worker| {
            result.worker.enabled = 1;
            if (worker.usage()) |usage| {
                result.worker.available = 1;
                result.worker.attempts = usage.attempts;
                result.worker.logical_bytes = usage.bytes;
                result.worker.uncertain = usage.uncertain;
            } else |_| {}
        }
        if (server.remote_attempt_coordinator) |coordinator| {
            result.coordinator.enabled = 1;
            if (coordinator.store.usage()) |usage| {
                result.coordinator.available = 1;
                result.coordinator.attempts = usage.attempts;
                result.coordinator.logical_bytes = usage.bytes;
            } else |_| {}
        }
        return result;
    }

    pub fn appendPrometheusMetrics(self: RecoveryStats, writer: *std.Io.Writer) !void {
        try writer.print("antfly_recovery_ingress_enabled {d}\nantfly_recovery_ingress_capacity_requests {d}\nantfly_recovery_ingress_outstanding_requests {d}\nantfly_recovery_ingress_retained_bytes {d}\nantfly_recovery_ingress_capacity_bytes {d}\n", .{
            self.ingress_enabled, self.ingress_capacity, self.ingress_in_flight, self.ingress_retained_bytes, self.ingress_capacity_bytes,
        });
        inline for (.{ "worker", "coordinator" }) |role| {
            const value = @field(self, role);
            try writer.print("antfly_remote_attempt_" ++ role ++ "_enabled {d}\nantfly_remote_attempt_" ++ role ++ "_sample_available {d}\n", .{ value.enabled, value.available });
            if (value.available != 0) {
                try writer.print("antfly_remote_attempt_" ++ role ++ "_records {d}\nantfly_remote_attempt_" ++ role ++ "_logical_bytes {d}\n", .{ value.attempts, value.logical_bytes });
                if (comptime std.mem.eql(u8, role, "worker")) try writer.print("antfly_remote_attempt_worker_uncertain {d}\n", .{value.uncertain});
            }
        }
    }
};

test "workload admission unavailable journal samples never manufacture zero debt" {
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    const snapshot: RecoveryStats = .{
        .ingress_enabled = 1,
        .ingress_capacity = 2,
        .ingress_in_flight = 1,
        .ingress_retained_bytes = 128,
        .ingress_capacity_bytes = 65536,
        .worker = .{ .enabled = 1, .available = 1, .attempts = 3, .uncertain = 2, .logical_bytes = 4096 },
        .coordinator = .{ .enabled = 1, .available = 0 },
    };
    try snapshot.appendPrometheusMetrics(&output.writer);
    const rendered = output.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_remote_attempt_worker_uncertain 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_remote_attempt_coordinator_sample_available 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_remote_attempt_coordinator_records") == null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_recovery_ingress_retained_bytes 128\n") != null);
}
