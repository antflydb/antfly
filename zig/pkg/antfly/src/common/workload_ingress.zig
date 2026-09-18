// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Fixed ingress partitions. Ordinary planning cannot borrow the control floor.
//! The configured totals include both partitions; control is not extra capacity.
//! Transport framing and durable retained-state pools have separate owners.
const std = @import("std");
const admission = @import("workload_admission.zig");

pub const Config = struct {
    max_requests: u32 = 0,
    max_retained_bytes: u64 = 0,
    control_requests: u32 = 2,
    control_retained_bytes: u64 = 256 * 1024,
    /// Authenticated, bounded recovery RPCs have a separate nonborrowable
    /// partition. Empty probes cannot consume this completion headroom.
    recovery_requests: u32 = 0,
    recovery_retained_bytes: u64 = 0,

    pub fn validate(self: Config) !void {
        if ((self.recovery_requests == 0) != (self.recovery_retained_bytes == 0) or
            (self.recovery_requests != 0 and self.recovery_retained_bytes < 64 * 1024)) return error.InvalidConfig;
        if (self.max_requests == 0) {
            if (self.max_retained_bytes != 0 or self.recovery_requests != 0) return error.InvalidConfig;
            return;
        }
        if (self.max_requests > 65_536 or self.control_requests == 0 or @as(u64, self.control_requests) + self.recovery_requests >= self.max_requests or
            self.control_retained_bytes < 4096 or @as(u128, self.control_retained_bytes) + self.recovery_retained_bytes >= self.max_retained_bytes or
            self.max_retained_bytes > 1_099_511_627_776 or self.max_retained_bytes > std.math.maxInt(usize)) return error.InvalidConfig;
    }
};

pub const Runtime = struct {
    config: Config,
    general: admission.Controller,
    control: admission.Controller,
    recovery: admission.Controller,

    /// Move only before the first allocation owner is attached to a controller.
    pub fn init(config: Config) Runtime {
        config.validate() catch unreachable;
        const is_enabled = config.max_requests != 0;
        return .{
            .config = config,
            .general = .initConfigured(if (is_enabled) config.max_requests - config.control_requests - config.recovery_requests else 0, .{
                .max_retained_bytes = if (is_enabled) @intCast(config.max_retained_bytes - config.control_retained_bytes - config.recovery_retained_bytes) else 0,
            }),
            .control = .initConfigured(if (is_enabled) config.control_requests else 0, .{
                .max_retained_bytes = if (is_enabled) @intCast(config.control_retained_bytes) else 0,
            }),
            .recovery = .initConfigured(if (is_enabled) config.recovery_requests else 0, .{
                .max_retained_bytes = if (is_enabled) @intCast(config.recovery_retained_bytes) else 0,
            }),
        };
    }

    pub fn enabled(self: *const Runtime) bool {
        return self.config.max_requests != 0;
    }

    pub fn closeForeground(self: *Runtime) void {
        self.general.close();
        self.control.close();
        // Recovery/control remains reachable while foreground drains. The
        // listener owner calls close() after outstanding handlers have joined.
    }

    pub fn close(self: *Runtime) void {
        self.general.close();
        self.control.close();
        self.recovery.close();
    }

    pub fn deinitMemory(self: *Runtime) void {
        self.general.deinitMemory();
        self.control.deinitMemory();
        self.recovery.deinitMemory();
    }
};

test "workload admission ingress control floor is inside the hard envelope" {
    var runtime = Runtime.init(.{ .max_requests = 3, .max_retained_bytes = 16384, .control_requests = 1, .control_retained_bytes = 4096 });
    defer runtime.deinitMemory();
    var first = try runtime.general.acquire(.{ .io = std.testing.io, .retained_bytes = 8192 });
    defer first.release();
    var second = try runtime.general.acquire(.{ .io = std.testing.io, .retained_bytes = 4096 });
    defer second.release();
    try std.testing.expectError(error.AdmissionFull, runtime.general.acquire(.{ .io = std.testing.io }));
    var control = try runtime.control.acquire(.{ .io = std.testing.io, .retained_bytes = 4096 });
    defer control.release();
    try std.testing.expectEqual(@as(usize, 16384), runtime.general.stats().retained_bytes + runtime.control.stats().retained_bytes);
    runtime.close();
    try std.testing.expectError(error.AdmissionClosed, runtime.control.acquire(.{ .io = std.testing.io }));
}

test "workload admission ingress rejects impossible partitions" {
    try (Config{}).validate();
    try std.testing.expectError(error.InvalidConfig, (Config{ .max_retained_bytes = 65536 }).validate());
    try std.testing.expectError(error.InvalidConfig, (Config{ .max_requests = 2, .max_retained_bytes = 1048576 }).validate());
    try std.testing.expectError(error.InvalidConfig, (Config{ .max_requests = 16, .max_retained_bytes = 262144 }).validate());
    try std.testing.expectError(error.InvalidConfig, (Config{ .max_requests = 16, .max_retained_bytes = 1048576, .control_requests = 0 }).validate());
}

test "workload admission recovery ingress floor survives general and probe saturation" {
    var runtime = Runtime.init(.{ .max_requests = 4, .max_retained_bytes = 256 * 1024, .control_requests = 1, .control_retained_bytes = 64 * 1024, .recovery_requests = 1, .recovery_retained_bytes = 64 * 1024 });
    defer runtime.deinitMemory();
    var general = try runtime.general.acquire(.{ .io = std.testing.io, .retained_bytes = 128 * 1024 });
    defer general.release();
    var probe = try runtime.control.acquire(.{ .io = std.testing.io, .retained_bytes = 64 * 1024 });
    defer probe.release();
    try std.testing.expectError(error.AdmissionBytesExhausted, runtime.general.acquire(.{ .io = std.testing.io, .retained_bytes = 1 }));
    try std.testing.expectError(error.AdmissionFull, runtime.control.acquire(.{ .io = std.testing.io }));
    var recovery = try runtime.recovery.acquire(.{ .io = std.testing.io, .retained_bytes = 64 * 1024 });
    defer recovery.release();
    try std.testing.expectError(error.AdmissionFull, runtime.recovery.acquire(.{ .io = std.testing.io }));
    try std.testing.expectEqual(@as(usize, 256 * 1024), runtime.general.stats().retained_bytes + runtime.control.stats().retained_bytes + runtime.recovery.stats().retained_bytes);
    try std.testing.expectError(error.InvalidConfig, (Config{ .max_requests = 4, .max_retained_bytes = 256 * 1024, .control_requests = 2, .control_retained_bytes = 64 * 1024, .recovery_requests = 2, .recovery_retained_bytes = 64 * 1024 }).validate());
}
