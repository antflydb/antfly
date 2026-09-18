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

    pub fn validate(self: Config) !void {
        if (self.max_requests == 0) {
            if (self.max_retained_bytes != 0) return error.InvalidConfig;
            return;
        }
        if (self.max_requests > 65_536 or self.control_requests == 0 or self.control_requests >= self.max_requests or
            self.control_retained_bytes < 4096 or self.control_retained_bytes >= self.max_retained_bytes or
            self.max_retained_bytes > 1_099_511_627_776 or self.max_retained_bytes > std.math.maxInt(usize)) return error.InvalidConfig;
    }
};

pub const Runtime = struct {
    config: Config,
    general: admission.Controller,
    control: admission.Controller,

    /// Move only before the first allocation owner is attached to a controller.
    pub fn init(config: Config) Runtime {
        config.validate() catch unreachable;
        const is_enabled = config.max_requests != 0;
        return .{
            .config = config,
            .general = .initConfigured(if (is_enabled) config.max_requests - config.control_requests else 0, .{
                .max_retained_bytes = if (is_enabled) @intCast(config.max_retained_bytes - config.control_retained_bytes) else 0,
            }),
            .control = .initConfigured(if (is_enabled) config.control_requests else 0, .{
                .max_retained_bytes = if (is_enabled) @intCast(config.control_retained_bytes) else 0,
            }),
        };
    }

    pub fn enabled(self: *const Runtime) bool {
        return self.config.max_requests != 0;
    }

    pub fn close(self: *Runtime) void {
        self.general.close();
        self.control.close();
    }

    pub fn deinitMemory(self: *Runtime) void {
        self.general.deinitMemory();
        self.control.deinitMemory();
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
