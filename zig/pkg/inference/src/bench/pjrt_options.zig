// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit provider policy shared by public-task benchmarks. GPU allocation
//! quantities must describe the chosen device; never infer them from its name.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;

pub const Options = struct {
    device: enum { cpu, cuda } = .cpu,
    unified_memory: bool = false,
    capacity_bytes: usize = 0,
    device_bytes: usize = 0,
    precision: factory.PjrtSessionOptions.Precision = .f32,

    pub fn parse(self: *Options, arg: []const u8, value: []const u8) !bool {
        if (std.mem.eql(u8, arg, "--pjrt-device")) {
            self.device = std.meta.stringToEnum(@FieldType(Options, "device"), value) orelse return error.InvalidPjrtDevice;
        } else if (std.mem.eql(u8, arg, "--pjrt-unified-memory")) {
            self.unified_memory = if (std.mem.eql(u8, value, "true")) true else if (std.mem.eql(u8, value, "false")) false else return error.InvalidPjrtMemoryPolicy;
        } else if (std.mem.eql(u8, arg, "--pjrt-capacity-bytes")) {
            self.capacity_bytes = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--pjrt-device-bytes")) {
            self.device_bytes = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--pjrt-precision")) {
            self.precision = std.meta.stringToEnum(factory.PjrtSessionOptions.Precision, value) orelse return error.InvalidPjrtPrecision;
        } else return false;
        return true;
    }

    pub fn sessionOptions(self: Options) !factory.PjrtSessionOptions {
        return .{
            .precision = self.precision,
            .memory_policy = switch (self.device) {
                .cpu => if (self.capacity_bytes != 0 or self.device_bytes != 0 or self.unified_memory) return error.InvalidPjrtMemoryPolicy else .cpu,
                .cuda => if (self.capacity_bytes == 0 or self.device_bytes == 0 or self.capacity_bytes > self.device_bytes)
                    return error.InvalidPjrtMemoryPolicy
                else
                    .{ .cuda = .{ .capacity_bytes = self.capacity_bytes, .device_bytes = self.device_bytes, .unified_memory = self.unified_memory } },
            },
        };
    }

    pub fn configure(self: Options, manager: *inference.backends.SessionManager) !void {
        const options = try self.sessionOptions();
        manager.preferred_backends = &.{.pjrt};
        manager.required_backend = .pjrt;
        manager.required_backend_invalid = false;
        manager.pjrt_memory_policy = options.memory_policy;
        manager.pjrt_precision = options.precision;
    }
};
