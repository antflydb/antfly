// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit provider policy shared by serving and public-task qualification. GPU allocation
//! quantities must describe the chosen device; never infer them from its name.
const std = @import("std");
const factory = @import("../architectures/session_factory.zig");

pub const usage = "PJRT: --backend xla [--pjrt-device cpu|cuda] [--pjrt-precision f32|bf16]. CUDA requires --pjrt-capacity-bytes N and --pjrt-device-bytes N; --pjrt-unified-memory true|false selects the admitted allocation policy.";

pub const Options = struct {
    device: enum { cpu, cuda } = .cpu,
    unified_memory: bool = false,
    capacity_bytes: usize = 0,
    device_bytes: usize = 0,
    precision: factory.PjrtSessionOptions.Precision = .f32,
    extraction_batch_size: usize = 1,
    media_bucket_rows: usize = 0,
    decoder_cache: ?bool = null,
    gemm: @FieldType(factory.PjrtSessionOptions, "gemm") = .provider_default,
    task_controls: packed struct { batch: bool = false, media: bool = false, cache: bool = false } = .{},

    pub const Task = enum { generation, extraction, transcription, legacy_extraction };

    /// Provider policy is independent of task policy. Reject a control when the
    /// selected task cannot honor it instead of silently benchmarking a default.
    pub fn validateTask(self: Options, task: Task) !void {
        if (self.task_controls.batch and task != .extraction) return error.UnsupportedPjrtTaskOption;
        if (self.task_controls.media and task != .generation) return error.UnsupportedPjrtTaskOption;
        if (self.task_controls.cache and task != .generation and task != .transcription) return error.UnsupportedPjrtTaskOption;
    }

    pub fn validateTaskPlacement(self: Options, pjrt_selected: bool) !void {
        if (!pjrt_selected and (self.task_controls.batch or self.task_controls.media or self.task_controls.cache)) return error.UnsupportedPjrtTaskOption;
    }

    pub fn parse(self: *Options, arg: []const u8, value: []const u8) !bool {
        if (std.mem.eql(u8, arg, "--pjrt-device")) {
            self.device = std.meta.stringToEnum(@FieldType(Options, "device"), value) orelse return error.InvalidPjrtDevice;
        } else if (std.mem.eql(u8, arg, "--pjrt-extraction-batch-size")) {
            self.task_controls.batch = true;
            self.extraction_batch_size = try std.fmt.parseInt(usize, value, 10);
            if (self.extraction_batch_size == 0 or self.extraction_batch_size > 8) return error.InvalidPjrtBatch;
        } else if (std.mem.eql(u8, arg, "--pjrt-media-bucket-rows")) {
            self.task_controls.media = true;
            self.media_bucket_rows = try std.fmt.parseInt(usize, value, 10);
            if (self.media_bucket_rows > 256) return error.InvalidPjrtBucket;
        } else if (std.mem.eql(u8, arg, "--pjrt-decoder-cache")) {
            self.task_controls.cache = true;
            self.decoder_cache = if (std.mem.eql(u8, value, "auto")) null else if (std.mem.eql(u8, value, "true")) true else if (std.mem.eql(u8, value, "false")) false else return error.InvalidPjrtDecoderCache;
        } else if (std.mem.eql(u8, arg, "--pjrt-gemm")) {
            self.gemm = std.meta.stringToEnum(@TypeOf(self.gemm), value) orelse return error.InvalidPjrtGemm;
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
            .gemm = self.gemm,
            .memory_policy = switch (self.device) {
                .cpu => if (self.capacity_bytes != 0 or self.device_bytes != 0 or self.unified_memory) return error.InvalidPjrtMemoryPolicy else .cpu,
                .cuda => if (self.capacity_bytes == 0 or self.device_bytes == 0 or self.capacity_bytes > self.device_bytes)
                    return error.InvalidPjrtMemoryPolicy
                else
                    .{ .cuda = .{ .capacity_bytes = self.capacity_bytes, .device_bytes = self.device_bytes, .unified_memory = self.unified_memory } },
            },
        };
    }

    pub fn configure(self: Options, manager: *@import("../backends/backends.zig").SessionManager) !void {
        try self.applyPolicy(manager);
        manager.preferred_backends = &.{.pjrt};
        manager.required_backend = .pjrt;
        manager.required_backend_invalid = false;
    }

    /// Carry provider policy without changing native/Metal/CUDA placement. This
    /// also lets an explicitly placed decoder share one task's PJRT policy.
    pub fn applyPolicy(self: Options, manager: *@import("../backends/backends.zig").SessionManager) !void {
        const options = try self.sessionOptions();
        manager.pjrt_memory_policy = options.memory_policy;
        manager.pjrt_precision = options.precision;
        manager.pjrt_gemm = options.gemm;
    }
};

test "PJRT options require explicit feasible CUDA capacities and preserve selection" {
    var value: Options = .{};
    try std.testing.expectEqual(@as(?bool, null), value.decoder_cache);
    try std.testing.expect(try value.parse("--pjrt-decoder-cache", "true"));
    try std.testing.expectEqual(@as(?bool, true), value.decoder_cache);
    try std.testing.expect(try value.parse("--pjrt-decoder-cache", "false"));
    try std.testing.expectEqual(@as(?bool, false), value.decoder_cache);
    try std.testing.expect(try value.parse("--pjrt-decoder-cache", "auto"));
    try std.testing.expectEqual(@as(?bool, null), value.decoder_cache);
    try std.testing.expectError(error.InvalidPjrtDecoderCache, value.parse("--pjrt-decoder-cache", "yes"));
    try std.testing.expect(try value.parse("--pjrt-device", "cuda"));
    try std.testing.expectError(error.InvalidPjrtMemoryPolicy, value.sessionOptions());
    try std.testing.expect(try value.parse("--pjrt-capacity-bytes", "4096"));
    try std.testing.expect(try value.parse("--pjrt-device-bytes", "2048"));
    try std.testing.expectError(error.InvalidPjrtMemoryPolicy, value.sessionOptions());
    try std.testing.expect(try value.parse("--pjrt-device-bytes", "8192"));
    try std.testing.expect(try value.parse("--pjrt-gemm", "vendor"));
    try std.testing.expect(try value.parse("--pjrt-precision", "bf16"));
    const result = try value.sessionOptions();
    try std.testing.expectEqual(@as(usize, 4096), result.memory_policy.?.cuda.capacity_bytes);
    try std.testing.expectEqual(.vendor, result.gemm);
    try std.testing.expectEqual(.bf16, result.precision);
    try std.testing.expectError(error.InvalidPjrtBucket, value.parse("--pjrt-media-bucket-rows", "257"));
}

test "task PJRT policy is independent of placement and explicit selection cannot fall back" {
    var manager = @import("../backends/backends.zig").SessionManager.init(std.testing.allocator);
    manager.preferred_backends = &.{.native};
    var options = Options{ .device = .cuda, .capacity_bytes = 1024, .device_bytes = 4096, .precision = .bf16 };
    try options.applyPolicy(&manager);
    try std.testing.expectEqual(@import("../backends/backends.zig").BackendType.native, manager.preferred_backends[0]);
    try std.testing.expect(manager.pjrt_memory_policy.? == .cuda);
    try std.testing.expectEqual(@as(usize, 1024), manager.pjrt_memory_policy.?.cuda.capacity_bytes);
    try options.configure(&manager);
    try std.testing.expectEqual(@import("../backends/backends.zig").BackendType.pjrt, manager.preferred_backends[0]);
    try std.testing.expectEqual(@as(?@import("../backends/backends.zig").BackendType, .pjrt), manager.required_backend);
    options.capacity_bytes = 8192;
    try std.testing.expectError(error.InvalidPjrtMemoryPolicy, options.applyPolicy(&manager));
}

test "task PJRT controls are honored or explicitly rejected" {
    var options = Options{};
    try std.testing.expect(try options.parse("--pjrt-decoder-cache", "true"));
    try options.validateTask(.generation);
    try options.validateTask(.transcription);
    try std.testing.expectError(error.UnsupportedPjrtTaskOption, options.validateTask(.extraction));
    try std.testing.expect(try options.parse("--pjrt-decoder-cache", "false"));
    try options.validateTask(.transcription);
    options = .{};
    try std.testing.expect(try options.parse("--pjrt-media-bucket-rows", "0"));
    try options.validateTask(.generation);
    try std.testing.expectError(error.UnsupportedPjrtTaskOption, options.validateTask(.legacy_extraction));
    options = .{};
    try std.testing.expect(try options.parse("--pjrt-extraction-batch-size", "4"));
    try options.validateTask(.extraction);
    try options.validateTaskPlacement(true);
    try std.testing.expectError(error.UnsupportedPjrtTaskOption, options.validateTaskPlacement(false));
    try std.testing.expectError(error.UnsupportedPjrtTaskOption, options.validateTask(.generation));
}
