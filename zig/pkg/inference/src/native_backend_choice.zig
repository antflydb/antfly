// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const backends = @import("backends/backends.zig");
const ops = @import("ops/ops.zig");

pub const Choice = enum {
    auto,
    onnx,
    native,
    metal,
    cuda,
    pjrt,
    webgpu,
};

pub fn parse(value: []const u8) ?Choice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    if (std.mem.eql(u8, value, "pjrt")) return .pjrt;
    if (std.mem.eql(u8, value, "webgpu")) return .webgpu;
    return null;
}

pub fn validate(choice: Choice) !void {
    switch (choice) {
        .onnx => if (!build_options.enable_onnx) return error.BackendUnavailable,
        .native => if (!backends.BackendType.native.available()) return error.BackendUnavailable,
        .metal => if (!build_options.enable_metal) return error.BackendUnavailable,
        .cuda => if (!build_options.enable_cuda) return error.BackendUnavailable,
        .pjrt => if (!build_options.enable_pjrt) return error.BackendUnavailable,
        .webgpu => if (!(build_options.enable_wasm and build_options.enable_webgpu)) return error.BackendUnavailable,
        .auto => {},
    }
}

pub fn configureSessionPreference(session_manager: *backends.SessionManager, choice: Choice) void {
    session_manager.preserve_backend_order = choice != .auto;
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_wasm and build_options.enable_webgpu)
            &.{ backends.BackendType.webgpu, backends.BackendType.native }
        else if (build_options.enable_wasm)
            &.{backends.BackendType.native}
        else if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .onnx => &.{backends.BackendType.onnx},
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
        .cuda => &.{backends.BackendType.cuda},
        .webgpu => if (build_options.enable_wasm and build_options.enable_webgpu)
            &.{backends.BackendType.webgpu}
        else
            &.{},
        .pjrt => if (build_options.enable_native)
            &.{backends.BackendType.native}
        else if (build_options.enable_metal)
            &.{backends.BackendType.metal}
        else
            &.{},
    };
}

pub fn compiledPartitionBackend(choice: Choice) ?ops.BackendKind {
    return switch (choice) {
        .onnx => .onnx,
        .pjrt => .pjrt,
        .webgpu => .webgpu,
        .auto, .native, .metal, .cuda => null,
    };
}

pub fn compiledPartitionBackendForMode(choice: Choice, compiled_mode_requested: bool) ?ops.BackendKind {
    if (compiled_mode_requested and choice == .metal and build_options.enable_metal) return .metal;
    return compiledPartitionBackend(choice);
}

pub fn forcesGraphMode(choice: Choice) bool {
    return compiledPartitionBackend(choice) != null;
}

pub fn resolvePjrtPluginPath(allocator: std.mem.Allocator, configured: ?[]const u8) !?[:0]u8 {
    if (!build_options.enable_pjrt) return null;
    const raw = configured orelse platform.env.getenv("PJRT_PLUGIN_PATH") orelse return null;
    if (raw.len == 0) return error.InvalidPjrtPluginPath;
    return try allocator.dupeZ(u8, raw);
}

pub fn pjrtPluginPathFromEnv(allocator: std.mem.Allocator) !?[:0]u8 {
    return resolvePjrtPluginPath(allocator, null);
}

test "configured PJRT plugin path takes precedence over environment lookup" {
    if (!build_options.enable_pjrt) {
        try std.testing.expectEqual(@as(?[:0]u8, null), try resolvePjrtPluginPath(std.testing.allocator, "/configured/libpjrt.so"));
        return;
    }

    const path = (try resolvePjrtPluginPath(std.testing.allocator, "/configured/libpjrt.so")).?;
    defer std.testing.allocator.free(path);
    try std.testing.expectEqualStrings("/configured/libpjrt.so", path);
    try std.testing.expectError(error.InvalidPjrtPluginPath, resolvePjrtPluginPath(std.testing.allocator, ""));
}

test "parse accepts explicit compiled backends" {
    try std.testing.expectEqual(Choice.onnx, parse("onnx").?);
    try std.testing.expectEqual(Choice.pjrt, parse("pjrt").?);
    try std.testing.expectEqual(Choice.webgpu, parse("webgpu").?);
}

test "explicit backend validation never silently substitutes a runtime" {
    if (build_options.enable_onnx) {
        try validate(.onnx);
    } else {
        try std.testing.expectError(error.BackendUnavailable, validate(.onnx));
    }
    if (build_options.enable_metal) {
        try validate(.metal);
    } else {
        try std.testing.expectError(error.BackendUnavailable, validate(.metal));
    }
    try std.testing.expectEqualSlices(backends.BackendType, &.{.onnx}, blk: {
        var manager = backends.SessionManager.init(std.testing.allocator);
        configureSessionPreference(&manager, .onnx);
        break :blk manager.preferred_backends;
    });
    try std.testing.expectEqualSlices(backends.BackendType, &.{.metal}, blk: {
        var manager = backends.SessionManager.init(std.testing.allocator);
        configureSessionPreference(&manager, .metal);
        break :blk manager.preferred_backends;
    });
}

test "compiledPartitionBackend maps explicit compiled backends" {
    try std.testing.expectEqual(@as(?ops.BackendKind, .onnx), compiledPartitionBackend(.onnx));
    try std.testing.expectEqual(@as(?ops.BackendKind, .pjrt), compiledPartitionBackend(.pjrt));
    try std.testing.expectEqual(@as(?ops.BackendKind, .webgpu), compiledPartitionBackend(.webgpu));
    try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackend(.metal));
    if (build_options.enable_metal) {
        try std.testing.expectEqual(@as(?ops.BackendKind, .metal), compiledPartitionBackendForMode(.metal, true));
    } else {
        try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackendForMode(.metal, true));
    }
    try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackendForMode(.metal, false));
    try std.testing.expectEqual(@as(?ops.BackendKind, .webgpu), compiledPartitionBackendForMode(.webgpu, true));
    try std.testing.expectEqual(@as(?ops.BackendKind, .webgpu), compiledPartitionBackendForMode(.webgpu, false));
}
