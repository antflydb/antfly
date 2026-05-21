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
const supports_onnx_models = !build_options.enable_wasm;

pub const Choice = enum {
    auto,
    onnx,
    native,
    metal,
    mlx,
    cuda,
    xla,
    webgpu,
};

pub fn parse(value: []const u8) ?Choice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    if (std.mem.eql(u8, value, "xla")) return .xla;
    if (std.mem.eql(u8, value, "webgpu")) return .webgpu;
    return null;
}

pub fn validate(choice: Choice) !void {
    switch (choice) {
        .onnx => if (!supports_onnx_models) return error.BackendUnavailable,
        .cuda => if (!build_options.enable_cuda) return error.BackendUnavailable,
        .xla => if (!build_options.enable_pjrt) return error.BackendUnavailable,
        .webgpu => if (!(build_options.enable_wasm and build_options.enable_webgpu)) return error.BackendUnavailable,
        .auto, .native, .metal, .mlx => {},
    }
}

pub fn validateRuntime(choice: Choice) !void {
    try validate(choice);
    if (choice == .cuda and !backends.gpu_inventory.cudaRuntimeAvailable()) return error.CudaRuntimeUnavailable;
}

pub fn configureSessionPreference(session_manager: *backends.SessionManager, choice: Choice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (backends.gpu_inventory.cudaRuntimeAvailable() and build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.cuda, backends.BackendType.metal, backends.BackendType.mlx, backends.BackendType.native }
        else if (backends.gpu_inventory.cudaRuntimeAvailable() and build_options.enable_metal)
            &.{ backends.BackendType.cuda, backends.BackendType.metal, backends.BackendType.native }
        else if (backends.gpu_inventory.cudaRuntimeAvailable() and build_options.enable_mlx)
            &.{ backends.BackendType.cuda, backends.BackendType.mlx, backends.BackendType.native }
        else if (backends.gpu_inventory.cudaRuntimeAvailable())
            &.{ backends.BackendType.cuda, backends.BackendType.native }
        else if (build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.metal, backends.BackendType.mlx, backends.BackendType.native }
        else if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else if (build_options.enable_mlx)
            &.{ backends.BackendType.mlx, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .onnx => if (supports_onnx_models and build_options.enable_native)
            &.{ backends.BackendType.onnx, backends.BackendType.native }
        else if (supports_onnx_models and build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.onnx, backends.BackendType.metal, backends.BackendType.mlx }
        else if (supports_onnx_models and build_options.enable_metal)
            &.{ backends.BackendType.onnx, backends.BackendType.metal }
        else if (supports_onnx_models and build_options.enable_mlx)
            &.{ backends.BackendType.onnx, backends.BackendType.mlx }
        else if (supports_onnx_models)
            &.{backends.BackendType.onnx}
        else if (build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.metal, backends.BackendType.mlx }
        else if (build_options.enable_metal)
            &.{backends.BackendType.metal}
        else if (build_options.enable_mlx)
            &.{backends.BackendType.mlx}
        else
            &.{backends.BackendType.native},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
        .mlx => if (build_options.enable_mlx) &.{backends.BackendType.mlx} else &.{backends.BackendType.native},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{backends.BackendType.native},
        .webgpu => if (build_options.enable_wasm and build_options.enable_webgpu)
            &.{backends.BackendType.wasm}
        else
            &.{},
        .xla => if (build_options.enable_native)
            &.{backends.BackendType.native}
        else if (build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.metal, backends.BackendType.mlx }
        else if (build_options.enable_metal)
            &.{backends.BackendType.metal}
        else if (build_options.enable_mlx)
            &.{backends.BackendType.mlx}
        else
            &.{},
    };
}

pub fn compiledPartitionBackend(choice: Choice) ?ops.BackendKind {
    return switch (choice) {
        .onnx => .onnx,
        .xla => .pjrt,
        .auto, .native, .metal, .mlx, .cuda, .webgpu => null,
    };
}

pub fn compiledPartitionBackendForMode(choice: Choice, compiled_mode_requested: bool) ?ops.BackendKind {
    if (compiled_mode_requested and choice == .metal and build_options.enable_metal) return .metal;
    if (compiled_mode_requested and choice == .webgpu and build_options.enable_wasm and build_options.enable_webgpu) return .webgpu;
    return compiledPartitionBackend(choice);
}

pub fn forcesGraphMode(choice: Choice) bool {
    return compiledPartitionBackend(choice) != null;
}

pub fn pjrtPluginPathFromEnv(allocator: std.mem.Allocator) !?[:0]u8 {
    if (!build_options.enable_pjrt) return null;
    const raw = platform.env.getenv("TERMITE_XLA_PLUGIN") orelse
        platform.env.getenv("TERMITE_PJRT_PLUGIN") orelse
        platform.env.getenv("PJRT_PLUGIN_PATH") orelse
        platform.env.getenv("PJRT_PLUGIN") orelse
        return null;
    return try allocator.dupeZ(u8, raw);
}

test "parse accepts explicit compiled backends" {
    try std.testing.expectEqual(Choice.onnx, parse("onnx").?);
    try std.testing.expectEqual(Choice.xla, parse("xla").?);
    try std.testing.expectEqual(Choice.webgpu, parse("webgpu").?);
}

test "compiledPartitionBackend maps explicit compiled backends" {
    try std.testing.expectEqual(@as(?ops.BackendKind, .onnx), compiledPartitionBackend(.onnx));
    try std.testing.expectEqual(@as(?ops.BackendKind, .pjrt), compiledPartitionBackend(.xla));
    try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackend(.mlx));
    try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackend(.metal));
    try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackend(.webgpu));
    if (build_options.enable_metal) {
        try std.testing.expectEqual(@as(?ops.BackendKind, .metal), compiledPartitionBackendForMode(.metal, true));
    } else {
        try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackendForMode(.metal, true));
    }
    try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackendForMode(.metal, false));
    if (build_options.enable_wasm and build_options.enable_webgpu) {
        try std.testing.expectEqual(@as(?ops.BackendKind, .webgpu), compiledPartitionBackendForMode(.webgpu, true));
    } else {
        try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackendForMode(.webgpu, true));
    }
    try std.testing.expectEqual(@as(?ops.BackendKind, null), compiledPartitionBackendForMode(.webgpu, false));
}
