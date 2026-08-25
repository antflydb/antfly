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
    xla,
    webgpu,
};

pub fn parse(value: []const u8) ?Choice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    if (std.mem.eql(u8, value, "xla") or std.mem.eql(u8, value, "pjrt")) return .xla;
    if (std.mem.eql(u8, value, "webgpu")) return .webgpu;
    return null;
}

pub fn preferredChoiceFromEnv() Choice {
    if (build_options.enable_wasm or !build_options.link_libc) return .auto;
    const value = platform.env.getenv("ANTFLY_INFERENCE_PREFERRED_BACKEND") orelse
        platform.env.getenv("TERMITE_PREFERRED_BACKEND") orelse
        return .auto;
    return parse(value) orelse .auto;
}

pub fn withPreferredDefault(requested: Choice, preferred: Choice) Choice {
    return if (requested == .auto) preferred else requested;
}

pub fn validate(choice: Choice) !void {
    switch (choice) {
        .onnx => if (!build_options.enable_onnx) return error.BackendUnavailable,
        .native => if (!build_options.enable_native) return error.BackendUnavailable,
        .metal => if (!build_options.enable_metal) return error.BackendUnavailable,
        .cuda => if (!build_options.enable_cuda) return error.BackendUnavailable,
        .xla => if (!build_options.enable_pjrt) return error.BackendUnavailable,
        .webgpu => if (!(build_options.enable_wasm and build_options.enable_webgpu)) return error.BackendUnavailable,
        .auto => {},
    }
}

pub fn configureSessionPreference(session_manager: *backends.SessionManager, choice: Choice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_wasm)
            &.{backends.BackendType.wasm}
        else if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .onnx => &.{backends.BackendType.onnx},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{},
        .webgpu => if (build_options.enable_wasm and build_options.enable_webgpu)
            &.{backends.BackendType.wasm}
        else
            &.{},
        .xla => if (build_options.enable_native)
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
        .xla => .pjrt,
        .auto, .native, .metal, .cuda, .webgpu => null,
    };
}

pub fn compiledPartitionBackendForMode(choice: Choice, compiled_mode_requested: bool) ?ops.BackendKind {
    if (compiled_mode_requested and choice == .metal and build_options.enable_metal) return .metal;
    if (compiled_mode_requested and choice == .webgpu and build_options.enable_wasm and build_options.enable_webgpu) return .webgpu;
    return compiledPartitionBackend(choice);
}

pub fn validateRequiredCompiledBackend(
    session_manager: *const backends.SessionManager,
    compiled_backend: ?ops.BackendKind,
) !void {
    const backend = switch (compiled_backend orelse return) {
        .native => backends.BackendType.native,
        .metal => backends.BackendType.metal,
        .onnx => backends.BackendType.onnx,
        .pjrt => backends.BackendType.pjrt,
        .cuda => backends.BackendType.cuda,
        .wasm, .webgpu => backends.BackendType.wasm,
        .graph => {
            if (session_manager.required_backend_invalid) return error.InvalidRequiredBackend;
            if (session_manager.required_backend != null) return error.RequiredBackendUnavailable;
            return;
        },
    };
    if (!try session_manager.allowsBackend(backend)) return error.RequiredBackendUnavailable;
}

pub fn forcesGraphMode(choice: Choice) bool {
    return compiledPartitionBackend(choice) != null;
}

pub fn pjrtPluginPathFromEnv(allocator: std.mem.Allocator) !?[:0]u8 {
    if (!build_options.enable_pjrt) return null;
    const raw = platform.env.getenv("ANTFLY_INFERENCE_XLA_PLUGIN") orelse
        platform.env.getenv("ANTFLY_INFERENCE_PJRT_PLUGIN") orelse
        platform.env.getenv("TERMITE_XLA_PLUGIN") orelse
        platform.env.getenv("TERMITE_PJRT_PLUGIN") orelse
        platform.env.getenv("PJRT_PLUGIN_PATH") orelse
        platform.env.getenv("PJRT_PLUGIN") orelse
        return null;
    return try allocator.dupeZ(u8, raw);
}

test "parse accepts explicit compiled backends" {
    try std.testing.expectEqual(Choice.onnx, parse("onnx").?);
    try std.testing.expectEqual(Choice.xla, parse("xla").?);
    try std.testing.expectEqual(Choice.xla, parse("pjrt").?);
    try std.testing.expectEqual(Choice.webgpu, parse("webgpu").?);
}

test "preferred default only replaces auto choice" {
    try std.testing.expectEqual(Choice.xla, withPreferredDefault(.auto, .xla));
    try std.testing.expectEqual(Choice.cuda, withPreferredDefault(.cuda, .xla));
}

test "compiledPartitionBackend maps explicit compiled backends" {
    try std.testing.expectEqual(@as(?ops.BackendKind, .onnx), compiledPartitionBackend(.onnx));
    try std.testing.expectEqual(@as(?ops.BackendKind, .pjrt), compiledPartitionBackend(.xla));
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

test "required backend gates compiled backend execution" {
    var manager = backends.SessionManager.init(std.testing.allocator);
    manager.required_backend = .cuda;
    manager.required_backend_invalid = false;
    try validateRequiredCompiledBackend(&manager, null);
    try validateRequiredCompiledBackend(&manager, .cuda);
    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        validateRequiredCompiledBackend(&manager, .onnx),
    );

    manager.required_backend = .pjrt;
    try validateRequiredCompiledBackend(&manager, .pjrt);

    manager.required_backend = null;
    manager.required_backend_invalid = true;
    try std.testing.expectError(
        error.InvalidRequiredBackend,
        validateRequiredCompiledBackend(&manager, .onnx),
    );
    try std.testing.expectError(
        error.InvalidRequiredBackend,
        validateRequiredCompiledBackend(&manager, .graph),
    );
}

test "explicit session preferences never silently fall back" {
    var manager = backends.SessionManager.init(std.testing.allocator);
    configureSessionPreference(&manager, .native);
    try std.testing.expectEqualSlices(backends.BackendType, &.{.native}, manager.preferred_backends);

    configureSessionPreference(&manager, .onnx);
    try std.testing.expectEqualSlices(backends.BackendType, &.{.onnx}, manager.preferred_backends);

    configureSessionPreference(&manager, .metal);
    if (build_options.enable_metal) {
        try std.testing.expectEqualSlices(backends.BackendType, &.{.metal}, manager.preferred_backends);
    } else {
        try std.testing.expectEqual(@as(usize, 0), manager.preferred_backends.len);
    }
}

test "automatic native session selection retains safe fallback" {
    var manager = backends.SessionManager.init(std.testing.allocator);
    configureSessionPreference(&manager, .auto);
    if (build_options.enable_metal and !build_options.enable_wasm) {
        try std.testing.expectEqualSlices(
            backends.BackendType,
            &.{ .metal, .native },
            manager.preferred_backends,
        );
    } else {
        try std.testing.expect(manager.preferred_backends.len > 0);
    }
}
