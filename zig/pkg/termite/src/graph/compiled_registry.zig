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
const contracts = @import("backend_contracts.zig");
const compiled_backend = @import("compiled_backend.zig");
const compiled_pjrt = if (build_options.enable_pjrt) @import("compiled_pjrt.zig") else struct {};
const compiled_onnx = if (build_options.enable_onnx) @import("compiled_onnx.zig") else struct {};
const compiled_metal = if (build_options.enable_metal) @import("compiled_metal.zig") else struct {};
const compiled_webgpu = if (build_options.enable_wasm and build_options.enable_webgpu) @import("compiled_webgpu.zig") else struct {};

pub fn all() []const compiled_backend.Definition {
    return switch ((@as(u4, @intFromBool(build_options.enable_pjrt)) << 3) |
        (@as(u4, @intFromBool(build_options.enable_onnx)) << 2) |
        (@as(u4, @intFromBool(build_options.enable_metal)) << 1) |
        @as(u4, @intFromBool(build_options.enable_wasm and build_options.enable_webgpu))) {
        0b0000 => &.{},
        0b0001 => &.{compiled_webgpu.backend},
        0b0010 => &.{compiled_metal.backend},
        0b0011 => &.{ compiled_metal.backend, compiled_webgpu.backend },
        0b0100 => &.{compiled_onnx.backend},
        0b0101 => &.{ compiled_onnx.backend, compiled_webgpu.backend },
        0b0110 => &.{ compiled_metal.backend, compiled_onnx.backend },
        0b0111 => &.{ compiled_metal.backend, compiled_onnx.backend, compiled_webgpu.backend },
        0b1000 => &.{compiled_pjrt.backend},
        0b1001 => &.{ compiled_pjrt.backend, compiled_webgpu.backend },
        0b1010 => &.{ compiled_metal.backend, compiled_pjrt.backend },
        0b1011 => &.{ compiled_metal.backend, compiled_pjrt.backend, compiled_webgpu.backend },
        0b1100 => &.{ compiled_pjrt.backend, compiled_onnx.backend },
        0b1101 => &.{ compiled_pjrt.backend, compiled_onnx.backend, compiled_webgpu.backend },
        0b1110 => &.{ compiled_metal.backend, compiled_pjrt.backend, compiled_onnx.backend },
        0b1111 => &.{ compiled_metal.backend, compiled_pjrt.backend, compiled_onnx.backend, compiled_webgpu.backend },
    };
}

pub fn find(kind: contracts.BackendKind) ?compiled_backend.Definition {
    for (all()) |definition| {
        if (definition.kind == kind) return definition;
    }
    return null;
}

test "compiled backend registry ignores non-compiled host backends" {
    try std.testing.expect(find(.native) == null);
    try std.testing.expect(find(.metal) == null);
}

test "compiled backend registry returns enabled compiled backends" {
    if (build_options.enable_pjrt) {
        try std.testing.expect(find(.pjrt) != null);
        try std.testing.expectEqual(compiled_backend.ModelRuntimeStrategy.inline_compiled_graph, find(.pjrt).?.model_runtime_strategy);
    } else {
        try std.testing.expect(find(.pjrt) == null);
    }

    if (build_options.enable_onnx) {
        try std.testing.expect(find(.onnx) != null);
        try std.testing.expectEqual(compiled_backend.ModelRuntimeStrategy.offline_artifact, find(.onnx).?.model_runtime_strategy);
    } else {
        try std.testing.expect(find(.onnx) == null);
    }

    if (build_options.enable_metal) {
        try std.testing.expect(find(.metal) != null);
        try std.testing.expectEqual(compiled_backend.ModelRuntimeStrategy.direct_session, find(.metal).?.model_runtime_strategy);
    } else {
        try std.testing.expect(find(.metal) == null);
    }

    if (build_options.enable_wasm and build_options.enable_webgpu) {
        try std.testing.expect(find(.webgpu) != null);
        try std.testing.expectEqual(compiled_backend.ModelRuntimeStrategy.none, find(.webgpu).?.model_runtime_strategy);
    } else {
        try std.testing.expect(find(.webgpu) == null);
    }
}
