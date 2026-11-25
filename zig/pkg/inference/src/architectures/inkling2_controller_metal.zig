// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Metal checkpoint bindings for a loop-controller stream.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const metal = @import("../backends/metal_runtime.zig");
const Tensor = @import("../backends/metal_tensor.zig").MetalTensor;

pub const Controller = struct {
    down: Tensor,
    gates: Tensor,
    width: u32,
    rank: u32,

    pub fn load(allocator: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: *const checkpoint.Checkpoint, layer: usize, stream: checkpoint.ControllerStream) !Controller {
        const text = source.config.value.text_config;
        return fromWeights(allocator, runtime, text.hidden_size, text.loop_config.gate_rank, try source.controller(layer, stream));
    }

    pub fn fromWeights(allocator: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, width: u32, rank: u32, weights: checkpoint.ControllerWeights) !Controller {
        const gate_bytes = try std.math.mul(usize, try std.math.mul(usize, width, rank), 2);
        const down_bytes = try std.math.mul(usize, gate_bytes, 2);
        if (gate_bytes == 0 or weights.down.len != down_bytes or weights.gain.len != gate_bytes or weights.forget.len != gate_bytes)
            return error.InvalidShape;
        const elements = std.math.cast(i32, gate_bytes) orelse return error.InvalidShape;
        var down = try Tensor.deviceAllocateTyped(allocator, runtime, .i16, .shared, &.{elements});
        errdefer down.deinit();
        try down.uploadBytes(weights.down);
        var gates = try Tensor.deviceAllocateTyped(allocator, runtime, .i16, .shared, &.{elements});
        errdefer gates.deinit();
        for ([_][]const u8{ weights.gain, weights.forget }, 0..) |bytes, index| {
            var view = try gates.retainedView(index * gate_bytes, gate_bytes, &.{@intCast(gate_bytes / 2)});
            defer view.deinit();
            try view.uploadBytes(bytes);
        }
        return .{ .down = down, .gates = gates, .width = width, .rank = rank };
    }

    pub fn deinit(self: *Controller) void {
        self.gates.deinit();
        self.down.deinit();
    }

    pub fn encode(
        self: *const Controller,
        runtime: *metal.RawMetalDecodeRuntime,
        rows: u32,
        epsilon: f32,
        destination: *const Tensor,
        message: *const Tensor,
        gamma: *const Tensor,
        scratch: metal.GatedResidualBf16.Workspace,
        output: *Tensor,
    ) !void {
        const controller: metal.GatedResidualBf16 = .{ .rows = rows, .width = self.width, .rank = self.rank, .epsilon = epsilon };
        try controller.encode(runtime, destination, message, .{ .down = &self.down, .gates = &self.gates, .gamma = gamma }, scratch, output);
    }
};

test "Inkling Metal controller checkpoint packs exact BF16 gain and forget rows" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const down: [4]u16 = .{ 0x3f80, 0, 0, 0 };
    const gain: [2]u16 = .{ 0x3f00, 0x4000 };
    const forget: [2]u16 = .{ 0xbf80, 0x4040 };
    const weights: checkpoint.ControllerWeights = .{
        .down = std.mem.asBytes(&down),
        .gain = std.mem.asBytes(&gain),
        .forget = std.mem.asBytes(&forget),
    };
    var controller = try Controller.fromWeights(std.testing.allocator, runtime, 2, 1, weights);
    defer controller.deinit();
    var packed_gates: [4]u16 = undefined;
    try controller.gates.downloadBytesInto(std.mem.asBytes(&packed_gates));
    try std.testing.expectEqual(gain ++ forget, packed_gates);
    var copied_down: [4]u16 = undefined;
    try controller.down.downloadBytesInto(std.mem.asBytes(&copied_down));
    try std.testing.expectEqual(down, copied_down);
}
