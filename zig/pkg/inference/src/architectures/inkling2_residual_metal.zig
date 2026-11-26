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

//! Metal residual stack with the checkpoint's loop prelude and cross-pass bank.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const metal = @import("../backends/metal_runtime.zig");
const Tensor = @import("../backends/metal_tensor.zig").MetalTensor;

pub const Stack = struct {
    const capacity = 8;

    backing: Tensor,
    blocks: [capacity]Tensor,
    bank: Tensor,
    bank_blocks: [2]Tensor,
    width: u32,
    rows: u32,
    depth: usize = 0,
    saved_depth: ?usize = null,

    pub fn deviceBytes(width: u32, rows: u32) !usize {
        const count = try std.math.mul(usize, width, rows);
        return std.math.mul(usize, count, (capacity + 2) * @sizeOf(u16));
    }

    pub fn init(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, width: u32, rows: u32) !Stack {
        if (width == 0 or rows == 0) return error.InvalidShape;
        const count = try std.math.mul(usize, width, rows);
        const total = try std.math.mul(usize, count, capacity + 2);
        const extent = std.math.cast(i32, count) orelse return error.InvalidShape;
        var backing = try allocate(a, runtime, total);
        errdefer backing.deinit();
        const row_bytes = count * @sizeOf(u16);
        var bank = try backing.retainedView(capacity * row_bytes, 2 * row_bytes, &.{@intCast(2 * count)});
        errdefer bank.deinit();
        var blocks: [capacity]Tensor = undefined;
        var initialized: usize = 0;
        errdefer for (blocks[0..initialized]) |*block| block.deinit();
        for (&blocks, 0..) |*block, index| {
            block.* = try backing.retainedView(index * count * 2, count * 2, &.{extent});
            initialized += 1;
        }
        var first = try bank.retainedView(0, count * 2, &.{extent});
        errdefer first.deinit();
        const second = try bank.retainedView(count * 2, count * 2, &.{extent});
        return .{
            .backing = backing,
            .blocks = blocks,
            .bank = bank,
            .bank_blocks = .{ first, second },
            .width = width,
            .rows = rows,
        };
    }

    fn allocate(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, count: usize) !Tensor {
        return Tensor.deviceAllocateTyped(a, runtime, .i16, .private, &.{std.math.cast(i32, count) orelse return error.InvalidShape});
    }

    pub fn deinit(self: *Stack) void {
        for (&self.bank_blocks) |*block| block.deinit();
        for (&self.blocks) |*block| block.deinit();
        self.bank.deinit();
        self.backing.deinit();
    }

    pub fn reset(self: *Stack) *Tensor {
        self.depth = 0;
        self.saved_depth = null;
        return &self.blocks[0];
    }

    pub fn current(self: *const Stack) *const Tensor {
        return &self.blocks[self.depth];
    }

    pub fn advance(self: *Stack, visit: checkpoint.Visit, core_start: usize, boundary: bool) !void {
        const action = visit.preludeAction(core_start);
        // Each core pass starts a fresh block. The saved block is read-only
        // while the pass writes the two blocks that follow it.
        if (action != .none and !boundary) return error.InvalidInkling2Schedule;
        switch (action) {
            .none => {},
            .save => {
                self.saved_depth = self.depth;
            },
            .restore => {
                const saved = self.saved_depth orelse return error.InvalidInkling2Schedule;
                if (self.depth != saved + self.bank_blocks.len) return error.InvalidInkling2Schedule;
                const bytes = self.bank.deviceByteLen();
                var loop = try self.backing.retainedView(
                    (saved + 1) * self.blocks[0].deviceByteLen(),
                    bytes,
                    &.{@intCast(bytes / @sizeOf(u16))},
                );
                defer loop.deinit();
                try loop.copyInto(&self.bank);
                self.depth = saved;
            },
        }
    }

    pub fn mix(self: *const Stack, runtime: *metal.RawMetalDecodeRuntime, epsilon: f32, query: *const Tensor, gamma: ?*const Tensor, output: *Tensor) !void {
        try self.mixPartial(runtime, epsilon, self.depth, self.current(), null, query, gamma, output);
    }

    pub fn mixPartial(self: *const Stack, runtime: *metal.RawMetalDecodeRuntime, epsilon: f32, completed: usize, last: *const Tensor, previous: ?*const Tensor, query: *const Tensor, gamma: ?*const Tensor, output: *Tensor) !void {
        if (completed >= capacity) return error.InvalidInkling2Schedule;
        const mixer: metal.ResidualMixBf16 = .{
            .rows = self.rows,
            .width = self.width,
            .sources = @intCast(completed + 1),
            .epsilon = epsilon,
            .norm_epsilon = epsilon,
        };
        try mixer.encode(runtime, .{
            .blocks = &self.backing,
            .last = last,
            .add_last = previous,
            .query = query,
            .gamma = gamma,
        }, output);
    }

    pub fn mixBank(self: *const Stack, runtime: *metal.RawMetalDecodeRuntime, epsilon: f32, query: *const Tensor, output: *Tensor) !void {
        if (self.saved_depth == null) return error.InvalidInkling2Schedule;
        const mixer: metal.ResidualMixBf16 = .{ .rows = self.rows, .width = self.width, .sources = 2, .epsilon = epsilon };
        try mixer.encode(runtime, .{ .blocks = &self.bank_blocks[0], .last = &self.bank_blocks[1], .query = query }, output);
    }

    pub fn finish(self: *Stack, runtime: *metal.RawMetalDecodeRuntime, boundary: bool, attention: *const Tensor, expert: *const Tensor) !void {
        const next = self.depth + @intFromBool(boundary);
        if (next >= capacity) return error.InvalidInkling2Schedule;
        const size = try std.math.mul(u32, self.width, self.rows);
        try metal.residualSumBf16(runtime, size, if (boundary) null else self.current(), attention, expert, &self.blocks[next]);
        self.depth = next;
    }
};

test "Inkling Metal residual stack restores the prelude and retains both cross-pass blocks" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    var stack = try Stack.init(a, runtime, 2, 1);
    defer stack.deinit();
    const initial = [2]u16{ 0x3f80, 0xbf80 };
    const update = [2]u16{ 0x4000, 0x4000 };
    try stack.reset().uploadBytes(std.mem.asBytes(&initial));
    var partial = try Tensor.deviceAllocateTyped(a, runtime, .i16, .shared, &.{2});
    defer partial.deinit();
    try partial.uploadBytes(std.mem.asBytes(&update));
    var query = try Tensor.deviceAllocateTyped(a, runtime, .f32, .shared, &.{2});
    defer query.deinit();
    try query.uploadBytes(std.mem.asBytes(&([2]f32{ 0, 0 })));
    var message = try Tensor.deviceAllocateTyped(a, runtime, .f32, .shared, &.{2});
    defer message.deinit();

    try metal.beginFrame(runtime);
    errdefer metal.cancelFrame(runtime) catch {};
    const visit: checkpoint.Visit = .{ .weight_layer = 6, .pass = 0, .group = .sliding, .slot = 0 };
    try std.testing.expectError(error.InvalidInkling2Schedule, stack.advance(visit, 6, false));
    try stack.advance(visit, 6, true);
    try stack.finish(runtime, true, &partial, &partial);
    try stack.finish(runtime, true, &partial, &partial);
    var replay = visit;
    replay.pass = 1;
    try stack.advance(replay, 6, true);
    try stack.mixBank(runtime, 0.000001, &query, &message);
    try metal.submitFrame(runtime);
    try metal.waitFrame(runtime);
    try std.testing.expectEqual(@as(usize, 0), stack.depth);
    var restored: [2]u16 = undefined;
    try stack.current().downloadBytesInto(std.mem.asBytes(&restored));
    try std.testing.expectEqual(initial, restored);
    var mixed: [2]f32 = undefined;
    try message.downloadF32Into(&mixed);
    for (mixed) |value| try std.testing.expectApproxEqAbs(@as(f32, 1), value, 0.00001);
}
