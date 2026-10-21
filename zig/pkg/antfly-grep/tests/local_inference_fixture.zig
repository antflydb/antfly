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

//! Deterministic ABI fixture; does not implement or evaluate a real model.
const std = @import("std");
const Slice = extern struct { ptr: ?[*]const u8, len: usize };
const Buffer = extern struct { ptr: ?[*]u8 = null, len: usize = 0 };
const Options = extern struct { abi_size: u32, flags: u32, models_dir: Slice, budgets: [6]u32, timeout: u64, reserved: [8]u64 };
pub export fn antfly_abi_version() u32 {
    return 3;
}
pub export fn antfly_inference_options_size() u32 {
    return @sizeOf(Options);
}
pub export fn antfly_inference_options_init(out: *Options) c_int {
    out.* = std.mem.zeroes(Options);
    out.abi_size = @sizeOf(Options);
    return 0;
}
pub export fn antfly_inference_open(options: *const Options, out: *?*anyopaque) c_int {
    if (options.abi_size != @sizeOf(Options)) return 1;
    out.* = @ptrFromInt(1);
    return 0;
}
pub export fn antfly_inference_close(_: ?*anyopaque) void {}
pub export fn antfly_buffer_free(out: *Buffer) void {
    if (out.ptr) |ptr| std.heap.page_allocator.free(ptr[0..out.len]);
    out.* = .{};
}
pub export fn antfly_inference_decide_json(_: ?*anyopaque, body: Slice, out: *Buffer) c_int {
    return invoke(body, out) catch 255;
}
fn invoke(body: Slice, out: *Buffer) !c_int {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parsed = try std.json.parseFromSlice(std.json.Value, a, body.ptr.?[0..body.len], .{});
    const inputs = parsed.value.object.get("inputs").?.array.items;
    const Answer = struct { name: []const u8 = "relevant", type: []const u8 = "predicate", decision_method: []const u8 = "typed", probability: f64 };
    const Row = struct { input_index: usize, id: []const u8, answers: []Answer };
    const rows = try a.alloc(Row, inputs.len);
    for (inputs, 0..) |input, i| {
        const text = input.object.get("input").?.string;
        const questions = parsed.value.object.get("questions").?.array.items;
        const answers = try a.alloc(Answer, questions.len);
        for (questions, answers) |question, *answer| answer.* = .{ .name = question.object.get("name").?.string, .probability = if (std.mem.indexOf(u8, text, "STOP") != null) 0.9 else 0.1 };
        rows[i] = .{ .input_index = i, .id = input.object.get("id").?.string, .answers = answers };
    }
    const encoded = try std.json.Stringify.valueAlloc(std.heap.page_allocator, .{ .data = rows }, .{});
    out.* = .{ .ptr = encoded.ptr, .len = encoded.len };
    return 0;
}
