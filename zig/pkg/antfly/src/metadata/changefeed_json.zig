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

//! Bounded-work JSON emission for already validated canonical projection values.
//! Each node is visited once; nesting consumes an explicit arena work stack,
//! not native recursion or Stringify's fixed-depth checked nesting stack.
const std = @import("std");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const Value = struct {
    a: A,
    value: Json,
    pub fn jsonStringify(self: Value, out: *std.json.Stringify) std.Io.Writer.Error!void {
        try out.beginWriteRaw();
        // valueAlloc maps WriteFailed back to OutOfMemory. Our counting sink
        // cannot otherwise fail; its wrapper below preserves the same identity.
        self.emit(out.writer) catch return error.WriteFailed;
        out.endWriteRaw();
    }
    fn emit(self: Value, out: *std.Io.Writer) !void {
        const Task = union(enum) { value: Json, field: struct { name: []const u8, value: Json }, literal: []const u8 };
        var tasks: std.ArrayList(Task) = .empty;
        defer tasks.deinit(self.a);
        try tasks.append(self.a, .{ .value = self.value });
        while (tasks.pop()) |task| switch (task) {
            .literal => |bytes| try out.writeAll(bytes),
            .field => |field| {
                try std.json.Stringify.value(field.name, .{}, out);
                try out.writeByte(':');
                try tasks.append(self.a, .{ .value = field.value });
            },
            .value => |value| switch (value) {
                .object => |object| {
                    try out.writeByte('{');
                    try tasks.append(self.a, .{ .literal = "}" });
                    var index = object.count();
                    while (index != 0) {
                        index -= 1;
                        try tasks.append(self.a, .{ .field = .{ .name = object.keys()[index], .value = object.values()[index] } });
                        if (index != 0) try tasks.append(self.a, .{ .literal = "," });
                    }
                },
                .array => |array| {
                    try out.writeByte('[');
                    try tasks.append(self.a, .{ .literal = "]" });
                    var index = array.items.len;
                    while (index != 0) {
                        index -= 1;
                        try tasks.append(self.a, .{ .value = array.items[index] });
                        if (index != 0) try tasks.append(self.a, .{ .literal = "," });
                    }
                },
                else => try std.json.Stringify.value(value, .{}, out),
            },
        };
    }
};
pub const Raw = struct {
    value: []const u8,
    pub fn jsonStringify(self: Raw, writer: *std.json.Stringify) std.Io.Writer.Error!void {
        try writer.print("{s}", .{self.value});
    }
};
/// No output allocation occurs during the byte preflight. A callback's only
/// possible failure with a discarding writer is its explicit work-stack OOM.
pub fn size(value: anytype) !u64 {
    var buffer: [256]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    std.json.Stringify.value(value, .{}, &counter.writer) catch return error.OutOfMemory;
    return counter.fullCount();
}

test "feed JSON emits deeply nested canonical values without recursive formatter state" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var value: Json = .{ .number_string = "18446744073709551617" };
    for (0..3000) |_| {
        var array = std.json.Array.init(scratch);
        try array.append(value);
        value = .{ .array = array };
    }
    const wrapped = .{ .payload = Value{ .a = scratch, .value = value } };
    const bytes = try std.json.Stringify.valueAlloc(a, wrapped, .{});
    defer a.free(bytes);
    try std.testing.expectEqual(try size(wrapped), bytes.len);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "18446744073709551617") != null);
    try std.testing.expectEqual(@as(usize, 3000), std.mem.count(u8, bytes, "["));
    try std.testing.expectEqual(@as(usize, 3000), std.mem.count(u8, bytes, "]"));
}

test "feed JSON preserves punctuation escaping empty containers and exact decimals" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Json, a, "{\"a\\\"\":[{},[],null,true,1.00e999999999999999999999999,\"x\\n\\u0000\"]}", .{ .parse_numbers = false });
    defer parsed.deinit();
    const expected = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(expected);
    const bytes = try std.json.Stringify.valueAlloc(a, Value{ .a = a, .value = parsed.value }, .{});
    defer a.free(bytes);
    try std.testing.expectEqualStrings(expected, bytes);
    const array = [_]Raw{ .{ .value = "{\"one\":1}" }, .{ .value = "{\"two\":2}" } };
    const envelope = try std.json.Stringify.valueAlloc(a, .{ .events = &array }, .{});
    defer a.free(envelope);
    try std.testing.expectEqualStrings("{\"events\":[{\"one\":1},{\"two\":2}]}", envelope);
}
fn allocationScenario(a: A) !void {
    const wrapped = Value{ .a = a, .value = .{ .string = "known" } };
    _ = try size(wrapped);
    const bytes = try std.json.Stringify.valueAlloc(a, wrapped, .{});
    defer a.free(bytes);
}
test "feed JSON counting and output preserve allocation failures and release work stacks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationScenario, .{});
}
