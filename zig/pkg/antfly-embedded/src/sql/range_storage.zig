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

//! Typed range JSON/physical boundaries. One temporary region owns parsing
//! scratch; returned physical bytes/text are independently owned by the caller.
const std = @import("std");
const ranges = @import("range_value.zig");
pub fn encodeJsonAlloc(a: std.mem.Allocator, kind: ranges.Kind, input: std.json.Value) ![]u8 {
    if (input != .string) return error.InvalidBatchRequest;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var parse: ranges.Context = .{ .alloc = scratch.allocator() };
    const value = try ranges.parseLeaky(&parse, kind, input.string);
    var encode: ranges.Context = .{ .alloc = a, .parent = &parse };
    return ranges.encodeLeaky(&encode, value);
}
pub fn jsonValueAlloc(a: std.mem.Allocator, kind: ranges.Kind, bytes: []const u8) !std.json.Value {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var context: ranges.Context = .{ .alloc = scratch.allocator() };
    const value = try ranges.decodeCanonicalLeaky(&context, kind, bytes);
    return .{ .string = try a.dupe(u8, try ranges.formatLeaky(&context, value)) };
}
pub fn validateCanonical(_: std.mem.Allocator, kind: ranges.Kind, bytes: []const u8) !void {
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var context: ranges.Context = .{ .alloc = none.allocator() };
    _ = try @import("range_binary.zig").View.open(&context, kind, bytes);
}
