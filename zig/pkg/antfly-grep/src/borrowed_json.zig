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

//! Arena-owned containers over a retained response. std.json.Value's dynamic
//! parser always copies strings, even with allocate=alloc_if_needed. Consume
//! validated Scanner tokens directly so plain strings borrow their backing.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
pub fn parse(a: A, bytes: []const u8) !V {
    var scanner = std.json.Scanner.initCompleteInput(a, bytes);
    defer scanner.deinit();
    const result = try value(a, &scanner, try scanner.nextAllocMax(a, .alloc_if_needed, bytes.len), bytes.len, 0);
    if (try scanner.next() != .end_of_document) return error.SyntaxError;
    return result;
}
fn value(a: A, scanner: *std.json.Scanner, token: std.json.Token, max: usize, depth: usize) anyerror!V {
    if (depth > 256) return error.JsonNestingTooDeep;
    return switch (token) {
        .null => .null,
        .true => .{ .bool = true },
        .false => .{ .bool = false },
        .string, .allocated_string => |bytes| .{ .string = bytes },
        .number, .allocated_number => |bytes| V.parseFromNumberSlice(bytes),
        .array_begin => blk: {
            var result: V = .{ .array = std.json.Array.init(a) };
            while (true) {
                const next = try scanner.nextAllocMax(a, .alloc_if_needed, max);
                if (next == .array_end) break;
                try result.array.append(try value(a, scanner, next, max, depth + 1));
            }
            break :blk result;
        },
        .object_begin => blk: {
            var result: V = .{ .object = .empty };
            while (true) {
                const next = try scanner.nextAllocMax(a, .alloc_if_needed, max);
                if (next == .object_end) break;
                const key = switch (next) {
                    .string, .allocated_string => |bytes| bytes,
                    else => return error.SyntaxError,
                };
                const slot = try result.object.getOrPut(a, key);
                if (slot.found_existing) return error.DuplicateField;
                slot.value_ptr.* = try value(a, scanner, try scanner.nextAllocMax(a, .alloc_if_needed, max), max, depth + 1);
            }
            break :blk result;
        },
        else => error.SyntaxError,
    };
}

test "response JSON borrows plain strings, owns escapes and rejects ambiguous input" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = "{\"plain\":\"data:image/png;base64,AAAA==\",\"escaped\":\"line\\ntext\",\"array\":[true,null,12.5]}";
    const parsed = try parse(a, bytes);
    const plain = parsed.object.get("plain").?.string;
    try std.testing.expect(@intFromPtr(plain.ptr) >= @intFromPtr(bytes.ptr) and @intFromPtr(plain.ptr) < @intFromPtr(bytes.ptr) + bytes.len);
    try std.testing.expectEqualStrings("line\ntext", parsed.object.get("escaped").?.string);
    try std.testing.expectEqual(@as(f64, 12.5), parsed.object.get("array").?.array.items[2].float);
    try std.testing.expectError(error.DuplicateField, parse(a, "{\"a\":1,\"a\":2}"));
    try std.testing.expectError(error.SyntaxError, parse(a, "{} []"));
    try std.testing.expectError(error.SyntaxError, parse(a, "[1,]"));
}

test "borrowed response parser agrees with the owning parser on values and malformed input" {
    const cases = [_][]const u8{
        "null",                                             "true", "false",                   "-0",          "1.25e-3",  "18446744073709551615", "1e999",
        "{\"a\":[1,2,{\"b\":\"é\\uD83D\\uDE00\"}],\"c\":\"\\\"\\\\\\t\"}",
        "{\"line\":\"\\u0061\",\"empty\":{},\"other\":[]}", "",     " ",                       "null true",   "[1,]",     "{\"a\":}",             "{\"a\":1,}",
        "[01]",                                             "[1e]", "{\"a\":1,\"\\u0061\":2}", "\"\\uD800\"", "\"\xff\"", "\"line\ntext\"",
    };
    for (cases) |bytes| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const owning = std.json.parseFromSlice(V, a, bytes, .{}) catch {
            if (parse(a, bytes)) |_| return error.AcceptedMalformedResponse else |_| {}
            continue;
        };
        defer owning.deinit();
        const borrowed = try parse(a, bytes);
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, owning.value, .{}), try std.json.Stringify.valueAlloc(a, borrowed, .{}));
    }
    var deeply_nested: [600]u8 = undefined;
    @memset(deeply_nested[0..300], '[');
    @memset(deeply_nested[300..], ']');
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.JsonNestingTooDeep, parse(arena.allocator(), &deeply_nested));
    try std.testing.expectError(error.OutOfMemory, parse(std.testing.failing_allocator, "{\"a\":1}"));
}
