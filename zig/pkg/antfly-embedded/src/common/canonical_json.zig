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

//! Streaming typed-JSON normalization for already strictly validated sources.
//! Retains no payload DOM; numbers use the same parser and formatter as the VM.
const std = @import("std");
pub const Options = struct {
    depth: usize = 256,
    preserve_strings: bool = false,
    context: ?*anyopaque = null,
    value: ?*const fn (?*anyopaque, usize) anyerror!void = null,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
};
pub fn write(a: std.mem.Allocator, raw: []const u8, writer: *std.Io.Writer, options: Options) !void {
    var scanner = std.json.Scanner.initCompleteInput(a, raw);
    defer scanner.deinit();
    var out: std.json.Stringify = .{ .writer = writer };
    var objects: [256]bool = undefined;
    var keys: [256]bool = undefined;
    var depth: usize = 0;
    var fragment = false;
    var root = false;
    var tokens: usize = 0;
    var string_start: usize = 0;
    while (true) {
        const token_start = scanner.cursor;
        const token = try scanner.next();
        if (token == .end_of_document) {
            if (!root or depth != 0 or fragment) return error.SyntaxError;
            return;
        }
        tokens += 1;
        if (tokens % 256 == 0) if (options.checkpoint) |poll| try poll(options.context);
        if (token == .object_end or token == .array_end) {
            if (fragment or depth == 0 or objects[depth - 1] != (token == .object_end)) return error.SyntaxError;
            depth -= 1;
            if (token == .object_end) try out.endObject() else try out.endArray();
            continue;
        }
        const string = switch (token) {
            .string, .partial_string, .partial_string_escaped_1, .partial_string_escaped_2, .partial_string_escaped_3, .partial_string_escaped_4 => true,
            else => false,
        };
        const key = string and depth != 0 and objects[depth - 1] and keys[depth - 1];
        if (!fragment) {
            if (!key) {
                if (depth == 0) {
                    if (root) return error.SyntaxError;
                    root = true;
                }
                if (depth >= options.depth or depth >= objects.len) return error.SqlProgramLimitExceeded;
                if (options.value) |charge| try charge(options.context, depth);
            }
            if (string) {
                if (key) try out.beginObjectFieldRaw() else try out.beginWriteRaw();
                if (options.preserve_strings) string_start = std.mem.indexOfScalarPos(u8, raw, token_start, '"') orelse return error.SyntaxError else try writer.writeByte('"');
            }
        }
        switch (token) {
            .string => |chars| {
                if (options.preserve_strings) try writer.writeAll(raw[string_start..scanner.cursor]) else {
                    try writeChars(writer, chars);
                    try writer.writeByte('"');
                }
                if (key) out.endObjectFieldRaw() else out.endWriteRaw();
                fragment = false;
            },
            .partial_string => |chars| {
                if (!options.preserve_strings) try writeChars(writer, chars);
                fragment = true;
            },
            .partial_string_escaped_1 => |chars| {
                if (!options.preserve_strings) try std.json.Stringify.encodeJsonStringChars(&chars, .{}, writer);
                fragment = true;
            },
            .partial_string_escaped_2 => |chars| {
                if (!options.preserve_strings) try writer.writeAll(&chars);
                fragment = true;
            },
            .partial_string_escaped_3 => |chars| {
                if (!options.preserve_strings) try writer.writeAll(&chars);
                fragment = true;
            },
            .partial_string_escaped_4 => |chars| {
                if (!options.preserve_strings) try writer.writeAll(&chars);
                fragment = true;
            },
            .number => |number| try out.write(std.json.Value.parseFromNumberSlice(number)),
            .true => try out.write(true),
            .false => try out.write(false),
            .null => try out.write(null),
            .object_begin, .array_begin => {},
            else => return error.SyntaxError,
        }
        if (fragment) continue;
        if (depth != 0 and objects[depth - 1]) keys[depth - 1] = !key;
        if (token == .object_begin or token == .array_begin) {
            objects[depth] = token == .object_begin;
            keys[depth] = true;
            depth += 1;
            if (token == .object_begin) try out.beginObject() else try out.beginArray();
        }
    }
}
// Scanner fragments can start with a decoded \" or \\. SIMD-search the
// common plain fragment; only special fragments need the encoder's byte loop.
fn writeChars(writer: *std.Io.Writer, chars: []const u8) !void {
    if (std.mem.indexOfAny(u8, chars, "\"\\") == null) return writer.writeAll(chars);
    try std.json.Stringify.encodeJsonStringChars(chars, .{}, writer);
}
/// Allocate only when canonical bytes first differ; unchanged values borrow raw.
pub fn normalize(a: std.mem.Allocator, raw: []const u8, max_bytes: usize, options: Options) ![]const u8 {
    var changed: ChangeWriter = .{ .a = a, .raw = raw, .limit = max_bytes };
    defer if (changed.output) |*output| output.deinit();
    write(a, raw, &changed.writer, options) catch |err| return if (changed.failure) |failure| (if (failure == error.WriteFailed) error.OutOfMemory else failure) else err;
    if (changed.output) |*output| return output.toOwnedSlice();
    return raw[0..changed.position];
}
const ChangeWriter = struct {
    a: std.mem.Allocator,
    raw: []const u8,
    limit: usize,
    position: usize = 0,
    failure: ?anyerror = null,
    output: ?std.Io.Writer.Allocating = null,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    fn append(self: *@This(), bytes: []const u8) !void {
        if (bytes.len > self.limit -| self.position) return error.SqlProgramLimitExceeded;
        if (self.output == null and (self.position > self.raw.len or bytes.len > self.raw.len -| self.position or !std.mem.eql(u8, bytes, self.raw[self.position..][0..bytes.len]))) {
            self.output = .init(self.a);
            try self.output.?.writer.writeAll(self.raw[0..self.position]);
        }
        if (self.output) |*output| try output.writer.writeAll(bytes);
        self.position += bytes.len;
    }
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
        var count: usize = 0;
        for (data[0 .. data.len - 1]) |bytes| {
            self.append(bytes) catch |err| {
                self.failure = err;
                return error.WriteFailed;
            };
            count += bytes.len;
        }
        for (0..splat) |_| {
            const bytes = data[data.len - 1];
            self.append(bytes) catch |err| {
                self.failure = err;
                return error.WriteFailed;
            };
            count += bytes.len;
        }
        return count;
    }
};
test "streaming canonical JSON matches typed numbers and borrows unchanged strings" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "1e-400", "1.234567890123456789", "18446744073709551615", "-0.0", "{\"n\":1e-400,\"a\":[1.234567890123456789,\"x\\n\\uD83D\\uDE80\"]}", "[true,false,null,{}]" }) |raw| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer parsed.deinit();
        const expected = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(expected);
        const actual = try normalize(a, raw, 1024 * 1024, .{});
        defer if (actual.ptr != raw.ptr) a.free(actual);
        try std.testing.expectEqualStrings(expected, actual);
    }
    const plain = "\"ordinary string\"";
    const borrowed = try normalize(a, plain, 1024, .{});
    try std.testing.expect(borrowed.ptr == plain.ptr);
}

test "streaming canonical fragments preserve every ASCII escape" {
    const a = std.testing.allocator;
    var chars: [128]u8 = undefined;
    for (&chars, 0..) |*char, i| char.* = @intCast(i);
    const raw = try std.json.Stringify.valueAlloc(a, .{ .text = &chars }, .{});
    defer a.free(raw);
    const normalized = try normalize(a, raw, 4096, .{});
    defer if (normalized.ptr != raw.ptr) a.free(normalized);
    try std.testing.expectEqualStrings(raw, normalized);
    try std.testing.expect(normalized.ptr == raw.ptr);
}
