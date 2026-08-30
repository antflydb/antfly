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

const Allocator = std.mem.Allocator;

/// Maximum nesting accepted when raw JSON is serialized. Public Antfly query
/// admission is substantially tighter; this larger generic bound prevents a
/// caller-constructed value from turning validation into unbounded stack state.
pub const max_raw_nesting: usize = 256;

pub const RawValue = Raw(.any);
pub const RawObject = Raw(.object);

const RootKind = enum {
    any,
    object,
};

/// Compact, owned JSON bytes with parsing and serialization hooks for
/// std.json. `bytes` remains public for ordinary Zig ergonomics, so
/// jsonStringify performs a final allocation-free validation pass before raw
/// emission. This makes direct struct initialization safe at the wire boundary.
fn Raw(comptime root_kind: RootKind) type {
    return struct {
        bytes: []const u8,

        const Self = @This();

        pub fn parseOwned(allocator: Allocator, input: []const u8) !Self {
            var scanner = std.json.Scanner.initCompleteInput(allocator, input);
            defer scanner.deinit();
            var result = try jsonParse(allocator, &scanner, .{ .max_value_len = input.len });
            errdefer result.deinit(allocator);
            if (try scanner.next() != .end_of_document) return error.UnexpectedToken;
            return result;
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            allocator.free(self.bytes);
            self.* = undefined;
        }

        pub fn jsonParse(
            allocator: Allocator,
            source: anytype,
            options: std.json.ParseOptions,
        ) !Self {
            if (root_kind == .object and try source.peekNextTokenType() != .object_begin)
                return error.UnexpectedToken;

            var output: std.Io.Writer.Allocating = .init(allocator);
            errdefer output.deinit();
            var stringify: std.json.Stringify = .{ .writer = &output.writer };
            copyValue(allocator, source, options, &stringify, 0) catch |err| switch (err) {
                error.WriteFailed => return error.OutOfMemory,
                else => |parse_err| return parse_err,
            };
            return .{ .bytes = try output.toOwnedSlice() };
        }

        pub fn jsonParseFromValue(
            allocator: Allocator,
            source: std.json.Value,
            _: std.json.ParseOptions,
        ) !Self {
            if (root_kind == .object and source != .object) return error.UnexpectedToken;
            const bytes = try std.json.Stringify.valueAlloc(allocator, source, .{});
            errdefer allocator.free(bytes);
            const result: Self = .{ .bytes = bytes };
            result.validate() catch return error.UnexpectedToken;
            return result;
        }

        pub fn jsonStringify(self: Self, jw: anytype) !void {
            // std.json's custom stringify hook can only surface WriteFailed.
            // Call validate() when a caller needs to distinguish malformed raw
            // input before serialization.
            self.validate() catch return error.WriteFailed;
            try jw.beginWriteRaw();
            try jw.writer.writeAll(self.bytes);
            jw.endWriteRaw();
        }

        pub fn validate(self: Self) error{InvalidRawJson}!void {
            if (!validForRoot(self.bytes)) return error.InvalidRawJson;
        }

        fn validForRoot(input: []const u8) bool {
            const trimmed = trimJsonWhitespace(input);
            if (trimmed.len == 0) return false;
            if (root_kind == .object and
                (trimmed[0] != '{' or trimmed[trimmed.len - 1] != '}'))
            {
                return false;
            }

            // Scanner nesting consumes one bit per level. ArrayList growth can
            // reserve beyond the exact byte count, so retain a small bounded
            // margin and explicitly enforce the public nesting ceiling.
            var nesting_bytes: [max_raw_nesting * 2]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&nesting_bytes);
            var scanner = std.json.Scanner.initCompleteInput(fixed.allocator(), input);
            defer scanner.deinit();
            scanner.ensureTotalStackCapacity(max_raw_nesting) catch return false;
            while (true) {
                const token = scanner.next() catch return false;
                if (scanner.stackHeight() > max_raw_nesting) return false;
                if (token == .end_of_document) return true;
            }
        }
    };
}

fn trimJsonWhitespace(input: []const u8) []const u8 {
    var start: usize = 0;
    while (start < input.len and isJsonWhitespace(input[start])) start += 1;
    var end = input.len;
    while (end > start and isJsonWhitespace(input[end - 1])) end -= 1;
    return input[start..end];
}

fn isJsonWhitespace(byte: u8) bool {
    return byte == ' ' or byte == '\t' or byte == '\r' or byte == '\n';
}

fn copyValue(
    allocator: Allocator,
    source: anytype,
    options: std.json.ParseOptions,
    jw: *std.json.Stringify,
    depth: usize,
) !void {
    const max_value_len = options.max_value_len orelse std.json.default_max_value_len;
    switch (try source.peekNextTokenType()) {
        .object_begin => {
            if (depth == max_raw_nesting) return error.UnexpectedToken;
            _ = try source.next();
            try jw.beginObject();
            while (try source.peekNextTokenType() != .object_end) {
                const token = try source.nextAllocMax(allocator, .alloc_if_needed, max_value_len);
                switch (token) {
                    .string => |name| try jw.objectField(name),
                    .allocated_string => |name| {
                        defer allocator.free(name);
                        try jw.objectField(name);
                    },
                    else => return error.UnexpectedToken,
                }
                try copyValue(allocator, source, options, jw, depth + 1);
            }
            _ = try source.next();
            try jw.endObject();
        },
        .array_begin => {
            if (depth == max_raw_nesting) return error.UnexpectedToken;
            _ = try source.next();
            try jw.beginArray();
            while (try source.peekNextTokenType() != .array_end)
                try copyValue(allocator, source, options, jw, depth + 1);
            _ = try source.next();
            try jw.endArray();
        },
        .string => switch (try source.nextAllocMax(allocator, .alloc_if_needed, max_value_len)) {
            .string => |value| try jw.write(value),
            .allocated_string => |value| {
                defer allocator.free(value);
                try jw.write(value);
            },
            else => unreachable,
        },
        .number => switch (try source.nextAllocMax(allocator, .alloc_if_needed, max_value_len)) {
            .number => |value| try writeNumber(jw, value),
            .allocated_number => |value| {
                defer allocator.free(value);
                try writeNumber(jw, value);
            },
            else => unreachable,
        },
        .true => {
            _ = try source.next();
            try jw.write(true);
        },
        .false => {
            _ = try source.next();
            try jw.write(false);
        },
        .null => {
            _ = try source.next();
            try jw.write(null);
        },
        .object_end, .array_end, .end_of_document => return error.UnexpectedToken,
    }
}

fn writeNumber(jw: *std.json.Stringify, value: []const u8) !void {
    try jw.beginWriteRaw();
    try jw.writer.writeAll(value);
    jw.endWriteRaw();
}

test "raw object streams compact owned bytes" {
    const alloc = std.testing.allocator;
    var raw = try RawObject.parseOwned(alloc, " { \"query\" : [1, true, null] } ");
    defer raw.deinit(alloc);
    try std.testing.expectEqualStrings("{\"query\":[1,true,null]}", raw.bytes);

    const encoded = try std.json.Stringify.valueAlloc(alloc, raw, .{});
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings(raw.bytes, encoded);
}

test "raw object rejects non-object roots and invalid direct construction" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnexpectedToken, RawObject.parseOwned(alloc, "[1,2,3]"));
    const malformed = RawObject{ .bytes = "{" };
    try std.testing.expectError(error.InvalidRawJson, malformed.validate());
    // valueAlloc collapses its writer error set to OutOfMemory; the important
    // contract here is that invalid bytes are never emitted.
    try std.testing.expectError(error.OutOfMemory, std.json.Stringify.valueAlloc(alloc, malformed, .{}));

    const wrong_root = RawObject{ .bytes = "[1]" };
    try std.testing.expectError(error.InvalidRawJson, wrong_root.validate());
    try std.testing.expectError(error.OutOfMemory, std.json.Stringify.valueAlloc(alloc, wrong_root, .{}));

    try std.testing.expectError(
        error.InvalidRawJson,
        (RawObject{ .bytes = "\x0b{}" }).validate(),
    );
}

test "raw value accepts scalar roots but validates direct construction" {
    const alloc = std.testing.allocator;
    var raw = try RawValue.parseOwned(alloc, " 42 ");
    defer raw.deinit(alloc);
    try std.testing.expectEqualStrings("42", raw.bytes);

    const malformed = RawValue{ .bytes = "true false" };
    try std.testing.expectError(error.InvalidRawJson, malformed.validate());
    try std.testing.expectError(error.OutOfMemory, std.json.Stringify.valueAlloc(alloc, malformed, .{}));
}

test "raw JSON nesting is bounded during parsing and final validation" {
    const alloc = std.testing.allocator;
    const at_limit = try alloc.alloc(u8, max_raw_nesting * 2 + 1);
    defer alloc.free(at_limit);
    @memset(at_limit[0..max_raw_nesting], '[');
    at_limit[max_raw_nesting] = '0';
    @memset(at_limit[max_raw_nesting + 1 ..], ']');

    var parsed = try RawValue.parseOwned(alloc, at_limit);
    defer parsed.deinit(alloc);
    try parsed.validate();

    const over_limit = try alloc.alloc(u8, (max_raw_nesting + 1) * 2 + 1);
    defer alloc.free(over_limit);
    @memset(over_limit[0 .. max_raw_nesting + 1], '[');
    over_limit[max_raw_nesting + 1] = '0';
    @memset(over_limit[max_raw_nesting + 2 ..], ']');
    try std.testing.expectError(error.UnexpectedToken, RawValue.parseOwned(alloc, over_limit));
    try std.testing.expectError(error.InvalidRawJson, (RawValue{ .bytes = over_limit }).validate());
}
