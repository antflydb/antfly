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

const lane_count = 16;
const Vec = @Vector(lane_count, u8);
const Mask = @Int(.unsigned, lane_count);

pub const StructuralIndex = struct {
    /// Offsets of structural punctuation outside strings: `{ } [ ] : ,`.
    offsets: []u32,
    /// Offsets of unescaped quote delimiters.
    quote_offsets: []u32,
    has_escapes: bool,
    has_non_ascii: bool,
    has_string_controls: bool,

    pub fn deinit(self: *StructuralIndex, allocator: Allocator) void {
        allocator.free(self.offsets);
        allocator.free(self.quote_offsets);
        self.* = undefined;
    }

    pub fn supportsStage2FastPath(self: StructuralIndex) bool {
        return !self.has_escapes and !self.has_non_ascii;
    }
};

pub fn buildStructuralIndexAlloc(allocator: Allocator, input: []const u8) Allocator.Error!StructuralIndex {
    var offsets = std.ArrayListUnmanaged(u32).empty;
    errdefer offsets.deinit(allocator);
    var quote_offsets = std.ArrayListUnmanaged(u32).empty;
    errdefer quote_offsets.deinit(allocator);

    var has_escapes = false;
    var has_non_ascii = false;
    var has_string_controls = false;
    var in_string = false;
    var escaped = false;

    var i: usize = 0;
    while (i + lane_count <= input.len) : (i += lane_count) {
        const chunk: Vec = input[i..][0..lane_count].*;

        if (!has_non_ascii and hasAnyNonAscii(chunk)) {
            has_non_ascii = true;
        }

        for (input[i .. i + lane_count], 0..) |b, lane| {
            const offset: u32 = @intCast(i + lane);
            if (b == '\\') has_escapes = true;

            if (in_string) {
                if (escaped) {
                    if (b <= 0x1F) has_string_controls = true;
                    escaped = false;
                    continue;
                }
                if (b <= 0x1F) has_string_controls = true;
                switch (b) {
                    '\\' => escaped = true,
                    '"' => {
                        in_string = false;
                        try quote_offsets.append(allocator, offset);
                    },
                    else => {},
                }
                continue;
            }

            switch (b) {
                '"' => {
                    in_string = true;
                    try quote_offsets.append(allocator, offset);
                },
                '{', '}', '[', ']', ':', ',' => try offsets.append(allocator, offset),
                else => {},
            }
        }
    }

    while (i < input.len) : (i += 1) {
        const b = input[i];
        const offset: u32 = @intCast(i);
        if (b == '\\') has_escapes = true;
        if (b & 0x80 != 0) has_non_ascii = true;

        if (in_string) {
            if (escaped) {
                if (b <= 0x1F) has_string_controls = true;
                escaped = false;
                continue;
            }
            if (b <= 0x1F) has_string_controls = true;
            switch (b) {
                '\\' => escaped = true,
                '"' => {
                    in_string = false;
                    try quote_offsets.append(allocator, offset);
                },
                else => {},
            }
            continue;
        }

        switch (b) {
            '"' => {
                in_string = true;
                try quote_offsets.append(allocator, offset);
            },
            '{', '}', '[', ']', ':', ',' => try offsets.append(allocator, offset),
            else => {},
        }
    }

    return .{
        .offsets = try offsets.toOwnedSlice(allocator),
        .quote_offsets = try quote_offsets.toOwnedSlice(allocator),
        .has_escapes = has_escapes,
        .has_non_ascii = has_non_ascii,
        .has_string_controls = has_string_controls,
    };
}

fn hasAnyNonAscii(chunk: Vec) bool {
    const high_bits: Vec = @splat(0x80);
    const zeros: Vec = @splat(0);
    const non_ascii_mask: Mask = @bitCast((chunk & high_bits) != zeros);
    return non_ascii_mask != 0;
}

fn buildStructuralIndexScalarAlloc(allocator: Allocator, input: []const u8) Allocator.Error!StructuralIndex {
    var offsets = std.ArrayListUnmanaged(u32).empty;
    errdefer offsets.deinit(allocator);
    var quote_offsets = std.ArrayListUnmanaged(u32).empty;
    errdefer quote_offsets.deinit(allocator);

    var has_escapes = false;
    var has_non_ascii = false;
    var has_string_controls = false;
    var in_string = false;
    var escaped = false;
    for (input, 0..) |b, i| {
        const offset: u32 = @intCast(i);
        if (b == '\\') has_escapes = true;
        if (b & 0x80 != 0) has_non_ascii = true;

        if (in_string) {
            if (escaped) {
                if (b <= 0x1F) has_string_controls = true;
                escaped = false;
                continue;
            }
            if (b <= 0x1F) has_string_controls = true;
            switch (b) {
                '\\' => escaped = true,
                '"' => {
                    in_string = false;
                    try quote_offsets.append(allocator, offset);
                },
                else => {},
            }
            continue;
        }

        switch (b) {
            '"' => {
                in_string = true;
                try quote_offsets.append(allocator, offset);
            },
            '{', '}', '[', ']', ':', ',' => try offsets.append(allocator, offset),
            else => {},
        }
    }

    return .{
        .offsets = try offsets.toOwnedSlice(allocator),
        .quote_offsets = try quote_offsets.toOwnedSlice(allocator),
        .has_escapes = has_escapes,
        .has_non_ascii = has_non_ascii,
        .has_string_controls = has_string_controls,
    };
}

test "stage1 structural index matches scalar reference" {
    const alloc = std.testing.allocator;
    const sample =
        \\{"name":"ada","tags":["x","y"],"meta":{"ok":true}}
    ;

    var simd = try buildStructuralIndexAlloc(alloc, sample);
    defer simd.deinit(alloc);
    var scalar = try buildStructuralIndexScalarAlloc(alloc, sample);
    defer scalar.deinit(alloc);

    try std.testing.expectEqualDeep(scalar.offsets, simd.offsets);
    try std.testing.expectEqualDeep(scalar.quote_offsets, simd.quote_offsets);
    try std.testing.expectEqual(scalar.has_escapes, simd.has_escapes);
    try std.testing.expectEqual(scalar.has_non_ascii, simd.has_non_ascii);
    try std.testing.expectEqual(scalar.has_string_controls, simd.has_string_controls);
    try std.testing.expect(simd.supportsStage2FastPath());
}

test "stage1 detects escapes and non ascii bytes" {
    const alloc = std.testing.allocator;

    var escaped = try buildStructuralIndexAlloc(alloc, "{\"msg\":\"line\\n\"}");
    defer escaped.deinit(alloc);
    try std.testing.expect(escaped.has_escapes);
    try std.testing.expect(!escaped.has_non_ascii);
    try std.testing.expect(!escaped.supportsStage2FastPath());

    var utf8 = try buildStructuralIndexAlloc(alloc, "\"\xC3\xA9\"");
    defer utf8.deinit(alloc);
    try std.testing.expect(!utf8.has_escapes);
    try std.testing.expect(utf8.has_non_ascii);
    try std.testing.expect(!utf8.supportsStage2FastPath());
}

test "stage1 detects raw control bytes inside strings" {
    const alloc = std.testing.allocator;

    var clean = try buildStructuralIndexAlloc(alloc, "{\"msg\":\"hello\"}");
    defer clean.deinit(alloc);
    try std.testing.expect(!clean.has_string_controls);

    const raw = [_]u8{ '{', '"', 'm', 's', 'g', '"', ':', '"', 0x01, '"', '}' };
    var invalid = try buildStructuralIndexAlloc(alloc, raw[0..]);
    defer invalid.deinit(alloc);
    try std.testing.expect(invalid.has_string_controls);
}

test "stage1 structural masking excludes punctuation inside strings" {
    const alloc = std.testing.allocator;
    const sample =
        \\{"msg":"a,b:c[1]{2}","list":[1,2]}
    ;

    var idx = try buildStructuralIndexAlloc(alloc, sample);
    defer idx.deinit(alloc);

    const expected_offsets = [_]u32{ 0, 6, 20, 27, 28, 30, 32, 33 };
    try std.testing.expectEqualDeep(expected_offsets[0..], idx.offsets);

    const expected_quotes = [_]u32{ 1, 5, 7, 19, 21, 26 };
    try std.testing.expectEqualDeep(expected_quotes[0..], idx.quote_offsets);
}
