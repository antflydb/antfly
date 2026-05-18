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
const simd_stage1 = @import("simd_stage1.zig");

const Allocator = std.mem.Allocator;
const json = std.json;

pub const SkipTape = struct {
    end_cursors: []u32,

    pub const no_end = std.math.maxInt(u32);

    pub fn deinit(self: *SkipTape, allocator: Allocator) void {
        allocator.free(self.end_cursors);
        self.* = undefined;
    }

    pub fn endCursorForOpen(self: SkipTape, open_cursor: usize) ?usize {
        if (open_cursor >= self.end_cursors.len) return null;
        const cursor = self.end_cursors[open_cursor];
        if (cursor == no_end) return null;
        return cursor;
    }
};

pub const BuildError = Allocator.Error || json.ParseError(json.Scanner) || error{UnsupportedSkipTapeInput};
pub const ValidationResult = struct {
    quote_cursor: usize,
};

pub fn buildSkipTapeAlloc(
    allocator: Allocator,
    input: []const u8,
    structural_index: simd_stage1.StructuralIndex,
) BuildError!SkipTape {
    if (structural_index.has_escapes or structural_index.has_string_controls) {
        return error.UnsupportedSkipTapeInput;
    }

    const end_cursors = try allocator.alloc(u32, structural_index.offsets.len);
    errdefer allocator.free(end_cursors);
    @memset(end_cursors, SkipTape.no_end);

    var builder = Builder{
        .allocator = allocator,
        .input = input,
        .structural_offsets = structural_index.offsets,
        .quote_offsets = structural_index.quote_offsets,
        .end_cursors = end_cursors,
    };
    try builder.build();

    return .{ .end_cursors = end_cursors };
}

pub fn validateCompositeAtAlloc(
    allocator: Allocator,
    input: []const u8,
    start_pos: usize,
    end_pos: usize,
    structural_offsets: []const u32,
    structural_cursor: usize,
    quote_offsets: []const u32,
    quote_cursor: usize,
) BuildError!ValidationResult {
    var builder = Builder{
        .allocator = allocator,
        .input = input,
        .structural_offsets = structural_offsets,
        .quote_offsets = quote_offsets,
        .end_cursors = null,
        .pos = start_pos,
        .end_pos = end_pos,
        .structural_cursor = structural_cursor,
        .quote_cursor = quote_cursor,
    };
    try builder.buildExact();
    return .{ .quote_cursor = builder.quote_cursor };
}

const Builder = struct {
    const FrameKind = enum { object, array };

    const Frame = struct {
        kind: FrameKind,
        open_cursor: usize,
        state: enum {
            object_key_or_end,
            object_colon,
            object_value,
            object_comma_or_end,
            array_value_or_end,
            array_comma_or_end,
        },
    };

    allocator: Allocator,
    input: []const u8,
    structural_offsets: []const u32,
    quote_offsets: []const u32,
    end_cursors: ?[]u32 = null,
    pos: usize = 0,
    end_pos: usize = std.math.maxInt(usize),
    structural_cursor: usize = 0,
    quote_cursor: usize = 0,
    stack: std.ArrayListUnmanaged(Frame) = .empty,

    fn build(self: *Builder) BuildError!void {
        defer self.stack.deinit(self.allocator);

        try self.parseValue();
        self.skipWhitespace();
        if (!self.atEnd()) return error.SyntaxError;
    }

    fn buildExact(self: *Builder) BuildError!void {
        defer self.stack.deinit(self.allocator);

        try self.parseValue();
        if (self.pos != self.end_pos) return error.SyntaxError;
    }

    fn parseValue(self: *Builder) BuildError!void {
        self.skipWhitespace();
        if (self.atEnd()) return error.UnexpectedEndOfInput;

        switch (self.input[self.pos]) {
            '{' => try self.parseComposite(.object),
            '[' => try self.parseComposite(.array),
            '"' => try self.skipString(),
            't' => try self.skipLiteral("true"),
            'f' => try self.skipLiteral("false"),
            'n' => try self.skipLiteral("null"),
            '-', '0'...'9' => try self.skipNumberLike(),
            else => return error.UnexpectedToken,
        }
    }

    fn parseComposite(self: *Builder, kind: FrameKind) BuildError!void {
        const open_cursor = try self.consumeStructuralOpen(switch (kind) {
            .object => '{',
            .array => '[',
        });

        try self.stack.append(self.allocator, .{
            .kind = kind,
            .open_cursor = open_cursor,
            .state = switch (kind) {
                .object => .object_key_or_end,
                .array => .array_value_or_end,
            },
        });

        while (self.stack.items.len > 0) {
            self.skipWhitespace();
            if (self.atEnd()) return error.UnexpectedEndOfInput;

            var frame = &self.stack.items[self.stack.items.len - 1];
            switch (frame.state) {
                .object_key_or_end => switch (self.input[self.pos]) {
                    '}' => {
                        const close_cursor = try self.consumeStructuralClose('}');
                        self.recordEnd(frame.open_cursor, close_cursor);
                        _ = self.stack.pop();
                        if (self.stack.items.len == 0) return;
                        self.advanceParentAfterChild();
                    },
                    '"' => {
                        try self.skipString();
                        frame.state = .object_colon;
                    },
                    else => return error.UnexpectedToken,
                },
                .object_colon => {
                    _ = try self.consumeStructuralOpen(':');
                    frame.state = .object_value;
                },
                .object_value => {
                    try self.parseChildValue();
                    if (self.stack.items.len == 0) return;
                },
                .object_comma_or_end => switch (self.input[self.pos]) {
                    ',' => {
                        _ = try self.consumeStructuralOpen(',');
                        frame.state = .object_key_or_end;
                    },
                    '}' => {
                        const close_cursor = try self.consumeStructuralClose('}');
                        self.recordEnd(frame.open_cursor, close_cursor);
                        _ = self.stack.pop();
                        if (self.stack.items.len == 0) return;
                        self.advanceParentAfterChild();
                    },
                    else => return error.UnexpectedToken,
                },
                .array_value_or_end => switch (self.input[self.pos]) {
                    ']' => {
                        const close_cursor = try self.consumeStructuralClose(']');
                        self.recordEnd(frame.open_cursor, close_cursor);
                        _ = self.stack.pop();
                        if (self.stack.items.len == 0) return;
                        self.advanceParentAfterChild();
                    },
                    else => try self.parseChildValue(),
                },
                .array_comma_or_end => switch (self.input[self.pos]) {
                    ',' => {
                        _ = try self.consumeStructuralOpen(',');
                        frame.state = .array_value_or_end;
                    },
                    ']' => {
                        const close_cursor = try self.consumeStructuralClose(']');
                        self.recordEnd(frame.open_cursor, close_cursor);
                        _ = self.stack.pop();
                        if (self.stack.items.len == 0) return;
                        self.advanceParentAfterChild();
                    },
                    else => return error.UnexpectedToken,
                },
            }
        }
    }

    fn parseChildValue(self: *Builder) BuildError!void {
        self.skipWhitespace();
        if (self.atEnd()) return error.UnexpectedEndOfInput;

        switch (self.input[self.pos]) {
            '{' => try self.parseComposite(.object),
            '[' => try self.parseComposite(.array),
            '"' => {
                try self.skipString();
                self.advanceParentAfterChild();
            },
            't' => {
                try self.skipLiteral("true");
                self.advanceParentAfterChild();
            },
            'f' => {
                try self.skipLiteral("false");
                self.advanceParentAfterChild();
            },
            'n' => {
                try self.skipLiteral("null");
                self.advanceParentAfterChild();
            },
            '-', '0'...'9' => {
                try self.skipNumberLike();
                self.advanceParentAfterChild();
            },
            else => return error.UnexpectedToken,
        }
    }

    fn advanceParentAfterChild(self: *Builder) void {
        var parent = &self.stack.items[self.stack.items.len - 1];
        parent.state = switch (parent.kind) {
            .object => .object_comma_or_end,
            .array => .array_comma_or_end,
        };
    }

    fn recordEnd(self: *Builder, open_cursor: usize, close_cursor: usize) void {
        if (self.end_cursors) |end_cursors| {
            end_cursors[open_cursor] = @intCast(close_cursor);
        }
    }

    fn consumeStructuralOpen(self: *Builder, expected: u8) BuildError!usize {
        return self.consumeStructural(expected);
    }

    fn consumeStructuralClose(self: *Builder, expected: u8) BuildError!usize {
        return self.consumeStructural(expected);
    }

    fn consumeStructural(self: *Builder, expected: u8) BuildError!usize {
        self.syncStructuralCursor();
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != expected) return error.UnexpectedToken;
        if (self.structural_cursor >= self.structural_offsets.len or self.structural_offsets[self.structural_cursor] != self.pos) {
            return error.SyntaxError;
        }
        const cursor = self.structural_cursor;
        self.structural_cursor += 1;
        self.pos += 1;
        return cursor;
    }

    fn skipString(self: *Builder) BuildError!void {
        self.syncQuoteCursor();
        if (self.quote_cursor >= self.quote_offsets.len or self.quote_offsets[self.quote_cursor] != self.pos) {
            return error.SyntaxError;
        }
        if (self.quote_cursor + 1 >= self.quote_offsets.len) return error.UnexpectedEndOfInput;
        self.pos = self.quote_offsets[self.quote_cursor + 1] + 1;
        self.quote_cursor += 2;
    }

    fn skipLiteral(self: *Builder, comptime literal: []const u8) BuildError!void {
        if (self.pos + literal.len > self.input.len) return error.UnexpectedEndOfInput;
        if (!std.mem.eql(u8, self.input[self.pos .. self.pos + literal.len], literal)) return error.UnexpectedToken;
        self.pos += literal.len;
    }

    fn skipNumberLike(self: *Builder) BuildError!void {
        const start = self.pos;
        const end = self.findPrimitiveEnd();
        const slice = self.input[start..end];
        try validateNumberLikeSlice(slice);
        self.pos = end;
    }

    fn findPrimitiveEnd(self: *Builder) usize {
        self.syncStructuralCursor();
        const structural_end = if (self.structural_cursor < self.structural_offsets.len) self.structural_offsets[self.structural_cursor] else self.input.len;
        var end = structural_end;
        while (end > self.pos) {
            switch (self.input[end - 1]) {
                ' ', '\t', '\r', '\n' => end -= 1,
                else => break,
            }
        }
        return end;
    }

    fn skipWhitespace(self: *Builder) void {
        while (!self.atEnd()) {
            switch (self.input[self.pos]) {
                ' ', '\t', '\r', '\n' => self.pos += 1,
                else => return,
            }
        }
    }

    fn syncStructuralCursor(self: *Builder) void {
        while (self.structural_cursor < self.structural_offsets.len and self.structural_offsets[self.structural_cursor] < self.pos) {
            self.structural_cursor += 1;
        }
    }

    fn syncQuoteCursor(self: *Builder) void {
        while (self.quote_cursor < self.quote_offsets.len and self.quote_offsets[self.quote_cursor] < self.pos) {
            self.quote_cursor += 1;
        }
    }

    fn atEnd(self: *const Builder) bool {
        return self.pos >= self.input.len;
    }
};

fn validateNumberLikeSlice(slice: []const u8) json.ParseError(json.Scanner)!void {
    if (slice.len == 0) return error.UnexpectedToken;

    var i: usize = 0;
    if (slice[i] == '-') {
        i += 1;
        if (i == slice.len) return error.UnexpectedToken;
    }

    switch (slice[i]) {
        '0' => {
            i += 1;
            if (i < slice.len and std.ascii.isDigit(slice[i])) return error.UnexpectedToken;
        },
        '1'...'9' => {
            i += 1;
            while (i < slice.len and std.ascii.isDigit(slice[i])) : (i += 1) {}
        },
        else => return error.UnexpectedToken,
    }

    if (i < slice.len and slice[i] == '.') {
        i += 1;
        if (i == slice.len or !std.ascii.isDigit(slice[i])) return error.InvalidNumber;
        while (i < slice.len and std.ascii.isDigit(slice[i])) : (i += 1) {}
    }

    if (i < slice.len and (slice[i] == 'e' or slice[i] == 'E')) {
        i += 1;
        if (i == slice.len) return error.InvalidNumber;
        if (slice[i] == '+' or slice[i] == '-') {
            i += 1;
            if (i == slice.len) return error.InvalidNumber;
        }
        if (!std.ascii.isDigit(slice[i])) return error.InvalidNumber;
        while (i < slice.len and std.ascii.isDigit(slice[i])) : (i += 1) {}
    }

    if (i != slice.len) return error.UnexpectedToken;
}

test "skip tape builds end cursors for nested composites" {
    const alloc = std.testing.allocator;
    const sample = "{\"a\":[1,{\"b\":2}],\"c\":{}}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, sample);
    defer structural.deinit(alloc);

    var tape = try buildSkipTapeAlloc(alloc, sample, structural);
    defer tape.deinit(alloc);

    try std.testing.expectEqual(@as(?usize, 12), tape.endCursorForOpen(0));
    try std.testing.expectEqual(@as(?usize, 7), tape.endCursorForOpen(2));
    try std.testing.expectEqual(@as(?usize, 11), tape.endCursorForOpen(10));
    try std.testing.expectEqual(@as(?usize, null), tape.endCursorForOpen(1));
}

test "skip tape rejects invalid container grammar" {
    const alloc = std.testing.allocator;
    const sample = "{\"a\" true}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, sample);
    defer structural.deinit(alloc);

    try std.testing.expectError(error.UnexpectedToken, buildSkipTapeAlloc(alloc, sample, structural));
}

test "skip tape rejects unsupported escaped input" {
    const alloc = std.testing.allocator;
    const sample = "{\"a\":\"line\\n\"}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, sample);
    defer structural.deinit(alloc);

    try std.testing.expectError(error.UnsupportedSkipTapeInput, buildSkipTapeAlloc(alloc, sample, structural));
}

test "skip tape validates a subtree without building a full tape" {
    const alloc = std.testing.allocator;
    const sample = "{\"keep\":1,\"extra\":{\"a\":[1,{\"b\":2}],\"c\":{}}}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, sample);
    defer structural.deinit(alloc);

    const start_pos = std.mem.indexOfScalar(u8, sample, '{').? + 18;
    const end_pos = sample.len - 1;
    const structural_cursor = std.mem.indexOfScalar(u32, structural.offsets, @as(u32, @intCast(start_pos))).?;
    const quote_cursor = std.mem.indexOfScalar(u32, structural.quote_offsets, @as(u32, @intCast(start_pos + 1))).? - 1;

    const result = try validateCompositeAtAlloc(
        alloc,
        sample,
        start_pos,
        end_pos,
        structural.offsets,
        structural_cursor,
        structural.quote_offsets,
        quote_cursor,
    );
    try std.testing.expect(result.quote_cursor > quote_cursor);
}
