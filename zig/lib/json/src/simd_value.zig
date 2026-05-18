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

pub fn parseValueFromSlice(
    allocator: Allocator,
    input: []const u8,
    options: json.ParseOptions,
    structural_index: simd_stage1.StructuralIndex,
) json.ParseError(json.Scanner)!json.Parsed(json.Value) {
    var parsed = json.Parsed(json.Value){
        .arena = try allocator.create(std.heap.ArenaAllocator),
        .value = undefined,
    };
    errdefer allocator.destroy(parsed.arena);
    parsed.arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer parsed.arena.deinit();

    parsed.value = try parseValueFromSliceLeaky(parsed.arena.allocator(), input, options, structural_index);
    return parsed;
}

pub fn parseValueFromSliceLeaky(
    allocator: Allocator,
    input: []const u8,
    options: json.ParseOptions,
    structural_index: simd_stage1.StructuralIndex,
) json.ParseError(json.Scanner)!json.Value {
    var parser = Parser.init(allocator, input, options, structural_index);
    parser.skipWhitespace();
    const value = try parser.parseValue();
    parser.skipWhitespace();
    if (!parser.atEnd()) return error.SyntaxError;
    return value;
}

const Parser = struct {
    allocator: Allocator,
    input: []const u8,
    pos: usize,
    quote_cursor: usize,
    max_value_len: usize,
    options: json.ParseOptions,
    quote_offsets: []const u32,
    has_escapes: bool,
    has_string_controls: bool,
    structural_offsets: []const u32,

    fn init(
        allocator: Allocator,
        input: []const u8,
        options: json.ParseOptions,
        structural_index: simd_stage1.StructuralIndex,
    ) Parser {
        return .{
            .allocator = allocator,
            .input = input,
            .pos = 0,
            .quote_cursor = 0,
            .max_value_len = options.max_value_len orelse input.len,
            .options = options,
            .quote_offsets = structural_index.quote_offsets,
            .has_escapes = structural_index.has_escapes,
            .has_string_controls = structural_index.has_string_controls,
            .structural_offsets = structural_index.offsets,
        };
    }

    fn parseValue(self: *Parser) json.ParseError(json.Scanner)!json.Value {
        if (self.atEnd()) return error.UnexpectedEndOfInput;

        return switch (self.input[self.pos]) {
            '{' => self.parseObject(),
            '[' => self.parseArray(),
            '"' => self.parseStringValue(),
            't' => self.parseLiteral("true", .{ .bool = true }),
            'f' => self.parseLiteral("false", .{ .bool = false }),
            'n' => self.parseLiteral("null", .null),
            '-', '0'...'9' => self.parseNumberValue(),
            else => error.SyntaxError,
        };
    }

    fn parseObject(self: *Parser) json.ParseError(json.Scanner)!json.Value {
        std.debug.assert(self.input[self.pos] == '{');
        self.pos += 1;
        self.skipWhitespace();

        var object = json.ObjectMap.empty;
        errdefer {
            var temp: json.Value = .{ .object = object };
            deinitValue(self.allocator, &temp);
        }

        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] == '}') {
            self.pos += 1;
            return .{ .object = object };
        }

        while (true) {
            {
                if (self.atEnd()) return error.UnexpectedEndOfInput;
                if (self.input[self.pos] != '"') return error.SyntaxError;

                const key = try self.parseStringAlloc();
                errdefer self.allocator.free(key);

                self.skipWhitespace();
                if (self.atEnd()) return error.UnexpectedEndOfInput;
                if (self.input[self.pos] != ':') return error.SyntaxError;
                self.pos += 1;

                self.skipWhitespace();
                var value = try self.parseValue();
                var value_owned = true;
                errdefer if (value_owned) deinitValue(self.allocator, &value);

                const gop = try object.getOrPut(self.allocator, key);
                if (gop.found_existing) {
                    switch (self.options.duplicate_field_behavior) {
                        .use_first => {
                            deinitValue(self.allocator, &value);
                            value_owned = false;
                            self.allocator.free(key);
                        },
                        .@"error" => {
                            deinitValue(self.allocator, &value);
                            value_owned = false;
                            return error.DuplicateField;
                        },
                        .use_last => {
                            deinitValue(self.allocator, gop.value_ptr);
                            gop.value_ptr.* = value;
                            value_owned = false;
                            self.allocator.free(key);
                        },
                    }
                } else {
                    gop.value_ptr.* = value;
                    value_owned = false;
                }
            }

            self.skipWhitespace();
            if (self.atEnd()) return error.UnexpectedEndOfInput;

            switch (self.input[self.pos]) {
                ',' => {
                    self.pos += 1;
                    self.skipWhitespace();
                },
                '}' => {
                    self.pos += 1;
                    return .{ .object = object };
                },
                else => return error.SyntaxError,
            }
        }
    }

    fn parseArray(self: *Parser) json.ParseError(json.Scanner)!json.Value {
        std.debug.assert(self.input[self.pos] == '[');
        self.pos += 1;
        self.skipWhitespace();

        var array = json.Array.init(self.allocator);
        errdefer {
            var temp: json.Value = .{ .array = array };
            deinitValue(self.allocator, &temp);
        }

        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] == ']') {
            self.pos += 1;
            return .{ .array = array };
        }

        while (true) {
            {
                var value = try self.parseValue();
                errdefer deinitValue(self.allocator, &value);
                try array.append(value);
            }

            self.skipWhitespace();
            if (self.atEnd()) return error.UnexpectedEndOfInput;

            switch (self.input[self.pos]) {
                ',' => {
                    self.pos += 1;
                    self.skipWhitespace();
                },
                ']' => {
                    self.pos += 1;
                    return .{ .array = array };
                },
                else => return error.SyntaxError,
            }
        }
    }

    fn parseStringValue(self: *Parser) json.ParseError(json.Scanner)!json.Value {
        return .{ .string = try self.parseStringAlloc() };
    }

    fn parseStringAlloc(self: *Parser) json.ParseError(json.Scanner)![]u8 {
        std.debug.assert(self.input[self.pos] == '"');

        self.syncQuoteCursor();
        if (self.quote_cursor >= self.quote_offsets.len or self.quote_offsets[self.quote_cursor] != self.pos) {
            return error.SyntaxError;
        }
        if (self.quote_cursor + 1 >= self.quote_offsets.len) return error.UnexpectedEndOfInput;

        const closing_quote = self.quote_offsets[self.quote_cursor + 1];
        const direct_content = self.input[self.pos + 1 .. closing_quote];
        if (self.canDirectCopyString(direct_content)) {
            if (direct_content.len > self.max_value_len) return error.ValueTooLong;
            self.pos = closing_quote + 1;
            self.quote_cursor += 2;
            return try self.allocator.dupe(u8, direct_content);
        }

        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(self.allocator);
        try out.ensureTotalCapacityPrecise(self.allocator, direct_content.len);

        var i: usize = 0;
        while (i < direct_content.len) {
            const plain_start = i;
            while (i < direct_content.len and direct_content[i] != '\\' and direct_content[i] > 0x1F) : (i += 1) {}
            out.appendSliceAssumeCapacity(direct_content[plain_start..i]);
            if (out.items.len > self.max_value_len) return error.ValueTooLong;
            if (i == direct_content.len) break;

            if (direct_content[i] <= 0x1F) return error.SyntaxError;
            i += 1;
            try self.appendEscapeSequenceFromSlice(&out, direct_content, &i);
            if (out.items.len > self.max_value_len) return error.ValueTooLong;
        }

        self.pos = closing_quote + 1;
        self.quote_cursor += 2;
        return try out.toOwnedSlice(self.allocator);
    }

    fn appendEscapeSequenceFromSlice(
        self: *Parser,
        out: *std.ArrayListUnmanaged(u8),
        content: []const u8,
        index: *usize,
    ) json.ParseError(json.Scanner)!void {
        if (index.* >= content.len) return error.SyntaxError;

        const b = content[index.*];
        switch (b) {
            '"', '\\', '/' => {
                out.appendAssumeCapacity(b);
                index.* += 1;
            },
            'b' => {
                out.appendAssumeCapacity(0x08);
                index.* += 1;
            },
            'f' => {
                out.appendAssumeCapacity(0x0C);
                index.* += 1;
            },
            'n' => {
                out.appendAssumeCapacity('\n');
                index.* += 1;
            },
            'r' => {
                out.appendAssumeCapacity('\r');
                index.* += 1;
            },
            't' => {
                out.appendAssumeCapacity('\t');
                index.* += 1;
            },
            'u' => {
                index.* += 1;
                const codepoint = try self.decodeUnicodeEscapeFromSlice(content, index);
                var buf: [4]u8 = undefined;
                const len = std.unicode.utf8Encode(codepoint, &buf) catch unreachable;
                out.appendSliceAssumeCapacity(buf[0..len]);
            },
            else => return error.SyntaxError,
        }
    }

    fn decodeUnicodeEscapeFromSlice(self: *Parser, content: []const u8, index: *usize) json.ParseError(json.Scanner)!u21 {
        const first = try self.parseHexQuadFromSlice(content, index);
        if (first >= 0xD800 and first <= 0xDBFF) {
            if (index.* + 2 > content.len) return error.SyntaxError;
            if (content[index.*] != '\\' or content[index.* + 1] != 'u') return error.SyntaxError;
            index.* += 2;
            const second = try self.parseHexQuadFromSlice(content, index);
            if (second < 0xDC00 or second > 0xDFFF) return error.SyntaxError;

            const high_ten = @as(u21, first - 0xD800);
            const low_ten = @as(u21, second - 0xDC00);
            return 0x10000 + ((high_ten << 10) | low_ten);
        }

        if (first >= 0xDC00 and first <= 0xDFFF) return error.SyntaxError;
        return @intCast(first);
    }

    fn parseHexQuadFromSlice(self: *Parser, content: []const u8, index: *usize) json.ParseError(json.Scanner)!u16 {
        _ = self;
        if (index.* + 4 > content.len) return error.SyntaxError;

        var value: u16 = 0;
        for (content[index.* .. index.* + 4]) |c| {
            const digit: u16 = switch (c) {
                '0'...'9' => c - '0',
                'a'...'f' => c - 'a' + 10,
                'A'...'F' => c - 'A' + 10,
                else => return error.SyntaxError,
            };
            value = (value << 4) | digit;
        }
        index.* += 4;
        return value;
    }

    fn parseLiteral(self: *Parser, comptime literal: []const u8, value: json.Value) json.ParseError(json.Scanner)!json.Value {
        if (self.pos + literal.len > self.input.len) return error.UnexpectedEndOfInput;
        if (!std.mem.eql(u8, self.input[self.pos .. self.pos + literal.len], literal)) return error.SyntaxError;
        self.pos += literal.len;
        return value;
    }

    fn parseNumberValue(self: *Parser) json.ParseError(json.Scanner)!json.Value {
        const start = self.pos;

        if (self.input[self.pos] == '-') {
            self.pos += 1;
            if (self.atEnd()) return error.UnexpectedEndOfInput;
        }

        switch (self.input[self.pos]) {
            '0' => self.pos += 1,
            '1'...'9' => {
                self.pos += 1;
                self.advanceDigitRun();
            },
            else => return error.SyntaxError,
        }

        if (!self.atEnd() and self.input[self.pos] == '.') {
            self.pos += 1;
            if (self.atEnd()) return error.UnexpectedEndOfInput;
            if (!std.ascii.isDigit(self.input[self.pos])) return error.SyntaxError;
            self.advanceDigitRun();
        }

        if (!self.atEnd() and (self.input[self.pos] == 'e' or self.input[self.pos] == 'E')) {
            self.pos += 1;
            if (self.atEnd()) return error.UnexpectedEndOfInput;
            if (self.input[self.pos] == '+' or self.input[self.pos] == '-') {
                self.pos += 1;
                if (self.atEnd()) return error.UnexpectedEndOfInput;
            }
            if (!std.ascii.isDigit(self.input[self.pos])) return error.SyntaxError;
            self.advanceDigitRun();
        }

        const slice = self.input[start..self.pos];
        if (slice.len > self.max_value_len) return error.ValueTooLong;

        if (!self.options.parse_numbers) {
            return .{ .number_string = try self.allocator.dupe(u8, slice) };
        }

        var parsed = json.Value.parseFromNumberSlice(slice);
        if (parsed == .number_string) {
            parsed.number_string = try self.allocator.dupe(u8, slice);
        }
        return parsed;
    }

    fn skipWhitespace(self: *Parser) void {
        while (!self.atEnd()) {
            switch (self.input[self.pos]) {
                ' ', '\t', '\r', '\n' => self.pos += 1,
                else => return,
            }
        }
    }

    fn advanceDigitRun(self: *Parser) void {
        self.pos = std.mem.indexOfNonePos(u8, self.input, self.pos, "0123456789") orelse self.input.len;
    }

    fn canDirectCopyString(self: *const Parser, content: []const u8) bool {
        if (!self.has_escapes and !self.has_string_controls) return true;
        for (content) |b| {
            if (self.has_escapes and b == '\\') return false;
            if (b <= 0x1F) return false;
        }
        return true;
    }

    fn syncQuoteCursor(self: *Parser) void {
        while (self.quote_cursor < self.quote_offsets.len and self.quote_offsets[self.quote_cursor] < self.pos) {
            self.quote_cursor += 1;
        }
    }

    fn quoteCursorAdvanceOnClosing(self: *Parser) void {
        if (self.quote_cursor < self.quote_offsets.len and self.quote_offsets[self.quote_cursor] == self.pos) {
            self.quote_cursor += 1;
            return;
        }
        self.syncQuoteCursor();
        if (self.quote_cursor < self.quote_offsets.len and self.quote_offsets[self.quote_cursor] == self.pos) {
            self.quote_cursor += 1;
        }
    }

    fn atEnd(self: *const Parser) bool {
        return self.pos >= self.input.len;
    }
};

fn deinitValue(allocator: Allocator, value: *json.Value) void {
    switch (value.*) {
        .string => |text| allocator.free(text),
        .number_string => |text| allocator.free(text),
        .array => |*array| {
            for (array.items) |*item| deinitValue(allocator, item);
            array.deinit();
        },
        .object => |*object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                allocator.free(@constCast(entry.key_ptr.*));
                deinitValue(allocator, entry.value_ptr);
            }
            object.deinit(allocator);
        },
        else => {},
    }
    value.* = undefined;
}

test "simd value parser handles simple object arrays and numbers" {
    const alloc = std.testing.allocator;
    const input =
        \\{"name":"ada","nums":[1,2.5,3e2],"ok":true,"none":null}
    ;

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, input);
    defer structural.deinit(alloc);

    var parsed = try parseValueFromSlice(alloc, input, .{}, structural);
    defer parsed.deinit();

    try std.testing.expect(parsed.value == .object);
    try std.testing.expectEqualStrings("ada", parsed.value.object.get("name").?.string);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("nums").?.array.items[0].integer);
    try std.testing.expectEqual(@as(f64, 2.5), parsed.value.object.get("nums").?.array.items[1].float);
    try std.testing.expectEqual(@as(f64, 300), parsed.value.object.get("nums").?.array.items[2].float);
    try std.testing.expect(parsed.value.object.get("ok").?.bool);
    try std.testing.expect(parsed.value.object.get("none").? == .null);
}

test "simd value parser respects parse_numbers and duplicate behavior" {
    const alloc = std.testing.allocator;
    const input = "{\"n\":1,\"n\":2}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, input);
    defer structural.deinit(alloc);

    var use_first = try parseValueFromSlice(alloc, input, .{
        .duplicate_field_behavior = .use_first,
        .parse_numbers = false,
    }, structural);
    defer use_first.deinit();
    try std.testing.expectEqualStrings("1", use_first.value.object.get("n").?.number_string);

    var use_last = try parseValueFromSlice(alloc, input, .{
        .duplicate_field_behavior = .use_last,
    }, structural);
    defer use_last.deinit();
    try std.testing.expectEqual(@as(i64, 2), use_last.value.object.get("n").?.integer);

    try std.testing.expectError(error.DuplicateField, parseValueFromSliceLeaky(alloc, input, .{}, structural));
}

test "simd value parser decodes escapes and unicode" {
    const alloc = std.testing.allocator;
    const input =
        \\{"msg":"line\n","quote":"\"","unicode":"\u00E9","pair":"\uD83D\uDE00"}
    ;

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, input);
    defer structural.deinit(alloc);

    var parsed = try parseValueFromSlice(alloc, input, .{}, structural);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("line\n", parsed.value.object.get("msg").?.string);
    try std.testing.expectEqualStrings("\"", parsed.value.object.get("quote").?.string);
    try std.testing.expectEqualStrings("é", parsed.value.object.get("unicode").?.string);
    try std.testing.expectEqualStrings("😀", parsed.value.object.get("pair").?.string);
}
