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
pub const Value = std.json.Value;

pub const Delimiter = enum {
    comma,
    tab,
    pipe,

    fn byte(self: Delimiter) u8 {
        return switch (self) {
            .comma => ',',
            .tab => '\t',
            .pipe => '|',
        };
    }

    fn marker(self: Delimiter) []const u8 {
        return switch (self) {
            .comma => "",
            .tab => "\t",
            .pipe => "|",
        };
    }
};

pub const KeyFolding = enum {
    off,
    safe,
};

pub const PathExpansion = enum {
    off,
    safe,
};

pub const EncodeOptions = struct {
    indent: usize = 2,
    delimiter: Delimiter = .comma,
    key_folding: KeyFolding = .off,
    flatten_depth: ?usize = null,
};

pub const DecodeOptions = struct {
    indent: usize = 2,
    strict: bool = true,
    expand_paths: PathExpansion = .off,
};

pub const Error = error{
    InvalidIndent,
    InvalidHeader,
    InvalidKey,
    InvalidEscape,
    UnterminatedString,
    MissingColon,
    LengthMismatch,
    RowWidthMismatch,
    InvalidStructure,
    ExpansionConflict,
    InvalidOptions,
};

const Header = struct {
    key: ?[]const u8,
    key_quoted: bool = false,
    count: usize,
    delimiter: Delimiter,
    fields: ?[]Field,
    inline_values: []const u8,
};

const Field = struct {
    name: []const u8,
    quoted: bool = false,
};

const Line = struct {
    depth: usize,
    text: []const u8,
};

pub fn encodeValueAlloc(allocator: Allocator, value: Value, options: EncodeOptions) ![]u8 {
    if (options.indent == 0) return error.InvalidOptions;
    var encoder = Encoder{
        .allocator = allocator,
        .options = options,
        .out = .empty,
    };
    errdefer encoder.out.deinit(allocator);

    try encoder.emitValue(value, 0, null, .object_value);
    return try encoder.out.toOwnedSlice(allocator);
}

pub fn decodeValueAlloc(allocator: Allocator, input: []const u8, options: DecodeOptions) !std.json.Parsed(Value) {
    if (options.indent == 0) return error.InvalidOptions;
    var arena = try allocator.create(std.heap.ArenaAllocator);
    errdefer allocator.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    var parser = Parser{
        .allocator = arena.allocator(),
        .options = options,
        .lines = .empty,
    };
    try parser.scan(input);
    var value = try parser.parseRoot();
    if (options.expand_paths == .safe) {
        value = try parser.expandPaths(value);
    }
    return .{ .arena = arena, .value = value };
}

const PrimitiveContext = enum {
    object_value,
    array_cell,
};

const Encoder = struct {
    allocator: Allocator,
    options: EncodeOptions,
    out: std.ArrayList(u8),

    fn emitValue(self: *Encoder, value: Value, depth: usize, key: ?[]const u8, primitive_context: PrimitiveContext) anyerror!void {
        switch (value) {
            .object => |object| try self.emitObject(object, depth, key, true),
            .array => |array| try self.emitArray(array.items, depth, key),
            else => {
                if (key) |name| {
                    try self.startLine(depth);
                    try self.emitKey(name);
                    try self.out.appendSlice(self.allocator, ": ");
                    try self.emitPrimitive(value, switch (primitive_context) {
                        .object_value => self.options.delimiter,
                        .array_cell => self.options.delimiter,
                    });
                } else {
                    try self.emitPrimitive(value, self.options.delimiter);
                }
            },
        }
    }

    fn emitObject(self: *Encoder, object: std.json.ObjectMap, depth: usize, key: ?[]const u8, allow_folding: bool) anyerror!void {
        if (key) |name| {
            try self.startLine(depth);
            try self.emitKey(name);
            try self.out.append(self.allocator, ':');
            if (object.count() == 0) return;
        } else if (object.count() == 0) {
            return;
        }

        var it = object.iterator();
        while (it.next()) |entry| {
            const folded = if (allow_folding)
                try self.foldKeyIfSafe(object, entry.key_ptr.*, entry.value_ptr.*, depth + @as(usize, if (key == null) 0 else 1))
            else
                FoldedField{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
            defer if (folded.owned) self.allocator.free(folded.key);
            try self.emitObjectFieldEx(folded.key, folded.value, depth + @as(usize, if (key == null) 0 else 1), !folded.suppress_child_folding);
        }
    }

    fn emitObjectField(self: *Encoder, key: []const u8, value: Value, depth: usize) anyerror!void {
        try self.emitObjectFieldEx(key, value, depth, true);
    }

    fn emitObjectFieldEx(self: *Encoder, key: []const u8, value: Value, depth: usize, allow_folding: bool) anyerror!void {
        switch (value) {
            .object => |object| try self.emitObject(object, depth, key, allow_folding),
            .array => |array| try self.emitArray(array.items, depth, key),
            else => try self.emitValue(value, depth, key, .object_value),
        }
    }

    const FoldedField = struct {
        key: []const u8,
        value: Value,
        owned: bool = false,
        suppress_child_folding: bool = false,
    };

    fn foldKeyIfSafe(self: *Encoder, siblings: std.json.ObjectMap, key: []const u8, value: Value, depth: usize) !FoldedField {
        _ = depth;
        if (self.options.key_folding != .safe) return .{ .key = key, .value = value };
        const max_depth = self.options.flatten_depth orelse std.math.maxInt(usize);
        if (max_depth < 2) return .{ .key = key, .value = value };
        if (!isIdentifierSegment(key)) return .{ .key = key, .value = value };

        var segments = std.ArrayList([]const u8).empty;
        defer segments.deinit(self.allocator);
        try segments.append(self.allocator, key);

        var current = value;
        while (segments.items.len < max_depth) {
            if (current != .object) break;
            if (current.object.count() != 1) break;
            var it = current.object.iterator();
            const child = it.next() orelse break;
            if (!isIdentifierSegment(child.key_ptr.*)) break;
            try segments.append(self.allocator, child.key_ptr.*);
            current = child.value_ptr.*;
        }

        if (segments.items.len < 2) return .{ .key = key, .value = value };

        var joined = std.ArrayList(u8).empty;
        errdefer joined.deinit(self.allocator);
        for (segments.items, 0..) |segment, i| {
            if (i > 0) try joined.append(self.allocator, '.');
            try joined.appendSlice(self.allocator, segment);
        }
        const folded_key = try joined.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(folded_key);

        if (!std.mem.eql(u8, folded_key, key) and siblings.get(folded_key) != null) {
            self.allocator.free(folded_key);
            return .{ .key = key, .value = value, .suppress_child_folding = true };
        }
        return .{ .key = folded_key, .value = current, .owned = true, .suppress_child_folding = current == .object };
    }

    fn emitArray(self: *Encoder, items: []const Value, depth: usize, key: ?[]const u8) anyerror!void {
        if (allPrimitive(items)) {
            try self.emitArrayHeader(depth, key, items.len, null);
            if (items.len > 0) {
                try self.out.append(self.allocator, ' ');
                try self.emitPrimitiveJoined(items);
            }
            return;
        }

        if (try tabularFieldsAlloc(self.allocator, items)) |fields| {
            defer self.allocator.free(fields);
            try self.emitArrayHeader(depth, key, items.len, fields);
            for (items) |item| {
                try self.startLine(depth + 1);
                for (fields, 0..) |field_name, i| {
                    if (i > 0) try self.out.append(self.allocator, self.options.delimiter.byte());
                    const cell = item.object.get(field_name) orelse return error.InvalidStructure;
                    try self.emitPrimitive(cell, self.options.delimiter);
                }
            }
            return;
        }

        try self.emitArrayHeader(depth, key, items.len, null);
        for (items) |item| {
            try self.emitListItem(item, depth + 1);
        }
    }

    fn emitListItem(self: *Encoder, item: Value, depth: usize) anyerror!void {
        switch (item) {
            .object => |object| {
                try self.startLine(depth);
                try self.out.append(self.allocator, '-');
                if (object.count() == 0) return;

                var it = object.iterator();
                const first = it.next() orelse return;
                try self.out.append(self.allocator, ' ');
                try self.emitFirstListObjectField(first.key_ptr.*, first.value_ptr.*, depth);
                while (it.next()) |entry| {
                    try self.emitObjectField(entry.key_ptr.*, entry.value_ptr.*, depth + 1);
                }
            },
            .array => |array| {
                try self.startLine(depth);
                try self.out.appendSlice(self.allocator, "- ");
                try self.emitArrayAfterPrefix(array.items, null, depth + 1);
            },
            else => {
                try self.startLine(depth);
                try self.out.appendSlice(self.allocator, "- ");
                try self.emitPrimitive(item, self.options.delimiter);
            },
        }
    }

    fn emitFirstListObjectField(self: *Encoder, key: []const u8, value: Value, depth: usize) anyerror!void {
        switch (value) {
            .array => |array| try self.emitArrayAfterPrefix(array.items, key, depth + 2),
            .object => |object| {
                try self.emitKey(key);
                try self.out.append(self.allocator, ':');
                if (object.count() == 0) return;
                var it = object.iterator();
                while (it.next()) |entry| {
                    try self.emitObjectField(entry.key_ptr.*, entry.value_ptr.*, depth + 2);
                }
            },
            else => {
                try self.emitKey(key);
                try self.out.appendSlice(self.allocator, ": ");
                try self.emitPrimitive(value, self.options.delimiter);
            },
        }
    }

    fn emitArrayAfterPrefix(self: *Encoder, items: []const Value, key: ?[]const u8, child_depth: usize) anyerror!void {
        if (allPrimitive(items)) {
            try self.emitHeaderPrefix(key, items.len, null);
            if (items.len > 0) {
                try self.out.append(self.allocator, ' ');
                try self.emitPrimitiveJoined(items);
            }
            return;
        }

        if (try tabularFieldsAlloc(self.allocator, items)) |fields| {
            defer self.allocator.free(fields);
            try self.emitHeaderPrefix(key, items.len, fields);
            for (items) |item| {
                try self.startLine(child_depth);
                for (fields, 0..) |field_name, i| {
                    if (i > 0) try self.out.append(self.allocator, self.options.delimiter.byte());
                    try self.emitPrimitive(item.object.get(field_name).?, self.options.delimiter);
                }
            }
            return;
        }

        try self.emitHeaderPrefix(key, items.len, null);
        for (items) |item| try self.emitListItem(item, child_depth);
    }

    fn emitPrimitiveJoined(self: *Encoder, items: []const Value) !void {
        for (items, 0..) |item, i| {
            if (i > 0) try self.out.append(self.allocator, self.options.delimiter.byte());
            try self.emitPrimitive(item, self.options.delimiter);
        }
    }

    fn emitArrayHeader(self: *Encoder, depth: usize, key: ?[]const u8, count: usize, fields: ?[]const []const u8) !void {
        try self.startLine(depth);
        try self.emitHeaderPrefix(key, count, fields);
    }

    fn emitHeaderPrefix(self: *Encoder, key: ?[]const u8, count: usize, fields: ?[]const []const u8) !void {
        if (key) |name| try self.emitKey(name);
        try self.out.print(self.allocator, "[{d}{s}]", .{ count, self.options.delimiter.marker() });
        if (fields) |field_names| {
            try self.out.append(self.allocator, '{');
            for (field_names, 0..) |field_name, i| {
                if (i > 0) try self.out.append(self.allocator, self.options.delimiter.byte());
                try self.emitKey(field_name);
            }
            try self.out.append(self.allocator, '}');
        }
        try self.out.append(self.allocator, ':');
    }

    fn emitKey(self: *Encoder, key: []const u8) !void {
        if (isUnquotedKey(key)) {
            try self.out.appendSlice(self.allocator, key);
        } else {
            try self.emitQuoted(key);
        }
    }

    fn emitPrimitive(self: *Encoder, value: Value, delimiter: Delimiter) !void {
        switch (value) {
            .null => try self.out.appendSlice(self.allocator, "null"),
            .bool => |b| try self.out.appendSlice(self.allocator, if (b) "true" else "false"),
            .integer => |n| try self.out.print(self.allocator, "{d}", .{n}),
            .float => |n| try self.emitFloat(n),
            .number_string => |s| try self.out.appendSlice(self.allocator, s),
            .string => |s| {
                if (mustQuoteString(s, delimiter)) {
                    try self.emitQuoted(s);
                } else {
                    try self.out.appendSlice(self.allocator, s);
                }
            },
            .array, .object => return error.InvalidStructure,
        }
    }

    fn emitFloat(self: *Encoder, n: f64) !void {
        if (!std.math.isFinite(n)) {
            try self.out.appendSlice(self.allocator, "null");
            return;
        }
        if (n == 0) {
            try self.out.append(self.allocator, '0');
            return;
        }
        try self.out.print(self.allocator, "{d}", .{n});
    }

    fn emitQuoted(self: *Encoder, text: []const u8) !void {
        try self.out.append(self.allocator, '"');
        for (text) |c| {
            switch (c) {
                '\\' => try self.out.appendSlice(self.allocator, "\\\\"),
                '"' => try self.out.appendSlice(self.allocator, "\\\""),
                '\n' => try self.out.appendSlice(self.allocator, "\\n"),
                '\r' => try self.out.appendSlice(self.allocator, "\\r"),
                '\t' => try self.out.appendSlice(self.allocator, "\\t"),
                else => try self.out.append(self.allocator, c),
            }
        }
        try self.out.append(self.allocator, '"');
    }

    fn startLine(self: *Encoder, depth: usize) !void {
        if (self.out.items.len > 0) try self.out.append(self.allocator, '\n');
        try self.out.appendNTimes(self.allocator, ' ', depth * self.options.indent);
    }
};

const Parser = struct {
    allocator: Allocator,
    options: DecodeOptions,
    lines: std.ArrayList(Line),
    quoted_keys: std.ArrayList([]const u8) = .empty,
    index: usize = 0,

    fn scan(self: *Parser, input: []const u8) !void {
        var it = std.mem.splitScalar(u8, input, '\n');
        var previous_nonblank: ?Line = null;
        var saw_blank = false;
        while (it.next()) |raw_line_with_cr| {
            const raw_line = if (std.mem.endsWith(u8, raw_line_with_cr, "\r"))
                raw_line_with_cr[0 .. raw_line_with_cr.len - 1]
            else
                raw_line_with_cr;
            if (std.mem.trim(u8, raw_line, " \t").len == 0) {
                saw_blank = true;
                continue;
            }
            var spaces: usize = 0;
            while (spaces < raw_line.len and raw_line[spaces] == ' ') : (spaces += 1) {}
            if (spaces < raw_line.len and raw_line[spaces] == '\t' and self.options.strict) return error.InvalidIndent;
            if (self.options.strict and spaces % self.options.indent != 0) return error.InvalidIndent;
            const depth = spaces / self.options.indent;
            const text = raw_line[spaces..];
            const line = Line{ .depth = depth, .text = text };
            if (self.options.strict and saw_blank) {
                if (previous_nonblank) |previous| {
                    if (blankLineIsInsideArray(previous, line)) return error.InvalidStructure;
                }
            }
            try self.lines.append(self.allocator, line);
            previous_nonblank = line;
            saw_blank = false;
        }
    }

    fn parseRoot(self: *Parser) anyerror!Value {
        if (self.lines.items.len == 0) return .{ .object = std.json.ObjectMap.empty };
        if (self.lines.items[0].depth != 0) return error.InvalidStructure;

        if (try self.parseHeader(self.lines.items[0].text)) |header| {
            if (header.key == null) {
                const value = try self.parseArray(header, 0, 1);
                if (self.index != self.lines.items.len) return error.InvalidStructure;
                return value;
            }
        }

        if (self.lines.items.len == 1 and !hasUnquotedColon(self.lines.items[0].text)) {
            self.index = 1;
            return try self.parsePrimitiveToken(std.mem.trim(u8, self.lines.items[0].text, " \t"));
        }

        if (self.options.strict) {
            for (self.lines.items) |line| {
                if (line.depth == 0 and !hasUnquotedColon(line.text)) return error.MissingColon;
            }
        }

        const value = try self.parseObjectAtDepth(0);
        if (self.index != self.lines.items.len) return error.InvalidStructure;
        return value;
    }

    fn parseObjectAtDepth(self: *Parser, depth: usize) anyerror!Value {
        var object = std.json.ObjectMap.empty;
        try self.parseObjectFields(&object, depth, if (depth == 0) null else depth - 1);
        return .{ .object = object };
    }

    fn parseObjectFields(self: *Parser, object: *std.json.ObjectMap, depth: usize, stop_depth: ?usize) anyerror!void {
        while (self.index < self.lines.items.len) {
            const line = self.lines.items[self.index];
            if (stop_depth) |stop| {
                if (line.depth <= stop) break;
            }
            if (line.depth < depth) break;
            if (line.depth > depth) return error.InvalidIndent;
            if (std.mem.startsWith(u8, line.text, "-")) break;

            if (try self.parseHeader(line.text)) |header| {
                if (header.key) |key| {
                    if (header.key_quoted) try self.quoted_keys.append(self.allocator, key);
                    const value = try self.parseArray(header, depth, depth + 1);
                    try object.put(self.allocator, key, value);
                    continue;
                }
            }

            const kv = try self.parseKeyValue(line.text);
            if (kv.quoted) try self.quoted_keys.append(self.allocator, kv.key);
            self.index += 1;
            const value = if (kv.value.len == 0)
                if (self.index < self.lines.items.len and self.lines.items[self.index].depth > depth)
                    try self.parseObjectAtDepth(depth + 1)
                else
                    Value{ .object = std.json.ObjectMap.empty }
            else
                try self.parsePrimitiveToken(std.mem.trim(u8, kv.value, " \t"));
            try object.put(self.allocator, kv.key, value);
        }
    }

    fn parseArray(self: *Parser, header: Header, header_depth: usize, child_depth: usize) anyerror!Value {
        _ = header_depth;
        self.index += 1;
        var array = std.json.Array.init(self.allocator);

        if (header.fields) |fields| {
            while (self.index < self.lines.items.len and self.lines.items[self.index].depth == child_depth) {
                const line = self.lines.items[self.index];
                if (hasUnquotedColonBeforeDelimiter(line.text, header.delimiter)) break;
                var tokens = try self.splitDelimited(line.text, header.delimiter);
                defer tokens.deinit(self.allocator);
                if (tokens.items.len != fields.len) return error.RowWidthMismatch;
                var object = std.json.ObjectMap.empty;
                for (fields, 0..) |field, i| {
                    try object.put(self.allocator, field.name, try self.parsePrimitiveToken(tokens.items[i]));
                }
                try array.append(.{ .object = object });
                self.index += 1;
            }
            if (self.options.strict and array.items.len != header.count) return error.LengthMismatch;
            return .{ .array = array };
        }

        if (header.inline_values.len > 0 or header.count == 0) {
            if (header.count > 0) {
                var tokens = try self.splitDelimited(header.inline_values, header.delimiter);
                defer tokens.deinit(self.allocator);
                if (self.options.strict and tokens.items.len != header.count) return error.LengthMismatch;
                for (tokens.items) |token| try array.append(try self.parsePrimitiveToken(token));
            } else if (std.mem.trim(u8, header.inline_values, " \t").len != 0) {
                return error.LengthMismatch;
            }
            return .{ .array = array };
        }

        while (self.index < self.lines.items.len and self.lines.items[self.index].depth == child_depth) {
            const line = self.lines.items[self.index];
            if (!std.mem.startsWith(u8, line.text, "-")) break;
            try array.append(try self.parseListItem(line, child_depth));
        }
        if (self.options.strict and array.items.len != header.count) return error.LengthMismatch;
        return .{ .array = array };
    }

    fn parseListItem(self: *Parser, line: Line, item_depth: usize) anyerror!Value {
        if (std.mem.eql(u8, line.text, "-")) {
            self.index += 1;
            return .{ .object = std.json.ObjectMap.empty };
        }
        if (!std.mem.startsWith(u8, line.text, "- ")) return error.InvalidStructure;
        const rest = line.text[2..];

        if (try self.parseHeader(rest)) |header| {
            if (header.key == null) {
                return try self.parseArrayFromCurrentLine(header, item_depth, item_depth + 1);
            }
            return try self.parseListItemObjectFromHeader(header, item_depth);
        }

        if (hasUnquotedColon(rest)) {
            return try self.parseListItemObjectFromKeyValue(rest, item_depth);
        }

        self.index += 1;
        return try self.parsePrimitiveToken(std.mem.trim(u8, rest, " \t"));
    }

    fn parseArrayFromCurrentLine(self: *Parser, header: Header, header_depth: usize, child_depth: usize) anyerror!Value {
        return try self.parseArray(header, header_depth, child_depth);
    }

    fn parseListItemObjectFromHeader(self: *Parser, header: Header, item_depth: usize) anyerror!Value {
        var object = std.json.ObjectMap.empty;
        const key = header.key orelse return error.InvalidHeader;
        if (header.key_quoted) try self.quoted_keys.append(self.allocator, key);
        const value = try self.parseArray(header, item_depth, item_depth + 2);
        try object.put(self.allocator, key, value);
        try self.parseObjectFields(&object, item_depth + 1, item_depth);
        return .{ .object = object };
    }

    fn parseListItemObjectFromKeyValue(self: *Parser, rest: []const u8, item_depth: usize) anyerror!Value {
        var object = std.json.ObjectMap.empty;
        const kv = try self.parseKeyValue(rest);
        if (kv.quoted) try self.quoted_keys.append(self.allocator, kv.key);
        self.index += 1;
        const value = if (kv.value.len == 0)
            if (self.index < self.lines.items.len and self.lines.items[self.index].depth > item_depth + 1)
                try self.parseObjectAtDepth(item_depth + 2)
            else
                Value{ .object = std.json.ObjectMap.empty }
        else
            try self.parsePrimitiveToken(std.mem.trim(u8, kv.value, " \t"));
        try object.put(self.allocator, kv.key, value);
        try self.parseObjectFields(&object, item_depth + 1, item_depth);
        return .{ .object = object };
    }

    const KeyValue = struct {
        key: []const u8,
        quoted: bool,
        value: []const u8,
    };

    fn parseKeyValue(self: *Parser, text: []const u8) !KeyValue {
        const colon = firstUnquoted(text, ':') orelse return error.MissingColon;
        const raw_key = std.mem.trim(u8, text[0..colon], " \t");
        const value = if (colon + 1 < text.len) text[colon + 1 ..] else "";
        if (raw_key.len == 0) return error.InvalidKey;
        if (raw_key[0] == '"') {
            return .{ .key = try self.parseQuoted(raw_key), .quoted = true, .value = if (std.mem.startsWith(u8, value, " ")) value[1..] else value };
        }
        return .{ .key = try self.allocator.dupe(u8, raw_key), .quoted = false, .value = if (std.mem.startsWith(u8, value, " ")) value[1..] else value };
    }

    fn parseHeader(self: *Parser, text: []const u8) !?Header {
        var pos: usize = 0;
        var key: ?[]const u8 = null;
        var key_quoted = false;

        if (text.len == 0) return null;
        if (text[0] != '[') {
            if (text[0] == '"') {
                const parsed = try parseQuotedTokenBounds(text);
                key = try self.parseQuoted(text[0..parsed.end]);
                key_quoted = true;
                pos = parsed.end;
            } else {
                const bracket = firstUnquoted(text, '[') orelse return null;
                const colon = firstUnquoted(text, ':') orelse return null;
                if (colon < bracket) return null;
                key = try self.allocator.dupe(u8, text[0..bracket]);
                pos = bracket;
            }
        }

        if (pos >= text.len or text[pos] != '[') return null;
        const close = std.mem.indexOfScalarPos(u8, text, pos, ']') orelse return null;
        const bracket_body = text[pos + 1 .. close];
        if (bracket_body.len == 0) return null;
        var delimiter: Delimiter = .comma;
        var digits = bracket_body;
        if (bracket_body[bracket_body.len - 1] == '\t') {
            delimiter = .tab;
            digits = bracket_body[0 .. bracket_body.len - 1];
        } else if (bracket_body[bracket_body.len - 1] == '|') {
            delimiter = .pipe;
            digits = bracket_body[0 .. bracket_body.len - 1];
        }
        if (digits.len == 0) return null;
        const count = std.fmt.parseInt(usize, digits, 10) catch return null;

        pos = close + 1;
        while (pos < text.len and text[pos] == ' ') : (pos += 1) {}

        var fields: ?[]Field = null;
        if (pos < text.len and text[pos] == '{') {
            const field_close = findMatchingBrace(text, pos) orelse return error.InvalidHeader;
            const field_text = text[pos + 1 .. field_close];
            var split = try self.splitDelimitedFields(field_text, delimiter);
            fields = try split.toOwnedSlice(self.allocator);
            pos = field_close + 1;
            while (pos < text.len and text[pos] == ' ') : (pos += 1) {}
        }

        if (pos >= text.len or text[pos] != ':') return null;
        pos += 1;
        var inline_values: []const u8 = "";
        if (pos < text.len) {
            if (text[pos] == ' ') {
                inline_values = text[pos + 1 ..];
            } else {
                inline_values = text[pos..];
            }
        }
        return .{
            .key = key,
            .key_quoted = key_quoted,
            .count = count,
            .delimiter = delimiter,
            .fields = fields,
            .inline_values = inline_values,
        };
    }

    fn splitDelimited(self: *Parser, text: []const u8, delimiter: Delimiter) !std.ArrayList([]const u8) {
        var out = std.ArrayList([]const u8).empty;
        var start: usize = 0;
        var i: usize = 0;
        var in_quotes = false;
        while (i < text.len) : (i += 1) {
            const c = text[i];
            if (c == '"' and (i == 0 or text[i - 1] != '\\')) in_quotes = !in_quotes;
            if (!in_quotes and c == delimiter.byte()) {
                try out.append(self.allocator, std.mem.trim(u8, text[start..i], " \t"));
                start = i + 1;
            }
        }
        try out.append(self.allocator, std.mem.trim(u8, text[start..], " \t"));
        return out;
    }

    fn splitDelimitedFields(self: *Parser, text: []const u8, delimiter: Delimiter) !std.ArrayList(Field) {
        var tokens = try self.splitDelimited(text, delimiter);
        defer tokens.deinit(self.allocator);
        var out = std.ArrayList(Field).empty;
        for (tokens.items) |token| {
            if (token.len > 0 and token[0] == '"') {
                try out.append(self.allocator, .{ .name = try self.parseQuoted(token), .quoted = true });
            } else {
                try out.append(self.allocator, .{ .name = try self.allocator.dupe(u8, token), .quoted = false });
            }
        }
        return out;
    }

    fn parsePrimitiveToken(self: *Parser, raw: []const u8) !Value {
        const token = std.mem.trim(u8, raw, " \t");
        if (token.len == 0) return .{ .string = "" };
        if (token[0] == '"') return .{ .string = try self.parseQuoted(token) };
        if (std.mem.eql(u8, token, "true")) return .{ .bool = true };
        if (std.mem.eql(u8, token, "false")) return .{ .bool = false };
        if (std.mem.eql(u8, token, "null")) return .null;
        if (isNumericLike(token) and !hasForbiddenLeadingZero(token)) {
            if (std.mem.indexOfAny(u8, token, ".eE") == null) {
                const parsed_int = std.fmt.parseInt(i64, token, 10) catch {
                    return .{ .number_string = try self.allocator.dupe(u8, token) };
                };
                return .{ .integer = if (parsed_int == 0) 0 else parsed_int };
            }
            const parsed_float = std.fmt.parseFloat(f64, token) catch return .{ .string = try self.allocator.dupe(u8, token) };
            if (!std.math.isFinite(parsed_float)) return .{ .string = try self.allocator.dupe(u8, token) };
            if (parsed_float == 0) return .{ .integer = 0 };
            const truncated = @trunc(parsed_float);
            if (truncated == parsed_float and truncated >= @as(f64, @floatFromInt(std.math.minInt(i64))) and truncated <= @as(f64, @floatFromInt(std.math.maxInt(i64)))) {
                return .{ .integer = @intFromFloat(truncated) };
            }
            return .{ .float = parsed_float };
        }
        return .{ .string = try self.allocator.dupe(u8, token) };
    }

    fn parseQuoted(self: *Parser, token: []const u8) ![]u8 {
        const bounds = try parseQuotedTokenBounds(token);
        if (bounds.end != token.len) return error.InvalidEscape;
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        var i: usize = 1;
        while (i + 1 < bounds.end) : (i += 1) {
            const c = token[i];
            if (c == '\\') {
                i += 1;
                if (i >= bounds.end - 1) return error.InvalidEscape;
                try out.append(self.allocator, switch (token[i]) {
                    '\\' => '\\',
                    '"' => '"',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    else => return error.InvalidEscape,
                });
            } else {
                try out.append(self.allocator, c);
            }
        }
        return try out.toOwnedSlice(self.allocator);
    }

    fn expandPaths(self: *Parser, value: Value) anyerror!Value {
        if (value != .object) return value;
        var expanded = std.json.ObjectMap.empty;
        var it = value.object.iterator();
        while (it.next()) |entry| {
            const child = try self.expandPaths(entry.value_ptr.*);
            if (canExpandKey(entry.key_ptr.*) and !self.isQuotedKey(entry.key_ptr.*)) {
                try self.insertExpanded(&expanded, entry.key_ptr.*, child);
            } else {
                if (expanded.getPtr(entry.key_ptr.*)) |existing| {
                    if (self.options.strict) return error.ExpansionConflict;
                    existing.* = child;
                } else {
                    try expanded.put(self.allocator, entry.key_ptr.*, child);
                }
            }
        }
        return .{ .object = expanded };
    }

    fn insertExpanded(self: *Parser, object: *std.json.ObjectMap, key: []const u8, value: Value) !void {
        var parts = std.mem.splitScalar(u8, key, '.');
        try self.insertExpandedParts(object, &parts, value);
    }

    fn isQuotedKey(self: *Parser, key: []const u8) bool {
        for (self.quoted_keys.items) |quoted| {
            if (std.mem.eql(u8, quoted, key)) return true;
        }
        return false;
    }

    fn insertExpandedParts(self: *Parser, object: *std.json.ObjectMap, parts: *std.mem.SplitIterator(u8, .scalar), value: Value) anyerror!void {
        const part = parts.next() orelse return;
        if (parts.peek() == null) {
            if (object.getPtr(part)) |existing| {
                if (self.options.strict) return error.ExpansionConflict;
                existing.* = value;
            } else {
                try object.put(self.allocator, try self.allocator.dupe(u8, part), value);
            }
            return;
        }

        if (object.getPtr(part)) |existing| {
            if (existing.* != .object) {
                if (self.options.strict) return error.ExpansionConflict;
                existing.* = .{ .object = std.json.ObjectMap.empty };
            }
            try self.insertExpandedParts(&existing.object, parts, value);
        } else {
            var child = Value{ .object = std.json.ObjectMap.empty };
            try self.insertExpandedParts(&child.object, parts, value);
            try object.put(self.allocator, try self.allocator.dupe(u8, part), child);
        }
    }
};

fn allPrimitive(items: []const Value) bool {
    for (items) |item| switch (item) {
        .array, .object => return false,
        else => {},
    };
    return true;
}

fn tabularFieldsAlloc(allocator: Allocator, items: []const Value) !?[]const []const u8 {
    if (items.len == 0) return null;
    if (items[0] != .object or items[0].object.count() == 0) return null;
    var first_fields = std.ArrayList([]const u8).empty;
    defer first_fields.deinit(allocator);
    var first_it = items[0].object.iterator();
    while (first_it.next()) |entry| {
        switch (entry.value_ptr.*) {
            .array, .object => return null,
            else => {},
        }
        try first_fields.append(allocator, entry.key_ptr.*);
    }

    for (items[1..]) |item| {
        if (item != .object) return null;
        if (item.object.count() != first_fields.items.len) return null;
        for (first_fields.items) |field| {
            const value = item.object.get(field) orelse return null;
            switch (value) {
                .array, .object => return null,
                else => {},
            }
        }
    }
    return try first_fields.toOwnedSlice(allocator);
}

fn isUnquotedKey(key: []const u8) bool {
    if (key.len == 0) return false;
    if (!(std.ascii.isAlphabetic(key[0]) or key[0] == '_')) return false;
    for (key[1..]) |c| {
        if (!(std.ascii.isAlphanumeric(c) or c == '_' or c == '.')) return false;
    }
    return true;
}

fn isIdentifierSegment(key: []const u8) bool {
    if (key.len == 0) return false;
    if (!(std.ascii.isAlphabetic(key[0]) or key[0] == '_')) return false;
    for (key[1..]) |c| {
        if (!(std.ascii.isAlphanumeric(c) or c == '_')) return false;
    }
    return true;
}

fn canExpandKey(key: []const u8) bool {
    if (std.mem.indexOfScalar(u8, key, '.') == null) return false;
    var it = std.mem.splitScalar(u8, key, '.');
    while (it.next()) |part| {
        if (!isIdentifierSegment(part)) return false;
    }
    return true;
}

fn mustQuoteString(s: []const u8, delimiter: Delimiter) bool {
    if (s.len == 0) return true;
    if (std.ascii.isWhitespace(s[0]) or std.ascii.isWhitespace(s[s.len - 1])) return true;
    if (std.mem.eql(u8, s, "true") or std.mem.eql(u8, s, "false") or std.mem.eql(u8, s, "null")) return true;
    if (isNumericLike(s) or isLeadingZeroDecimal(s)) return true;
    if (s[0] == '-') return true;
    for (s) |c| switch (c) {
        ':', '"', '\\', '[', ']', '{', '}', '\n', '\r', '\t' => return true,
        else => if (c == delimiter.byte()) return true,
    };
    return false;
}

fn isNumericLike(s: []const u8) bool {
    if (s.len == 0) return false;
    var i: usize = 0;
    if (s[i] == '-') {
        i += 1;
        if (i == s.len) return false;
    }
    var int_digits: usize = 0;
    while (i < s.len and std.ascii.isDigit(s[i])) : (i += 1) int_digits += 1;
    if (int_digits == 0) return false;
    if (i < s.len and s[i] == '.') {
        i += 1;
        var frac_digits: usize = 0;
        while (i < s.len and std.ascii.isDigit(s[i])) : (i += 1) frac_digits += 1;
        if (frac_digits == 0) return false;
    }
    if (i < s.len and (s[i] == 'e' or s[i] == 'E')) {
        i += 1;
        if (i < s.len and (s[i] == '+' or s[i] == '-')) i += 1;
        var exp_digits: usize = 0;
        while (i < s.len and std.ascii.isDigit(s[i])) : (i += 1) exp_digits += 1;
        if (exp_digits == 0) return false;
    }
    return i == s.len;
}

fn isLeadingZeroDecimal(s: []const u8) bool {
    const start: usize = if (std.mem.startsWith(u8, s, "-")) 1 else 0;
    return s.len - start > 1 and s[start] == '0' and std.ascii.isDigit(s[start + 1]);
}

fn hasForbiddenLeadingZero(s: []const u8) bool {
    const start: usize = if (std.mem.startsWith(u8, s, "-")) 1 else 0;
    if (s.len <= start + 1) return false;
    return s[start] == '0' and std.ascii.isDigit(s[start + 1]);
}

fn firstUnquoted(text: []const u8, needle: u8) ?usize {
    var in_quotes = false;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '\\' and in_quotes) {
            i += 1;
            continue;
        }
        if (c == '"') {
            in_quotes = !in_quotes;
            continue;
        }
        if (!in_quotes and c == needle) return i;
    }
    return null;
}

fn hasUnquotedColon(text: []const u8) bool {
    return firstUnquoted(text, ':') != null;
}

fn hasUnquotedColonBeforeDelimiter(text: []const u8, delimiter: Delimiter) bool {
    const colon = firstUnquoted(text, ':') orelse return false;
    const delim = firstUnquoted(text, delimiter.byte()) orelse return true;
    return colon < delim;
}

fn blankLineIsInsideArray(previous: Line, next: Line) bool {
    if (previous.depth == next.depth and std.mem.startsWith(u8, previous.text, "-") and std.mem.startsWith(u8, next.text, "-")) {
        return true;
    }
    if (previous.depth == next.depth and previous.depth > 0 and !hasUnquotedColon(previous.text) and !hasUnquotedColon(next.text)) {
        return true;
    }
    return false;
}

fn findMatchingBrace(text: []const u8, open: usize) ?usize {
    var in_quotes = false;
    var i = open + 1;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '\\' and in_quotes) {
            i += 1;
            continue;
        }
        if (c == '"') {
            in_quotes = !in_quotes;
            continue;
        }
        if (!in_quotes and c == '}') return i;
    }
    return null;
}

const QuotedBounds = struct { end: usize };

fn parseQuotedTokenBounds(token: []const u8) !QuotedBounds {
    if (token.len == 0 or token[0] != '"') return error.InvalidEscape;
    var i: usize = 1;
    while (i < token.len) : (i += 1) {
        switch (token[i]) {
            '\\' => {
                i += 1;
                if (i >= token.len) return error.UnterminatedString;
                switch (token[i]) {
                    '\\', '"', 'n', 'r', 't' => {},
                    else => return error.InvalidEscape,
                }
            },
            '"' => return .{ .end = i + 1 },
            else => {},
        }
    }
    return error.UnterminatedString;
}

test "encodes and decodes tabular objects" {
    const alloc = std.testing.allocator;
    var users = std.json.Array.init(alloc);
    defer users.deinit();

    var ada = std.json.ObjectMap.empty;
    defer ada.deinit(alloc);
    try ada.put(alloc, "id", .{ .integer = 1 });
    try ada.put(alloc, "name", .{ .string = "Ada" });
    try users.append(.{ .object = ada });

    var bob = std.json.ObjectMap.empty;
    defer bob.deinit(alloc);
    try bob.put(alloc, "id", .{ .integer = 2 });
    try bob.put(alloc, "name", .{ .string = "Bob" });
    try users.append(.{ .object = bob });

    var root = std.json.ObjectMap.empty;
    defer root.deinit(alloc);
    try root.put(alloc, "users", .{ .array = users });

    const encoded = try encodeValueAlloc(alloc, .{ .object = root }, .{});
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings("users[2]{id,name}:\n  1,Ada\n  2,Bob", encoded);

    var parsed = try decodeValueAlloc(alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("users").?.array.items.len);
}
