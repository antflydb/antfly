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

pub fn supportsType(comptime T: type) bool {
    if (std.meta.hasFn(T, "jsonParse")) return true;

    switch (@typeInfo(T)) {
        .bool, .int, .float, .comptime_int, .comptime_float => return true,
        .optional => |info| return supportsType(info.child),
        .@"enum" => return true,
        .@"union" => |info| {
            if (info.tag_type == null) return false;
            inline for (info.fields) |field| {
                if (field.type != void and !supportsType(field.type)) return false;
            }
            return true;
        },
        .@"struct" => |info| {
            inline for (info.fields) |field| {
                if (field.is_comptime) return false;
                if (!supportsType(field.type)) return false;
            }
            return true;
        },
        .array => |info| return supportsType(info.child),
        .vector => |info| return supportsType(info.child),
        .pointer => |info| switch (info.size) {
            .slice => return if (info.child == u8) true else supportsType(info.child),
            .one => return supportsType(info.child),
            else => return false,
        },
        else => return false,
    }
}

pub fn containsCustomJsonParseType(comptime T: type) bool {
    if (std.meta.hasFn(T, "jsonParse")) return true;

    switch (@typeInfo(T)) {
        .optional => |info| return containsCustomJsonParseType(info.child),
        .@"struct" => |info| {
            inline for (info.fields) |field| {
                if (containsCustomJsonParseType(field.type)) return true;
            }
            return false;
        },
        .@"union" => |info| {
            inline for (info.fields) |field| {
                if (containsCustomJsonParseType(field.type)) return true;
            }
            return false;
        },
        .array => |info| return containsCustomJsonParseType(info.child),
        .vector => |info| return containsCustomJsonParseType(info.child),
        else => return false,
    }
}

pub fn parseFromSlice(
    comptime T: type,
    allocator: Allocator,
    input: []const u8,
    options: json.ParseOptions,
    structural_index: simd_stage1.StructuralIndex,
) json.ParseError(json.Scanner)!json.Parsed(T) {
    var parsed = json.Parsed(T){
        .arena = try allocator.create(std.heap.ArenaAllocator),
        .value = undefined,
    };
    errdefer allocator.destroy(parsed.arena);
    parsed.arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer parsed.arena.deinit();

    parsed.value = try parseFromSliceLeaky(T, parsed.arena.allocator(), input, options, structural_index);
    return parsed;
}

pub fn parseFromSliceLeaky(
    comptime T: type,
    allocator: Allocator,
    input: []const u8,
    options: json.ParseOptions,
    structural_index: simd_stage1.StructuralIndex,
) json.ParseError(json.Scanner)!T {
    var parser = Parser.init(allocator, input, options, structural_index);
    parser.skipWhitespace();
    const value = try parser.parseType(T);
    parser.skipWhitespace();
    if (!parser.atEnd()) return error.SyntaxError;
    return value;
}

const Parser = struct {
    const StructFieldCacheEntry = struct {
        type_name: []const u8,
        field_name: []const u8,
        field_index: usize,
    };

    allocator: Allocator,
    input: []const u8,
    pos: usize,
    quote_cursor: usize,
    structural_cursor: usize,
    max_value_len: usize,
    options: json.ParseOptions,
    allocate_strings: json.AllocWhen,
    quote_offsets: []const u32,
    has_escapes: bool,
    has_string_controls: bool,
    structural_offsets: []const u32,
    structural_index: simd_stage1.StructuralIndex,
    struct_field_cache_len: usize,
    struct_field_cache: [8]StructFieldCacheEntry,

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
            .structural_cursor = 0,
            .max_value_len = options.max_value_len orelse input.len,
            .options = options,
            .allocate_strings = options.allocate orelse .alloc_if_needed,
            .quote_offsets = structural_index.quote_offsets,
            .has_escapes = structural_index.has_escapes,
            .has_string_controls = structural_index.has_string_controls,
            .structural_offsets = structural_index.offsets,
            .structural_index = structural_index,
            .struct_field_cache_len = 0,
            .struct_field_cache = undefined,
        };
    }

    fn parseType(self: *Parser, comptime T: type) json.ParseError(json.Scanner)!T {
        if (comptime std.meta.hasFn(T, "jsonParse")) {
            return self.parseViaStdlibSubtree(T);
        }

        switch (@typeInfo(T)) {
            .bool => return self.parseBool(),
            .float, .comptime_float => return self.parseFloat(T),
            .int, .comptime_int => return self.parseInt(T),
            .optional => |info| {
                if (self.startsWith("null")) {
                    self.pos += 4;
                    return null;
                }
                return try self.parseType(info.child);
            },
            .@"enum" => return self.parseEnum(T),
            .@"union" => |info| return self.parseUnion(T, info),
            .@"struct" => |info| {
                if (info.is_tuple) return self.parseTupleStruct(T, info);
                return self.parseStruct(T, info);
            },
            .array => |info| return self.parseArray(T, info.child, info.len),
            .vector => |info| return self.parseVector(T, info.child, info.len),
            .pointer => |info| return switch (info.size) {
                .slice => self.parseSlice(T, info.child),
                .one => self.parseOnePointer(T, info.child),
                else => @compileError("pointer size not supported by simd_typed: " ++ @typeName(T)),
            },
            else => @compileError("type not supported by simd_typed: " ++ @typeName(T)),
        }
    }

    fn parseViaStdlibSubtree(self: *Parser, comptime T: type) json.ParseError(json.Scanner)!T {
        const raw = try self.captureValueSliceForStdlib();
        return std.json.parseFromSliceLeaky(T, self.allocator, raw, self.options);
    }

    fn parseBool(self: *Parser) json.ParseError(json.Scanner)!bool {
        if (self.startsWith("true")) {
            self.pos += 4;
            return true;
        }
        if (self.startsWith("false")) {
            self.pos += 5;
            return false;
        }
        return error.UnexpectedToken;
    }

    fn parseFloat(self: *Parser, comptime T: type) json.ParseError(json.Scanner)!T {
        if (!self.atEnd() and self.input[self.pos] == '"') {
            const owned = try self.parseStringAlloc();
            defer self.allocator.free(owned);
            return try std.fmt.parseFloat(T, owned);
        }

        const slice = try self.parseNumberLikeSlice();
        return try std.fmt.parseFloat(T, slice);
    }

    fn parseInt(self: *Parser, comptime T: type) json.ParseError(json.Scanner)!T {
        if (!self.atEnd() and self.input[self.pos] == '"') {
            const owned = try self.parseStringAlloc();
            defer self.allocator.free(owned);
            return try std.fmt.parseInt(T, owned, 10);
        }

        const slice = try self.parseNumberLikeSlice();
        return try std.fmt.parseInt(T, slice, 10);
    }

    fn parseEnum(self: *Parser, comptime T: type) json.ParseError(json.Scanner)!T {
        if (!self.atEnd() and self.input[self.pos] == '"') {
            const owned = try self.parseStringAlloc();
            defer self.allocator.free(owned);
            return std.meta.stringToEnum(T, owned) orelse return error.InvalidEnumTag;
        }

        const slice = try self.parseNumberLikeSlice();

        if (slice.len > 0 and (slice[0] == '-' or std.ascii.isDigit(slice[0]))) {
            const tag_int = try std.fmt.parseInt(std.meta.Tag(T), slice, 10);
            return std.enums.fromInt(T, tag_int) orelse return error.InvalidEnumTag;
        }

        return std.meta.stringToEnum(T, slice) orelse return error.InvalidEnumTag;
    }

    fn parseStruct(self: *Parser, comptime T: type, comptime info: std.builtin.Type.Struct) json.ParseError(json.Scanner)!T {
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '{') return error.UnexpectedToken;
        self.pos += 1;
        self.skipWhitespace();

        var result: T = undefined;
        var seen = [_]bool{false} ** info.fields.len;

        if (!self.atEnd() and self.input[self.pos] == '}') {
            self.pos += 1;
            try fillDefaults(T, &result, &seen);
            return result;
        }

        while (true) {
            if (self.atEnd()) return error.UnexpectedEndOfInput;
            if (self.input[self.pos] != '"') return error.UnexpectedToken;

            const field_name = try self.parseFieldNameForMatch();
            defer field_name.deinit(self.allocator);

            self.skipWhitespace();
            if (self.atEnd()) return error.UnexpectedEndOfInput;
            if (self.input[self.pos] != ':') return error.UnexpectedToken;
            self.pos += 1;
            self.skipWhitespace();

            var matched = false;
            if (self.cachedStructFieldIndex(T, field_name.slice())) |i| {
                try self.parseStructFieldAtIndex(T, info, &result, &seen, i);
                matched = true;
            } else inline for (info.fields, 0..) |field, i| {
                if (std.mem.eql(u8, field.name, field_name.slice())) {
                    self.rememberStructField(T, field.name, i);
                    if (seen[i]) {
                        switch (self.options.duplicate_field_behavior) {
                            .use_first => {
                                _ = try self.parseType(field.type);
                                matched = true;
                                break;
                            },
                            .@"error" => return error.DuplicateField,
                            .use_last => {},
                        }
                    }
                    @field(result, field.name) = try self.parseType(field.type);
                    seen[i] = true;
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                if (self.options.ignore_unknown_fields) {
                    try self.skipValue();
                } else {
                    return error.UnknownField;
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
                    try fillDefaults(T, &result, &seen);
                    return result;
                },
                else => return error.UnexpectedToken,
            }
        }
    }

    fn parseStructFieldAtIndex(
        self: *Parser,
        comptime T: type,
        comptime info: std.builtin.Type.Struct,
        result: *T,
        seen: []bool,
        field_index: usize,
    ) json.ParseError(json.Scanner)!void {
        inline for (info.fields, 0..) |field, i| {
            if (field_index == i) {
                if (seen[i]) {
                    switch (self.options.duplicate_field_behavior) {
                        .use_first => {
                            _ = try self.parseType(field.type);
                            return;
                        },
                        .@"error" => return error.DuplicateField,
                        .use_last => {},
                    }
                }
                @field(result.*, field.name) = try self.parseType(field.type);
                seen[i] = true;
                return;
            }
        }
        unreachable;
    }

    fn cachedStructFieldIndex(self: *const Parser, comptime T: type, field_name: []const u8) ?usize {
        const type_name = @typeName(T);
        var i: usize = self.struct_field_cache_len;
        while (i > 0) {
            i -= 1;
            const entry = self.struct_field_cache[i];
            if (std.mem.eql(u8, entry.type_name, type_name) and std.mem.eql(u8, entry.field_name, field_name)) {
                return entry.field_index;
            }
        }
        return null;
    }

    fn rememberStructField(self: *Parser, comptime T: type, field_name: []const u8, field_index: usize) void {
        const entry: StructFieldCacheEntry = .{
            .type_name = @typeName(T),
            .field_name = field_name,
            .field_index = field_index,
        };
        if (self.struct_field_cache_len < self.struct_field_cache.len) {
            self.struct_field_cache[self.struct_field_cache_len] = entry;
            self.struct_field_cache_len += 1;
            return;
        }

        std.mem.copyBackwards(StructFieldCacheEntry, self.struct_field_cache[0 .. self.struct_field_cache.len - 1], self.struct_field_cache[1..]);
        self.struct_field_cache[self.struct_field_cache.len - 1] = entry;
    }

    fn parseTupleStruct(self: *Parser, comptime T: type, comptime info: std.builtin.Type.Struct) json.ParseError(json.Scanner)!T {
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '[') return error.UnexpectedToken;
        self.pos += 1;
        self.skipWhitespace();

        var result: T = undefined;

        inline for (info.fields, 0..) |field, i| {
            if (self.atEnd()) return error.UnexpectedEndOfInput;
            if (i > 0) {
                if (self.input[self.pos] != ',') return error.UnexpectedToken;
                self.pos += 1;
                self.skipWhitespace();
            }
            result[i] = try self.parseType(field.type);
            self.skipWhitespace();
        }

        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != ']') return error.UnexpectedToken;
        self.pos += 1;
        return result;
    }

    fn parseVector(self: *Parser, comptime T: type, comptime Child: type, comptime len: usize) json.ParseError(json.Scanner)!T {
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '[') return error.UnexpectedToken;

        const A = [len]Child;
        const arr: A = try self.parseArray(A, Child, len);
        return arr;
    }

    fn parseUnion(self: *Parser, comptime T: type, comptime info: std.builtin.Type.Union) json.ParseError(json.Scanner)!T {
        if (info.tag_type == null) @compileError("untagged unions are not supported by simd_typed yet");
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '{') return error.UnexpectedToken;
        self.pos += 1;
        self.skipWhitespace();

        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '"') return error.UnexpectedToken;

        const field_name = try self.parseFieldNameForMatch();
        defer field_name.deinit(self.allocator);

        self.skipWhitespace();
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != ':') return error.UnexpectedToken;
        self.pos += 1;
        self.skipWhitespace();

        inline for (info.fields) |field| {
            if (std.mem.eql(u8, field.name, field_name.slice())) {
                const result = if (field.type == void)
                    try self.parseVoidPayloadUnion(T, field.name)
                else
                    @unionInit(T, field.name, try self.parseType(field.type));

                self.skipWhitespace();
                if (self.atEnd()) return error.UnexpectedEndOfInput;
                if (self.input[self.pos] != '}') return error.UnexpectedToken;
                self.pos += 1;
                return result;
            }
        }

        return error.UnknownField;
    }

    fn parseVoidPayloadUnion(self: *Parser, comptime T: type, comptime field_name: []const u8) json.ParseError(json.Scanner)!T {
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '{') return error.UnexpectedToken;
        self.pos += 1;
        self.skipWhitespace();
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '}') return error.UnexpectedToken;
        self.pos += 1;
        return @unionInit(T, field_name, {});
    }

    const ParsedFieldName = union(enum) {
        direct: []const u8,
        allocated: []u8,

        fn slice(self: ParsedFieldName) []const u8 {
            return switch (self) {
                .direct => |s| s,
                .allocated => |s| s,
            };
        }

        fn deinit(self: ParsedFieldName, allocator: Allocator) void {
            switch (self) {
                .direct => {},
                .allocated => |s| allocator.free(s),
            }
        }
    };

    fn parseArray(self: *Parser, comptime T: type, comptime Child: type, comptime len: usize) json.ParseError(json.Scanner)!T {
        if (Child == u8 and !self.atEnd() and self.input[self.pos] == '"') {
            const s = try self.parseStringAlloc();
            defer self.allocator.free(s);
            if (s.len != len) return error.LengthMismatch;
            var result: T = undefined;
            @memcpy(result[0..], s);
            return result;
        }

        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '[') return error.UnexpectedToken;
        self.pos += 1;
        self.skipWhitespace();

        var result: T = undefined;
        var i: usize = 0;
        if (!self.atEnd() and self.input[self.pos] == ']') {
            if (len != 0) return error.LengthMismatch;
            self.pos += 1;
            return result;
        }

        while (true) {
            if (i >= len) return error.LengthMismatch;
            result[i] = try self.parseType(Child);
            i += 1;

            self.skipWhitespace();
            if (self.atEnd()) return error.UnexpectedEndOfInput;
            switch (self.input[self.pos]) {
                ',' => {
                    self.pos += 1;
                    self.skipWhitespace();
                },
                ']' => {
                    self.pos += 1;
                    if (i != len) return error.LengthMismatch;
                    return result;
                },
                else => return error.UnexpectedToken,
            }
        }
    }

    fn parseSlice(self: *Parser, comptime T: type, comptime Child: type) json.ParseError(json.Scanner)!T {
        if (Child == u8) return try self.parseStringSlice(T);

        if (self.atEnd()) return error.UnexpectedEndOfInput;
        if (self.input[self.pos] != '[') return error.UnexpectedToken;
        self.pos += 1;
        self.skipWhitespace();

        var list = std.ArrayListUnmanaged(Child).empty;
        errdefer list.deinit(self.allocator);

        if (!self.atEnd() and self.input[self.pos] == ']') {
            self.pos += 1;
            return try list.toOwnedSlice(self.allocator);
        }

        while (true) {
            try list.append(self.allocator, try self.parseType(Child));
            self.skipWhitespace();
            if (self.atEnd()) return error.UnexpectedEndOfInput;
            switch (self.input[self.pos]) {
                ',' => {
                    self.pos += 1;
                    self.skipWhitespace();
                },
                ']' => {
                    self.pos += 1;
                    return try list.toOwnedSlice(self.allocator);
                },
                else => return error.UnexpectedToken,
            }
        }
    }

    fn parseOnePointer(self: *Parser, comptime T: type, comptime Child: type) json.ParseError(json.Scanner)!T {
        const ptr = try self.allocator.create(Child);
        errdefer self.allocator.destroy(ptr);
        ptr.* = try self.parseType(Child);
        return ptr;
    }

    fn skipValue(self: *Parser) json.ParseError(json.Scanner)!void {
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        switch (self.input[self.pos]) {
            '{' => {
                self.pos += 1;
                self.skipWhitespace();
                if (!self.atEnd() and self.input[self.pos] == '}') {
                    self.pos += 1;
                    return;
                }
                while (true) {
                    if (self.atEnd() or self.input[self.pos] != '"') return error.UnexpectedToken;
                    try self.skipString();
                    self.skipWhitespace();
                    if (self.atEnd() or self.input[self.pos] != ':') return error.UnexpectedToken;
                    self.pos += 1;
                    self.skipWhitespace();
                    try self.skipValue();
                    self.skipWhitespace();
                    if (self.atEnd()) return error.UnexpectedEndOfInput;
                    switch (self.input[self.pos]) {
                        ',' => {
                            self.pos += 1;
                            self.skipWhitespace();
                        },
                        '}' => {
                            self.pos += 1;
                            return;
                        },
                        else => return error.UnexpectedToken,
                    }
                }
            },
            '[' => {
                self.pos += 1;
                self.skipWhitespace();
                if (!self.atEnd() and self.input[self.pos] == ']') {
                    self.pos += 1;
                    return;
                }
                while (true) {
                    try self.skipValue();
                    self.skipWhitespace();
                    if (self.atEnd()) return error.UnexpectedEndOfInput;
                    switch (self.input[self.pos]) {
                        ',' => {
                            self.pos += 1;
                            self.skipWhitespace();
                        },
                        ']' => {
                            self.pos += 1;
                            return;
                        },
                        else => return error.UnexpectedToken,
                    }
                }
            },
            '"' => try self.skipString(),
            't', 'f', 'n', '-', '0'...'9' => try self.skipScalar(),
            else => return error.UnexpectedToken,
        }
    }

    fn skipScalar(self: *Parser) json.ParseError(json.Scanner)!void {
        if (self.atEnd()) return error.UnexpectedEndOfInput;
        switch (self.input[self.pos]) {
            't' => try self.skipLiteral("true"),
            'f' => try self.skipLiteral("false"),
            'n' => try self.skipLiteral("null"),
            '-', '0'...'9' => try self.skipNumberLike(),
            else => return error.UnexpectedToken,
        }
    }

    fn skipLiteral(self: *Parser, comptime literal: []const u8) json.ParseError(json.Scanner)!void {
        if (!self.startsWith(literal)) return error.UnexpectedToken;
        self.pos += literal.len;
    }

    fn skipNumberLike(self: *Parser) json.ParseError(json.Scanner)!void {
        const start = self.pos;
        const end = self.findPrimitiveEnd();
        const slice = self.input[start..end];
        try validateNumberLikeSlice(slice);
        if (slice.len > self.max_value_len) return error.ValueTooLong;
        self.pos = end;
    }

    fn parseNumberLikeSlice(self: *Parser) json.ParseError(json.Scanner)![]const u8 {
        const start = self.pos;
        try self.skipNumberLike();
        return self.input[start..self.pos];
    }

    fn captureValueSliceForStdlib(self: *Parser) json.ParseError(json.Scanner)![]const u8 {
        const start = self.pos;
        const end = try self.findValueEndByStructural();
        self.pos = end;
        return self.input[start..end];
    }

    fn findValueEndByStructural(self: *Parser) json.ParseError(json.Scanner)!usize {
        if (self.atEnd()) return error.UnexpectedEndOfInput;

        return switch (self.input[self.pos]) {
            '"' => (try self.currentStringClosingQuote()) + 1,
            '{', '[' => self.findCompositeEndByStructural(),
            else => self.findPrimitiveEnd(),
        };
    }

    fn findCompositeEndByStructural(self: *Parser) json.ParseError(json.Scanner)!usize {
        std.debug.assert(self.input[self.pos] == '{' or self.input[self.pos] == '[');

        self.syncStructuralCursor();
        if (self.structural_cursor >= self.structural_offsets.len or self.structural_offsets[self.structural_cursor] != self.pos) {
            return error.SyntaxError;
        }

        var depth: usize = 0;
        var cursor = self.structural_cursor;
        while (cursor < self.structural_offsets.len) : (cursor += 1) {
            const offset = self.structural_offsets[cursor];
            switch (self.input[offset]) {
                '{', '[' => depth += 1,
                '}', ']' => {
                    if (depth == 0) return error.SyntaxError;
                    depth -= 1;
                    if (depth == 0) {
                        self.structural_cursor = cursor + 1;
                        return offset + 1;
                    }
                },
                else => {},
            }
        }

        return error.UnexpectedEndOfInput;
    }

    fn findPrimitiveEnd(self: *Parser) usize {
        return std.mem.indexOfAnyPos(u8, self.input, self.pos, " \t\r\n{}[]:,") orelse self.input.len;
    }

    fn parseStringSlice(self: *Parser, comptime T: type) json.ParseError(json.Scanner)!T {
        const ptr_info = @typeInfo(T).pointer;
        std.debug.assert(ptr_info.size == .slice and ptr_info.child == u8);

        const closing_quote = try self.currentStringClosingQuote();
        const direct_content = self.input[self.pos + 1 .. closing_quote];

        if (ptr_info.is_const and ptr_info.sentinel() == null and self.allocate_strings == .alloc_if_needed and self.canDirectCopyString(direct_content)) {
            if (direct_content.len > self.max_value_len) return error.ValueTooLong;
            self.pos = closing_quote + 1;
            self.quote_cursor += 2;
            return direct_content;
        }

        const owned = try self.parseStringAlloc();
        if (ptr_info.sentinel()) |sentinel| {
            const out = try self.allocator.allocSentinel(u8, owned.len, sentinel);
            @memcpy(out[0..owned.len], owned);
            self.allocator.free(owned);
            return out;
        }
        return owned;
    }

    fn parseFieldNameForMatch(self: *Parser) json.ParseError(json.Scanner)!ParsedFieldName {
        std.debug.assert(self.input[self.pos] == '"');

        const closing_quote = try self.currentStringClosingQuote();
        const direct_content = self.input[self.pos + 1 .. closing_quote];
        if (self.canDirectCopyString(direct_content)) {
            if (direct_content.len > self.max_value_len) return error.ValueTooLong;
            self.pos = closing_quote + 1;
            self.quote_cursor += 2;
            return .{ .direct = direct_content };
        }

        return .{ .allocated = try self.parseStringAlloc() };
    }

    fn skipString(self: *Parser) json.ParseError(json.Scanner)!void {
        std.debug.assert(self.input[self.pos] == '"');

        const closing_quote = try self.currentStringClosingQuote();
        const content = self.input[self.pos + 1 .. closing_quote];

        if (!self.canDirectCopyString(content)) {
            try self.validateEscapedStringContent(content);
        }

        if (content.len > self.max_value_len) return error.ValueTooLong;
        self.pos = closing_quote + 1;
        self.quote_cursor += 2;
    }

    fn parseStringAlloc(self: *Parser) json.ParseError(json.Scanner)![]u8 {
        std.debug.assert(self.input[self.pos] == '"');

        const closing_quote = try self.currentStringClosingQuote();
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
            return 0x10000 + ((@as(u21, first - 0xD800) << 10) | @as(u21, second - 0xDC00));
        }
        if (first >= 0xDC00 and first <= 0xDFFF) return error.SyntaxError;
        return @intCast(first);
    }

    fn skipWhitespace(self: *Parser) void {
        while (!self.atEnd()) {
            switch (self.input[self.pos]) {
                ' ', '\t', '\r', '\n' => self.pos += 1,
                else => return,
            }
        }
    }

    fn startsWith(self: *const Parser, comptime needle: []const u8) bool {
        return self.pos + needle.len <= self.input.len and std.mem.eql(u8, self.input[self.pos .. self.pos + needle.len], needle);
    }

    fn currentStringClosingQuote(self: *Parser) json.ParseError(json.Scanner)!usize {
        self.syncQuoteCursor();
        if (self.quote_cursor >= self.quote_offsets.len or self.quote_offsets[self.quote_cursor] != self.pos) {
            return error.SyntaxError;
        }
        if (self.quote_cursor + 1 >= self.quote_offsets.len) return error.UnexpectedEndOfInput;
        return self.quote_offsets[self.quote_cursor + 1];
    }

    fn canDirectCopyString(self: *const Parser, content: []const u8) bool {
        if (!self.has_escapes and !self.has_string_controls) return true;
        for (content) |b| {
            if (self.has_escapes and b == '\\') return false;
            if (b <= 0x1F) return false;
        }
        return true;
    }

    fn validateEscapedStringContent(self: *Parser, content: []const u8) json.ParseError(json.Scanner)!void {
        var i: usize = 0;
        while (i < content.len) {
            const b = content[i];
            switch (b) {
                '\\' => {
                    i += 1;
                    if (i >= content.len) return error.SyntaxError;
                    switch (content[i]) {
                        '"', '\\', '/', 'b', 'f', 'n', 'r', 't' => i += 1,
                        'u' => {
                            i += 1;
                            const first = try self.parseHexQuadFromSlice(content, &i);
                            if (first >= 0xD800 and first <= 0xDBFF) {
                                if (i + 2 > content.len) return error.SyntaxError;
                                if (content[i] != '\\' or content[i + 1] != 'u') return error.SyntaxError;
                                i += 2;
                                const second = try self.parseHexQuadFromSlice(content, &i);
                                if (second < 0xDC00 or second > 0xDFFF) return error.SyntaxError;
                            } else if (first >= 0xDC00 and first <= 0xDFFF) {
                                return error.SyntaxError;
                            }
                        },
                        else => return error.SyntaxError,
                    }
                },
                0...0x1F => return error.SyntaxError,
                else => i += 1,
            }
        }
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

    fn syncQuoteCursor(self: *Parser) void {
        while (self.quote_cursor < self.quote_offsets.len and self.quote_offsets[self.quote_cursor] < self.pos) {
            self.quote_cursor += 1;
        }
    }

    fn syncStructuralCursor(self: *Parser) void {
        while (self.structural_cursor < self.structural_offsets.len and self.structural_offsets[self.structural_cursor] < self.pos) {
            self.structural_cursor += 1;
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
        _ = self.structural_index;
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

fn fillDefaults(comptime T: type, result: *T, seen: []bool) json.ParseFromValueError!void {
    const info = @typeInfo(T).@"struct";
    inline for (info.fields, 0..) |field, i| {
        if (!seen[i]) {
            if (field.default_value_ptr) |ptr| {
                const typed_ptr: *align(@alignOf(field.type)) const field.type = @ptrCast(@alignCast(ptr));
                @field(result.*, field.name) = typed_ptr.*;
            } else if (@typeInfo(field.type) == .optional) {
                @field(result.*, field.name) = null;
            } else {
                return error.MissingField;
            }
        }
    }
}

test "simd typed parser parses nested structs arrays and escapes natively" {
    const alloc = std.testing.allocator;
    const T = struct {
        name: []const u8,
        count: u32,
        enabled: bool = false,
        tags: []const []const u8,
        sub: struct {
            msg: []const u8,
        },
    };

    const raw =
        \\{"name":"ada","count":"7","tags":["x","y"],"sub":{"msg":"line\n"}}
    ;

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, raw);
    defer structural.deinit(alloc);

    var parsed = try parseFromSlice(T, alloc, raw, .{}, structural);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("ada", parsed.value.name);
    try std.testing.expectEqual(@as(u32, 7), parsed.value.count);
    try std.testing.expectEqual(false, parsed.value.enabled);
    try std.testing.expectEqualStrings("x", parsed.value.tags[0]);
    try std.testing.expectEqualStrings("line\n", parsed.value.sub.msg);
}

test "simd typed parser handles duplicate and unknown fields" {
    const alloc = std.testing.allocator;
    const T = struct {
        count: u32 = 0,
    };
    const raw = "{\"count\":1,\"count\":2,\"extra\":true}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, raw);
    defer structural.deinit(alloc);

    var parsed = try parseFromSlice(T, alloc, raw, .{
        .duplicate_field_behavior = .use_last,
        .ignore_unknown_fields = true,
    }, structural);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 2), parsed.value.count);
}

test "simd typed parser skips unknown nested values without allocating discarded strings" {
    const alloc = std.testing.allocator;
    const T = struct {
        count: u32,
    };
    const raw =
        \\{"count":1,"extra":{"msg":"line\\n","list":["alpha","beta"],"nested":{"emoji":"\uD83D\uDE00"}}}
    ;

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, raw);
    defer structural.deinit(alloc);

    var buf: [16]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    const parsed = try parseFromSliceLeaky(T, fba.allocator(), raw, .{
        .ignore_unknown_fields = true,
    }, structural);
    try std.testing.expectEqual(@as(u32, 1), parsed.count);
}

test "simd typed parser still validates skipped unknown string escapes" {
    const alloc = std.testing.allocator;
    const T = struct {
        count: u32,
    };
    const raw = "{\"count\":1,\"extra\":\"\\q\"}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, raw);
    defer structural.deinit(alloc);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    try std.testing.expectError(error.SyntaxError, parseFromSliceLeaky(T, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    }, structural));
}

test "simd typed parser still validates skipped unknown numbers" {
    const alloc = std.testing.allocator;
    const T = struct {
        count: u32,
    };
    const raw = "{\"count\":1,\"extra\":1e+}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, raw);
    defer structural.deinit(alloc);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    try std.testing.expectError(error.InvalidNumber, parseFromSliceLeaky(T, arena.allocator(), raw, .{
        .ignore_unknown_fields = true,
    }, structural));
}

test "simd typed parser matches plain field names without allocating keys" {
    const T = struct {
        count: u32,
        enabled: bool,
    };
    const raw = "{\"count\":1,\"enabled\":true}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(std.testing.allocator, raw);
    defer structural.deinit(std.testing.allocator);

    var buf: [1]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    const parsed = try parseFromSliceLeaky(T, fba.allocator(), raw, .{}, structural);
    try std.testing.expectEqual(@as(u32, 1), parsed.count);
    try std.testing.expectEqual(true, parsed.enabled);
}

test "simd typed parser matches escaped field names" {
    const T = struct {
        count: u32,
    };
    const raw = "{\"co\\u0075nt\":7}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(std.testing.allocator, raw);
    defer structural.deinit(std.testing.allocator);

    var parsed = try parseFromSlice(T, std.testing.allocator, raw, .{}, structural);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 7), parsed.value.count);
}

test "simd typed parser supports tagged unions with payloads and void tags" {
    const alloc = std.testing.allocator;
    const T = union(enum) {
        count: u32,
        msg: []const u8,
        none: void,
    };

    var count_structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "{\"count\":7}");
    defer count_structural.deinit(alloc);
    const count_parsed = try parseFromSliceLeaky(T, alloc, "{\"count\":7}", .{}, count_structural);
    try std.testing.expect(count_parsed == .count);
    try std.testing.expectEqual(@as(u32, 7), count_parsed.count);

    var msg_structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "{\"msg\":\"hi\"}");
    defer msg_structural.deinit(alloc);
    var msg_parsed = try parseFromSlice(T, alloc, "{\"msg\":\"hi\"}", .{}, msg_structural);
    defer msg_parsed.deinit();
    try std.testing.expect(msg_parsed.value == .msg);
    try std.testing.expectEqualStrings("hi", msg_parsed.value.msg);

    var none_structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "{\"none\":{}}");
    defer none_structural.deinit(alloc);
    const none_parsed = try parseFromSliceLeaky(T, alloc, "{\"none\":{}}", .{}, none_structural);
    try std.testing.expect(none_parsed == .none);
}

test "simd typed parser rejects unknown union field names" {
    const alloc = std.testing.allocator;
    const T = union(enum) {
        count: u32,
    };

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "{\"other\":7}");
    defer structural.deinit(alloc);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    try std.testing.expectError(error.UnknownField, parseFromSliceLeaky(T, arena.allocator(), "{\"other\":7}", .{}, structural));
}

test "simd typed parser supports tuple structs as arrays" {
    const alloc = std.testing.allocator;
    const T = struct { u32, []const u8, bool };

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "[7,\"hi\",true]");
    defer structural.deinit(alloc);

    var parsed = try parseFromSlice(T, alloc, "[7,\"hi\",true]", .{}, structural);
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u32, 7), parsed.value[0]);
    try std.testing.expectEqualStrings("hi", parsed.value[1]);
    try std.testing.expectEqual(true, parsed.value[2]);
}

test "simd typed parser enforces tuple struct length" {
    const alloc = std.testing.allocator;
    const T = struct { u32, bool };

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "[1]");
    defer structural.deinit(alloc);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    try std.testing.expectError(error.UnexpectedToken, parseFromSliceLeaky(T, arena.allocator(), "[1]", .{}, structural));
}

test "simd typed parser supports vectors from arrays" {
    const alloc = std.testing.allocator;
    const T = @Vector(4, i32);

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "[1,2,3,4]");
    defer structural.deinit(alloc);

    const parsed = try parseFromSliceLeaky(T, alloc, "[1,2,3,4]", .{}, structural);
    try std.testing.expectEqual(@as(i32, 1), parsed[0]);
    try std.testing.expectEqual(@as(i32, 4), parsed[3]);
}

test "simd typed parser rejects strings for u8 vectors" {
    const alloc = std.testing.allocator;
    const T = @Vector(2, u8);

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "\"hi\"");
    defer structural.deinit(alloc);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    try std.testing.expectError(error.UnexpectedToken, parseFromSliceLeaky(T, arena.allocator(), "\"hi\"", .{}, structural));
}

test "simd typed parser falls back per-subtree for custom jsonParse types" {
    const alloc = std.testing.allocator;
    const Custom = struct {
        value: u32,

        pub fn jsonParse(allocator: Allocator, source: anytype, options: json.ParseOptions) !@This() {
            const raw = try std.json.innerParse(u32, allocator, source, options);
            return .{ .value = raw + 1 };
        }
    };

    const T = struct {
        plain: u32,
        custom: Custom,
    };

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, "{\"plain\":1,\"custom\":4}");
    defer structural.deinit(alloc);

    var parsed = try parseFromSlice(T, alloc, "{\"plain\":1,\"custom\":4}", .{}, structural);
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u32, 1), parsed.value.plain);
    try std.testing.expectEqual(@as(u32, 5), parsed.value.custom.value);
}

test "simd typed parser captures composite subtrees for custom jsonParse" {
    const alloc = std.testing.allocator;
    const Custom = struct {
        total: usize,

        pub fn jsonParse(allocator: Allocator, source: anytype, options: json.ParseOptions) !@This() {
            const Inner = struct {
                items: []const struct {
                    id: u32,
                },
            };
            const parsed = try std.json.innerParse(Inner, allocator, source, options);
            return .{ .total = parsed.items.len };
        }
    };

    const T = struct {
        plain: u32,
        custom: Custom,
    };

    const raw =
        \\{"plain":1,"custom":{"items":[{"id":1},{"id":2},{"id":3}]}}
    ;

    var structural = try simd_stage1.buildStructuralIndexAlloc(alloc, raw);
    defer structural.deinit(alloc);

    var parsed = try parseFromSlice(T, alloc, raw, .{}, structural);
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u32, 1), parsed.value.plain);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.custom.total);
}

test "simd typed parser borrows plain const string slices when alloc_if_needed" {
    const T = struct {
        msg: []const u8,
    };
    const raw = "{\"msg\":\"hello\"}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(std.testing.allocator, raw);
    defer structural.deinit(std.testing.allocator);

    var buf: [0]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    const parsed = try parseFromSliceLeaky(T, fba.allocator(), raw, .{
        .allocate = .alloc_if_needed,
    }, structural);

    try std.testing.expectEqualStrings("hello", parsed.msg);
    try std.testing.expectEqual(@intFromPtr(raw[8..13].ptr), @intFromPtr(parsed.msg.ptr));
}

test "simd typed parser allocate_always still allocates const string slices" {
    const T = struct {
        msg: []const u8,
    };
    const raw = "{\"msg\":\"hello\"}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(std.testing.allocator, raw);
    defer structural.deinit(std.testing.allocator);

    var buf: [0]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectError(error.OutOfMemory, parseFromSliceLeaky(T, fba.allocator(), raw, .{
        .allocate = .alloc_always,
    }, structural));
}

test "simd typed parser alloc_if_needed still allocates escaped const string slices" {
    const T = struct {
        msg: []const u8,
    };
    const raw = "{\"msg\":\"line\\n\"}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(std.testing.allocator, raw);
    defer structural.deinit(std.testing.allocator);

    var buf: [0]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectError(error.OutOfMemory, parseFromSliceLeaky(T, fba.allocator(), raw, .{
        .allocate = .alloc_if_needed,
    }, structural));
}

test "simd typed parser still borrows plain slices when another field is escaped" {
    const T = struct {
        plain: []const u8,
        escaped: []const u8,
    };
    const raw = "{\"plain\":\"hello\",\"escaped\":\"line\\n\"}";

    var structural = try simd_stage1.buildStructuralIndexAlloc(std.testing.allocator, raw);
    defer structural.deinit(std.testing.allocator);

    var parsed = try parseFromSlice(T, std.testing.allocator, raw, .{
        .allocate = .alloc_if_needed,
    }, structural);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("hello", parsed.value.plain);
    try std.testing.expectEqualStrings("line\n", parsed.value.escaped);
    try std.testing.expectEqual(@intFromPtr(raw[10..15].ptr), @intFromPtr(parsed.value.plain.ptr));
}
