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

pub const ObjRef = struct {
    id: u32,
    gen: u16,
};

pub const DictEntry = struct {
    key: []u8,
    value: Object,

    fn deinit(self: *DictEntry, alloc: Allocator) void {
        alloc.free(self.key);
        self.value.deinit(alloc);
        self.* = undefined;
    }
};

pub const Object = union(enum) {
    null,
    boolean: bool,
    integer: i64,
    real: f64,
    string: []u8,
    name: []u8,
    array: []Object,
    dict: []DictEntry,
    stream: struct {
        header: []DictEntry,
        data_offset: usize,
        data_length: usize,
    },
    obj_ref: ObjRef,
    obj_def: struct {
        ptr: ObjRef,
        value: *Object,
    },

    pub fn clone(self: Object, alloc: Allocator) !Object {
        return switch (self) {
            .null => .null,
            .boolean => |value| .{ .boolean = value },
            .integer => |value| .{ .integer = value },
            .real => |value| .{ .real = value },
            .string => |value| .{ .string = try alloc.dupe(u8, value) },
            .name => |value| .{ .name = try alloc.dupe(u8, value) },
            .array => |items| blk: {
                const out = try alloc.alloc(Object, items.len);
                var initialized: usize = 0;
                errdefer {
                    for (out[0..initialized]) |*item| item.deinit(alloc);
                    alloc.free(out);
                }
                for (items, 0..) |item, i| {
                    out[i] = .null;
                    initialized += 1;
                    out[i] = try item.clone(alloc);
                }
                break :blk .{ .array = out };
            },
            .dict => |entries| blk: {
                const out = try alloc.alloc(DictEntry, entries.len);
                var initialized: usize = 0;
                errdefer {
                    for (out[0..initialized]) |*entry| entry.deinit(alloc);
                    alloc.free(out);
                }
                for (entries, 0..) |entry, i| {
                    out[i] = .{
                        .key = try alloc.dupe(u8, entry.key),
                        .value = .null,
                    };
                    initialized += 1;
                    out[i].value = try entry.value.clone(alloc);
                }
                break :blk .{ .dict = out };
            },
            .stream => |stream_value| blk: {
                const out = try alloc.alloc(DictEntry, stream_value.header.len);
                var initialized: usize = 0;
                errdefer {
                    for (out[0..initialized]) |*entry| entry.deinit(alloc);
                    alloc.free(out);
                }
                for (stream_value.header, 0..) |entry, i| {
                    out[i] = .{
                        .key = try alloc.dupe(u8, entry.key),
                        .value = .null,
                    };
                    initialized += 1;
                    out[i].value = try entry.value.clone(alloc);
                }
                break :blk .{ .stream = .{
                    .header = out,
                    .data_offset = stream_value.data_offset,
                    .data_length = stream_value.data_length,
                } };
            },
            .obj_ref => |value| .{ .obj_ref = value },
            .obj_def => |def| blk: {
                const boxed = try alloc.create(Object);
                errdefer alloc.destroy(boxed);
                boxed.* = try def.value.clone(alloc);
                break :blk .{ .obj_def = .{ .ptr = def.ptr, .value = boxed } };
            },
        };
    }

    pub fn get(self: *const Object, key: []const u8) ?*const Object {
        const entries = switch (self.*) {
            .dict => self.dict,
            .stream => self.stream.header,
            else => return null,
        };
        for (entries) |*entry| {
            if (std.mem.eql(u8, entry.key, key)) return &entry.value;
        }
        return null;
    }

    pub fn asInteger(self: *const Object) ?i64 {
        return switch (self.*) {
            .integer => |value| value,
            else => null,
        };
    }

    pub fn asName(self: *const Object) ?[]const u8 {
        return switch (self.*) {
            .name => |value| value,
            else => null,
        };
    }

    pub fn deinit(self: *Object, alloc: Allocator) void {
        switch (self.*) {
            .string => |value| alloc.free(value),
            .name => |value| alloc.free(value),
            .array => |items| {
                for (items) |*item| item.deinit(alloc);
                alloc.free(items);
            },
            .dict => |entries| {
                for (entries) |*entry| entry.deinit(alloc);
                alloc.free(entries);
            },
            .stream => |stream_value| {
                for (stream_value.header) |*entry| entry.deinit(alloc);
                alloc.free(stream_value.header);
            },
            .obj_def => |def| {
                def.value.deinit(alloc);
                alloc.destroy(def.value);
            },
            else => {},
        }
        self.* = undefined;
    }
};

const Token = union(enum) {
    eof,
    boolean: bool,
    integer: i64,
    real: f64,
    string: []u8,
    name: []u8,
    keyword: []u8,

    fn deinit(self: *Token, alloc: Allocator) void {
        switch (self.*) {
            .string => |value| alloc.free(value),
            .name => |value| alloc.free(value),
            .keyword => |value| alloc.free(value),
            else => {},
        }
        self.* = undefined;
    }
};

pub const Lexeme = Token;

pub const Scanner = struct {
    alloc: Allocator,
    bytes: []const u8,
    base_offset: usize = 0,
    pos: usize = 0,
    unread: std.ArrayList(Token) = .empty,

    pub fn init(alloc: Allocator, bytes: []const u8) Scanner {
        return initWithOffset(alloc, bytes, 0);
    }

    pub fn initWithOffset(alloc: Allocator, bytes: []const u8, base_offset: usize) Scanner {
        return .{
            .alloc = alloc,
            .bytes = bytes,
            .base_offset = base_offset,
        };
    }

    pub fn deinit(self: *Scanner) void {
        for (self.unread.items) |*tok| tok.deinit(self.alloc);
        self.unread.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn readObject(self: *Scanner) anyerror!Object {
        var tok = try self.readToken();
        defer tok.deinit(self.alloc);

        switch (tok) {
            .eof => return error.UnexpectedEof,
            .boolean => |value| return .{ .boolean = value },
            .integer => |value| {
                if (value >= 0 and value <= std.math.maxInt(u32)) {
                    var tok2 = try self.readToken();
                    defer tok2.deinit(self.alloc);
                    if (tok2 == .integer and tok2.integer >= 0 and tok2.integer <= std.math.maxInt(u16)) {
                        var tok3 = try self.readToken();
                        defer tok3.deinit(self.alloc);
                        if (tok3 == .keyword and std.mem.eql(u8, tok3.keyword, "R")) {
                            return .{ .obj_ref = .{
                                .id = @intCast(value),
                                .gen = @intCast(tok2.integer),
                            } };
                        }
                        if (tok3 == .keyword and std.mem.eql(u8, tok3.keyword, "obj")) {
                            const boxed = try self.alloc.create(Object);
                            var boxed_initialized = false;
                            errdefer {
                                if (boxed_initialized) boxed.deinit(self.alloc);
                                self.alloc.destroy(boxed);
                            }
                            boxed.* = try self.readObject();
                            boxed_initialized = true;

                            var end_tok = try self.readToken();
                            defer end_tok.deinit(self.alloc);
                            if (end_tok != .keyword or !std.mem.eql(u8, end_tok.keyword, "endobj")) {
                                return error.MissingEndObj;
                            }

                            return .{ .obj_def = .{
                                .ptr = .{
                                    .id = @intCast(value),
                                    .gen = @intCast(tok2.integer),
                                },
                                .value = boxed,
                            } };
                        }
                        try self.unreadToken(try cloneToken(self.alloc, tok3));
                    }
                    try self.unreadToken(try cloneToken(self.alloc, tok2));
                }
                return .{ .integer = value };
            },
            .real => |value| return .{ .real = value },
            .string => |value| return .{ .string = try self.alloc.dupe(u8, value) },
            .name => |value| return .{ .name = try self.alloc.dupe(u8, value) },
            .keyword => |kw| {
                if (std.mem.eql(u8, kw, "null")) return .null;
                if (std.mem.eql(u8, kw, "[")) return try self.readArray();
                if (std.mem.eql(u8, kw, "<<")) return try self.readDictOrStream();
                return error.UnexpectedKeyword;
            },
        }
    }

    pub fn readLexeme(self: *Scanner) anyerror!Lexeme {
        return try self.readToken();
    }

    pub fn unreadLexeme(self: *Scanner, tok: Lexeme) anyerror!void {
        try self.unreadToken(tok);
    }

    pub fn freeLexeme(alloc: Allocator, tok: *Lexeme) void {
        tok.deinit(alloc);
    }

    fn readArray(self: *Scanner) anyerror!Object {
        var items = std.ArrayList(Object).empty;
        defer items.deinit(self.alloc);
        errdefer {
            for (items.items) |*item| item.deinit(self.alloc);
        }

        while (true) {
            var tok = try self.readToken();
            defer tok.deinit(self.alloc);
            if (tok == .eof) return error.UnexpectedEof;
            if (tok == .keyword and std.mem.eql(u8, tok.keyword, "]")) break;
            try self.unreadToken(try cloneToken(self.alloc, tok));
            var item = try self.readObject();
            errdefer item.deinit(self.alloc);
            try items.append(self.alloc, item);
            item = .null;
        }

        const owned = try items.toOwnedSlice(self.alloc);
        items = .empty;
        return .{ .array = owned };
    }

    fn readDictOrStream(self: *Scanner) anyerror!Object {
        var entries = std.ArrayList(DictEntry).empty;
        defer {
            for (entries.items) |*entry| entry.deinit(self.alloc);
            entries.deinit(self.alloc);
        }

        while (true) {
            var tok = try self.readToken();
            defer tok.deinit(self.alloc);
            if (tok == .eof) return error.UnexpectedEof;
            if (tok == .keyword and std.mem.eql(u8, tok.keyword, ">>")) break;
            if (tok != .name) return error.UnexpectedDictKey;

            const key = try self.alloc.dupe(u8, tok.name);
            errdefer self.alloc.free(key);

            const value = try self.readObject();
            errdefer {
                var mutable = value;
                mutable.deinit(self.alloc);
            }

            try entries.append(self.alloc, .{ .key = key, .value = value });
        }

        const owned = try entries.toOwnedSlice(self.alloc);
        entries = .empty;
        errdefer {
            for (owned) |*entry| entry.deinit(self.alloc);
            self.alloc.free(owned);
        }

        var tok = try self.readToken();
        defer tok.deinit(self.alloc);
        if (tok != .keyword or !std.mem.eql(u8, tok.keyword, "stream")) {
            try self.unreadToken(try cloneToken(self.alloc, tok));
            return .{ .dict = owned };
        }

        switch (self.readByte() orelse return error.UnexpectedEof) {
            '\r' => {
                if (self.peekByte()) |next| {
                    if (next == '\n') _ = self.readByte();
                }
            },
            '\n' => {},
            else => return error.StreamMissingNewline,
        }

        const data_start = self.pos;
        const data_offset = self.base_offset + data_start;
        const actual_length = if (streamLength(owned)) |direct_length| blk: {
            if (self.pos + direct_length > self.bytes.len) {
                return error.UnexpectedEof;
            }
            self.pos += direct_length;
            break :blk direct_length;
        } else blk: {
            const data_end = findEndStreamDataEnd(self.bytes, self.pos) orelse {
                return error.MissingEndStream;
            };
            self.pos = data_end;
            break :blk data_end - data_start;
        };

        var end_tok = try self.readToken();
        defer end_tok.deinit(self.alloc);
        if (end_tok != .keyword or !std.mem.eql(u8, end_tok.keyword, "endstream")) {
            return error.MissingEndStream;
        }

        return .{ .stream = .{
            .header = owned,
            .data_offset = data_offset,
            .data_length = actual_length,
        } };
    }

    fn readToken(self: *Scanner) anyerror!Token {
        if (self.unread.items.len > 0) {
            return self.unread.pop().?;
        }

        self.skipSpaceAndComments();
        if (self.pos >= self.bytes.len) return .eof;

        const c = self.readByte() orelse return .eof;
        switch (c) {
            '<' => {
                if (self.peekByte()) |next| {
                    if (next == '<') {
                        _ = self.readByte();
                        return .{ .keyword = try self.alloc.dupe(u8, "<<") };
                    }
                }
                if (!self.nextStartsHexString()) return .{ .keyword = try self.alloc.dupe(u8, "<") };
                return .{ .string = try self.readHexString() };
            },
            '(' => return .{ .string = try self.readLiteralString() },
            '[', ']', '{', '}', ')' => {
                var buf: [1]u8 = .{c};
                return .{ .keyword = try self.alloc.dupe(u8, &buf) };
            },
            '/' => return .{ .name = try self.readName() },
            '>' => {
                if (self.peekByte()) |next| {
                    if (next == '>') {
                        _ = self.readByte();
                        return .{ .keyword = try self.alloc.dupe(u8, ">>") };
                    }
                }
                return .{ .keyword = try self.alloc.dupe(u8, ">") };
            },
            else => {
                if (isDelim(c)) return error.UnexpectedDelimiter;
                self.unreadByte();
                return try self.readKeywordToken();
            },
        }
    }

    fn unreadToken(self: *Scanner, tok: Token) anyerror!void {
        var owned = tok;
        errdefer owned.deinit(self.alloc);
        try self.unread.append(self.alloc, owned);
    }

    fn skipSpaceAndComments(self: *Scanner) void {
        while (self.pos < self.bytes.len) {
            const c = self.bytes[self.pos];
            if (isSpace(c)) {
                self.pos += 1;
                continue;
            }
            if (c == '%') {
                while (self.pos < self.bytes.len and self.bytes[self.pos] != '\r' and self.bytes[self.pos] != '\n') : (self.pos += 1) {}
                continue;
            }
            break;
        }
    }

    fn nextStartsHexString(self: *const Scanner) bool {
        var i = self.pos;
        while (i < self.bytes.len and isSpace(self.bytes[i])) : (i += 1) {}
        if (i >= self.bytes.len) return false;
        return self.bytes[i] == '>' or unhex(self.bytes[i]) != null;
    }

    fn readHexString(self: *Scanner) anyerror![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        while (true) {
            const c = self.readSkippingSpace() orelse break;
            if (c == '>') break;
            const hi = unhex(c) orelse break;
            const c2 = self.readSkippingSpace() orelse {
                try out.append(self.alloc, hi << 4);
                break;
            };
            if (c2 == '>') {
                try out.append(self.alloc, hi << 4);
                break;
            }
            const lo = unhex(c2) orelse {
                try out.append(self.alloc, hi << 4);
                break;
            };
            try out.append(self.alloc, (hi << 4) | lo);
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn readLiteralString(self: *Scanner) anyerror![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        var depth: usize = 1;
        while (self.pos < self.bytes.len) {
            const c = self.readByte().?;
            switch (c) {
                '(' => {
                    depth += 1;
                    try out.append(self.alloc, c);
                },
                ')' => {
                    depth -= 1;
                    if (depth == 0) break;
                    try out.append(self.alloc, c);
                },
                '\\' => {
                    const escaped = self.readByte() orelse return error.UnexpectedEof;
                    switch (escaped) {
                        'n' => try out.append(self.alloc, '\n'),
                        'r' => try out.append(self.alloc, '\r'),
                        'b' => try out.append(self.alloc, '\x08'),
                        't' => try out.append(self.alloc, '\t'),
                        'f' => try out.append(self.alloc, '\x0c'),
                        '(', ')', '\\' => try out.append(self.alloc, escaped),
                        '\r' => {
                            if (self.peekByte()) |next| {
                                if (next == '\n') _ = self.readByte();
                            }
                        },
                        '\n' => {},
                        '0', '1', '2', '3', '4', '5', '6', '7' => {
                            var x: u16 = escaped - '0';
                            var i: usize = 0;
                            while (i < 2) : (i += 1) {
                                const next = self.peekByte() orelse break;
                                if (next < '0' or next > '7') break;
                                _ = self.readByte();
                                x = x * 8 + (next - '0');
                            }
                            if (x > 255) return error.InvalidOctalEscape;
                            try out.append(self.alloc, @intCast(x));
                        },
                        else => try out.append(self.alloc, escaped),
                    }
                },
                else => try out.append(self.alloc, c),
            }
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn readName(self: *Scanner) anyerror![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        while (self.pos < self.bytes.len) {
            const c = self.readByte().?;
            if (isDelim(c) or isSpace(c)) {
                self.unreadByte();
                break;
            }
            if (c == '#') {
                const hi_c = self.readByte() orelse {
                    try out.append(self.alloc, '#');
                    break;
                };
                const lo_c = self.readByte() orelse {
                    try out.append(self.alloc, '#');
                    try out.append(self.alloc, hi_c);
                    break;
                };
                const hi = unhex(hi_c) orelse {
                    try out.append(self.alloc, '#');
                    try out.append(self.alloc, hi_c);
                    try out.append(self.alloc, lo_c);
                    continue;
                };
                const lo = unhex(lo_c) orelse {
                    try out.append(self.alloc, '#');
                    try out.append(self.alloc, hi_c);
                    try out.append(self.alloc, lo_c);
                    continue;
                };
                try out.append(self.alloc, (hi << 4) | lo);
                continue;
            }
            try out.append(self.alloc, c);
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn readKeywordToken(self: *Scanner) anyerror!Token {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        while (self.pos < self.bytes.len) {
            const c = self.readByte().?;
            if (isDelim(c) or isSpace(c)) {
                self.unreadByte();
                break;
            }
            try out.append(self.alloc, c);
        }

        const text = try out.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(text);

        if (std.mem.eql(u8, text, "true")) {
            self.alloc.free(text);
            return .{ .boolean = true };
        }
        if (std.mem.eql(u8, text, "false")) {
            self.alloc.free(text);
            return .{ .boolean = false };
        }
        if (isInteger(text)) {
            const value = try std.fmt.parseInt(i64, text, 10);
            self.alloc.free(text);
            return .{ .integer = value };
        }
        if (isReal(text)) {
            const value = try std.fmt.parseFloat(f64, text);
            self.alloc.free(text);
            return .{ .real = value };
        }
        return .{ .keyword = text };
    }

    fn readSkippingSpace(self: *Scanner) ?u8 {
        while (self.pos < self.bytes.len) {
            const c = self.readByte().?;
            if (!isSpace(c)) return c;
        }
        return null;
    }

    fn readByte(self: *Scanner) ?u8 {
        if (self.pos >= self.bytes.len) return null;
        const c = self.bytes[self.pos];
        self.pos += 1;
        return c;
    }

    fn peekByte(self: *Scanner) ?u8 {
        if (self.pos >= self.bytes.len) return null;
        return self.bytes[self.pos];
    }

    fn unreadByte(self: *Scanner) void {
        if (self.pos > 0) self.pos -= 1;
    }
};

fn cloneToken(alloc: Allocator, tok: Token) !Token {
    return switch (tok) {
        .eof => .eof,
        .boolean => |value| .{ .boolean = value },
        .integer => |value| .{ .integer = value },
        .real => |value| .{ .real = value },
        .string => |value| .{ .string = try alloc.dupe(u8, value) },
        .name => |value| .{ .name = try alloc.dupe(u8, value) },
        .keyword => |value| .{ .keyword = try alloc.dupe(u8, value) },
    };
}

fn streamLength(entries: []const DictEntry) ?usize {
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.key, "Length")) {
            const len_i = entry.value.asInteger() orelse return null;
            if (len_i < 0) return null;
            return @intCast(len_i);
        }
    }
    return null;
}

fn findEndStreamDataEnd(bytes: []const u8, start: usize) ?usize {
    var search = start;
    while (search < bytes.len) {
        const maybe_idx = std.mem.indexOf(u8, bytes[search..], "endstream") orelse return null;
        const kw_pos = search + maybe_idx;
        if (kw_pos > start and !isSpace(bytes[kw_pos - 1])) {
            search = kw_pos + "endstream".len;
            continue;
        }
        if (kw_pos + "endstream".len < bytes.len) {
            const after = bytes[kw_pos + "endstream".len];
            if (!isSpace(after) and !isDelim(after)) {
                search = kw_pos + "endstream".len;
                continue;
            }
        }

        var data_end = kw_pos;
        if (data_end > start and bytes[data_end - 1] == '\n') {
            data_end -= 1;
            if (data_end > start and bytes[data_end - 1] == '\r') data_end -= 1;
        } else if (data_end > start and bytes[data_end - 1] == '\r') {
            data_end -= 1;
        }
        return data_end;
    }
    return null;
}

fn isSpace(b: u8) bool {
    return switch (b) {
        '\x00', '\t', '\n', '\x0c', '\r', ' ' => true,
        else => false,
    };
}

fn isDelim(b: u8) bool {
    return switch (b) {
        '<', '>', '(', ')', '[', ']', '{', '}', '/', '%' => true,
        else => false,
    };
}

fn unhex(b: u8) ?u8 {
    return switch (b) {
        '0'...'9' => b - '0',
        'a'...'f' => b - 'a' + 10,
        'A'...'F' => b - 'A' + 10,
        else => null,
    };
}

fn isInteger(s: []const u8) bool {
    var slice = s;
    if (slice.len > 0 and (slice[0] == '+' or slice[0] == '-')) slice = slice[1..];
    if (slice.len == 0) return false;
    for (slice) |c| if (!std.ascii.isDigit(c)) return false;
    return true;
}

fn isReal(s: []const u8) bool {
    var slice = s;
    if (slice.len > 0 and (slice[0] == '+' or slice[0] == '-')) slice = slice[1..];
    if (slice.len == 0) return false;
    var dots: usize = 0;
    var digits: usize = 0;
    for (slice) |c| {
        if (c == '.') {
            dots += 1;
            continue;
        }
        if (!std.ascii.isDigit(c)) return false;
        digits += 1;
    }
    return dots == 1 and digits > 0;
}

test "scanner treats a bare decimal point as a keyword" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, ".");
    defer scanner.deinit();

    var token = try scanner.readToken();
    defer token.deinit(alloc);
    try std.testing.expect(token == .keyword);
    try std.testing.expectEqualStrings(".", token.keyword);
}

test "scanner returns unterminated literal string prefix" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "(abc");
    defer scanner.deinit();

    var tok = try scanner.readLexeme();
    defer tok.deinit(alloc);
    try std.testing.expect(tok == .string);
    try std.testing.expectEqualStrings("abc", tok.string);
}

test "scanner preserves unknown literal string escapes" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "(a\\qb)");
    defer scanner.deinit();

    var tok = try scanner.readLexeme();
    defer tok.deinit(alloc);
    try std.testing.expect(tok == .string);
    try std.testing.expectEqualStrings("aqb", tok.string);
}

test "scanner keeps malformed name escapes literal" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "/A#6$B /C#");
    defer scanner.deinit();

    {
        var tok = try scanner.readLexeme();
        defer tok.deinit(alloc);
        try std.testing.expect(tok == .name);
        try std.testing.expectEqualStrings("A#6$B", tok.name);
    }

    {
        var tok = try scanner.readLexeme();
        defer tok.deinit(alloc);
        try std.testing.expect(tok == .name);
        try std.testing.expectEqualStrings("C#", tok.name);
    }
}

test "scanner treats non-hex less-than as keyword" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "<K");
    defer scanner.deinit();

    var tok = try scanner.readLexeme();
    defer tok.deinit(alloc);
    try std.testing.expect(tok == .keyword);
    try std.testing.expectEqualStrings("<", tok.keyword);
}

test "scanner returns unterminated hex string prefix" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "<AB");
    defer scanner.deinit();

    var tok = try scanner.readLexeme();
    defer tok.deinit(alloc);
    try std.testing.expect(tok == .string);
    try std.testing.expectEqualSlices(u8, &.{0xAB}, tok.string);
}

test "scanner terminates malformed hex strings with decoded prefix" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "<ABZ>");
    defer scanner.deinit();

    var tok = try scanner.readLexeme();
    defer tok.deinit(alloc);
    try std.testing.expect(tok == .string);
    try std.testing.expectEqualSlices(u8, &.{0xAB}, tok.string);
}

test "scanner accepts odd-length hex strings" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "<ABC>");
    defer scanner.deinit();

    var tok = try scanner.readLexeme();
    defer tok.deinit(alloc);
    try std.testing.expect(tok == .string);
    try std.testing.expectEqualSlices(u8, &.{ 0xAB, 0xC0 }, tok.string);
}

test "scanner tokenizes standalone closing delimiters" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, ") > >>");
    defer scanner.deinit();

    {
        var tok = try scanner.readLexeme();
        defer tok.deinit(alloc);
        try std.testing.expect(tok == .keyword);
        try std.testing.expectEqualStrings(")", tok.keyword);
    }

    {
        var tok = try scanner.readLexeme();
        defer tok.deinit(alloc);
        try std.testing.expect(tok == .keyword);
        try std.testing.expectEqualStrings(">", tok.keyword);
    }

    {
        var tok = try scanner.readLexeme();
        defer tok.deinit(alloc);
        try std.testing.expect(tok == .keyword);
        try std.testing.expectEqualStrings(">>", tok.keyword);
    }
}

test "scanner cleans partial array objects on parse error" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "[(abc");
    defer scanner.deinit();

    try std.testing.expectError(error.UnexpectedEof, scanner.readObject());
}

test "scanner tokenizes strings names and numbers" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "/Name#20Here (hi\\nthere) 42 -3.5");
    defer scanner.deinit();

    {
        var tok = try scanner.readToken();
        defer tok.deinit(alloc);
        try std.testing.expect(tok == .name);
        try std.testing.expectEqualStrings("Name Here", tok.name);
    }

    {
        var tok = try scanner.readToken();
        defer tok.deinit(alloc);
        try std.testing.expect(tok == .string);
        try std.testing.expectEqualStrings("hi\nthere", tok.string);
    }

    {
        var tok = try scanner.readToken();
        defer tok.deinit(alloc);
        try std.testing.expectEqual(@as(i64, 42), tok.integer);
    }

    {
        var tok = try scanner.readToken();
        defer tok.deinit(alloc);
        try std.testing.expectEqual(@as(f64, -3.5), tok.real);
    }
}

test "scanner parses dict array object ref and objdef" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "<< /Type /Example /Nums [1 2 3] /Ref 9 0 R >> 5 0 obj (hi) endobj");
    defer scanner.deinit();

    var dict = try scanner.readObject();
    defer dict.deinit(alloc);
    try std.testing.expect(dict == .dict);
    try std.testing.expectEqual(@as(usize, 3), dict.dict.len);
    try std.testing.expectEqualStrings("Type", dict.dict[0].key);
    try std.testing.expect(dict.dict[2].value == .obj_ref);
    try std.testing.expectEqual(@as(u32, 9), dict.dict[2].value.obj_ref.id);

    var def = try scanner.readObject();
    defer def.deinit(alloc);
    try std.testing.expect(def == .obj_def);
    try std.testing.expectEqual(@as(u32, 5), def.obj_def.ptr.id);
    try std.testing.expect(def.obj_def.value.* == .string);
    try std.testing.expectEqualStrings("hi", def.obj_def.value.string);
}

test "object clone duplicates nested data" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(alloc, "<< /Type /Example /Nums [1 2] >>");
    defer scanner.deinit();

    var obj = try scanner.readObject();
    defer obj.deinit(alloc);
    var cloned = try obj.clone(alloc);
    defer cloned.deinit(alloc);

    try std.testing.expect(cloned == .dict);
    try std.testing.expectEqualStrings("Type", cloned.dict[0].key);
    try std.testing.expect(cloned.get("Nums").?.* == .array);
}

test "object clone cleans up only initialized values on allocation failure" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var array_items = [_]Object{
                .{ .string = @constCast("first"[0..]) },
                .{ .name = @constCast("second"[0..]) },
            };
            var nested_entries = [_]DictEntry{
                .{
                    .key = @constCast("Items"[0..]),
                    .value = .{ .array = array_items[0..] },
                },
            };
            var stream_entries = [_]DictEntry{
                .{
                    .key = @constCast("Nested"[0..]),
                    .value = .{ .dict = nested_entries[0..] },
                },
            };
            const source = Object{ .stream = .{
                .header = stream_entries[0..],
                .data_offset = 10,
                .data_length = 20,
            } };

            var cloned = try source.clone(alloc);
            defer cloned.deinit(alloc);
            try std.testing.expect(cloned == .stream);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "scanner parses stream object with inline length" {
    const alloc = std.testing.allocator;
    const sample = "7 0 obj\n<< /Length 5 >>\nstream\nhello\nendstream\nendobj";
    const expected_offset = 100 + std.mem.indexOf(u8, sample, "hello").?;
    var scanner = Scanner.initWithOffset(alloc, sample, 100);
    defer scanner.deinit();

    var def = try scanner.readObject();
    defer def.deinit(alloc);
    try std.testing.expect(def == .obj_def);
    try std.testing.expect(def.obj_def.value.* == .stream);
    try std.testing.expectEqual(@as(usize, 5), def.obj_def.value.stream.data_length);
    try std.testing.expectEqual(expected_offset, def.obj_def.value.stream.data_offset);
}

test "scanner parses stream object with indirect length" {
    const alloc = std.testing.allocator;
    const sample =
        "7 0 obj\n" ++
        "<< /Length 8 0 R >>\n" ++
        "stream\n" ++
        "hello\n" ++
        "endstream\n" ++
        "endobj";
    const expected_offset = 200 + std.mem.indexOf(u8, sample, "hello").?;
    var scanner = Scanner.initWithOffset(alloc, sample, 200);
    defer scanner.deinit();

    var def = try scanner.readObject();
    defer def.deinit(alloc);
    try std.testing.expect(def == .obj_def);
    try std.testing.expect(def.obj_def.value.* == .stream);
    try std.testing.expectEqual(@as(usize, 5), def.obj_def.value.stream.data_length);
    try std.testing.expectEqual(expected_offset, def.obj_def.value.stream.data_offset);
}

test "scanner cleans stream dictionaries on malformed stream endings" {
    const cases = [_]struct {
        input: []const u8,
        expected: anyerror,
    }{
        .{
            .input = "<< /Length 100 >> stream\nx",
            .expected = error.UnexpectedEof,
        },
        .{
            .input = "<< /Type /Example >> stream\nunterminated",
            .expected = error.MissingEndStream,
        },
        .{
            .input = "<< /Length 3 >> stream\nabc nope",
            .expected = error.MissingEndStream,
        },
    };

    for (cases) |case| {
        var scanner = Scanner.init(std.testing.allocator, case.input);
        defer scanner.deinit();
        try std.testing.expectError(case.expected, scanner.readObject());
    }
}
