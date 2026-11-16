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

const std = @import("std");

const Allocator = std.mem.Allocator;

pub const ObjRef = struct {
    id: u32,
    gen: u16,
};

// Source offsets survive object-stream eviction; no payload bytes are retained.
pub const DeferredValue = struct {
    reference: ObjRef,
    start: usize,
    end: usize,
};

pub const ScanControl = struct {
    remaining_work: usize = std.math.maxInt(usize),
    context: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (?*const anyopaque) bool = null,
    until_poll: usize = 0,

    pub fn spend(self: *@This(), work: usize) !void {
        if (work >= self.until_poll or work > self.remaining_work) {
            self.until_poll = 4096;
            if (self.is_cancelled_fn) |check| if (check(self.context)) return error.Canceled;
        } else self.until_poll -= work;
        if (work > self.remaining_work) {
            self.remaining_work = 0;
            return error.PdfMetadataWorkLimit;
        }
        self.remaining_work -= work;
    }
};

pub const DictEntry = struct {
    key: []u8,
    value: Object,

    pub fn deinit(self: *DictEntry, alloc: Allocator) void {
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
    stream_header: []DictEntry,
    stream: struct {
        header: []DictEntry,
        data_offset: usize,
        data_length: usize,
    },
    obj_ref: ObjRef,
    deferred: DeferredValue,
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
            .dict, .stream_header => |entries| blk: {
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
                break :blk if (self == .stream_header) .{ .stream_header = out } else .{ .dict = out };
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
            .deferred => |value| .{ .deferred = value },
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
            .stream_header => self.stream_header,
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
            .dict, .stream_header => |entries| {
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

    pub fn deinit(self: *Token, alloc: Allocator) void {
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

// Metadata readers keep only the fields needed for their selected purpose.
// Full parsing remains the default for rendering and general object consumers.
pub const Projection = enum {
    full,
    text_xobject,
    xobject_header,
    color_profile_header,
    appearance_header,
    structure_metadata,
    resource_table,
    resource_entries,
    resource_fields,
    name,
    resources,
    required_resources,
    resource_container,
    container_properties,
    resource_properties,
    property_table,
    property,
    integer,
    text,

    const Field = struct { keep: bool, skip: bool = false, view: bool = false, child: Projection = .full };

    fn field(self: Projection, key: []const u8) Field {
        return switch (self) {
            .full => .{ .keep = true },
            .structure_metadata => blk: {
                // Structure nodes, number trees, and content-container headers
                // share this resolver. Payload-related dictionaries are never
                // needed, even when a referenced container is a PDF Form.
                for ([_][]const u8{ "Type", "ParentTree", "K", "Kids", "Limits", "Nums", "P", "Pg", "S", "Stm", "MCID", "ActualText", "StructParents" }) |name| {
                    if (std.mem.eql(u8, key, name)) break :blk .{ .keep = true, .child = .structure_metadata };
                }
                break :blk .{ .keep = false };
            },
            // Normal appearances can also be state dictionaries. Preserve their
            // names/references while leaving unrelated compound fields as views.
            .appearance_header => if (std.mem.eql(u8, key, "Subtype") or std.mem.eql(u8, key, "BBox") or std.mem.eql(u8, key, "Matrix")) .{ .keep = true } else .{ .keep = true, .view = true },
            .resource_table, .resource_entries => .{ .keep = true, .view = true },
            .text_xobject => blk: {
                if (std.mem.eql(u8, key, "Subtype") or std.mem.eql(u8, key, "Subtype2") or std.mem.eql(u8, key, "Type")) break :blk .{ .keep = true, .child = .name };
                if (std.mem.eql(u8, key, "StructParent")) break :blk .{ .keep = true, .skip = true };
                if (std.mem.eql(u8, key, "Length") or std.mem.eql(u8, key, "StructParents")) break :blk .{ .keep = true, .child = .integer };
                if (std.mem.eql(u8, key, "Resources")) break :blk .{ .keep = true, .child = .resource_fields };
                for ([_][]const u8{ "BBox", "Matrix", "Group", "Filter", "DecodeParms" }) |name| {
                    if (std.mem.eql(u8, key, name)) break :blk .{ .keep = true };
                }
                break :blk .{ .keep = false };
            },
            .xobject_header => if (std.mem.eql(u8, key, "Subtype") or std.mem.eql(u8, key, "Subtype2")) .{ .keep = true, .child = .name } else if (std.mem.eql(u8, key, "StructParent")) .{ .keep = true, .skip = true } else .{ .keep = false },
            .color_profile_header => if (std.mem.eql(u8, key, "N")) .{ .keep = true, .child = .integer } else .{ .keep = std.mem.eql(u8, key, "Alternate") or std.mem.eql(u8, key, "Range") },
            .resource_fields => blk: {
                for ([_][]const u8{ "Font", "XObject", "ExtGState", "ColorSpace", "Properties" }) |name| {
                    if (std.mem.eql(u8, key, name)) break :blk .{ .keep = true, .view = std.mem.eql(u8, key, "Properties"), .child = .resource_table };
                }
                break :blk .{ .keep = false };
            },
            .resources, .required_resources => blk: {
                if (std.mem.eql(u8, key, "Properties")) break :blk .{ .keep = true, .skip = self == .required_resources, .child = .property_table };
                for ([_][]const u8{ "Font", "XObject", "ExtGState", "ColorSpace" }) |name| {
                    if (std.mem.eql(u8, key, name)) break :blk .{ .keep = true, .child = .resource_table };
                }
                break :blk .{ .keep = false };
            },
            .resource_container => .{ .keep = std.mem.eql(u8, key, "Parent") or std.mem.eql(u8, key, "Resources"), .child = if (std.mem.eql(u8, key, "Resources")) .required_resources else .full },
            .container_properties => .{ .keep = std.mem.eql(u8, key, "Resources"), .child = .resource_properties },
            .resource_properties => .{ .keep = std.mem.eql(u8, key, "Properties"), .child = .property_table },
            .property_table => .{ .keep = true, .child = .property },
            .property => if (std.mem.eql(u8, key, "ActualText")) .{ .keep = true, .child = .text } else if (std.mem.eql(u8, key, "MCID")) .{ .keep = true, .child = .integer } else .{ .keep = false },
            .integer, .text, .name => .{ .keep = false },
        };
    }
};

pub const Scanner = struct {
    alloc: Allocator,
    bytes: []const u8,
    base_offset: usize = 0,
    pos: usize = 0,
    unread: std.ArrayList(Token) = .empty,
    max_object_depth: usize = std.math.maxInt(usize),
    object_depth: usize = 0,
    source_ref: ?ObjRef = null,
    resource_names: ?*const std.StringHashMapUnmanaged(void) = null,
    allow_stream: bool = true,
    control: ?*ScanControl = null,
    last_checked_pos: usize = 0,
    stream_length_resolver: ?struct {
        context: *const anyopaque,
        resolve: *const fn (*const anyopaque, ObjRef, bool) anyerror!?usize,
    } = null,

    fn checkpoint(self: *Scanner) !void {
        try self.checkpointUsing(self.control);
    }

    fn checkpointUsing(self: *Scanner, active: ?*ScanControl) !void {
        if (active) |control| {
            const work = (self.pos -| self.last_checked_pos) +| 1;
            self.last_checked_pos = self.pos;
            try control.spend(work);
        }
    }

    fn findStringMarker(self: *Scanner) !?usize {
        const bytes = self.bytes[self.pos..];
        if (self.control) |control| {
            var offset: usize = 0;
            while (offset < bytes.len) {
                const end = @min(bytes.len, offset + 4096);
                if (std.mem.indexOfAny(u8, bytes[offset..end], "()\\")) |found| {
                    try control.spend(found + 1);
                    return offset + found;
                }
                try control.spend(end - offset);
                offset = end;
            }
            return null;
        }
        return std.mem.indexOfAny(u8, bytes, "()\\");
    }

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
        return self.readObjectProjected(.full);
    }

    pub fn readObjectProjected(self: *Scanner, projection: Projection) anyerror!Object {
        if (projection != .full and self.unread.items.len == 0) {
            try self.skipSpaceAndComments();
            if (self.peekByte()) |first| {
                const compound = first == '[' or (first == '<' and self.pos + 1 < self.bytes.len and self.bytes[self.pos + 1] == '<');
                const string = first == '(' or (first == '<' and !compound);
                const reject = switch (projection) {
                    .full, .structure_metadata => false,
                    .integer => compound or string or first == '/',
                    .text => compound or first == '/',
                    .name => compound or string,
                    else => first == '[' or string or first == '/',
                };
                if (reject) {
                    try self.skipObject(self.object_depth);
                    return .{ .boolean = false };
                }
            }
        }
        if (self.object_depth >= self.max_object_depth) return error.PdfObjectNestingTooDeep;
        self.object_depth += 1;
        defer self.object_depth -= 1;
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
                            const prior_source_ref = self.source_ref;
                            if ((projection == .text_xobject or projection == .resource_table or projection == .resource_entries or projection == .resource_fields) and self.source_ref == null)
                                self.source_ref = .{ .id = @intCast(value), .gen = @intCast(tok2.integer) };
                            defer self.source_ref = prior_source_ref;
                            boxed.* = try self.readObjectProjected(projection);
                            boxed_initialized = true;
                            if (boxed.* == .stream_header) {
                                return .{ .obj_def = .{ .ptr = .{ .id = @intCast(value), .gen = @intCast(tok2.integer) }, .value = boxed } };
                            }

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
                        try self.unreadOwnedToken(&tok3);
                    }
                    try self.unreadOwnedToken(&tok2);
                }
                return .{ .integer = value };
            },
            .real => |value| return .{ .real = value },
            .string => |value| {
                tok = .eof;
                return .{ .string = value };
            },
            .name => |value| {
                tok = .eof;
                return .{ .name = value };
            },
            .keyword => |kw| {
                if (std.mem.eql(u8, kw, "null")) return .null;
                if (std.mem.eql(u8, kw, "[")) return try self.readArray(if (projection == .structure_metadata) .structure_metadata else .full);
                if (std.mem.eql(u8, kw, "<<")) return try self.readDictOrStream(projection);
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

    fn readArray(self: *Scanner, projection: Projection) anyerror!Object {
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
            try self.unreadOwnedToken(&tok);
            var item = try self.readObjectProjected(projection);
            errdefer item.deinit(self.alloc);
            try items.append(self.alloc, item);
            item = .null;
        }

        const owned = try items.toOwnedSlice(self.alloc);
        items = .empty;
        return .{ .array = owned };
    }

    fn nextIsDictionary(self: *Scanner) !bool {
        try self.skipSpaceAndComments();
        return self.pos + 1 < self.bytes.len and std.mem.eql(u8, self.bytes[self.pos..][0..2], "<<");
    }

    // Common tables keep a tiny reusable index. Large tables retain only their
    // source span until a collector knows the selected names. In particular,
    // ignored XObjects and unused tables never allocate an unbounded index.
    fn readResourceTable(self: *Scanner, bounded: bool) !Object {
        const table_start = self.pos - 2;
        var entries = std.ArrayList(DictEntry).empty;
        defer entries.deinit(self.alloc);
        errdefer for (entries.items) |*entry| entry.deinit(self.alloc);
        while (true) {
            try self.skipSpaceAndComments();
            if (self.pos + 1 < self.bytes.len and std.mem.eql(u8, self.bytes[self.pos..][0..2], ">>")) {
                self.pos += 2;
                return .{ .dict = try entries.toOwnedSlice(self.alloc) };
            }
            if (self.peekByte() != '/') return error.UnexpectedDictKey;
            const key_start = self.pos;
            try self.skipObject(self.object_depth);
            const key_end = self.pos;
            if (bounded and (entries.items.len >= 8 or key_end - key_start > 128)) {
                try self.skipObject(self.object_depth);
                while (true) {
                    try self.skipSpaceAndComments();
                    if (self.pos + 1 < self.bytes.len and std.mem.eql(u8, self.bytes[self.pos..][0..2], ">>")) {
                        self.pos += 2;
                        break;
                    }
                    if (self.peekByte() != '/') return error.UnexpectedDictKey;
                    try self.skipObject(self.object_depth);
                    try self.skipObject(self.object_depth);
                }
                for (entries.items) |*entry| entry.deinit(self.alloc);
                entries.clearRetainingCapacity();
                return .{ .deferred = .{ .reference = self.source_ref.?, .start = self.base_offset + table_start, .end = self.base_offset + self.pos } };
            }
            if (!bounded) if (self.resource_names) |names| {
                var failure: ?anyerror = null;
                const selected = names.containsAdapted(self.bytes[key_start + 1 .. key_end], ResourceNameContext{ .control = self.control, .failure = &failure });
                if (failure) |err| return err;
                if (!selected) {
                    try self.skipObject(self.object_depth);
                    continue;
                }
            };
            var key_scanner = Scanner.init(self.alloc, self.bytes[key_start..key_end]);
            key_scanner.control = self.control;
            defer key_scanner.deinit();
            var key = try key_scanner.readObject();
            defer key.deinit(self.alloc);
            if (key != .name) return error.UnexpectedDictKey;
            try self.skipSpaceAndComments();
            const value_start = self.pos;
            const first = self.peekByte() orelse return error.UnexpectedEof;
            try self.skipObject(self.object_depth);
            var value: Object = if (first == '<' or first == '[' or first == '(' or first == '/')
                .{ .deferred = .{ .reference = self.source_ref.?, .start = self.base_offset + value_start, .end = self.base_offset + self.pos } }
            else blk: {
                // Parse only this scalar, so numeric lookahead cannot allocate
                // the next (possibly enormous and unselected) dictionary key.
                var scalar = Scanner.init(self.alloc, self.bytes[value_start..self.pos]);
                scalar.control = self.control;
                scalar.max_object_depth = self.max_object_depth;
                scalar.object_depth = self.object_depth;
                defer scalar.deinit();
                break :blk try scalar.readObject();
            };
            errdefer value.deinit(self.alloc);
            try entries.append(self.alloc, .{ .key = key.name, .value = value });
            key = .null;
        }
    }

    // Hash decoded PDF names without allocating unselected keys. The adapted
    // context has exactly StringHashMap's hash/equality semantics after #xx
    // decoding, including long names and escaped bytes.
    const ResourceNameContext = struct {
        control: ?*ScanControl = null,
        failure: ?*?anyerror = null,
        fn spend(self: @This(), work: usize) bool {
            if (self.control) |control| control.spend(work) catch |err| {
                self.failure.?.* = err;
                return false;
            };
            return true;
        }
        fn next(raw: []const u8, offset: *usize, literal_until: *usize) ?u8 {
            if (offset.* == raw.len) return null;
            const byte = raw[offset.*];
            offset.* += 1;
            if (byte == '#' and offset.* > literal_until.* and offset.* + 1 < raw.len) {
                if (unhex(raw[offset.*])) |hi| if (unhex(raw[offset.* + 1])) |lo| {
                    offset.* += 2;
                    return hi << 4 | lo;
                };
                // Match readName's literal recovery for malformed escapes.
                literal_until.* = offset.* + 2;
            }
            return byte;
        }
        pub fn hash(self: @This(), raw: []const u8) u64 {
            var hasher = std.hash.Wyhash.init(0);
            var offset: usize = 0;
            var literal_until: usize = 0;
            var buffer: [64]u8 = undefined;
            var count: usize = 0;
            while (next(raw, &offset, &literal_until)) |byte| {
                buffer[count] = byte;
                count += 1;
                if (count == buffer.len) {
                    if (!self.spend(buffer.len)) return 0;
                    hasher.update(&buffer);
                    count = 0;
                }
            }
            if (!self.spend(count)) return 0;
            hasher.update(buffer[0..count]);
            return hasher.final();
        }
        pub fn eql(self: @This(), raw: []const u8, name: []const u8) bool {
            var offset: usize = 0;
            var i: usize = 0;
            var literal_until: usize = 0;
            while (next(raw, &offset, &literal_until)) |byte| {
                if (i % 4096 == 0 and !self.spend(@min(4096, name.len -| i))) return false;
                if (i == name.len or name[i] != byte) return false;
                i += 1;
            }
            return i == name.len;
        }
    };

    fn readDictOrStream(self: *Scanner, projection: Projection) anyerror!Object {
        if ((projection == .resource_table or projection == .resource_entries) and self.source_ref != null)
            return self.readResourceTable(projection == .resource_table);
        var entries = std.ArrayList(DictEntry).empty;
        var header_only = false;
        var subtype_seen = false;
        var secondary_subtype_seen = false;
        var form = false;
        var postscript = false;
        var deferred_form_fields = false;
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

            const selected = (if (header_only) Projection.xobject_header else projection).field(tok.name);
            if (!selected.keep) {
                try self.skipObject(self.object_depth);
                continue;
            }
            if (projection == .resource_fields) {
                // The view has at most five fields. Preserve first-key lookup
                // semantics without retaining repeated attacker-owned entries.
                var duplicate = false;
                for (entries.items) |entry| if (std.mem.eql(u8, entry.key, tok.name)) {
                    duplicate = true;
                    break;
                };
                if (duplicate) {
                    try self.skipObject(self.object_depth);
                    continue;
                }
            }
            const key = try self.alloc.dupe(u8, tok.name);
            errdefer self.alloc.free(key);

            const value: Object = if (selected.view and self.source_ref != null) blk: {
                try self.skipSpaceAndComments();
                const first = self.peekByte() orelse return error.UnexpectedEof;
                if (first != '<' and first != '[' and first != '(' and first != '/')
                    break :blk try self.readObjectProjected(.full);
                const start = self.base_offset + self.pos;
                try self.skipObject(self.object_depth);
                break :blk .{ .deferred = .{ .reference = self.source_ref.?, .start = start, .end = self.base_offset + self.pos } };
            } else if (projection == .text_xobject and selected.child == .full and !selected.skip) blk: {
                // Classification includes an optional late Subtype2 /PS. Defer
                // allocating Form fields until it is complete, but parse the
                // common scalar resource references once without a rescan.
                try self.skipSpaceAndComments();
                if (self.peekByte()) |first| if (std.ascii.isDigit(first))
                    break :blk try self.readObjectProjected(selected.child);
                const start = self.base_offset + self.pos;
                try self.skipObject(self.object_depth);
                deferred_form_fields = true;
                break :blk .{ .deferred = .{ .reference = self.source_ref orelse .{ .id = 0, .gen = 0 }, .start = start, .end = self.base_offset + self.pos } };
            } else if (projection == .property_table and self.source_ref != null and self.unread.items.len == 0 and (try self.nextIsDictionary())) blk: {
                try self.skipSpaceAndComments();
                const start = self.base_offset + self.pos;
                try self.skipObject(self.object_depth);
                const end = self.base_offset + self.pos;
                // Bound eager work per name. Tiny dictionaries avoid a second
                // resolver/cache entry in dense tables; larger values retain
                // only their source location, regardless of string size.
                if (end - start <= 96) {
                    var inline_scanner = Scanner.initWithOffset(self.alloc, self.bytes[start - self.base_offset .. end - self.base_offset], start);
                    inline_scanner.source_ref = self.source_ref;
                    inline_scanner.control = self.control;
                    inline_scanner.allow_stream = false;
                    inline_scanner.max_object_depth = self.max_object_depth;
                    inline_scanner.object_depth = self.object_depth;
                    defer inline_scanner.deinit();
                    break :blk try inline_scanner.readObjectProjected(.property);
                }
                break :blk .{ .deferred = .{ .reference = self.source_ref.?, .start = start, .end = end } };
            } else if (selected.skip) blk: {
                try self.skipObject(self.object_depth);
                break :blk .null;
            } else try self.readObjectProjected(selected.child);
            errdefer {
                var mutable = value;
                mutable.deinit(self.alloc);
            }

            try entries.append(self.alloc, .{ .key = key, .value = value });
            if (projection == .text_xobject and !subtype_seen and std.mem.eql(u8, key, "Subtype")) {
                subtype_seen = true;
                form = std.mem.eql(u8, value.asName() orelse "", "Form");
                header_only = !form or postscript;
            }
            if (projection == .text_xobject and !secondary_subtype_seen and std.mem.eql(u8, key, "Subtype2")) {
                secondary_subtype_seen = true;
                postscript = std.mem.eql(u8, value.asName() orelse "", "PS");
                header_only = postscript or (subtype_seen and !form);
            }
        }

        // Native extraction consumes only PDF Form payloads. Other subtypes
        // (including both PostScript encodings) need no validated byte range.
        header_only = projection == .text_xobject and (!form or postscript);
        if (header_only) {
            // Classification keys may occur anywhere. Drop deferred Form
            // fields; fields after a known ignored subtype were skipped.
            var kept: usize = 0;
            for (entries.items) |*entry| {
                if (Projection.xobject_header.field(entry.key).keep) {
                    entries.items[kept] = entry.*;
                    kept += 1;
                } else entry.deinit(self.alloc);
            }
            entries.shrinkRetainingCapacity(kept);
        } else if (deferred_form_fields) {
            for (entries.items) |*entry| {
                if (entry.value != .deferred) continue;
                const location = entry.value.deferred;
                var field_scanner = Scanner.initWithOffset(self.alloc, self.bytes[location.start - self.base_offset .. location.end - self.base_offset], location.start);
                field_scanner.source_ref = self.source_ref;
                field_scanner.control = self.control;
                field_scanner.allow_stream = self.allow_stream;
                field_scanner.max_object_depth = self.max_object_depth;
                field_scanner.object_depth = self.object_depth;
                field_scanner.stream_length_resolver = self.stream_length_resolver;
                defer field_scanner.deinit();
                entry.value = try field_scanner.readObjectProjected(projection.field(entry.key).child);
            }
        }
        const owned = try entries.toOwnedSlice(self.alloc);
        entries = .empty;
        errdefer {
            for (owned) |*entry| entry.deinit(self.alloc);
            self.alloc.free(owned);
        }

        if (!self.allow_stream or projection == .resource_fields or projection == .resource_table) return .{ .dict = owned };
        var tok = try self.readToken();
        defer tok.deinit(self.alloc);
        if (tok != .keyword or !std.mem.eql(u8, tok.keyword, "stream")) {
            try self.unreadOwnedToken(&tok);
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

        // Header-only views deliberately have no payload range. They cannot
        // be passed to a stream decoder or mistaken for a validated stream.
        if (projection == .xobject_header or projection == .color_profile_header or projection == .appearance_header or projection == .structure_metadata or header_only) return .{ .stream_header = owned };

        const data_start = self.pos;
        const data_offset = self.base_offset + data_start;
        const actual_length = if (try self.streamLength(owned)) |direct_length| blk: {
            if (direct_length > self.bytes.len - self.pos) {
                return error.UnexpectedEof;
            }
            self.pos += direct_length;
            // Seeking by a validated length does not scan payload bytes.
            if (self.control != null) self.last_checked_pos = self.pos;
            break :blk direct_length;
        } else blk: {
            const data_end = (try findEndStreamDataEnd(self.bytes, self.pos, self.control)) orelse {
                return error.MissingEndStream;
            };
            self.pos = data_end;
            if (self.control != null) self.last_checked_pos = self.pos;
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

    fn streamLength(self: *Scanner, entries: []DictEntry) !?usize {
        for (entries) |*entry| {
            if (!std.mem.eql(u8, entry.key, "Length")) continue;
            if (entry.value.asInteger()) |length| {
                if (length < 0) return error.InvalidStreamLength;
                return std.math.cast(usize, length) orelse error.InvalidStreamLength;
            }
            if (entry.value == .obj_ref) if (self.stream_length_resolver) |resolver| {
                var object_stream = false;
                for (entries) |field| {
                    if (std.mem.eql(u8, field.key, "Type") and std.mem.eql(u8, field.value.asName() orelse "", "ObjStm")) {
                        object_stream = true;
                        break;
                    }
                }
                const length = try resolver.resolve(resolver.context, entry.value.obj_ref, object_stream);
                if (length) |resolved| {
                    if (resolved > self.bytes.len - self.pos) return error.UnexpectedEof;
                    // Cache the validated scalar in the owned header without
                    // enlarging every Object or retaining a second map.
                    entry.value = .{ .integer = std.math.cast(i64, resolved) orelse return error.InvalidStreamLength };
                }
                return length;
            };
            return null;
        }
        return null;
    }

    fn readToken(self: *Scanner) anyerror!Token {
        try self.checkpoint();
        if (self.unread.items.len > 0) {
            return self.unread.pop().?;
        }

        try self.skipSpaceAndComments();
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
                if (!(try self.nextStartsHexString())) return .{ .keyword = try self.alloc.dupe(u8, "<") };
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

    fn unreadOwnedToken(self: *Scanner, tok: *Token) anyerror!void {
        // unreadToken takes ownership even if growing the stack fails. Clear
        // the caller's deferred cleanup before transferring the payload.
        const owned = tok.*;
        tok.* = .eof;
        try self.unreadToken(owned);
    }

    fn skipObject(self: *Scanner, depth: usize) anyerror!void {
        try self.checkpoint();
        if (depth >= self.max_object_depth) return error.PdfObjectNestingTooDeep;
        if (self.unread.items.len != 0) {
            // Normally a dictionary key consumes the final lookahead token.
            // Preserve generic parser behavior for unusual buffered input.
            var object = try self.readObject();
            object.deinit(self.alloc);
            return;
        }
        try self.skipSpaceAndComments();
        const first = self.readByte() orelse return error.UnexpectedEof;
        switch (first) {
            '(' => {
                var nesting: usize = 1;
                while (nesting != 0) {
                    const offset = try self.findStringMarker() orelse {
                        self.pos = self.bytes.len;
                        return;
                    };
                    self.pos += offset;
                    switch (self.readByte().?) {
                        '(' => nesting += 1,
                        ')' => nesting -= 1,
                        '\\' => {
                            const escaped = self.readByte() orelse return error.UnexpectedEof;
                            if (escaped >= '0' and escaped <= '7') {
                                var value: u16 = escaped - '0';
                                var count: usize = 0;
                                while (count < 2) : (count += 1) {
                                    const next = self.peekByte() orelse break;
                                    if (next < '0' or next > '7') break;
                                    self.pos += 1;
                                    value = value * 8 + next - '0';
                                }
                                if (value > 255) return error.InvalidOctalEscape;
                            }
                        },
                        else => unreachable,
                    }
                }
            },
            '[' => while (true) {
                try self.skipSpaceAndComments();
                if (self.peekByte() == ']') {
                    self.pos += 1;
                    break;
                }
                try self.skipObject(depth + 1);
            },
            '<' => {
                if (self.peekByte() == '<') {
                    self.pos += 1;
                    while (true) {
                        try self.skipSpaceAndComments();
                        if (self.pos + 1 < self.bytes.len and std.mem.eql(u8, self.bytes[self.pos..][0..2], ">>")) {
                            self.pos += 2;
                            break;
                        }
                        if (self.peekByte() != '/') return error.UnexpectedDictKey;
                        try self.skipObject(depth + 1);
                        try self.skipObject(depth + 1);
                    }
                } else {
                    if (!(try self.nextStartsHexString())) return error.UnexpectedKeyword;
                    while (true) {
                        const hi = (try self.readSkippingSpace()) orelse break;
                        if (hi == '>' or unhex(hi) == null) break;
                        const lo = (try self.readSkippingSpace()) orelse break;
                        if (lo == '>' or unhex(lo) == null) break;
                    }
                }
            },
            '/' => {
                while (self.pos < self.bytes.len) {
                    try self.checkpoint();
                    const byte = self.readByte().?;
                    if (isSpace(byte) or isDelim(byte)) {
                        self.pos -= 1;
                        break;
                    }
                    if (byte == '#') {
                        _ = self.readByte() orelse break;
                        _ = self.readByte() orelse break;
                    }
                }
            },
            else => {
                self.pos -= 1;
                const word = try self.skipWord();
                if (std.mem.eql(u8, word, "null") or std.mem.eql(u8, word, "true") or std.mem.eql(u8, word, "false")) return;
                const number = (try numberToken(word)) orelse return error.UnexpectedKeyword;
                if (number != .integer or number.integer < 0 or number.integer > std.math.maxInt(u32)) return;
                const end = self.pos;
                const generation = self.skipWord() catch |err| switch (err) {
                    error.UnexpectedEof => {
                        self.pos = end;
                        return;
                    },
                    else => return err,
                };
                const gen = try numberToken(generation);
                if (gen == null or gen.? != .integer or gen.?.integer < 0 or gen.?.integer > std.math.maxInt(u16)) {
                    self.pos = end;
                    return;
                }
                const suffix = self.skipWord() catch |err| switch (err) {
                    error.UnexpectedEof => {
                        self.pos = end;
                        return;
                    },
                    else => return err,
                };
                if (!std.mem.eql(u8, suffix, "R")) self.pos = end;
            },
        }
    }

    fn skipWord(self: *Scanner) ![]const u8 {
        const active = self.control;
        try self.skipSpaceAndComments();
        const start = self.pos;
        while (self.pos < self.bytes.len and !isSpace(self.bytes[self.pos]) and !isDelim(self.bytes[self.pos])) {
            try self.checkpointUsing(active);
            self.pos += 1;
        }
        if (self.pos == start) return error.UnexpectedEof;
        return self.bytes[start..self.pos];
    }

    fn skipSpaceAndComments(self: *Scanner) !void {
        const active = self.control;
        while (self.pos < self.bytes.len) {
            try self.checkpointUsing(active);
            const c = self.bytes[self.pos];
            if (isSpace(c)) {
                self.pos += 1;
                continue;
            }
            if (c == '%') {
                while (self.pos < self.bytes.len and self.bytes[self.pos] != '\r' and self.bytes[self.pos] != '\n') : (self.pos += 1) {
                    try self.checkpointUsing(active);
                }
                continue;
            }
            break;
        }
    }

    fn nextStartsHexString(self: *Scanner) !bool {
        var i = self.pos;
        while (i < self.bytes.len and isSpace(self.bytes[i])) : (i += 1) {
            if (self.control) |control| try control.spend(1);
        }
        if (i >= self.bytes.len) return false;
        return self.bytes[i] == '>' or unhex(self.bytes[i]) != null;
    }

    fn readHexString(self: *Scanner) anyerror![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        while (true) {
            const c = (try self.readSkippingSpace()) orelse break;
            if (c == '>') break;
            const hi = unhex(c) orelse break;
            const c2 = (try self.readSkippingSpace()) orelse {
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
        const active = self.control;
        // Most literal strings need no escape or nesting processing. Allocate
        // their exact size once: geometric growth otherwise retains both old
        // and new buffers alongside a decoded object stream during relocation.
        if (try self.findStringMarker()) |end| {
            if (self.bytes[self.pos + end] == ')') {
                const result = try self.alloc.dupe(u8, self.bytes[self.pos .. self.pos + end]);
                self.pos += end + 1;
                return result;
            }
        }
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        var depth: usize = 1;
        while (self.pos < self.bytes.len) {
            try self.checkpointUsing(active);
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
        const active = self.control;
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        while (self.pos < self.bytes.len) {
            try self.checkpointUsing(active);
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
        const active = self.control;
        const start = self.pos;
        while (self.pos < self.bytes.len and !isSpace(self.bytes[self.pos]) and !isDelim(self.bytes[self.pos])) {
            try self.checkpointUsing(active);
            self.pos += 1;
        }
        const text = self.bytes[start..self.pos];
        if (std.mem.eql(u8, text, "true")) return .{ .boolean = true };
        if (std.mem.eql(u8, text, "false")) return .{ .boolean = false };
        if (try numberToken(text)) |number| return number;
        return .{ .keyword = try self.alloc.dupe(u8, text) };
    }

    fn readSkippingSpace(self: *Scanner) !?u8 {
        const active = self.control;
        while (self.pos < self.bytes.len) {
            try self.checkpointUsing(active);
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

// Scalar tokens borrow no storage. Shared numeric validation keeps skipped
// metadata lexically consistent with ordinary object parsing.
fn numberToken(text: []const u8) !?Token {
    if (isInteger(text)) {
        const value = std.fmt.parseInt(i64, text, 10) catch |err| switch (err) {
            error.Overflow => {
                const real = try std.fmt.parseFloat(f64, text);
                if (!std.math.isFinite(real)) return error.Overflow;
                return .{ .real = real };
            },
            else => return err,
        };
        return .{ .integer = value };
    }
    if (isReal(text)) return .{ .real = try std.fmt.parseFloat(f64, text) };
    return null;
}

fn findEndStreamDataEnd(bytes: []const u8, start: usize, control: ?*ScanControl) !?usize {
    var search = start;
    while (search < bytes.len) {
        // Include marker overlap while bounding each cancellation/work interval.
        const window_end = if (control != null) search + @min(bytes.len - search, 4096) else bytes.len;
        const scan_end = window_end + @min(bytes.len - window_end, "endstream".len - 1);
        const maybe_idx = std.mem.indexOf(u8, bytes[search..scan_end], "endstream") orelse {
            if (control) |active| try active.spend(window_end - search);
            search = window_end;
            continue;
        };
        const kw_pos = search + maybe_idx;
        if (control) |active| try active.spend(maybe_idx + "endstream".len);
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

test "scanner represents oversized finite integer tokens as real numbers" {
    const alloc = std.testing.allocator;
    var scanner = Scanner.init(
        alloc,
        "[9223372036854775807 -9223372036854775808 9223372036854775808 -9223372036854775809]",
    );
    defer scanner.deinit();
    var numbers = try scanner.readObject();
    defer numbers.deinit(alloc);
    try std.testing.expectEqual(std.math.maxInt(i64), numbers.array[0].integer);
    try std.testing.expectEqual(std.math.minInt(i64), numbers.array[1].integer);
    try std.testing.expectEqual(@as(f64, 9223372036854775808.0), numbers.array[2].real);
    try std.testing.expectEqual(@as(f64, -9223372036854775809.0), numbers.array[3].real);
}

test "scanner rejects integer tokens beyond finite real range" {
    var scanner = Scanner.init(std.testing.allocator, z17RepeatString("9", 400));
    defer scanner.deinit();
    try std.testing.expectError(error.Overflow, scanner.readObject());
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

test "bounded object scanner rejects nested arrays before recursive expansion" {
    var scanner = Scanner.init(std.testing.allocator, "[[[[[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]]]]");
    defer scanner.deinit();
    scanner.max_object_depth = 16;
    try std.testing.expectError(error.PdfObjectNestingTooDeep, scanner.readObject());
    try std.testing.expectEqual(@as(usize, 0), scanner.object_depth);
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

fn z17RepeatString(comptime bytes: []const u8, comptime repetitions: usize) *const [bytes.len * repetitions:0]u8 {
    const result = comptime blk: {
        @setEvalBranchQuota(@intCast(@min(std.math.maxInt(u32), 100000 +| (repetitions *| 16))));
        var repeated: [bytes.len * repetitions:0]u8 = undefined;
        for (0..repetitions) |i| @memcpy(repeated[i * bytes.len ..][0..bytes.len], bytes);
        repeated[bytes.len * repetitions] = 0;
        break :blk repeated;
    };
    return &result;
}

test "scanner lookahead keeps a single large payload allocation" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 55 * 1024);
    defer a.free(payload);
    @memset(payload, 'X');
    for ([_][]const u8{ "[ (", "[ 0 (", "[ 0 1 (", "[ << >> (" }) |prefix| {
        const input = try std.fmt.allocPrint(a, "{s}{s}) ]", .{ prefix, payload });
        defer a.free(input);
        var storage: [64 * 1024]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&storage);
        var scanner = Scanner.init(bounded.allocator(), input);
        defer scanner.deinit();
        var object = try scanner.readObject();
        defer object.deinit(bounded.allocator());
        try std.testing.expect(object == .array);
        try std.testing.expectEqualStrings(payload, object.array[object.array.len - 1].string);
        try std.testing.expectEqual(@as(usize, 0), scanner.unread.items.len);
    }
}

test "scanner lookahead transfers release partial allocations on every failure" {
    const backing = std.testing.allocator;
    const Check = struct {
        fn run(a: Allocator, input: []const u8, expected: ?anyerror) !void {
            var moving = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
            const alloc = moving.allocator();
            var scanner = Scanner.init(alloc, input);
            defer scanner.deinit();
            var object = scanner.readObject() catch |err| {
                if (err == error.OutOfMemory or expected == null) return err;
                try std.testing.expectEqual(expected.?, err);
                return;
            };
            defer object.deinit(alloc);
            if (expected != null) return error.ExpectedParseFailure;
        }
    };
    for ([_]struct { input: []const u8, expected: ?anyerror = null }{
        .{ .input = "[(payload)]" },
        .{ .input = "[0 (payload)]" },
        .{ .input = "[0 1 (payload)]" },
        .{ .input = "[<< >> (payload)]" },
        .{ .input = "[1 0 R 1.5 null true /Name]" },
        .{ .input = "[0 1 (payload)", .expected = error.UnexpectedEof },
        .{ .input = "[<< >> keyword]", .expected = error.UnexpectedKeyword },
    }) |case| {
        try std.testing.checkAllAllocationFailures(backing, Check.run, .{ case.input, case.expected });
    }
}

test "scanner metadata projections skip unknown payloads within a small heap" {
    const a = std.testing.allocator;
    const padding = try a.alloc(u8, 55 * 1024);
    defer a.free(padding);
    @memset(padding, 'X');
    const input = try std.fmt.allocPrint(a, "11 0 obj << /Font << /F1 2 0 R >> /Unknown -0 +0 R /Properties << /Tag << /MCID 0 /ActualText (Label) /Private [ /Name#20WithSpace <CAFE> [<< /Nested (\\n{s}) >>] ] >> >> >> endobj", .{padding});
    defer a.free(input);
    for ([_]Projection{ .resources, .required_resources, .resource_properties }) |projection| {
        var storage: [8 * 1024]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&storage);
        var scanner = Scanner.init(bounded.allocator(), input);
        scanner.max_object_depth = 128;
        defer scanner.deinit();
        var object = try scanner.readObjectProjected(projection);
        defer object.deinit(bounded.allocator());
        const value = object.obj_def.value;
        try std.testing.expect(value.get("Unknown") == null);
        if (projection != .resource_properties) try std.testing.expectEqual(@as(u32, 2), value.get("Font").?.get("F1").?.obj_ref.id);
        const properties = value.get("Properties").?;
        if (projection == .required_resources) {
            try std.testing.expect(properties.* == .null);
        } else {
            const tag = properties.get("Tag").?;
            try std.testing.expectEqual(@as(i64, 0), tag.get("MCID").?.integer);
            try std.testing.expectEqualStrings("Label", tag.get("ActualText").?.string);
            try std.testing.expect(tag.get("Private") == null);
        }
    }
}

test "scanner projected values preserve references and reject compound scalars without retaining them" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "<< /MCID [1 2 3] /ActualText << /Nested [null true] >> /Private 99 0 R >>",
        "<< /MCID (not an integer) /ActualText /NotText /Private [0 1 (unread) << >>] >>",
    }) |input| {
        var scanner = Scanner.init(a, input);
        defer scanner.deinit();
        var object = try scanner.readObjectProjected(.property);
        defer object.deinit(a);
        try std.testing.expect(object.get("MCID").?.* == .boolean);
        try std.testing.expect(object.get("ActualText").?.* == .boolean);
        try std.testing.expect(object.get("Private") == null);
    }
    var scanner = Scanner.init(a, "<< /MCID 12 0 R /ActualText 13 0 R /Private -0 -0 R >>");
    defer scanner.deinit();
    var object = try scanner.readObjectProjected(.property);
    defer object.deinit(a);
    try std.testing.expectEqual(@as(u32, 12), object.get("MCID").?.obj_ref.id);
    try std.testing.expectEqual(@as(u32, 13), object.get("ActualText").?.obj_ref.id);
}

test "scanner skipped metadata preserves malformed token boundaries" {
    const cases = [_]struct { input: []const u8, expected: anyerror }{
        .{ .input = "<< /Private /Bad# /ActualText (forged) >>", .expected = error.UnexpectedDictKey },
        .{ .input = "<< /Private <q /ActualText (forged) >>", .expected = error.UnexpectedKeyword },
        .{ .input = "<< /Private (\\777) /ActualText (forged) >>", .expected = error.InvalidOctalEscape },
        .{ .input = "<< /Private (unterminated", .expected = error.UnexpectedEof },
    };
    for (cases) |case| for ([_]Projection{ .full, .property }) |projection| {
        var scanner = Scanner.init(std.testing.allocator, case.input);
        defer scanner.deinit();
        try std.testing.expectError(case.expected, scanner.readObjectProjected(projection));
    };
}

test "scanner projected object ownership is safe under allocation failure" {
    const Check = struct {
        fn run(backing: Allocator, projection: Projection) !void {
            var moving = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
            const alloc = moving.allocator();
            var scanner = Scanner.init(alloc, "11 0 obj << /Font << /F1 2 0 R >> /Ignored [0 1 (skip) << /Name /Value >>] /Properties << /Tag << /MCID 0 /ActualText (Label) /Private [<CAFE> null true] >> >> >> endobj");
            defer scanner.deinit();
            var object = try scanner.readObjectProjected(projection);
            defer object.deinit(alloc);
            try std.testing.expect(object.obj_def.value.get("Ignored") == null);
        }
    };
    for ([_]Projection{ .resources, .required_resources, .resource_properties }) |projection| {
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{projection});
    }
}

test "scanner inherited resource projections defer properties and discard unrelated parent fields" {
    const a = std.testing.allocator;
    const padding = try a.alloc(u8, 55 * 1024);
    defer a.free(padding);
    @memset(padding, 'X');
    const input = try std.fmt.allocPrint(a, "<< /Parent 9 0 R /Kids [({s})] /Resources << /Font << /F1 5 0 R >> /Properties << /Tag << /MCID 2 /ActualText (Label) /Private (\\n{s}) >> >> >> >>", .{ padding, padding });
    defer a.free(input);
    for ([_]Projection{ .resource_container, .container_properties }) |projection| {
        var storage: [8 * 1024]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&storage);
        var scanner = Scanner.init(bounded.allocator(), input);
        scanner.max_object_depth = 128;
        defer scanner.deinit();
        var object = try scanner.readObjectProjected(projection);
        defer object.deinit(bounded.allocator());
        try std.testing.expect(object.get("Kids") == null);
        const resources = object.get("Resources").?;
        if (projection == .resource_container) {
            try std.testing.expectEqual(@as(u32, 9), object.get("Parent").?.obj_ref.id);
            try std.testing.expect(resources.get("Properties").?.* == .null);
            try std.testing.expectEqual(@as(u32, 5), resources.get("Font").?.get("F1").?.obj_ref.id);
        } else {
            try std.testing.expect(object.get("Parent") == null);
            try std.testing.expect(resources.get("Font") == null);
            const tag = resources.get("Properties").?.get("Tag").?;
            try std.testing.expectEqualStrings("Label", tag.get("ActualText").?.string);
            try std.testing.expectEqual(@as(i64, 2), tag.get("MCID").?.integer);
            try std.testing.expect(tag.get("Private") == null);
        }
    }
}

test "scanner defers direct property dictionaries as source locations" {
    const a = std.testing.allocator;
    const padding = try a.alloc(u8, 55 * 1024);
    defer a.free(padding);
    @memset(padding, 'X');
    const input = try std.fmt.allocPrint(a, "<< /Unused << /ActualText (\\n{s}) >> /Tag << /ActualText (Label) >> /Indirect 9 0 R >>", .{padding});
    defer a.free(input);
    var storage: [8 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&storage);
    var scanner = Scanner.initWithOffset(bounded.allocator(), input, 100);
    scanner.source_ref = .{ .id = 7, .gen = 2 };
    var control = ScanControl{};
    scanner.control = &control;
    defer scanner.deinit();
    var object = try scanner.readObjectProjected(.property_table);
    defer object.deinit(bounded.allocator());
    try std.testing.expect(object.get("Unused").?.* == .deferred);
    try std.testing.expectEqual(@as(u32, 9), object.get("Indirect").?.obj_ref.id);
    const location = object.get("Unused").?.deferred;
    try std.testing.expectEqualStrings("Label", object.get("Tag").?.get("ActualText").?.string);
    try std.testing.expectEqual(@as(u32, 7), location.reference.id);
    try std.testing.expectEqual(@as(u16, 2), location.reference.gen);
    try std.testing.expect(std.mem.startsWith(u8, input[location.start - 100 .. location.end - 100], "<< /ActualText"));
    try std.testing.expect(location.end - location.start > padding.len);
    var copy = try object.clone(bounded.allocator());
    defer copy.deinit(bounded.allocator());
    try std.testing.expectEqual(location, copy.get("Unused").?.deferred);
}

test "scanner polls cancellation and bounds work throughout long tokens and skipped values" {
    const a = std.testing.allocator;
    const padding = try a.alloc(u8, 32 * 1024);
    defer a.free(padding);
    const Probe = struct {
        calls: usize = 0,
        fn cancelled(raw: ?*const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            return self.calls == 2;
        }
    };
    inline for (.{ "({s})", "(\\n{s})", "/{s}", "<{s}>", "[{s}]", "%{s}\nnull", "{s}null", "{s}" }, 0..) |format, i| {
        @memset(padding, if (i == 6) ' ' else if (i >= 4) '0' else 'A');
        if (i == 4) for (padding, 0..) |*byte, j| {
            byte.* = if (j % 2 == 0) '0' else ' ';
        };
        const value = try std.fmt.allocPrint(a, format, .{padding});
        defer a.free(value);
        const input = try std.fmt.allocPrint(a, "<< /Private {s} /ActualText (Label) >>", .{value});
        defer a.free(input);
        for ([_]Projection{ .full, .property }) |projection| {
            var probe = Probe{};
            var control = ScanControl{ .context = &probe, .is_cancelled_fn = Probe.cancelled };
            var scanner = Scanner.init(a, input);
            scanner.control = &control;
            defer scanner.deinit();
            try std.testing.expectError(error.Canceled, scanner.readObjectProjected(projection));
            try std.testing.expectEqual(@as(usize, 2), probe.calls);
            try std.testing.expect(scanner.pos < input.len / 2);
        }
        var control = ScanControl{ .remaining_work = 64 };
        var scanner = Scanner.init(a, input);
        scanner.control = &control;
        defer scanner.deinit();
        try std.testing.expectError(error.PdfMetadataWorkLimit, scanner.readObjectProjected(.property));
    }
}

test "scanner numeric lookahead propagates cancellation and work exhaustion" {
    const a = std.testing.allocator;
    const padding: [2048]u8 = @splat('X');
    const Probe = struct {
        calls: usize = 0,
        fn cancelled(raw: ?*const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            return self.calls >= 2;
        }
    };
    // Exercise both generation and R-suffix lookahead at every nearby poll
    // boundary, including the two prefixes that previously swallowed Canceled.
    for ([_][]const u8{ "0", "0 1 R" }) |number| {
        for (1980..2020) |len| {
            const input = try std.fmt.allocPrint(a, "<< /Padding ({s}) /ActualText (Label) /Private {s} >>", .{ padding[0..len], number });
            defer a.free(input);
            var probe = Probe{};
            var control = ScanControl{ .context = &probe, .is_cancelled_fn = Probe.cancelled };
            var scanner = Scanner.init(a, input);
            scanner.control = &control;
            defer scanner.deinit();
            if (scanner.readObjectProjected(.property)) |result| {
                var object = result;
                defer object.deinit(a);
                try std.testing.expect(probe.calls < 2);
            } else |err| try std.testing.expectEqual(error.Canceled, err);
        }
        for (1..128) |limit| {
            const input = try std.fmt.allocPrint(a, "<< /Private {s} >>", .{number});
            defer a.free(input);
            var control = ScanControl{ .remaining_work = limit };
            var scanner = Scanner.init(a, input);
            scanner.control = &control;
            defer scanner.deinit();
            if (scanner.readObjectProjected(.property)) |result| {
                var object = result;
                object.deinit(a);
            } else |err| try std.testing.expectEqual(error.PdfMetadataWorkLimit, err);
        }
    }
}

test "scanner stream boundary search polls cancellation and bounds work" {
    const a = std.testing.allocator;
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, "<< /Type /ObjStm /N 1 /First 4 /Length 9 0 R >>\nstream\n");
    for (0..8192) |_| try input.appendSlice(a, "xendstreamx");
    try input.appendSlice(a, "\nendstream");
    const Probe = struct {
        calls: usize = 0,
        fn cancelled(raw: ?*const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            return self.calls >= 2;
        }
    };
    var probe = Probe{};
    var control = ScanControl{ .context = &probe, .is_cancelled_fn = Probe.cancelled };
    var scanner = Scanner.init(a, input.items);
    scanner.control = &control;
    defer scanner.deinit();
    try std.testing.expectError(error.Canceled, scanner.readObject());
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    try std.testing.expect(scanner.pos < input.items.len / 2);
    try std.testing.expect(std.math.maxInt(usize) - control.remaining_work < 16 * 1024);
    var limited = ScanControl{ .remaining_work = 1024 };
    var bounded = Scanner.init(a, input.items);
    bounded.control = &limited;
    defer bounded.deinit();
    try std.testing.expectError(error.PdfMetadataWorkLimit, bounded.readObject());
    try std.testing.expect(bounded.pos < input.items.len / 2);
}

test "scanner controlled stream searches preserve markers across chunk boundaries" {
    const a = std.testing.allocator;
    for (0..9) |offset| {
        const padding = try a.alloc(u8, 4096 - offset);
        defer a.free(padding);
        @memset(padding, 'X');
        for ([_][]const u8{ "\n", "\r", "\r\n" }) |newline| {
            const input = try std.fmt.allocPrint(a, "<< /Length 9 0 R >>\nstream\n{s}{s}endstream", .{ padding, newline });
            defer a.free(input);
            for ([_]bool{ false, true }) |controlled| {
                var control = ScanControl{};
                var scanner = Scanner.init(a, input);
                scanner.control = if (controlled) &control else null;
                defer scanner.deinit();
                var object = try scanner.readObject();
                defer object.deinit(a);
                try std.testing.expectEqual(padding.len, object.stream.data_length);
                try std.testing.expectEqualStrings(padding, input[object.stream.data_offset..][0..object.stream.data_length]);
            }
        }
    }
    // Unterminated payloads must consume bounded chunks, including a final
    // partial marker, rather than retrying the same bytes or allocating.
    for ([_][]const u8{ "", "X", "endstrea", "xendstreamx", " endstreamx", "xendstream " }) |input| {
        var control = ScanControl{};
        try std.testing.expectEqual(@as(?usize, null), try findEndStreamDataEnd(input, 0, &control));
    }
}

test "scanner resolves indirect lengths before examining payload markers" {
    const a = std.testing.allocator;
    const payload = "before\nendstream\nendobj\nafter\r\n";
    const input = "<< /Length 8 0 R >>\nstream\n" ++ payload ++ "\nendstream";
    const Resolver = struct {
        calls: usize = 0,
        length: usize = payload.len,
        failure: ?anyerror = null,
        fn resolve(raw: *const anyopaque, ref: ObjRef, _: bool) anyerror!?usize {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            try std.testing.expectEqual(@as(u32, 8), ref.id);
            if (self.failure) |err| return err;
            return self.length;
        }
    };
    var resolver = Resolver{};
    var control = ScanControl{ .remaining_work = 1024 };
    var scanner = Scanner.init(a, input);
    scanner.control = &control;
    scanner.stream_length_resolver = .{ .context = &resolver, .resolve = Resolver.resolve };
    defer scanner.deinit();
    var object = try scanner.readObject();
    defer object.deinit(a);
    try std.testing.expectEqualStrings(payload, input[object.stream.data_offset..][0..object.stream.data_length]);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqual(@as(i64, payload.len), object.get("Length").?.integer);
    for ([_]anyerror{ error.OutOfMemory, error.Canceled, error.PdfMetadataWorkLimit, error.InvalidStreamLength }) |failure| {
        var failing = Resolver{ .failure = failure };
        var failed = Scanner.init(a, input);
        failed.stream_length_resolver = .{ .context = &failing, .resolve = Resolver.resolve };
        defer failed.deinit();
        try std.testing.expectError(failure, failed.readObject());
    }
    var too_long = Resolver{ .length = std.math.maxInt(usize) };
    var invalid = Scanner.init(a, input);
    invalid.stream_length_resolver = .{ .context = &too_long, .resolve = Resolver.resolve };
    defer invalid.deinit();
    try std.testing.expectError(error.UnexpectedEof, invalid.readObject());
}

test "scanner known stream lengths seek without charging payload scan work" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 32 * 1024);
    defer a.free(payload);
    @memset(payload, 'X');
    const input = try std.fmt.allocPrint(a, "<< /Length {d} >>\nstream\n{s}\nendstream", .{ payload.len, payload });
    defer a.free(input);
    var control = ScanControl{ .remaining_work = 1024 };
    var scanner = Scanner.init(a, input);
    scanner.control = &control;
    defer scanner.deinit();
    var object = try scanner.readObject();
    defer object.deinit(a);
    try std.testing.expectEqual(payload.len, object.stream.data_length);
    try std.testing.expect(control.remaining_work > 0);
    var invalid = Scanner.init(a, "<< /Length -1 >>\nstream\nx\nendstream");
    defer invalid.deinit();
    try std.testing.expectError(error.InvalidStreamLength, invalid.readObject());
}

test "scanner image header views never inspect payloads or resolve lengths" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 32 * 1024);
    defer a.free(payload);
    @memset(payload, 'X');
    const input = try std.fmt.allocPrint(a, "8 0 obj\n<< /Subtype /Image /Length 99 0 R /StructParent 10 0 R >>\nstream\n{s}", .{payload});
    defer a.free(input);
    const Resolver = struct {
        calls: usize = 0,
        fn resolve(raw: *const anyopaque, _: ObjRef, _: bool) anyerror!?usize {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            return error.InvalidStreamLength;
        }
    };
    var resolver = Resolver{};
    var control = ScanControl{ .remaining_work = 1024 };
    var scanner = Scanner.init(a, input);
    scanner.control = &control;
    scanner.stream_length_resolver = .{ .context = &resolver, .resolve = Resolver.resolve };
    defer scanner.deinit();
    var object = try scanner.readObjectProjected(.xobject_header);
    defer object.deinit(a);
    const header = object.obj_def.value;
    try std.testing.expect(header.* == .stream_header);
    try std.testing.expectEqualStrings("Image", header.get("Subtype").?.asName().?);
    try std.testing.expect(header.get("StructParent") != null);
    try std.testing.expect(header.get("Length") == null);
    try std.testing.expectEqual(@as(usize, 0), resolver.calls);
    try std.testing.expect(scanner.pos < input.len / 2);
}

test "scanner native XObjects classify images without resolving lengths in either dictionary order" {
    const a = std.testing.allocator;
    const Resolver = struct {
        fn resolve(_: *const anyopaque, _: ObjRef, _: bool) anyerror!?usize {
            return error.InvalidStreamLength;
        }
    };
    for ([_][]const u8{
        \\8 0 obj << /Subtype /Image /Length 99 0 R /StructParent 10 0 R /Resources << /Private (ignored) >> /Matrix [1 0 0 1 0 0] >>
        \\stream
        \\payload without a terminator
        ,
        \\8 0 obj << /Length 99 0 R /Resources << /Private (ignored) >> /Matrix [1 0 0 1 0 0] /StructParent 10 0 R /Subtype /Image >>
        \\stream
        \\payload without a terminator
        ,
    }) |input| {
        var scanner = Scanner.init(a, input);
        defer scanner.deinit();
        scanner.stream_length_resolver = .{ .context = &scanner, .resolve = Resolver.resolve };
        var object = try scanner.readObjectProjected(.text_xobject);
        defer object.deinit(a);
        const image = object.obj_def.value;
        try std.testing.expect(image.* == .stream_header);
        try std.testing.expectEqual(@as(usize, 2), image.stream_header.len);
        try std.testing.expectEqualStrings("Image", image.get("Subtype").?.asName().?);
        try std.testing.expect(image.get("StructParent") != null);
        try std.testing.expect(image.get("Length") == null);
        try std.testing.expect(image.get("Resources") == null);
    }
}

fn exerciseNativeFormProjection(a: Allocator, input: []const u8) !void {
    const payload = "BT (Hello) Tj ET";
    const Resolver = struct {
        calls: usize = 0,
        fn resolve(raw: *const anyopaque, reference: ObjRef, object_stream: bool) anyerror!?usize {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            try std.testing.expectEqual(@as(u32, 9), reference.id);
            try std.testing.expect(!object_stream);
            return payload.len;
        }
    };
    var resolver = Resolver{};
    var scanner = Scanner.init(a, input);
    defer scanner.deinit();
    scanner.stream_length_resolver = .{ .context = &resolver, .resolve = Resolver.resolve };
    var object = try scanner.readObjectProjected(.text_xobject);
    defer object.deinit(a);
    const form = object.obj_def.value;
    try std.testing.expect(form.* == .stream);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqualStrings(payload, input[form.stream.data_offset..][0..form.stream.data_length]);
    for ([_][]const u8{ "Subtype", "Type", "Length", "StructParents", "StructParent", "Resources", "BBox", "Matrix", "Group", "Filter", "DecodeParms" }) |field|
        try std.testing.expect(form.get(field) != null);
    try std.testing.expect(form.get("Private") == null);
    try std.testing.expectEqual(@as(i64, payload.len), form.get("Length").?.integer);
    try std.testing.expectEqual(@as(u32, 10), form.get("StructParents").?.obj_ref.id);
}

test "scanner native Form projection retains extraction fields and resolves length once" {
    const a = std.testing.allocator;
    const fields = "/Type /XObject /Length 9 0 R /StructParents 10 0 R /StructParent 11 0 R /Resources << /Font << /F1 5 0 R >> >> /BBox [0 0 10 10] /Matrix [1 0 0 1 0 0] /Group << /S /Transparency /I true /K false >> /Filter [] /DecodeParms null /Private (ignored) ";
    for ([_][]const u8{
        "8 0 obj << /Subtype /Form " ++ fields ++ ">>\nstream\nBT (Hello) Tj ET\nendstream\nendobj",
        "8 0 obj << " ++ fields ++ "/Subtype /Form >>\nstream\nBT (Hello) Tj ET\nendstream\nendobj",
        "8 0 obj << /Subtype /Form " ++ fields ++ "/Subtype2 /Other >>\nstream\nBT (Hello) Tj ET\nendstream\nendobj",
        "8 0 obj << /Subtype2 /Other " ++ fields ++ "/Subtype /Form >>\nstream\nBT (Hello) Tj ET\nendstream\nendobj",
    }) |input| {
        try exerciseNativeFormProjection(a, input);
        try std.testing.checkAllAllocationFailures(a, exerciseNativeFormProjection, .{input});
    }
}

test "scanner native image fields before Subtype do not materialize" {
    const a = std.testing.allocator;
    const padding = try a.alloc(u8, 256 * 1024);
    defer a.free(padding);
    @memset(padding, 'X');
    const input = try std.fmt.allocPrint(a, "8 0 obj << /Resources << /Private ({s}) >> /Filter [/FlateDecode /{s}] /DecodeParms [null << /Private ({s}) >>] /Subtype /Image /Length 99 0 R >>\nstream\nABC", .{ padding, padding, padding });
    defer a.free(input);
    var backing: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&backing);
    var scanner = Scanner.init(fixed.allocator(), input);
    defer scanner.deinit();
    var object = try scanner.readObjectProjected(.text_xobject);
    defer object.deinit(fixed.allocator());
    try std.testing.expect(object.obj_def.value.* == .stream_header);
    try std.testing.expectEqual(@as(usize, 1), object.obj_def.value.stream_header.len);
    var control = ScanControl{ .remaining_work = 1024 };
    var limited = Scanner.init(a, input);
    defer limited.deinit();
    limited.control = &control;
    try std.testing.expectError(error.PdfMetadataWorkLimit, limited.readObjectProjected(.text_xobject));
}

fn exerciseIgnoredXObjectProjection(a: Allocator, input: []const u8) !void {
    var scanner = Scanner.init(a, input);
    defer scanner.deinit();
    var object = try scanner.readObjectProjected(.text_xobject);
    defer object.deinit(a);
    try std.testing.expect(object.obj_def.value.* == .stream_header);
    try std.testing.expect(object.obj_def.value.get("Resources") == null);
    try std.testing.expect(object.obj_def.value.get("Length") == null);
    var copy = try object.clone(a);
    defer copy.deinit(a);
    try std.testing.expect(copy.obj_def.value.get("StructParent") != null);
}

test "scanner native ignored XObjects never allocate payload fields in any subtype order" {
    const a = std.testing.allocator;
    const padding = try a.alloc(u8, 64 * 1024);
    defer a.free(padding);
    @memset(padding, 'X');
    const cases = [_]struct { before: []const u8, after: []const u8 }{
        .{ .before = "/Subtype /PS", .after = "" },
        .{ .before = "", .after = "/Subtype /PS" },
        .{ .before = "/Subtype /Form", .after = "/Subtype2 /PS" },
        .{ .before = "/Subtype2 /PS", .after = "/Subtype /Form" },
        .{ .before = "", .after = "/Subtype /Form /Subtype2 /PS" },
        .{ .before = "/Subtype /Unknown", .after = "" },
        .{ .before = "", .after = "" },
    };
    for (cases) |case| {
        const input = try std.fmt.allocPrint(a, "8 0 obj << {s} /Resources << /Private ({s}) >> /Filter [/{s}] /DecodeParms << /Private ({s}) >> /Length 99 0 R {s} /StructParent 10 0 R >>\nstream\nno payload terminator", .{ case.before, padding, padding, padding, case.after });
        defer a.free(input);
        var backing: [4096]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&backing);
        try exerciseIgnoredXObjectProjection(fixed.allocator(), input);
        var control = ScanControl{ .remaining_work = 1024 };
        var limited = Scanner.init(a, input);
        defer limited.deinit();
        limited.control = &control;
        try std.testing.expectError(error.PdfMetadataWorkLimit, limited.readObjectProjected(.text_xobject));
        const Probe = struct {
            calls: usize = 0,
            fn cancelled(raw: ?*const anyopaque) bool {
                const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
                self.calls += 1;
                return self.calls >= 2;
            }
        };
        var probe = Probe{};
        var cancellation = ScanControl{ .context = &probe, .is_cancelled_fn = Probe.cancelled };
        var canceled = Scanner.init(a, input);
        defer canceled.deinit();
        canceled.control = &cancellation;
        try std.testing.expectError(error.Canceled, canceled.readObjectProjected(.text_xobject));
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
        const small = try std.fmt.allocPrint(a, "8 0 obj << {s} /Resources << /Private (owned) >> /Filter [/Private] /Length 99 0 R {s} /StructParent 10 0 R >>\nstream\nignored", .{ case.before, case.after });
        defer a.free(small);
        try std.testing.checkAllAllocationFailures(a, exerciseIgnoredXObjectProjection, .{small});
    }
}

fn exerciseFormResourceViews(a: Allocator, input: []const u8) !void {
    var scanner = Scanner.init(a, input);
    defer scanner.deinit();
    var object = try scanner.readObjectProjected(.text_xobject);
    defer object.deinit(a);
    const resources = object.obj_def.value.get("Resources").?;
    try std.testing.expect(resources.* == .dict);
    try std.testing.expect(resources.get("Private") == null);
    try std.testing.expectEqual(@as(u32, 5), resources.get("Font").?.get("F1").?.obj_ref.id);
    for ([_][]const u8{"Properties"}) |name| {
        const field = resources.get(name).?;
        try std.testing.expect(field.* == .deferred);
        try std.testing.expectEqual(@as(u32, 8), field.deferred.reference.id);
        try std.testing.expect(field.deferred.start < field.deferred.end);
    }
    var copy = try object.clone(a);
    defer copy.deinit(a);
    try std.testing.expectEqual(@as(u32, 5), copy.obj_def.value.get("Resources").?.get("Font").?.get("F1").?.obj_ref.id);
}

test "scanner native Form resources index only needed fields without rescanning opaque data" {
    const a = std.testing.allocator;
    const padding = try a.alloc(u8, 64 * 1024);
    defer a.free(padding);
    @memset(padding, 'X');
    const input = try std.fmt.allocPrint(a, "8 0 obj << /Subtype /Form /Resources << /Font << /F1 5 0 R >>\n%{s}\n/Private ({s}) /Properties << /Unused << /ActualText ({s}) >> >> >> /Length 1 >>\nstream\nx\nendstream\nendobj", .{ padding, padding, padding });
    defer a.free(input);
    var backing: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&backing);
    try exerciseFormResourceViews(fixed.allocator(), input);
    var control = ScanControl{ .remaining_work = input.len * 3 };
    var scanner = Scanner.init(a, input);
    defer scanner.deinit();
    scanner.control = &control;
    var object = try scanner.readObjectProjected(.text_xobject);
    defer object.deinit(a);
    try std.testing.expect(control.remaining_work > 0);
    const small = "8 0 obj << /Subtype /Form /Resources << /Font << /F1 5 0 R >> /Private (ignored) /Properties << /Tag << /ActualText (Label) >> >> >> /Length 1 >>\nstream\nx\nendstream\nendobj";
    try std.testing.checkAllAllocationFailures(a, exerciseFormResourceViews, .{small});
}

test "scanner resource views preserve scalar references and bound repeated keys" {
    const a = std.testing.allocator;
    const input = "8 0 obj << /Resources << /Font +5 0 R /Font << /Ignored (duplicate) >> /Properties 6 0 R /XObject null >> /Subtype /Form /Length 1 >>\nstream\nx\nendstream\nendobj";
    var scanner = Scanner.init(a, input);
    defer scanner.deinit();
    var object = try scanner.readObjectProjected(.text_xobject);
    defer object.deinit(a);
    const resources = object.obj_def.value.get("Resources").?;
    try std.testing.expectEqual(@as(usize, 3), resources.dict.len);
    try std.testing.expectEqual(@as(u32, 5), resources.get("Font").?.obj_ref.id);
    try std.testing.expectEqual(@as(u32, 6), resources.get("Properties").?.obj_ref.id);
    try std.testing.expect(resources.get("XObject").?.* == .null);
}

fn exerciseColorProfileHeader(a: Allocator, input: []const u8) !void {
    const Resolver = struct {
        fn resolve(_: *const anyopaque, _: ObjRef, _: bool) anyerror!?usize {
            return error.InvalidStreamLength;
        }
    };
    var scanner = Scanner.init(a, input);
    defer scanner.deinit();
    scanner.stream_length_resolver = .{ .context = &scanner, .resolve = Resolver.resolve };
    var object = try scanner.readObjectProjected(.color_profile_header);
    defer object.deinit(a);
    const header = object.obj_def.value;
    try std.testing.expect(header.* == .stream_header);
    try std.testing.expectEqual(@as(i64, 3), header.get("N").?.integer);
    try std.testing.expectEqualStrings("DeviceRGB", header.get("Alternate").?.asName().?);
    try std.testing.expectEqual(@as(usize, 6), header.get("Range").?.array.len);
    try std.testing.expect(header.get("Length") == null);
    try std.testing.expect(header.get("Metadata") == null);
    var copy = try header.clone(a);
    defer copy.deinit(a);
    try std.testing.expect(copy == .stream_header);
}

test "scanner color profile headers never resolve lengths or inspect payload boundaries" {
    const a = std.testing.allocator;
    const input = "8 0 obj << /Length 99 0 R /N 3 /Alternate /DeviceRGB /Range [0 1 0 1 0 1] /Metadata (ignored) >>\nstream\nno payload terminator";
    try std.testing.checkAllAllocationFailures(a, exerciseColorProfileHeader, .{input});
}

fn exerciseSelectedResourceEntries(a: Allocator, input: []const u8) !void {
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(a);
    try names.put(a, "F1", {});
    try names.put(a, "Bad#x#41", {});
    var scanner = Scanner.init(a, input);
    defer scanner.deinit();
    scanner.source_ref = .{ .id = 8, .gen = 0 };
    scanner.resource_names = &names;
    var table = try scanner.readObjectProjected(.resource_entries);
    defer table.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), table.dict.len);
    try std.testing.expect(table.get("F1").?.* == .deferred);
    try std.testing.expectEqual(@as(u32, 5), table.get("Bad#x#41").?.obj_ref.id);
}

test "scanner resource indexes stay bounded across large tables and subtype orders" {
    const a = std.testing.allocator;
    var table: std.ArrayList(u8) = .empty;
    defer table.deinit(a);
    for (0..4096) |i| try table.print(a, "/F{d} 5 0 R ", .{i});
    for ([_]struct { before: []const u8, after: []const u8 }{
        .{ .before = "", .after = "/Subtype /Image" },
        .{ .before = "/Subtype /Form", .after = "/Subtype2 /PS" },
        .{ .before = "/Subtype2 /PS", .after = "/Subtype /Form" },
        .{ .before = "/Subtype /Image", .after = "" },
    }) |order| {
        const input = try std.fmt.allocPrint(a, "8 0 obj << {s} /Resources << /Font << {s} >> /XObject << {s} >> /ExtGState << {s} >> /ColorSpace << {s} >> >> {s} /Length 99 0 R >>\nstream\nignored", .{ order.before, table.items, table.items, table.items, table.items, order.after });
        defer a.free(input);
        var buffer: [8192]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&buffer);
        var scanner = Scanner.init(fixed.allocator(), input);
        defer scanner.deinit();
        var object = try scanner.readObjectProjected(.text_xobject);
        defer object.deinit(fixed.allocator());
        try std.testing.expect(object.obj_def.value.* == .stream_header);
    }
}

test "scanner demanded resource tables select decoded names without allocating unused keys" {
    const a = std.testing.allocator;
    const huge_key = try a.alloc(u8, 128 * 1024);
    defer a.free(huge_key);
    @memset(huge_key, 'X');
    const input = try std.fmt.allocPrint(a, "<< /Unused 1 /{s} << /Private ({s}) >> /F#31 << /Subtype /Type1 >> /Bad#x#41 5 0 R >>", .{ huge_key, huge_key });
    defer a.free(input);
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    try exerciseSelectedResourceEntries(fixed.allocator(), input);
    try std.testing.checkAllAllocationFailures(a, exerciseSelectedResourceEntries, .{"<< /Unused 1 /F#31 << /Subtype /Type1 >> /Bad#x#41 5 0 R >>"});
}
