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

//! Bounded in-memory OOXML access. No filesystem extraction, entity expansion,
//! archive subprocess, or trust in ZIP entry size declarations.
const std = @import("std");
const A = std.mem.Allocator;
pub const Entry = struct { name: []const u8, method: u16, offset: usize, compressed: usize, size: usize, crc: u32 };
pub const Archive = struct {
    bytes: []const u8,
    entries: []const Entry,
    index: std.StringHashMapUnmanaged(usize),
    control: @import("contract.zig").Control,
    max_bytes: usize,
    pub fn init(a: A, bytes: []const u8, max_bytes: usize) !Archive {
        return initWithControl(a, bytes, max_bytes, .{});
    }
    pub fn initWithControl(a: A, bytes: []const u8, max_bytes: usize, control: @import("contract.zig").Control) !Archive {
        try control.check();
        if (bytes.len > max_bytes or bytes.len < 22) return error.InvalidDocumentArchive;
        const end = std.mem.lastIndexOf(u8, bytes, "PK\x05\x06") orelse return error.InvalidDocumentArchive;
        if (end + 22 > bytes.len or end + 22 + u16At(bytes, end + 20) != bytes.len or
            u16At(bytes, end + 4) != 0 or u16At(bytes, end + 6) != 0 or
            u16At(bytes, end + 8) != u16At(bytes, end + 10)) return error.InvalidDocumentArchive;
        const count = u16At(bytes, end + 10);
        if (count == 0 or count > 16384) return error.DocumentArchiveLimitExceeded;
        const offset = u32At(bytes, end + 16);
        const size = u32At(bytes, end + 12);
        if (offset > end or size != end - offset) return error.InvalidDocumentArchive;
        const entries = try a.alloc(Entry, count);
        var index = std.StringHashMapUnmanaged(usize){};
        var cursor: usize = offset;
        var total: usize = 0;
        for (entries, 0..) |*entry, i| {
            try control.check();
            if (cursor > end or end - cursor < 46 or !std.mem.eql(u8, bytes[cursor..][0..4], "PK\x01\x02")) return error.InvalidDocumentArchive;
            if (u16At(bytes, cursor + 8) & 1 != 0) return error.EncryptedDocumentUnsupported;
            const name_len = u16At(bytes, cursor + 28);
            const length = @as(usize, 46) + name_len + u16At(bytes, cursor + 30) + u16At(bytes, cursor + 32);
            if (length > end - cursor) return error.InvalidDocumentArchive;
            const name = bytes[cursor + 46 ..][0..name_len];
            if (name.len == 0 or name[0] == '/' or std.mem.indexOfScalar(u8, name, '\\') != null or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidDocumentArchive;
            var parts = std.mem.splitScalar(u8, name, '/');
            while (parts.next()) |part| if (std.mem.eql(u8, part, "..") or std.mem.eql(u8, part, ".")) return error.InvalidDocumentArchive;
            const seen = try index.getOrPut(a, name);
            if (seen.found_existing) return error.InvalidDocumentArchive;
            seen.value_ptr.* = i;
            entry.* = .{ .name = name, .method = u16At(bytes, cursor + 10), .crc = u32At(bytes, cursor + 16), .compressed = u32At(bytes, cursor + 20), .size = u32At(bytes, cursor + 24), .offset = u32At(bytes, cursor + 42) };
            if (entry.size > max_bytes -| total) return error.DocumentArchiveLimitExceeded;
            total += entry.size;
            cursor += length;
        }
        if (cursor != end) return error.InvalidDocumentArchive;
        return .{ .bytes = bytes, .entries = entries, .index = index, .control = control, .max_bytes = max_bytes };
    }
    pub fn get(self: Archive, a: A, name: []const u8) ![]const u8 {
        try self.control.check();
        if (self.index.get(name)) |index| {
            const entry = self.entries[index];
            const o = entry.offset;
            if (o > self.bytes.len or self.bytes.len - o < 30 or !std.mem.eql(u8, self.bytes[o..][0..4], "PK\x03\x04")) return error.InvalidDocumentArchive;
            const start = o + 30 + @as(usize, u16At(self.bytes, o + 26)) + u16At(self.bytes, o + 28);
            if (start > self.bytes.len or entry.compressed > self.bytes.len - start) return error.InvalidDocumentArchive;
            const compressed = self.bytes[start..][0..entry.compressed];
            const data = switch (entry.method) {
                0 => compressed,
                8 => blk: {
                    const out = try a.alloc(u8, entry.size);
                    var input: std.Io.Reader = .fixed(compressed);
                    var window: [std.compress.flate.max_window_len]u8 = undefined;
                    var decoder: std.compress.flate.Decompress = .init(&input, .raw, &window);
                    var written: usize = 0;
                    while (written < out.len) {
                        try self.control.check();
                        const end = @min(out.len, written + 65536);
                        try decoder.reader.readSliceAll(out[written..end]);
                        written = end;
                    }
                    var extra: [1]u8 = undefined;
                    if (try decoder.reader.readSliceShort(&extra) != 0) return error.InvalidDocumentArchive;
                    break :blk out;
                },
                else => return error.UnsupportedDocumentCompression,
            };
            if (data.len != entry.size or std.hash.Crc32.hash(data) != entry.crc) return error.InvalidDocumentArchive;
            return data;
        }
        return error.MissingDocumentPart;
    }
};
fn u16At(b: []const u8, o: usize) u16 {
    return std.mem.readInt(u16, b[o..][0..2], .little);
}
fn u32At(b: []const u8, o: usize) u32 {
    return std.mem.readInt(u32, b[o..][0..4], .little);
}

pub const Tag = struct {
    name: []const u8,
    raw: []const u8,
    closing: bool,
    empty: bool,
    pub fn local(self: Tag) []const u8 {
        return if (std.mem.indexOfScalar(u8, self.name, ':')) |i| self.name[i + 1 ..] else self.name;
    }
    /// Visibility metadata must not silently use the first of contradictory
    /// attributes or treat malformed attributes as an absent/default flag.
    fn uniqueAttr(self: Tag, wanted: []const u8) !?[]const u8 {
        var result: ?[]const u8 = null;
        var attrs = Attributes{ .raw = self.raw, .index = self.name.len };
        while (try attrs.next()) |attr_value| if (std.mem.eql(u8, attr_value.name, wanted)) {
            if (result != null) return error.InvalidDocumentXml;
            result = attr_value.value;
        };
        return result;
    }
    /// XML boolean attributes share entity decoding, whitespace handling and
    /// duplicate detection, including default-true visibility properties.
    pub fn booleanAttr(self: Tag, name: []const u8, default: bool) !bool {
        return booleanValue((try self.uniqueAttr(name)) orelse return default);
    }
    pub fn attr(self: Tag, wanted: []const u8) ?[]const u8 {
        var i = self.name.len;
        while (i < self.raw.len) {
            while (i < self.raw.len and std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            const start = i;
            while (i < self.raw.len and self.raw[i] != '=' and !std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            const key = self.raw[start..i];
            while (i < self.raw.len and std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            if (i == self.raw.len or self.raw[i] != '=') return null;
            i += 1;
            while (i < self.raw.len and std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            if (i == self.raw.len or (self.raw[i] != '\'' and self.raw[i] != '"')) return null;
            const quote = self.raw[i];
            i += 1;
            const value_start = i;
            while (i < self.raw.len and self.raw[i] != quote) : (i += 1) {}
            if (i == self.raw.len) return null;
            const value = self.raw[value_start..i];
            i += 1;
            if (std.mem.eql(u8, key, wanted)) return value;
        }
        return null;
    }
};
const Attributes = struct {
    raw: []const u8,
    index: usize,
    fn next(self: *@This()) !?struct { name: []const u8, value: []const u8 } {
        const raw = self.raw;
        var i = self.index;
        if (i == raw.len) return null;
        if (!std.ascii.isWhitespace(raw[i])) return error.InvalidDocumentXml;
        while (i < raw.len and std.ascii.isWhitespace(raw[i])) : (i += 1) {}
        if (i == raw.len) return null;
        const start = i;
        while (i < raw.len and raw[i] != '=' and !std.ascii.isWhitespace(raw[i])) : (i += 1) {}
        const name = raw[start..i];
        while (i < raw.len and std.ascii.isWhitespace(raw[i])) : (i += 1) {}
        if (name.len == 0 or i == raw.len or raw[i] != '=') return error.InvalidDocumentXml;
        i += 1;
        while (i < raw.len and std.ascii.isWhitespace(raw[i])) : (i += 1) {}
        if (i == raw.len or (raw[i] != '\'' and raw[i] != '"')) return error.InvalidDocumentXml;
        const quote = raw[i];
        i += 1;
        const value_start = i;
        while (i < raw.len and raw[i] != quote) : (i += 1) {}
        if (i == raw.len) return error.InvalidDocumentXml;
        self.index = i + 1;
        return .{ .name = name, .value = raw[value_start..i] };
    }
};

pub const Token = union(enum) { tag: Tag, text: []const u8 };
pub const Xml = struct {
    bytes: []const u8,
    index: usize = 0,
    stack: [256][]const u8 = undefined,
    depth: usize = 0,
    pub fn next(self: *Xml) !?Token {
        while (true) {
            if (self.index == self.bytes.len) {
                if (self.depth != 0) return error.InvalidDocumentXml;
                return null;
            }
            const start = self.index;
            if (self.bytes[start] != '<') {
                self.index = std.mem.indexOfScalarPos(u8, self.bytes, start, '<') orelse self.bytes.len;
                return .{ .text = self.bytes[start..self.index] };
            }
            if (std.mem.startsWith(u8, self.bytes[start..], "<!--")) {
                self.index = (std.mem.indexOfPos(u8, self.bytes, start + 4, "-->") orelse return error.InvalidDocumentXml) + 3;
                continue;
            }
            if (std.mem.startsWith(u8, self.bytes[start..], "<!")) return error.UnsupportedDocumentXml;
            var i = start + 1;
            var quote: ?u8 = null;
            while (i < self.bytes.len) : (i += 1) {
                const b = self.bytes[i];
                if (quote) |q| {
                    if (b == q) quote = null;
                } else if (b == '"' or b == '\'') {
                    quote = b;
                } else if (b == '>') break;
            }
            if (i == self.bytes.len) return error.InvalidDocumentXml;
            self.index = i + 1;
            var raw = std.mem.trim(u8, self.bytes[start + 1 .. i], " \r\n\t");
            if (raw.len == 0) return error.InvalidDocumentXml;
            if (raw[0] == '?') continue;
            const closing = raw[0] == '/';
            if (closing) raw = raw[1..];
            const empty = std.mem.endsWith(u8, raw, "/");
            if (empty) raw = raw[0 .. raw.len - 1];
            const end = std.mem.indexOfAny(u8, raw, " \r\n\t") orelse raw.len;
            const name = raw[0..end];
            if (name.len == 0) return error.InvalidDocumentXml;
            if (closing) {
                if (empty or self.depth == 0 or !std.mem.eql(u8, self.stack[self.depth - 1], name)) return error.InvalidDocumentXml;
                self.depth -= 1;
            } else if (!empty) {
                if (self.depth == self.stack.len) return error.InvalidDocumentXml;
                self.stack[self.depth] = name;
                self.depth += 1;
            }
            return .{ .tag = .{ .name = name, .raw = raw, .closing = closing, .empty = empty } };
        }
    }
};

/// All public consumers share one compatibility- and visibility-checked view.
/// Unchanged parts borrow bytes; one projection pass copies retained spans once.
pub const View = struct {
    bytes: []const u8,
    part: Part = .fragment,
    unsupported_content: bool = false,
    pub const Part = enum { fragment, slide, notes, layout, master };
    pub fn init(a: A, bytes: []const u8) !View {
        return initWithControl(a, bytes, .{});
    }
    pub fn initWithControl(a: A, bytes: []const u8, control: @import("contract.zig").Control) !View {
        return initMode(a, bytes, control, .part);
    }
    fn initMode(a: A, bytes: []const u8, control: @import("contract.zig").Control, mode: ProjectionMode) !View {
        return projectBytes(a, bytes, control, mode);
    }

    pub fn requirePart(self: View, part: Part) !void {
        if (self.part != part) return error.InvalidDocumentXml;
    }
    /// Only for content assembled from this already-checked part. Preserve its
    /// unsupported-content finding without reparsing fragments out of scope.
    pub fn fragment(self: View, bytes: []const u8) View {
        return .{ .bytes = bytes, .unsupported_content = self.unsupported_content };
    }
    pub fn text(self: View, a: A) ![]const u8 {
        return extractText(a, self.bytes);
    }
    pub fn tables(self: View, a: A, limit: usize) ![]const @import("contract.zig").Table {
        return extractTablesWithLimit(a, self.bytes, limit);
    }
    pub fn needsVisualInspection(self: View) !bool {
        return self.unsupported_content or try requiresVisualInspection(self.bytes);
    }
};
fn isDrawing(local: []const u8) bool {
    for ([_][]const u8{ "sp", "pic", "graphicFrame", "cxnSp", "grpSp", "spTree", "contentPart" }) |name| {
        if (std.mem.eql(u8, local, name)) return true;
    }
    return false;
}
fn booleanValue(raw: []const u8) !bool {
    var token: [5]u8 = undefined;
    var length: usize = 0;
    var trailing_space = false;
    var i: usize = 0;
    while (i < raw.len) {
        const cp: u21 = if (raw[i] == '&') entity: {
            const end = std.mem.indexOfScalarPos(u8, raw, i, ';') orelse return error.InvalidDocumentXml;
            const cp = try entityScalar(raw[i + 1 .. end]);
            i = end + 1;
            break :entity cp;
        } else char: {
            const cp = raw[i];
            i += 1;
            break :char cp;
        };
        if (cp < 128 and std.ascii.isWhitespace(@intCast(cp))) {
            trailing_space = length > 0;
            continue;
        }
        if (trailing_space or cp >= 128 or length == token.len) return error.InvalidDocumentXml;
        token[length] = @intCast(cp);
        length += 1;
    }
    const value = token[0..length];
    if (std.mem.eql(u8, value, "1") or std.mem.eql(u8, value, "true")) return true;
    if (std.mem.eql(u8, value, "0") or std.mem.eql(u8, value, "false")) return false;
    return error.InvalidDocumentXml;
}

const mc_namespace = "http://schemas.openxmlformats.org/markup-compatibility/2006";
const pml_namespace = "http://schemas.openxmlformats.org/presentationml/2006/main";
const dml_namespace = "http://schemas.openxmlformats.org/drawingml/2006/main";
const table_namespace = "http://schemas.openxmlformats.org/drawingml/2006/table";
fn isTableUri(raw: []const u8) !bool {
    return try decodedEqual(raw, table_namespace) or try decodedEqual(raw, "http://purl.oclc.org/ooxml/drawingml/table");
}
const Namespace = enum { presentation, drawing, compatibility, other, fragment };
const ProjectionMode = enum { part, fragment };
fn namespaceKind(uri: []const u8) !Namespace {
    if (try decodedEqual(uri, pml_namespace) or try decodedEqual(uri, "http://purl.oclc.org/ooxml/presentationml/main")) return .presentation;
    if (try decodedEqual(uri, dml_namespace) or try decodedEqual(uri, "http://purl.oclc.org/ooxml/drawingml/main")) return .drawing;
    if (try decodedEqual(uri, mc_namespace)) return .compatibility;
    return .other;
}
const Namespaces = struct {
    const Binding = struct { prefix: []const u8, previous: ?usize, kind: Namespace, understood: bool };
    bindings: std.ArrayListUnmanaged(Binding) = .empty,
    index: std.StringHashMapUnmanaged(usize) = .{},
    starts: [258]usize = undefined,
    fn deinit(self: *@This(), a: A) void {
        self.bindings.deinit(a);
        self.index.deinit(a);
    }
    fn open(self: *@This(), a: A, tag: Tag, depth: usize) !void {
        self.starts[depth] = self.bindings.items.len;
        var attrs = Attributes{ .raw = tag.raw, .index = tag.name.len };
        while (try attrs.next()) |attribute| {
            const prefix = if (std.mem.eql(u8, attribute.name, "xmlns")) "" else if (std.mem.startsWith(u8, attribute.name, "xmlns:")) attribute.name[6..] else continue;
            const previous = self.index.get(prefix);
            if (previous) |prior| if (prior >= self.starts[depth]) return error.InvalidDocumentXml;
            const binding_kind = try namespaceKind(attribute.value);
            var understood = binding_kind == .presentation or binding_kind == .drawing;
            for ([_][]const u8{
                "http://schemas.openxmlformats.org/presentationml/2006/main",
                "http://schemas.openxmlformats.org/drawingml/2006/main",
                "http://schemas.openxmlformats.org/drawingml/2006/table",
                "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
            }) |namespace| understood = understood or try decodedEqual(attribute.value, namespace);
            const binding = Binding{ .prefix = prefix, .previous = previous, .kind = binding_kind, .understood = understood };
            const position = self.bindings.items.len;
            try self.bindings.append(a, binding);
            try self.index.put(a, prefix, position);
        }
    }
    fn close(self: *@This(), depth: usize) void {
        while (self.bindings.items.len > self.starts[depth]) {
            const binding = self.bindings.pop().?;
            if (binding.previous) |previous| {
                self.index.getPtr(binding.prefix).?.* = previous;
            } else {
                const removed = self.index.remove(binding.prefix);
                std.debug.assert(removed);
            }
        }
    }
    fn lookup(self: *const @This(), prefix: []const u8) ?Binding {
        return self.bindings.items[self.index.get(prefix) orelse return null];
    }
    fn kind(self: *const @This(), tag: Tag, mode: ProjectionMode) Namespace {
        const colon = std.mem.indexOfScalar(u8, tag.name, ':');
        const prefix = if (colon) |i| tag.name[0..i] else "";
        return if (self.lookup(prefix)) |binding| binding.kind else if (mode == .fragment) .fragment else .other;
    }
    fn isMc(self: *const @This(), tag: Tag) bool {
        return self.kind(tag, .part) == .compatibility;
    }
    fn supported(self: *const @This(), requires: []const u8) !bool {
        var prefixes = std.mem.tokenizeAny(u8, requires, " \t\r\n");
        var found = false;
        var all_known = true;
        while (prefixes.next()) |prefix| {
            found = true;
            const binding = self.lookup(prefix) orelse return error.InvalidDocumentXml;
            all_known = all_known and binding.understood;
        }
        if (!found) return error.InvalidDocumentXml;
        return all_known;
    }
};
fn decodedEqual(raw: []const u8, expected: []const u8) !bool {
    var i: usize = 0;
    var j: usize = 0;
    var equal = true;
    while (i < raw.len) {
        const cp: u21 = if (raw[i] == '&') entity: {
            const end = std.mem.indexOfScalarPos(u8, raw, i, ';') orelse return error.InvalidDocumentXml;
            const value = try entityScalar(raw[i + 1 .. end]);
            i = end + 1;
            break :entity value;
        } else char: {
            const value = raw[i];
            i += 1;
            break :char value;
        };
        equal = equal and j < expected.len and cp == expected[j];
        j += 1;
    }
    return equal and j == expected.len;
}

// Signal names consumed by downstream native text, table and visual readers
// must have the right vocabulary. Ordinary formatting tags can remain opaque
// core markup; they never acquire source/evidence meaning from a local name.
const presentation_signals = std.StaticStringMap(void).initComptime(.{
    .{ "sp", {} },          .{ "pic", {} },   .{ "graphicFrame", {} }, .{ "cxnSp", {} }, .{ "grpSp", {} },  .{ "spTree", {} },
    .{ "contentPart", {} }, .{ "cNvPr", {} }, .{ "cNvSpPr", {} },      .{ "ph", {} },    .{ "oleObj", {} }, .{ "bg", {} },
});
const drawing_signals = std.StaticStringMap(void).initComptime(.{
    .{ "t", {} },           .{ "p", {} },        .{ "r", {} },    .{ "fld", {} },     .{ "br", {} },       .{ "tbl", {} },       .{ "tr", {} },       .{ "tc", {} },
    .{ "graphicData", {} }, .{ "custGeom", {} }, .{ "sp3d", {} }, .{ "scene3d", {} }, .{ "prstGeom", {} }, .{ "solidFill", {} }, .{ "gradFill", {} }, .{ "pattFill", {} },
});

/// Namespace and content projection precedes every native consumer. Extension
/// payloads are opaque even when they reuse core names. Native text lives in
/// shape/cell text bodies, and tables live in their graphic-data vocabulary.
/// Retained MC wrappers preserve scopes and make the projection idempotent.
fn projectBytes(a: A, bytes: []const u8, control: @import("contract.zig").Control, mode: ProjectionMode) !View {
    var buffer: [4096]u8 = undefined;
    var first = std.heap.BufferFirstAllocator.init(&buffer, a);
    const scratch = first.allocator();
    var ns = Namespaces{};
    defer ns.deinit(scratch);
    const Kind = enum { ordinary, alternate, choice, fallback };
    const Role = enum { other, shape, body, paragraph, run, table_data, table, row, cell };
    const Frame = struct {
        kind: Kind = .ordinary,
        role: Role = .other,
        owner: bool = false,
        drawing_owner: usize = 0,
        logical_depth: usize = 0,
        start: usize = 0,
        output_start: usize = 0,
        hidden: bool = false,
        visibility_seen: bool = false,
        unsupported_before: bool = false,
        selected: bool = false,
        fallback_seen: bool = false,
    };
    var frames: [258]Frame = @splat(.{});
    var iter = Xml{ .bytes = bytes };
    var output = @import("output.zig").Writer.init(a, bytes.len);
    defer output.deinit();
    var cursor: usize = 0;
    const Skip = struct { depth: usize, start: usize, whole: bool };
    var skip: ?Skip = null;
    var result = View{ .bytes = bytes };
    var root_seen = false;
    var changed = false;
    while (try iter.next()) |token| {
        try control.check();
        if (token != .tag) continue;
        const tag = token.tag;
        const depth = iter.depth + @intFromBool(tag.closing or tag.empty);
        if (skip) |excluded| {
            if (tag.closing and depth == excluded.depth) {
                const raw_offset = @intFromPtr(tag.raw.ptr) - @intFromPtr(bytes.ptr);
                const end = if (excluded.whole) iter.index else std.mem.lastIndexOfScalar(u8, bytes[0..raw_offset], '<') orelse return error.InvalidDocumentXml;
                if (end > excluded.start) {
                    try output.raw(bytes[cursor..excluded.start]);
                    cursor = end;
                    changed = true;
                }
                skip = null;
                ns.close(depth);
            }
            continue;
        }
        if (tag.closing) {
            const frame = frames[depth];
            if (frame.drawing_owner == depth and frame.hidden) {
                if (cursor <= frame.start) try output.raw(bytes[cursor..frame.start]);
                std.debug.assert(frame.output_start <= output.len());
                output.writer.end = frame.output_start;
                cursor = iter.index;
                changed = true;
                result.unsupported_content = frame.unsupported_before;
            }
            ns.close(depth);
            continue;
        }
        try ns.open(scratch, tag, depth);
        defer if (tag.empty) ns.close(depth);
        const local = tag.local();
        const vocabulary = ns.kind(tag, mode);
        const parent = &frames[depth - 1];
        frames[depth] = .{
            .owner = parent.owner,
            .drawing_owner = parent.drawing_owner,
            .logical_depth = parent.logical_depth + 1,
            .unsupported_before = result.unsupported_content,
        };
        const frame = &frames[depth];
        if (depth == 1 and mode == .part) {
            if (root_seen or vocabulary != .presentation) return error.InvalidDocumentXml;
            root_seen = true;
            result.part = if (std.mem.eql(u8, local, "sld")) .slide else if (std.mem.eql(u8, local, "notes")) .notes else if (std.mem.eql(u8, local, "sldLayout")) .layout else if (std.mem.eql(u8, local, "sldMaster")) .master else return error.InvalidDocumentXml;
        }
        const mc = vocabulary == .compatibility;
        if (mc and std.mem.eql(u8, local, "AlternateContent")) {
            if (tag.empty) return error.InvalidDocumentXml;
            frame.kind = .alternate;
            frame.logical_depth = parent.logical_depth;
            frame.role = parent.role;
            continue;
        }
        if (mc and (std.mem.eql(u8, local, "Choice") or std.mem.eql(u8, local, "Fallback"))) {
            if (depth < 2 or parent.kind != .alternate or parent.fallback_seen) return error.InvalidDocumentXml;
            frame.role = parent.role;
            frame.logical_depth = parent.logical_depth;
            const choice = std.mem.eql(u8, local, "Choice");
            var selected = false;
            if (choice) {
                const raw = (try tag.uniqueAttr("Requires")) orelse return error.InvalidDocumentXml;
                const encoded = std.mem.indexOfScalar(u8, raw, '&') != null;
                const decoded = if (encoded) try scratch.alloc(u8, raw.len) else null;
                defer if (decoded) |allocation| scratch.free(allocation);
                var writer = std.Io.Writer.fixed(decoded orelse &.{});
                if (encoded) try unescape(&writer, raw);
                const supported = try ns.supported(if (encoded) writer.buffer[0..writer.end] else raw);
                selected = !parent.selected and supported;
                frame.kind = .choice;
            } else {
                parent.fallback_seen = true;
                selected = !parent.selected;
                frame.kind = .fallback;
            }
            parent.selected = parent.selected or selected;
            if (!selected and !tag.empty) skip = .{ .depth = depth, .start = iter.index, .whole = false };
            continue;
        }
        if (parent.kind == .alternate) return error.InvalidDocumentXml;

        const core = vocabulary == .presentation or vocabulary == .drawing;
        const is_metadata = core and (std.mem.eql(u8, local, "extLst") or std.mem.eql(u8, local, "ext") or std.mem.eql(u8, local, "custDataLst"));
        var excluded = is_metadata or vocabulary == .other or vocabulary == .compatibility;
        if (mode == .part and !excluded) {
            if (presentation_signals.has(local) and vocabulary != .presentation) excluded = true;
            if (drawing_signals.has(local) and vocabulary != .drawing) excluded = true;
            if (std.mem.eql(u8, local, "sp") and vocabulary == .presentation) {
                frame.owner = true;
                frame.role = .shape;
            } else if (std.mem.eql(u8, local, "txBody")) {
                excluded = !parent.owner;
                frame.role = .body;
            } else if (std.mem.eql(u8, local, "p") and vocabulary == .drawing) {
                excluded = parent.role != .body;
                frame.role = .paragraph;
            } else if ((std.mem.eql(u8, local, "r") or std.mem.eql(u8, local, "fld")) and vocabulary == .drawing) {
                excluded = parent.role != .paragraph;
                frame.role = .run;
            } else if (std.mem.eql(u8, local, "t") and vocabulary == .drawing) {
                excluded = parent.role != .run;
            } else if (std.mem.eql(u8, local, "br") and vocabulary == .drawing) {
                excluded = parent.role != .paragraph;
            } else if (std.mem.eql(u8, local, "graphicData") and vocabulary == .drawing) {
                const uri = (try tag.uniqueAttr("uri")) orelse "";
                if (try isTableUri(uri)) frame.role = .table_data;
            } else if (std.mem.eql(u8, local, "tbl") and vocabulary == .drawing) {
                excluded = parent.role != .table_data;
                frame.role = .table;
            } else if (std.mem.eql(u8, local, "tr") and vocabulary == .drawing) {
                excluded = parent.role != .table;
                frame.role = .row;
            } else if (std.mem.eql(u8, local, "tc") and vocabulary == .drawing) {
                excluded = parent.role != .row;
                frame.owner = true;
                frame.role = .cell;
            }
        }
        if (excluded) {
            if (!is_metadata) result.unsupported_content = true;
            const raw_offset = @intFromPtr(tag.raw.ptr) - @intFromPtr(bytes.ptr);
            const start = std.mem.lastIndexOfScalar(u8, bytes[0..raw_offset], '<') orelse return error.InvalidDocumentXml;
            if (tag.empty) {
                try output.raw(bytes[cursor..start]);
                cursor = iter.index;
                changed = true;
            } else skip = .{ .depth = depth, .start = start, .whole = true };
            continue;
        }
        if (!tag.empty and isDrawing(local)) {
            const raw_offset = @intFromPtr(tag.raw.ptr) - @intFromPtr(bytes.ptr);
            const start = std.mem.lastIndexOfScalar(u8, bytes[0..raw_offset], '<') orelse return error.InvalidDocumentXml;
            frame.drawing_owner = depth;
            frame.start = start;
            frame.output_start = output.len() + start - cursor;
        }
        if (std.mem.eql(u8, local, "cNvPr") and frame.drawing_owner > 0) {
            const drawing = &frames[frame.drawing_owner];
            if (frame.logical_depth == drawing.logical_depth + 2) {
                if (drawing.visibility_seen) return error.InvalidDocumentXml;
                drawing.visibility_seen = true;
                drawing.hidden = try tag.booleanAttr("hidden", false);
            }
        }
    }
    if (mode == .part and !root_seen) return error.InvalidDocumentXml;
    if (changed) {
        try output.raw(bytes[cursor..]);
        result.bytes = try output.toOwnedSlice();
    }
    return result;
}

// Small isolated XML snippets exercise visibility and compatibility machinery
// without claiming to be valid source parts or namespace-qualified evidence.
fn fragmentView(a: A, bytes: []const u8) !View {
    return View.initMode(a, bytes, .{}, .fragment);
}
fn controlledFragmentView(a: A, bytes: []const u8, control: @import("contract.zig").Control) !View {
    return View.initMode(a, bytes, control, .fragment);
}

/// Native tables and text boxes have a structural projection. Every other
/// drawing requires a visual reader; inspect XML tokens, never prefix strings.
fn requiresVisualInspection(xml: []const u8) !bool {
    var iter = Xml{ .bytes = xml };
    var shape = false;
    var textbox = false;
    var geometry = false;
    var nonrect = false;
    while (try iter.next()) |token| {
        if (token != .tag) continue;
        const tag = token.tag;
        const local = tag.local();
        if (tag.closing) {
            if (std.mem.eql(u8, local, "sp")) {
                if (geometry and (!textbox or nonrect)) return true;
                shape = false;
            }
            continue;
        }
        for ([_][]const u8{ "pic", "cxnSp", "grpSp", "oleObj", "contentPart", "chart", "custGeom", "sp3d", "scene3d", "blipFill" }) |drawing| {
            if (std.mem.eql(u8, local, drawing)) return true;
        }
        if (std.mem.eql(u8, local, "graphicData")) {
            const uri = tag.attr("uri") orelse return true;
            if (!try isTableUri(uri)) return true;
        }
        if (std.mem.eql(u8, local, "sp")) {
            shape = true;
            textbox = false;
            geometry = false;
            nonrect = false;
        }
        if (shape and std.mem.eql(u8, local, "cNvSpPr")) {
            textbox = try tag.booleanAttr("txBox", false);
        }
        if (shape and (std.mem.eql(u8, local, "solidFill") or std.mem.eql(u8, local, "gradFill") or std.mem.eql(u8, local, "pattFill"))) geometry = true;
        if (shape and std.mem.eql(u8, local, "prstGeom")) {
            geometry = true;
            nonrect = !std.mem.eql(u8, tag.attr("prst") orelse "", "rect");
        }
        if (shape and std.mem.eql(u8, local, "ph")) {
            const kind = tag.attr("type") orelse "obj";
            if (std.mem.eql(u8, kind, "pic") or std.mem.eql(u8, kind, "chart") or std.mem.eql(u8, kind, "media")) return true;
        }
    }
    return false;
}

fn extractText(a: A, xml: []const u8) ![]const u8 {
    var iter = Xml{ .bytes = xml };
    var output = @import("output.zig").Writer.init(a, xml.len);
    defer output.deinit();
    var inside = false;
    while (try iter.next()) |token| switch (token) {
        .tag => |tag| {
            if (std.mem.eql(u8, tag.local(), "t")) inside = !tag.closing and !tag.empty;
            // DrawingML hard breaks separate runs within one paragraph. Emit
            // once at the opening tag for both empty and paired br elements.
            if (!tag.closing and std.mem.eql(u8, tag.local(), "br")) try output.raw("\n");
            if (tag.closing and (std.mem.eql(u8, tag.local(), "p") or std.mem.eql(u8, tag.local(), "tr"))) try output.raw("\n");
            if (tag.closing and std.mem.eql(u8, tag.local(), "tc")) try output.raw("\t");
        },
        .text => |value| if (inside) {
            unescape(&output.writer, value) catch |err| return output.failure orelse err;
        },
    };
    const result = try output.toOwnedSlice();
    errdefer a.free(result);
    if (!std.unicode.utf8ValidateSlice(result)) return error.InvalidDocumentXml;
    return result;
}
fn unescape(w: *std.Io.Writer, value: []const u8) !void {
    var i: usize = 0;
    while (i < value.len) {
        if (value[i] != '&') {
            try w.writeByte(value[i]);
            i += 1;
            continue;
        }
        const end = std.mem.indexOfScalarPos(u8, value, i, ';') orelse return error.InvalidDocumentXml;
        const entity = value[i + 1 .. end];
        const cp = try entityScalar(entity);
        var bytes: [4]u8 = undefined;
        const n = std.unicode.utf8Encode(cp, &bytes) catch return error.InvalidDocumentXml;
        try w.writeAll(bytes[0..n]);
        i = end + 1;
    }
}

fn entityScalar(entity: []const u8) !u21 {
    const entities = .{ .{ "amp", '&' }, .{ "lt", '<' }, .{ "gt", '>' }, .{ "quot", '"' }, .{ "apos", '\'' } };
    inline for (entities) |pair| if (std.mem.eql(u8, entity, pair[0])) return pair[1];
    if (!std.mem.startsWith(u8, entity, "#")) return error.InvalidDocumentXml;
    const hex = std.mem.startsWith(u8, entity, "#x");
    return std.fmt.parseInt(u21, entity[if (hex) @as(usize, 2) else 1..], if (hex) 16 else 10) catch error.InvalidDocumentXml;
}

test "document OOXML text preserves cells entities and paragraph boundaries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const result = try extractText(arena.allocator(), "<a:tr><a:tc><a:p><a:r><a:t>A &amp; B</a:t></a:r></a:p></a:tc><a:tc><a:p><a:r><a:t>1&#x32;</a:t></a:r></a:p></a:tc></a:tr>");
    try std.testing.expectEqualStrings("A & B\n\t12\n\t\n", result);
    try std.testing.expectError(error.UnsupportedDocumentXml, extractText(arena.allocator(), "<!DOCTYPE x><t>bad</t>"));
}
test "document XML attributes tolerate quoted greater-than and reordered attributes" {
    var iter = Xml{ .bytes = "<Relationship Target='slides/slide2.xml' Id=\"rId2\" Type='a>b'/>" };
    const tag = (try iter.next()).?.tag;
    try std.testing.expectEqualStrings("rId2", tag.attr("Id").?);
    try std.testing.expectEqualStrings("a>b", tag.attr("Type").?);
}

/// Native DrawingML tables retain empty cells and span declarations. Cell
/// geometry remains unknown unless supplied by a renderer; never invent boxes.
fn extractTables(a: A, xml: []const u8) ![]const @import("contract.zig").Table {
    return extractTablesWithLimit(a, xml, 65536);
}
fn extractTablesWithLimit(a: A, xml: []const u8, limit: usize) ![]const @import("contract.zig").Table {
    const c = @import("contract.zig");
    var result = std.ArrayList(c.Table).empty;
    var cells = std.ArrayList(c.Cell).empty;
    var iter = Xml{ .bytes = xml };
    var active = false;
    var row: usize = 0;
    var column: usize = 0;
    var columns: usize = 0;
    var cell_start: ?usize = null;
    var row_span: usize = 1;
    var column_span: usize = 1;
    var total_cells: usize = 0;
    while (true) {
        const before = iter.index;
        const token = (try iter.next()) orelse break;
        if (token != .tag) continue;
        const tag = token.tag;
        const local = tag.local();
        if (std.mem.eql(u8, local, "tbl")) {
            if (!tag.closing) {
                if (active) return error.InvalidDocumentTable;
                if (result.items.len >= @min(limit, 4096)) return error.DocumentRegionLimitExceeded;
                active = true;
                row = 0;
                columns = 0;
                cells = .empty;
            } else {
                if (!active or cell_start != null) return error.InvalidDocumentTable;
                const table = c.Table{ .id = result.items.len, .rows = row, .columns = columns, .cells = try cells.toOwnedSlice(a) };
                try table.validate();
                try result.append(a, table);
                active = false;
            }
        } else if (active and std.mem.eql(u8, local, "tr")) {
            if (tag.closing) {
                columns = @max(columns, column);
                row += 1;
            } else column = 0;
        } else if (active and std.mem.eql(u8, local, "tc")) {
            if (tag.closing) {
                const start = cell_start orelse return error.InvalidDocumentTable;
                if (total_cells >= limit) return error.DocumentRegionLimitExceeded;
                total_cells += 1;
                try cells.append(a, .{ .row = row, .column = column, .row_span = row_span, .column_span = column_span, .text = try extractText(a, xml[start..before]) });
                column += 1;
                cell_start = null;
            } else {
                if (cell_start != null or tag.empty) return error.InvalidDocumentTable;
                cell_start = iter.index;
                row_span = if (tag.attr("rowSpan")) |n| try std.fmt.parseUnsigned(usize, n, 10) else 1;
                column_span = if (tag.attr("gridSpan")) |n| try std.fmt.parseUnsigned(usize, n, 10) else 1;
                if (row_span == 0 or column_span == 0 or row_span > 65536 or column_span > 65536) return error.InvalidDocumentTable;
            }
        }
    }
    if (active) return error.InvalidDocumentTable;
    return result.toOwnedSlice(a);
}

test "document native slide tables retain blank cells and merge spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const result = try extractTables(arena.allocator(), "<a:tbl><a:tr><a:tc gridSpan='2'><a:p><a:t>Total</a:t></a:p></a:tc><a:tc><a:p/></a:tc></a:tr></a:tbl>");
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(usize, 2), result[0].columns);
    try std.testing.expectEqual(@as(usize, 2), result[0].cells[0].column_span);
    try std.testing.expectEqualStrings("", result[0].cells[1].text);
}

test "document XML rejects mismatched and unclosed text rather than returning partial native content" {
    try std.testing.expectError(error.InvalidDocumentXml, extractText(std.testing.allocator, "<p:sld><a:t>partial"));
    try std.testing.expectError(error.InvalidDocumentXml, extractText(std.testing.allocator, "<p:sld><a:t>partial</p:sld>"));
    try std.testing.expectError(error.InvalidDocumentXml, extractText(std.testing.allocator, "<p:sld><a:t>\xff</a:t></p:sld>"));
}

test "document slide coverage detects vector drawings independently of XML prefixes" {
    try std.testing.expect(try requiresVisualInspection("<x:sld><x:sp><d:prstGeom prst='ellipse'/></x:sp></x:sld>"));
    try std.testing.expect(try requiresVisualInspection("<sld><sp><prstGeom prst='rect'/></sp></sld>"));
    try std.testing.expect(try requiresVisualInspection("<sld><pic/></sld>"));
    try std.testing.expect(try requiresVisualInspection("<sld><graphicData uri='chart'/></sld>"));
    try std.testing.expect(!try requiresVisualInspection("<sld><sp><cNvSpPr txBox='1'/><prstGeom prst='rect'/><txBody><t>Native</t></txBody></sp></sld>"));
    try std.testing.expect(!try requiresVisualInspection("<sld><graphicData uri='http://schemas.openxmlformats.org/drawingml/2006/table'><tbl/></graphicData></sld>"));
}

test "document native text preserves hard breaks in runs and table cells" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const xml = "<a:p><a:r><a:t>12</a:t></a:r><a:br/><a:r><a:t>34</a:t></a:r><a:br><a:rPr lang='en-US'/></a:br><a:br/><a:r><a:t>Éva &amp; 李</a:t></a:r></a:p>";
    try std.testing.expectEqualStrings("12\n34\n\nÉva & 李\n", try extractText(a, xml));
    // Run boundaries alone are not line breaks.
    try std.testing.expectEqualStrings("1234\n", try extractText(a, "<p><r><t>12</t></r><r><t>34</t></r></p>"));
    const table_xml = "<tbl><tr><tc><p><r><t>12</t></r><br/><r><t>34</t></r></p></tc></tr></tbl>";
    const table = try extractTables(a, table_xml);
    try std.testing.expectEqualStrings("12\n34\n", table[0].cells[0].text);
    try std.testing.expectEqualStrings("12\n34\n\t\n", try extractText(a, table_xml));
}

test "document slide table limits are shared across cells and checked during parsing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const xml = "<a:tbl><a:tr><a:tc><a:t>One</a:t></a:tc><a:tc><a:t>Two</a:t></a:tc></a:tr></a:tbl>" ++
        "<a:tbl><a:tr><a:tc><a:t>Three</a:t></a:tc><a:tc><a:t>Four</a:t></a:tc></a:tr></a:tbl>";
    try std.testing.expectError(error.DocumentRegionLimitExceeded, extractTablesWithLimit(a, xml, 3));
    const valid = try extractTablesWithLimit(a, xml, 4);
    try std.testing.expectEqual(@as(usize, 2), valid.len);
    try std.testing.expectEqualStrings("Four", valid[1].cells[1].text);
    try std.testing.expectError(error.DocumentRegionLimitExceeded, extractTablesWithLimit(a, "<a:tbl><a:tr><a:tc><a:p/></a:tc></a:tr></a:tbl><a:tbl><a:tr><a:tc><a:p/></a:tc></a:tr></a:tbl>", 1));
}

test "document visibility borrows visible XML and filters hidden owners consistently" {
    const visible = "<p:sld><p:sp><p:nvSpPr><p:cNvPr hidden='false'/></p:nvSpPr><a:p><a:t>Visible &amp; 李</a:t></a:p></p:sp></p:sld>";
    const borrowed = try fragmentView(std.testing.failing_allocator, visible);
    try std.testing.expectEqual(visible.ptr, borrowed.bytes.ptr);
    const encoded: []const u8 = "<sp><nvSpPr><cNvPr hidden='&#32;f&#97;lse&#32;'/></nvSpPr><t>Visible</t></sp>";
    try std.testing.expectEqual(encoded.ptr, (try fragmentView(std.testing.failing_allocator, encoded)).bytes.ptr);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "1", "true", " true ", "&#49;", "tr&#117;e", "&#x20;true&#32;" }) |hidden| {
        const bytes = try std.fmt.allocPrint(a, "<sld><sp><nvSpPr><cNvPr hidden='{s}'/></nvSpPr><t>Obsolete</t></sp><graphicFrame><nvGraphicFramePr><cNvPr hidden='{s}'/></nvGraphicFramePr><tbl><tr><tc><t>999.99</t></tc></tr></tbl></graphicFrame><pic><nvPicPr><cNvPr hidden='{s}'/></nvPicPr><blipFill/></pic><sp><nvSpPr><cNvPr hidden='0'/></nvSpPr><p><t>Visible &amp; 李</t></p></sp></sld>", .{ hidden, hidden, hidden });
        const view = try fragmentView(a, bytes);
        try std.testing.expectEqualStrings("Visible & 李\n", try view.text(a));
        try std.testing.expectEqual(@as(usize, 0), (try view.tables(a, 1)).len);
        try std.testing.expect(!try view.needsVisualInspection());
    }
    // Identical token payloads outside a drawing's nonvisual wrapper are not
    // properties of that drawing and cannot silently hide its visible text.
    const unrelated = try fragmentView(a, "<sp><ext><other><cNvPr hidden='1'/></other></ext><t>Keep</t></sp>");
    try std.testing.expectEqualStrings("Keep", try unrelated.text(a));
}

test "document visibility handles nested groups without copying hidden descendants back" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const xml = "<sld><sp><t>Before</t></sp><grpSp><nvGrpSpPr><cNvPr hidden='1'/></nvGrpSpPr><sp><nvSpPr><cNvPr hidden='false'/></nvSpPr><t>Hidden by parent</t></sp><grpSp><nvGrpSpPr><cNvPr hidden='true'/></nvGrpSpPr><sp><t>Hidden nested</t></sp></grpSp><sp><t>Hidden tail</t></sp></grpSp><sp><t>After</t></sp></sld>";
    const view = try fragmentView(a, xml);
    try std.testing.expectEqualStrings("BeforeAfter", try view.text(a));
    try std.testing.expect(!try view.needsVisualInspection());
    const visible_group = try fragmentView(a, "<grpSp><nvGrpSpPr><cNvPr hidden='0'/></nvGrpSpPr><sp><nvSpPr><cNvPr hidden='1'/></nvSpPr><t>Hidden</t></sp><sp><t>Visible</t></sp></grpSp>");
    try std.testing.expectEqualStrings("Visible", try visible_group.text(a));
    try std.testing.expect(try visible_group.needsVisualInspection());
}

test "document visibility validates malformed hidden XML and cleans up allocation failures" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "<sp><nvSpPr><cNvPr hidden='maybe'/></nvSpPr><t>Wrong</t></sp>",
        "<sp><nvSpPr><cNvPr hidden='0' hidden='1'/></nvSpPr><t>Wrong</t></sp>",
        "<sp><nvSpPr><cNvPr hidden='0'hidden='1'/></nvSpPr><t>Wrong</t></sp>",
        "<sp><nvSpPr><cNvPr hidden/></nvSpPr><t>Wrong</t></sp>",
        "<sp><nvSpPr><cNvPr hidden='1'/><cNvPr hidden='0'/></nvSpPr></sp>",
        "<sp><nvSpPr><cNvPr hidden='1'/></nvSpPr><t>Unclosed",
        "<grpSp><nvGrpSpPr><cNvPr hidden='1'/></nvGrpSpPr></sp>",
    }) |xml| try std.testing.expectError(error.InvalidDocumentXml, fragmentView(a, xml));
    const Case = struct {
        fn run(alloc: A) !void {
            const bytes: []const u8 = "<sld><sp><t>Before</t></sp><sp><nvSpPr><cNvPr hidden='true'/></nvSpPr><t>Hidden</t></sp><sp><t>After</t></sp></sld>";
            const view = try fragmentView(alloc, bytes);
            defer alloc.free(view.bytes);
            try std.testing.expect(std.mem.indexOf(u8, view.bytes, "Hidden") == null);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{});
}

test "document visibility validates hidden root groups and observes cancellation while filtering" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = "<sld><spTree><nvGrpSpPr><cNvPr hidden='true'/></nvGrpSpPr><sp><t>Hidden root</t></sp></spTree></sld>";
    const view = try fragmentView(a, bytes);
    try std.testing.expectEqualStrings("", try view.text(a));
    try std.testing.expect(!try view.needsVisualInspection());
    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks > 4) return error.Canceled;
        }
    };
    var cancelled = Cancel{};
    try std.testing.expectError(error.Canceled, controlledFragmentView(a, bytes, .{ .ptr = &cancelled, .check_fn = Cancel.check }));
}

const test_mc_root = "<sld xmlns:m='http://schemas.openxmlformats.org/markup-compatibility/2006' xmlns:a='http://schemas.openxmlformats.org/drawingml/2006/main' xmlns:future='urn:unsupported'>";
test "document alternate content selects one namespace supported branch for every projection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const xml = test_mc_root ++ "<m:AlternateContent><m:Choice Requires='future'><t>Wrong</t><pic/><tbl><tr><tc><t>999</t></tc></tr></tbl></m:Choice><m:Fallback><t>Visible</t><tbl><tr><tc><t>12.50</t></tc></tr></tbl></m:Fallback></m:AlternateContent></sld>";
    const view = try fragmentView(a, xml);
    try std.testing.expectEqualStrings("Visible12.50\t\n", try view.text(a));
    const tables = try view.tables(a, 10);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings("12.50", tables[0].cells[0].text);
    try std.testing.expect(!try view.needsVisualInspection());
    // Retained wrappers preserve bindings and the same selection on re-entry.
    const again = try fragmentView(std.testing.failing_allocator, view.bytes);
    try std.testing.expectEqual(view.bytes.ptr, again.bytes.ptr);
    const supported = try fragmentView(a, test_mc_root ++ "<m:AlternateContent><m:Choice Requires='a'><pic/><t>First</t></m:Choice><m:Choice Requires='a'><t>Second</t></m:Choice><m:Fallback><t>Fallback</t></m:Fallback></m:AlternateContent></sld>");
    try std.testing.expectEqualStrings("First", try supported.text(a));
    try std.testing.expect(try supported.needsVisualInspection());
}

test "document alternate content resolves scoped aliases and encoded requirements" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const view = try fragmentView(a, test_mc_root ++ "<m:AlternateContent><m:Choice xmlns:a='urn:rebound' Requires='a'><t>Wrong</t></m:Choice><m:Choice Requires='&#97;'><m:AlternateContent xmlns:x='http://schemas.openxmlformats.org/markup-compatibility/2006'><x:Choice Requires='future'><t>Wrong nested</t></x:Choice><x:Fallback><t>Nested</t></x:Fallback></m:AlternateContent></m:Choice><m:Fallback><t>Wrong fallback</t></m:Fallback></m:AlternateContent><m:AlternateContent><m:Choice Requires='a future'><t>Wrong partial</t></m:Choice><m:Fallback><t>Tail</t></m:Fallback></m:AlternateContent></sld>");
    try std.testing.expectEqualStrings("NestedTail", try view.text(a));
    // MC control wrappers do not change ownership of visibility properties.
    const hidden = try fragmentView(a, test_mc_root ++ "<sp><nvSpPr><m:AlternateContent><m:Choice Requires='a'><cNvPr hidden='true'/></m:Choice><m:Fallback><cNvPr hidden='false'/></m:Fallback></m:AlternateContent></nvSpPr><t>Hidden</t></sp></sld>");
    try std.testing.expectEqualStrings("", try hidden.text(a));
}

test "document alternate content rejects malformed controls and releases allocations" {
    for ([_][]const u8{
        "<m:AlternateContent><m:Choice><t>Missing</t></m:Choice></m:AlternateContent>",
        "<m:AlternateContent><m:Choice Requires='undefined'/></m:AlternateContent>",
        "<m:AlternateContent><m:Fallback/><m:Fallback/></m:AlternateContent>",
        "<m:AlternateContent><m:Fallback/><m:Choice Requires='a'/></m:AlternateContent>",
        "<m:Choice Requires='a'/>",
    }) |body| {
        const bytes = try std.mem.concat(std.testing.allocator, u8, &.{ test_mc_root, body, "</sld>" });
        defer std.testing.allocator.free(bytes);
        try std.testing.expectError(error.InvalidDocumentXml, fragmentView(std.testing.allocator, bytes));
    }
    const Case = struct {
        fn run(a: A) !void {
            const bytes = test_mc_root ++ "<m:AlternateContent><m:Choice Requires='future'><t>Wrong</t></m:Choice><m:Fallback><sp><nvSpPr><cNvPr hidden='true'/></nvSpPr><t>Hidden</t></sp><t>Visible</t></m:Fallback></m:AlternateContent></sld>";
            const view = try fragmentView(a, bytes);
            defer a.free(view.bytes);
            try std.testing.expect(std.mem.indexOf(u8, view.bytes, "Wrong") == null);
            try std.testing.expect(std.mem.indexOf(u8, view.bytes, "Hidden") == null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "document namespace indexing restores scoped bindings and cleans up large namespace sets" {
    var xml = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer xml.deinit();
    try xml.writer.writeAll("<sld xmlns:m='http://schemas.openxmlformats.org/markup-compatibility/2006'");
    for (0..160) |i| try xml.writer.print(" xmlns:n{d}='urn:unknown:{d}'", .{ i, i });
    try xml.writer.writeAll(">" ++
        "<m:AlternateContent><m:Choice Requires='n&#49;59'><t>Wrong</t></m:Choice><m:Fallback><t>Visible</t></m:Fallback></m:AlternateContent>" ++
        "<m:AlternateContent xmlns:n159='http://schemas.openxmlformats.org/drawingml/2006/main'><m:Choice Requires='n159'><t>Supported</t></m:Choice><m:Fallback><t>Wrong rebound</t></m:Fallback></m:AlternateContent>" ++
        "<m:AlternateContent><m:Choice Requires='n159'><t>Wrong restored</t></m:Choice><m:Fallback><t>Tail</t></m:Fallback></m:AlternateContent></sld>");
    const Case = struct {
        fn run(a: A, bytes: []const u8) !void {
            const view = try fragmentView(a, bytes);
            defer a.free(view.bytes);
            const text = try view.text(a);
            defer a.free(text);
            try std.testing.expectEqualStrings("VisibleSupportedTail", text);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{xml.written()});
}

test "document boolean attributes decode visibility consistently without allocation" {
    for ([_][]const u8{ "0", "false", "&#48;", "&#x30;", " f&#97;lse ", "&#32;false&#32;" }) |value| {
        var bytes: [256]u8 = undefined;
        const xml = try std.fmt.bufPrint(&bytes, "<sld show='{s}' showMasterSp='{s}'/>", .{ value, value });
        var iter = Xml{ .bytes = xml };
        const tag = (try iter.next()).?.tag;
        try std.testing.expect(!try tag.booleanAttr("show", true));
        try std.testing.expect(!try tag.booleanAttr("showMasterSp", true));
    }
    for ([_][]const u8{ "1", "true", "&#49;", "tr&#117;e", " true " }) |value| {
        var bytes: [128]u8 = undefined;
        const xml = try std.fmt.bufPrint(&bytes, "<cNvSpPr txBox='{s}'/>", .{value});
        var iter = Xml{ .bytes = xml };
        try std.testing.expect(try (try iter.next()).?.tag.booleanAttr("txBox", false));
    }
    var defaults = Xml{ .bytes = "<sld/>" };
    const tag = (try defaults.next()).?.tag;
    try std.testing.expect(try tag.booleanAttr("show", true));
    try std.testing.expect(!try tag.booleanAttr("hidden", false));
    for ([_][]const u8{ "<sld show='maybe'/>", "<sld show=''/>", "<sld show='0' show='1'/>", "<sld show='0'show='1'/>", "<sld show='f alse'/>" }) |invalid| {
        var iter = Xml{ .bytes = invalid };
        try std.testing.expectError(error.InvalidDocumentXml, (try iter.next()).?.tag.booleanAttr("show", true));
    }
    const textbox = try fragmentView(std.testing.failing_allocator, "<sp><cNvSpPr txBox='tr&#117;e'/><prstGeom prst='rect'/><t>Visible</t></sp>");
    try std.testing.expect(!try textbox.needsVisualInspection());
}

const test_part_root = "<p:sld xmlns:p='http://schemas.openxmlformats.org/presentationml/2006/main' xmlns:a='http://schemas.openxmlformats.org/drawingml/2006/main' xmlns:m='http://schemas.openxmlformats.org/markup-compatibility/2006' xmlns:x='urn:opaque'>";
const test_text_shape = "<p:sp><p:nvSpPr><p:cNvPr hidden='0'/><p:cNvSpPr txBox='1'/></p:nvSpPr><p:txBody><a:p><a:r><a:t>Visible 12.50</a:t></a:r></a:p></p:txBody></p:sp>";
test "document part projection excludes opaque metadata before text tables and visual inspection" {
    const bytes = test_part_root ++ test_text_shape ++
        "<p:extLst><p:ext uri='urn:opaque'><x:t>999.99</x:t><a:p><a:r><a:t>Wrong core text</a:t></a:r></a:p><a:tbl><a:tr><a:tc rowSpan='99'><a:t>Wrong table</a:t></a:tc></a:tr></a:tbl><p:pic/><p:cNvPr hidden='invalid'/></p:ext></p:extLst></p:sld>";
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const view = try View.init(a, bytes);
    try view.requirePart(.slide);
    try std.testing.expectEqualStrings("Visible 12.50\n", try view.text(a));
    try std.testing.expectEqual(@as(usize, 0), (try view.tables(a, 1)).len);
    try std.testing.expect(!try view.needsVisualInspection());
    try std.testing.expect(!view.unsupported_content);
    const again = try View.init(std.testing.failing_allocator, view.bytes);
    try std.testing.expectEqual(view.bytes.ptr, again.bytes.ptr);
    try std.testing.expect(!again.unsupported_content);
    const borrowed = try View.init(std.testing.failing_allocator, test_part_root ++ test_text_shape ++ "</p:sld>");
    try std.testing.expect(!try borrowed.needsVisualInspection());
}

test "document part projection resolves namespaces content scopes and compatibility choices" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const view = try View.init(a, test_part_root ++ "<p:sp><p:txBody><m:AlternateContent><m:Choice Requires='x'><a:p><a:r><a:t>Wrong</a:t></a:r></a:p></m:Choice><m:Fallback><a:p><a:r><a:t>Chosen</a:t></a:r></a:p></m:Fallback></m:AlternateContent></p:txBody></p:sp></p:sld>");
    try std.testing.expectEqualStrings("Chosen\n", try view.text(a));
    try std.testing.expect(!view.unsupported_content);
    const alias = try View.init(a, "<q:sld xmlns:q='http://schemas.openxmlformats.org/presentationml/2006/main' xmlns:d='http://schemas.openxmlformats.org/drawingml/2006/main'><q:sp><q:txBody><d:p><d:r><d:t>Aliased</d:t></d:r></d:p></q:txBody></q:sp></q:sld>");
    try std.testing.expectEqualStrings("Aliased\n", try alias.text(a));
    for ([_][]const u8{
        "<p:sp><p:txBody><a:p><a:r><x:t>Foreign text</x:t><a:t>Keep</a:t></a:r></a:p></p:txBody></p:sp>",
        "<p:sp><p:txBody><a:p><a:r><a:t xmlns:a='urn:rebound'>Wrong</a:t><a:t>Keep</a:t></a:r></a:p></p:txBody></p:sp>",
        "<p:sp><p:txBody><a:p><a:r><p:t>Wrong vocabulary</p:t><a:t>Keep</a:t></a:r></a:p></p:txBody></p:sp>",
        "<p:sp><p:nvSpPr><a:t>Wrong context</a:t></p:nvSpPr><p:txBody><a:p><a:r><a:t>Keep</a:t></a:r></a:p></p:txBody></p:sp>",
    }) |body| {
        const part = try View.init(a, try std.mem.concat(a, u8, &.{ test_part_root, body, "</p:sld>" }));
        try std.testing.expectEqualStrings("Keep\n", try part.text(a));
        try std.testing.expect(part.unsupported_content);
        try std.testing.expect(try part.needsVisualInspection());
    }
    try std.testing.expectError(error.InvalidDocumentXml, View.init(a, "<x:sld xmlns:x='urn:foreign'><x:t>Wrong root</x:t></x:sld>"));
}

test "document hidden groups discard unsupported findings without clearing visible findings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const hidden = "<p:grpSp><p:nvGrpSpPr><p:cNvPr hidden='1'/></p:nvGrpSpPr><x:t>Foreign hidden text</x:t><p:sp><p:nvSpPr><p:cNvPr hidden='1'/></p:nvSpPr><a:t>Wrong hidden context</a:t></p:sp><x:future/></p:grpSp>";
    const view = try View.init(a, test_part_root ++ test_text_shape ++ hidden ++ "</p:sld>");
    try std.testing.expectEqualStrings("Visible 12.50\n", try view.text(a));
    try std.testing.expect(!view.unsupported_content);
    try std.testing.expect(!try view.needsVisualInspection());
    const unresolved = try View.init(a, test_part_root ++ "<x:visible/>" ++ hidden ++ test_text_shape ++ "</p:sld>");
    try std.testing.expect(unresolved.unsupported_content);
    try std.testing.expect(try unresolved.needsVisualInspection());
    try std.testing.expectEqualStrings("Visible 12.50\n", try unresolved.text(a));
}

test "document checked projection releases allocations and ignores opaque payload properties" {
    const Case = struct {
        fn run(a: A) !void {
            const bytes = test_part_root ++ test_text_shape ++ "<p:extLst><p:ext uri='unknown'><p:sp><p:nvSpPr><p:cNvPr hidden='invalid'/></p:nvSpPr><a:t>Metadata</a:t></p:sp></p:ext></p:extLst></p:sld>";
            const view = try View.init(a, bytes);
            defer a.free(view.bytes);
            const text = try view.text(a);
            defer a.free(text);
            try std.testing.expectEqualStrings("Visible 12.50\n", text);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
