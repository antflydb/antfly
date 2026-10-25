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
        var i = self.name.len;
        while (i < self.raw.len) {
            if (!std.ascii.isWhitespace(self.raw[i])) return error.InvalidDocumentXml;
            while (i < self.raw.len and std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            if (i == self.raw.len) break;
            const start = i;
            while (i < self.raw.len and self.raw[i] != '=' and !std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            const key = self.raw[start..i];
            while (i < self.raw.len and std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            if (key.len == 0 or i == self.raw.len or self.raw[i] != '=') return error.InvalidDocumentXml;
            i += 1;
            while (i < self.raw.len and std.ascii.isWhitespace(self.raw[i])) : (i += 1) {}
            if (i == self.raw.len or (self.raw[i] != '\'' and self.raw[i] != '"')) return error.InvalidDocumentXml;
            const quote = self.raw[i];
            i += 1;
            const value_start = i;
            while (i < self.raw.len and self.raw[i] != quote) : (i += 1) {}
            if (i == self.raw.len) return error.InvalidDocumentXml;
            if (std.mem.eql(u8, key, wanted)) {
                if (result != null) return error.InvalidDocumentXml;
                result = self.raw[value_start..i];
            }
            i += 1;
        }
        return result;
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

/// All public consumers share one visibility-checked view. The view borrows
/// bytes when no shapes are hidden; filtered parts copy visible spans once.
pub const View = struct {
    bytes: []const u8,
    pub fn init(a: A, bytes: []const u8) !View {
        return initWithControl(a, bytes, .{});
    }
    pub fn initWithControl(a: A, bytes: []const u8, control: @import("contract.zig").Control) !View {
        return .{ .bytes = try visibleBytes(a, bytes, control) };
    }
    pub fn text(self: View, a: A) ![]const u8 {
        return extractText(a, self.bytes);
    }
    pub fn tables(self: View, a: A, limit: usize) ![]const @import("contract.zig").Table {
        return extractTablesWithLimit(a, self.bytes, limit);
    }
    pub fn needsVisualInspection(self: View) !bool {
        return requiresVisualInspection(self.bytes);
    }
};
fn isDrawing(local: []const u8) bool {
    for ([_][]const u8{ "sp", "pic", "graphicFrame", "cxnSp", "grpSp", "spTree", "contentPart" }) |name| {
        if (std.mem.eql(u8, local, name)) return true;
    }
    return false;
}
fn hiddenAttribute(tag: Tag) !bool {
    const raw = (try tag.uniqueAttr("hidden")) orelse return false;
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

fn visibleBytes(a: A, bytes: []const u8, control: @import("contract.zig").Control) ![]const u8 {
    try control.check();
    const Frame = struct { start: usize, depth: usize, output_start: usize, hidden: bool = false, visibility_seen: bool = false };
    var frames: [256]Frame = undefined;
    var count: usize = 0;
    var iter = Xml{ .bytes = bytes };
    var output = @import("output.zig").Writer.init(a, bytes.len);
    defer output.deinit();
    var cursor: usize = 0;
    var changed = false;
    while (try iter.next()) |token| {
        try control.check();
        if (token != .tag) continue;
        const tag = token.tag;
        const local = tag.local();
        if (!tag.closing and !tag.empty and isDrawing(local)) {
            if (count == frames.len) return error.InvalidDocumentXml;
            const raw_offset = @intFromPtr(tag.raw.ptr) - @intFromPtr(bytes.ptr);
            const start = std.mem.lastIndexOfScalar(u8, bytes[0..raw_offset], '<') orelse return error.InvalidDocumentXml;
            frames[count] = .{ .start = start, .depth = iter.depth, .output_start = output.len() + start - cursor };
            count += 1;
        }
        if (!tag.closing and std.mem.eql(u8, local, "cNvPr") and count > 0) {
            const frame = &frames[count - 1];
            const depth = iter.depth + @intFromBool(tag.empty);
            // A child shape's properties cannot override its parent/group.
            // The owning drawing's cNvPr lives in its nonvisual wrapper.
            if (depth == frame.depth + 2) {
                if (frame.visibility_seen) return error.InvalidDocumentXml;
                frame.visibility_seen = true;
                frame.hidden = try hiddenAttribute(tag);
            }
        }
        if (tag.closing and count > 0 and iter.depth < frames[count - 1].depth) {
            count -= 1;
            const frame = frames[count];
            if (frame.hidden) {
                // A hidden outer group may close after hidden children. Roll
                // back any copied prefix in that group without retaining a
                // range index or copying any source span a second time.
                if (cursor <= frame.start) try output.raw(bytes[cursor..frame.start]);
                std.debug.assert(frame.output_start <= output.len());
                output.writer.end = frame.output_start;
                cursor = iter.index;
                changed = true;
            }
        }
    }
    if (count != 0) return error.InvalidDocumentXml;
    if (!changed) return bytes;
    try output.raw(bytes[cursor..]);
    return output.toOwnedSlice();
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
            if (!std.mem.eql(u8, uri, "http://schemas.openxmlformats.org/drawingml/2006/table")) return true;
        }
        if (std.mem.eql(u8, local, "sp")) {
            shape = true;
            textbox = false;
            geometry = false;
            nonrect = false;
        }
        if (shape and std.mem.eql(u8, local, "cNvSpPr")) {
            if (tag.attr("txBox")) |value| textbox = std.mem.eql(u8, value, "1") or std.mem.eql(u8, value, "true");
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
    var output = std.Io.Writer.Allocating.init(a);
    var inside = false;
    while (try iter.next()) |token| switch (token) {
        .tag => |tag| {
            if (std.mem.eql(u8, tag.local(), "t")) inside = !tag.closing and !tag.empty;
            // DrawingML hard breaks separate runs within one paragraph. Emit
            // once at the opening tag for both empty and paired br elements.
            if (!tag.closing and std.mem.eql(u8, tag.local(), "br")) try output.writer.writeByte('\n');
            if (tag.closing and (std.mem.eql(u8, tag.local(), "p") or std.mem.eql(u8, tag.local(), "tr"))) try output.writer.writeByte('\n');
            if (tag.closing and std.mem.eql(u8, tag.local(), "tc")) try output.writer.writeByte('\t');
        },
        .text => |value| if (inside) {
            try unescape(&output.writer, value);
        },
    };
    const result = try output.toOwnedSlice();
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
                try result.append(a, .{ .id = result.items.len, .rows = row, .columns = columns, .cells = try cells.toOwnedSlice(a) });
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
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.InvalidDocumentXml, extractText(arena.allocator(), "<p:sld><a:t>partial"));
    try std.testing.expectError(error.InvalidDocumentXml, extractText(arena.allocator(), "<p:sld><a:t>partial</p:sld>"));
    try std.testing.expectError(error.InvalidDocumentXml, extractText(arena.allocator(), "<p:sld><a:t>\xff</a:t></p:sld>"));
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
    try std.testing.expectError(error.DocumentRegionLimitExceeded, extractTablesWithLimit(a, "<a:tbl></a:tbl><a:tbl></a:tbl>", 1));
}

test "document visibility borrows visible XML and filters hidden owners consistently" {
    const visible = "<p:sld><p:sp><p:nvSpPr><p:cNvPr hidden='false'/></p:nvSpPr><a:p><a:t>Visible &amp; 李</a:t></a:p></p:sp></p:sld>";
    const borrowed = try View.init(std.testing.failing_allocator, visible);
    try std.testing.expectEqual(visible.ptr, borrowed.bytes.ptr);
    const encoded: []const u8 = "<sp><nvSpPr><cNvPr hidden='&#32;f&#97;lse&#32;'/></nvSpPr><t>Visible</t></sp>";
    try std.testing.expectEqual(encoded.ptr, (try View.init(std.testing.failing_allocator, encoded)).bytes.ptr);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "1", "true", " true ", "&#49;", "tr&#117;e", "&#x20;true&#32;" }) |hidden| {
        const bytes = try std.fmt.allocPrint(a, "<sld><sp><nvSpPr><cNvPr hidden='{s}'/></nvSpPr><t>Obsolete</t></sp><graphicFrame><nvGraphicFramePr><cNvPr hidden='{s}'/></nvGraphicFramePr><tbl><tr><tc><t>999.99</t></tc></tr></tbl></graphicFrame><pic><nvPicPr><cNvPr hidden='{s}'/></nvPicPr><blipFill/></pic><sp><nvSpPr><cNvPr hidden='0'/></nvSpPr><p><t>Visible &amp; 李</t></p></sp></sld>", .{ hidden, hidden, hidden });
        const view = try View.init(a, bytes);
        try std.testing.expectEqualStrings("Visible & 李\n", try view.text(a));
        try std.testing.expectEqual(@as(usize, 0), (try view.tables(a, 1)).len);
        try std.testing.expect(!try view.needsVisualInspection());
    }
    // Identical token payloads outside a drawing's nonvisual wrapper are not
    // properties of that drawing and cannot silently hide its visible text.
    const unrelated = try View.init(a, "<sp><ext><other><cNvPr hidden='1'/></other></ext><t>Keep</t></sp>");
    try std.testing.expectEqualStrings("Keep", try unrelated.text(a));
}

test "document visibility handles nested groups without copying hidden descendants back" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const xml = "<sld><sp><t>Before</t></sp><grpSp><nvGrpSpPr><cNvPr hidden='1'/></nvGrpSpPr><sp><nvSpPr><cNvPr hidden='false'/></nvSpPr><t>Hidden by parent</t></sp><grpSp><nvGrpSpPr><cNvPr hidden='true'/></nvGrpSpPr><sp><t>Hidden nested</t></sp></grpSp><sp><t>Hidden tail</t></sp></grpSp><sp><t>After</t></sp></sld>";
    const view = try View.init(a, xml);
    try std.testing.expectEqualStrings("BeforeAfter", try view.text(a));
    try std.testing.expect(!try view.needsVisualInspection());
    const visible_group = try View.init(a, "<grpSp><nvGrpSpPr><cNvPr hidden='0'/></nvGrpSpPr><sp><nvSpPr><cNvPr hidden='1'/></nvSpPr><t>Hidden</t></sp><sp><t>Visible</t></sp></grpSp>");
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
    }) |xml| try std.testing.expectError(error.InvalidDocumentXml, View.init(a, xml));
    const Case = struct {
        fn run(alloc: A) !void {
            const bytes: []const u8 = "<sld><sp><t>Before</t></sp><sp><nvSpPr><cNvPr hidden='true'/></nvSpPr><t>Hidden</t></sp><sp><t>After</t></sp></sld>";
            const view = try View.init(alloc, bytes);
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
    const view = try View.init(a, bytes);
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
    try std.testing.expectError(error.Canceled, View.initWithControl(a, bytes, .{ .ptr = &cancelled, .check_fn = Cancel.check }));
}
