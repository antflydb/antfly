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

pub const Error = error{
    InvalidSfnt,
    MissingTable,
    TruncatedSfnt,
    InvalidGlyphIndex,
};

const ParseError = Error || std.mem.Allocator.Error;

pub const GlyphPoint = struct {
    x: f64,
    y: f64,
    on_curve: bool,
};

pub const GlyphContour = struct {
    points: []GlyphPoint,

    pub fn deinit(self: *GlyphContour, alloc: std.mem.Allocator) void {
        alloc.free(self.points);
        self.* = undefined;
    }
};

pub const GlyphOutline = struct {
    contours: []GlyphContour,
    x_min: i16,
    y_min: i16,
    x_max: i16,
    y_max: i16,

    pub fn deinit(self: *GlyphOutline, alloc: std.mem.Allocator) void {
        for (self.contours) |*contour| contour.deinit(alloc);
        alloc.free(self.contours);
        self.* = undefined;
    }
};

pub const TableRecord = struct {
    tag: [4]u8,
    checksum: u32,
    offset: u32,
    length: u32,
};

pub const Header = struct {
    scaler_type: u32,
    num_tables: u16,
    search_range: u16,
    entry_selector: u16,
    range_shift: u16,
};

pub const Head = struct {
    units_per_em: u16,
    index_to_loc_format: i16,
};

pub const Maxp = struct {
    num_glyphs: u16,
};

pub const Hhea = struct {
    ascender: i16,
    descender: i16,
    num_h_metrics: u16,
};

pub const GlyphRange = struct {
    offset: u32,
    length: u32,
};

pub const Font = struct {
    bytes: []const u8,
    header: Header,
    tables: []const TableRecord,

    pub fn init(alloc: std.mem.Allocator, bytes: []const u8) ParseError!Font {
        if (bytes.len < 12) return error.TruncatedSfnt;
        const scaler_type = readU32(bytes, 0);
        if (!isSupportedScalerType(scaler_type)) return error.InvalidSfnt;

        const num_tables = readU16(bytes, 4);
        const table_bytes = @as(usize, num_tables) * 16;
        if (bytes.len < 12 + table_bytes) return error.TruncatedSfnt;

        const header = Header{
            .scaler_type = scaler_type,
            .num_tables = num_tables,
            .search_range = readU16(bytes, 6),
            .entry_selector = readU16(bytes, 8),
            .range_shift = readU16(bytes, 10),
        };

        return .{
            .bytes = bytes,
            .header = header,
            .tables = try parseTableDirectory(alloc, bytes, num_tables),
        };
    }

    pub fn deinit(self: *Font, alloc: std.mem.Allocator) void {
        alloc.free(self.tables);
        self.* = undefined;
    }

    pub fn findTable(self: Font, tag: [4]u8) ?TableRecord {
        for (self.tables) |table| {
            if (std.mem.eql(u8, &table.tag, &tag)) return table;
        }
        return null;
    }

    pub fn tableData(self: Font, tag: [4]u8) Error![]const u8 {
        const table = self.findTable(tag) orelse return error.MissingTable;
        return try self.tableBytes(table);
    }

    pub fn head(self: Font) Error!Head {
        const table = self.findTable(.{ 'h', 'e', 'a', 'd' }) orelse return error.MissingTable;
        const data = try self.tableBytes(table);
        if (data.len < 54) return error.TruncatedSfnt;
        return .{
            .units_per_em = readU16(data, 18),
            .index_to_loc_format = readI16(data, 50),
        };
    }

    pub fn maxp(self: Font) Error!Maxp {
        const table = self.findTable(.{ 'm', 'a', 'x', 'p' }) orelse return error.MissingTable;
        const data = try self.tableBytes(table);
        if (data.len < 6) return error.TruncatedSfnt;
        return .{
            .num_glyphs = readU16(data, 4),
        };
    }

    pub fn hhea(self: Font) Error!Hhea {
        const table = self.findTable(.{ 'h', 'h', 'e', 'a' }) orelse return error.MissingTable;
        const data = try self.tableBytes(table);
        if (data.len < 36) return error.TruncatedSfnt;
        return .{
            .ascender = readI16(data, 4),
            .descender = readI16(data, 6),
            .num_h_metrics = readU16(data, 34),
        };
    }

    pub fn glyphRange(self: Font, glyph_index: u16) Error!GlyphRange {
        const maxp_table = try self.maxp();
        if (glyph_index >= maxp_table.num_glyphs) return error.InvalidGlyphIndex;
        const head_table = try self.head();
        const loca = self.findTable(.{ 'l', 'o', 'c', 'a' }) orelse return error.MissingTable;
        const glyf = self.findTable(.{ 'g', 'l', 'y', 'f' }) orelse return error.MissingTable;
        const loca_bytes = try self.tableBytes(loca);

        const start = switch (head_table.index_to_loc_format) {
            0 => @as(u32, readU16(loca_bytes, @as(usize, glyph_index) * 2)) * 2,
            1 => readU32(loca_bytes, @as(usize, glyph_index) * 4),
            else => return error.InvalidSfnt,
        };
        const next = switch (head_table.index_to_loc_format) {
            0 => @as(u32, readU16(loca_bytes, (@as(usize, glyph_index) + 1) * 2)) * 2,
            1 => readU32(loca_bytes, (@as(usize, glyph_index) + 1) * 4),
            else => return error.InvalidSfnt,
        };
        if (next < start) return error.InvalidSfnt;
        return .{
            .offset = glyf.offset + start,
            .length = next - start,
        };
    }

    pub fn advanceWidth(self: Font, glyph_index: u16) Error!u16 {
        const maxp_table = try self.maxp();
        if (glyph_index >= maxp_table.num_glyphs) return error.InvalidGlyphIndex;
        const hhea_table = try self.hhea();
        const hmtx = self.findTable(.{ 'h', 'm', 't', 'x' }) orelse return error.MissingTable;
        const data = try self.tableBytes(hmtx);
        const metric_index = @min(glyph_index, hhea_table.num_h_metrics -| 1);
        const offset = @as(usize, metric_index) * 4;
        if (offset + 2 > data.len) return error.TruncatedSfnt;
        return readU16(data, offset);
    }

    pub fn horizontalKerning(self: Font, left_glyph: u16, right_glyph: u16) Error!i16 {
        const kern = self.findTable(.{ 'k', 'e', 'r', 'n' }) orelse return 0;
        const data = try self.tableBytes(kern);
        if (data.len < 4) return error.TruncatedSfnt;
        const version = readU16(data, 0);
        if (version != 0) return 0;
        const n_tables = readU16(data, 2);
        var cursor: usize = 4;
        var table_idx: usize = 0;
        while (table_idx < n_tables) : (table_idx += 1) {
            if (cursor + 6 > data.len) return error.TruncatedSfnt;
            const length = readU16(data, cursor + 2);
            const coverage = readU16(data, cursor + 4);
            if (length < 6 or cursor + length > data.len) return error.TruncatedSfnt;
            const format: u8 = @intCast((coverage >> 8) & 0xff);
            const horizontal = (coverage & 0x0001) != 0;
            if (horizontal and format == 0) {
                const sub = data[cursor .. cursor + length];
                if (sub.len < 14) return error.TruncatedSfnt;
                const n_pairs = readU16(sub, 6);
                var pair_idx: usize = 0;
                while (pair_idx < n_pairs) : (pair_idx += 1) {
                    const base = 14 + pair_idx * 6;
                    if (base + 6 > sub.len) return error.TruncatedSfnt;
                    const left = readU16(sub, base);
                    const right = readU16(sub, base + 2);
                    if (left == left_glyph and right == right_glyph) {
                        return readI16(sub, base + 4);
                    }
                }
            }
            cursor += length;
        }
        return 0;
    }

    pub fn cmapGlyphIndex(self: Font, codepoint: u21) Error!?u16 {
        const cmap = self.findTable(.{ 'c', 'm', 'a', 'p' }) orelse return error.MissingTable;
        const data = try self.tableBytes(cmap);
        if (data.len < 4) return error.TruncatedSfnt;
        const num_tables = readU16(data, 2);

        const preferred = [_]struct { platform: u16, encoding: ?u16 }{
            .{ .platform = 3, .encoding = 10 },
            .{ .platform = 0, .encoding = null },
            .{ .platform = 3, .encoding = 1 },
            .{ .platform = 3, .encoding = 0 },
        };

        for (preferred) |pref| {
            var i: usize = 0;
            while (i < num_tables) : (i += 1) {
                const base = 4 + i * 8;
                if (base + 8 > data.len) return error.TruncatedSfnt;
                const platform_id = readU16(data, base);
                const encoding_id = readU16(data, base + 2);
                if (platform_id != pref.platform) continue;
                if (pref.encoding) |enc| {
                    if (encoding_id != enc) continue;
                }

                const sub_offset = readU32(data, base + 4);
                if (sub_offset >= data.len) return error.TruncatedSfnt;
                const sub = data[sub_offset..];
                if (sub.len < 2) return error.TruncatedSfnt;
                const format = readU16(sub, 0);
                const glyph = switch (format) {
                    0 => try cmapFormat0Glyph(sub, codepoint),
                    4 => try cmapFormat4Glyph(sub, codepoint),
                    12 => try cmapFormat12Glyph(sub, codepoint),
                    else => null,
                };
                if (glyph != null) return glyph;
            }
        }

        return null;
    }

    pub fn glyphOutlineAlloc(self: Font, alloc: std.mem.Allocator, glyph_index: u16) ParseError!?GlyphOutline {
        return try self.glyphOutlineAllocDepth(alloc, glyph_index, 0);
    }

    fn glyphOutlineAllocDepth(self: Font, alloc: std.mem.Allocator, glyph_index: u16, depth: u8) ParseError!?GlyphOutline {
        if (depth > 8) return error.InvalidSfnt;
        const range = try self.glyphRange(glyph_index);
        if (range.length == 0) return null;

        const start: usize = @intCast(range.offset);
        const end = start + @as(usize, @intCast(range.length));
        if (end > self.bytes.len or end < start) return error.TruncatedSfnt;
        const data = self.bytes[start..end];
        if (data.len < 10) return error.TruncatedSfnt;

        const num_contours = readI16(data, 0);
        if (num_contours < 0) return try self.parseCompositeGlyphOutlineAlloc(alloc, data, depth);
        if (num_contours == 0) {
            return GlyphOutline{
                .contours = try alloc.alloc(GlyphContour, 0),
                .x_min = readI16(data, 2),
                .y_min = readI16(data, 4),
                .x_max = readI16(data, 6),
                .y_max = readI16(data, 8),
            };
        }

        var cursor: usize = 10;
        const contour_count: usize = @intCast(num_contours);
        if (cursor + contour_count * 2 > data.len) return error.TruncatedSfnt;
        const end_pts = try alloc.alloc(u16, contour_count);
        defer alloc.free(end_pts);
        for (end_pts, 0..) |*end_pt, i| {
            end_pt.* = readU16(data, cursor + i * 2);
        }
        cursor += contour_count * 2;
        const point_count = @as(usize, end_pts[contour_count - 1]) + 1;

        if (cursor + 2 > data.len) return error.TruncatedSfnt;
        const instruction_len = readU16(data, cursor);
        cursor += 2;
        if (cursor + instruction_len > data.len) return error.TruncatedSfnt;
        cursor += instruction_len;

        const flags = try alloc.alloc(u8, point_count);
        defer alloc.free(flags);
        var flag_index: usize = 0;
        while (flag_index < point_count) {
            if (cursor >= data.len) return error.TruncatedSfnt;
            const flag = data[cursor];
            cursor += 1;
            var repeat: usize = 1;
            if ((flag & 0x08) != 0) {
                if (cursor >= data.len) return error.TruncatedSfnt;
                repeat += data[cursor];
                cursor += 1;
            }
            if (flag_index + repeat > point_count) return error.InvalidSfnt;
            @memset(flags[flag_index .. flag_index + repeat], flag);
            flag_index += repeat;
        }

        const xs = try alloc.alloc(i32, point_count);
        defer alloc.free(xs);
        const ys = try alloc.alloc(i32, point_count);
        defer alloc.free(ys);

        var x_accum: i32 = 0;
        for (flags, 0..) |flag, i| {
            if ((flag & 0x02) != 0) {
                if (cursor >= data.len) return error.TruncatedSfnt;
                const delta = @as(i32, data[cursor]);
                cursor += 1;
                x_accum += if ((flag & 0x10) != 0) delta else -delta;
            } else if ((flag & 0x10) == 0) {
                if (cursor + 2 > data.len) return error.TruncatedSfnt;
                x_accum += readI16(data, cursor);
                cursor += 2;
            }
            xs[i] = x_accum;
        }

        var y_accum: i32 = 0;
        for (flags, 0..) |flag, i| {
            if ((flag & 0x04) != 0) {
                if (cursor >= data.len) return error.TruncatedSfnt;
                const delta = @as(i32, data[cursor]);
                cursor += 1;
                y_accum += if ((flag & 0x20) != 0) delta else -delta;
            } else if ((flag & 0x20) == 0) {
                if (cursor + 2 > data.len) return error.TruncatedSfnt;
                y_accum += readI16(data, cursor);
                cursor += 2;
            }
            ys[i] = y_accum;
        }

        const contours = try alloc.alloc(GlyphContour, contour_count);
        errdefer {
            var i: usize = 0;
            while (i < contour_count) : (i += 1) {
                if (contours[i].points.len > 0) alloc.free(contours[i].points);
            }
            alloc.free(contours);
        }

        var start_index: usize = 0;
        for (contours, 0..) |*contour, contour_index| {
            const end_index = @as(usize, end_pts[contour_index]);
            const count = end_index - start_index + 1;
            contour.* = .{ .points = try alloc.alloc(GlyphPoint, count) };
            for (contour.points, 0..) |*point, i| {
                const src_index = start_index + i;
                point.* = .{
                    .x = @floatFromInt(xs[src_index]),
                    .y = @floatFromInt(ys[src_index]),
                    .on_curve = (flags[src_index] & 0x01) != 0,
                };
            }
            start_index = end_index + 1;
        }

        return GlyphOutline{
            .contours = contours,
            .x_min = readI16(data, 2),
            .y_min = readI16(data, 4),
            .x_max = readI16(data, 6),
            .y_max = readI16(data, 8),
        };
    }

    fn parseCompositeGlyphOutlineAlloc(self: Font, alloc: std.mem.Allocator, data: []const u8, depth: u8) ParseError!?GlyphOutline {
        var cursor: usize = 10;
        var last_flags: u16 = 0;
        var contours = std.ArrayList(GlyphContour).empty;
        errdefer {
            for (contours.items) |*contour| contour.deinit(alloc);
            contours.deinit(alloc);
        }

        while (true) {
            if (cursor + 4 > data.len) return error.TruncatedSfnt;
            const flags = readU16(data, cursor);
            const component_glyph = readU16(data, cursor + 2);
            last_flags = flags;
            cursor += 4;

            var arg1: i16 = 0;
            var arg2: i16 = 0;
            if ((flags & 0x0001) != 0) {
                if (cursor + 4 > data.len) return error.TruncatedSfnt;
                arg1 = readI16(data, cursor);
                arg2 = readI16(data, cursor + 2);
                cursor += 4;
            } else {
                if (cursor + 2 > data.len) return error.TruncatedSfnt;
                arg1 = @as(i8, @bitCast(data[cursor]));
                arg2 = @as(i8, @bitCast(data[cursor + 1]));
                cursor += 2;
            }

            if ((flags & 0x0002) == 0) return null;

            var a: f64 = 1;
            var b: f64 = 0;
            var c: f64 = 0;
            var d: f64 = 1;
            if ((flags & 0x0008) != 0) {
                if (cursor + 2 > data.len) return error.TruncatedSfnt;
                const scale = readF2Dot14(data, cursor);
                cursor += 2;
                a = scale;
                d = scale;
            } else if ((flags & 0x0040) != 0) {
                if (cursor + 4 > data.len) return error.TruncatedSfnt;
                a = readF2Dot14(data, cursor);
                d = readF2Dot14(data, cursor + 2);
                cursor += 4;
            } else if ((flags & 0x0080) != 0) {
                if (cursor + 8 > data.len) return error.TruncatedSfnt;
                a = readF2Dot14(data, cursor);
                b = readF2Dot14(data, cursor + 2);
                c = readF2Dot14(data, cursor + 4);
                d = readF2Dot14(data, cursor + 6);
                cursor += 8;
            }

            if (try self.glyphOutlineAllocDepth(alloc, component_glyph, depth + 1)) |child_value| {
                var child = child_value;
                defer child.deinit(alloc);
                for (child.contours) |contour| {
                    var transformed = try alloc.alloc(GlyphPoint, contour.points.len);
                    errdefer alloc.free(transformed);
                    for (contour.points, 0..) |point, i| {
                        transformed[i] = .{
                            .x = a * point.x + b * point.y + @as(f64, @floatFromInt(arg1)),
                            .y = c * point.x + d * point.y + @as(f64, @floatFromInt(arg2)),
                            .on_curve = point.on_curve,
                        };
                    }
                    try contours.append(alloc, .{ .points = transformed });
                }
            }

            if ((flags & 0x0020) == 0) break;
        }

        if ((last_flags & 0x0100) != 0) {
            if (cursor + 2 > data.len) return error.TruncatedSfnt;
            const instruction_len = readU16(data, cursor);
            cursor += 2;
            if (cursor + instruction_len > data.len) return error.TruncatedSfnt;
        }

        return GlyphOutline{
            .contours = try contours.toOwnedSlice(alloc),
            .x_min = readI16(data, 2),
            .y_min = readI16(data, 4),
            .x_max = readI16(data, 6),
            .y_max = readI16(data, 8),
        };
    }

    fn tableBytes(self: Font, table: TableRecord) Error![]const u8 {
        const start: usize = @intCast(table.offset);
        const len: usize = @intCast(table.length);
        const end = start + len;
        if (end > self.bytes.len or end < start) return error.TruncatedSfnt;
        return self.bytes[start..end];
    }
};

fn parseTableDirectory(alloc: std.mem.Allocator, bytes: []const u8, num_tables: u16) ParseError![]const TableRecord {
    const out = try alloc.alloc(TableRecord, num_tables);
    errdefer alloc.free(out);
    var cursor: usize = 12;
    for (out) |*table| {
        if (cursor + 16 > bytes.len) return error.TruncatedSfnt;
        table.* = .{
            .tag = .{ bytes[cursor], bytes[cursor + 1], bytes[cursor + 2], bytes[cursor + 3] },
            .checksum = readU32(bytes, cursor + 4),
            .offset = readU32(bytes, cursor + 8),
            .length = readU32(bytes, cursor + 12),
        };
        cursor += 16;
    }
    return out;
}

fn cmapFormat0Glyph(sub: []const u8, codepoint: u21) Error!?u16 {
    if (sub.len < 262) return error.TruncatedSfnt;
    if (codepoint > 0xff) return null;
    const glyph = sub[6 + @as(usize, @intCast(codepoint))];
    return if (glyph == 0) null else glyph;
}

fn cmapFormat4Glyph(sub: []const u8, codepoint: u21) Error!?u16 {
    if (sub.len < 16 or codepoint > 0xffff) return null;
    const seg_count = readU16(sub, 6) / 2;
    const seg_count_usize: usize = seg_count;
    const end_codes_offset: usize = 14;
    const start_codes_offset = end_codes_offset + seg_count_usize * 2 + 2;
    const id_deltas_offset = start_codes_offset + seg_count_usize * 2;
    const id_range_offsets_offset = id_deltas_offset + seg_count_usize * 2;
    if (id_range_offsets_offset + seg_count_usize * 2 > sub.len) return error.TruncatedSfnt;
    const cp16: u16 = @intCast(codepoint);

    var i: usize = 0;
    while (i < seg_count_usize) : (i += 1) {
        const end_code = readU16(sub, end_codes_offset + i * 2);
        const start_code = readU16(sub, start_codes_offset + i * 2);
        if (cp16 < start_code or cp16 > end_code) continue;

        const id_delta = readI16(sub, id_deltas_offset + i * 2);
        const id_range_offset_addr = id_range_offsets_offset + i * 2;
        const id_range_offset = readU16(sub, id_range_offset_addr);
        if (id_range_offset == 0) {
            const glyph = @as(u16, @intCast((@as(i32, cp16) + id_delta) & 0xffff));
            return if (glyph == 0) null else glyph;
        }

        const glyph_addr = id_range_offset_addr + id_range_offset + @as(usize, cp16 - start_code) * 2;
        if (glyph_addr + 2 > sub.len) return error.TruncatedSfnt;
        const raw_glyph = readU16(sub, glyph_addr);
        if (raw_glyph == 0) return null;
        const glyph = @as(u16, @intCast((@as(i32, raw_glyph) + id_delta) & 0xffff));
        return if (glyph == 0) null else glyph;
    }
    return null;
}

fn cmapFormat12Glyph(sub: []const u8, codepoint: u21) Error!?u16 {
    if (sub.len < 16) return error.TruncatedSfnt;
    const num_groups = readU32(sub, 12);
    var i: usize = 0;
    while (i < num_groups) : (i += 1) {
        const base = 16 + i * 12;
        if (base + 12 > sub.len) return error.TruncatedSfnt;
        const start_char = readU32(sub, base);
        const end_char = readU32(sub, base + 4);
        if (codepoint < start_char or codepoint > end_char) continue;
        const start_glyph = readU32(sub, base + 8);
        const glyph = start_glyph + (codepoint - start_char);
        return @intCast(glyph);
    }
    return null;
}

fn readF2Dot14(bytes: []const u8, offset: usize) f64 {
    const raw: i16 = @bitCast(readU16(bytes, offset));
    return @as(f64, @floatFromInt(raw)) / 16384.0;
}

fn isSupportedScalerType(scaler_type: u32) bool {
    return scaler_type == 0x00010000 or
        scaler_type == 0x74727565 or
        scaler_type == 0x4F54544F;
}

fn readU16(bytes: []const u8, offset: usize) u16 {
    return (@as(u16, bytes[offset]) << 8) | bytes[offset + 1];
}

fn readI16(bytes: []const u8, offset: usize) i16 {
    return @bitCast(readU16(bytes, offset));
}

fn readU32(bytes: []const u8, offset: usize) u32 {
    return (@as(u32, bytes[offset]) << 24) |
        (@as(u32, bytes[offset + 1]) << 16) |
        (@as(u32, bytes[offset + 2]) << 8) |
        bytes[offset + 3];
}

fn appendU16(alloc: std.mem.Allocator, out: *std.ArrayList(u8), value: u16) !void {
    try out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToBig(u16, value)));
}

fn appendI16(alloc: std.mem.Allocator, out: *std.ArrayList(u8), value: i16) !void {
    try appendU16(alloc, out, @bitCast(value));
}

fn appendU32(alloc: std.mem.Allocator, out: *std.ArrayList(u8), value: u32) !void {
    try out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToBig(u32, value)));
}

fn appendTag(alloc: std.mem.Allocator, out: *std.ArrayList(u8), tag: [4]u8) !void {
    try out.appendSlice(alloc, &tag);
}

fn appendBytes(alloc: std.mem.Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    try out.appendSlice(alloc, bytes);
}

fn pad4(alloc: std.mem.Allocator, out: *std.ArrayList(u8)) !void {
    while ((out.items.len % 4) != 0) try out.append(alloc, 0);
}

fn buildSimpleTrueTypeFontAlloc(alloc: std.mem.Allocator) ![]u8 {
    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16(alloc, &head, 0);
    try appendU16(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32(alloc, &maxp, 0x00010000);
    try appendU16(alloc, &maxp, 3);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16(alloc, &hhea, 3);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16(alloc, &hmtx, 500);
    try appendI16(alloc, &hmtx, 0);
    try appendU16(alloc, &hmtx, 1000);
    try appendI16(alloc, &hmtx, 0);
    try appendU16(alloc, &hmtx, 1600);
    try appendI16(alloc, &hmtx, 0);

    var glyph = std.ArrayList(u8).empty;
    defer glyph.deinit(alloc);
    try appendI16(alloc, &glyph, 1);
    try appendI16(alloc, &glyph, 0);
    try appendI16(alloc, &glyph, 0);
    try appendI16(alloc, &glyph, 1000);
    try appendI16(alloc, &glyph, 1000);
    try appendU16(alloc, &glyph, 2);
    try appendU16(alloc, &glyph, 0);
    try glyph.appendSlice(alloc, &.{ 0x31, 0x21, 0x01 });
    try appendI16(alloc, &glyph, 1000);
    try appendI16(alloc, &glyph, -500);
    try appendI16(alloc, &glyph, 1000);
    if ((glyph.items.len % 2) != 0) try glyph.append(alloc, 0);

    var composite = std.ArrayList(u8).empty;
    defer composite.deinit(alloc);
    try appendI16(alloc, &composite, -1);
    try appendI16(alloc, &composite, 0);
    try appendI16(alloc, &composite, 0);
    try appendI16(alloc, &composite, 1600);
    try appendI16(alloc, &composite, 1000);
    try appendU16(alloc, &composite, 0x0023);
    try appendU16(alloc, &composite, 1);
    try appendI16(alloc, &composite, 0);
    try appendI16(alloc, &composite, 0);
    try appendU16(alloc, &composite, 0x0003);
    try appendU16(alloc, &composite, 1);
    try appendI16(alloc, &composite, 600);
    try appendI16(alloc, &composite, 0);
    if ((composite.items.len % 2) != 0) try composite.append(alloc, 0);

    var loca = std.ArrayList(u8).empty;
    defer loca.deinit(alloc);
    try appendU16(alloc, &loca, 0);
    try appendU16(alloc, &loca, 0);
    try appendU16(alloc, &loca, @intCast(glyph.items.len / 2));
    try appendU16(alloc, &loca, @intCast((glyph.items.len + composite.items.len) / 2));

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16(alloc, &cmap, 0);
    try appendU16(alloc, &cmap, 1);
    try appendU16(alloc, &cmap, 3);
    try appendU16(alloc, &cmap, 1);
    try appendU32(alloc, &cmap, 12);
    try appendU16(alloc, &cmap, 4);
    try appendU16(alloc, &cmap, 32);
    try appendU16(alloc, &cmap, 0);
    try appendU16(alloc, &cmap, 4);
    try appendU16(alloc, &cmap, 4);
    try appendU16(alloc, &cmap, 1);
    try appendU16(alloc, &cmap, 0);
    try appendU16(alloc, &cmap, 66);
    try appendU16(alloc, &cmap, 0xFFFF);
    try appendU16(alloc, &cmap, 0);
    try appendU16(alloc, &cmap, 65);
    try appendU16(alloc, &cmap, 0xFFFF);
    try appendU16(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16(alloc, &cmap, 1);
    try appendU16(alloc, &cmap, 0);
    try appendU16(alloc, &cmap, 0);

    var kern = std.ArrayList(u8).empty;
    defer kern.deinit(alloc);
    try appendU16(alloc, &kern, 0);
    try appendU16(alloc, &kern, 1);
    try appendU16(alloc, &kern, 0);
    try appendU16(alloc, &kern, 20);
    try appendU16(alloc, &kern, 0x0001);
    try appendU16(alloc, &kern, 1);
    try appendU16(alloc, &kern, 0);
    try appendU16(alloc, &kern, 0);
    try appendU16(alloc, &kern, 0);
    try appendU16(alloc, &kern, 1);
    try appendU16(alloc, &kern, 2);
    try appendI16(alloc, &kern, -200);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'g', 'l', 'y', 'f' }, .bytes = &.{} },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'k', 'e', 'r', 'n' }, .bytes = kern.items },
        .{ .tag = .{ 'l', 'o', 'c', 'a' }, .bytes = loca.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var bytes = std.ArrayList(u8).empty;
    errdefer bytes.deinit(alloc);
    try appendU32(alloc, &bytes, 0x00010000);
    try appendU16(alloc, &bytes, tables.len);
    try appendU16(alloc, &bytes, 0);
    try appendU16(alloc, &bytes, 0);
    try appendU16(alloc, &bytes, 0);

    const dir_offset = bytes.items.len;
    try bytes.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &bytes);
        const table_offset: u32 = @intCast(bytes.items.len);
        if (std.mem.eql(u8, &table.tag, "glyf")) {
            try appendBytes(alloc, &bytes, glyph.items);
            try appendBytes(alloc, &bytes, composite.items);
        } else {
            try appendBytes(alloc, &bytes, table.bytes);
        }
        try pad4(alloc, &bytes);
        const base = dir_offset + i * 16;
        bytes.items[base + 0] = table.tag[0];
        bytes.items[base + 1] = table.tag[1];
        bytes.items[base + 2] = table.tag[2];
        bytes.items[base + 3] = table.tag[3];
        writeU32BE(bytes.items[base + 4 ..][0..4], 0);
        writeU32BE(bytes.items[base + 8 ..][0..4], table_offset);
        const table_len: u32 = if (std.mem.eql(u8, &table.tag, "glyf"))
            @intCast(glyph.items.len + composite.items.len)
        else
            @intCast(table.bytes.len);
        writeU32BE(bytes.items[base + 12 ..][0..4], table_len);
    }

    return try bytes.toOwnedSlice(alloc);
}

test "sfnt reader parses head maxp hhea and glyph range" {
    const alloc = std.testing.allocator;
    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(alloc);

    try appendU32(alloc, &bytes, 0x00010000);
    try appendU16(alloc, &bytes, 5);
    try appendU16(alloc, &bytes, 0);
    try appendU16(alloc, &bytes, 0);
    try appendU16(alloc, &bytes, 0);

    const table_dir_offset = bytes.items.len;
    try bytes.appendNTimes(alloc, 0, 5 * 16);

    const head_offset: u32 = @intCast(bytes.items.len);
    try bytes.appendNTimes(alloc, 0, 18);
    try appendU16(alloc, &bytes, 1000);
    try bytes.appendNTimes(alloc, 0, 30);
    try appendI16(alloc, &bytes, 0);

    const maxp_offset: u32 = @intCast(bytes.items.len);
    try appendU32(alloc, &bytes, 0x00010000);
    try appendU16(alloc, &bytes, 2);

    const hhea_offset: u32 = @intCast(bytes.items.len);
    try bytes.appendNTimes(alloc, 0, 34);
    try appendU16(alloc, &bytes, 1);

    const loca_offset: u32 = @intCast(bytes.items.len);
    try appendU16(alloc, &bytes, 0);
    try appendU16(alloc, &bytes, 5);
    try appendU16(alloc, &bytes, 5);

    const glyf_offset: u32 = @intCast(bytes.items.len);
    try bytes.appendNTimes(alloc, 0xaa, 10);

    const tables = [_]struct { tag: [4]u8, offset: u32, length: u32 }{
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .offset = head_offset, .length = 54 },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .offset = maxp_offset, .length = 6 },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .offset = hhea_offset, .length = 36 },
        .{ .tag = .{ 'l', 'o', 'c', 'a' }, .offset = loca_offset, .length = 6 },
        .{ .tag = .{ 'g', 'l', 'y', 'f' }, .offset = glyf_offset, .length = 10 },
    };

    var dir = bytes.items[table_dir_offset..][0 .. tables.len * 16];
    for (tables, 0..) |table, i| {
        const base = i * 16;
        dir[base + 0] = table.tag[0];
        dir[base + 1] = table.tag[1];
        dir[base + 2] = table.tag[2];
        dir[base + 3] = table.tag[3];
        writeU32BE(dir[base + 4 ..][0..4], 0);
        writeU32BE(dir[base + 8 ..][0..4], table.offset);
        writeU32BE(dir[base + 12 ..][0..4], table.length);
    }

    var font = try Font.init(alloc, bytes.items);
    defer alloc.free(font.tables);
    const head = try font.head();
    try std.testing.expectEqual(@as(u16, 1000), head.units_per_em);
    try std.testing.expectEqual(@as(i16, 0), head.index_to_loc_format);
    const maxp = try font.maxp();
    try std.testing.expectEqual(@as(u16, 2), maxp.num_glyphs);
    const hhea = try font.hhea();
    try std.testing.expectEqual(@as(i16, 0), hhea.ascender);
    try std.testing.expectEqual(@as(i16, 0), hhea.descender);
    try std.testing.expectEqual(@as(u16, 1), hhea.num_h_metrics);
    const range = try font.glyphRange(0);
    try std.testing.expectEqual(glyf_offset, range.offset);
    try std.testing.expectEqual(@as(u32, 10), range.length);
}

test "sfnt reader rejects invalid scaler type" {
    const bytes = [_]u8{ 0, 0, 0, 0 } ++ [_]u8{0} ** 8;
    try std.testing.expectError(error.InvalidSfnt, Font.init(std.testing.allocator, &bytes));
}

test "sfnt reader maps cmap and extracts simple glyph outline" {
    const alloc = std.testing.allocator;
    const bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(bytes);

    var font = try Font.init(alloc, bytes);
    defer font.deinit(alloc);

    try std.testing.expectEqual(@as(?u16, 1), try font.cmapGlyphIndex('A'));
    try std.testing.expectEqual(@as(u16, 1000), try font.advanceWidth(1));

    var outline = (try font.glyphOutlineAlloc(alloc, 1)).?;
    defer outline.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), outline.contours.len);
    try std.testing.expectEqual(@as(usize, 3), outline.contours[0].points.len);
    try std.testing.expectApproxEqAbs(@as(f64, 0), outline.contours[0].points[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1000), outline.contours[0].points[1].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1000), outline.contours[0].points[2].y, 0.001);
    try std.testing.expect(outline.contours[0].points[0].on_curve);
}

test "sfnt reader extracts composite glyph outline" {
    const alloc = std.testing.allocator;
    const bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(bytes);

    var font = try Font.init(alloc, bytes);
    defer font.deinit(alloc);

    try std.testing.expectEqual(@as(?u16, 2), try font.cmapGlyphIndex('B'));
    try std.testing.expectEqual(@as(u16, 1600), try font.advanceWidth(2));

    var outline = (try font.glyphOutlineAlloc(alloc, 2)).?;
    defer outline.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), outline.contours.len);
    try std.testing.expectApproxEqAbs(@as(f64, 0), outline.contours[0].points[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 600), outline.contours[1].points[0].x, 0.001);
}

test "sfnt reader parses horizontal kerning" {
    const alloc = std.testing.allocator;
    const bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(bytes);

    var font = try Font.init(alloc, bytes);
    defer font.deinit(alloc);

    try std.testing.expectEqual(@as(i16, -200), try font.horizontalKerning(1, 2));
    try std.testing.expectEqual(@as(i16, 0), try font.horizontalKerning(2, 1));
}

fn writeU32BE(dst: []u8, value: u32) void {
    dst[0] = @intCast((value >> 24) & 0xff);
    dst[1] = @intCast((value >> 16) & 0xff);
    dst[2] = @intCast((value >> 8) & 0xff);
    dst[3] = @intCast(value & 0xff);
}
