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

//! Native DrawingML display compilation. Supported drawings become bounded
//! PDF display lists and use Antfly's existing native vector/image rasterizer.
//! Unsupported drawing semantics fail closed before any provider invocation.
const std = @import("std");
const xml = @import("ooxml.zig");
const parts = @import("slides.zig");
const c = @import("contract.zig");
const image = @import("antfly_image");
const Writer = @import("output.zig").Writer;
const A = std.mem.Allocator;
const emu_per_point = 12700.0;
const Theme = @import("drawingml_theme.zig").Theme;
const text = @import("drawingml_text.zig");
const charts = @import("drawingml_chart.zig");
const native_fonts = @import("antfly_pdf").native_fonts;

pub const Size = struct { width: f64, height: f64 };
pub const Layer = struct { path: []const u8, view: xml.View, shapes: bool = true, background: bool = false, theme: Theme = .{} };

fn print(writer: *Writer, comptime format: []const u8, args: anytype) !void {
    writer.writer.print(format, args) catch |err| return writer.failure orelse err;
}
fn eq(left: []const u8, right: []const u8) bool {
    return std.mem.eql(u8, left, right);
}
fn number(a: A, tag: xml.Tag, name: []const u8) !f64 {
    const raw = (try tag.uniqueAttr(name)) orelse return error.DocumentRendererUnavailable;
    const integer = std.fmt.parseInt(i64, try xml.decodedText(a, raw), 10) catch return error.InvalidDocumentXml;
    const value: f64 = @floatFromInt(integer);
    if (!std.math.isFinite(value) or @abs(value) > 1e12) return error.InvalidDocumentXml;
    return value;
}
pub fn presentationSize(a: A, bytes: []const u8, control: c.Control) !Size {
    var iter = xml.QualifiedXml.init(a, bytes);
    defer iter.deinit();
    var result: ?Size = null;
    while (try iter.next()) |token| {
        try control.check();
        if (token != .tag or token.tag.value.closing) continue;
        const tag = token.tag;
        if (tag.depth == 1 and (tag.namespace != .presentation or !eq(tag.value.local(), "presentation"))) return error.InvalidDocumentXml;
        if (tag.depth != 2 or tag.namespace != .presentation or !eq(tag.value.local(), "sldSz")) continue;
        if (result != null) return error.InvalidDocumentXml;
        const width = try number(a, tag.value, "cx");
        const height = try number(a, tag.value, "cy");
        if (width <= 0 or height <= 0) return error.InvalidDocumentXml;
        result = .{ .width = width, .height = height };
    }
    return result orelse error.DocumentRendererUnavailable;
}

const Drawing = struct {
    picture: bool,
    group: bool = false,
    depth: usize,
    x: f64 = 0,
    y: f64 = 0,
    width: f64 = 0,
    height: f64 = 0,
    positioned: bool = false,
    sized: bool = false,
    rotation: f64 = 0,
    flip_x: bool = false,
    flip_y: bool = false,
    fill: ?[3]u8 = null,
    stroke: ?[3]u8 = null,
    fill_alpha: f64 = 1,
    stroke_alpha: f64 = 1,
    stroke_width: f64 = 1,
    geometry: enum { rect, ellipse, line } = .rect,
    embed: ?[]const u8 = null,
    crop: [4]f64 = @splat(0),
    child_x: f64 = 0,
    child_y: f64 = 0,
    child_width: ?f64 = null,
    child_height: ?f64 = null,
    text_body: ?text.Body = null,
    graphic: bool = false,
    chart: ?[]const u8 = null,
};

const Builder = struct {
    allocator: A,
    scratch_backing: A,
    run: *c.Run,
    archive: xml.Archive,
    size: Size,
    objects: std.ArrayList([]const u8) = .empty,
    object_bytes: usize = 0,
    drawings: usize = 0,
    content: Writer,
    resources: Writer,
    states: Writer,
    font_resources: Writer,
    fonts: ?text.Fonts = null,
    glyph_index: [2]std.AutoHashMapUnmanaged(u21, u16) = .{ .empty, .empty },
    glyphs: [2]std.ArrayList(struct { id: u16, codepoint: u21 }) = .{ .empty, .empty },
    alpha_states: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    theme: Theme = .{},
    images: std.StringHashMapUnmanaged(usize) = .empty,
    links: parts.Cache = .{},

    fn setAlpha(self: *Builder, fill: f64, stroke: f64) !void {
        const key = @as(u64, @intFromFloat(@round(fill * 100000))) * 100001 + @as(u64, @intFromFloat(@round(stroke * 100000)));
        const index = self.alpha_states.get(key) orelse new: {
            const index = self.objects.items.len + 1;
            try self.appendObject(try std.fmt.allocPrint(self.allocator, "<< /Type /ExtGState /ca {d} /CA {d} >>", .{ fill, stroke }));
            try print(&self.states, "/GS{d} {d} 0 R ", .{ index, index });
            try self.alpha_states.put(self.allocator, key, index);
            break :new index;
        };
        try print(&self.content, "/GS{d} gs\n", .{index});
    }
    fn paintText(self: *Builder, body: text.Body, width: f64, height: f64) !void {
        if (self.fonts == null) self.fonts = try text.Fonts.init(self.allocator);
        var scratch = std.heap.ArenaAllocator.init(self.scratch_backing);
        defer scratch.deinit();
        const glyphs = try text.layout(scratch.allocator(), body, width, height, self.fonts.?, self.run.control);
        for (glyphs) |glyph| {
            try self.run.control.check();
            const face = @intFromBool(glyph.style.bold);
            const cid = self.glyph_index[face].get(glyph.codepoint) orelse new: {
                if (self.glyphs[face].items.len >= 65534) return error.DocumentRendererUnavailable;
                const cid: u16 = @intCast(self.glyphs[face].items.len + 1);
                try self.glyphs[face].append(self.allocator, .{ .id = glyph.id, .codepoint = glyph.codepoint });
                try self.glyph_index[face].put(self.allocator, glyph.codepoint, cid);
                break :new cid;
            };
            try self.setAlpha(glyph.style.color.alpha, 1);
            const rgb = glyph.style.color.rgb;
            try print(&self.content, "{d} {d} {d} rg BT /Ft{d} {d} Tf 1 0 {d} -1 {d} {d} Tm <{x:0>4}> Tj ET\n", .{
                @as(f64, @floatFromInt(rgb[0])) / 255, @as(f64, @floatFromInt(rgb[1])) / 255, @as(f64, @floatFromInt(rgb[2])) / 255,
                face,                                  glyph.style.size,                      @as(f64, if (glyph.style.italic) 0.212556 else 0),
                glyph.x,                               glyph.y,                               cid,
            });
        }
    }
    fn chartRect(raw: *anyopaque, box: [4]f64, color: @import("drawingml_theme.zig").Color) !void {
        const self: *Builder = @ptrCast(@alignCast(raw));
        try self.setAlpha(color.alpha, 1);
        try print(&self.content, "{d} {d} {d} rg {d} {d} {d} {d} re f\n", .{ @as(f64, @floatFromInt(color.rgb[0])) / 255, @as(f64, @floatFromInt(color.rgb[1])) / 255, @as(f64, @floatFromInt(color.rgb[2])) / 255, box[0], box[1], box[2], box[3] });
    }
    fn chartPath(raw: *anyopaque, points: []const [2]f64, color: @import("drawingml_theme.zig").Color, filled: bool) !void {
        if (points.len < 2) return;
        const self: *Builder = @ptrCast(@alignCast(raw));
        try self.setAlpha(color.alpha, color.alpha);
        try print(&self.content, "{d} {d} {d} {s} 1 w {d} {d} m\n", .{ @as(f64, @floatFromInt(color.rgb[0])) / 255, @as(f64, @floatFromInt(color.rgb[1])) / 255, @as(f64, @floatFromInt(color.rgb[2])) / 255, if (filled) @as([]const u8, "rg") else "RG", points[0][0], points[0][1] });
        for (points[1..]) |point| try print(&self.content, "{d} {d} l\n", .{ point[0], point[1] });
        try self.content.raw(if (filled) "h f\n" else "S\n");
    }
    fn chartLine(raw: *anyopaque, points: []const [2]f64, color: @import("drawingml_theme.zig").Color) !void {
        return chartPath(raw, points, color, false);
    }
    fn chartPolygon(raw: *anyopaque, points: []const [2]f64, color: @import("drawingml_theme.zig").Color) !void {
        return chartPath(raw, points, color, true);
    }
    fn chartLabel(raw: *anyopaque, box: [4]f64, value: []const u8, size: f64, bold: bool) !void {
        if (value.len == 0) return;
        const self: *Builder = @ptrCast(@alignCast(raw));
        if (value.len > @min(self.run.limits.max_output_bytes, 1_000_000)) return error.DocumentOutputTooLarge;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_backing);
        defer scratch.deinit();
        var runes = std.ArrayList(text.Rune).empty;
        var unicode = (try std.unicode.Utf8View.init(value)).iterator();
        while (unicode.nextCodepoint()) |codepoint| try runes.append(scratch.allocator(), .{ .codepoint = codepoint, .style = .{ .size = size, .bold = bold } });
        try print(&self.content, "q 1 0 0 1 {d} {d} cm\n", .{ box[0], box[1] });
        try self.paintText(.{ .paragraphs = &.{.{ .text = runes.items }}, .inset = @splat(0), .wrap = false }, box[2], box[3]);
        try self.content.raw("Q\n");
    }
    fn paintChart(self: *Builder, path: []const u8, id: []const u8, width: f64, height: f64) !void {
        const links = (try self.links.relationshipsFor(self.allocator, self.archive, path)) orelse return error.MissingDocumentPart;
        const related = try links.get(id, .chart);
        const slash = std.mem.lastIndexOfScalar(u8, path, '/') orelse return error.InvalidDocumentSource;
        const target = try parts.normalizedPart(self.allocator, path[0 .. slash + 1], related.target);
        const chart = try charts.parse(self.allocator, try self.archive.get(self.allocator, target), self.theme, self.run);
        try charts.paint(self.allocator, chart, width, height, self.theme, .{ .ptr = self, .rect = chartRect, .line = chartLine, .polygon = chartPolygon, .label = chartLabel }, self.run);
    }
    fn finishFonts(self: *Builder) !void {
        for (&self.glyphs, 0..) |*glyphs, face| {
            if (glyphs.items.len == 0) continue;
            const first = self.objects.items.len + 1;
            const bytes = if (face == 0) native_fonts.regular else native_fonts.bold;
            try self.appendObject(try self.stream(try std.fmt.allocPrint(self.allocator, "/Length1 {d}", .{bytes.len}), bytes));
            const metrics = try self.fonts.?.faces[face].hhea();
            try self.appendObject(try std.fmt.allocPrint(self.allocator, "<< /Type /FontDescriptor /FontName /AntflyRoboto{d} /Flags 32 /FontBBox [-2048 -2048 4096 4096] /ItalicAngle 0 /Ascent {d} /Descent {d} /CapHeight 700 /StemV 80 /FontFile2 {d} 0 R >>", .{ face, @as(f64, @floatFromInt(metrics.ascender)) * 1000 / self.fonts.?.units[face], @as(f64, @floatFromInt(metrics.descender)) * 1000 / self.fonts.?.units[face], first }));
            const mapping = try self.allocator.alloc(u8, (glyphs.items.len + 1) * 2);
            @memset(mapping, 0);
            for (glyphs.items, 0..) |glyph, index| std.mem.writeInt(u16, mapping[(index + 1) * 2 ..][0..2], glyph.id, .big);
            try self.appendObject(try self.stream("", mapping));
            var unicode = Writer.init(self.allocator, self.run.limits.max_source_bytes);
            defer unicode.deinit();
            try unicode.raw("/CIDInit /ProcSet findresource begin 12 dict begin begincmap /CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def /CMapName /AntflyUnicode def /CMapType 2 def 1 begincodespacerange <0000> <FFFF> endcodespacerange\n");
            var cursor: usize = 0;
            while (cursor < glyphs.items.len) {
                const end = cursor + @min(100, glyphs.items.len - cursor);
                try print(&unicode, "{d} beginbfchar\n", .{end - cursor});
                for (glyphs.items[cursor..end], cursor..) |glyph, index| {
                    try print(&unicode, "<{x:0>4}> <", .{index + 1});
                    if (glyph.codepoint <= 0xffff) try print(&unicode, "{x:0>4}", .{glyph.codepoint}) else {
                        const scalar: u32 = glyph.codepoint - 0x10000;
                        try print(&unicode, "{x:0>4}{x:0>4}", .{ @as(u32, 0xd800) + (scalar >> 10), @as(u32, 0xdc00) + (scalar & 1023) });
                    }
                    try unicode.raw(">\n");
                }
                try unicode.raw("endbfchar\n");
                cursor = end;
            }
            try unicode.raw("endcmap CMapName currentdict /CMap defineresource pop end end\n");
            try self.appendObject(try self.stream("", unicode.writer.buffered()));
            var widths = Writer.init(self.allocator, self.run.limits.max_source_bytes);
            defer widths.deinit();
            try print(&widths, "<< /Type /Font /Subtype /CIDFontType2 /BaseFont /AntflyRoboto{d} /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /FontDescriptor {d} 0 R /CIDToGIDMap {d} 0 R /DW 1000 /W [1 [", .{ face, first + 1, first + 2 });
            for (glyphs.items) |glyph| try print(&widths, "{d} ", .{@as(f64, @floatFromInt(try self.fonts.?.faces[face].advanceWidth(glyph.id))) * 1000 / self.fonts.?.units[face]});
            try widths.raw("]] >>");
            try self.appendObject(try widths.toOwnedSlice());
            try self.appendObject(try std.fmt.allocPrint(self.allocator, "<< /Type /Font /Subtype /Type0 /BaseFont /AntflyRoboto{d} /Encoding /Identity-H /DescendantFonts [{d} 0 R] /ToUnicode {d} 0 R >>", .{ face, first + 4, first + 3 }));
            try print(&self.font_resources, "/Ft{d} {d} 0 R ", .{ face, first + 5 });
        }
    }

    fn appendObject(self: *Builder, object: []const u8) !void {
        if (object.len > self.run.limits.max_source_bytes -| self.object_bytes) return error.DocumentOutputTooLarge;
        try self.objects.append(self.allocator, object);
        self.object_bytes += object.len;
    }

    fn stream(self: *Builder, attributes: []const u8, bytes: []const u8) ![]const u8 {
        var out = Writer.init(self.allocator, self.run.limits.max_source_bytes);
        defer out.deinit();
        try print(&out, "<< {s} /Length {d} >>\nstream\n", .{ attributes, bytes.len });
        try out.raw(bytes);
        try out.raw("\nendstream");
        return out.toOwnedSlice();
    }
    fn compressedSamples(self: *Builder, rgba: []const u8, width: u32, height: u32, alpha: bool) ![]const u8 {
        var out = Writer.init(self.allocator, self.run.limits.max_source_bytes);
        defer out.deinit();
        try out.reserve(16);
        var history: [std.compress.flate.max_window_len]u8 = undefined;
        var compressor = try std.compress.flate.Compress.init(&out.writer, &history, .zlib, .default);
        const channels: usize = if (alpha) 1 else 3;
        const row = try self.allocator.alloc(u8, @as(usize, width) * channels);
        defer self.allocator.free(row);
        for (0..height) |y| {
            try self.run.control.check();
            for (0..width) |x| {
                const pixel = rgba[(@as(usize, y) * width + x) * 4 ..][0..4];
                if (alpha) row[x] = pixel[3] else @memcpy(row[x * 3 ..][0..3], pixel[0..3]);
            }
            compressor.writer.writeAll(row) catch |err| return out.failure orelse err;
        }
        compressor.finish() catch |err| return out.failure orelse err;
        return out.toOwnedSlice();
    }
    fn picture(self: *Builder, path: []const u8, id: []const u8) !usize {
        const relationships = (try self.links.relationshipsFor(self.allocator, self.archive, path)) orelse return error.MissingDocumentPart;
        const related = try relationships.get(id, .image);
        const slash = std.mem.lastIndexOfScalar(u8, path, '/') orelse return error.InvalidDocumentSource;
        const target = try parts.normalizedPart(self.allocator, path[0 .. slash + 1], related.target);
        if (self.images.get(target)) |known| return known;
        const encoded = try self.archive.get(self.allocator, target);
        switch (image.detectFormat(encoded)) {
            .png, .jpeg, .gif, .bmp, .webp => {},
            // SVG/EMF/WMF and other unsupported embedded graphics must retain
            // unresolved visual coverage rather than become transport failures.
            else => return error.DocumentRendererUnavailable,
        }
        const info = try image.inspectEncoded(encoded);
        if (info.width > self.run.limits.max_dimension or info.height > self.run.limits.max_dimension or try info.pixels() > self.run.limits.max_pixels) return error.DocumentPixelLimitExceeded;
        try self.run.pixels(try info.pixels());
        const decoded = @import("../pipelines/image.zig").decodeRgba(self.allocator, encoded) catch |err| switch (err) {
            error.ImageDecodeFailed => return error.DocumentRendererUnavailable,
            else => return err,
        };
        defer decoded.deinit(self.allocator);
        const rgb = try self.compressedSamples(decoded.data, decoded.width, decoded.height, false);
        var is_opaque = true;
        var offset: usize = 3;
        while (offset < decoded.data.len) : (offset += 4) if (decoded.data[offset] != 255) {
            is_opaque = false;
            break;
        };
        var mask: ?usize = null;
        if (!is_opaque) {
            const alpha = try self.compressedSamples(decoded.data, decoded.width, decoded.height, true);
            mask = self.objects.items.len + 1;
            const attrs = try std.fmt.allocPrint(self.allocator, "/Type /XObject /Subtype /Image /Width {d} /Height {d} /ColorSpace /DeviceGray /BitsPerComponent 8 /Interpolate true /Filter /FlateDecode", .{ decoded.width, decoded.height });
            try self.appendObject(try self.stream(attrs, alpha));
        }
        const index = self.objects.items.len + 1;
        const smask = if (mask) |m| try std.fmt.allocPrint(self.allocator, "/SMask {d} 0 R", .{m}) else "";
        const attrs = try std.fmt.allocPrint(self.allocator, "/Type /XObject /Subtype /Image /Width {d} /Height {d} /ColorSpace /DeviceRGB /BitsPerComponent 8 /Interpolate true /Filter /FlateDecode {s}", .{ decoded.width, decoded.height, smask });
        try self.appendObject(try self.stream(attrs, rgb));
        try print(&self.resources, "/Im{d} {d} 0 R ", .{ index, index });
        try self.images.put(self.allocator, target, index);
        return index;
    }
    fn paint(self: *Builder, path: []const u8, drawing: Drawing) !void {
        if (self.drawings >= self.run.limits.max_regions) return error.DocumentRegionLimitExceeded;
        self.drawings += 1;
        if (!drawing.positioned or !drawing.sized or drawing.width <= 0 or drawing.height <= 0) return error.DocumentRendererUnavailable;
        if (drawing.picture and (drawing.geometry != .rect or drawing.fill != null or drawing.stroke != null)) return error.DocumentRendererUnavailable;
        const w = drawing.width;
        const h = drawing.height;
        const theta = drawing.rotation * std.math.pi / 180;
        const cos = @cos(theta);
        const sin = @sin(theta);
        const sx: f64 = if (drawing.flip_x) -1 else 1;
        const sy: f64 = if (drawing.flip_y) -1 else 1;
        const a = cos * sx;
        const b = sin * sx;
        const cc = -sin * sy;
        const d = cos * sy;
        try print(&self.content, "q {d} {d} {d} {d} {d} {d} cm\n", .{ a, b, cc, d, drawing.x + w / 2 - a * w / 2 - cc * h / 2, drawing.y + h / 2 - b * w / 2 - d * h / 2 });
        if (drawing.fill_alpha != 1 or drawing.stroke_alpha != 1) {
            try self.setAlpha(drawing.fill_alpha, drawing.stroke_alpha);
        }
        if (drawing.group) {
            const child_w = drawing.child_width orelse return error.DocumentRendererUnavailable;
            const child_h = drawing.child_height orelse return error.DocumentRendererUnavailable;
            if (child_w <= 0 or child_h <= 0 or drawing.fill != null or drawing.stroke != null) return error.DocumentRendererUnavailable;
            const scale_x = w / child_w;
            const scale_y = h / child_h;
            try print(&self.content, "{d} 0 0 {d} {d} {d} cm\n", .{ scale_x, scale_y, -drawing.child_x * scale_x, -drawing.child_y * scale_y });
            return; // The matching grpSp close restores this graphics state.
        }
        if (drawing.graphic) {
            try self.paintChart(path, drawing.chart orelse return error.DocumentRendererUnavailable, w, h);
            try self.content.raw("Q\n");
            return;
        }
        if (drawing.picture) {
            const index = try self.picture(path, drawing.embed orelse return error.DocumentRendererUnavailable);
            const crop = drawing.crop;
            const visible_w = 1 - crop[0] - crop[2];
            const visible_h = 1 - crop[1] - crop[3];
            if (visible_w <= 0 or visible_h <= 0) return error.InvalidDocumentXml;
            // PDF image sample rows run downward, while image-space Y runs
            // upward. Undo that flip within the slide's downward coordinate CTM.
            try print(&self.content, "0 0 {d} {d} re W n {d} 0 0 {d} {d} {d} cm /Im{d} Do\n", .{ w, h, w / visible_w, -h / visible_h, -w * crop[0] / visible_w, h * (1 - crop[1]) / visible_h, index });
        } else {
            if (drawing.fill) |color| try print(&self.content, "{d} {d} {d} rg\n", .{ @as(f64, @floatFromInt(color[0])) / 255, @as(f64, @floatFromInt(color[1])) / 255, @as(f64, @floatFromInt(color[2])) / 255 });
            if (drawing.stroke) |color| try print(&self.content, "{d} {d} {d} RG {d} w\n", .{ @as(f64, @floatFromInt(color[0])) / 255, @as(f64, @floatFromInt(color[1])) / 255, @as(f64, @floatFromInt(color[2])) / 255, drawing.stroke_width });
            switch (drawing.geometry) {
                .rect => try print(&self.content, "0 0 {d} {d} re\n", .{ w, h }),
                .line => try print(&self.content, "0 0 m {d} {d} l\n", .{ w, h }),
                .ellipse => {
                    const k = 0.5522847498307936;
                    const rx = w / 2;
                    const ry = h / 2;
                    try print(&self.content, "{d} 0 m {d} 0 {d} {d} {d} {d} c {d} {d} {d} {d} {d} {d} c {d} {d} {d} {d} 0 {d} c 0 {d} {d} 0 {d} 0 c h\n", .{ rx, rx + k * rx, w, ry - k * ry, w, ry, w, ry + k * ry, rx + k * rx, h, rx, h, rx - k * rx, h, 0.0, ry + k * ry, ry, ry - k * ry, rx - k * rx, rx });
                },
            }
            try self.content.raw(if (drawing.fill != null and drawing.stroke != null) "B\n" else if (drawing.fill != null) "f\n" else if (drawing.stroke != null) "S\n" else "n\n");
        }
        if (drawing.text_body) |body| try self.paintText(body, w, h);
        try self.content.raw("Q\n");
    }
    fn layer(self: *Builder, layer_value: Layer) !void {
        if (layer_value.view.unsupported_content) return error.DocumentRendererUnavailable;
        self.theme = layer_value.theme;
        var iter = xml.QualifiedXml.init(self.allocator, layer_value.view.bytes);
        defer iter.deinit();
        var drawing: ?Drawing = null;
        var xfrm: ?usize = null;
        var solid: ?usize = null;
        var line: ?usize = null;
        var skip: ?usize = null;
        var root_group: ?usize = null;
        var group_properties: ?usize = null;
        var groups: [64]struct { depth: usize, transformed: bool } = undefined;
        var group_count: usize = 0;
        while (try iter.next()) |token| {
            try self.run.control.check();
            if (token != .tag) continue;
            const item = token.tag;
            const tag = item.value;
            const name = tag.local();
            if (skip) |depth| {
                if (tag.closing and item.depth == depth) skip = null;
                continue;
            }
            if (tag.closing) {
                if (root_group == item.depth) root_group = null;
                if (group_properties == item.depth) group_properties = null;
                if (xfrm == item.depth) {
                    if (drawing) |value| if (value.group) {
                        try self.paint(layer_value.path, value);
                        groups[group_count - 1].transformed = true;
                        drawing = null;
                    };
                    xfrm = null;
                }
                if (solid == item.depth) solid = null;
                if (line == item.depth) line = null;
                if (drawing) |value| if (item.depth == value.depth) {
                    try self.paint(layer_value.path, value);
                    drawing = null;
                };
                if (group_count > 0 and item.depth == groups[group_count - 1].depth) {
                    if (!groups[group_count - 1].transformed) return error.DocumentRendererUnavailable;
                    group_count -= 1;
                    try self.content.raw("Q\n");
                }
                continue;
            }
            if (item.namespace == .presentation) {
                // The shared checked view already enforced owner visibility.
                // Editing locks, names and application metadata do not paint.
                if (eq(name, "nvPicPr") or eq(name, "nvSpPr") or eq(name, "nvCxnSpPr") or eq(name, "nvGrpSpPr") or eq(name, "nvGraphicFramePr")) {
                    if (!tag.empty) skip = item.depth;
                    continue;
                }
                if ((!layer_value.shapes and (eq(name, "sp") or eq(name, "pic") or eq(name, "grpSp") or eq(name, "graphicFrame") or eq(name, "cxnSp"))) or (!layer_value.background and eq(name, "bg"))) {
                    if (!tag.empty) skip = item.depth;
                    continue;
                }
                if (eq(name, "bg")) {
                    if (drawing != null or tag.empty) return error.InvalidDocumentXml;
                    drawing = .{ .picture = false, .depth = item.depth, .positioned = true, .sized = true, .width = self.size.width / emu_per_point, .height = self.size.height / emu_per_point };
                }
                if (eq(name, "grpSpPr") and drawing == null and group_count == 0 and !tag.empty) root_group = item.depth;
                if (eq(name, "grpSpPr") and group_count > 0 and !tag.empty) group_properties = item.depth;
                if (eq(name, "grpSp")) {
                    if (drawing != null or tag.empty) return error.InvalidDocumentXml;
                    if (group_count == groups.len) return error.DocumentRendererUnavailable;
                    groups[group_count] = .{ .depth = item.depth, .transformed = false };
                    group_count += 1;
                    drawing = .{ .picture = false, .group = true, .depth = item.depth };
                }
                if (eq(name, "txBody")) {
                    if (drawing == null or drawing.?.text_body != null) return error.InvalidDocumentXml;
                    drawing.?.text_body = try text.parse(self.allocator, &iter, tag, self.theme, self.run.control, @min(self.run.limits.max_output_bytes, 1_000_000));
                    continue;
                }
                if (eq(name, "graphicFrame")) {
                    if (drawing != null or tag.empty) return error.InvalidDocumentXml;
                    drawing = .{ .picture = false, .graphic = true, .depth = item.depth };
                }
                if (eq(name, "xfrm") and drawing != null and drawing.?.graphic) xfrm = item.depth;
                if (eq(name, "contentPart") or eq(name, "style") or eq(name, "bgRef")) return error.DocumentRendererUnavailable;
                if (eq(name, "blipFill") and !try tag.booleanAttr("rotWithShape", true)) return error.DocumentRendererUnavailable;
                if (eq(name, "sp") or eq(name, "pic") or eq(name, "cxnSp")) {
                    if (drawing != null or tag.empty) return error.InvalidDocumentXml;
                    drawing = .{ .picture = eq(name, "pic"), .depth = item.depth };
                }
            }
            if (item.namespace == .chart and eq(name, "chart") and drawing != null and drawing.?.graphic) {
                if (drawing.?.chart != null) return error.InvalidDocumentXml;
                drawing.?.chart = try xml.decodedText(self.allocator, (try iter.attribute(tag, .office_relationships, "id")) orelse return error.InvalidDocumentXml);
                continue;
            }
            if (root_group != null and item.namespace == .drawing) {
                if (eq(name, "off") or eq(name, "chOff")) {
                    if (try number(self.allocator, tag, "x") != 0 or try number(self.allocator, tag, "y") != 0) return error.DocumentRendererUnavailable;
                } else if (eq(name, "ext") or eq(name, "chExt")) {
                    if (try number(self.allocator, tag, "cx") != 0 or try number(self.allocator, tag, "cy") != 0) return error.DocumentRendererUnavailable;
                } else if (eq(name, "xfrm")) {
                    if ((try tag.uniqueAttr("rot") != null and try number(self.allocator, tag, "rot") != 0) or try tag.booleanAttr("flipH", false) or try tag.booleanAttr("flipV", false)) return error.DocumentRendererUnavailable;
                } else return error.DocumentRendererUnavailable;
                continue;
            }
            if (group_properties != null and drawing == null and item.namespace == .drawing) return error.DocumentRendererUnavailable;
            if (item.namespace != .drawing or drawing == null) continue;
            const current = &drawing.?;
            if (eq(name, "xfrm")) {
                if (xfrm != null or current.positioned or current.sized) return error.InvalidDocumentXml;
                xfrm = item.depth;
                if (try tag.uniqueAttr("rot") != null) current.rotation = try number(self.allocator, tag, "rot") / 60000;
                current.flip_x = try tag.booleanAttr("flipH", false);
                current.flip_y = try tag.booleanAttr("flipV", false);
            } else if (eq(name, "off") and xfrm != null and item.depth == xfrm.? + 1) {
                if (current.positioned) return error.InvalidDocumentXml;
                current.x = try number(self.allocator, tag, "x") / emu_per_point;
                current.y = try number(self.allocator, tag, "y") / emu_per_point;
                current.positioned = true;
            } else if (eq(name, "ext") and xfrm != null and item.depth == xfrm.? + 1) {
                if (current.sized) return error.InvalidDocumentXml;
                current.width = try number(self.allocator, tag, "cx") / emu_per_point;
                current.height = try number(self.allocator, tag, "cy") / emu_per_point;
                current.sized = true;
            } else if (eq(name, "chOff") and current.group and xfrm != null and item.depth == xfrm.? + 1) {
                current.child_x = try number(self.allocator, tag, "x") / emu_per_point;
                current.child_y = try number(self.allocator, tag, "y") / emu_per_point;
            } else if (eq(name, "chExt") and current.group and xfrm != null and item.depth == xfrm.? + 1) {
                if (current.child_width != null or current.child_height != null) return error.InvalidDocumentXml;
                current.child_width = try number(self.allocator, tag, "cx") / emu_per_point;
                current.child_height = try number(self.allocator, tag, "cy") / emu_per_point;
            } else if (eq(name, "blip")) {
                if (current.embed != null or !current.picture or !tag.empty) return error.DocumentRendererUnavailable;
                const raw = (try iter.attribute(tag, .office_relationships, "embed")) orelse return error.DocumentRendererUnavailable;
                current.embed = try xml.decodedText(self.allocator, raw);
            } else if (eq(name, "srcRect")) {
                inline for (.{ "l", "t", "r", "b" }, 0..) |attr, index| if (try tag.uniqueAttr(attr) != null) {
                    current.crop[index] = try number(self.allocator, tag, attr) / 100000;
                    if (@abs(current.crop[index]) > 10) return error.DocumentRendererUnavailable;
                };
            } else if (eq(name, "ln")) {
                line = item.depth;
                inline for (.{ .{ "cap", "flat" }, .{ "cmpd", "sng" }, .{ "algn", "ctr" } }) |attribute| if (try tag.uniqueAttr(attribute[0])) |raw| {
                    if (!eq(try xml.decodedText(self.allocator, raw), attribute[1])) return error.DocumentRendererUnavailable;
                };
                if (try tag.uniqueAttr("w") != null) current.stroke_width = try number(self.allocator, tag, "w") / emu_per_point;
                if (current.stroke_width < 0) return error.InvalidDocumentXml;
            } else if (eq(name, "solidFill")) {
                if (tag.empty) return error.InvalidDocumentXml;
                solid = item.depth;
            } else if ((eq(name, "srgbClr") or eq(name, "schemeClr") or eq(name, "sysClr")) and solid != null and item.depth == solid.? + 1) {
                const color = try self.theme.readColor(self.allocator, &iter, tag, self.run.control);
                if (line != null) {
                    current.stroke = color.rgb;
                    current.stroke_alpha = color.alpha;
                } else {
                    current.fill = color.rgb;
                    current.fill_alpha = color.alpha;
                }
            } else if (eq(name, "noFill")) {
                if (line != null) current.stroke = null else current.fill = null;
            } else if (eq(name, "prstGeom")) {
                const preset = try xml.decodedText(self.allocator, (try tag.uniqueAttr("prst")) orelse return error.InvalidDocumentXml);
                current.geometry = if (eq(preset, "rect")) .rect else if (eq(preset, "ellipse")) .ellipse else if (eq(preset, "line")) .line else return error.DocumentRendererUnavailable;
            } else {
                var known = false;
                for ([_][]const u8{ "avLst", "stretch", "fillRect", "effectLst", "extLst", "graphic", "graphicData" }) |allowed| known = known or eq(name, allowed);
                if (!known) return error.DocumentRendererUnavailable;
                if (eq(name, "fillRect")) inline for (.{ "l", "t", "r", "b" }) |attr| if (try tag.uniqueAttr(attr) != null and try number(self.allocator, tag, attr) != 0) return error.DocumentRendererUnavailable;
            }
            if (tag.empty) {
                if (xfrm == item.depth) xfrm = null;
                if (solid == item.depth) solid = null;
                if (line == item.depth) line = null;
            }
        }
    }
    fn finish(self: *Builder) ![]u8 {
        try self.content.raw("Q\n");
        try self.finishFonts();
        self.objects.items[0] = "<< /Type /Catalog /Pages 2 0 R >>";
        self.objects.items[1] = "<< /Type /Pages /Kids [3 0 R] /Count 1 >>";
        self.objects.items[2] = try std.fmt.allocPrint(self.allocator, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {d} {d}] /Resources << /XObject << {s} >> /ExtGState << {s} >> /Font << {s} >> >> /Contents 4 0 R >>", .{ self.size.width / emu_per_point, self.size.height / emu_per_point, self.resources.writer.buffered(), self.states.writer.buffered(), self.font_resources.writer.buffered() });
        self.objects.items[3] = try self.stream("", self.content.writer.buffered());
        var out = Writer.init(self.allocator, self.run.limits.max_source_bytes);
        defer out.deinit();
        try out.raw("%PDF-1.7\n");
        const offsets = try self.allocator.alloc(usize, self.objects.items.len);
        for (self.objects.items, 0..) |object, i| {
            try self.run.control.check();
            offsets[i] = out.len();
            try print(&out, "{d} 0 obj\n{s}\nendobj\n", .{ i + 1, object });
        }
        const start = out.len();
        try print(&out, "xref\n0 {d}\n0000000000 65535 f \n", .{self.objects.items.len + 1});
        for (offsets) |offset| try print(&out, "{d:0>10} 00000 n \n", .{offset});
        try print(&out, "trailer\n<< /Size {d} /Root 1 0 R >>\nstartxref\n{d}\n%%EOF\n", .{ self.objects.items.len + 1, start });
        return out.toOwnedSlice();
    }
};

/// The caller owns one source/page arena. Images are decoded once per part
/// target and retain alpha through native PDF soft masks.
pub fn compile(a: A, archive: xml.Archive, size: Size, layers: []const Layer, run: *c.Run) ![]u8 {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const work = scratch.allocator();
    var builder = Builder{ .allocator = work, .scratch_backing = run.work_allocator orelse a, .archive = archive, .size = size, .run = run, .content = Writer.init(work, run.limits.max_source_bytes), .resources = Writer.init(work, run.limits.max_source_bytes), .states = Writer.init(work, run.limits.max_source_bytes), .font_resources = Writer.init(work, run.limits.max_source_bytes) };
    defer builder.content.deinit();
    defer builder.resources.deinit();
    defer builder.states.deinit();
    defer builder.font_resources.deinit();
    try builder.objects.appendSlice(work, &.{ "", "", "", "" });
    try print(&builder.content, "q 1 0 0 -1 0 {d} cm\n", .{size.height / emu_per_point});
    // Every background is beneath every shape, even if the nearest background
    // belongs to a layout above a visible master's decorations.
    for (layers) |layer_value| if (layer_value.background) {
        var background = layer_value;
        background.shapes = false;
        try builder.layer(background);
    };
    for (layers) |layer_value| if (layer_value.shapes) {
        var shapes = layer_value;
        shapes.background = false;
        try builder.layer(shapes);
    };
    return a.dupe(u8, try builder.finish());
}

test "document native slide dimensions require direct qualified metadata" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const size = try presentationSize(work, "<p:presentation xmlns:p='http://purl.oclc.org/ooxml/presentationml/main'><p:sldSz cx='9144000' cy='5143500'/><p:extLst><p:sldSz cx='1' cy='1'/></p:extLst></p:presentation>", .{});
    try std.testing.expectEqual(@as(f64, 9144000), size.width);
    try std.testing.expectEqual(@as(f64, 5143500), size.height);
    try std.testing.expectError(error.DocumentRendererUnavailable, presentationSize(work, "<p:presentation xmlns:p='http://purl.oclc.org/ooxml/presentationml/main'><sldSz cx='1' cy='1'/></p:presentation>", .{}));
}

test "document native text display retains Unicode through embedded CID fonts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const archive = try xml.Archive.init(a, @embedFile("testdata/native-themed-text.pptx"), 1024 * 1024);
    const view = try xml.View.init(a, try archive.get(a, "ppt/slides/slide1.xml"));
    const theme = try Theme.parse(a, try archive.get(a, "ppt/theme/theme1.xml"), .{});
    var run = c.Run{ .limits = .{} };
    defer run.deinit();
    const bytes = try compile(a, archive, .{ .width = 2540000, .height = 1270000 }, &.{.{ .path = "ppt/slides/slide1.xml", .view = view, .theme = theme }}, &run);
    var reader = try @import("antfly_pdf").reader.Reader.init(a, bytes);
    defer reader.deinit();
    const result = try reader.extractPageTextAlloc(1);
    try std.testing.expect(std.mem.indexOf(u8, result, "Invoice") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "€12.50") != null);
}

test "document native display compilation frees failed allocations and bounds drawings before painting" {
    const Check = struct {
        fn exercise(a: A, input: []const u8, with_theme: bool) !void {
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const work = scratch.allocator();
            const archive = try xml.Archive.init(work, input, 1024 * 1024);
            const view = try xml.View.init(work, try archive.get(work, "ppt/slides/slide1.xml"));
            const theme = if (with_theme) try Theme.parse(work, try archive.get(work, "ppt/theme/theme1.xml"), .{}) else Theme{};
            var run = c.Run{ .limits = .{} };
            defer run.deinit();
            const bytes = try compile(a, archive, .{ .width = 2540000, .height = 1270000 }, &.{.{ .path = "ppt/slides/slide1.xml", .view = view, .theme = theme }}, &run);
            defer a.free(bytes);
            try std.testing.expect(std.mem.startsWith(u8, bytes, "%PDF-"));
        }
    };
    // In-place arena growth can depend on the backing address chosen for a
    // prior trial. Force all growth through allocation so every failure index
    // is deterministic and every induced OOM must propagate without leaks.
    const NoGrowth = struct {
        backing: A,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(bytes, alignment, ra);
        }
    };
    var no_growth = NoGrowth{ .backing = std.testing.allocator };
    const failing_backing: A = .{ .ptr = &no_growth, .vtable = &.{ .alloc = NoGrowth.alloc, .free = NoGrowth.free, .resize = A.noResize, .remap = A.noRemap } };
    try std.testing.checkAllAllocationFailures(failing_backing, Check.exercise, .{ @as([]const u8, @embedFile("testdata/native-pictures.pptx")), false });
    try std.testing.checkAllAllocationFailures(failing_backing, Check.exercise, .{ @as([]const u8, @embedFile("testdata/native-themed-text.pptx")), true });
    try std.testing.checkAllAllocationFailures(failing_backing, Check.exercise, .{ @as([]const u8, @embedFile("testdata/native-chart.pptx")), true });
    var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const archive = try xml.Archive.init(a, @embedFile("testdata/native-pictures.pptx"), 1024 * 1024);
    const view = try xml.View.init(a, try archive.get(a, "ppt/slides/slide1.xml"));
    var run = c.Run{ .limits = .{ .max_regions = 1 } };
    defer run.deinit();
    try std.testing.expectError(error.DocumentRegionLimitExceeded, compile(a, archive, .{ .width = 2540000, .height = 1270000 }, &.{.{ .path = "ppt/slides/slide1.xml", .view = view }}, &run));
    try std.testing.expectEqual(@as(u64, 0), run.usage.rendered_pixels);
}
