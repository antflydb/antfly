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

//! Native paragraph layout with per-run metrics, wrapping and explicit unknown
//! glyph rejection. Layout and rendering share immutable bundled SFNT faces.
const std = @import("std");
const xml = @import("ooxml.zig");
const themes = @import("drawingml_theme.zig");
const c = @import("types.zig");
const native = @import("antfly_pdf").native_fonts;
const A = std.mem.Allocator;
pub const Style = struct { size: f64 = 18, bold: bool = false, italic: bool = false, color: themes.Color = .{ .rgb = .{ 0, 0, 0 } }, spacing: f64 = 0, baseline: f64 = 0, family: []const u8 = "Roboto" };
pub const Rune = struct { codepoint: u21, style: Style };
pub const Paragraph = struct { text: []const Rune, alignment: enum { left, center, right } = .left, before: f64 = 0, after: f64 = 0, line_height: f64 = 1.2, margin: f64 = 0, indent: f64 = 0 };
pub const Body = struct { paragraphs: []const Paragraph, inset: [4]f64 = .{ 7.2, 3.6, 7.2, 3.6 }, wrap: bool = true, anchor: enum { top, center, bottom } = .top, scale: f64 = 1 };
pub const Glyph = struct { id: u16, codepoint: u21, x: f64, y: f64, style: Style };
pub const Fonts = struct {
    faces: [2]native.sfnt.Font,
    units: [2]f64,
    ascents: [2]f64,
    pub fn init(a: A) !Fonts {
        var result: Fonts = undefined;
        for ([_][]const u8{ native.regular, native.bold }, 0..) |bytes, index| {
            result.faces[index] = try native.sfnt.Font.init(a, bytes);
            result.units[index] = @floatFromInt((try result.faces[index].head()).units_per_em);
            result.ascents[index] = @as(f64, @floatFromInt((try result.faces[index].hhea()).ascender)) / result.units[index];
        }
        return result;
    }
    fn glyph(self: Fonts, rune: Rune) !struct { id: u16, advance: f64 } {
        if (std.ascii.startsWithIgnoreCase(rune.style.family, "wingdings") or std.ascii.eqlIgnoreCase(rune.style.family, "symbol")) return error.DocumentRendererUnavailable;
        // Complex-script shaping is not represented by one cmap glyph per
        // scalar. Keep those scripts explicit rather than painting wrong text.
        if ((rune.codepoint >= 0x590 and rune.codepoint <= 0x1cff) or (rune.codepoint >= 0xfb1d and rune.codepoint <= 0xfeff)) return error.DocumentRendererUnavailable;
        const index = @intFromBool(rune.style.bold);
        const id = (try self.faces[index].cmapGlyphIndex(rune.codepoint)) orelse return error.DocumentRendererUnavailable;
        if (id == 0) return error.DocumentRendererUnavailable;
        return .{ .id = id, .advance = @as(f64, @floatFromInt(try self.faces[index].advanceWidth(id))) / self.units[index] * rune.style.size + rune.style.spacing };
    }
};
fn eq(left: []const u8, right: []const u8) bool {
    return std.mem.eql(u8, left, right);
}
fn numeric(a: A, tag: xml.Tag, attr: []const u8, divisor: f64) !f64 {
    const raw = (try tag.uniqueAttr(attr)) orelse return error.InvalidDocumentXml;
    const integer = std.fmt.parseInt(i64, try xml.decodedText(a, raw), 10) catch return error.InvalidDocumentXml;
    if (@abs(@as(f64, @floatFromInt(integer))) > 1e12) return error.InvalidDocumentXml;
    return @as(f64, @floatFromInt(integer)) / divisor;
}
fn properties(a: A, tag: xml.Tag, inherited: Style) !Style {
    var result = inherited;
    if (try tag.uniqueAttr("sz") != null) result.size = try numeric(a, tag, "sz", 100);
    if (result.size <= 0 or result.size > 4000) return error.DocumentRendererUnavailable;
    result.bold = try tag.booleanAttr("b", inherited.bold);
    result.italic = try tag.booleanAttr("i", inherited.italic);
    if (try tag.uniqueAttr("spc") != null) result.spacing = try numeric(a, tag, "spc", 100);
    if (try tag.uniqueAttr("baseline") != null) result.baseline = try numeric(a, tag, "baseline", 100000);
    if (try tag.uniqueAttr("strike")) |raw| if (!eq(raw, "noStrike")) return error.DocumentRendererUnavailable;
    if (try tag.uniqueAttr("u")) |raw| if (!eq(raw, "none")) return error.DocumentRendererUnavailable;
    return result;
}

/// Consume one checked txBody in its original namespace scope.
pub fn parse(a: A, iter: *xml.QualifiedXml, opening: xml.Tag, theme: themes.Theme, control: c.Control, max_chars: usize) !Body {
    if (opening.empty) return .{ .paragraphs = &.{} };
    const depth = iter.xml.depth;
    var body = Body{ .paragraphs = &.{} };
    var paragraphs = std.ArrayList(Paragraph).empty;
    var runes = std.ArrayList(Rune).empty;
    var paragraph = Paragraph{ .text = &.{} };
    var defaults = Style{ .family = theme.minor_latin };
    var style = defaults;
    var paragraph_depth: ?usize = null;
    var run_depth: ?usize = null;
    var property_depth: ?usize = null;
    var default_property = false;
    var text_depth: ?usize = null;
    var spacing: ?enum { before, after, line } = null;
    var spacing_depth: usize = 0;
    var total: usize = 0;
    while (try iter.next()) |token| {
        try control.check();
        if (token == .text) {
            if (text_depth == null) continue;
            const value = try xml.decodedText(a, token.text);
            var unicode = (try std.unicode.Utf8View.init(value)).iterator();
            while (unicode.nextCodepoint()) |codepoint| {
                if (total >= max_chars) return error.DocumentOutputTooLarge;
                try runes.append(a, .{ .codepoint = codepoint, .style = style });
                total += 1;
            }
            continue;
        }
        const item = token.tag;
        const tag = item.value;
        const name = tag.local();
        if (tag.closing) {
            if (item.depth == depth) {
                body.paragraphs = try paragraphs.toOwnedSlice(a);
                return body;
            }
            if (text_depth == item.depth) text_depth = null;
            if (property_depth == item.depth) {
                if (default_property) defaults = style;
                property_depth = null;
            }
            if (run_depth == item.depth) {
                run_depth = null;
                style = defaults;
            }
            if (spacing != null and item.depth == spacing_depth) spacing = null;
            if (paragraph_depth == item.depth) {
                paragraph.text = try runes.toOwnedSlice(a);
                try paragraphs.append(a, paragraph);
                paragraph_depth = null;
            }
            continue;
        }
        if (item.namespace != .drawing) return error.DocumentRendererUnavailable;
        if (eq(name, "bodyPr")) {
            inline for (.{ "lIns", "tIns", "rIns", "bIns" }, 0..) |attr, index| if (try tag.uniqueAttr(attr) != null) {
                body.inset[index] = try numeric(a, tag, attr, 12700);
            };
            if (try tag.uniqueAttr("wrap")) |raw| body.wrap = if (eq(raw, "none")) false else if (eq(raw, "square")) true else return error.DocumentRendererUnavailable;
            if (try tag.uniqueAttr("anchor")) |raw| body.anchor = if (eq(raw, "t")) .top else if (eq(raw, "ctr")) .center else if (eq(raw, "b")) .bottom else return error.DocumentRendererUnavailable;
            if (try tag.uniqueAttr("vert")) |raw| if (!eq(raw, "horz")) return error.DocumentRendererUnavailable;
            if (try tag.uniqueAttr("rot") != null and try numeric(a, tag, "rot", 1) != 0) return error.DocumentRendererUnavailable;
            if (try tag.uniqueAttr("numCol") != null and try numeric(a, tag, "numCol", 1) != 1) return error.DocumentRendererUnavailable;
        } else if (eq(name, "normAutofit")) {
            if (try tag.uniqueAttr("fontScale") != null) body.scale = try numeric(a, tag, "fontScale", 100000);
            if (body.scale <= 0 or body.scale > 1) return error.DocumentRendererUnavailable;
        } else if (eq(name, "p")) {
            if (total >= max_chars) return error.DocumentOutputTooLarge;
            total += 1;
            if (paragraph_depth != null) return error.InvalidDocumentXml;
            if (tag.empty) {
                try paragraphs.append(a, .{ .text = &.{} });
                continue;
            }
            paragraph_depth = item.depth;
            paragraph = .{ .text = &.{} };
            defaults = .{ .family = theme.minor_latin };
            style = defaults;
        } else if (eq(name, "pPr")) {
            if (try tag.uniqueAttr("algn")) |raw| paragraph.alignment = if (eq(raw, "l")) .left else if (eq(raw, "ctr")) .center else if (eq(raw, "r")) .right else return error.DocumentRendererUnavailable;
            if (try tag.uniqueAttr("marL") != null) paragraph.margin = try numeric(a, tag, "marL", 12700);
            if (try tag.uniqueAttr("indent") != null) paragraph.indent = try numeric(a, tag, "indent", 12700);
        } else if (eq(name, "r") or eq(name, "fld")) {
            run_depth = item.depth;
            style = defaults;
        } else if (eq(name, "rPr") or eq(name, "defRPr")) {
            property_depth = if (tag.empty) null else item.depth;
            default_property = eq(name, "defRPr");
            style = try properties(a, tag, if (default_property) defaults else style);
            if (tag.empty and default_property) defaults = style;
        } else if (eq(name, "t")) {
            if (run_depth == null) return error.InvalidDocumentXml;
            text_depth = if (tag.empty) null else item.depth;
        } else if (eq(name, "br")) {
            if (total >= max_chars) return error.DocumentOutputTooLarge;
            total += 1;
            try runes.append(a, .{ .codepoint = '\n', .style = defaults });
        } else if (eq(name, "srgbClr") or eq(name, "schemeClr") or eq(name, "sysClr")) {
            style.color = try theme.readColor(a, iter, tag, control);
        } else if (eq(name, "latin")) {
            const family = try xml.decodedText(a, (try tag.uniqueAttr("typeface")) orelse return error.InvalidDocumentXml);
            style.family = if (eq(family, "+mj-lt")) theme.major_latin else if (eq(family, "+mn-lt")) theme.minor_latin else family;
            // Non-Unicode symbol fonts have different scalar-to-glyph meaning.
            if ((family.len >= 9 and std.ascii.eqlIgnoreCase(family[0..9], "wingdings")) or std.ascii.eqlIgnoreCase(family, "symbol")) return error.DocumentRendererUnavailable;
        } else if (eq(name, "spcBef") or eq(name, "spcAft") or eq(name, "lnSpc")) {
            spacing = if (eq(name, "spcBef")) .before else if (eq(name, "spcAft")) .after else .line;
            spacing_depth = item.depth;
        } else if (eq(name, "spcPts")) {
            const value = try numeric(a, tag, "val", 100);
            if (value < 0) return error.InvalidDocumentXml;
            if (spacing == .before) paragraph.before = value else if (spacing == .after) paragraph.after = value else return error.DocumentRendererUnavailable;
        } else if (eq(name, "spcPct")) {
            if (spacing != .line) return error.DocumentRendererUnavailable;
            paragraph.line_height = try numeric(a, tag, "val", 100000);
            if (paragraph.line_height <= 0 or paragraph.line_height > 1000) return error.DocumentRendererUnavailable;
        } else {
            var known = false;
            for ([_][]const u8{ "lstStyle", "noAutofit", "solidFill", "endParaRPr", "buNone", "ea", "cs" }) |allowed| known = known or eq(name, allowed);
            if (!known) return error.DocumentRendererUnavailable;
        }
    }
    return error.InvalidDocumentXml;
}

pub fn layout(a: A, body: Body, width: f64, height: f64, fonts: Fonts, control: c.Control) ![]const Glyph {
    const available = width - body.inset[0] - body.inset[2];
    if (available <= 0 or height <= 0) return error.DocumentRendererUnavailable;
    var output = std.ArrayList(Glyph).empty;
    var y = body.inset[1];
    for (body.paragraphs) |paragraph| {
        y += paragraph.before;
        var start: usize = 0;
        var first = true;
        while (start < paragraph.text.len) {
            try control.check();
            const left = paragraph.margin + if (first) paragraph.indent else @as(f64, 0);
            var end = start;
            var last_space: ?usize = null;
            var used: f64 = 0;
            var line_size: f64 = 0;
            while (end < paragraph.text.len and paragraph.text[end].codepoint != '\n') : (end += 1) {
                var rune = paragraph.text[end];
                rune.style.size *= body.scale;
                const metric = try fonts.glyph(rune);
                if (body.wrap and end > start and used + metric.advance > available - left) {
                    if (last_space) |space| end = space;
                    break;
                }
                used += metric.advance;
                line_size = @max(line_size, rune.style.size);
                if (rune.codepoint == ' ') last_space = end;
            }
            if (end == start and paragraph.text[end].codepoint != '\n') end += 1;
            used = 0;
            line_size = 0;
            for (paragraph.text[start..end]) |value| {
                var rune = value;
                rune.style.size *= body.scale;
                used += (try fonts.glyph(rune)).advance;
                line_size = @max(line_size, rune.style.size);
            }
            if (line_size == 0) line_size = 18 * body.scale;
            var x = body.inset[0] + left + switch (paragraph.alignment) {
                .left => @as(f64, 0),
                .center => (available - left - used) / 2,
                .right => available - left - used,
            };
            for (paragraph.text[start..end]) |value| {
                var rune = value;
                rune.style.size *= body.scale;
                const metric = try fonts.glyph(rune);
                try output.append(a, .{ .id = metric.id, .codepoint = rune.codepoint, .x = x, .y = y + fonts.ascents[@intFromBool(rune.style.bold)] * line_size - rune.style.baseline * rune.style.size, .style = rune.style });
                x += metric.advance;
            }
            y += line_size * paragraph.line_height;
            start = end;
            if (start < paragraph.text.len and (paragraph.text[start].codepoint == '\n' or paragraph.text[start].codepoint == ' ')) start += 1;
            first = false;
        }
        if (paragraph.text.len == 0) y += 18 * body.scale * paragraph.line_height;
        y += paragraph.after;
    }
    const offset = switch (body.anchor) {
        .top => @as(f64, 0),
        .center => (height - body.inset[3] - y) / 2,
        .bottom => height - body.inset[3] - y,
    };
    if (offset != 0) for (output.items) |*glyph| {
        glyph.y += offset;
    };
    return output.toOwnedSlice(a);
}

test "document native text layout wraps metric glyphs and preserves per-run font and color" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var iter = xml.QualifiedXml.init(a,
        \\<p:txBody xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"><a:bodyPr lIns="0" tIns="0" rIns="0" bIns="0"/><a:lstStyle/><a:p><a:pPr><a:defRPr sz="1200"/></a:pPr><a:r><a:rPr b="1"><a:solidFill><a:srgbClr val="FF0000"/></a:solidFill></a:rPr><a:t>Hello </a:t></a:r><a:r><a:t>world again</a:t></a:r></a:p></p:txBody>
    );
    defer iter.deinit();
    const body = try parse(a, &iter, (try iter.next()).?.tag.value, .{}, .{}, 1000);
    const fonts = try Fonts.init(a);
    const glyphs = try layout(a, body, 45, 100, fonts, .{});
    try std.testing.expect(glyphs.len > 10);
    try std.testing.expect(glyphs[0].style.bold);
    try std.testing.expectEqual(@as(f64, 12), glyphs[0].style.size);
    try std.testing.expectEqualSlices(u8, &.{ 255, 0, 0 }, &glyphs[0].style.color.rgb);
    try std.testing.expect(glyphs[glyphs.len - 1].y > glyphs[0].y);
    try std.testing.expect(!glyphs[glyphs.len - 1].style.bold);
    for (glyphs) |glyph| try std.testing.expect(glyph.id != 0 and glyph.x >= 0);
}

test "document native text bounds empty paragraphs and hard breaks before allocating layout" {
    const fixtures = [_][]const u8{
        "<a:p/><a:p/><a:p/>",
        "<a:p><a:br/><a:br/><a:br/></a:p>",
    };
    for (fixtures) |fixture| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const bytes = try std.fmt.allocPrint(a, "<p:txBody xmlns:p='http://schemas.openxmlformats.org/presentationml/2006/main' xmlns:a='http://schemas.openxmlformats.org/drawingml/2006/main'>{s}</p:txBody>", .{fixture});
        var iter = xml.QualifiedXml.init(a, bytes);
        defer iter.deinit();
        try std.testing.expectError(error.DocumentOutputTooLarge, parse(a, &iter, (try iter.next()).?.tag.value, .{}, .{}, 2));
    }
}
