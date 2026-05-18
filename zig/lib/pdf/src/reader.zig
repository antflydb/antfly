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
const syntax = @import("syntax.zig");
const text_encoding = @import("text_encoding.zig");
const image_lib = @import("antfly_image");
const font_lib = @import("antfly_font");

const Allocator = std.mem.Allocator;

pub const XrefEntry = struct {
    ptr: syntax.ObjRef,
    offset: usize,
    in_use: bool,
    compressed_obj_stream_id: ?u32 = null,
    compressed_index: ?usize = null,
};

const ToUnicodeEntry = struct {
    src: u32,
    src_len: u8,
    dst: []u8,
};

const CodeSpaceRange = struct {
    lo: u32,
    hi: u32,
    len: u8,
};

const FontDecoder = struct {
    code_bytes: usize = 1,
    base_encoding: text_encoding.NamedEncoding = .pdf_doc,
    differences: [256]?u21 = [_]?u21{null} ** 256,
    to_unicode: []ToUnicodeEntry = &.{},
    codespace_ranges: []CodeSpaceRange = &.{},

    fn deinit(self: *FontDecoder, alloc: Allocator) void {
        for (self.to_unicode) |entry| alloc.free(entry.dst);
        if (self.to_unicode.len > 0) alloc.free(self.to_unicode);
        if (self.codespace_ranges.len > 0) alloc.free(self.codespace_ranges);
        self.* = undefined;
    }

    fn decodeAlloc(self: *const FontDecoder, alloc: Allocator, raw: []const u8) ![]u8 {
        if (self.to_unicode.len > 0) {
            return try self.decodeToUnicodeAlloc(alloc, raw);
        }

        var out = std.ArrayList(u8).empty;
        defer out.deinit(alloc);
        for (raw) |b| {
            if (self.differences[b]) |cp| {
                var buf: [4]u8 = undefined;
                const encoded_len = try std.unicode.utf8Encode(cp, &buf);
                try out.appendSlice(alloc, buf[0..encoded_len]);
            } else {
                const decoded = try text_encoding.decodeNamedAlloc(alloc, self.base_encoding, &.{b});
                defer alloc.free(decoded);
                try out.appendSlice(alloc, decoded);
            }
        }
        return try out.toOwnedSlice(alloc);
    }

    fn decodeToUnicodeAlloc(self: *const FontDecoder, alloc: Allocator, raw: []const u8) ![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(alloc);

        var i: usize = 0;
        while (i < raw.len) {
            const step = self.detectCodeWidth(raw[i..]);
            if (i + step > raw.len) break;
            const code_start = i;
            const code = parseRawCode(raw[code_start .. code_start + step]);
            i += step;

            if (self.lookupToUnicode(code, @intCast(step))) |decoded| {
                try out.appendSlice(alloc, decoded);
                continue;
            }

            if (step == 1) {
                const b = raw[code_start];
                if (self.differences[b]) |cp| {
                    var buf: [4]u8 = undefined;
                    const encoded_len = try std.unicode.utf8Encode(cp, &buf);
                    try out.appendSlice(alloc, buf[0..encoded_len]);
                } else {
                    const fallback = try text_encoding.decodeNamedAlloc(alloc, self.base_encoding, raw[code_start .. code_start + 1]);
                    defer alloc.free(fallback);
                    try out.appendSlice(alloc, fallback);
                }
            }
        }

        return try out.toOwnedSlice(alloc);
    }

    fn lookupToUnicode(self: *const FontDecoder, src: u32, src_len: u8) ?[]const u8 {
        for (self.to_unicode) |entry| {
            if (entry.src == src and entry.src_len == src_len) return entry.dst;
        }
        return null;
    }

    fn detectCodeWidth(self: *const FontDecoder, remaining: []const u8) usize {
        if (self.codespace_ranges.len == 0) return self.code_bytes;

        var len: usize = 1;
        while (len <= 4 and len <= remaining.len) : (len += 1) {
            const code = parseRawCode(remaining[0..len]);
            for (self.codespace_ranges) |range| {
                if (range.len == len and code >= range.lo and code <= range.hi) return len;
            }
        }

        return @min(self.code_bytes, remaining.len);
    }
};

const PageFont = struct {
    name: []u8,
    decoder: FontDecoder,
    type3: ?Type3Font = null,
    type1: ?Type1Font = null,
    truetype: ?TrueTypeFont = null,
    cff_otf: ?CffOpenTypeFont = null,

    fn deinit(self: *PageFont, alloc: Allocator) void {
        alloc.free(self.name);
        self.decoder.deinit(alloc);
        if (self.type3) |*type3| type3.deinit(alloc);
        if (self.type1) |*type1| type1.deinit(alloc);
        if (self.truetype) |*truetype| truetype.deinit(alloc);
        if (self.cff_otf) |*cff_otf| cff_otf.deinit(alloc);
        self.* = undefined;
    }
};

const Type1Glyph = struct {
    code: u8,
    name: []u8,
    charstring: []u8,
    advance: f64,

    fn deinit(self: *Type1Glyph, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.charstring);
        self.* = undefined;
    }
};

const Type1Font = struct {
    bytes: []u8,
    local_subrs: [][]u8 = &.{},
    glyphs: []Type1Glyph = &.{},

    fn deinit(self: *Type1Font, alloc: Allocator) void {
        for (self.local_subrs) |subr| alloc.free(subr);
        if (self.local_subrs.len > 0) alloc.free(self.local_subrs);
        for (self.glyphs) |*glyph| glyph.deinit(alloc);
        if (self.glyphs.len > 0) alloc.free(self.glyphs);
        alloc.free(self.bytes);
        self.* = undefined;
    }

    fn glyphForCode(self: *const Type1Font, code: u8) ?*const Type1Glyph {
        for (self.glyphs) |*glyph| {
            if (glyph.code == code) return glyph;
        }
        return null;
    }

    fn glyphForStandardCode(self: *const Type1Font, code: u8) ?*const Type1Glyph {
        const rune = text_encoding.standard_encoding[code];
        for (self.glyphs) |*glyph| {
            const glyph_rune = text_encoding.glyphNameToRune(glyph.name) orelse continue;
            if (glyph_rune == rune) return glyph;
        }
        return null;
    }
};

const TrueTypeFont = struct {
    bytes: []u8,
    font: font_lib.sfnt.Font,
    units_per_em: u16,

    fn deinit(self: *TrueTypeFont, alloc: Allocator) void {
        self.font.deinit(alloc);
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

const CffOpenTypeFont = struct {
    bytes: []u8,
    sfnt: font_lib.sfnt.Font,
    cff: font_lib.cff.Font,
    units_per_em: u16,

    fn deinit(self: *CffOpenTypeFont, alloc: Allocator) void {
        self.cff.deinit(alloc);
        self.sfnt.deinit(alloc);
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

const Type3Glyph = struct {
    code: u8,
    name: []u8,
    content: []u8,
    advance_x: f64,
    advance_y: f64,

    fn deinit(self: *Type3Glyph, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.content);
        self.* = undefined;
    }
};

const Type3Font = struct {
    paint_type: i64 = 0,
    font_matrix: [6]f64 = .{ 0.001, 0, 0, 0.001, 0, 0 },
    glyphs: []Type3Glyph = &.{},

    fn deinit(self: *Type3Font, alloc: Allocator) void {
        for (self.glyphs) |*glyph| glyph.deinit(alloc);
        if (self.glyphs.len > 0) alloc.free(self.glyphs);
        self.* = undefined;
    }

    fn glyphForCode(self: *const Type3Font, code: u8) ?*const Type3Glyph {
        for (self.glyphs) |*glyph| {
            if (glyph.code == code) return glyph;
        }
        return null;
    }
};

const TextExtractionState = struct {
    current_font_index: ?usize = null,
};

pub const TextRun = struct {
    text: []const u8,
    raw_text: ?[]const u8 = null,
    font_index: ?u16 = null,
    vectorizable: bool = false,
    x: f64,
    y: f64,
    font_size: f64,
    a: f64 = 1,
    b: f64 = 0,
    c: f64 = 0,
    d: f64 = 1,
    alpha: u8 = 0xff,
    stroke_alpha: u8 = 0xff,
    render_mode: i64 = 0,
    fill_color: [4]u8 = .{ 0, 0, 0, 0xff },
    stroke_color: [4]u8 = .{ 0, 0, 0, 0xff },
    stroke_width: f64 = 1,
    horizontal_scale: f64 = 1.0,
    char_spacing: f64 = 0,
    word_spacing: f64 = 0,
    advance_width: f64 = 0,
    ascent: f64 = 0,
    descent: f64 = 0,
    paint_order: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    fill_pattern_name: ?[]const u8 = null,
    stroke_pattern_name: ?[]const u8 = null,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,

    pub fn deinit(self: *TextRun, alloc: Allocator) void {
        if (self.clip_points) |points| alloc.free(points);
        if (self.raw_text) |raw| alloc.free(raw);
        alloc.free(self.text);
        self.* = undefined;
    }
};

pub const ImageRun = struct {
    rgba: []u8,
    width: u32,
    height: u32,
    alpha: u8 = 0xff,
    paint_order: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    a: f64,
    b: f64,
    c: f64,
    d: f64,
    e: f64,
    f: f64,
    x: f64,
    y: f64,
    draw_width: f64,
    draw_height: f64,

    pub fn deinit(self: *ImageRun, alloc: Allocator) void {
        if (self.clip_points) |points| alloc.free(points);
        alloc.free(self.rgba);
        self.* = undefined;
    }
};

pub const ShadingRun = struct {
    kind: enum { axial, radial },
    paint_order: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    x0: f64,
    y0: f64,
    r0: f64 = 0,
    x1: f64,
    y1: f64,
    r1: f64 = 0,
    c0: [4]u8,
    c1: [4]u8,
    extend_start: bool = false,
    extend_end: bool = false,

    pub fn deinit(self: *ShadingRun, alloc: Allocator) void {
        if (self.clip_points) |points| alloc.free(points);
        self.* = undefined;
    }
};

pub const PatternRun = struct {
    kind: enum { fill, stroke },
    mode: enum { tiling, shading } = .tiling,
    paint_order: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    fill_rule: FillRule = .nonzero,
    line_cap: @FieldType(ShapeRun, "line_cap") = .butt,
    line_join: @FieldType(ShapeRun, "line_join") = .miter,
    miter_limit: f64 = 10,
    dash_array: ?[]f64 = null,
    dash_phase: f64 = 0,
    stroke_width: f64 = 1,
    closed: bool = true,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    points: [][2]f64,
    pattern_matrix: GraphicsMatrix = .{},
    pattern_bbox: PageBox,
    pattern_x_step: f64,
    pattern_y_step: f64,
    base_color: ?[4]u8 = null,
    shading: ?ShadingRun = null,
    tile_text_runs: []TextRun = &.{},
    tile_image_runs: []ImageRun = &.{},
    tile_shading_runs: []ShadingRun = &.{},
    tile_pattern_runs: []PatternRun = &.{},
    tile_shape_runs: []ShapeRun = &.{},

    pub fn deinit(self: *PatternRun, alloc: Allocator) void {
        if (self.dash_array) |dash| alloc.free(dash);
        if (self.clip_points) |clip| alloc.free(clip);
        alloc.free(self.points);
        if (self.shading) |*shading| shading.deinit(alloc);
        for (self.tile_text_runs) |*run| run.deinit(alloc);
        if (self.tile_text_runs.len > 0) alloc.free(self.tile_text_runs);
        for (self.tile_image_runs) |*run| run.deinit(alloc);
        if (self.tile_image_runs.len > 0) alloc.free(self.tile_image_runs);
        for (self.tile_shading_runs) |*run| run.deinit(alloc);
        if (self.tile_shading_runs.len > 0) alloc.free(self.tile_shading_runs);
        for (self.tile_pattern_runs) |*run| run.deinit(alloc);
        if (self.tile_pattern_runs.len > 0) alloc.free(self.tile_pattern_runs);
        for (self.tile_shape_runs) |*run| run.deinit(alloc);
        if (self.tile_shape_runs.len > 0) alloc.free(self.tile_shape_runs);
        self.* = undefined;
    }
};

pub const PageBox = struct {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
};

const FillRule = enum { nonzero, even_odd };

pub const BlendMode = enum {
    normal,
    multiply,
    screen,
    overlay,
    darken,
    lighten,
};

pub const ShapeRun = struct {
    kind: enum { fill, stroke },
    paint_order: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    fill_rule: FillRule = .nonzero,
    line_cap: enum { butt, round, square } = .butt,
    line_join: enum { miter, round, bevel } = .miter,
    miter_limit: f64 = 10,
    dash_array: ?[]f64 = null,
    dash_phase: f64 = 0,
    color: [4]u8,
    stroke_width: f64,
    closed: bool,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    points: [][2]f64,

    pub fn deinit(self: *ShapeRun, alloc: Allocator) void {
        if (self.dash_array) |dash| alloc.free(dash);
        if (self.clip_points) |clip| alloc.free(clip);
        alloc.free(self.points);
        self.* = undefined;
    }
};

pub const PageRenderRuns = struct {
    page_box: PageBox = .{ .min_x = 0, .min_y = 0, .max_x = 612, .max_y = 792 },
    text_runs: []TextRun = &.{},
    image_runs: []ImageRun = &.{},
    shading_runs: []ShadingRun = &.{},
    pattern_runs: []PatternRun = &.{},
    shape_runs: []ShapeRun = &.{},

    pub fn deinit(self: *PageRenderRuns, alloc: Allocator) void {
        for (self.text_runs) |*run| run.deinit(alloc);
        if (self.text_runs.len > 0) alloc.free(self.text_runs);
        for (self.image_runs) |*run| run.deinit(alloc);
        if (self.image_runs.len > 0) alloc.free(self.image_runs);
        for (self.shading_runs) |*run| run.deinit(alloc);
        if (self.shading_runs.len > 0) alloc.free(self.shading_runs);
        for (self.pattern_runs) |*run| run.deinit(alloc);
        if (self.pattern_runs.len > 0) alloc.free(self.pattern_runs);
        for (self.shape_runs) |*run| run.deinit(alloc);
        if (self.shape_runs.len > 0) alloc.free(self.shape_runs);
        self.* = undefined;
    }
};

const TextRunState = struct {
    current_font_index: ?usize = null,
    font_size: f64 = 12,
    alpha: u8 = 0xff,
    stroke_alpha: u8 = 0xff,
    text_a: f64 = 1,
    text_b: f64 = 0,
    text_c: f64 = 0,
    text_d: f64 = 1,
    horizontal_scale: f64 = 1.0,
    char_spacing: f64 = 0,
    word_spacing: f64 = 0,
    rise: f64 = 0,
    render_mode: i64 = 0,
    fill_color: [4]u8 = .{ 0, 0, 0, 0xff },
    stroke_color: [4]u8 = .{ 0, 0, 0, 0xff },
    stroke_width: f64 = 1,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    fill_color_space: []const u8 = "DeviceGray",
    stroke_color_space: []const u8 = "DeviceGray",
    fill_pattern_name: ?[]const u8 = null,
    stroke_pattern_name: ?[]const u8 = null,
    x: f64 = 0,
    y: f64 = 0,
    line_x: f64 = 0,
    line_y: f64 = 0,
    leading: f64 = 14,
    matrix: GraphicsMatrix = .{},
    clip_box: ?PageBox = null,
};

const TextVerticalMetrics = struct {
    ascent: f64,
    descent: f64,
};

const TextRunStackEntry = struct {
    matrix: GraphicsMatrix,
    alpha: u8,
    stroke_alpha: u8,
    text_a: f64,
    text_b: f64,
    text_c: f64,
    text_d: f64,
    fill_color: [4]u8,
    stroke_color: [4]u8,
    stroke_width: f64,
    blend_mode: BlendMode,
    group_id: ?u32,
    group_parent_id: ?u32,
    group_isolated: bool,
    group_knockout: bool,
    fill_color_space: []const u8,
    stroke_color_space: []const u8,
    fill_pattern_name: ?[]const u8,
    stroke_pattern_name: ?[]const u8,
    clip_box: ?PageBox,
    clip_points: []const [2]f64,
    clip_fill_rule: FillRule,

    fn deinit(self: *TextRunStackEntry, alloc: Allocator) void {
        alloc.free(self.clip_points);
        self.* = undefined;
    }
};

const GraphicsMatrix = struct {
    a: f64 = 1,
    b: f64 = 0,
    c: f64 = 0,
    d: f64 = 1,
    e: f64 = 0,
    f: f64 = 0,
};

const GraphicsState = struct {
    matrix: GraphicsMatrix = .{},
    fill_color: [4]u8 = .{ 0, 0, 0, 0xff },
    stroke_color: [4]u8 = .{ 0, 0, 0, 0xff },
    fill_alpha: u8 = 0xff,
    stroke_alpha: u8 = 0xff,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    fill_color_space: []const u8 = "DeviceGray",
    stroke_color_space: []const u8 = "DeviceGray",
    fill_pattern_name: ?[]const u8 = null,
    stroke_pattern_name: ?[]const u8 = null,
    line_cap: @FieldType(ShapeRun, "line_cap") = .butt,
    line_join: @FieldType(ShapeRun, "line_join") = .miter,
    miter_limit: f64 = 10,
    stroke_width: f64 = 1,
    clip_box: ?PageBox = null,
};

const ShapeStackEntry = struct {
    state: GraphicsState,
    dash_phase: f64,
    dash_array: []f64,
    clip_points: []const [2]f64,
    clip_fill_rule: FillRule,

    fn deinit(self: *ShapeStackEntry, alloc: Allocator) void {
        alloc.free(self.dash_array);
        alloc.free(self.clip_points);
        self.* = undefined;
    }
};

const ImageStackEntry = struct {
    state: GraphicsState,
    clip_points: []const [2]f64,
    clip_fill_rule: FillRule,

    fn deinit(self: *ImageStackEntry, alloc: Allocator) void {
        alloc.free(self.clip_points);
        self.* = undefined;
    }
};

const PageImage = struct {
    name: []u8,
    rgba: []u8,
    width: u32,
    height: u32,

    fn deinit(self: *PageImage, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.rgba);
        self.* = undefined;
    }
};

const PageShading = struct {
    name: []u8,
    kind: @FieldType(ShadingRun, "kind"),
    x0: f64,
    y0: f64,
    r0: f64 = 0,
    x1: f64,
    y1: f64,
    r1: f64 = 0,
    c0: [4]u8,
    c1: [4]u8,
    extend_start: bool = false,
    extend_end: bool = false,

    fn deinit(self: *PageShading, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

const PageExtGState = struct {
    name: []u8,
    fill_alpha: u8 = 0xff,
    stroke_alpha: u8 = 0xff,
    blend_mode: BlendMode = .normal,

    fn deinit(self: *PageExtGState, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

const PagePattern = struct {
    name: []u8,
    kind: enum { tiling, shading } = .tiling,
    content: []u8,
    bbox: PageBox,
    x_step: f64,
    y_step: f64,
    matrix: GraphicsMatrix = .{},
    colored: bool = true,
    shading: ?PageShading = null,
    fonts: []PageFont = &.{},
    images: []PageImage = &.{},
    shadings: []PageShading = &.{},
    patterns: []PagePattern = &.{},
    gstates: []PageExtGState = &.{},
    forms: []PageForm = &.{},

    fn deinit(self: *PagePattern, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.content);
        if (self.shading) |*shading| shading.deinit(alloc);
        for (self.fonts) |*font| font.deinit(alloc);
        if (self.fonts.len > 0) alloc.free(self.fonts);
        for (self.images) |*image| image.deinit(alloc);
        if (self.images.len > 0) alloc.free(self.images);
        for (self.shadings) |*shading| shading.deinit(alloc);
        if (self.shadings.len > 0) alloc.free(self.shadings);
        for (self.patterns) |*pattern| pattern.deinit(alloc);
        if (self.patterns.len > 0) alloc.free(self.patterns);
        for (self.gstates) |*gstate| gstate.deinit(alloc);
        if (self.gstates.len > 0) alloc.free(self.gstates);
        for (self.forms) |*form| form.deinit(alloc);
        if (self.forms.len > 0) alloc.free(self.forms);
        self.* = undefined;
    }
};

const PageForm = struct {
    name: []u8,
    content: []u8,
    matrix: GraphicsMatrix = .{},
    bbox: ?PageBox = null,
    transparency_group: bool = false,
    group_isolated: bool = false,
    group_knockout: bool = false,
    fonts: []PageFont = &.{},
    images: []PageImage = &.{},
    shadings: []PageShading = &.{},
    patterns: []PagePattern = &.{},
    gstates: []PageExtGState = &.{},
    forms: []PageForm = &.{},

    fn deinit(self: *PageForm, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.content);
        for (self.fonts) |*font| font.deinit(alloc);
        if (self.fonts.len > 0) alloc.free(self.fonts);
        for (self.images) |*image| image.deinit(alloc);
        if (self.images.len > 0) alloc.free(self.images);
        for (self.shadings) |*shading| shading.deinit(alloc);
        if (self.shadings.len > 0) alloc.free(self.shadings);
        for (self.patterns) |*pattern| pattern.deinit(alloc);
        if (self.patterns.len > 0) alloc.free(self.patterns);
        for (self.gstates) |*gstate| gstate.deinit(alloc);
        if (self.gstates.len > 0) alloc.free(self.gstates);
        for (self.forms) |*form| form.deinit(alloc);
        if (self.forms.len > 0) alloc.free(self.forms);
        self.* = undefined;
    }
};

const ExponentialTintTransform = struct {
    n: f64,
    c0: []f64,
    c1: []f64,

    fn deinit(self: *ExponentialTintTransform, alloc: Allocator) void {
        alloc.free(self.c0);
        alloc.free(self.c1);
        self.* = undefined;
    }
};

fn extractTextRunsFromContentAppend(
    alloc: Allocator,
    out: *std.ArrayList(TextRun),
    bytes: []const u8,
    fonts: []const PageFont,
    gstates: []const PageExtGState,
    forms: []const PageForm,
) !void {
    var paint_order: usize = 0;
    var next_group_id: u32 = 1;
    return try extractTextRunsFromContentAppendWithState(alloc, out, bytes, fonts, gstates, forms, .{}, &.{}, .nonzero, &paint_order, &next_group_id);
}

fn extractTextRunsFromContentAppendWithState(
    alloc: Allocator,
    out: *std.ArrayList(TextRun),
    bytes: []const u8,
    fonts: []const PageFont,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    initial_state: TextRunState,
    initial_clip_points: []const [2]f64,
    initial_clip_fill_rule: FillRule,
    paint_order: *usize,
    next_group_id: *u32,
) anyerror!void {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();

    var operands = std.ArrayList(syntax.Object).empty;
    defer {
        for (operands.items) |*obj| obj.deinit(alloc);
        operands.deinit(alloc);
    }

    var state = initial_state;
    var stack = std.ArrayList(TextRunStackEntry).empty;
    defer {
        for (stack.items) |*entry| entry.deinit(alloc);
        stack.deinit(alloc);
    }
    var current_path = std.ArrayList([2]f64).empty;
    defer current_path.deinit(alloc);
    var current_path_closed = false;
    var current_clip_points = std.ArrayList([2]f64).empty;
    defer current_clip_points.deinit(alloc);
    var current_clip_fill_rule: FillRule = initial_clip_fill_rule;
    try current_clip_points.appendSlice(alloc, initial_clip_points);
    while (true) {
        var lex = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &lex);

        if (lex == .eof) break;
        if (lex == .keyword and !isContentObjectStartKeyword(lex.keyword)) {
            try applyTextRunOperator(alloc, out, &state, &stack, &current_path, &current_path_closed, &current_clip_points, &current_clip_fill_rule, fonts, gstates, forms, paint_order, next_group_id, lex.keyword, operands.items);
            for (operands.items) |*obj| obj.deinit(alloc);
            operands.clearRetainingCapacity();
            continue;
        }

        try scanner.unreadLexeme(try cloneLexemeForContent(alloc, lex));
        try operands.append(alloc, try scanner.readObject());
    }
}

fn extractImageRunsFromContentAppend(
    alloc: Allocator,
    out: *std.ArrayList(ImageRun),
    bytes: []const u8,
    images: []const PageImage,
    gstates: []const PageExtGState,
    forms: []const PageForm,
) !void {
    var paint_order: usize = 0;
    var next_group_id: u32 = 1;
    return try extractImageRunsFromContentAppendWithState(alloc, out, bytes, images, gstates, forms, .{}, &.{}, .nonzero, &paint_order, &next_group_id);
}

fn extractImageRunsFromContentAppendWithState(
    alloc: Allocator,
    out: *std.ArrayList(ImageRun),
    bytes: []const u8,
    images: []const PageImage,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    initial_state: GraphicsState,
    initial_clip_points: []const [2]f64,
    initial_clip_fill_rule: FillRule,
    paint_order: *usize,
    next_group_id: *u32,
) anyerror!void {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();

    var operands = std.ArrayList(syntax.Object).empty;
    defer {
        for (operands.items) |*obj| obj.deinit(alloc);
        operands.deinit(alloc);
    }

    var state = initial_state;
    var stack = std.ArrayList(ImageStackEntry).empty;
    defer {
        for (stack.items) |*entry| entry.deinit(alloc);
        stack.deinit(alloc);
    }
    var current_path = std.ArrayList([2]f64).empty;
    defer current_path.deinit(alloc);
    var current_path_closed = false;
    var current_clip_points = std.ArrayList([2]f64).empty;
    defer current_clip_points.deinit(alloc);
    var current_clip_fill_rule: FillRule = initial_clip_fill_rule;
    try current_clip_points.appendSlice(alloc, initial_clip_points);

    while (true) {
        var lex = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &lex);

        if (lex == .eof) break;
        if (lex == .keyword and !isContentObjectStartKeyword(lex.keyword)) {
            try applyImageOperator(alloc, out, &state, &stack, &current_path, &current_path_closed, &current_clip_points, &current_clip_fill_rule, images, gstates, forms, paint_order, next_group_id, lex.keyword, operands.items);
            for (operands.items) |*obj| obj.deinit(alloc);
            operands.clearRetainingCapacity();
            continue;
        }

        try scanner.unreadLexeme(try cloneLexemeForContent(alloc, lex));
        try operands.append(alloc, try scanner.readObject());
    }
}

fn extractShapeRunsFromContentAppend(
    alloc: Allocator,
    out: *std.ArrayList(ShapeRun),
    bytes: []const u8,
    gstates: []const PageExtGState,
    forms: []const PageForm,
) !void {
    var paint_order: usize = 0;
    var next_group_id: u32 = 1;
    return try extractShapeRunsFromContentAppendWithState(alloc, out, bytes, gstates, forms, .{}, &.{}, .nonzero, &.{}, 0, &paint_order, &next_group_id);
}

fn extractPatternRunsFromContentAppend(
    alloc: Allocator,
    out: *std.ArrayList(PatternRun),
    bytes: []const u8,
    patterns: []const PagePattern,
    gstates: []const PageExtGState,
    forms: []const PageForm,
) !void {
    var paint_order: usize = 0;
    var next_group_id: u32 = 1;
    return try extractPatternRunsFromContentAppendWithState(alloc, out, bytes, patterns, gstates, forms, .{}, &.{}, .nonzero, &.{}, 0, &paint_order, &next_group_id);
}

fn extractShadingRunsFromContentAppend(
    alloc: Allocator,
    out: *std.ArrayList(ShadingRun),
    bytes: []const u8,
    shadings: []const PageShading,
    gstates: []const PageExtGState,
    forms: []const PageForm,
) !void {
    var paint_order: usize = 0;
    var next_group_id: u32 = 1;
    return try extractShadingRunsFromContentAppendWithState(alloc, out, bytes, shadings, gstates, forms, .{}, &.{}, .nonzero, &paint_order, &next_group_id);
}

fn extractShadingRunsFromContentAppendWithState(
    alloc: Allocator,
    out: *std.ArrayList(ShadingRun),
    bytes: []const u8,
    shadings: []const PageShading,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    initial_state: GraphicsState,
    initial_clip_points: []const [2]f64,
    initial_clip_fill_rule: FillRule,
    paint_order: *usize,
    next_group_id: *u32,
) anyerror!void {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();

    var operands = std.ArrayList(syntax.Object).empty;
    defer {
        for (operands.items) |*obj| obj.deinit(alloc);
        operands.deinit(alloc);
    }

    var state = initial_state;
    var stack = std.ArrayList(ImageStackEntry).empty;
    defer {
        for (stack.items) |*entry| entry.deinit(alloc);
        stack.deinit(alloc);
    }
    var current_path = std.ArrayList([2]f64).empty;
    defer current_path.deinit(alloc);
    var current_path_closed = false;
    var current_clip_points = std.ArrayList([2]f64).empty;
    defer current_clip_points.deinit(alloc);
    var current_clip_fill_rule: FillRule = initial_clip_fill_rule;
    try current_clip_points.appendSlice(alloc, initial_clip_points);

    while (true) {
        var lex = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &lex);

        if (lex == .eof) break;
        if (lex == .keyword and !isContentObjectStartKeyword(lex.keyword)) {
            try applyShadingOperator(alloc, out, &state, &stack, &current_path, &current_path_closed, &current_clip_points, &current_clip_fill_rule, shadings, gstates, forms, paint_order, next_group_id, lex.keyword, operands.items);
            for (operands.items) |*obj| obj.deinit(alloc);
            operands.clearRetainingCapacity();
            continue;
        }

        try scanner.unreadLexeme(try cloneLexemeForContent(alloc, lex));
        try operands.append(alloc, try scanner.readObject());
    }
}

fn extractPatternRunsFromContentAppendWithState(
    alloc: Allocator,
    out: *std.ArrayList(PatternRun),
    bytes: []const u8,
    patterns: []const PagePattern,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    initial_state: GraphicsState,
    initial_clip_points: []const [2]f64,
    initial_clip_fill_rule: FillRule,
    initial_dash_array: []const f64,
    initial_dash_phase: f64,
    paint_order: *usize,
    next_group_id: *u32,
) anyerror!void {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();

    var operands = std.ArrayList(syntax.Object).empty;
    defer {
        for (operands.items) |*obj| obj.deinit(alloc);
        operands.deinit(alloc);
    }

    var state = initial_state;
    var stack = std.ArrayList(ShapeStackEntry).empty;
    defer {
        for (stack.items) |*entry| entry.deinit(alloc);
        stack.deinit(alloc);
    }
    var current_path = std.ArrayList([2]f64).empty;
    defer current_path.deinit(alloc);
    var current_path_closed = false;
    var current_dash = std.ArrayList(f64).empty;
    defer current_dash.deinit(alloc);
    try current_dash.appendSlice(alloc, initial_dash_array);
    var dash_phase = initial_dash_phase;
    var current_clip_points = std.ArrayList([2]f64).empty;
    defer current_clip_points.deinit(alloc);
    var current_clip_fill_rule: FillRule = initial_clip_fill_rule;
    try current_clip_points.appendSlice(alloc, initial_clip_points);

    while (true) {
        var lex = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &lex);

        if (lex == .eof) break;
        if (lex == .keyword and !isContentObjectStartKeyword(lex.keyword)) {
            try applyPatternOperator(alloc, out, &state, &stack, &current_path, &current_path_closed, &current_dash, &dash_phase, &current_clip_points, &current_clip_fill_rule, patterns, gstates, forms, paint_order, next_group_id, lex.keyword, operands.items);
            for (operands.items) |*obj| obj.deinit(alloc);
            operands.clearRetainingCapacity();
            continue;
        }

        try scanner.unreadLexeme(try cloneLexemeForContent(alloc, lex));
        try operands.append(alloc, try scanner.readObject());
    }
}

fn extractShapeRunsFromContentAppendWithState(
    alloc: Allocator,
    out: *std.ArrayList(ShapeRun),
    bytes: []const u8,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    initial_state: GraphicsState,
    initial_clip_points: []const [2]f64,
    initial_clip_fill_rule: FillRule,
    initial_dash: []const f64,
    initial_dash_phase: f64,
    paint_order: *usize,
    next_group_id: *u32,
) anyerror!void {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();

    var operands = std.ArrayList(syntax.Object).empty;
    defer {
        for (operands.items) |*obj| obj.deinit(alloc);
        operands.deinit(alloc);
    }

    var state = initial_state;
    var stack = std.ArrayList(ShapeStackEntry).empty;
    defer {
        for (stack.items) |*entry| entry.deinit(alloc);
        stack.deinit(alloc);
    }
    var current_path = std.ArrayList([2]f64).empty;
    defer current_path.deinit(alloc);
    var current_path_closed = false;
    var current_dash = std.ArrayList(f64).empty;
    defer current_dash.deinit(alloc);
    var dash_phase: f64 = initial_dash_phase;
    var current_clip_points = std.ArrayList([2]f64).empty;
    defer current_clip_points.deinit(alloc);
    var current_clip_fill_rule: FillRule = initial_clip_fill_rule;
    try current_dash.appendSlice(alloc, initial_dash);
    try current_clip_points.appendSlice(alloc, initial_clip_points);

    while (true) {
        var lex = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &lex);

        if (lex == .eof) break;
        if (lex == .keyword and !isContentObjectStartKeyword(lex.keyword)) {
            try applyShapeOperator(alloc, out, &state, &stack, &current_path, &current_path_closed, &current_dash, &dash_phase, &current_clip_points, &current_clip_fill_rule, gstates, forms, paint_order, next_group_id, lex.keyword, operands.items);
            for (operands.items) |*obj| obj.deinit(alloc);
            operands.clearRetainingCapacity();
            continue;
        }

        try scanner.unreadLexeme(try cloneLexemeForContent(alloc, lex));
        try operands.append(alloc, try scanner.readObject());
    }
}

pub const Reader = struct {
    alloc: Allocator,
    bytes: []const u8,
    version_minor: u8,
    startxref_offset: usize,
    xref_entries: []XrefEntry,
    trailer: syntax.Object,

    pub fn init(alloc: Allocator, bytes: []const u8) !Reader {
        if (bytes.len < 10) return error.InvalidPdfHeader;
        if (!std.mem.startsWith(u8, bytes, "%PDF-1.")) return error.InvalidPdfHeader;

        const minor = bytes[7];
        if (minor < '0' or minor > '9') return error.InvalidPdfHeader;
        const newline = bytes[8];
        if (newline != '\r' and newline != '\n') return error.InvalidPdfHeader;

        const trailer_slice = trimPdfTrailer(bytes);
        if (!std.mem.endsWith(u8, trailer_slice, "%%EOF")) return error.MissingPdfEof;

        const startxref_pos = findLastLine(trailer_slice, "startxref") orelse return error.MissingStartXref;
        const startxref_offset = try parseStartXref(trailer_slice[startxref_pos..]);
        if (startxref_offset >= bytes.len) return error.InvalidStartXref;

        var entries = std.ArrayList(XrefEntry).empty;
        defer entries.deinit(alloc);

        var trailer: ?syntax.Object = null;
        errdefer if (trailer) |*value| value.deinit(alloc);

        try parseXrefTable(alloc, bytes, startxref_offset, &entries, &trailer);
        if (trailer == null) return error.MissingTrailer;

        return .{
            .alloc = alloc,
            .bytes = bytes,
            .version_minor = minor - '0',
            .startxref_offset = startxref_offset,
            .xref_entries = try entries.toOwnedSlice(alloc),
            .trailer = trailer.?,
        };
    }

    pub fn deinit(self: *Reader) void {
        self.alloc.free(self.xref_entries);
        self.trailer.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn trailerGet(self: *const Reader, key: []const u8) ?*const syntax.Object {
        return self.trailer.get(key);
    }

    pub fn readIndirectObject(self: *const Reader, ptr: syntax.ObjRef) anyerror!syntax.Object {
        const entry = self.findXref(ptr) orelse return error.ObjectNotFound;
        if (!entry.in_use) return error.ObjectNotInUse;
        if (entry.compressed_obj_stream_id) |obj_stream_id| {
            return try self.readCompressedObject(ptr, obj_stream_id, entry.compressed_index orelse return error.InvalidObjectStream);
        }
        if (entry.offset >= self.bytes.len) return error.InvalidObjectOffset;

        var scanner = syntax.Scanner.initWithOffset(self.alloc, self.bytes[entry.offset..], entry.offset);
        defer scanner.deinit();

        var parsed = try scanner.readObject();
        errdefer parsed.deinit(self.alloc);
        if (parsed != .obj_def) return error.ExpectedIndirectObject;
        if (parsed.obj_def.ptr.id != ptr.id or parsed.obj_def.ptr.gen != ptr.gen) return error.ObjectPointerMismatch;

        const value = try parsed.obj_def.value.clone(self.alloc);
        parsed.deinit(self.alloc);
        return value;
    }

    pub fn readRawStreamData(self: *const Reader, obj: *const syntax.Object) ![]u8 {
        if (obj.* != .stream) return error.NotAStream;
        const stream_value = obj.stream;
        const end = stream_value.data_offset + stream_value.data_length;
        if (end > self.bytes.len) return error.InvalidObjectOffset;
        return try self.alloc.dupe(u8, self.bytes[stream_value.data_offset..end]);
    }

    pub fn readDecodedStreamData(self: *const Reader, obj: *const syntax.Object) ![]u8 {
        if (obj.* != .stream) return error.NotAStream;
        const raw = try self.readRawStreamData(obj);
        defer self.alloc.free(raw);
        return try decodeStreamFiltersAlloc(self.alloc, raw, obj.get("Filter"), obj.get("DecodeParms"));
    }

    pub fn pageCount(self: *const Reader) !usize {
        var root = try self.resolveValue(self.trailerGet("Root") orelse return error.MissingRoot);
        defer root.deinit(self.alloc);

        var pages = try self.resolveValue(root.get("Pages") orelse return error.MissingPages);
        defer pages.deinit(self.alloc);

        const count = pages.get("Count") orelse return error.MissingPageCount;
        const count_i = count.asInteger() orelse return error.InvalidPageCount;
        if (count_i < 0) return error.InvalidPageCount;
        return @intCast(count_i);
    }

    pub fn readPageObject(self: *const Reader, page_num: usize) !syntax.Object {
        if (page_num == 0) return error.PageOutOfRange;

        var root = try self.resolveValue(self.trailerGet("Root") orelse return error.MissingRoot);
        defer root.deinit(self.alloc);

        var pages = try self.resolveValue(root.get("Pages") orelse return error.MissingPages);
        defer pages.deinit(self.alloc);

        var remaining = page_num;
        return try self.findPageObject(&pages, &remaining);
    }

    pub fn extractPageTextAlloc(self: *const Reader, page_num: usize) ![]u8 {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);

        const fonts = try self.collectPageFontsAlloc(&page);
        defer {
            for (fonts) |*font| font.deinit(self.alloc);
            self.alloc.free(fonts);
        }

        const contents = page.get("Contents") orelse return try self.alloc.dupe(u8, "");
        return try self.extractContentsTextAlloc(contents, fonts);
    }

    pub fn extractPageTextRunsAlloc(self: *const Reader, page_num: usize) ![]TextRun {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);

        const fonts = try self.collectPageFontsAlloc(&page);
        defer {
            for (fonts) |*font| font.deinit(self.alloc);
            self.alloc.free(fonts);
        }
        const gstates = try self.collectPageExtGStatesAlloc(&page);
        defer {
            for (gstates) |*gstate| gstate.deinit(self.alloc);
            self.alloc.free(gstates);
        }
        const forms = try self.collectPageFormsAlloc(&page);
        defer {
            for (forms) |*form| form.deinit(self.alloc);
            self.alloc.free(forms);
        }

        const contents = page.get("Contents") orelse return try self.alloc.alloc(TextRun, 0);
        return try self.extractContentsTextRunsAlloc(contents, fonts, gstates, forms);
    }

    pub fn extractPageImageRunsAlloc(self: *const Reader, page_num: usize) ![]ImageRun {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);

        const images = try self.collectPageImagesAlloc(&page);
        defer {
            for (images) |*image| image.deinit(self.alloc);
            self.alloc.free(images);
        }
        const gstates = try self.collectPageExtGStatesAlloc(&page);
        defer {
            for (gstates) |*gstate| gstate.deinit(self.alloc);
            self.alloc.free(gstates);
        }
        const forms = try self.collectPageFormsAlloc(&page);
        defer {
            for (forms) |*form| form.deinit(self.alloc);
            self.alloc.free(forms);
        }

        const contents = page.get("Contents") orelse return try self.alloc.alloc(ImageRun, 0);
        return try self.extractContentsImageRunsAlloc(contents, images, gstates, forms);
    }

    pub fn extractPageShadingRunsAlloc(self: *const Reader, page_num: usize) ![]ShadingRun {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);

        const shadings = try self.collectPageShadingsAlloc(&page);
        defer {
            for (shadings) |*shading| shading.deinit(self.alloc);
            self.alloc.free(shadings);
        }
        const gstates = try self.collectPageExtGStatesAlloc(&page);
        defer {
            for (gstates) |*gstate| gstate.deinit(self.alloc);
            self.alloc.free(gstates);
        }
        const forms = try self.collectPageFormsAlloc(&page);
        defer {
            for (forms) |*form| form.deinit(self.alloc);
            self.alloc.free(forms);
        }

        const contents = page.get("Contents") orelse return try self.alloc.alloc(ShadingRun, 0);
        return try self.extractContentsShadingRunsAlloc(contents, shadings, gstates, forms);
    }

    pub fn extractPagePatternRunsAlloc(self: *const Reader, page_num: usize) ![]PatternRun {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);

        const patterns = try self.collectPagePatternsAlloc(&page);
        defer {
            for (patterns) |*pattern| pattern.deinit(self.alloc);
            self.alloc.free(patterns);
        }
        const gstates = try self.collectPageExtGStatesAlloc(&page);
        defer {
            for (gstates) |*gstate| gstate.deinit(self.alloc);
            self.alloc.free(gstates);
        }
        const forms = try self.collectPageFormsAlloc(&page);
        defer {
            for (forms) |*form| form.deinit(self.alloc);
            self.alloc.free(forms);
        }

        const contents = page.get("Contents") orelse return try self.alloc.alloc(PatternRun, 0);
        return try self.extractContentsPatternRunsAlloc(contents, patterns, gstates, forms);
    }

    pub fn extractPageRenderRunsAlloc(self: *const Reader, page_num: usize) !PageRenderRuns {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);
        const page_box = try self.extractPageBoxFromObject(&page);

        const fonts = try self.collectPageFontsAlloc(&page);
        defer {
            for (fonts) |*font| font.deinit(self.alloc);
            self.alloc.free(fonts);
        }
        const images = try self.collectPageImagesAlloc(&page);
        defer {
            for (images) |*image| image.deinit(self.alloc);
            self.alloc.free(images);
        }
        const shadings = try self.collectPageShadingsAlloc(&page);
        defer {
            for (shadings) |*shading| shading.deinit(self.alloc);
            self.alloc.free(shadings);
        }
        const patterns = try self.collectPagePatternsAlloc(&page);
        defer {
            for (patterns) |*pattern| pattern.deinit(self.alloc);
            self.alloc.free(patterns);
        }
        const gstates = try self.collectPageExtGStatesAlloc(&page);
        defer {
            for (gstates) |*gstate| gstate.deinit(self.alloc);
            self.alloc.free(gstates);
        }
        const forms = try self.collectPageFormsAlloc(&page);
        defer {
            for (forms) |*form| form.deinit(self.alloc);
            self.alloc.free(forms);
        }

        var text_out = std.ArrayList(TextRun).empty;
        defer text_out.deinit(self.alloc);
        var image_out = std.ArrayList(ImageRun).empty;
        defer image_out.deinit(self.alloc);
        var shading_out = std.ArrayList(ShadingRun).empty;
        defer shading_out.deinit(self.alloc);
        var pattern_out = std.ArrayList(PatternRun).empty;
        defer pattern_out.deinit(self.alloc);
        var shape_out = std.ArrayList(ShapeRun).empty;
        defer shape_out.deinit(self.alloc);

        var result: PageRenderRuns = .{ .page_box = page_box };
        errdefer result.deinit(self.alloc);
        var text_transferred = false;
        var image_transferred = false;
        var shading_transferred = false;
        var pattern_transferred = false;
        var shape_transferred = false;
        errdefer if (!text_transferred) for (text_out.items) |*run| run.deinit(self.alloc);
        errdefer if (!image_transferred) for (image_out.items) |*run| run.deinit(self.alloc);
        errdefer if (!shading_transferred) for (shading_out.items) |*run| run.deinit(self.alloc);
        errdefer if (!pattern_transferred) for (pattern_out.items) |*run| run.deinit(self.alloc);
        errdefer if (!shape_transferred) for (shape_out.items) |*run| run.deinit(self.alloc);

        if (page.get("Contents")) |contents| {
            switch (contents.*) {
                .array => |items| {
                    for (items) |item| {
                        var resolved = try self.resolveValue(&item);
                        defer resolved.deinit(self.alloc);
                        const decoded = try self.readDecodedStreamData(&resolved);
                        defer self.alloc.free(decoded);
                        try self.extractRenderRunsFromContentAppend(&text_out, &image_out, &shading_out, &pattern_out, &shape_out, decoded, fonts, images, shadings, patterns, gstates, forms);
                    }
                },
                else => {
                    var resolved = try self.resolveValue(contents);
                    defer resolved.deinit(self.alloc);
                    const decoded = try self.readDecodedStreamData(&resolved);
                    defer self.alloc.free(decoded);
                    try self.extractRenderRunsFromContentAppend(&text_out, &image_out, &shading_out, &pattern_out, &shape_out, decoded, fonts, images, shadings, patterns, gstates, forms);
                },
            }
        }

        result.text_runs = try text_out.toOwnedSlice(self.alloc);
        text_transferred = true;
        result.image_runs = try image_out.toOwnedSlice(self.alloc);
        image_transferred = true;
        result.shading_runs = try shading_out.toOwnedSlice(self.alloc);
        shading_transferred = true;
        result.pattern_runs = try pattern_out.toOwnedSlice(self.alloc);
        pattern_transferred = true;
        result.shape_runs = try shape_out.toOwnedSlice(self.alloc);
        shape_transferred = true;
        return result;
    }

    pub fn extractPageBox(self: *const Reader, page_num: usize) !PageBox {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);
        return try self.extractPageBoxFromObject(&page);
    }

    fn extractPageBoxFromObject(self: *const Reader, page: *const syntax.Object) !PageBox {
        if (try self.findInheritedPageValue(page, "CropBox")) |box_value| {
            var box = box_value;
            defer box.deinit(self.alloc);
            return try parsePageBox(&box);
        }
        if (try self.findInheritedPageValue(page, "MediaBox")) |box_value| {
            var box = box_value;
            defer box.deinit(self.alloc);
            return try parsePageBox(&box);
        }
        return .{ .min_x = 0, .min_y = 0, .max_x = 612, .max_y = 792 };
    }

    pub fn extractPageShapeRunsAlloc(self: *const Reader, page_num: usize) ![]ShapeRun {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);
        const gstates = try self.collectPageExtGStatesAlloc(&page);
        defer {
            for (gstates) |*gstate| gstate.deinit(self.alloc);
            self.alloc.free(gstates);
        }
        const forms = try self.collectPageFormsAlloc(&page);
        defer {
            for (forms) |*form| form.deinit(self.alloc);
            self.alloc.free(forms);
        }
        const contents = page.get("Contents") orelse return try self.alloc.alloc(ShapeRun, 0);
        return try self.extractContentsShapeRunsAlloc(contents, gstates, forms);
    }

    pub fn extractPageType3TextShapeRunsAlloc(self: *const Reader, page_num: usize) ![]ShapeRun {
        return try self.extractPageVectorTextShapeRunsAlloc(page_num);
    }

    pub fn extractPageVectorTextShapeRunsAlloc(self: *const Reader, page_num: usize) ![]ShapeRun {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);

        const fonts = try self.collectPageFontsAlloc(&page);
        defer {
            for (fonts) |*font| font.deinit(self.alloc);
            self.alloc.free(fonts);
        }

        const text_runs = try self.extractPageTextRunsAlloc(page_num);
        defer {
            for (text_runs) |*run| run.deinit(self.alloc);
            self.alloc.free(text_runs);
        }

        var out = std.ArrayList(ShapeRun).empty;
        defer out.deinit(self.alloc);
        for (text_runs) |run| {
            if (run.fill_pattern_name != null or run.stroke_pattern_name != null) continue;
            const font_index = run.font_index orelse continue;
            if (font_index >= fonts.len) continue;
            if (fonts[font_index].type3) |type3| {
                const raw = run.raw_text orelse continue;
                try appendType3RunShapesAlloc(self.alloc, &out, run, type3, raw);
                continue;
            }
            if (fonts[font_index].type1) |type1| {
                const raw = run.raw_text orelse continue;
                try appendType1RunShapesAlloc(self.alloc, &out, run, type1, raw);
                continue;
            }
            if (fonts[font_index].truetype) |truetype| {
                try appendTrueTypeRunShapesAlloc(self.alloc, &out, run, truetype);
                continue;
            }
            if (fonts[font_index].cff_otf) |cff_otf| {
                try appendCffRunShapesAlloc(self.alloc, &out, run, cff_otf);
            }
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn extractPageVectorTextPatternRunsAlloc(self: *const Reader, page_num: usize) ![]PatternRun {
        var page = try self.readPageObject(page_num);
        defer page.deinit(self.alloc);

        const fonts = try self.collectPageFontsAlloc(&page);
        defer {
            for (fonts) |*font| font.deinit(self.alloc);
            self.alloc.free(fonts);
        }
        const patterns = try self.collectPagePatternsAlloc(&page);
        defer {
            for (patterns) |*pattern| pattern.deinit(self.alloc);
            self.alloc.free(patterns);
        }
        const text_runs = try self.extractPageTextRunsAlloc(page_num);
        defer {
            for (text_runs) |*run| run.deinit(self.alloc);
            self.alloc.free(text_runs);
        }

        var out = std.ArrayList(PatternRun).empty;
        defer out.deinit(self.alloc);
        try appendVectorTextPatternRunsAlloc(self.alloc, &out, fonts, patterns, text_runs);
        return try out.toOwnedSlice(self.alloc);
    }

    fn appendVectorTextPatternRunsAlloc(
        alloc: Allocator,
        out: *std.ArrayList(PatternRun),
        fonts: []const PageFont,
        patterns: []const PagePattern,
        text_runs: []const TextRun,
    ) anyerror!void {
        for (text_runs) |run| {
            if (run.fill_pattern_name == null and run.stroke_pattern_name == null) continue;
            const font_index = run.font_index orelse continue;
            if (font_index >= fonts.len) continue;

            var temp_shapes = std.ArrayList(ShapeRun).empty;
            defer {
                for (temp_shapes.items) |*shape| shape.deinit(alloc);
                temp_shapes.deinit(alloc);
            }

            if (fonts[font_index].type3) |type3| {
                const raw = run.raw_text orelse continue;
                try appendType3RunShapesAlloc(alloc, &temp_shapes, run, type3, raw);
            } else if (fonts[font_index].type1) |type1| {
                const raw = run.raw_text orelse continue;
                try appendType1RunShapesAlloc(alloc, &temp_shapes, run, type1, raw);
            } else if (fonts[font_index].truetype) |truetype| {
                try appendTrueTypeRunShapesAlloc(alloc, &temp_shapes, run, truetype);
            } else if (fonts[font_index].cff_otf) |cff_otf| {
                try appendCffRunShapesAlloc(alloc, &temp_shapes, run, cff_otf);
            }

            for (temp_shapes.items) |shape| {
                const pattern_name = switch (shape.kind) {
                    .fill => run.fill_pattern_name,
                    .stroke => run.stroke_pattern_name,
                } orelse continue;
                const pattern = findPagePattern(patterns, pattern_name) orelse continue;
                if (pattern.kind == .shading) {
                    try out.append(alloc, try buildShadingPatternRunAlloc(
                        alloc,
                        pattern,
                        .{
                            .fill_color = shape.color,
                            .stroke_color = shape.color,
                            .fill_alpha = shape.color[3],
                            .stroke_alpha = shape.color[3],
                            .blend_mode = shape.blend_mode,
                            .group_id = shape.group_id,
                            .group_parent_id = shape.group_parent_id,
                            .group_isolated = shape.group_isolated,
                            .group_knockout = shape.group_knockout,
                            .line_cap = shape.line_cap,
                            .line_join = shape.line_join,
                            .miter_limit = shape.miter_limit,
                            .stroke_width = shape.stroke_width,
                            .clip_box = shape.clip_box,
                        },
                        shape.points,
                        switch (shape.kind) {
                            .fill => .fill,
                            .stroke => .stroke,
                        },
                        shape.closed,
                        shape.fill_rule,
                        if (shape.dash_array) |dash| dash else &.{},
                        shape.dash_phase,
                        if (shape.clip_points) |clip| clip else &.{},
                        shape.clip_fill_rule,
                        shape.paint_order,
                    ));
                } else {
                    try out.append(alloc, try buildPatternRunAlloc(
                        alloc,
                        pattern,
                        .{
                            .fill_color = shape.color,
                            .stroke_color = shape.color,
                            .fill_alpha = shape.color[3],
                            .stroke_alpha = shape.color[3],
                            .blend_mode = shape.blend_mode,
                            .group_id = shape.group_id,
                            .group_parent_id = shape.group_parent_id,
                            .group_isolated = shape.group_isolated,
                            .group_knockout = shape.group_knockout,
                            .line_cap = shape.line_cap,
                            .line_join = shape.line_join,
                            .miter_limit = shape.miter_limit,
                            .stroke_width = shape.stroke_width,
                            .clip_box = shape.clip_box,
                        },
                        shape.points,
                        switch (shape.kind) {
                            .fill => .fill,
                            .stroke => .stroke,
                        },
                        shape.closed,
                        shape.fill_rule,
                        if (shape.dash_array) |dash| dash else &.{},
                        shape.dash_phase,
                        if (shape.clip_points) |clip| clip else &.{},
                        shape.clip_fill_rule,
                        shape.paint_order,
                    ));
                }
            }
        }
    }

    pub fn extractPlainTextAlloc(self: *const Reader) ![]u8 {
        const count = try self.pageCount();
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        for (1..count + 1) |page_num| {
            const text = try self.extractPageTextAlloc(page_num);
            defer self.alloc.free(text);
            try out.appendSlice(self.alloc, text);
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn extractContentsTextRunsAlloc(self: *const Reader, contents: *const syntax.Object, fonts: []const PageFont, gstates: []const PageExtGState, forms: []const PageForm) ![]TextRun {
        var out = std.ArrayList(TextRun).empty;
        defer out.deinit(self.alloc);

        switch (contents.*) {
            .array => |items| {
                for (items) |item| {
                    var resolved = try self.resolveValue(&item);
                    defer resolved.deinit(self.alloc);
                    try self.extractSingleContentTextRunsAppend(&out, &resolved, fonts, gstates, forms);
                }
            },
            else => {
                var resolved = try self.resolveValue(contents);
                defer resolved.deinit(self.alloc);
                try self.extractSingleContentTextRunsAppend(&out, &resolved, fonts, gstates, forms);
            },
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn extractContentsImageRunsAlloc(self: *const Reader, contents: *const syntax.Object, images: []const PageImage, gstates: []const PageExtGState, forms: []const PageForm) ![]ImageRun {
        var out = std.ArrayList(ImageRun).empty;
        defer out.deinit(self.alloc);

        switch (contents.*) {
            .array => |items| {
                for (items) |item| {
                    var resolved = try self.resolveValue(&item);
                    defer resolved.deinit(self.alloc);
                    try self.extractSingleContentImageRunsAppend(&out, &resolved, images, gstates, forms);
                }
            },
            else => {
                var resolved = try self.resolveValue(contents);
                defer resolved.deinit(self.alloc);
                try self.extractSingleContentImageRunsAppend(&out, &resolved, images, gstates, forms);
            },
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn extractContentsShadingRunsAlloc(self: *const Reader, contents: *const syntax.Object, shadings: []const PageShading, gstates: []const PageExtGState, forms: []const PageForm) ![]ShadingRun {
        var out = std.ArrayList(ShadingRun).empty;
        defer out.deinit(self.alloc);

        switch (contents.*) {
            .array => |items| {
                for (items) |item| {
                    var resolved = try self.resolveValue(&item);
                    defer resolved.deinit(self.alloc);
                    try self.extractSingleContentShadingRunsAppend(&out, &resolved, shadings, gstates, forms);
                }
            },
            else => {
                var resolved = try self.resolveValue(contents);
                defer resolved.deinit(self.alloc);
                try self.extractSingleContentShadingRunsAppend(&out, &resolved, shadings, gstates, forms);
            },
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn extractContentsShapeRunsAlloc(self: *const Reader, contents: *const syntax.Object, gstates: []const PageExtGState, forms: []const PageForm) ![]ShapeRun {
        var out = std.ArrayList(ShapeRun).empty;
        defer out.deinit(self.alloc);

        switch (contents.*) {
            .array => |items| {
                for (items) |item| {
                    var resolved = try self.resolveValue(&item);
                    defer resolved.deinit(self.alloc);
                    try self.extractSingleContentShapeRunsAppend(&out, &resolved, gstates, forms);
                }
            },
            else => {
                var resolved = try self.resolveValue(contents);
                defer resolved.deinit(self.alloc);
                try self.extractSingleContentShapeRunsAppend(&out, &resolved, gstates, forms);
            },
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn extractContentsPatternRunsAlloc(self: *const Reader, contents: *const syntax.Object, patterns: []const PagePattern, gstates: []const PageExtGState, forms: []const PageForm) ![]PatternRun {
        var out = std.ArrayList(PatternRun).empty;
        defer out.deinit(self.alloc);

        switch (contents.*) {
            .array => |items| {
                for (items) |item| {
                    var resolved = try self.resolveValue(&item);
                    defer resolved.deinit(self.alloc);
                    try self.extractSingleContentPatternRunsAppend(&out, &resolved, patterns, gstates, forms);
                }
            },
            else => {
                var resolved = try self.resolveValue(contents);
                defer resolved.deinit(self.alloc);
                try self.extractSingleContentPatternRunsAppend(&out, &resolved, patterns, gstates, forms);
            },
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn extractRenderRunsFromContentAppend(
        self: *const Reader,
        text_out: *std.ArrayList(TextRun),
        image_out: *std.ArrayList(ImageRun),
        shading_out: *std.ArrayList(ShadingRun),
        pattern_out: *std.ArrayList(PatternRun),
        shape_out: *std.ArrayList(ShapeRun),
        decoded: []const u8,
        fonts: []const PageFont,
        images: []const PageImage,
        shadings: []const PageShading,
        patterns: []const PagePattern,
        gstates: []const PageExtGState,
        forms: []const PageForm,
    ) !void {
        try extractTextRunsFromContentAppend(self.alloc, text_out, decoded, fonts, gstates, forms);
        const has_do = contentMayContainOperator(decoded, "Do");
        const has_shape_paint = contentMayContainShapePaintOperator(decoded);
        const has_shading_paint = contentMayContainOperator(decoded, "sh");

        if ((images.len > 0 or forms.len > 0) and has_do) {
            try extractImageRunsFromContentAppend(self.alloc, image_out, decoded, images, gstates, forms);
        }
        if ((shadings.len > 0 and has_shading_paint) or (forms.len > 0 and has_do)) {
            try extractShadingRunsFromContentAppend(self.alloc, shading_out, decoded, shadings, gstates, forms);
        }
        if ((patterns.len > 0 and has_shape_paint) or (forms.len > 0 and has_do)) {
            try extractPatternRunsFromContentAppend(self.alloc, pattern_out, decoded, patterns, gstates, forms);
        }
        if (has_shape_paint or (forms.len > 0 and has_do)) {
            try extractShapeRunsFromContentAppend(self.alloc, shape_out, decoded, gstates, forms);
        }
    }

    fn findXref(self: *const Reader, ptr: syntax.ObjRef) ?XrefEntry {
        for (self.xref_entries) |entry| {
            if (entry.ptr.id == ptr.id and entry.ptr.gen == ptr.gen) return entry;
        }
        return null;
    }

    fn readCompressedObject(self: *const Reader, ptr: syntax.ObjRef, obj_stream_id: u32, obj_index: usize) anyerror!syntax.Object {
        var obj_stream = try self.readIndirectObject(.{ .id = obj_stream_id, .gen = 0 });
        defer obj_stream.deinit(self.alloc);
        if (obj_stream != .stream) return error.ExpectedObjectStream;

        const ty = obj_stream.get("Type") orelse return error.ExpectedObjectStream;
        if (!std.mem.eql(u8, ty.asName() orelse return error.ExpectedObjectStream, "ObjStm")) {
            return error.ExpectedObjectStream;
        }

        const first_i = (obj_stream.get("First") orelse return error.InvalidObjectStream).asInteger() orelse return error.InvalidObjectStream;
        const count_i = (obj_stream.get("N") orelse return error.InvalidObjectStream).asInteger() orelse return error.InvalidObjectStream;
        if (first_i < 0 or count_i < 0) return error.InvalidObjectStream;
        const first: usize = @intCast(first_i);
        const count: usize = @intCast(count_i);
        if (obj_index >= count) return error.ObjectNotFound;

        const decoded = try self.readDecodedStreamData(&obj_stream);
        defer self.alloc.free(decoded);
        if (first > decoded.len) return error.InvalidObjectStream;

        var header_scanner = syntax.Scanner.init(self.alloc, decoded[0..first]);
        defer header_scanner.deinit();

        var found_id: ?u32 = null;
        var found_offset: ?usize = null;
        var i: usize = 0;
        while (i < count) : (i += 1) {
            var id_obj = try header_scanner.readObject();
            defer id_obj.deinit(self.alloc);
            var off_obj = try header_scanner.readObject();
            defer off_obj.deinit(self.alloc);

            const id_i = id_obj.asInteger() orelse return error.InvalidObjectStream;
            const off_i = off_obj.asInteger() orelse return error.InvalidObjectStream;
            if (id_i < 0 or off_i < 0) return error.InvalidObjectStream;
            if (i == obj_index) {
                found_id = @intCast(id_i);
                found_offset = @intCast(off_i);
            }
        }

        const target_id = found_id orelse return error.ObjectNotFound;
        const target_offset = found_offset orelse return error.ObjectNotFound;
        if (target_id != ptr.id) return error.ObjectPointerMismatch;
        if (first + target_offset > decoded.len) return error.InvalidObjectStream;

        var obj_scanner = syntax.Scanner.init(self.alloc, decoded[first + target_offset ..]);
        defer obj_scanner.deinit();
        return try obj_scanner.readObject();
    }

    fn resolveValue(self: *const Reader, obj: *const syntax.Object) !syntax.Object {
        return switch (obj.*) {
            .obj_ref => |ptr| try self.readIndirectObject(ptr),
            else => try obj.clone(self.alloc),
        };
    }

    fn findPageObject(self: *const Reader, node: *const syntax.Object, remaining: *usize) anyerror!syntax.Object {
        if (node.* != .dict) return error.InvalidPageTree;

        const ty = node.get("Type") orelse return error.InvalidPageTree;
        if (ty.asName()) |name| {
            if (std.mem.eql(u8, name, "Page")) {
                if (remaining.* == 1) return try node.clone(self.alloc);
                remaining.* -= 1;
                return error.PageOutOfRange;
            }
            if (!std.mem.eql(u8, name, "Pages")) return error.InvalidPageTree;
        } else return error.InvalidPageTree;

        const kids = node.get("Kids") orelse return error.InvalidPageTree;
        if (kids.* != .array) return error.InvalidPageTree;

        for (kids.array) |kid| {
            var resolved = try self.resolveValue(&kid);
            defer resolved.deinit(self.alloc);

            if (resolved == .dict) {
                const kid_ty = resolved.get("Type");
                if (kid_ty) |kt| {
                    if (kt.asName()) |name| {
                        if (std.mem.eql(u8, name, "Pages")) {
                            if (resolved.get("Count")) |count_obj| {
                                if (count_obj.asInteger()) |count_i| {
                                    if (count_i > 0 and remaining.* > @as(usize, @intCast(count_i))) {
                                        remaining.* -= @intCast(count_i);
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (self.findPageObject(&resolved, remaining)) |page| {
                return page;
            } else |err| switch (err) {
                error.PageOutOfRange => {},
                else => return err,
            }
        }

        return error.PageOutOfRange;
    }

    fn extractContentsTextAlloc(self: *const Reader, contents: *const syntax.Object, fonts: []const PageFont) ![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        switch (contents.*) {
            .array => |items| {
                for (items) |item| {
                    var resolved = try self.resolveValue(&item);
                    defer resolved.deinit(self.alloc);
                    const chunk = try self.extractSingleContentTextAlloc(&resolved, fonts);
                    defer self.alloc.free(chunk);
                    try out.appendSlice(self.alloc, chunk);
                }
            },
            else => {
                var resolved = try self.resolveValue(contents);
                defer resolved.deinit(self.alloc);
                const chunk = try self.extractSingleContentTextAlloc(&resolved, fonts);
                defer self.alloc.free(chunk);
                try out.appendSlice(self.alloc, chunk);
            },
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn extractSingleContentTextAlloc(self: *const Reader, obj: *const syntax.Object, fonts: []const PageFont) ![]u8 {
        const decoded = try self.readDecodedStreamData(obj);
        defer self.alloc.free(decoded);
        return try extractTextFromContentAlloc(self.alloc, decoded, fonts);
    }

    fn extractSingleContentTextRunsAppend(self: *const Reader, out: *std.ArrayList(TextRun), obj: *const syntax.Object, fonts: []const PageFont, gstates: []const PageExtGState, forms: []const PageForm) !void {
        const decoded = try self.readDecodedStreamData(obj);
        defer self.alloc.free(decoded);
        try extractTextRunsFromContentAppend(self.alloc, out, decoded, fonts, gstates, forms);
    }

    fn extractSingleContentImageRunsAppend(self: *const Reader, out: *std.ArrayList(ImageRun), obj: *const syntax.Object, images: []const PageImage, gstates: []const PageExtGState, forms: []const PageForm) !void {
        const decoded = try self.readDecodedStreamData(obj);
        defer self.alloc.free(decoded);
        try extractImageRunsFromContentAppend(self.alloc, out, decoded, images, gstates, forms);
    }

    fn extractSingleContentShadingRunsAppend(self: *const Reader, out: *std.ArrayList(ShadingRun), obj: *const syntax.Object, shadings: []const PageShading, gstates: []const PageExtGState, forms: []const PageForm) !void {
        const decoded = try self.readDecodedStreamData(obj);
        defer self.alloc.free(decoded);
        try extractShadingRunsFromContentAppend(self.alloc, out, decoded, shadings, gstates, forms);
    }

    fn extractSingleContentShapeRunsAppend(self: *const Reader, out: *std.ArrayList(ShapeRun), obj: *const syntax.Object, gstates: []const PageExtGState, forms: []const PageForm) !void {
        const decoded = try self.readDecodedStreamData(obj);
        defer self.alloc.free(decoded);
        try extractShapeRunsFromContentAppend(self.alloc, out, decoded, gstates, forms);
    }

    fn extractSingleContentPatternRunsAppend(self: *const Reader, out: *std.ArrayList(PatternRun), obj: *const syntax.Object, patterns: []const PagePattern, gstates: []const PageExtGState, forms: []const PageForm) !void {
        const decoded = try self.readDecodedStreamData(obj);
        defer self.alloc.free(decoded);
        try extractPatternRunsFromContentAppend(self.alloc, out, decoded, patterns, gstates, forms);
    }

    fn appendType3RunShapesAlloc(
        alloc: Allocator,
        out: *std.ArrayList(ShapeRun),
        run: TextRun,
        type3: Type3Font,
        raw: []const u8,
    ) !void {
        var cursor_x: f64 = 0;
        var cursor_y: f64 = 0;
        for (raw) |code| {
            const glyph = type3.glyphForCode(code) orelse {
                cursor_x += estimateType3MissingAdvance(run);
                continue;
            };

            var glyph_shapes = std.ArrayList(ShapeRun).empty;
            defer {
                for (glyph_shapes.items) |*shape| shape.deinit(alloc);
                glyph_shapes.deinit(alloc);
            }
            try extractShapeRunsFromContentAppend(alloc, &glyph_shapes, glyph.content, &.{}, &.{});
            for (glyph_shapes.items) |shape| {
                try out.append(alloc, try transformType3ShapeRunAlloc(alloc, shape, run, type3, cursor_x, cursor_y));
            }

            cursor_x += glyph.advance_x * run.font_size * run.horizontal_scale;
            cursor_y += glyph.advance_y * run.font_size;
            cursor_x += run.char_spacing;
            if (code == ' ') cursor_x += run.word_spacing;
        }
    }

    fn transformType3ShapeRunAlloc(
        alloc: Allocator,
        shape: ShapeRun,
        run: TextRun,
        type3: Type3Font,
        cursor_x: f64,
        cursor_y: f64,
    ) !ShapeRun {
        var points = try alloc.alloc([2]f64, shape.points.len);
        errdefer alloc.free(points);
        for (shape.points, 0..) |point, i| {
            points[i] = transformType3Point(point, run, type3, cursor_x, cursor_y);
        }

        return .{
            .paint_order = run.paint_order,
            .blend_mode = run.blend_mode,
            .group_id = run.group_id,
            .group_parent_id = run.group_parent_id,
            .group_isolated = run.group_isolated,
            .group_knockout = run.group_knockout,
            .kind = shape.kind,
            .fill_rule = shape.fill_rule,
            .line_cap = shape.line_cap,
            .line_join = shape.line_join,
            .miter_limit = shape.miter_limit,
            .dash_array = if (shape.dash_array) |dash| try alloc.dupe(f64, dash) else null,
            .dash_phase = shape.dash_phase,
            .color = if (type3.paint_type == 2)
                switch (shape.kind) {
                    .fill => colorWithAlpha(run.fill_color, run.alpha),
                    .stroke => colorWithAlpha(run.stroke_color, run.stroke_alpha),
                }
            else
                shape.color,
            .stroke_width = scaleType3StrokeWidth(shape.stroke_width, run, type3),
            .closed = shape.closed,
            .clip_box = run.clip_box,
            .clip_points = if (run.clip_points) |points_in| try alloc.dupe([2]f64, points_in) else null,
            .clip_fill_rule = run.clip_fill_rule,
            .points = points,
        };
    }

    fn appendTrueTypeRunShapesAlloc(
        alloc: Allocator,
        out: *std.ArrayList(ShapeRun),
        run: TextRun,
        truetype: TrueTypeFont,
    ) !void {
        if (run.text.len == 0) return;
        const mode = @mod(run.render_mode, 8);
        if (mode == 3 or mode == 7) return;

        const scale = if (truetype.units_per_em == 0) 1.0 else run.font_size / @as(f64, @floatFromInt(truetype.units_per_em));
        var cursor_x: f64 = 0;
        var view = std.unicode.Utf8View.init(run.text) catch return;
        var iter = view.iterator();
        var prev_glyph: ?u16 = null;
        while (iter.nextCodepoint()) |cp| {
            const glyph_index = try truetype.font.cmapGlyphIndex(cp) orelse {
                cursor_x += estimateRenderedRunCodepointAdvance(run, cp);
                prev_glyph = null;
                continue;
            };
            const advance_width = try truetype.font.advanceWidth(glyph_index);
            const kern = if (prev_glyph) |left| try truetype.font.horizontalKerning(left, glyph_index) else 0;
            cursor_x += @as(f64, @floatFromInt(kern)) * scale * run.horizontal_scale;
            if (try truetype.font.glyphOutlineAlloc(alloc, glyph_index)) |outline_value| {
                var outline = outline_value;
                defer outline.deinit(alloc);
                for (outline.contours) |contour| {
                    const points = try flattenTrueTypeContourAlloc(alloc, contour.points, run, cursor_x, scale);
                    defer alloc.free(points);
                    if (points.len < 3) continue;

                    if (mode == 0 or mode == 2 or mode == 4 or mode == 6) {
                        try out.append(alloc, .{
                            .paint_order = run.paint_order,
                            .blend_mode = run.blend_mode,
                            .group_id = run.group_id,
                            .group_parent_id = run.group_parent_id,
                            .group_isolated = run.group_isolated,
                            .group_knockout = run.group_knockout,
                            .kind = .fill,
                            .fill_rule = .nonzero,
                            .color = colorWithAlpha(run.fill_color, run.alpha),
                            .stroke_width = 0,
                            .closed = true,
                            .clip_box = run.clip_box,
                            .clip_points = if (run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                            .clip_fill_rule = run.clip_fill_rule,
                            .points = try alloc.dupe([2]f64, points),
                        });
                    }
                    if (mode == 1 or mode == 2 or mode == 5 or mode == 6) {
                        try out.append(alloc, .{
                            .paint_order = run.paint_order,
                            .blend_mode = run.blend_mode,
                            .group_id = run.group_id,
                            .group_parent_id = run.group_parent_id,
                            .group_isolated = run.group_isolated,
                            .group_knockout = run.group_knockout,
                            .kind = .stroke,
                            .fill_rule = .nonzero,
                            .color = colorWithAlpha(run.stroke_color, run.stroke_alpha),
                            .stroke_width = run.stroke_width,
                            .closed = true,
                            .clip_box = run.clip_box,
                            .clip_points = if (run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                            .clip_fill_rule = run.clip_fill_rule,
                            .points = try alloc.dupe([2]f64, points),
                        });
                    }
                }
            }
            const spacing = run.char_spacing + if (cp == ' ') run.word_spacing else 0.0;
            cursor_x += (@as(f64, @floatFromInt(advance_width)) * scale + spacing) * run.horizontal_scale;
            prev_glyph = glyph_index;
        }
    }

    fn appendType1RunShapesAlloc(
        alloc: Allocator,
        out: *std.ArrayList(ShapeRun),
        run: TextRun,
        type1: Type1Font,
        raw: []const u8,
    ) !void {
        if (raw.len == 0) return;
        const mode = @mod(run.render_mode, 8);
        if (mode == 3 or mode == 7) return;

        const scale = run.font_size / 1000.0;
        var cursor_x: f64 = 0;
        for (raw) |code| {
            const glyph = type1.glyphForCode(code) orelse {
                cursor_x += estimateType1MissingAdvance(run);
                continue;
            };
            const outline_value = font_lib.type1.glyphOutlineAlloc(alloc, glyph.charstring, type1.local_subrs) catch |err| blk: {
                if (err != error.UnsupportedType1) return err;
                if (try font_lib.type1.seacComponentsAlloc(alloc, glyph.charstring, type1.local_subrs)) |seac| {
                    try appendType1SeacRunShapesAlloc(alloc, out, run, type1, cursor_x, scale, seac);
                }
                break :blk null;
            };
            if (outline_value) |outline| {
                var owned_outline = outline;
                defer owned_outline.deinit(alloc);
                try appendType1OutlineShapesAlloc(alloc, out, run, cursor_x, scale, owned_outline, mode, 0);
            }
            const spacing = run.char_spacing + if (code == ' ') run.word_spacing else 0.0;
            cursor_x += (glyph.advance * scale + spacing) * run.horizontal_scale;
        }
    }

    fn appendType1SeacRunShapesAlloc(
        alloc: Allocator,
        out: *std.ArrayList(ShapeRun),
        run: TextRun,
        type1: Type1Font,
        cursor_x: f64,
        scale: f64,
        seac: font_lib.type1.SeacComponents,
    ) !void {
        const mode = @mod(run.render_mode, 8);
        if (type1.glyphForStandardCode(seac.bchar)) |base_glyph| {
            if (try font_lib.type1.glyphOutlineAlloc(alloc, base_glyph.charstring, type1.local_subrs)) |outline| {
                var owned_outline = outline;
                defer owned_outline.deinit(alloc);
                try appendType1OutlineShapesAlloc(alloc, out, run, cursor_x, scale, owned_outline, mode, 0);
            }
        }
        if (type1.glyphForStandardCode(seac.achar)) |accent_glyph| {
            if (try font_lib.type1.glyphOutlineAlloc(alloc, accent_glyph.charstring, type1.local_subrs)) |outline| {
                var owned_outline = outline;
                defer owned_outline.deinit(alloc);
                try appendType1OutlineShapesAlloc(alloc, out, run, cursor_x + (seac.adx - seac.asb) * scale, scale, owned_outline, mode, seac.ady * scale);
            }
        }
    }

    fn appendType1OutlineShapesAlloc(
        alloc: Allocator,
        out: *std.ArrayList(ShapeRun),
        run: TextRun,
        cursor_x: f64,
        scale: f64,
        outline: font_lib.sfnt.GlyphOutline,
        mode: i64,
        y_offset: f64,
    ) !void {
        for (outline.contours) |contour| {
            var shifted = try alloc.alloc(font_lib.sfnt.GlyphPoint, contour.points.len);
            defer alloc.free(shifted);
            for (contour.points, 0..) |point, i| {
                shifted[i] = .{
                    .x = point.x,
                    .y = point.y + y_offset / scale,
                    .on_curve = point.on_curve,
                };
            }
            const points = try flattenTrueTypeContourAlloc(alloc, shifted, run, cursor_x, scale);
            defer alloc.free(points);
            if (points.len < 3) continue;

            if (mode == 0 or mode == 2 or mode == 4 or mode == 6) {
                try out.append(alloc, .{
                    .paint_order = run.paint_order,
                    .blend_mode = run.blend_mode,
                    .group_id = run.group_id,
                    .group_parent_id = run.group_parent_id,
                    .group_isolated = run.group_isolated,
                    .group_knockout = run.group_knockout,
                    .kind = .fill,
                    .fill_rule = .nonzero,
                    .color = colorWithAlpha(run.fill_color, run.alpha),
                    .stroke_width = 0,
                    .closed = true,
                    .clip_box = run.clip_box,
                    .clip_points = if (run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                    .clip_fill_rule = run.clip_fill_rule,
                    .points = try alloc.dupe([2]f64, points),
                });
            }
            if (mode == 1 or mode == 2 or mode == 5 or mode == 6) {
                try out.append(alloc, .{
                    .paint_order = run.paint_order,
                    .blend_mode = run.blend_mode,
                    .group_id = run.group_id,
                    .group_parent_id = run.group_parent_id,
                    .group_isolated = run.group_isolated,
                    .group_knockout = run.group_knockout,
                    .kind = .stroke,
                    .fill_rule = .nonzero,
                    .color = colorWithAlpha(run.stroke_color, run.stroke_alpha),
                    .stroke_width = run.stroke_width,
                    .closed = true,
                    .clip_box = run.clip_box,
                    .clip_points = if (run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                    .clip_fill_rule = run.clip_fill_rule,
                    .points = try alloc.dupe([2]f64, points),
                });
            }
        }
    }

    fn appendCffRunShapesAlloc(
        alloc: Allocator,
        out: *std.ArrayList(ShapeRun),
        run: TextRun,
        cff_otf: CffOpenTypeFont,
    ) !void {
        if (run.text.len == 0) return;
        const mode = @mod(run.render_mode, 8);
        if (mode == 3 or mode == 7) return;

        const scale = if (cff_otf.units_per_em == 0) 1.0 else run.font_size / @as(f64, @floatFromInt(cff_otf.units_per_em));
        var cursor_x: f64 = 0;
        var view = std.unicode.Utf8View.init(run.text) catch return;
        var iter = view.iterator();
        var prev_glyph: ?u16 = null;
        while (iter.nextCodepoint()) |cp| {
            const glyph_index = try cff_otf.sfnt.cmapGlyphIndex(cp) orelse {
                cursor_x += estimateRenderedRunCodepointAdvance(run, cp);
                prev_glyph = null;
                continue;
            };
            const advance_width = try cff_otf.sfnt.advanceWidth(glyph_index);
            const kern = if (prev_glyph) |left| try cff_otf.sfnt.horizontalKerning(left, glyph_index) else 0;
            cursor_x += @as(f64, @floatFromInt(kern)) * scale * run.horizontal_scale;
            if (try cff_otf.cff.glyphOutlineAlloc(alloc, glyph_index)) |outline_value| {
                var outline = outline_value;
                defer outline.deinit(alloc);
                for (outline.contours) |contour| {
                    const points = try flattenTrueTypeContourAlloc(alloc, contour.points, run, cursor_x, scale);
                    defer alloc.free(points);
                    if (points.len < 3) continue;

                    if (mode == 0 or mode == 2 or mode == 4 or mode == 6) {
                        try out.append(alloc, .{
                            .paint_order = run.paint_order,
                            .blend_mode = run.blend_mode,
                            .group_id = run.group_id,
                            .group_parent_id = run.group_parent_id,
                            .group_isolated = run.group_isolated,
                            .group_knockout = run.group_knockout,
                            .kind = .fill,
                            .fill_rule = .nonzero,
                            .color = colorWithAlpha(run.fill_color, run.alpha),
                            .stroke_width = 0,
                            .closed = true,
                            .clip_box = run.clip_box,
                            .clip_points = if (run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                            .clip_fill_rule = run.clip_fill_rule,
                            .points = try alloc.dupe([2]f64, points),
                        });
                    }
                    if (mode == 1 or mode == 2 or mode == 5 or mode == 6) {
                        try out.append(alloc, .{
                            .paint_order = run.paint_order,
                            .blend_mode = run.blend_mode,
                            .group_id = run.group_id,
                            .group_parent_id = run.group_parent_id,
                            .group_isolated = run.group_isolated,
                            .group_knockout = run.group_knockout,
                            .kind = .stroke,
                            .fill_rule = .nonzero,
                            .color = colorWithAlpha(run.stroke_color, run.stroke_alpha),
                            .stroke_width = run.stroke_width,
                            .closed = true,
                            .clip_box = run.clip_box,
                            .clip_points = if (run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                            .clip_fill_rule = run.clip_fill_rule,
                            .points = try alloc.dupe([2]f64, points),
                        });
                    }
                }
            }
            const spacing = run.char_spacing + if (cp == ' ') run.word_spacing else 0.0;
            cursor_x += (@as(f64, @floatFromInt(advance_width)) * scale + spacing) * run.horizontal_scale;
            prev_glyph = glyph_index;
        }
    }

    fn collectPageFontsAlloc(self: *const Reader, page: *const syntax.Object) ![]PageFont {
        var resources = try self.findInheritedPageValue(page, "Resources");
        if (resources == null) return try self.alloc.alloc(PageFont, 0);
        defer if (resources) |*obj| obj.deinit(self.alloc);
        return try self.collectFontsFromResourcesAlloc(&resources.?);
    }

    fn collectPageImagesAlloc(self: *const Reader, page: *const syntax.Object) ![]PageImage {
        var resources = try self.findInheritedPageValue(page, "Resources");
        if (resources == null) return try self.alloc.alloc(PageImage, 0);
        defer if (resources) |*obj| obj.deinit(self.alloc);
        return try self.collectImagesFromResourcesAlloc(&resources.?);
    }

    fn collectPageShadingsAlloc(self: *const Reader, page: *const syntax.Object) ![]PageShading {
        var resources = try self.findInheritedPageValue(page, "Resources");
        if (resources == null) return try self.alloc.alloc(PageShading, 0);
        defer if (resources) |*obj| obj.deinit(self.alloc);
        return try self.collectShadingsFromResourcesAlloc(&resources.?);
    }

    fn collectPagePatternsAlloc(self: *const Reader, page: *const syntax.Object) ![]PagePattern {
        var resources = try self.findInheritedPageValue(page, "Resources");
        if (resources == null) return try self.alloc.alloc(PagePattern, 0);
        defer if (resources) |*obj| obj.deinit(self.alloc);
        return try self.collectPatternsFromResourcesAlloc(&resources.?, &resources.?, 0);
    }

    fn collectPageExtGStatesAlloc(self: *const Reader, page: *const syntax.Object) ![]PageExtGState {
        var resources = try self.findInheritedPageValue(page, "Resources");
        if (resources == null) return try self.alloc.alloc(PageExtGState, 0);
        defer if (resources) |*obj| obj.deinit(self.alloc);
        return try self.collectExtGStatesFromResourcesAlloc(&resources.?);
    }

    fn collectPageFormsAlloc(self: *const Reader, page: *const syntax.Object) ![]PageForm {
        var resources = try self.findInheritedPageValue(page, "Resources");
        if (resources == null) return try self.alloc.alloc(PageForm, 0);
        defer if (resources) |*obj| obj.deinit(self.alloc);
        return try self.collectFormsFromResourcesAlloc(&resources.?, &resources.?, 0);
    }

    fn collectFontsFromResourcesAlloc(self: *const Reader, resources: *const syntax.Object) ![]PageFont {
        const fonts_obj = resources.get("Font") orelse return try self.alloc.alloc(PageFont, 0);
        var resolved_fonts = try self.resolveValue(fonts_obj);
        defer resolved_fonts.deinit(self.alloc);
        if (resolved_fonts != .dict) return try self.alloc.alloc(PageFont, 0);

        var out = std.ArrayList(PageFont).empty;
        defer out.deinit(self.alloc);
        for (resolved_fonts.dict) |entry| {
            var font_obj = try self.resolveValue(&entry.value);
            defer font_obj.deinit(self.alloc);
            if (font_obj != .dict) continue;
            try out.append(self.alloc, try self.buildPageFont(entry.key, &font_obj));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn collectImagesFromResourcesAlloc(self: *const Reader, resources: *const syntax.Object) ![]PageImage {
        const xobject_obj = resources.get("XObject") orelse return try self.alloc.alloc(PageImage, 0);
        var resolved_xobjects = try self.resolveValue(xobject_obj);
        defer resolved_xobjects.deinit(self.alloc);
        if (resolved_xobjects != .dict) return try self.alloc.alloc(PageImage, 0);

        var out = std.ArrayList(PageImage).empty;
        defer out.deinit(self.alloc);
        for (resolved_xobjects.dict) |entry| {
            var xobj = try self.resolveValue(&entry.value);
            defer xobj.deinit(self.alloc);
            if (xobj != .stream) continue;
            const subtype = xobj.get("Subtype") orelse continue;
            if (!std.mem.eql(u8, subtype.asName() orelse continue, "Image")) continue;
            const decoded = try self.decodeImageToRgbaAlloc(&xobj);
            errdefer self.alloc.free(decoded.rgba);
            try out.append(self.alloc, .{
                .name = try self.alloc.dupe(u8, entry.key),
                .rgba = decoded.rgba,
                .width = decoded.width,
                .height = decoded.height,
            });
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn collectShadingsFromResourcesAlloc(self: *const Reader, resources: *const syntax.Object) ![]PageShading {
        const shadings_obj = resources.get("Shading") orelse return try self.alloc.alloc(PageShading, 0);
        var resolved_shadings = try self.resolveValue(shadings_obj);
        defer resolved_shadings.deinit(self.alloc);
        if (resolved_shadings != .dict) return try self.alloc.alloc(PageShading, 0);

        var out = std.ArrayList(PageShading).empty;
        defer out.deinit(self.alloc);
        for (resolved_shadings.dict) |entry| {
            var shading_obj = try self.resolveValue(&entry.value);
            defer shading_obj.deinit(self.alloc);
            const shading = self.buildPageShading(entry.key, &shading_obj) catch |err| switch (err) {
                error.UnsupportedPdfRendering => continue,
                else => return err,
            };
            try out.append(self.alloc, shading);
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn collectExtGStatesFromResourcesAlloc(self: *const Reader, resources: *const syntax.Object) ![]PageExtGState {
        const ext_obj = resources.get("ExtGState") orelse return try self.alloc.alloc(PageExtGState, 0);
        var resolved_ext = try self.resolveValue(ext_obj);
        defer resolved_ext.deinit(self.alloc);
        if (resolved_ext != .dict) return try self.alloc.alloc(PageExtGState, 0);

        var out = std.ArrayList(PageExtGState).empty;
        defer out.deinit(self.alloc);
        for (resolved_ext.dict) |entry| {
            var gs = try self.resolveValue(&entry.value);
            defer gs.deinit(self.alloc);
            if (gs != .dict) continue;
            const fill_alpha = if (gs.get("ca")) |obj| alphaByteFromNumber(numericObjectValue(obj) orelse 1.0) else @as(u8, 0xff);
            const stroke_alpha = if (gs.get("CA")) |obj| alphaByteFromNumber(numericObjectValue(obj) orelse 1.0) else @as(u8, 0xff);
            const blend_mode = if (gs.get("BM")) |obj| parseBlendModeObject(obj) else BlendMode.normal;
            try out.append(self.alloc, .{
                .name = try self.alloc.dupe(u8, entry.key),
                .fill_alpha = fill_alpha,
                .stroke_alpha = stroke_alpha,
                .blend_mode = blend_mode,
            });
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn collectPatternsFromResourcesAlloc(self: *const Reader, resources: *const syntax.Object, fallback_resources: ?*const syntax.Object, depth: u8) anyerror![]PagePattern {
        if (depth > 1) return try self.alloc.alloc(PagePattern, 0);
        const pattern_obj = resources.get("Pattern") orelse return try self.alloc.alloc(PagePattern, 0);
        var resolved_patterns = try self.resolveValue(pattern_obj);
        defer resolved_patterns.deinit(self.alloc);
        if (resolved_patterns != .dict) return try self.alloc.alloc(PagePattern, 0);

        var out = std.ArrayList(PagePattern).empty;
        defer out.deinit(self.alloc);
        for (resolved_patterns.dict) |entry| {
            var pat = try self.resolveValue(&entry.value);
            defer pat.deinit(self.alloc);
            const pattern_type = pat.get("PatternType") orelse continue;
            const pattern_type_value = pattern_type.asInteger() orelse -1;
            if (pattern_type_value != 1 and pattern_type_value != 2) continue;

            const matrix = blk: {
                const matrix_obj = pat.get("Matrix") orelse break :blk GraphicsMatrix{};
                if (matrix_obj.* != .array or matrix_obj.array.len < 6) break :blk GraphicsMatrix{};
                break :blk GraphicsMatrix{
                    .a = numericObjectValue(&matrix_obj.array[0]) orelse 1,
                    .b = numericObjectValue(&matrix_obj.array[1]) orelse 0,
                    .c = numericObjectValue(&matrix_obj.array[2]) orelse 0,
                    .d = numericObjectValue(&matrix_obj.array[3]) orelse 1,
                    .e = numericObjectValue(&matrix_obj.array[4]) orelse 0,
                    .f = numericObjectValue(&matrix_obj.array[5]) orelse 0,
                };
            };

            if (pattern_type_value == 2) {
                const shading_obj = pat.get("Shading") orelse continue;
                var resolved_shading = try self.resolveValue(shading_obj);
                defer resolved_shading.deinit(self.alloc);
                const shading = self.buildPageShading(entry.key, &resolved_shading) catch |err| switch (err) {
                    error.UnsupportedPdfRendering => continue,
                    else => return err,
                };
                try out.append(self.alloc, .{
                    .name = try self.alloc.dupe(u8, entry.key),
                    .kind = .shading,
                    .content = try self.alloc.alloc(u8, 0),
                    .bbox = .{ .min_x = 0, .min_y = 0, .max_x = 0, .max_y = 0 },
                    .x_step = 0,
                    .y_step = 0,
                    .matrix = matrix,
                    .colored = true,
                    .shading = shading,
                });
                continue;
            }

            if (pat != .stream) continue;

            const paint_type = if (pat.get("PaintType")) |obj| obj.asInteger() orelse 1 else 1;
            const bbox = if (pat.get("BBox")) |bbox_obj| try parsePageBox(bbox_obj) else continue;
            const x_step = if (pat.get("XStep")) |obj| numericObjectValue(obj) orelse 0 else 0;
            const y_step = if (pat.get("YStep")) |obj| numericObjectValue(obj) orelse 0 else 0;
            if (@abs(x_step) < 0.000001 or @abs(y_step) < 0.000001) continue;

            const content = try self.readDecodedStreamData(&pat);
            errdefer self.alloc.free(content);

            var resolved_pattern_resources: ?syntax.Object = null;
            defer if (resolved_pattern_resources) |*obj| obj.deinit(self.alloc);
            if (pat.get("Resources")) |pattern_resources_obj| {
                resolved_pattern_resources = try self.resolveValue(pattern_resources_obj);
            } else if (fallback_resources) |fallback| {
                resolved_pattern_resources = try fallback.clone(self.alloc);
            }

            const fonts = if (resolved_pattern_resources) |*pattern_resources| try self.collectFontsFromResourcesAlloc(pattern_resources) else try self.alloc.alloc(PageFont, 0);
            errdefer {
                for (fonts) |*font| font.deinit(self.alloc);
                if (fonts.len > 0) self.alloc.free(fonts);
            }
            const images = if (resolved_pattern_resources) |*pattern_resources| try self.collectImagesFromResourcesAlloc(pattern_resources) else try self.alloc.alloc(PageImage, 0);
            errdefer {
                for (images) |*image| image.deinit(self.alloc);
                if (images.len > 0) self.alloc.free(images);
            }
            const shadings = if (resolved_pattern_resources) |*pattern_resources| try self.collectShadingsFromResourcesAlloc(pattern_resources) else try self.alloc.alloc(PageShading, 0);
            errdefer {
                for (shadings) |*shading| shading.deinit(self.alloc);
                if (shadings.len > 0) self.alloc.free(shadings);
            }
            const patterns = if (resolved_pattern_resources) |*pattern_resources| try self.collectPatternsFromResourcesAlloc(pattern_resources, fallback_resources orelse pattern_resources, depth + 1) else try self.alloc.alloc(PagePattern, 0);
            errdefer {
                for (patterns) |*pattern| pattern.deinit(self.alloc);
                if (patterns.len > 0) self.alloc.free(patterns);
            }
            const gstates = if (resolved_pattern_resources) |*pattern_resources| try self.collectExtGStatesFromResourcesAlloc(pattern_resources) else try self.alloc.alloc(PageExtGState, 0);
            errdefer {
                for (gstates) |*gstate| gstate.deinit(self.alloc);
                if (gstates.len > 0) self.alloc.free(gstates);
            }
            const forms = if (resolved_pattern_resources) |*pattern_resources| try self.collectFormsFromResourcesAlloc(pattern_resources, fallback_resources orelse pattern_resources, depth + 1) else try self.alloc.alloc(PageForm, 0);
            errdefer {
                for (forms) |*form| form.deinit(self.alloc);
                if (forms.len > 0) self.alloc.free(forms);
            }

            try out.append(self.alloc, .{
                .name = try self.alloc.dupe(u8, entry.key),
                .content = content,
                .bbox = bbox,
                .x_step = x_step,
                .y_step = y_step,
                .matrix = matrix,
                .colored = paint_type == 1,
                .fonts = fonts,
                .images = images,
                .shadings = shadings,
                .patterns = patterns,
                .gstates = gstates,
                .forms = forms,
            });
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn collectFormsFromResourcesAlloc(self: *const Reader, resources: *const syntax.Object, fallback_resources: ?*const syntax.Object, depth: u8) anyerror![]PageForm {
        if (depth > 1) return try self.alloc.alloc(PageForm, 0);
        const xobject_obj = resources.get("XObject") orelse return try self.alloc.alloc(PageForm, 0);
        var resolved_xobjects = try self.resolveValue(xobject_obj);
        defer resolved_xobjects.deinit(self.alloc);
        if (resolved_xobjects != .dict) return try self.alloc.alloc(PageForm, 0);

        var out = std.ArrayList(PageForm).empty;
        defer out.deinit(self.alloc);
        for (resolved_xobjects.dict) |entry| {
            var xobj = try self.resolveValue(&entry.value);
            defer xobj.deinit(self.alloc);
            if (xobj != .stream) continue;
            const subtype = xobj.get("Subtype") orelse continue;
            if (!std.mem.eql(u8, subtype.asName() orelse continue, "Form")) continue;

            const content = try self.readDecodedStreamData(&xobj);
            errdefer self.alloc.free(content);

            var resolved_form_resources: ?syntax.Object = null;
            defer if (resolved_form_resources) |*obj| obj.deinit(self.alloc);
            if (xobj.get("Resources")) |form_resources_obj| {
                resolved_form_resources = try self.resolveValue(form_resources_obj);
            } else if (fallback_resources) |fallback| {
                resolved_form_resources = try fallback.clone(self.alloc);
            }

            const fonts = if (resolved_form_resources) |*form_resources| try self.collectFontsFromResourcesAlloc(form_resources) else try self.alloc.alloc(PageFont, 0);
            errdefer {
                for (fonts) |*font| font.deinit(self.alloc);
                if (fonts.len > 0) self.alloc.free(fonts);
            }
            const images = if (resolved_form_resources) |*form_resources| try self.collectImagesFromResourcesAlloc(form_resources) else try self.alloc.alloc(PageImage, 0);
            errdefer {
                for (images) |*image| image.deinit(self.alloc);
                if (images.len > 0) self.alloc.free(images);
            }
            const shadings = if (resolved_form_resources) |*form_resources| try self.collectShadingsFromResourcesAlloc(form_resources) else try self.alloc.alloc(PageShading, 0);
            errdefer {
                for (shadings) |*shading| shading.deinit(self.alloc);
                if (shadings.len > 0) self.alloc.free(shadings);
            }
            const patterns = if (resolved_form_resources) |*form_resources| try self.collectPatternsFromResourcesAlloc(form_resources, fallback_resources orelse form_resources, depth + 1) else try self.alloc.alloc(PagePattern, 0);
            errdefer {
                for (patterns) |*pattern| pattern.deinit(self.alloc);
                if (patterns.len > 0) self.alloc.free(patterns);
            }
            const gstates = if (resolved_form_resources) |*form_resources| try self.collectExtGStatesFromResourcesAlloc(form_resources) else try self.alloc.alloc(PageExtGState, 0);
            errdefer {
                for (gstates) |*gstate| gstate.deinit(self.alloc);
                if (gstates.len > 0) self.alloc.free(gstates);
            }
            const forms = if (resolved_form_resources) |*form_resources| try self.collectFormsFromResourcesAlloc(form_resources, fallback_resources orelse form_resources, depth + 1) else try self.alloc.alloc(PageForm, 0);
            errdefer {
                for (forms) |*form| form.deinit(self.alloc);
                if (forms.len > 0) self.alloc.free(forms);
            }

            try out.append(self.alloc, .{
                .name = try self.alloc.dupe(u8, entry.key),
                .content = content,
                .matrix = blk: {
                    const matrix_obj = xobj.get("Matrix") orelse break :blk .{};
                    if (matrix_obj.* != .array or matrix_obj.array.len < 6) break :blk .{};
                    break :blk .{
                        .a = numericObjectValue(&matrix_obj.array[0]) orelse 1,
                        .b = numericObjectValue(&matrix_obj.array[1]) orelse 0,
                        .c = numericObjectValue(&matrix_obj.array[2]) orelse 0,
                        .d = numericObjectValue(&matrix_obj.array[3]) orelse 1,
                        .e = numericObjectValue(&matrix_obj.array[4]) orelse 0,
                        .f = numericObjectValue(&matrix_obj.array[5]) orelse 0,
                    };
                },
                .bbox = if (xobj.get("BBox")) |bbox_obj| try parsePageBox(bbox_obj) else null,
                .transparency_group = blk: {
                    const group_obj = xobj.get("Group") orelse break :blk false;
                    var resolved_group = try self.resolveValue(group_obj);
                    defer resolved_group.deinit(self.alloc);
                    if (resolved_group != .dict) break :blk false;
                    const group_subtype = dictGetObject(resolved_group.dict, "S") orelse break :blk false;
                    break :blk std.mem.eql(u8, group_subtype.asName() orelse "", "Transparency");
                },
                .group_isolated = blk: {
                    const group_obj = xobj.get("Group") orelse break :blk false;
                    var resolved_group = try self.resolveValue(group_obj);
                    defer resolved_group.deinit(self.alloc);
                    if (resolved_group != .dict) break :blk false;
                    const isolate = dictGetObject(resolved_group.dict, "I") orelse break :blk false;
                    break :blk switch (isolate.*) {
                        .boolean => |b| b,
                        else => false,
                    };
                },
                .group_knockout = blk: {
                    const group_obj = xobj.get("Group") orelse break :blk false;
                    var resolved_group = try self.resolveValue(group_obj);
                    defer resolved_group.deinit(self.alloc);
                    if (resolved_group != .dict) break :blk false;
                    const knockout = dictGetObject(resolved_group.dict, "K") orelse break :blk false;
                    break :blk switch (knockout.*) {
                        .boolean => |b| b,
                        else => false,
                    };
                },
                .fonts = fonts,
                .images = images,
                .shadings = shadings,
                .patterns = patterns,
                .gstates = gstates,
                .forms = forms,
            });
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn buildPageShading(self: *const Reader, name: []const u8, obj: *const syntax.Object) !PageShading {
        const dict: []const syntax.DictEntry = switch (obj.*) {
            .dict => |entries| entries,
            .stream => |stream| stream.header,
            else => return error.UnsupportedPdfRendering,
        };

        const shading_type = dictGetInteger(dict, "ShadingType") orelse return error.UnsupportedPdfRendering;
        if (shading_type != 2 and shading_type != 3) return error.UnsupportedPdfRendering;

        const color_space_name = if (dictGetArray(dict, "ColorSpace")) |arr|
            arr[0].asName() orelse return error.UnsupportedPdfRendering
        else blk: {
            for (dict) |entry| {
                if (std.mem.eql(u8, entry.key, "ColorSpace")) {
                    break :blk entry.value.asName() orelse return error.UnsupportedPdfRendering;
                }
            }
            return error.UnsupportedPdfRendering;
        };
        const components: usize = if (std.mem.eql(u8, color_space_name, "DeviceGray"))
            1
        else if (std.mem.eql(u8, color_space_name, "DeviceRGB"))
            3
        else if (std.mem.eql(u8, color_space_name, "DeviceCMYK"))
            4
        else
            return error.UnsupportedPdfRendering;

        const coords = dictGetArray(dict, "Coords") orelse return error.UnsupportedPdfRendering;
        if ((shading_type == 2 and coords.len < 4) or (shading_type == 3 and coords.len < 6)) return error.UnsupportedPdfRendering;

        var transform = try self.parseExponentialTintTransform(dictGetObject(dict, "Function") orelse return error.UnsupportedPdfRendering, components);
        defer transform.deinit(self.alloc);

        var c0_components: [4]u8 = .{ 0, 0, 0, 0xff };
        var c1_components: [4]u8 = .{ 0, 0, 0, 0xff };
        for (0..components) |i| {
            c0_components[i] = floatChannel(evalExponentialTintComponent(&transform, 0.0, i));
            c1_components[i] = floatChannel(evalExponentialTintComponent(&transform, 1.0, i));
        }

        const c0 = colorFromComponents(color_space_name, c0_components, components) orelse return error.UnsupportedPdfRendering;
        const c1 = colorFromComponents(color_space_name, c1_components, components) orelse return error.UnsupportedPdfRendering;
        const extend = dictGetArray(dict, "Extend");

        return .{
            .name = try self.alloc.dupe(u8, name),
            .kind = if (shading_type == 2) .axial else .radial,
            .x0 = numericObjectValue(&coords[0]) orelse 0,
            .y0 = numericObjectValue(&coords[1]) orelse 0,
            .r0 = if (shading_type == 3) numericObjectValue(&coords[2]) orelse 0 else 0,
            .x1 = numericObjectValue(&coords[if (shading_type == 2) 2 else 3]) orelse 0,
            .y1 = numericObjectValue(&coords[if (shading_type == 2) 3 else 4]) orelse 0,
            .r1 = if (shading_type == 3) numericObjectValue(&coords[5]) orelse 0 else 0,
            .c0 = c0,
            .c1 = c1,
            .extend_start = if (extend) |arr| if (arr.len >= 1) switch (arr[0]) {
                .boolean => |b| b,
                else => false,
            } else false else false,
            .extend_end = if (extend) |arr| if (arr.len >= 2) switch (arr[1]) {
                .boolean => |b| b,
                else => false,
            } else false else false,
        };
    }

    fn decodeImageToRgbaAlloc(self: *const Reader, obj: *const syntax.Object) anyerror!struct { rgba: []u8, width: u32, height: u32 } {
        const width_i = (obj.get("Width") orelse return error.UnsupportedPdfRendering).asInteger() orelse return error.UnsupportedPdfRendering;
        const height_i = (obj.get("Height") orelse return error.UnsupportedPdfRendering).asInteger() orelse return error.UnsupportedPdfRendering;
        const image_mask = if (obj.get("ImageMask")) |v| switch (v.*) {
            .boolean => |b| b,
            else => false,
        } else false;
        const bits_i: i64 = if (obj.get("BitsPerComponent")) |v|
            v.asInteger() orelse return error.UnsupportedPdfRendering
        else if (image_mask)
            1
        else
            8;
        if (width_i <= 0 or height_i <= 0) return error.UnsupportedPdfRendering;
        const has_ccitt = streamHasFilter(obj.get("Filter"), "CCITTFaxDecode");
        const has_jpx = streamHasFilter(obj.get("Filter"), "JPXDecode");
        if (!image_mask and !has_ccitt and !has_jpx and bits_i != 8) return error.UnsupportedPdfRendering;
        if (!image_mask and has_ccitt and bits_i != 1) return error.UnsupportedPdfRendering;
        if (image_mask and bits_i != 1) return error.UnsupportedPdfRendering;

        const width: u32 = @intCast(width_i);
        const height: u32 = @intCast(height_i);
        if (has_ccitt) {
            const raw = try self.readRawStreamData(obj);
            defer self.alloc.free(raw);
            const filter_param = streamFilterParamFor(obj.get("Filter"), obj.get("DecodeParms"), "CCITTFaxDecode");
            const gray = try decodeCcittGrayAlloc(self.alloc, raw, width, height, filter_param);
            defer self.alloc.free(gray);

            const pixel_count = @as(usize, width) * @as(usize, height);
            const rgba = try self.alloc.alloc(u8, pixel_count * 4);
            if (image_mask) {
                try decodeGrayMaskToRgba(rgba, gray, obj.get("Decode"));
                return .{ .rgba = rgba, .width = width, .height = height };
            }

            const color_space_obj = obj.get("ColorSpace") orelse return error.UnsupportedPdfRendering;
            var resolved_color_space = try self.resolveValue(color_space_obj);
            defer resolved_color_space.deinit(self.alloc);
            if (resolved_color_space.asName()) |color_space| {
                try decodeDeviceColorSpaceToRgba(rgba, pixel_count, gray, color_space, obj.get("Decode"));
            } else if (resolved_color_space == .array) {
                if (try self.tryDecodeIccBasedImageToRgba(rgba, pixel_count, gray, resolved_color_space.array, obj.get("Decode"))) {
                    // handled
                } else if (try self.tryDecodeCalibratedImageToRgba(rgba, pixel_count, gray, resolved_color_space.array, obj.get("Decode"))) {
                    // handled
                } else if (try self.tryDecodeSpotColorSpaceToRgba(rgba, pixel_count, gray, resolved_color_space.array, obj.get("Decode"))) {
                    // handled
                } else {
                    try self.decodeIndexedImageToRgba(rgba, pixel_count, gray, resolved_color_space.array, obj.get("Decode"));
                }
            } else {
                return error.UnsupportedPdfRendering;
            }
            try self.applyImageTransparencyAlloc(rgba, width, height, obj);
            return .{ .rgba = rgba, .width = width, .height = height };
        }
        if (!image_mask and has_jpx) {
            const raw = try self.readRawStreamData(obj);
            defer self.alloc.free(raw);
            var jp2_decoded = try image_lib.jpeg2000.decodeU8Bytes(self.alloc, raw);
            defer jp2_decoded.deinit();
            if (jp2_decoded.width != width or jp2_decoded.height != height) return error.UnsupportedPdfRendering;
            const rgba = try jpeg2000DecodedToRgbaAlloc(self.alloc, &jp2_decoded);
            errdefer self.alloc.free(rgba);
            try self.applyImageTransparencyAlloc(rgba, width, height, obj);
            return .{
                .rgba = rgba,
                .width = jp2_decoded.width,
                .height = jp2_decoded.height,
            };
        }
        if (!image_mask and streamHasFilter(obj.get("Filter"), "DCTDecode")) {
            const raw = try self.readRawStreamData(obj);
            defer self.alloc.free(raw);
            const jpeg_decoded = try image_lib.jpeg.decodeRgba(self.alloc, raw);
            errdefer self.alloc.free(jpeg_decoded.rgba);
            if (jpeg_decoded.width != width or jpeg_decoded.height != height) return error.UnsupportedPdfRendering;
            try self.applyImageTransparencyAlloc(jpeg_decoded.rgba, width, height, obj);
            return .{
                .rgba = jpeg_decoded.rgba,
                .width = jpeg_decoded.width,
                .height = jpeg_decoded.height,
            };
        }
        const decoded = try self.readDecodedStreamData(obj);
        errdefer self.alloc.free(decoded);

        const pixel_count = @as(usize, width) * @as(usize, height);
        const rgba = try self.alloc.alloc(u8, pixel_count * 4);
        if (image_mask) {
            try decodeImageMaskToRgba(rgba, width, height, decoded, obj.get("Decode"));
            self.alloc.free(decoded);
            return .{ .rgba = rgba, .width = width, .height = height };
        }

        const color_space_obj = obj.get("ColorSpace") orelse return error.UnsupportedPdfRendering;
        var resolved_color_space = try self.resolveValue(color_space_obj);
        defer resolved_color_space.deinit(self.alloc);
        if (resolved_color_space.asName()) |color_space| {
            try decodeDeviceColorSpaceToRgba(rgba, pixel_count, decoded, color_space, obj.get("Decode"));
        } else if (resolved_color_space == .array) {
            if (try self.tryDecodeIccBasedImageToRgba(rgba, pixel_count, decoded, resolved_color_space.array, obj.get("Decode"))) {
                // handled
            } else if (try self.tryDecodeCalibratedImageToRgba(rgba, pixel_count, decoded, resolved_color_space.array, obj.get("Decode"))) {
                // handled
            } else if (try self.tryDecodeSpotColorSpaceToRgba(rgba, pixel_count, decoded, resolved_color_space.array, obj.get("Decode"))) {
                // handled
            } else {
                try self.decodeIndexedImageToRgba(rgba, pixel_count, decoded, resolved_color_space.array, obj.get("Decode"));
            }
        } else {
            return error.UnsupportedPdfRendering;
        }

        try self.applyImageTransparencyAlloc(rgba, width, height, obj);

        self.alloc.free(decoded);
        return .{ .rgba = rgba, .width = width, .height = height };
    }

    fn jpeg2000DecodedToRgbaAlloc(alloc: Allocator, decoded: *const image_lib.jpeg2000.DecodedImage) ![]u8 {
        const pixel_count = @as(usize, decoded.width) * @as(usize, decoded.height);
        if (decoded.pixels.len != pixel_count * decoded.components) return error.UnsupportedPdfRendering;

        const rgba = try alloc.alloc(u8, pixel_count * 4);
        errdefer alloc.free(rgba);
        switch (decoded.components) {
            1 => {
                for (decoded.pixels, 0..) |gray, i| {
                    const dst = i * 4;
                    rgba[dst + 0] = gray;
                    rgba[dst + 1] = gray;
                    rgba[dst + 2] = gray;
                    rgba[dst + 3] = 0xff;
                }
            },
            3 => {
                var i: usize = 0;
                while (i < pixel_count) : (i += 1) {
                    const src = i * 3;
                    const dst = i * 4;
                    rgba[dst + 0] = decoded.pixels[src + 0];
                    rgba[dst + 1] = decoded.pixels[src + 1];
                    rgba[dst + 2] = decoded.pixels[src + 2];
                    rgba[dst + 3] = 0xff;
                }
            },
            else => return error.UnsupportedPdfRendering,
        }
        return rgba;
    }

    fn tryDecodeIccBasedImageToRgba(
        self: *const Reader,
        rgba: []u8,
        pixel_count: usize,
        decoded: []const u8,
        color_space: []const syntax.Object,
        decode_obj: ?*const syntax.Object,
    ) !bool {
        if (color_space.len < 2) return false;
        const color_space_name = color_space[0].asName() orelse return false;
        if (!std.mem.eql(u8, color_space_name, "ICCBased")) return false;

        var profile = try self.resolveValue(&color_space[1]);
        defer profile.deinit(self.alloc);
        if (profile != .stream) return error.UnsupportedPdfRendering;

        const device_name = if (profile.get("Alternate")) |alternate|
            alternate.asName() orelse return error.UnsupportedPdfRendering
        else if (profile.get("N")) |components|
            switch (components.asInteger() orelse return error.UnsupportedPdfRendering) {
                1 => "DeviceGray",
                3 => "DeviceRGB",
                4 => "DeviceCMYK",
                else => return error.UnsupportedPdfRendering,
            }
        else
            return error.UnsupportedPdfRendering;

        try decodeDeviceColorSpaceToRgba(rgba, pixel_count, decoded, device_name, decode_obj);
        return true;
    }

    fn tryDecodeCalibratedImageToRgba(
        _: *const Reader,
        rgba: []u8,
        pixel_count: usize,
        decoded: []const u8,
        color_space: []const syntax.Object,
        decode_obj: ?*const syntax.Object,
    ) !bool {
        if (color_space.len < 2) return false;
        const name = color_space[0].asName() orelse return false;
        if (std.mem.eql(u8, name, "CalGray")) {
            try decodeDeviceColorSpaceToRgba(rgba, pixel_count, decoded, "DeviceGray", decode_obj);
            return true;
        }
        if (std.mem.eql(u8, name, "CalRGB")) {
            try decodeDeviceColorSpaceToRgba(rgba, pixel_count, decoded, "DeviceRGB", decode_obj);
            return true;
        }
        if (std.mem.eql(u8, name, "Lab")) {
            if (decoded.len < pixel_count * 3) return error.UnsupportedPdfRendering;
            const params = if (color_space[1] == .dict) color_space[1].dict else return error.UnsupportedPdfRendering;
            const range_a = parseLabRange(params, "Range", 0, -100.0, 100.0);
            const range_b = parseLabRange(params, "Range", 2, -100.0, 100.0);
            const white = parseLabWhitePoint(params);
            var i: usize = 0;
            while (i < pixel_count) : (i += 1) {
                const src = i * 3;
                const dst = i * 4;
                const l = applyDecodeUnit(decoded[src + 0], decode_obj, 0) * 100.0;
                const a = mapUnitToRange(applyDecodeUnit(decoded[src + 1], decode_obj, 1), range_a[0], range_a[1]);
                const b = mapUnitToRange(applyDecodeUnit(decoded[src + 2], decode_obj, 2), range_b[0], range_b[1]);
                const color = labColor(l, a, b, white);
                rgba[dst + 0] = color[0];
                rgba[dst + 1] = color[1];
                rgba[dst + 2] = color[2];
                rgba[dst + 3] = color[3];
            }
            return true;
        }
        return false;
    }

    fn decodeIndexedImageToRgba(
        self: *const Reader,
        rgba: []u8,
        pixel_count: usize,
        decoded: []const u8,
        color_space: []const syntax.Object,
        decode_obj: ?*const syntax.Object,
    ) !void {
        if (color_space.len < 4) return error.UnsupportedPdfRendering;
        const indexed_name = color_space[0].asName() orelse return error.UnsupportedPdfRendering;
        if (!std.mem.eql(u8, indexed_name, "Indexed")) return error.UnsupportedPdfRendering;

        const base_name = color_space[1].asName() orelse return error.UnsupportedPdfRendering;
        const hi_val = color_space[2].asInteger() orelse return error.UnsupportedPdfRendering;
        if (hi_val < 0 or decoded.len < pixel_count) return error.UnsupportedPdfRendering;
        const comps: usize = if (std.mem.eql(u8, base_name, "DeviceRGB"))
            3
        else if (std.mem.eql(u8, base_name, "DeviceGray"))
            1
        else if (std.mem.eql(u8, base_name, "DeviceCMYK"))
            4
        else
            return error.UnsupportedPdfRendering;

        var lookup_holder: ?syntax.Object = null;
        defer if (lookup_holder) |*obj| obj.deinit(self.alloc);
        const lookup_obj: *const syntax.Object = blk: {
            const candidate = &color_space[3];
            if (candidate.* == .obj_ref) {
                lookup_holder = try self.resolveValue(candidate);
                break :blk &lookup_holder.?;
            }
            break :blk candidate;
        };
        const lookup = switch (lookup_obj.*) {
            .string => lookup_obj.string,
            else => return error.UnsupportedPdfRendering,
        };
        const palette_entries: usize = @intCast(hi_val + 1);
        if (lookup.len < palette_entries * comps) return error.UnsupportedPdfRendering;

        var i: usize = 0;
        while (i < pixel_count) : (i += 1) {
            const idx = try applyIndexedDecode(decoded[i], decode_obj, palette_entries);
            if (idx >= palette_entries) return error.UnsupportedPdfRendering;
            const src = @as(usize, idx) * comps;
            const dst = i * 4;
            if (comps == 1) {
                rgba[dst + 0] = lookup[src];
                rgba[dst + 1] = lookup[src];
                rgba[dst + 2] = lookup[src];
                rgba[dst + 3] = 0xff;
            } else if (comps == 3) {
                rgba[dst + 0] = lookup[src + 0];
                rgba[dst + 1] = lookup[src + 1];
                rgba[dst + 2] = lookup[src + 2];
                rgba[dst + 3] = 0xff;
            } else {
                const color = cmykColor(
                    @as(f64, @floatFromInt(lookup[src + 0])) / 255.0,
                    @as(f64, @floatFromInt(lookup[src + 1])) / 255.0,
                    @as(f64, @floatFromInt(lookup[src + 2])) / 255.0,
                    @as(f64, @floatFromInt(lookup[src + 3])) / 255.0,
                );
                rgba[dst + 0] = color[0];
                rgba[dst + 1] = color[1];
                rgba[dst + 2] = color[2];
                rgba[dst + 3] = color[3];
            }
        }
    }

    fn tryDecodeSpotColorSpaceToRgba(
        self: *const Reader,
        rgba: []u8,
        pixel_count: usize,
        decoded: []const u8,
        color_space: []const syntax.Object,
        decode_obj: ?*const syntax.Object,
    ) !bool {
        if (color_space.len < 4) return false;
        const cs_name = color_space[0].asName() orelse return false;
        var tint_components: usize = 0;
        var alt_index: usize = 0;
        var fn_index: usize = 0;
        if (std.mem.eql(u8, cs_name, "Separation")) {
            tint_components = 1;
            alt_index = 2;
            fn_index = 3;
        } else if (std.mem.eql(u8, cs_name, "DeviceN")) {
            if (color_space[1] != .array or color_space[1].array.len != 1) return false;
            tint_components = 1;
            alt_index = 2;
            fn_index = 3;
        } else {
            return false;
        }

        if (decoded.len < pixel_count * tint_components) return error.UnsupportedPdfRendering;
        const alt_name = color_space[alt_index].asName() orelse return error.UnsupportedPdfRendering;
        const alt_components: usize = if (std.mem.eql(u8, alt_name, "DeviceGray"))
            1
        else if (std.mem.eql(u8, alt_name, "DeviceRGB"))
            3
        else if (std.mem.eql(u8, alt_name, "DeviceCMYK"))
            4
        else
            return error.UnsupportedPdfRendering;

        var transform = try self.parseExponentialTintTransform(&color_space[fn_index], alt_components);
        defer transform.deinit(self.alloc);

        const alt_bytes = try self.alloc.alloc(u8, pixel_count * alt_components);
        defer self.alloc.free(alt_bytes);

        var i: usize = 0;
        while (i < pixel_count) : (i += 1) {
            const tint = applyDecodeUnit(decoded[i], decode_obj, 0);
            for (0..alt_components) |component| {
                alt_bytes[i * alt_components + component] = floatChannel(evalExponentialTintComponent(&transform, tint, component));
            }
        }

        try decodeDeviceColorSpaceToRgba(rgba, pixel_count, alt_bytes, alt_name, null);
        return true;
    }

    fn parseExponentialTintTransform(
        self: *const Reader,
        obj: *const syntax.Object,
        alt_components: usize,
    ) !ExponentialTintTransform {
        var resolved = try self.resolveValue(obj);
        defer resolved.deinit(self.alloc);

        const dict: []const syntax.DictEntry = switch (resolved) {
            .dict => |entries| entries,
            .stream => |stream| stream.header,
            else => return error.UnsupportedPdfRendering,
        };

        const fn_type = dictGetInteger(dict, "FunctionType") orelse return error.UnsupportedPdfRendering;
        if (fn_type != 2) return error.UnsupportedPdfRendering;
        const exponent = dictGetNumber(dict, "N") orelse return error.UnsupportedPdfRendering;
        if (exponent <= 0) return error.UnsupportedPdfRendering;

        var c0 = try self.alloc.alloc(f64, alt_components);
        errdefer self.alloc.free(c0);
        var c1 = try self.alloc.alloc(f64, alt_components);
        errdefer self.alloc.free(c1);
        for (0..alt_components) |i| {
            c0[i] = 0.0;
            c1[i] = 1.0;
        }

        if (dictGetArray(dict, "C0")) |values| {
            if (values.len < alt_components) return error.UnsupportedPdfRendering;
            for (0..alt_components) |i| c0[i] = numericObjectValue(&values[i]) orelse return error.UnsupportedPdfRendering;
        }
        if (dictGetArray(dict, "C1")) |values| {
            if (values.len < alt_components) return error.UnsupportedPdfRendering;
            for (0..alt_components) |i| c1[i] = numericObjectValue(&values[i]) orelse return error.UnsupportedPdfRendering;
        }

        return .{
            .n = exponent,
            .c0 = c0,
            .c1 = c1,
        };
    }

    fn decodeDeviceColorSpaceToRgba(
        rgba: []u8,
        pixel_count: usize,
        decoded: []const u8,
        color_space: []const u8,
        decode_obj: ?*const syntax.Object,
    ) !void {
        if (std.mem.eql(u8, color_space, "DeviceRGB")) {
            if (decoded.len < pixel_count * 3) return error.UnsupportedPdfRendering;
            var i: usize = 0;
            while (i < pixel_count) : (i += 1) {
                const src = i * 3;
                const dst = i * 4;
                rgba[dst + 0] = applyDecodeByte(decoded[src + 0], decode_obj, 0);
                rgba[dst + 1] = applyDecodeByte(decoded[src + 1], decode_obj, 1);
                rgba[dst + 2] = applyDecodeByte(decoded[src + 2], decode_obj, 2);
                rgba[dst + 3] = 0xff;
            }
            return;
        }
        if (std.mem.eql(u8, color_space, "DeviceGray")) {
            if (decoded.len < pixel_count) return error.UnsupportedPdfRendering;
            var i: usize = 0;
            while (i < pixel_count) : (i += 1) {
                const dst = i * 4;
                const gray = applyDecodeByte(decoded[i], decode_obj, 0);
                rgba[dst + 0] = gray;
                rgba[dst + 1] = gray;
                rgba[dst + 2] = gray;
                rgba[dst + 3] = 0xff;
            }
            return;
        }
        if (std.mem.eql(u8, color_space, "DeviceCMYK")) {
            if (decoded.len < pixel_count * 4) return error.UnsupportedPdfRendering;
            var i: usize = 0;
            while (i < pixel_count) : (i += 1) {
                const src = i * 4;
                const dst = i * 4;
                const color = cmykColor(
                    applyDecodeUnit(decoded[src + 0], decode_obj, 0),
                    applyDecodeUnit(decoded[src + 1], decode_obj, 1),
                    applyDecodeUnit(decoded[src + 2], decode_obj, 2),
                    applyDecodeUnit(decoded[src + 3], decode_obj, 3),
                );
                rgba[dst + 0] = color[0];
                rgba[dst + 1] = color[1];
                rgba[dst + 2] = color[2];
                rgba[dst + 3] = color[3];
            }
            return;
        }
        return error.UnsupportedPdfRendering;
    }

    fn decodeImageMaskToRgba(
        rgba: []u8,
        width: u32,
        height: u32,
        decoded: []const u8,
        decode_obj: ?*const syntax.Object,
    ) !void {
        const row_bytes = @divFloor(@as(usize, width) + 7, 8);
        if (decoded.len < row_bytes * @as(usize, height)) return error.UnsupportedPdfRendering;
        const decode_zero_is_paint = parseMaskDecodeInvert(decode_obj);

        var y: usize = 0;
        while (y < height) : (y += 1) {
            var x: usize = 0;
            while (x < width) : (x += 1) {
                const byte = decoded[y * row_bytes + x / 8];
                const bit_index: u3 = @intCast(7 - @as(u3, @intCast(x % 8)));
                const bit = (byte >> bit_index) & 1;
                const paint = if (decode_zero_is_paint) bit == 0 else bit == 1;
                const dst = (y * @as(usize, width) + x) * 4;
                rgba[dst + 0] = 0;
                rgba[dst + 1] = 0;
                rgba[dst + 2] = 0;
                rgba[dst + 3] = if (paint) 0xff else 0x00;
            }
        }
    }

    fn applyImageTransparencyAlloc(
        self: *const Reader,
        rgba: []u8,
        width: u32,
        height: u32,
        obj: *const syntax.Object,
    ) anyerror!void {
        if (obj.get("SMask")) |smask_obj| {
            var resolved = try self.resolveValue(smask_obj);
            defer resolved.deinit(self.alloc);
            if (resolved == .stream) {
                const smask = try self.decodeImageToRgbaAlloc(&resolved);
                defer self.alloc.free(smask.rgba);
                if (smask.width == width and smask.height == height) {
                    applySoftMaskAlpha(rgba, smask.rgba);
                }
            }
        }

        if (obj.get("Mask")) |mask_obj| {
            var resolved = try self.resolveValue(mask_obj);
            defer resolved.deinit(self.alloc);
            if (resolved == .array) {
                applyColorKeyMask(rgba, resolved.array);
            } else if (resolved == .stream) {
                const mask = try self.decodeImageToRgbaAlloc(&resolved);
                defer self.alloc.free(mask.rgba);
                if (mask.width == width and mask.height == height) {
                    const mask_is_stencil = if (resolved.get("ImageMask")) |value| switch (value.*) {
                        .boolean => |flag| flag,
                        else => false,
                    } else false;
                    applyExplicitMaskAlpha(rgba, mask.rgba, mask_is_stencil);
                }
            }
        }
    }

    fn findInheritedPageValue(self: *const Reader, page: *const syntax.Object, key: []const u8) !?syntax.Object {
        if (page.get(key)) |value| {
            return try self.resolveValue(value);
        }
        if (page.get("Parent")) |parent| {
            var resolved_parent = try self.resolveValue(parent);
            defer resolved_parent.deinit(self.alloc);
            if (resolved_parent != .dict) return null;
            return try self.findInheritedPageValue(&resolved_parent, key);
        }
        return null;
    }

    fn buildPageFont(self: *const Reader, name: []const u8, font_obj: *const syntax.Object) !PageFont {
        var font = PageFont{
            .name = try self.alloc.dupe(u8, name),
            .decoder = try self.buildFontDecoder(font_obj),
            .type3 = null,
            .type1 = null,
            .truetype = null,
            .cff_otf = null,
        };
        errdefer font.deinit(self.alloc);
        font.type3 = try self.buildType3Font(font_obj);
        font.type1 = try self.buildType1Font(font_obj);
        font.truetype = try self.buildTrueTypeFont(font_obj);
        font.cff_otf = try self.buildCffOpenTypeFont(font_obj);
        return font;
    }

    fn buildFontDecoder(self: *const Reader, font_obj: *const syntax.Object) !FontDecoder {
        var decoder = FontDecoder{};

        if (font_obj.get("Encoding")) |encoding_obj| {
            var resolved_encoding = try self.resolveValue(encoding_obj);
            defer resolved_encoding.deinit(self.alloc);
            switch (resolved_encoding) {
                .name => |name| {
                    decoder.base_encoding = namedEncodingFromName(name);
                },
                .dict => {
                    if (resolved_encoding.get("BaseEncoding")) |base_obj| {
                        if (base_obj.asName()) |name| decoder.base_encoding = namedEncodingFromName(name);
                    }
                    if (resolved_encoding.get("Differences")) |diff_obj| {
                        try applyEncodingDifferences(&decoder, diff_obj);
                    }
                },
                else => {},
            }
        }

        if (font_obj.get("ToUnicode")) |to_unicode_obj| {
            var resolved = try self.resolveValue(to_unicode_obj);
            defer resolved.deinit(self.alloc);
            if (resolved == .stream) {
                decoder.to_unicode = try self.parseToUnicodeMapAlloc(&resolved, &decoder.code_bytes, &decoder.codespace_ranges);
            }
        }

        return decoder;
    }

    fn buildType3Font(self: *const Reader, font_obj: *const syntax.Object) !?Type3Font {
        const subtype = font_obj.get("Subtype") orelse return null;
        if (!std.mem.eql(u8, subtype.asName() orelse return null, "Type3")) return null;

        var font = Type3Font{};
        errdefer font.deinit(self.alloc);
        if (font_obj.get("PaintType")) |obj| {
            font.paint_type = obj.asInteger() orelse font.paint_type;
        }
        if (font_obj.get("FontMatrix")) |obj| {
            if (obj.* == .array and obj.array.len >= 6) {
                var i: usize = 0;
                while (i < 6) : (i += 1) {
                    font.font_matrix[i] = numericObjectValue(&obj.array[i]) orelse font.font_matrix[i];
                }
            }
        }

        const charprocs_obj = font_obj.get("CharProcs") orelse return font;
        var resolved_charprocs = try self.resolveValue(charprocs_obj);
        defer resolved_charprocs.deinit(self.alloc);
        if (resolved_charprocs != .dict) return font;

        var code_to_name = [_]?[]const u8{null} ** 256;
        if (font_obj.get("Encoding")) |encoding_obj| {
            var resolved_encoding = try self.resolveValue(encoding_obj);
            defer resolved_encoding.deinit(self.alloc);
            if (resolved_encoding == .dict) {
                if (resolved_encoding.get("Differences")) |diff_obj| {
                    try applyEncodingDifferenceNames(&code_to_name, diff_obj);
                }
            }
        }
        for (resolved_charprocs.dict) |entry| {
            if (entry.key.len == 1) {
                const code = entry.key[0];
                if (code_to_name[code] == null) code_to_name[code] = entry.key;
            }
        }

        const first_char: usize = if (font_obj.get("FirstChar")) |obj|
            @intCast(@max(@as(i64, 0), obj.asInteger() orelse 0))
        else
            0;
        const widths = if (font_obj.get("Widths")) |obj|
            if (obj.* == .array) obj.array else &[_]syntax.Object{}
        else
            &[_]syntax.Object{};

        var glyphs = std.ArrayList(Type3Glyph).empty;
        defer glyphs.deinit(self.alloc);
        for (code_to_name, 0..) |maybe_name, code| {
            const glyph_name = maybe_name orelse continue;
            const proc_obj = resolved_charprocs.get(glyph_name) orelse continue;
            var resolved_proc = try self.resolveValue(proc_obj);
            defer resolved_proc.deinit(self.alloc);
            if (resolved_proc != .stream) continue;
            const content = try self.readDecodedStreamData(&resolved_proc);
            errdefer self.alloc.free(content);

            var advance_x: f64 = 1000.0 * font.font_matrix[0];
            var advance_y: f64 = 1000.0 * font.font_matrix[1];
            if (code >= first_char and code - first_char < widths.len) {
                const width_value = numericObjectValue(&widths[code - first_char]) orelse 1000.0;
                advance_x = width_value * font.font_matrix[0];
                advance_y = width_value * font.font_matrix[1];
            }
            if (parseType3GlyphAdvance(content)) |advance| {
                advance_x = advance[0] * font.font_matrix[0] + advance[1] * font.font_matrix[2];
                advance_y = advance[0] * font.font_matrix[1] + advance[1] * font.font_matrix[3];
            }

            try glyphs.append(self.alloc, .{
                .code = @intCast(code),
                .name = try self.alloc.dupe(u8, glyph_name),
                .content = content,
                .advance_x = advance_x,
                .advance_y = advance_y,
            });
        }

        font.glyphs = try glyphs.toOwnedSlice(self.alloc);
        return font;
    }

    fn buildTrueTypeFont(self: *const Reader, font_obj: *const syntax.Object) !?TrueTypeFont {
        const bytes = try self.readEmbeddedSfntAlloc(font_obj) orelse return null;
        errdefer self.alloc.free(bytes);
        var font = try font_lib.sfnt.Font.init(self.alloc, bytes);
        errdefer font.deinit(self.alloc);
        _ = font.tableData(.{ 'g', 'l', 'y', 'f' }) catch return null;
        const head = try font.head();

        return .{
            .bytes = bytes,
            .font = font,
            .units_per_em = head.units_per_em,
        };
    }

    fn buildCffOpenTypeFont(self: *const Reader, font_obj: *const syntax.Object) !?CffOpenTypeFont {
        const bytes = try self.readEmbeddedSfntAlloc(font_obj) orelse return null;
        errdefer self.alloc.free(bytes);
        var sfnt = try font_lib.sfnt.Font.init(self.alloc, bytes);
        errdefer sfnt.deinit(self.alloc);
        const cff_bytes = sfnt.tableData(.{ 'C', 'F', 'F', ' ' }) catch return null;
        var cff = try font_lib.cff.Font.init(self.alloc, cff_bytes);
        errdefer cff.deinit(self.alloc);
        const head = try sfnt.head();

        return .{
            .bytes = bytes,
            .sfnt = sfnt,
            .cff = cff,
            .units_per_em = head.units_per_em,
        };
    }

    fn buildType1Font(self: *const Reader, font_obj: *const syntax.Object) !?Type1Font {
        const raw_bytes = try self.readEmbeddedType1Alloc(font_obj) orelse return null;
        defer self.alloc.free(raw_bytes);
        const bytes = try normalizeType1ProgramAlloc(self.alloc, raw_bytes);
        errdefer self.alloc.free(bytes);

        const len_iv = try parseType1LenIV(bytes);
        const local_subrs = try parseType1LocalSubrsAlloc(self.alloc, bytes, len_iv);
        errdefer {
            for (local_subrs) |subr| self.alloc.free(subr);
            if (local_subrs.len > 0) self.alloc.free(local_subrs);
        }

        var code_to_name = [_]?[]const u8{null} ** 256;
        if (font_obj.get("Encoding")) |encoding_obj| {
            var resolved_encoding = try self.resolveValue(encoding_obj);
            defer resolved_encoding.deinit(self.alloc);
            if (resolved_encoding == .dict) {
                if (resolved_encoding.get("Differences")) |diff_obj| {
                    try applyEncodingDifferenceNames(&code_to_name, diff_obj);
                }
            }
        }

        const first_char: usize = if (font_obj.get("FirstChar")) |obj|
            @intCast(@max(@as(i64, 0), obj.asInteger() orelse 0))
        else
            0;
        const widths = if (font_obj.get("Widths")) |obj|
            if (obj.* == .array) obj.array else &[_]syntax.Object{}
        else
            &[_]syntax.Object{};

        const glyphs = try parseType1GlyphsAlloc(self.alloc, bytes, len_iv, font_obj, &code_to_name, first_char, widths);
        errdefer {
            for (glyphs) |*glyph| glyph.deinit(self.alloc);
            if (glyphs.len > 0) self.alloc.free(glyphs);
        }
        return .{
            .bytes = bytes,
            .local_subrs = local_subrs,
            .glyphs = glyphs,
        };
    }

    fn readEmbeddedSfntAlloc(self: *const Reader, font_obj: *const syntax.Object) !?[]u8 {
        if (try self.readEmbeddedSfntFromDescriptorAlloc(font_obj)) |bytes| return bytes;

        const subtype = font_obj.get("Subtype");
        if (subtype != null and std.mem.eql(u8, subtype.?.asName() orelse "", "Type0")) {
            const descendants_obj = font_obj.get("DescendantFonts") orelse return null;
            if (descendants_obj.* != .array or descendants_obj.array.len == 0) return null;
            var resolved_descendant = try self.resolveValue(&descendants_obj.array[0]);
            defer resolved_descendant.deinit(self.alloc);
            return try self.readEmbeddedSfntFromDescriptorAlloc(&resolved_descendant);
        }

        return null;
    }

    fn readEmbeddedType1Alloc(self: *const Reader, font_obj: *const syntax.Object) !?[]u8 {
        return try self.readEmbeddedType1FromDescriptorAlloc(font_obj);
    }

    fn readEmbeddedSfntFromDescriptorAlloc(self: *const Reader, font_obj: *const syntax.Object) !?[]u8 {
        const descriptor_obj = font_obj.get("FontDescriptor") orelse return null;
        var resolved_descriptor = try self.resolveValue(descriptor_obj);
        defer resolved_descriptor.deinit(self.alloc);
        if (resolved_descriptor != .dict) return null;

        if (resolved_descriptor.get("FontFile2")) |font_file_obj| {
            var resolved_stream = try self.resolveValue(font_file_obj);
            defer resolved_stream.deinit(self.alloc);
            if (resolved_stream != .stream) return null;
            return try self.readDecodedStreamData(&resolved_stream);
        }

        if (resolved_descriptor.get("FontFile3")) |font_file_obj| {
            var resolved_stream = try self.resolveValue(font_file_obj);
            defer resolved_stream.deinit(self.alloc);
            if (resolved_stream != .stream) return null;
            const subtype = resolved_stream.get("Subtype") orelse return null;
            if (!std.mem.eql(u8, subtype.asName() orelse "", "OpenType")) return null;
            return try self.readDecodedStreamData(&resolved_stream);
        }

        return null;
    }

    fn readEmbeddedType1FromDescriptorAlloc(self: *const Reader, font_obj: *const syntax.Object) !?[]u8 {
        const descriptor_obj = font_obj.get("FontDescriptor") orelse return null;
        var resolved_descriptor = try self.resolveValue(descriptor_obj);
        defer resolved_descriptor.deinit(self.alloc);
        if (resolved_descriptor != .dict) return null;

        if (resolved_descriptor.get("FontFile")) |font_file_obj| {
            var resolved_stream = try self.resolveValue(font_file_obj);
            defer resolved_stream.deinit(self.alloc);
            if (resolved_stream != .stream) return null;
            return try self.readDecodedStreamData(&resolved_stream);
        }

        return null;
    }

    fn parseToUnicodeMapAlloc(
        self: *const Reader,
        stream_obj: *const syntax.Object,
        code_bytes_out: *usize,
        codespace_ranges_out: *[]CodeSpaceRange,
    ) ![]ToUnicodeEntry {
        const decoded = try self.readDecodedStreamData(stream_obj);
        defer self.alloc.free(decoded);

        var entries = std.ArrayList(ToUnicodeEntry).empty;
        var codespace_ranges = std.ArrayList(CodeSpaceRange).empty;
        defer {
            for (entries.items) |entry| self.alloc.free(entry.dst);
            entries.deinit(self.alloc);
            codespace_ranges.deinit(self.alloc);
        }

        const Section = enum { none, codespace, bfchar, bfrange };
        var section: Section = .none;
        var remaining: usize = 0;
        var scanner = syntax.Scanner.init(self.alloc, decoded);
        defer scanner.deinit();

        while (true) {
            var lex = try scanner.readLexeme();
            defer syntax.Scanner.freeLexeme(self.alloc, &lex);
            if (lex == .eof) break;

            if (lex == .integer and section == .none) {
                remaining = @intCast(@max(@as(i64, 0), lex.integer));
                continue;
            }

            if (lex == .keyword) {
                if (std.mem.eql(u8, lex.keyword, "begincodespacerange")) {
                    section = .codespace;
                    continue;
                }
                if (std.mem.eql(u8, lex.keyword, "beginbfchar")) {
                    section = .bfchar;
                    continue;
                }
                if (std.mem.eql(u8, lex.keyword, "beginbfrange")) {
                    section = .bfrange;
                    continue;
                }
                if (std.mem.eql(u8, lex.keyword, "endcodespacerange") or std.mem.eql(u8, lex.keyword, "endbfchar") or std.mem.eql(u8, lex.keyword, "endbfrange")) {
                    section = .none;
                    remaining = 0;
                    continue;
                }
            }

            if (section == .none) continue;
            try scanner.unreadLexeme(try cloneLexemeForContent(self.alloc, lex));

            switch (section) {
                .codespace => {
                    var lo = try scanner.readObject();
                    defer lo.deinit(self.alloc);
                    var hi = try scanner.readObject();
                    defer hi.deinit(self.alloc);
                    if (lo != .string or hi != .string) return error.InvalidToUnicodeMap;
                    const code_len: u8 = @intCast(lo.string.len);
                    if (code_len > code_bytes_out.*) code_bytes_out.* = code_len;
                    try codespace_ranges.append(self.alloc, .{
                        .lo = try parseCodeBytesToU32(lo.string),
                        .hi = try parseCodeBytesToU32(hi.string),
                        .len = code_len,
                    });
                },
                .bfchar => {
                    var src = try scanner.readObject();
                    defer src.deinit(self.alloc);
                    var dst = try scanner.readObject();
                    defer dst.deinit(self.alloc);
                    if (src != .string or dst != .string) return error.InvalidToUnicodeMap;
                    const code_len: u8 = @intCast(src.string.len);
                    if (code_len > code_bytes_out.*) code_bytes_out.* = code_len;
                    try entries.append(self.alloc, .{
                        .src = try parseCodeBytesToU32(src.string),
                        .src_len = code_len,
                        .dst = try decodeToUnicodeDestAlloc(self.alloc, dst.string),
                    });
                },
                .bfrange => {
                    var src_lo = try scanner.readObject();
                    defer src_lo.deinit(self.alloc);
                    var src_hi = try scanner.readObject();
                    defer src_hi.deinit(self.alloc);
                    var dst = try scanner.readObject();
                    defer dst.deinit(self.alloc);
                    if (src_lo != .string or src_hi != .string) return error.InvalidToUnicodeMap;

                    const lo = try parseCodeBytesToU32(src_lo.string);
                    const hi = try parseCodeBytesToU32(src_hi.string);
                    const code_len: u8 = @intCast(src_lo.string.len);
                    if (code_len > code_bytes_out.*) code_bytes_out.* = code_len;

                    switch (dst) {
                        .string => {
                            const dst_start = try parseCodeBytesToU32(dst.string);
                            var current_src: u32 = lo;
                            while (current_src <= hi) : (current_src += 1) {
                                const dst_cp: u21 = @intCast(dst_start + (current_src - lo));
                                var buf: [4]u8 = undefined;
                                const n = try std.unicode.utf8Encode(dst_cp, &buf);
                                try entries.append(self.alloc, .{
                                    .src = current_src,
                                    .src_len = code_len,
                                    .dst = try self.alloc.dupe(u8, buf[0..n]),
                                });
                            }
                        },
                        .array => {
                            var current_src: u32 = lo;
                            for (dst.array) |item| {
                                if (current_src > hi) break;
                                if (item != .string) return error.InvalidToUnicodeMap;
                                try entries.append(self.alloc, .{
                                    .src = current_src,
                                    .src_len = code_len,
                                    .dst = try decodeToUnicodeDestAlloc(self.alloc, item.string),
                                });
                                current_src += 1;
                            }
                        },
                        else => return error.InvalidToUnicodeMap,
                    }
                },
                .none => unreachable,
            }

            if (remaining > 0) remaining -= 1;
        }

        codespace_ranges_out.* = try codespace_ranges.toOwnedSlice(self.alloc);
        return try entries.toOwnedSlice(self.alloc);
    }
};

fn trimPdfTrailer(bytes: []const u8) []const u8 {
    return std.mem.trimEnd(u8, bytes, "\r\n\t ");
}

fn findLastLine(bytes: []const u8, needle: []const u8) ?usize {
    var i = bytes.len;
    while (i > 0) {
        i -= 1;
        if ((i == 0 or bytes[i - 1] == '\n' or bytes[i - 1] == '\r') and std.mem.startsWith(u8, bytes[i..], needle)) {
            return i;
        }
    }
    return null;
}

fn parseStartXref(bytes: []const u8) !usize {
    if (!std.mem.startsWith(u8, bytes, "startxref")) return error.MissingStartXref;

    var i: usize = "startxref".len;
    while (i < bytes.len and isPdfWhitespace(bytes[i])) : (i += 1) {}
    if (i == bytes.len) return error.InvalidStartXref;

    const begin = i;
    while (i < bytes.len and std.ascii.isDigit(bytes[i])) : (i += 1) {}
    if (begin == i) return error.InvalidStartXref;

    return try std.fmt.parseInt(usize, bytes[begin..i], 10);
}

fn isPdfWhitespace(ch: u8) bool {
    return ch == ' ' or ch == '\t' or ch == '\r' or ch == '\n' or ch == '\x0c' or ch == '\x00';
}

fn parseXrefTable(
    alloc: Allocator,
    bytes: []const u8,
    offset: usize,
    entries: *std.ArrayList(XrefEntry),
    trailer_out: *?syntax.Object,
) anyerror!void {
    var cursor = offset;
    skipPdfWs(bytes, &cursor);
    if (!std.mem.startsWith(u8, bytes[cursor..], "xref")) {
        return try parseXrefStream(alloc, bytes, cursor, entries, trailer_out);
    }
    cursor += "xref".len;

    while (true) {
        skipPdfWs(bytes, &cursor);
        if (cursor >= bytes.len) return error.UnexpectedEof;
        if (std.mem.startsWith(u8, bytes[cursor..], "trailer")) {
            cursor += "trailer".len;
            break;
        }

        const header_line = try readLine(bytes, &cursor);
        const start, const count = try parseSectionHeader(header_line);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const entry_line = try readLine(bytes, &cursor);
            const parsed = try parseXrefEntry(start + i, entry_line);
            if (parsed) |entry| try putXref(alloc, entries, entry);
        }
    }

    skipPdfWs(bytes, &cursor);
    var scanner = syntax.Scanner.init(alloc, bytes[cursor..]);
    defer scanner.deinit();
    var trailer = try scanner.readObject();
    errdefer trailer.deinit(alloc);
    if (trailer != .dict) return error.ExpectedTrailerDict;

    if (trailer_out.* == null) {
        trailer_out.* = try trailer.clone(alloc);
    }
    defer trailer.deinit(alloc);

    if (trailer.get("Prev")) |prev_value| {
        if (prev_value.asInteger()) |prev| {
            if (prev >= 0) try parseXrefTable(alloc, bytes, @intCast(prev), entries, trailer_out);
        }
    }
}

fn parseXrefStream(
    alloc: Allocator,
    bytes: []const u8,
    offset: usize,
    entries: *std.ArrayList(XrefEntry),
    trailer_out: *?syntax.Object,
) anyerror!void {
    if (offset >= bytes.len) return error.InvalidStartXref;

    var scanner = syntax.Scanner.initWithOffset(alloc, bytes[offset..], offset);
    defer scanner.deinit();

    var parsed = try scanner.readObject();
    defer parsed.deinit(alloc);
    if (parsed != .obj_def) return error.UnsupportedXrefFormat;
    if (parsed.obj_def.value.* != .stream) return error.UnsupportedXrefFormat;

    const xref_stream = parsed.obj_def.value;
    const ty = xref_stream.get("Type") orelse return error.UnsupportedXrefFormat;
    if (!std.mem.eql(u8, ty.asName() orelse return error.UnsupportedXrefFormat, "XRef")) {
        return error.UnsupportedXrefFormat;
    }

    var trailer = try cloneStreamHeaderAsDict(alloc, xref_stream);
    defer trailer.deinit(alloc);
    if (trailer_out.* == null) {
        trailer_out.* = try trailer.clone(alloc);
    }

    const decoded = try decodeStreamDataAlloc(alloc, bytes, xref_stream);
    defer alloc.free(decoded);

    const widths = try parseXrefWidths(xref_stream.get("W") orelse return error.MalformedXrefStream);
    try parseXrefStreamEntries(alloc, decoded, widths, xref_stream, entries);

    if (trailer.get("Prev")) |prev_value| {
        if (prev_value.asInteger()) |prev| {
            if (prev >= 0) try parseXrefTable(alloc, bytes, @intCast(prev), entries, trailer_out);
        }
    }
}

fn putXref(alloc: Allocator, entries: *std.ArrayList(XrefEntry), entry: XrefEntry) !void {
    for (entries.items) |existing| {
        if (existing.ptr.id == entry.ptr.id) return;
    }
    try entries.append(alloc, entry);
}

fn skipPdfWs(bytes: []const u8, cursor: *usize) void {
    while (cursor.* < bytes.len and isPdfWhitespace(bytes[cursor.*])) : (cursor.* += 1) {}
}

fn readLine(bytes: []const u8, cursor: *usize) ![]const u8 {
    if (cursor.* >= bytes.len) return error.UnexpectedEof;
    const start = cursor.*;
    while (cursor.* < bytes.len and bytes[cursor.*] != '\n' and bytes[cursor.*] != '\r') : (cursor.* += 1) {}
    const line = bytes[start..cursor.*];
    if (cursor.* < bytes.len and bytes[cursor.*] == '\r') {
        cursor.* += 1;
        if (cursor.* < bytes.len and bytes[cursor.*] == '\n') cursor.* += 1;
    } else if (cursor.* < bytes.len and bytes[cursor.*] == '\n') {
        cursor.* += 1;
    }
    return std.mem.trim(u8, line, " \t");
}

fn parseSectionHeader(line: []const u8) !struct { usize, usize } {
    var it = std.mem.tokenizeAny(u8, line, " \t");
    const start_txt = it.next() orelse return error.MalformedXrefTable;
    const count_txt = it.next() orelse return error.MalformedXrefTable;
    return .{
        try std.fmt.parseInt(usize, start_txt, 10),
        try std.fmt.parseInt(usize, count_txt, 10),
    };
}

fn parseXrefEntry(index: usize, line: []const u8) !?XrefEntry {
    var it = std.mem.tokenizeAny(u8, line, " \t");
    const off_txt = it.next() orelse return error.MalformedXrefTable;
    const gen_txt = it.next() orelse return error.MalformedXrefTable;
    const state_txt = it.next() orelse return error.MalformedXrefTable;

    if (state_txt.len != 1) return error.MalformedXrefTable;
    const in_use = switch (state_txt[0]) {
        'n' => true,
        'f' => false,
        else => return error.MalformedXrefTable,
    };

    return .{
        .ptr = .{
            .id = @intCast(index),
            .gen = try std.fmt.parseInt(u16, gen_txt, 10),
        },
        .offset = try std.fmt.parseInt(usize, off_txt, 10),
        .in_use = in_use,
        .compressed_obj_stream_id = null,
        .compressed_index = null,
    };
}

fn cloneStreamHeaderAsDict(alloc: Allocator, obj: *const syntax.Object) !syntax.Object {
    if (obj.* != .stream) return error.NotAStream;
    const header = obj.stream.header;
    const out = try alloc.alloc(syntax.DictEntry, header.len);
    errdefer {
        for (out[0..header.len]) |*entry| {
            alloc.free(entry.key);
            entry.value.deinit(alloc);
        }
        alloc.free(out);
    }
    for (header, 0..) |entry, i| {
        out[i] = .{
            .key = try alloc.dupe(u8, entry.key),
            .value = try entry.value.clone(alloc),
        };
    }
    return .{ .dict = out };
}

fn decodeStreamDataAlloc(alloc: Allocator, bytes: []const u8, obj: *const syntax.Object) ![]u8 {
    if (obj.* != .stream) return error.NotAStream;
    const stream_value = obj.stream;
    const end = stream_value.data_offset + stream_value.data_length;
    if (end > bytes.len) return error.InvalidObjectOffset;
    const raw = bytes[stream_value.data_offset..end];
    return try decodeStreamFiltersAlloc(alloc, raw, obj.get("Filter"), obj.get("DecodeParms"));
}

fn streamHasFilter(filter_obj: ?*const syntax.Object, name: []const u8) bool {
    const obj = filter_obj orelse return false;
    return switch (obj.*) {
        .name => |filter_name| std.mem.eql(u8, filter_name, name),
        .array => |items| blk: {
            for (items) |item| {
                if (item == .name and std.mem.eql(u8, item.name, name)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn streamFilterParamFor(
    filter_obj: ?*const syntax.Object,
    decode_parms_obj: ?*const syntax.Object,
    name: []const u8,
) ?*const syntax.Object {
    const filter = filter_obj orelse return null;
    const parms = decode_parms_obj orelse return null;
    return switch (filter.*) {
        .name => |filter_name| if (std.mem.eql(u8, filter_name, name)) parms else null,
        .array => |items| blk: {
            for (items, 0..) |item, i| {
                if (item != .name or !std.mem.eql(u8, item.name, name)) continue;
                break :blk switch (parms.*) {
                    .array => if (i < parms.array.len) &parms.array[i] else null,
                    .null => null,
                    else => parms,
                };
            }
            break :blk null;
        },
        else => null,
    };
}

fn decodeCcittGrayAlloc(
    alloc: Allocator,
    raw: []const u8,
    width: u32,
    height: u32,
    param: ?*const syntax.Object,
) ![]u8 {
    var k: i64 = 0;
    var columns: usize = width;
    var rows: usize = height;
    var byte_align = false;
    var black_is_1 = false;

    if (param) |obj| {
        if (obj.* == .dict) {
            if (obj.get("K")) |value| k = value.asInteger() orelse 0;
            if (obj.get("Columns")) |value| {
                const n = value.asInteger() orelse return error.UnsupportedPdfRendering;
                if (n <= 0) return error.UnsupportedPdfRendering;
                columns = @intCast(n);
            }
            if (obj.get("Rows")) |value| {
                const n = value.asInteger() orelse return error.UnsupportedPdfRendering;
                if (n <= 0) return error.UnsupportedPdfRendering;
                rows = @intCast(n);
            }
            if (obj.get("EncodedByteAlign")) |value| {
                byte_align = switch (value.*) {
                    .boolean => |b| b,
                    .integer => |n| n != 0,
                    else => false,
                };
            }
            if (obj.get("BlackIs1")) |value| {
                black_is_1 = switch (value.*) {
                    .boolean => |b| b,
                    .integer => |n| n != 0,
                    else => false,
                };
            }
        }
    }
    if (columns != width or rows != height) return error.UnsupportedPdfRendering;
    const gray = try image_lib.ccitt.decodeGrayAlloc(
        alloc,
        raw,
        .msb,
        if (k < 0) .group4 else .group3,
        columns,
        rows,
        .{
            .byte_align = byte_align,
            .mixed_2d = k > 0,
        },
    );
    if (black_is_1) {
        for (gray) |*sample| sample.* = 0xff - sample.*;
    }
    return gray;
}

fn decodeGrayMaskToRgba(
    rgba: []u8,
    gray: []const u8,
    decode_obj: ?*const syntax.Object,
) !void {
    const decode_zero_is_paint = parseMaskDecodeInvert(decode_obj);
    if (rgba.len != gray.len * 4) return error.UnsupportedPdfRendering;
    for (gray, 0..) |sample, i| {
        const paint = if (decode_zero_is_paint) sample < 128 else sample >= 128;
        const dst = i * 4;
        rgba[dst + 0] = 0;
        rgba[dst + 1] = 0;
        rgba[dst + 2] = 0;
        rgba[dst + 3] = if (paint) 0xff else 0x00;
    }
}

fn parseXrefWidths(obj: *const syntax.Object) ![3]usize {
    if (obj.* != .array or obj.array.len != 3) return error.MalformedXrefStream;
    var widths: [3]usize = undefined;
    for (obj.array, 0..) |item, i| {
        const value = item.asInteger() orelse return error.MalformedXrefStream;
        if (value < 0) return error.MalformedXrefStream;
        widths[i] = @intCast(value);
    }
    return widths;
}

fn parseXrefStreamEntries(
    alloc: Allocator,
    decoded: []const u8,
    widths: [3]usize,
    stream_obj: *const syntax.Object,
    entries: *std.ArrayList(XrefEntry),
) !void {
    const total_width = widths[0] + widths[1] + widths[2];
    if (total_width == 0 or decoded.len % total_width != 0) return error.MalformedXrefStream;

    var spans = std.ArrayList(struct { start: usize, count: usize }).empty;
    defer spans.deinit(alloc);

    if (stream_obj.get("Index")) |index_obj| {
        if (index_obj.* != .array or index_obj.array.len % 2 != 0) return error.MalformedXrefStream;
        var i: usize = 0;
        while (i < index_obj.array.len) : (i += 2) {
            const start = index_obj.array[i].asInteger() orelse return error.MalformedXrefStream;
            const count = index_obj.array[i + 1].asInteger() orelse return error.MalformedXrefStream;
            if (start < 0 or count < 0) return error.MalformedXrefStream;
            try spans.append(alloc, .{ .start = @intCast(start), .count = @intCast(count) });
        }
    } else {
        const size_obj = stream_obj.get("Size") orelse return error.MalformedXrefStream;
        const size = size_obj.asInteger() orelse return error.MalformedXrefStream;
        if (size < 0) return error.MalformedXrefStream;
        try spans.append(alloc, .{ .start = 0, .count = @intCast(size) });
    }

    var expected_records: usize = 0;
    for (spans.items) |span| expected_records += span.count;
    if (expected_records * total_width != decoded.len) return error.MalformedXrefStream;

    var cursor: usize = 0;
    for (spans.items) |span| {
        var i: usize = 0;
        while (i < span.count) : (i += 1) {
            const obj_id = span.start + i;
            const entry_type = if (widths[0] == 0) @as(u64, 1) else readBigEndianInt(decoded[cursor .. cursor + widths[0]]);
            cursor += widths[0];
            const field2 = readBigEndianInt(decoded[cursor .. cursor + widths[1]]);
            cursor += widths[1];
            const field3 = readBigEndianInt(decoded[cursor .. cursor + widths[2]]);
            cursor += widths[2];

            switch (entry_type) {
                0 => try putXref(alloc, entries, .{
                    .ptr = .{ .id = @intCast(obj_id), .gen = @intCast(field3) },
                    .offset = @intCast(field2),
                    .in_use = false,
                    .compressed_obj_stream_id = null,
                    .compressed_index = null,
                }),
                1 => try putXref(alloc, entries, .{
                    .ptr = .{ .id = @intCast(obj_id), .gen = @intCast(field3) },
                    .offset = @intCast(field2),
                    .in_use = true,
                    .compressed_obj_stream_id = null,
                    .compressed_index = null,
                }),
                2 => try putXref(alloc, entries, .{
                    .ptr = .{ .id = @intCast(obj_id), .gen = 0 },
                    .offset = 0,
                    .in_use = true,
                    .compressed_obj_stream_id = @intCast(field2),
                    .compressed_index = @intCast(field3),
                }),
                else => return error.MalformedXrefStream,
            }
        }
    }
}

fn readBigEndianInt(bytes: []const u8) u64 {
    var value: u64 = 0;
    for (bytes) |b| value = (value << 8) | b;
    return value;
}

fn decodeStreamFiltersAlloc(
    alloc: Allocator,
    raw: []const u8,
    filter_obj: ?*const syntax.Object,
    decode_parms_obj: ?*const syntax.Object,
) ![]u8 {
    if (filter_obj == null) return try alloc.dupe(u8, raw);

    var current = try alloc.dupe(u8, raw);
    errdefer alloc.free(current);

    switch (filter_obj.?.*) {
        .name => |name| {
            const next = try applyStreamFilterAlloc(alloc, current, name, decode_parms_obj);
            alloc.free(current);
            return next;
        },
        .array => |items| {
            for (items, 0..) |item, i| {
                const name = item.asName() orelse return error.UnsupportedStreamFilter;
                const param = if (decode_parms_obj) |parms|
                    switch (parms.*) {
                        .array => if (i < parms.array.len) &parms.array[i] else null,
                        .null => null,
                        else => parms,
                    }
                else
                    null;
                const next = try applyStreamFilterAlloc(alloc, current, name, param);
                alloc.free(current);
                current = next;
            }
            return current;
        },
        else => return error.UnsupportedStreamFilter,
    }
}

fn applyStreamFilterAlloc(
    alloc: Allocator,
    input: []const u8,
    name: []const u8,
    param: ?*const syntax.Object,
) ![]u8 {
    if (std.mem.eql(u8, name, "FlateDecode")) {
        var in: std.Io.Reader = .fixed(input);
        var aw: std.Io.Writer.Allocating = .init(alloc);
        defer aw.deinit();
        var decompress: std.compress.flate.Decompress = .init(&in, .zlib, &.{});
        _ = try decompress.reader.streamRemaining(&aw.writer);
        const inflated = try aw.toOwnedSlice();
        defer alloc.free(inflated);
        return try applyPredictorAlloc(alloc, inflated, param);
    }
    if (std.mem.eql(u8, name, "ASCIIHexDecode")) {
        return try asciiHexDecodeAlloc(alloc, input);
    }
    if (std.mem.eql(u8, name, "ASCII85Decode")) {
        return try ascii85DecodeAlloc(alloc, input);
    }
    if (std.mem.eql(u8, name, "LZWDecode")) {
        return try lzwDecodeAlloc(alloc, input, param);
    }
    if (std.mem.eql(u8, name, "RunLengthDecode")) {
        return try runLengthDecodeAlloc(alloc, input);
    }
    return error.UnsupportedStreamFilter;
}

fn asciiHexDecodeAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    var nibbles = std.ArrayList(u8).empty;
    defer nibbles.deinit(alloc);

    for (input) |ch| {
        if (isPdfWhitespace(ch)) continue;
        if (ch == '>') break;
        const nibble = switch (ch) {
            '0'...'9' => ch - '0',
            'a'...'f' => ch - 'a' + 10,
            'A'...'F' => ch - 'A' + 10,
            else => return error.MalformedAsciiHex,
        };
        try nibbles.append(alloc, nibble);
    }

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);

    var i: usize = 0;
    while (i < nibbles.items.len) : (i += 2) {
        const hi = nibbles.items[i];
        const lo: u8 = if (i + 1 < nibbles.items.len) nibbles.items[i + 1] else 0;
        try out.append(alloc, (hi << 4) | lo);
    }
    return try out.toOwnedSlice(alloc);
}

fn ascii85DecodeAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    var digits = std.ArrayList(u8).empty;
    defer digits.deinit(alloc);
    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);

    var i: usize = 0;
    while (i < input.len) : (i += 1) {
        const ch = input[i];
        if (isPdfWhitespace(ch)) continue;
        if (ch == '~') break;
        if (ch == 'z') {
            if (digits.items.len != 0) return error.MalformedAscii85;
            try out.appendSlice(alloc, &.{ 0, 0, 0, 0 });
            continue;
        }
        if (ch < '!' or ch > 'u') return error.MalformedAscii85;
        try digits.append(alloc, ch - '!');
        if (digits.items.len == 5) {
            try appendAscii85Group(alloc, &out, digits.items, 4);
            digits.clearRetainingCapacity();
        }
    }

    if (digits.items.len > 0) {
        const original_len = digits.items.len;
        while (digits.items.len < 5) try digits.append(alloc, 'u' - '!');
        try appendAscii85Group(alloc, &out, digits.items, original_len - 1);
    }

    return try out.toOwnedSlice(alloc);
}

fn lzwDecodeAlloc(alloc: Allocator, input: []const u8, param: ?*const syntax.Object) ![]u8 {
    const early_change: u16 = blk: {
        if (param) |obj| {
            if (obj.* == .dict) {
                if (obj.get("EarlyChange")) |value| {
                    break :blk @intCast(@max(@as(i64, 0), value.asInteger() orelse 1));
                }
            }
        }
        break :blk 1;
    };

    var bit_pos: usize = 0;
    var code_size: u8 = 9;
    var prev_code: ?u16 = null;
    var dict = std.ArrayList([]u8).empty;
    defer {
        for (dict.items) |entry| alloc.free(entry);
        dict.deinit(alloc);
    }
    try dict.ensureTotalCapacity(alloc, 4096);
    for (0..258) |_| try dict.append(alloc, &.{});

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);

    while (true) {
        const code = readLzwCode(input, &bit_pos, code_size) orelse break;
        switch (code) {
            256 => {
                for (dict.items[258..]) |entry| alloc.free(entry);
                dict.shrinkRetainingCapacity(258);
                code_size = 9;
                prev_code = null;
                continue;
            },
            257 => break,
            else => {},
        }

        const entry = if (code < 256)
            try alloc.dupe(u8, &.{@intCast(code)})
        else if (code < dict.items.len and dict.items[code].len > 0)
            try alloc.dupe(u8, dict.items[code])
        else if (prev_code) |prev| blk: {
            const prev_entry = try dictionaryEntryAlloc(alloc, &dict, prev);
            defer alloc.free(prev_entry);
            if (prev_entry.len == 0) return error.MalformedLzw;
            var seq = try alloc.alloc(u8, prev_entry.len + 1);
            @memcpy(seq[0..prev_entry.len], prev_entry);
            seq[prev_entry.len] = prev_entry[0];
            break :blk seq;
        } else return error.MalformedLzw;
        defer alloc.free(entry);

        try out.appendSlice(alloc, entry);

        if (prev_code) |prev| {
            const prev_entry = try dictionaryEntryAlloc(alloc, &dict, prev);
            defer alloc.free(prev_entry);
            if (prev_entry.len > 0 and dict.items.len < 4096) {
                var seq = try alloc.alloc(u8, prev_entry.len + 1);
                @memcpy(seq[0..prev_entry.len], prev_entry);
                seq[prev_entry.len] = entry[0];
                try dict.append(alloc, seq);
                if (code_size < 12 and dict.items.len >= ((@as(usize, 1) << @as(u6, @intCast(code_size))) - early_change)) {
                    code_size += 1;
                }
            }
        }

        prev_code = code;
    }

    const decoded = try out.toOwnedSlice(alloc);
    defer alloc.free(decoded);
    return try applyPredictorAlloc(alloc, decoded, param);
}

fn runLengthDecodeAlloc(alloc: Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);

    var i: usize = 0;
    while (i < input.len) {
        const length = input[i];
        i += 1;
        if (length == 128) break;
        if (length <= 127) {
            const literal_len = @as(usize, length) + 1;
            if (i + literal_len > input.len) return error.MalformedRunLength;
            try out.appendSlice(alloc, input[i .. i + literal_len]);
            i += literal_len;
            continue;
        }

        if (i >= input.len) return error.MalformedRunLength;
        const repeat = input[i];
        i += 1;
        const repeat_len = 257 - @as(usize, length);
        try out.appendNTimes(alloc, repeat, repeat_len);
    }

    return try out.toOwnedSlice(alloc);
}

fn dictionaryEntryAlloc(alloc: Allocator, dict: *const std.ArrayList([]u8), code: u16) ![]u8 {
    if (code < 256) return try alloc.dupe(u8, &.{@intCast(code)});
    if (code >= dict.items.len or dict.items[code].len == 0) return error.MalformedLzw;
    return try alloc.dupe(u8, dict.items[code]);
}

fn readLzwCode(input: []const u8, bit_pos: *usize, code_size: u8) ?u16 {
    const bits = @as(usize, code_size);
    if (bit_pos.* + bits > input.len * 8) return null;

    var value: u16 = 0;
    var i: usize = 0;
    while (i < bits) : (i += 1) {
        const abs_bit = bit_pos.* + i;
        const byte = input[abs_bit / 8];
        const shift = 7 - @as(u3, @intCast(abs_bit % 8));
        value = (value << 1) | ((byte >> shift) & 1);
    }
    bit_pos.* += bits;
    return value;
}

fn parseRawCode(raw: []const u8) u32 {
    var code: u32 = 0;
    for (raw) |b| code = (code << 8) | b;
    return code;
}

fn appendAscii85Group(alloc: Allocator, out: *std.ArrayList(u8), digits: []const u8, keep_bytes: usize) !void {
    var value: u64 = 0;
    for (digits) |digit| value = value * 85 + digit;
    var buf: [4]u8 = .{
        @intCast((value >> 24) & 0xff),
        @intCast((value >> 16) & 0xff),
        @intCast((value >> 8) & 0xff),
        @intCast(value & 0xff),
    };
    try out.appendSlice(alloc, buf[0..keep_bytes]);
}

fn applyPredictorAlloc(alloc: Allocator, decoded: []const u8, param: ?*const syntax.Object) ![]u8 {
    if (param == null or param.?.* != .dict) return try alloc.dupe(u8, decoded);
    const predictor_obj = param.?.get("Predictor") orelse return try alloc.dupe(u8, decoded);
    const predictor = predictor_obj.asInteger() orelse return try alloc.dupe(u8, decoded);
    if (predictor <= 1) return try alloc.dupe(u8, decoded);
    if (predictor < 10 or predictor > 15) return error.UnsupportedPredictor;

    const columns_i = if (param.?.get("Columns")) |obj| obj.asInteger() orelse 1 else 1;
    const colors_i = if (param.?.get("Colors")) |obj| obj.asInteger() orelse 1 else 1;
    const bits_i = if (param.?.get("BitsPerComponent")) |obj| obj.asInteger() orelse 8 else 8;
    if (columns_i <= 0 or colors_i <= 0 or bits_i <= 0) return error.UnsupportedPredictor;
    if (@mod(bits_i, 8) != 0) return error.UnsupportedPredictor;

    const bytes_per_pixel: usize = @intCast(@divTrunc(colors_i * bits_i + 7, 8));
    const row_len: usize = @intCast(@divTrunc(columns_i * colors_i * bits_i + 7, 8));
    if ((row_len + 1) == 0 or decoded.len % (row_len + 1) != 0) return error.MalformedPredictorData;

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    const prev = try alloc.alloc(u8, row_len);
    defer alloc.free(prev);
    @memset(prev, 0);

    var cursor: usize = 0;
    while (cursor < decoded.len) {
        const filter = decoded[cursor];
        cursor += 1;
        const row = decoded[cursor .. cursor + row_len];
        cursor += row_len;

        const current = try alloc.dupe(u8, row);
        defer alloc.free(current);
        try applyPngPredictorRow(current, prev, bytes_per_pixel, filter);
        try out.appendSlice(alloc, current);
        @memcpy(prev, current);
    }

    return try out.toOwnedSlice(alloc);
}

fn applyPngPredictorRow(current: []u8, prev: []const u8, bpp: usize, filter: u8) !void {
    switch (filter) {
        0 => {},
        1 => {
            for (current, 0..) |*byte, i| {
                const left: u8 = if (i >= bpp) current[i - bpp] else 0;
                byte.* +%= left;
            }
        },
        2 => {
            for (current, prev) |*byte, up| byte.* +%= up;
        },
        3 => {
            for (current, 0..) |*byte, i| {
                const left: u8 = if (i >= bpp) current[i - bpp] else 0;
                const up: u8 = prev[i];
                byte.* +%= @intCast((@as(u16, left) + up) / 2);
            }
        },
        4 => {
            for (current, 0..) |*byte, i| {
                const left: u8 = if (i >= bpp) current[i - bpp] else 0;
                const up: u8 = prev[i];
                const up_left: u8 = if (i >= bpp) prev[i - bpp] else 0;
                byte.* +%= paethPredictor(left, up, up_left);
            }
        },
        else => return error.MalformedPredictorData,
    }
}

fn paethPredictor(a: u8, b: u8, c: u8) u8 {
    const p = @as(i32, a) + b - c;
    const pa = @abs(p - a);
    const pb = @abs(p - b);
    const pc = @abs(p - c);
    if (pa <= pb and pa <= pc) return a;
    if (pb <= pc) return b;
    return c;
}

fn namedEncodingFromName(name: []const u8) text_encoding.NamedEncoding {
    if (std.mem.eql(u8, name, "WinAnsiEncoding")) return .win_ansi;
    if (std.mem.eql(u8, name, "MacRomanEncoding")) return .mac_roman;
    if (std.mem.eql(u8, name, "StandardEncoding")) return .standard;
    return .pdf_doc;
}

fn applyEncodingDifferences(decoder: *FontDecoder, obj: *const syntax.Object) !void {
    if (obj.* != .array) return;
    var current_code: ?usize = null;
    for (obj.array) |item| {
        switch (item) {
            .integer => |value| {
                if (value < 0 or value > 255) continue;
                current_code = @intCast(value);
            },
            .name => |name| {
                if (current_code) |code| {
                    decoder.differences[code] = text_encoding.glyphNameToRune(name);
                    current_code = code + 1;
                }
            },
            else => {},
        }
    }
}

fn applyEncodingDifferenceNames(names: *[256]?[]const u8, obj: *const syntax.Object) !void {
    if (obj.* != .array) return;
    var current_code: ?usize = null;
    for (obj.array) |item| {
        switch (item) {
            .integer => |value| {
                if (value < 0 or value > 255) continue;
                current_code = @intCast(value);
            },
            .name => |name| {
                if (current_code) |code| {
                    names[code] = name;
                    current_code = code + 1;
                }
            },
            else => {},
        }
    }
}

fn parseType1LenIV(bytes: []const u8) !i64 {
    var scanner = syntax.Scanner.init(std.heap.page_allocator, bytes);
    defer scanner.deinit();
    while (true) {
        var tok = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(std.heap.page_allocator, &tok);
        switch (tok) {
            .eof => return 4,
            .name => |name| {
                if (!std.mem.eql(u8, name, "lenIV")) continue;
                var value_tok = try scanner.readLexeme();
                defer syntax.Scanner.freeLexeme(std.heap.page_allocator, &value_tok);
                return switch (value_tok) {
                    .integer => |value| value,
                    else => 4,
                };
            },
            else => {},
        }
    }
}

fn normalizeType1ProgramAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const normalized = if (looksLikePfb(bytes))
        try decodePfbAlloc(alloc, bytes)
    else
        try alloc.dupe(u8, bytes);
    defer alloc.free(normalized);

    const eexec_marker = std.mem.indexOf(u8, normalized, "currentfile eexec") orelse return try alloc.dupe(u8, normalized);
    var payload_start = eexec_marker + "currentfile eexec".len;
    while (payload_start < normalized.len and isType1Whitespace(normalized[payload_start])) : (payload_start += 1) {}
    const decrypted = try decodeType1EexecAlloc(alloc, normalized[payload_start..]);
    defer alloc.free(decrypted);

    const trimmed = if (std.mem.indexOf(u8, decrypted, "cleartomark")) |end|
        decrypted[0..end]
    else
        decrypted;

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, normalized[0..payload_start]);
    try out.append(alloc, '\n');
    try out.appendSlice(alloc, trimmed);
    return try out.toOwnedSlice(alloc);
}

fn looksLikePfb(bytes: []const u8) bool {
    return bytes.len >= 6 and bytes[0] == 0x80 and (bytes[1] == 0x01 or bytes[1] == 0x02);
}

fn decodePfbAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    var i: usize = 0;
    while (i < bytes.len) {
        if (bytes[i] != 0x80) return error.InvalidType1;
        if (i + 2 > bytes.len) return error.TruncatedType1;
        const kind = bytes[i + 1];
        i += 2;
        if (kind == 0x03) break;
        if (kind != 0x01 and kind != 0x02) return error.InvalidType1;
        if (i + 4 > bytes.len) return error.TruncatedType1;
        const seg_len =
            @as(usize, bytes[i]) |
            (@as(usize, bytes[i + 1]) << 8) |
            (@as(usize, bytes[i + 2]) << 16) |
            (@as(usize, bytes[i + 3]) << 24);
        i += 4;
        if (i + seg_len > bytes.len) return error.TruncatedType1;
        try out.appendSlice(alloc, bytes[i .. i + seg_len]);
        i += seg_len;
    }
    return try out.toOwnedSlice(alloc);
}

fn decodeType1EexecAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const cipher = if (looksLikeAsciiHexEexec(bytes))
        try decodeAsciiHexLooseAlloc(alloc, bytes)
    else
        try alloc.dupe(u8, bytes);
    defer alloc.free(cipher);
    return try font_lib.type1.decryptEexecAlloc(alloc, cipher);
}

fn looksLikeAsciiHexEexec(bytes: []const u8) bool {
    var seen: usize = 0;
    var i: usize = 0;
    while (i < bytes.len and seen < 64) : (i += 1) {
        const b = bytes[i];
        if (isType1Whitespace(b)) continue;
        if (hexNibble(b) == null) return false;
        seen += 1;
    }
    return seen >= 8;
}

fn decodeAsciiHexLooseAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var nibbles = std.ArrayList(u8).empty;
    defer nibbles.deinit(alloc);
    for (bytes) |b| {
        if (isType1Whitespace(b)) continue;
        const nibble = hexNibble(b) orelse break;
        try nibbles.append(alloc, nibble);
    }
    const out_len = (nibbles.items.len + 1) / 2;
    var out = try alloc.alloc(u8, out_len);
    var h: usize = 0;
    var o: usize = 0;
    while (h < nibbles.items.len) : (o += 1) {
        const hi = nibbles.items[h];
        const lo = if (h + 1 < nibbles.items.len) nibbles.items[h + 1] else 0;
        out[o] = (hi << 4) | lo;
        h += 2;
    }
    return out;
}

fn decryptType1BytesAlloc(alloc: Allocator, len_iv: i64, bytes: []const u8) ![]u8 {
    if (len_iv < 0) return try alloc.dupe(u8, bytes);
    return try font_lib.type1.decryptCharStringAlloc(alloc, bytes, @intCast(len_iv));
}

fn ensureType1SubrCapacity(alloc: Allocator, subrs: *std.ArrayList([]u8), index: usize) !void {
    if (index < subrs.items.len) return;
    const old_len = subrs.items.len;
    try subrs.resize(alloc, index + 1);
    for (subrs.items[old_len..]) |*item| item.* = &.{};
}

fn namedEncodingTable(enc: text_encoding.NamedEncoding) *const [256]u21 {
    return switch (enc) {
        .pdf_doc => &text_encoding.pdf_doc_encoding,
        .win_ansi => &text_encoding.win_ansi_encoding,
        .mac_roman => &text_encoding.mac_roman_encoding,
        .standard => &text_encoding.standard_encoding,
    };
}

fn populateType1EncodingFallbacks(
    font_obj: *const syntax.Object,
    code_to_name: *[256]?[]const u8,
    glyph_names: []const []const u8,
) !void {
    var base_encoding = text_encoding.NamedEncoding.standard;
    if (font_obj.get("Encoding")) |encoding_obj| {
        switch (encoding_obj.*) {
            .name => |name| base_encoding = namedEncodingFromName(name),
            .dict => {
                if (encoding_obj.get("BaseEncoding")) |base_obj| {
                    if (base_obj.asName()) |name| base_encoding = namedEncodingFromName(name);
                }
            },
            else => {},
        }
    }
    const table = namedEncodingTable(base_encoding);
    for (glyph_names) |name| {
        const cp = text_encoding.glyphNameToRune(name) orelse continue;
        var code: usize = 0;
        while (code < 256) : (code += 1) {
            if (code_to_name[code] != null) continue;
            if (table[code] != cp) continue;
            code_to_name[code] = name;
            break;
        }
    }
}

fn parseType1LocalSubrsAlloc(alloc: Allocator, bytes: []const u8, len_iv: i64) ![][]u8 {
    const scanner_result = parseType1LocalSubrsLexAlloc(alloc, bytes, len_iv) catch null;
    if (scanner_result) |subrs| {
        if (subrs.len > 0) return subrs;
        alloc.free(subrs);
    }
    return try parseType1LocalSubrsRdAlloc(alloc, bytes, len_iv);
}

fn parseType1LocalSubrsLexAlloc(alloc: Allocator, bytes: []const u8, len_iv: i64) ![][]u8 {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();
    var in_subrs = false;
    var subrs = std.ArrayList([]u8).empty;
    errdefer {
        for (subrs.items) |subr| alloc.free(subr);
        subrs.deinit(alloc);
    }

    while (true) {
        var tok = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &tok);
        switch (tok) {
            .eof => break,
            .name => |name| in_subrs = std.mem.eql(u8, name, "Subrs"),
            .keyword => |kw| {
                if (!in_subrs) continue;
                if (std.mem.eql(u8, kw, "dup")) {
                    var idx_tok = try scanner.readLexeme();
                    defer syntax.Scanner.freeLexeme(alloc, &idx_tok);
                    if (idx_tok != .integer or idx_tok.integer < 0) continue;
                    var value_tok = try scanner.readLexeme();
                    defer syntax.Scanner.freeLexeme(alloc, &value_tok);
                    if (value_tok != .string) continue;
                    const idx: usize = @intCast(idx_tok.integer);
                    try ensureType1SubrCapacity(alloc, &subrs, idx);
                    if (subrs.items[idx].len > 0) alloc.free(subrs.items[idx]);
                    subrs.items[idx] = try decryptType1BytesAlloc(alloc, len_iv, value_tok.string);
                    continue;
                }
                if (std.mem.eql(u8, kw, "end")) in_subrs = false;
            },
            else => {},
        }
    }

    return try subrs.toOwnedSlice(alloc);
}

fn parseType1GlyphsAlloc(
    alloc: Allocator,
    bytes: []const u8,
    len_iv: i64,
    font_obj: *const syntax.Object,
    code_to_name: *[256]?[]const u8,
    first_char: usize,
    widths: []const syntax.Object,
) ![]Type1Glyph {
    const scanner_result = parseType1GlyphsLexAlloc(alloc, bytes, len_iv, font_obj, code_to_name, first_char, widths) catch null;
    if (scanner_result) |glyphs| {
        if (glyphs.len > 0) return glyphs;
        alloc.free(glyphs);
    }
    return try parseType1GlyphsRdAlloc(alloc, bytes, len_iv, font_obj, code_to_name, first_char, widths);
}

fn parseType1GlyphsLexAlloc(
    alloc: Allocator,
    bytes: []const u8,
    len_iv: i64,
    font_obj: *const syntax.Object,
    code_to_name: *[256]?[]const u8,
    first_char: usize,
    widths: []const syntax.Object,
) ![]Type1Glyph {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();
    var in_charstrings = false;
    var glyph_names = std.ArrayList([]u8).empty;
    var glyph_programs = std.ArrayList([]u8).empty;
    errdefer {
        for (glyph_names.items) |name| alloc.free(name);
        glyph_names.deinit(alloc);
        for (glyph_programs.items) |program| alloc.free(program);
        glyph_programs.deinit(alloc);
    }

    while (true) {
        var tok = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &tok);
        switch (tok) {
            .eof => break,
            .name => |name| in_charstrings = std.mem.eql(u8, name, "CharStrings"),
            .keyword => |kw| {
                if (!in_charstrings) continue;
                if (std.mem.eql(u8, kw, "dup")) {
                    var name_tok = try scanner.readLexeme();
                    defer syntax.Scanner.freeLexeme(alloc, &name_tok);
                    if (name_tok != .name) continue;
                    var value_tok = try scanner.readLexeme();
                    defer syntax.Scanner.freeLexeme(alloc, &value_tok);
                    if (value_tok != .string) continue;
                    try glyph_names.append(alloc, try alloc.dupe(u8, name_tok.name));
                    try glyph_programs.append(alloc, try decryptType1BytesAlloc(alloc, len_iv, value_tok.string));
                    continue;
                }
                if (std.mem.eql(u8, kw, "end")) in_charstrings = false;
            },
            else => {},
        }
    }

    for (glyph_names.items) |name| {
        if (name.len == 1 and code_to_name[name[0]] == null) code_to_name[name[0]] = name;
    }
    try populateType1EncodingFallbacks(font_obj, code_to_name, glyph_names.items);

    var glyphs = std.ArrayList(Type1Glyph).empty;
    errdefer {
        for (glyphs.items) |*glyph| glyph.deinit(alloc);
        glyphs.deinit(alloc);
    }
    for (code_to_name, 0..) |maybe_name, code| {
        const glyph_name = maybe_name orelse continue;
        for (glyph_names.items, glyph_programs.items) |parsed_name, program| {
            if (!std.mem.eql(u8, glyph_name, parsed_name)) continue;
            const advance = if (code >= first_char and code - first_char < widths.len)
                numericObjectValue(&widths[code - first_char]) orelse 1000.0
            else
                1000.0;
            try glyphs.append(alloc, .{
                .code = @intCast(code),
                .name = try alloc.dupe(u8, glyph_name),
                .charstring = try alloc.dupe(u8, program),
                .advance = advance,
            });
            break;
        }
    }

    for (glyph_names.items) |name| alloc.free(name);
    glyph_names.deinit(alloc);
    for (glyph_programs.items) |program| alloc.free(program);
    glyph_programs.deinit(alloc);
    return try glyphs.toOwnedSlice(alloc);
}

fn parseType1LocalSubrsRdAlloc(alloc: Allocator, bytes: []const u8, len_iv: i64) ![][]u8 {
    const subrs_start = std.mem.indexOf(u8, bytes, "/Subrs") orelse return try alloc.alloc([]u8, 0);
    var i: usize = subrs_start + "/Subrs".len;
    var subrs = std.ArrayList([]u8).empty;
    errdefer {
        for (subrs.items) |subr| alloc.free(subr);
        subrs.deinit(alloc);
    }

    while (i < bytes.len) {
        skipType1WhitespaceAndComments(bytes, &i);
        if (i >= bytes.len) break;
        if (matchType1Word(bytes, i, "/CharStrings")) break;
        if (!matchType1Word(bytes, i, "dup")) {
            _ = readType1BareToken(bytes, &i);
            continue;
        }
        i += 3;
        skipType1WhitespaceAndComments(bytes, &i);
        const idx_tok = readType1BareToken(bytes, &i) orelse continue;
        const idx = std.fmt.parseInt(usize, idx_tok, 10) catch continue;
        skipType1WhitespaceAndComments(bytes, &i);
        var raw: []u8 = &.{};
        if (i < bytes.len and bytes[i] == '<') {
            raw = try parseType1HexStringAlloc(alloc, bytes, &i);
        } else {
            const len_tok = readType1BareToken(bytes, &i) orelse continue;
            const byte_len = std.fmt.parseInt(usize, len_tok, 10) catch continue;
            skipType1WhitespaceAndComments(bytes, &i);
            const op = readType1BareToken(bytes, &i) orelse continue;
            if (!std.mem.eql(u8, op, "RD") and !std.mem.eql(u8, op, "-|")) continue;
            if (i < bytes.len and isType1Whitespace(bytes[i])) i += 1;
            if (i + byte_len > bytes.len) return error.InvalidType1;
            raw = try alloc.dupe(u8, bytes[i .. i + byte_len]);
            i += byte_len;
        }
        defer if (raw.len > 0) alloc.free(raw);
        try ensureType1SubrCapacity(alloc, &subrs, idx);
        if (subrs.items[idx].len > 0) alloc.free(subrs.items[idx]);
        subrs.items[idx] = try decryptType1BytesAlloc(alloc, len_iv, raw);
    }

    return try subrs.toOwnedSlice(alloc);
}

fn parseType1GlyphsRdAlloc(
    alloc: Allocator,
    bytes: []const u8,
    len_iv: i64,
    font_obj: *const syntax.Object,
    code_to_name: *[256]?[]const u8,
    first_char: usize,
    widths: []const syntax.Object,
) ![]Type1Glyph {
    const section_start = std.mem.indexOf(u8, bytes, "/CharStrings") orelse return try alloc.alloc(Type1Glyph, 0);
    var i: usize = section_start + "/CharStrings".len;
    var glyph_names = std.ArrayList([]u8).empty;
    var glyph_programs = std.ArrayList([]u8).empty;
    errdefer {
        for (glyph_names.items) |name| alloc.free(name);
        glyph_names.deinit(alloc);
        for (glyph_programs.items) |program| alloc.free(program);
        glyph_programs.deinit(alloc);
    }

    while (i < bytes.len) {
        skipType1WhitespaceAndComments(bytes, &i);
        if (i >= bytes.len) break;
        if (!matchType1Word(bytes, i, "dup")) {
            const tok = readType1BareToken(bytes, &i) orelse break;
            if (std.mem.eql(u8, tok, "end")) break;
            continue;
        }
        i += 3;
        skipType1WhitespaceAndComments(bytes, &i);
        const name = readType1NameToken(bytes, &i) orelse continue;
        skipType1WhitespaceAndComments(bytes, &i);
        var raw: []u8 = &.{};
        if (i < bytes.len and bytes[i] == '<') {
            raw = try parseType1HexStringAlloc(alloc, bytes, &i);
        } else {
            const len_tok = readType1BareToken(bytes, &i) orelse continue;
            const byte_len = std.fmt.parseInt(usize, len_tok, 10) catch continue;
            skipType1WhitespaceAndComments(bytes, &i);
            const op = readType1BareToken(bytes, &i) orelse continue;
            if (!std.mem.eql(u8, op, "RD") and !std.mem.eql(u8, op, "-|")) continue;
            if (i < bytes.len and isType1Whitespace(bytes[i])) i += 1;
            if (i + byte_len > bytes.len) return error.InvalidType1;
            raw = try alloc.dupe(u8, bytes[i .. i + byte_len]);
            i += byte_len;
        }
        defer if (raw.len > 0) alloc.free(raw);
        try glyph_names.append(alloc, try alloc.dupe(u8, name));
        try glyph_programs.append(alloc, try decryptType1BytesAlloc(alloc, len_iv, raw));
    }

    for (glyph_names.items) |name| {
        if (name.len == 1 and code_to_name[name[0]] == null) code_to_name[name[0]] = name;
    }
    try populateType1EncodingFallbacks(font_obj, code_to_name, glyph_names.items);

    var glyphs = std.ArrayList(Type1Glyph).empty;
    errdefer {
        for (glyphs.items) |*glyph| glyph.deinit(alloc);
        glyphs.deinit(alloc);
    }
    for (code_to_name, 0..) |maybe_name, code| {
        const glyph_name = maybe_name orelse continue;
        for (glyph_names.items, glyph_programs.items) |parsed_name, program| {
            if (!std.mem.eql(u8, glyph_name, parsed_name)) continue;
            const advance = if (code >= first_char and code - first_char < widths.len)
                numericObjectValue(&widths[code - first_char]) orelse 1000.0
            else
                1000.0;
            try glyphs.append(alloc, .{
                .code = @intCast(code),
                .name = try alloc.dupe(u8, glyph_name),
                .charstring = try alloc.dupe(u8, program),
                .advance = advance,
            });
            break;
        }
    }

    for (glyph_names.items) |name| alloc.free(name);
    glyph_names.deinit(alloc);
    for (glyph_programs.items) |program| alloc.free(program);
    glyph_programs.deinit(alloc);
    return try glyphs.toOwnedSlice(alloc);
}

fn isType1Whitespace(b: u8) bool {
    return b == ' ' or b == '\t' or b == '\r' or b == '\n' or b == '\x0c' or b == '\x00';
}

fn isType1Delimiter(b: u8) bool {
    return isType1Whitespace(b) or b == '/' or b == '<' or b == '>' or b == '[' or b == ']' or b == '{' or b == '}' or b == '(' or b == ')' or b == '%';
}

fn skipType1WhitespaceAndComments(bytes: []const u8, index: *usize) void {
    while (index.* < bytes.len) {
        if (isType1Whitespace(bytes[index.*])) {
            index.* += 1;
            continue;
        }
        if (bytes[index.*] == '%') {
            while (index.* < bytes.len and bytes[index.*] != '\n' and bytes[index.*] != '\r') index.* += 1;
            continue;
        }
        break;
    }
}

fn matchType1Word(bytes: []const u8, index: usize, word: []const u8) bool {
    if (index + word.len > bytes.len) return false;
    if (!std.mem.eql(u8, bytes[index .. index + word.len], word)) return false;
    if (index > 0 and !isType1Delimiter(bytes[index - 1])) return false;
    if (index + word.len < bytes.len and !isType1Delimiter(bytes[index + word.len])) return false;
    return true;
}

fn readType1BareToken(bytes: []const u8, index: *usize) ?[]const u8 {
    skipType1WhitespaceAndComments(bytes, index);
    if (index.* >= bytes.len) return null;
    if (bytes[index.*] == '/') return null;
    const start = index.*;
    while (index.* < bytes.len and !isType1Delimiter(bytes[index.*])) index.* += 1;
    if (index.* == start) return null;
    return bytes[start..index.*];
}

fn readType1NameToken(bytes: []const u8, index: *usize) ?[]const u8 {
    skipType1WhitespaceAndComments(bytes, index);
    if (index.* >= bytes.len or bytes[index.*] != '/') return null;
    index.* += 1;
    const start = index.*;
    while (index.* < bytes.len and !isType1Delimiter(bytes[index.*])) index.* += 1;
    if (index.* == start) return null;
    return bytes[start..index.*];
}

fn hexNibble(value: u8) ?u8 {
    return switch (value) {
        '0'...'9' => value - '0',
        'a'...'f' => value - 'a' + 10,
        'A'...'F' => value - 'A' + 10,
        else => null,
    };
}

fn parseType1HexStringAlloc(alloc: Allocator, bytes: []const u8, index: *usize) ![]u8 {
    skipType1WhitespaceAndComments(bytes, index);
    if (index.* >= bytes.len or bytes[index.*] != '<') return error.InvalidType1;
    index.* += 1;
    var hex = std.ArrayList(u8).empty;
    defer hex.deinit(alloc);
    while (index.* < bytes.len and bytes[index.*] != '>') : (index.* += 1) {
        if (isType1Whitespace(bytes[index.*])) continue;
        const nibble = hexNibble(bytes[index.*]) orelse return error.InvalidType1;
        try hex.append(alloc, nibble);
    }
    if (index.* >= bytes.len) return error.InvalidType1;
    index.* += 1;
    const out_len = (hex.items.len + 1) / 2;
    var out = try alloc.alloc(u8, out_len);
    var h: usize = 0;
    var o: usize = 0;
    while (h < hex.items.len) : (o += 1) {
        const hi = hex.items[h];
        const lo = if (h + 1 < hex.items.len) hex.items[h + 1] else 0;
        out[o] = (hi << 4) | lo;
        h += 2;
    }
    return out;
}

fn parseType3GlyphAdvance(content: []const u8) ?[2]f64 {
    var scanner = syntax.Scanner.init(std.heap.page_allocator, content);
    defer scanner.deinit();
    var operands = std.ArrayList(syntax.Object).empty;
    defer {
        for (operands.items) |*obj| obj.deinit(std.heap.page_allocator);
        operands.deinit(std.heap.page_allocator);
    }

    while (true) {
        var lex = scanner.readLexeme() catch return null;
        defer syntax.Scanner.freeLexeme(std.heap.page_allocator, &lex);
        if (lex == .eof) break;

        switch (lex) {
            .integer => {
                const cloned = syntax.Object{ .integer = lex.integer };
                operands.append(std.heap.page_allocator, cloned) catch return null;
            },
            .real => {
                const cloned = syntax.Object{ .real = lex.real };
                operands.append(std.heap.page_allocator, cloned) catch return null;
            },
            .keyword => |keyword| {
                if ((std.mem.eql(u8, keyword, "d0") or std.mem.eql(u8, keyword, "d1")) and operands.items.len >= 2) {
                    return .{
                        numericObjectValue(&operands.items[operands.items.len - 2]) orelse 0,
                        numericObjectValue(&operands.items[operands.items.len - 1]) orelse 0,
                    };
                }
                for (operands.items) |*obj| obj.deinit(std.heap.page_allocator);
                operands.clearRetainingCapacity();
            },
            else => {
                for (operands.items) |*obj| obj.deinit(std.heap.page_allocator);
                operands.clearRetainingCapacity();
            },
        }
    }
    return null;
}

fn extractHexStringsAlloc(alloc: Allocator, line: []const u8) ![][]u8 {
    var out = std.ArrayList([]u8).empty;
    defer out.deinit(alloc);

    var i: usize = 0;
    while (i < line.len) {
        const start = std.mem.indexOfScalarPos(u8, line, i, '<') orelse break;
        const end = std.mem.indexOfScalarPos(u8, line, start + 1, '>') orelse break;
        try out.append(alloc, try alloc.dupe(u8, line[start + 1 .. end]));
        i = end + 1;
    }
    return try out.toOwnedSlice(alloc);
}

fn parseHexToU16(hex: []const u8) !u16 {
    if (hex.len == 0) return error.InvalidHexString;
    return try std.fmt.parseInt(u16, hex, 16);
}

fn parseHexToU32(hex: []const u8) !u32 {
    if (hex.len == 0 or hex.len > 8) return error.InvalidHexString;
    return try std.fmt.parseInt(u32, hex, 16);
}

fn parseCodeBytesToU32(bytes: []const u8) !u32 {
    if (bytes.len == 0 or bytes.len > 4) return error.InvalidHexString;
    var value: u32 = 0;
    for (bytes) |b| value = (value << 8) | b;
    return value;
}

fn decodeToUnicodeDestAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    if (bytes.len == 0) return try alloc.dupe(u8, "");
    if (bytes.len == 1) return try text_encoding.pdfDocDecodeAlloc(alloc, bytes);
    if (bytes.len % 2 != 0) return try alloc.dupe(u8, bytes);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    var i: usize = 0;
    while (i + 1 < bytes.len) : (i += 2) {
        const cp = (@as(u16, bytes[i]) << 8) | bytes[i + 1];
        var buf: [4]u8 = undefined;
        const n = try std.unicode.utf8Encode(cp, &buf);
        try out.appendSlice(alloc, buf[0..n]);
    }
    return try out.toOwnedSlice(alloc);
}

fn parseHexToUtf8Alloc(alloc: Allocator, hex: []const u8) ![]u8 {
    if (hex.len == 0) return try alloc.dupe(u8, "");
    if (hex.len % 4 == 0 and hex.len >= 4) {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(alloc);
        var i: usize = 0;
        while (i + 3 < hex.len) : (i += 4) {
            const cp = try std.fmt.parseInt(u16, hex[i .. i + 4], 16);
            var buf: [4]u8 = undefined;
            const n = try std.unicode.utf8Encode(cp, &buf);
            try out.appendSlice(alloc, buf[0..n]);
        }
        return try out.toOwnedSlice(alloc);
    }

    if (hex.len == 2) {
        const byte = try std.fmt.parseInt(u8, hex, 16);
        return try text_encoding.pdfDocDecodeAlloc(alloc, &.{byte});
    }

    const cp = try std.fmt.parseInt(u16, hex, 16);
    var buf: [4]u8 = undefined;
    const n = try std.unicode.utf8Encode(cp, &buf);
    return try alloc.dupe(u8, buf[0..n]);
}

fn extractTextFromContentAlloc(alloc: Allocator, bytes: []const u8, fonts: []const PageFont) ![]u8 {
    var scanner = syntax.Scanner.init(alloc, bytes);
    defer scanner.deinit();

    var operands = std.ArrayList(syntax.Object).empty;
    defer {
        for (operands.items) |*obj| obj.deinit(alloc);
        operands.deinit(alloc);
    }

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    var state = TextExtractionState{};

    while (true) {
        var lex = try scanner.readLexeme();
        defer syntax.Scanner.freeLexeme(alloc, &lex);

        if (lex == .eof) break;
        if (lex == .keyword) {
            try applyTextOperator(alloc, &out, &state, fonts, lex.keyword, operands.items);
            for (operands.items) |*obj| obj.deinit(alloc);
            operands.clearRetainingCapacity();
            continue;
        }

        try scanner.unreadLexeme(try cloneLexemeForContent(alloc, lex));
        try operands.append(alloc, try scanner.readObject());
    }

    return try out.toOwnedSlice(alloc);
}

fn cloneLexemeForContent(alloc: Allocator, lex: syntax.Lexeme) !syntax.Lexeme {
    return switch (lex) {
        .eof => .eof,
        .boolean => |v| .{ .boolean = v },
        .integer => |v| .{ .integer = v },
        .real => |v| .{ .real = v },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
        .name => |v| .{ .name = try alloc.dupe(u8, v) },
        .keyword => |v| .{ .keyword = try alloc.dupe(u8, v) },
    };
}

fn applyTextOperator(
    alloc: Allocator,
    out: *std.ArrayList(u8),
    state: *TextExtractionState,
    fonts: []const PageFont,
    op: []const u8,
    operands: []const syntax.Object,
) !void {
    if (std.mem.eql(u8, op, "Tf")) {
        if (operands.len >= 2 and operands[operands.len - 2] == .name) {
            const font_name = operands[operands.len - 2].name;
            state.current_font_index = null;
            for (fonts, 0..) |font, i| {
                if (std.mem.eql(u8, font.name, font_name)) {
                    state.current_font_index = i;
                    break;
                }
            }
        }
        return;
    }
    if (std.mem.eql(u8, op, "Tj")) {
        if (operands.len >= 1) try appendTextOperand(alloc, out, state, fonts, &operands[operands.len - 1]);
        return;
    }
    if (std.mem.eql(u8, op, "TJ")) {
        if (operands.len == 0) return;
        const last = &operands[operands.len - 1];
        if (last.* != .array) return;
        for (last.array) |item| {
            if (item == .string) try appendDecodedString(alloc, out, state, fonts, item.string);
        }
        return;
    }
    if (std.mem.eql(u8, op, "'")) {
        try appendNewline(alloc, out);
        if (operands.len >= 1) try appendTextOperand(alloc, out, state, fonts, &operands[operands.len - 1]);
        return;
    }
    if (std.mem.eql(u8, op, "\"")) {
        try appendNewline(alloc, out);
        if (operands.len >= 1) try appendTextOperand(alloc, out, state, fonts, &operands[operands.len - 1]);
        return;
    }
    if (std.mem.eql(u8, op, "T*") or std.mem.eql(u8, op, "ET")) {
        try appendNewline(alloc, out);
    }
}

fn applyTextRunOperator(
    alloc: Allocator,
    out: *std.ArrayList(TextRun),
    state: *TextRunState,
    stack: *std.ArrayList(TextRunStackEntry),
    current_path: *std.ArrayList([2]f64),
    current_path_closed: *bool,
    current_clip_points: *std.ArrayList([2]f64),
    current_clip_fill_rule: *FillRule,
    fonts: []const PageFont,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    paint_order: *usize,
    next_group_id: *u32,
    op: []const u8,
    operands: []const syntax.Object,
) anyerror!void {
    if (std.mem.eql(u8, op, "q")) {
        try stack.append(alloc, .{
            .matrix = state.matrix,
            .alpha = state.alpha,
            .stroke_alpha = state.stroke_alpha,
            .text_a = state.text_a,
            .text_b = state.text_b,
            .text_c = state.text_c,
            .text_d = state.text_d,
            .fill_color = state.fill_color,
            .stroke_color = state.stroke_color,
            .stroke_width = state.stroke_width,
            .blend_mode = state.blend_mode,
            .group_id = state.group_id,
            .group_parent_id = state.group_parent_id,
            .group_isolated = state.group_isolated,
            .group_knockout = state.group_knockout,
            .fill_color_space = state.fill_color_space,
            .stroke_color_space = state.stroke_color_space,
            .fill_pattern_name = state.fill_pattern_name,
            .stroke_pattern_name = state.stroke_pattern_name,
            .clip_box = state.clip_box,
            .clip_points = try alloc.dupe([2]f64, current_clip_points.items),
            .clip_fill_rule = current_clip_fill_rule.*,
        });
        return;
    }
    if (std.mem.eql(u8, op, "Q")) {
        if (stack.items.len > 0) {
            var entry = stack.pop().?;
            state.matrix = entry.matrix;
            state.alpha = entry.alpha;
            state.stroke_alpha = entry.stroke_alpha;
            state.text_a = entry.text_a;
            state.text_b = entry.text_b;
            state.text_c = entry.text_c;
            state.text_d = entry.text_d;
            state.fill_color = entry.fill_color;
            state.stroke_color = entry.stroke_color;
            state.stroke_width = entry.stroke_width;
            state.blend_mode = entry.blend_mode;
            state.group_id = entry.group_id;
            state.group_parent_id = entry.group_parent_id;
            state.group_isolated = entry.group_isolated;
            state.group_knockout = entry.group_knockout;
            state.fill_color_space = entry.fill_color_space;
            state.stroke_color_space = entry.stroke_color_space;
            state.fill_pattern_name = entry.fill_pattern_name;
            state.stroke_pattern_name = entry.stroke_pattern_name;
            state.clip_box = entry.clip_box;
            current_clip_points.clearRetainingCapacity();
            try current_clip_points.appendSlice(alloc, entry.clip_points);
            current_clip_fill_rule.* = entry.clip_fill_rule;
            entry.deinit(alloc);
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "gs") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        if (findExtGState(gstates, operands[operands.len - 1].name)) |gstate| {
            state.alpha = gstate.fill_alpha;
            state.stroke_alpha = gstate.stroke_alpha;
            state.blend_mode = gstate.blend_mode;
        }
        return;
    }
    if (std.mem.eql(u8, op, "cm")) {
        if (operands.len >= 6) {
            const m = GraphicsMatrix{
                .a = numericObjectValue(&operands[operands.len - 6]) orelse 1,
                .b = numericObjectValue(&operands[operands.len - 5]) orelse 0,
                .c = numericObjectValue(&operands[operands.len - 4]) orelse 0,
                .d = numericObjectValue(&operands[operands.len - 3]) orelse 1,
                .e = numericObjectValue(&operands[operands.len - 2]) orelse 0,
                .f = numericObjectValue(&operands[operands.len - 1]) orelse 0,
            };
            state.matrix = multiplyGraphicsMatrix(state.matrix, m);
        }
        return;
    }
    if (std.mem.eql(u8, op, "m") and operands.len >= 2) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "l") and operands.len >= 2) {
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "c") and operands.len >= 6 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 6]) orelse 0, numericObjectValue(&operands[operands.len - 5]) orelse 0);
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "v") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, start, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "y") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, end, end);
        return;
    }
    if (std.mem.eql(u8, op, "c") and operands.len >= 6 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 6]) orelse 0, numericObjectValue(&operands[operands.len - 5]) orelse 0);
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "v") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, start, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "y") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, end, end);
        return;
    }
    if (std.mem.eql(u8, op, "h")) {
        current_path_closed.* = true;
        return;
    }
    if (std.mem.eql(u8, op, "re") and operands.len >= 4) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = true;
        const x = numericObjectValue(&operands[operands.len - 4]) orelse 0;
        const y = numericObjectValue(&operands[operands.len - 3]) orelse 0;
        const w = numericObjectValue(&operands[operands.len - 2]) orelse 0;
        const h = numericObjectValue(&operands[operands.len - 1]) orelse 0;
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y + h));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y + h));
        return;
    }
    if (std.mem.eql(u8, op, "W") or std.mem.eql(u8, op, "W*")) {
        if (current_path.items.len >= 2) {
            const next_clip = pathBounds(current_path.items);
            state.clip_box = if (state.clip_box) |current_clip|
                intersectPageBoxes(current_clip, next_clip)
            else
                next_clip;
            if (current_clip_points.items.len == 0) {
                try current_clip_points.appendSlice(alloc, current_path.items);
                current_clip_fill_rule.* = if (std.mem.eql(u8, op, "W*")) .even_odd else .nonzero;
            } else {
                if (try tryIntersectRectClipPoints(alloc, current_clip_points.items, current_path.items)) |intersection| {
                    defer alloc.free(intersection);
                    current_clip_points.clearRetainingCapacity();
                    try current_clip_points.appendSlice(alloc, intersection);
                    current_clip_fill_rule.* = .nonzero;
                } else {
                    current_clip_points.clearRetainingCapacity();
                }
            }
        }
        return;
    }
    if (std.mem.eql(u8, op, "n")) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "BT")) {
        state.x = 0;
        state.y = 0;
        state.line_x = 0;
        state.line_y = 0;
        state.text_a = 1;
        state.text_b = 0;
        state.text_c = 0;
        state.text_d = 1;
        return;
    }
    if (std.mem.eql(u8, op, "Tf")) {
        if (operands.len >= 2 and operands[operands.len - 2] == .name) {
            const font_name = operands[operands.len - 2].name;
            state.current_font_index = null;
            for (fonts, 0..) |font, i| {
                if (std.mem.eql(u8, font.name, font_name)) {
                    state.current_font_index = i;
                    break;
                }
            }
            state.font_size = numericObjectValue(&operands[operands.len - 1]) orelse state.font_size;
            if (state.font_size <= 0) state.font_size = 12;
        }
        return;
    }
    if (std.mem.eql(u8, op, "Tc")) {
        if (operands.len >= 1) state.char_spacing = numericObjectValue(&operands[operands.len - 1]) orelse state.char_spacing;
        return;
    }
    if (std.mem.eql(u8, op, "Tw")) {
        if (operands.len >= 1) state.word_spacing = numericObjectValue(&operands[operands.len - 1]) orelse state.word_spacing;
        return;
    }
    if (std.mem.eql(u8, op, "Tz")) {
        if (operands.len >= 1) {
            const scale = numericObjectValue(&operands[operands.len - 1]) orelse 100.0;
            state.horizontal_scale = @max(0.0, scale / 100.0);
        }
        return;
    }
    if (std.mem.eql(u8, op, "Ts")) {
        if (operands.len >= 1) state.rise = numericObjectValue(&operands[operands.len - 1]) orelse state.rise;
        return;
    }
    if (std.mem.eql(u8, op, "Tr")) {
        if (operands.len >= 1) state.render_mode = operands[operands.len - 1].asInteger() orelse state.render_mode;
        return;
    }
    if (std.mem.eql(u8, op, "w")) {
        if (operands.len >= 1) state.stroke_width = @max(0.0, numericObjectValue(&operands[operands.len - 1]) orelse state.stroke_width);
        return;
    }
    if (std.mem.eql(u8, op, "rg")) {
        if (operands.len >= 3) {
            state.fill_color_space = "DeviceRGB";
            state.fill_pattern_name = null;
            state.fill_color = rgbColor(
                numericObjectValue(&operands[operands.len - 3]) orelse 0,
                numericObjectValue(&operands[operands.len - 2]) orelse 0,
                numericObjectValue(&operands[operands.len - 1]) orelse 0,
            );
        }
        return;
    }
    if (std.mem.eql(u8, op, "RG")) {
        if (operands.len >= 3) {
            state.stroke_color_space = "DeviceRGB";
            state.stroke_pattern_name = null;
            state.stroke_color = rgbColor(
                numericObjectValue(&operands[operands.len - 3]) orelse 0,
                numericObjectValue(&operands[operands.len - 2]) orelse 0,
                numericObjectValue(&operands[operands.len - 1]) orelse 0,
            );
        }
        return;
    }
    if (std.mem.eql(u8, op, "g")) {
        if (operands.len >= 1) {
            state.fill_color_space = "DeviceGray";
            state.fill_pattern_name = null;
            state.fill_color = grayColor(numericObjectValue(&operands[operands.len - 1]) orelse 0);
        }
        return;
    }
    if (std.mem.eql(u8, op, "G")) {
        if (operands.len >= 1) {
            state.stroke_color_space = "DeviceGray";
            state.stroke_pattern_name = null;
            state.stroke_color = grayColor(numericObjectValue(&operands[operands.len - 1]) orelse 0);
        }
        return;
    }
    if (std.mem.eql(u8, op, "k")) {
        if (operands.len >= 4) {
            state.fill_color_space = "DeviceCMYK";
            state.fill_pattern_name = null;
            state.fill_color = cmykColor(
                numericObjectValue(&operands[operands.len - 4]) orelse 0,
                numericObjectValue(&operands[operands.len - 3]) orelse 0,
                numericObjectValue(&operands[operands.len - 2]) orelse 0,
                numericObjectValue(&operands[operands.len - 1]) orelse 0,
            );
        }
        return;
    }
    if (std.mem.eql(u8, op, "K")) {
        if (operands.len >= 4) {
            state.stroke_color_space = "DeviceCMYK";
            state.stroke_pattern_name = null;
            state.stroke_color = cmykColor(
                numericObjectValue(&operands[operands.len - 4]) orelse 0,
                numericObjectValue(&operands[operands.len - 3]) orelse 0,
                numericObjectValue(&operands[operands.len - 2]) orelse 0,
                numericObjectValue(&operands[operands.len - 1]) orelse 0,
            );
        }
        return;
    }
    if (std.mem.eql(u8, op, "cs")) {
        if (operands.len >= 1 and operands[operands.len - 1] == .name) {
            state.fill_color_space = operands[operands.len - 1].name;
            if (!std.mem.eql(u8, state.fill_color_space, "Pattern")) state.fill_pattern_name = null;
        }
        return;
    }
    if (std.mem.eql(u8, op, "CS")) {
        if (operands.len >= 1 and operands[operands.len - 1] == .name) {
            state.stroke_color_space = operands[operands.len - 1].name;
            if (!std.mem.eql(u8, state.stroke_color_space, "Pattern")) state.stroke_pattern_name = null;
        }
        return;
    }
    if (std.mem.eql(u8, op, "sc") or std.mem.eql(u8, op, "scn")) {
        if (std.mem.eql(u8, state.fill_color_space, "Pattern")) {
            if (operands.len >= 1 and operands[operands.len - 1] == .name) {
                state.fill_pattern_name = operands[operands.len - 1].name;
                if (decodePatternBaseColorOperands(operands[0 .. operands.len - 1])) |color| state.fill_color = color;
            }
        } else if (try decodeShapeColorOperands(state.fill_color_space, operands)) |color| {
            state.fill_pattern_name = null;
            state.fill_color = color;
        }
        return;
    }
    if (std.mem.eql(u8, op, "SC") or std.mem.eql(u8, op, "SCN")) {
        if (std.mem.eql(u8, state.stroke_color_space, "Pattern")) {
            if (operands.len >= 1 and operands[operands.len - 1] == .name) {
                state.stroke_pattern_name = operands[operands.len - 1].name;
                if (decodePatternBaseColorOperands(operands[0 .. operands.len - 1])) |color| state.stroke_color = color;
            }
        } else if (try decodeShapeColorOperands(state.stroke_color_space, operands)) |color| {
            state.stroke_pattern_name = null;
            state.stroke_color = color;
        }
        return;
    }
    if (std.mem.eql(u8, op, "TL")) {
        if (operands.len >= 1) state.leading = numericObjectValue(&operands[operands.len - 1]) orelse state.leading;
        return;
    }
    if (std.mem.eql(u8, op, "Td") or std.mem.eql(u8, op, "TD")) {
        if (operands.len >= 2) {
            const tx = numericObjectValue(&operands[operands.len - 2]) orelse 0;
            const ty = numericObjectValue(&operands[operands.len - 1]) orelse 0;
            state.x += tx * state.text_a + ty * state.text_c;
            state.y += tx * state.text_b + ty * state.text_d;
            state.line_x = state.x;
            state.line_y = state.y;
            if (std.mem.eql(u8, op, "TD")) state.leading = -ty;
        }
        return;
    }
    if (std.mem.eql(u8, op, "Tm")) {
        if (operands.len >= 6) {
            state.text_a = numericObjectValue(&operands[operands.len - 6]) orelse state.text_a;
            state.text_b = numericObjectValue(&operands[operands.len - 5]) orelse state.text_b;
            state.text_c = numericObjectValue(&operands[operands.len - 4]) orelse state.text_c;
            state.text_d = numericObjectValue(&operands[operands.len - 3]) orelse state.text_d;
            state.x = numericObjectValue(&operands[operands.len - 2]) orelse state.x;
            state.y = numericObjectValue(&operands[operands.len - 1]) orelse state.y;
            state.line_x = state.x;
            state.line_y = state.y;
        }
        return;
    }
    if (std.mem.eql(u8, op, "T*")) {
        state.x = state.line_x - state.leading * state.text_c;
        state.y = state.line_y - state.leading * state.text_d;
        state.line_x = state.x;
        state.line_y = state.y;
        return;
    }
    if (std.mem.eql(u8, op, "Tj")) {
        if (operands.len >= 1) {
            try appendTextRunOperand(alloc, out, state, current_clip_points.items, current_clip_fill_rule.*, fonts, paint_order.*, &operands[operands.len - 1]);
            paint_order.* += 1;
        }
        return;
    }
    if (std.mem.eql(u8, op, "TJ")) {
        if (operands.len == 0) return;
        const last = &operands[operands.len - 1];
        if (last.* != .array) return;
        for (last.array) |item| {
            switch (item) {
                .string => try appendTextRunDecodedString(alloc, out, state, current_clip_points.items, current_clip_fill_rule.*, fonts, paint_order.*, item.string),
                .integer => |value| {
                    const adjust = (@as(f64, @floatFromInt(value)) / 1000.0) * state.font_size * state.horizontal_scale;
                    state.x -= adjust * state.text_a;
                    state.y -= adjust * state.text_b;
                },
                .real => |value| {
                    const adjust = (value / 1000.0) * state.font_size * state.horizontal_scale;
                    state.x -= adjust * state.text_a;
                    state.y -= adjust * state.text_b;
                },
                else => {},
            }
        }
        paint_order.* += 1;
        return;
    }
    if (std.mem.eql(u8, op, "'")) {
        state.x = state.line_x - state.leading * state.text_c;
        state.y = state.line_y - state.leading * state.text_d;
        state.line_x = state.x;
        state.line_y = state.y;
        if (operands.len >= 1) {
            try appendTextRunOperand(alloc, out, state, current_clip_points.items, current_clip_fill_rule.*, fonts, paint_order.*, &operands[operands.len - 1]);
            paint_order.* += 1;
        }
        return;
    }
    if (std.mem.eql(u8, op, "\"")) {
        if (operands.len >= 3) {
            state.word_spacing = numericObjectValue(&operands[operands.len - 3]) orelse state.word_spacing;
            state.char_spacing = numericObjectValue(&operands[operands.len - 2]) orelse state.char_spacing;
        }
        state.x = state.line_x - state.leading * state.text_c;
        state.y = state.line_y - state.leading * state.text_d;
        state.line_x = state.x;
        state.line_y = state.y;
        if (operands.len >= 1) {
            try appendTextRunOperand(alloc, out, state, current_clip_points.items, current_clip_fill_rule.*, fonts, paint_order.*, &operands[operands.len - 1]);
            paint_order.* += 1;
        }
        return;
    }
    if (std.mem.eql(u8, op, "Do")) {
        if (operands.len == 0 or operands[operands.len - 1] != .name) return;
        const name = operands[operands.len - 1].name;
        const form = findPageForm(forms, name) orelse return;
        const start_len = out.items.len;
        var nested_state = buildFormTextState(state.*, form);
        if (form.transparency_group) {
            nested_state.group_parent_id = state.group_id;
            nested_state.group_id = next_group_id.*;
            nested_state.group_isolated = form.group_isolated;
            nested_state.group_knockout = form.group_knockout;
            next_group_id.* += 1;
        }
        try extractTextRunsFromContentAppendWithState(alloc, out, form.content, form.fonts, form.gstates, form.forms, nested_state, current_clip_points.items, current_clip_fill_rule.*, paint_order, next_group_id);
        for (out.items[start_len..]) |*run| {
            run.vectorizable = false;
            run.font_index = null;
        }
        return;
    }
}

fn applyImageOperator(
    alloc: Allocator,
    out: *std.ArrayList(ImageRun),
    state: *GraphicsState,
    stack: *std.ArrayList(ImageStackEntry),
    current_path: *std.ArrayList([2]f64),
    current_path_closed: *bool,
    current_clip_points: *std.ArrayList([2]f64),
    current_clip_fill_rule: *FillRule,
    images: []const PageImage,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    paint_order: *usize,
    next_group_id: *u32,
    op: []const u8,
    operands: []const syntax.Object,
) anyerror!void {
    if (std.mem.eql(u8, op, "q")) {
        try stack.append(alloc, .{
            .state = state.*,
            .clip_points = try alloc.dupe([2]f64, current_clip_points.items),
            .clip_fill_rule = current_clip_fill_rule.*,
        });
        return;
    }
    if (std.mem.eql(u8, op, "Q")) {
        if (stack.items.len > 0) {
            var entry = stack.pop().?;
            state.* = entry.state;
            current_clip_points.clearRetainingCapacity();
            try current_clip_points.appendSlice(alloc, entry.clip_points);
            current_clip_fill_rule.* = entry.clip_fill_rule;
            entry.deinit(alloc);
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "cm")) {
        if (operands.len >= 6) {
            const m = GraphicsMatrix{
                .a = numericObjectValue(&operands[operands.len - 6]) orelse 1,
                .b = numericObjectValue(&operands[operands.len - 5]) orelse 0,
                .c = numericObjectValue(&operands[operands.len - 4]) orelse 0,
                .d = numericObjectValue(&operands[operands.len - 3]) orelse 1,
                .e = numericObjectValue(&operands[operands.len - 2]) orelse 0,
                .f = numericObjectValue(&operands[operands.len - 1]) orelse 0,
            };
            state.matrix = multiplyGraphicsMatrix(state.matrix, m);
        }
        return;
    }
    if (std.mem.eql(u8, op, "gs") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        if (findExtGState(gstates, operands[operands.len - 1].name)) |gstate| {
            state.fill_alpha = gstate.fill_alpha;
            state.stroke_alpha = gstate.stroke_alpha;
            state.blend_mode = gstate.blend_mode;
        }
        return;
    }
    if (std.mem.eql(u8, op, "m") and operands.len >= 2) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "l") and operands.len >= 2) {
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "c") and operands.len >= 6 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 6]) orelse 0, numericObjectValue(&operands[operands.len - 5]) orelse 0);
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "v") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, start, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "y") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, end, end);
        return;
    }
    if (std.mem.eql(u8, op, "h")) {
        current_path_closed.* = true;
        return;
    }
    if (std.mem.eql(u8, op, "re") and operands.len >= 4) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = true;
        const x = numericObjectValue(&operands[operands.len - 4]) orelse 0;
        const y = numericObjectValue(&operands[operands.len - 3]) orelse 0;
        const w = numericObjectValue(&operands[operands.len - 2]) orelse 0;
        const h = numericObjectValue(&operands[operands.len - 1]) orelse 0;
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y + h));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y + h));
        return;
    }
    if (std.mem.eql(u8, op, "W") or std.mem.eql(u8, op, "W*")) {
        if (current_path.items.len >= 2) {
            const next_clip = pathBounds(current_path.items);
            state.clip_box = if (state.clip_box) |current_clip|
                intersectPageBoxes(current_clip, next_clip)
            else
                next_clip;
            if (current_clip_points.items.len == 0) {
                try current_clip_points.appendSlice(alloc, current_path.items);
                current_clip_fill_rule.* = if (std.mem.eql(u8, op, "W*")) .even_odd else .nonzero;
            } else {
                if (try tryIntersectRectClipPoints(alloc, current_clip_points.items, current_path.items)) |intersection| {
                    defer alloc.free(intersection);
                    current_clip_points.clearRetainingCapacity();
                    try current_clip_points.appendSlice(alloc, intersection);
                    current_clip_fill_rule.* = .nonzero;
                } else {
                    current_clip_points.clearRetainingCapacity();
                }
            }
        }
        return;
    }
    if (std.mem.eql(u8, op, "n")) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "Do")) {
        if (operands.len == 0 or operands[operands.len - 1] != .name) return;
        const name = operands[operands.len - 1].name;
        for (images) |image| {
            if (!std.mem.eql(u8, image.name, name)) continue;
            try out.append(alloc, .{
                .rgba = try alloc.dupe(u8, image.rgba),
                .width = image.width,
                .height = image.height,
                .alpha = state.fill_alpha,
                .paint_order = paint_order.*,
                .blend_mode = state.blend_mode,
                .group_id = state.group_id,
                .group_parent_id = state.group_parent_id,
                .group_isolated = state.group_isolated,
                .group_knockout = state.group_knockout,
                .clip_box = state.clip_box,
                .clip_points = if (current_clip_points.items.len > 0) try alloc.dupe([2]f64, current_clip_points.items) else null,
                .clip_fill_rule = current_clip_fill_rule.*,
                .a = state.matrix.a,
                .b = state.matrix.b,
                .c = state.matrix.c,
                .d = state.matrix.d,
                .e = state.matrix.e,
                .f = state.matrix.f,
                .x = state.matrix.e,
                .y = state.matrix.f,
                .draw_width = @abs(state.matrix.a),
                .draw_height = @abs(state.matrix.d),
            });
            paint_order.* += 1;
            return;
        }
        const form = findPageForm(forms, name) orelse return;
        var nested_state = buildFormGraphicsState(state.*, form);
        if (form.transparency_group) {
            nested_state.group_parent_id = state.group_id;
            nested_state.group_id = next_group_id.*;
            nested_state.group_isolated = form.group_isolated;
            nested_state.group_knockout = form.group_knockout;
            next_group_id.* += 1;
        }
        try extractImageRunsFromContentAppendWithState(alloc, out, form.content, form.images, form.gstates, form.forms, nested_state, current_clip_points.items, current_clip_fill_rule.*, paint_order, next_group_id);
    }
}

fn applyShapeOperator(
    alloc: Allocator,
    out: *std.ArrayList(ShapeRun),
    state: *GraphicsState,
    stack: *std.ArrayList(ShapeStackEntry),
    current_path: *std.ArrayList([2]f64),
    current_path_closed: *bool,
    current_dash: *std.ArrayList(f64),
    dash_phase: *f64,
    current_clip_points: *std.ArrayList([2]f64),
    current_clip_fill_rule: *@FieldType(ShapeRun, "fill_rule"),
    gstates: []const PageExtGState,
    forms: []const PageForm,
    paint_order: *usize,
    next_group_id: *u32,
    op: []const u8,
    operands: []const syntax.Object,
) anyerror!void {
    if (std.mem.eql(u8, op, "q")) {
        try stack.append(alloc, .{
            .state = state.*,
            .dash_phase = dash_phase.*,
            .dash_array = try alloc.dupe(f64, current_dash.items),
            .clip_points = try alloc.dupe([2]f64, current_clip_points.items),
            .clip_fill_rule = current_clip_fill_rule.*,
        });
        return;
    }
    if (std.mem.eql(u8, op, "Q")) {
        if (stack.items.len > 0) {
            var entry = stack.pop().?;
            state.* = entry.state;
            dash_phase.* = entry.dash_phase;
            current_dash.clearRetainingCapacity();
            try current_dash.appendSlice(alloc, entry.dash_array);
            current_clip_points.clearRetainingCapacity();
            try current_clip_points.appendSlice(alloc, entry.clip_points);
            current_clip_fill_rule.* = entry.clip_fill_rule;
            entry.deinit(alloc);
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "cm")) {
        if (operands.len >= 6) {
            const m = GraphicsMatrix{
                .a = numericObjectValue(&operands[operands.len - 6]) orelse 1,
                .b = numericObjectValue(&operands[operands.len - 5]) orelse 0,
                .c = numericObjectValue(&operands[operands.len - 4]) orelse 0,
                .d = numericObjectValue(&operands[operands.len - 3]) orelse 1,
                .e = numericObjectValue(&operands[operands.len - 2]) orelse 0,
                .f = numericObjectValue(&operands[operands.len - 1]) orelse 0,
            };
            state.matrix = multiplyGraphicsMatrix(state.matrix, m);
        }
        return;
    }
    if (std.mem.eql(u8, op, "gs") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        if (findExtGState(gstates, operands[operands.len - 1].name)) |gstate| {
            state.fill_alpha = gstate.fill_alpha;
            state.stroke_alpha = gstate.stroke_alpha;
            state.blend_mode = gstate.blend_mode;
        }
        return;
    }
    if (std.mem.eql(u8, op, "Do")) {
        if (operands.len == 0 or operands[operands.len - 1] != .name) return;
        const form = findPageForm(forms, operands[operands.len - 1].name) orelse return;
        var nested_state = buildFormGraphicsState(state.*, form);
        if (form.transparency_group) {
            nested_state.group_parent_id = state.group_id;
            nested_state.group_id = next_group_id.*;
            nested_state.group_isolated = form.group_isolated;
            nested_state.group_knockout = form.group_knockout;
            next_group_id.* += 1;
        }
        try extractShapeRunsFromContentAppendWithState(alloc, out, form.content, form.gstates, form.forms, nested_state, current_clip_points.items, current_clip_fill_rule.*, current_dash.items, dash_phase.*, paint_order, next_group_id);
        return;
    }
    if (std.mem.eql(u8, op, "rg") and operands.len >= 3) {
        state.fill_color_space = "DeviceRGB";
        state.fill_pattern_name = null;
        state.fill_color = rgbColor(
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
        return;
    }
    if (std.mem.eql(u8, op, "RG") and operands.len >= 3) {
        state.stroke_color_space = "DeviceRGB";
        state.stroke_pattern_name = null;
        state.stroke_color = rgbColor(
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
        return;
    }
    if (std.mem.eql(u8, op, "g") and operands.len >= 1) {
        state.fill_color_space = "DeviceGray";
        state.fill_pattern_name = null;
        state.fill_color = grayColor(numericObjectValue(&operands[operands.len - 1]) orelse 0);
        return;
    }
    if (std.mem.eql(u8, op, "G") and operands.len >= 1) {
        state.stroke_color_space = "DeviceGray";
        state.stroke_pattern_name = null;
        state.stroke_color = grayColor(numericObjectValue(&operands[operands.len - 1]) orelse 0);
        return;
    }
    if (std.mem.eql(u8, op, "k") and operands.len >= 4) {
        state.fill_color_space = "DeviceCMYK";
        state.fill_pattern_name = null;
        state.fill_color = cmykColor(
            numericObjectValue(&operands[operands.len - 4]) orelse 0,
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
        return;
    }
    if (std.mem.eql(u8, op, "K") and operands.len >= 4) {
        state.stroke_color_space = "DeviceCMYK";
        state.stroke_pattern_name = null;
        state.stroke_color = cmykColor(
            numericObjectValue(&operands[operands.len - 4]) orelse 0,
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
        return;
    }
    if (std.mem.eql(u8, op, "cs") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        state.fill_color_space = operands[operands.len - 1].name;
        if (!std.mem.eql(u8, state.fill_color_space, "Pattern")) state.fill_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "CS") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        state.stroke_color_space = operands[operands.len - 1].name;
        if (!std.mem.eql(u8, state.stroke_color_space, "Pattern")) state.stroke_pattern_name = null;
        return;
    }
    if ((std.mem.eql(u8, op, "sc") or std.mem.eql(u8, op, "scn")) and operands.len >= 1) {
        if (std.mem.eql(u8, state.fill_color_space, "Pattern")) {
            if (operands[operands.len - 1] == .name) {
                state.fill_pattern_name = operands[operands.len - 1].name;
                if (decodePatternBaseColorOperands(operands[0 .. operands.len - 1])) |color| state.fill_color = color;
            }
        } else if (try decodeShapeColorOperands(state.fill_color_space, operands)) |color| {
            state.fill_pattern_name = null;
            state.fill_color = color;
        }
        return;
    }
    if ((std.mem.eql(u8, op, "SC") or std.mem.eql(u8, op, "SCN")) and operands.len >= 1) {
        if (std.mem.eql(u8, state.stroke_color_space, "Pattern")) {
            if (operands[operands.len - 1] == .name) {
                state.stroke_pattern_name = operands[operands.len - 1].name;
                if (decodePatternBaseColorOperands(operands[0 .. operands.len - 1])) |color| state.stroke_color = color;
            }
        } else if (try decodeShapeColorOperands(state.stroke_color_space, operands)) |color| {
            state.stroke_pattern_name = null;
            state.stroke_color = color;
        }
        return;
    }
    if (std.mem.eql(u8, op, "w") and operands.len >= 1) {
        state.stroke_width = @max(0.1, numericObjectValue(&operands[operands.len - 1]) orelse state.stroke_width);
        return;
    }
    if (std.mem.eql(u8, op, "J") and operands.len >= 1) {
        const cap = operands[operands.len - 1].asInteger() orelse return;
        state.line_cap = switch (cap) {
            0 => .butt,
            1 => .round,
            2 => .square,
            else => state.line_cap,
        };
        return;
    }
    if (std.mem.eql(u8, op, "j") and operands.len >= 1) {
        const join = operands[operands.len - 1].asInteger() orelse return;
        state.line_join = switch (join) {
            0 => .miter,
            1 => .round,
            2 => .bevel,
            else => state.line_join,
        };
        return;
    }
    if (std.mem.eql(u8, op, "M") and operands.len >= 1) {
        state.miter_limit = @max(1.0, numericObjectValue(&operands[operands.len - 1]) orelse state.miter_limit);
        return;
    }
    if (std.mem.eql(u8, op, "d") and operands.len >= 2) {
        const dash_array_obj = &operands[operands.len - 2];
        const phase = numericObjectValue(&operands[operands.len - 1]) orelse 0;
        if (dash_array_obj.* != .array) return;
        current_dash.clearRetainingCapacity();
        for (dash_array_obj.array) |item| {
            const value = numericObjectValue(&item) orelse continue;
            if (value > 0) try current_dash.append(alloc, value);
        }
        dash_phase.* = phase;
        return;
    }
    if (std.mem.eql(u8, op, "m") and operands.len >= 2) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "l") and operands.len >= 2) {
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "c") and operands.len >= 6 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 6]) orelse 0,
            numericObjectValue(&operands[operands.len - 5]) orelse 0,
        );
        const c2 = applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 4]) orelse 0,
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
        );
        const end = applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "v") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = start;
        const c2 = applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 4]) orelse 0,
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
        );
        const end = applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "y") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 4]) orelse 0,
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
        );
        const end = applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
        const c2 = end;
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "h")) {
        current_path_closed.* = true;
        return;
    }
    if (std.mem.eql(u8, op, "re") and operands.len >= 4) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = true;
        const x = numericObjectValue(&operands[operands.len - 4]) orelse 0;
        const y = numericObjectValue(&operands[operands.len - 3]) orelse 0;
        const w = numericObjectValue(&operands[operands.len - 2]) orelse 0;
        const h = numericObjectValue(&operands[operands.len - 1]) orelse 0;
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y + h));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y + h));
        return;
    }
    if (std.mem.eql(u8, op, "W") or std.mem.eql(u8, op, "W*")) {
        if (current_path.items.len >= 2) {
            const next_clip = pathBounds(current_path.items);
            state.clip_box = if (state.clip_box) |current_clip|
                intersectPageBoxes(current_clip, next_clip)
            else
                next_clip;
            if (current_clip_points.items.len == 0) {
                try current_clip_points.appendSlice(alloc, current_path.items);
                current_clip_fill_rule.* = if (std.mem.eql(u8, op, "W*")) .even_odd else .nonzero;
            } else {
                if (try tryIntersectRectClipPoints(alloc, current_clip_points.items, current_path.items)) |intersection| {
                    defer alloc.free(intersection);
                    current_clip_points.clearRetainingCapacity();
                    try current_clip_points.appendSlice(alloc, intersection);
                    current_clip_fill_rule.* = .nonzero;
                } else {
                    current_clip_points.clearRetainingCapacity();
                }
            }
        }
        return;
    }
    if (std.mem.eql(u8, op, "f") or std.mem.eql(u8, op, "F") or std.mem.eql(u8, op, "f*")) {
        if (current_path.items.len >= 3) {
            const order = paint_order.*;
            try emitPolygonShapeRun(
                alloc,
                out,
                order,
                state.*,
                current_path.items,
                .fill,
                true,
                if (std.mem.eql(u8, op, "f*")) .even_odd else .nonzero,
                current_dash.items,
                dash_phase.*,
                current_clip_points.items,
                current_clip_fill_rule.*,
            );
            paint_order.* += 1;
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "S") or std.mem.eql(u8, op, "s")) {
        if (std.mem.eql(u8, op, "s")) current_path_closed.* = true;
        if (current_path.items.len >= 2) {
            const order = paint_order.*;
            try emitPolygonShapeRun(alloc, out, order, state.*, current_path.items, .stroke, current_path_closed.*, .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*);
            paint_order.* += 1;
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "B") or std.mem.eql(u8, op, "B*") or std.mem.eql(u8, op, "b") or std.mem.eql(u8, op, "b*")) {
        const closed = current_path_closed.* or std.mem.eql(u8, op, "b") or std.mem.eql(u8, op, "b*");
        if (current_path.items.len >= 3) {
            const order = paint_order.*;
            try emitPolygonShapeRun(
                alloc,
                out,
                order,
                state.*,
                current_path.items,
                .fill,
                true,
                if (std.mem.eql(u8, op, "B*") or std.mem.eql(u8, op, "b*")) .even_odd else .nonzero,
                current_dash.items,
                dash_phase.*,
                current_clip_points.items,
                current_clip_fill_rule.*,
            );
            try emitPolygonShapeRun(alloc, out, order, state.*, current_path.items, .stroke, closed, .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*);
            paint_order.* += 1;
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "n")) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
    }
}

fn applyShadingOperator(
    alloc: Allocator,
    out: *std.ArrayList(ShadingRun),
    state: *GraphicsState,
    stack: *std.ArrayList(ImageStackEntry),
    current_path: *std.ArrayList([2]f64),
    current_path_closed: *bool,
    current_clip_points: *std.ArrayList([2]f64),
    current_clip_fill_rule: *FillRule,
    shadings: []const PageShading,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    paint_order: *usize,
    next_group_id: *u32,
    op: []const u8,
    operands: []const syntax.Object,
) anyerror!void {
    if (std.mem.eql(u8, op, "q")) {
        try stack.append(alloc, .{
            .state = state.*,
            .clip_points = try alloc.dupe([2]f64, current_clip_points.items),
            .clip_fill_rule = current_clip_fill_rule.*,
        });
        return;
    }
    if (std.mem.eql(u8, op, "Q")) {
        if (stack.items.len > 0) {
            var entry = stack.pop().?;
            state.* = entry.state;
            current_clip_points.clearRetainingCapacity();
            try current_clip_points.appendSlice(alloc, entry.clip_points);
            current_clip_fill_rule.* = entry.clip_fill_rule;
            entry.deinit(alloc);
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "cm")) {
        if (operands.len >= 6) {
            const m = GraphicsMatrix{
                .a = numericObjectValue(&operands[operands.len - 6]) orelse 1,
                .b = numericObjectValue(&operands[operands.len - 5]) orelse 0,
                .c = numericObjectValue(&operands[operands.len - 4]) orelse 0,
                .d = numericObjectValue(&operands[operands.len - 3]) orelse 1,
                .e = numericObjectValue(&operands[operands.len - 2]) orelse 0,
                .f = numericObjectValue(&operands[operands.len - 1]) orelse 0,
            };
            state.matrix = multiplyGraphicsMatrix(state.matrix, m);
        }
        return;
    }
    if (std.mem.eql(u8, op, "gs") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        if (findExtGState(gstates, operands[operands.len - 1].name)) |gstate| {
            state.fill_alpha = gstate.fill_alpha;
            state.stroke_alpha = gstate.stroke_alpha;
            state.blend_mode = gstate.blend_mode;
        }
        return;
    }
    if (std.mem.eql(u8, op, "m") and operands.len >= 2) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "l") and operands.len >= 2) {
        try current_path.append(alloc, applyMatrixToPoint(
            state.matrix,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        ));
        return;
    }
    if (std.mem.eql(u8, op, "h")) {
        current_path_closed.* = true;
        return;
    }
    if (std.mem.eql(u8, op, "re") and operands.len >= 4) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = true;
        const x = numericObjectValue(&operands[operands.len - 4]) orelse 0;
        const y = numericObjectValue(&operands[operands.len - 3]) orelse 0;
        const w = numericObjectValue(&operands[operands.len - 2]) orelse 0;
        const h = numericObjectValue(&operands[operands.len - 1]) orelse 0;
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y + h));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y + h));
        return;
    }
    if (std.mem.eql(u8, op, "W") or std.mem.eql(u8, op, "W*")) {
        if (current_path.items.len >= 2) {
            const next_clip = pathBounds(current_path.items);
            state.clip_box = if (state.clip_box) |current_clip|
                intersectPageBoxes(current_clip, next_clip)
            else
                next_clip;
            if (current_clip_points.items.len == 0) {
                try current_clip_points.appendSlice(alloc, current_path.items);
                current_clip_fill_rule.* = if (std.mem.eql(u8, op, "W*")) .even_odd else .nonzero;
            } else {
                if (try tryIntersectRectClipPoints(alloc, current_clip_points.items, current_path.items)) |intersection| {
                    defer alloc.free(intersection);
                    current_clip_points.clearRetainingCapacity();
                    try current_clip_points.appendSlice(alloc, intersection);
                    current_clip_fill_rule.* = .nonzero;
                } else {
                    current_clip_points.clearRetainingCapacity();
                }
            }
        }
        return;
    }
    if (std.mem.eql(u8, op, "n")) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "Do")) {
        if (operands.len == 0 or operands[operands.len - 1] != .name) return;
        const form = findPageForm(forms, operands[operands.len - 1].name) orelse return;
        var nested_state = buildFormGraphicsState(state.*, form);
        if (form.transparency_group) {
            nested_state.group_parent_id = state.group_id;
            nested_state.group_id = next_group_id.*;
            nested_state.group_isolated = form.group_isolated;
            nested_state.group_knockout = form.group_knockout;
            next_group_id.* += 1;
        }
        try extractShadingRunsFromContentAppendWithState(alloc, out, form.content, form.shadings, form.gstates, form.forms, nested_state, current_clip_points.items, current_clip_fill_rule.*, paint_order, next_group_id);
        return;
    }
    if (std.mem.eql(u8, op, "sh") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        const name = operands[operands.len - 1].name;
        for (shadings) |shading| {
            if (!std.mem.eql(u8, shading.name, name)) continue;
            const p0 = applyMatrixToPoint(state.matrix, shading.x0, shading.y0);
            const p1 = applyMatrixToPoint(state.matrix, shading.x1, shading.y1);
            try out.append(alloc, .{
                .kind = shading.kind,
                .paint_order = paint_order.*,
                .blend_mode = state.blend_mode,
                .group_id = state.group_id,
                .group_parent_id = state.group_parent_id,
                .group_isolated = state.group_isolated,
                .group_knockout = state.group_knockout,
                .clip_box = state.clip_box,
                .clip_points = if (current_clip_points.items.len > 0) try alloc.dupe([2]f64, current_clip_points.items) else null,
                .clip_fill_rule = current_clip_fill_rule.*,
                .x0 = p0[0],
                .y0 = p0[1],
                .r0 = shading.r0 * graphicsStrokeScale(state.matrix),
                .x1 = p1[0],
                .y1 = p1[1],
                .r1 = shading.r1 * graphicsStrokeScale(state.matrix),
                .c0 = colorWithAlpha(shading.c0, state.fill_alpha),
                .c1 = colorWithAlpha(shading.c1, state.fill_alpha),
                .extend_start = shading.extend_start,
                .extend_end = shading.extend_end,
            });
            paint_order.* += 1;
            return;
        }
    }
}

fn applyPatternOperator(
    alloc: Allocator,
    out: *std.ArrayList(PatternRun),
    state: *GraphicsState,
    stack: *std.ArrayList(ShapeStackEntry),
    current_path: *std.ArrayList([2]f64),
    current_path_closed: *bool,
    current_dash: *std.ArrayList(f64),
    dash_phase: *f64,
    current_clip_points: *std.ArrayList([2]f64),
    current_clip_fill_rule: *@FieldType(ShapeRun, "fill_rule"),
    patterns: []const PagePattern,
    gstates: []const PageExtGState,
    forms: []const PageForm,
    paint_order: *usize,
    next_group_id: *u32,
    op: []const u8,
    operands: []const syntax.Object,
) anyerror!void {
    if (std.mem.eql(u8, op, "q")) {
        try stack.append(alloc, .{
            .state = state.*,
            .dash_phase = dash_phase.*,
            .dash_array = try alloc.dupe(f64, current_dash.items),
            .clip_points = try alloc.dupe([2]f64, current_clip_points.items),
            .clip_fill_rule = current_clip_fill_rule.*,
        });
        return;
    }
    if (std.mem.eql(u8, op, "Q")) {
        if (stack.items.len > 0) {
            var entry = stack.pop().?;
            state.* = entry.state;
            dash_phase.* = entry.dash_phase;
            current_dash.clearRetainingCapacity();
            try current_dash.appendSlice(alloc, entry.dash_array);
            current_clip_points.clearRetainingCapacity();
            try current_clip_points.appendSlice(alloc, entry.clip_points);
            current_clip_fill_rule.* = entry.clip_fill_rule;
            entry.deinit(alloc);
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "cm")) {
        if (operands.len >= 6) {
            const m = GraphicsMatrix{
                .a = numericObjectValue(&operands[operands.len - 6]) orelse 1,
                .b = numericObjectValue(&operands[operands.len - 5]) orelse 0,
                .c = numericObjectValue(&operands[operands.len - 4]) orelse 0,
                .d = numericObjectValue(&operands[operands.len - 3]) orelse 1,
                .e = numericObjectValue(&operands[operands.len - 2]) orelse 0,
                .f = numericObjectValue(&operands[operands.len - 1]) orelse 0,
            };
            state.matrix = multiplyGraphicsMatrix(state.matrix, m);
        }
        return;
    }
    if (std.mem.eql(u8, op, "gs") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        if (findExtGState(gstates, operands[operands.len - 1].name)) |gstate| {
            state.fill_alpha = gstate.fill_alpha;
            state.stroke_alpha = gstate.stroke_alpha;
            state.blend_mode = gstate.blend_mode;
        }
        return;
    }
    if (std.mem.eql(u8, op, "Do")) {
        if (operands.len == 0 or operands[operands.len - 1] != .name) return;
        const form = findPageForm(forms, operands[operands.len - 1].name) orelse return;
        var nested_state = buildFormGraphicsState(state.*, form);
        if (form.transparency_group) {
            nested_state.group_parent_id = state.group_id;
            nested_state.group_id = next_group_id.*;
            nested_state.group_isolated = form.group_isolated;
            nested_state.group_knockout = form.group_knockout;
            next_group_id.* += 1;
        }
        try extractPatternRunsFromContentAppendWithState(alloc, out, form.content, form.patterns, form.gstates, form.forms, nested_state, current_clip_points.items, current_clip_fill_rule.*, current_dash.items, dash_phase.*, paint_order, next_group_id);
        return;
    }
    if (std.mem.eql(u8, op, "rg") and operands.len >= 3) {
        state.fill_color_space = "DeviceRGB";
        state.fill_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "RG") and operands.len >= 3) {
        state.stroke_color_space = "DeviceRGB";
        state.stroke_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "g") and operands.len >= 1) {
        state.fill_color_space = "DeviceGray";
        state.fill_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "G") and operands.len >= 1) {
        state.stroke_color_space = "DeviceGray";
        state.stroke_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "k") and operands.len >= 4) {
        state.fill_color_space = "DeviceCMYK";
        state.fill_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "K") and operands.len >= 4) {
        state.stroke_color_space = "DeviceCMYK";
        state.stroke_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "cs") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        state.fill_color_space = operands[operands.len - 1].name;
        if (!std.mem.eql(u8, state.fill_color_space, "Pattern")) state.fill_pattern_name = null;
        return;
    }
    if (std.mem.eql(u8, op, "CS") and operands.len >= 1 and operands[operands.len - 1] == .name) {
        state.stroke_color_space = operands[operands.len - 1].name;
        if (!std.mem.eql(u8, state.stroke_color_space, "Pattern")) state.stroke_pattern_name = null;
        return;
    }
    if ((std.mem.eql(u8, op, "sc") or std.mem.eql(u8, op, "scn")) and operands.len >= 1) {
        if (std.mem.eql(u8, state.fill_color_space, "Pattern") and operands[operands.len - 1] == .name) {
            state.fill_pattern_name = operands[operands.len - 1].name;
            if (decodePatternBaseColorOperands(operands[0 .. operands.len - 1])) |color| state.fill_color = color;
        } else if (!std.mem.eql(u8, state.fill_color_space, "Pattern")) {
            state.fill_pattern_name = null;
        }
        return;
    }
    if ((std.mem.eql(u8, op, "SC") or std.mem.eql(u8, op, "SCN")) and operands.len >= 1) {
        if (std.mem.eql(u8, state.stroke_color_space, "Pattern") and operands[operands.len - 1] == .name) {
            state.stroke_pattern_name = operands[operands.len - 1].name;
            if (decodePatternBaseColorOperands(operands[0 .. operands.len - 1])) |color| state.stroke_color = color;
        } else if (!std.mem.eql(u8, state.stroke_color_space, "Pattern")) {
            state.stroke_pattern_name = null;
        }
        return;
    }
    if (std.mem.eql(u8, op, "w") and operands.len >= 1) {
        state.stroke_width = @max(0.1, numericObjectValue(&operands[operands.len - 1]) orelse state.stroke_width);
        return;
    }
    if (std.mem.eql(u8, op, "J") and operands.len >= 1) {
        const cap = operands[operands.len - 1].asInteger() orelse return;
        state.line_cap = switch (cap) {
            0 => .butt,
            1 => .round,
            2 => .square,
            else => state.line_cap,
        };
        return;
    }
    if (std.mem.eql(u8, op, "j") and operands.len >= 1) {
        const join = operands[operands.len - 1].asInteger() orelse return;
        state.line_join = switch (join) {
            0 => .miter,
            1 => .round,
            2 => .bevel,
            else => state.line_join,
        };
        return;
    }
    if (std.mem.eql(u8, op, "M") and operands.len >= 1) {
        state.miter_limit = @max(1.0, numericObjectValue(&operands[operands.len - 1]) orelse state.miter_limit);
        return;
    }
    if (std.mem.eql(u8, op, "d") and operands.len >= 2) {
        const dash_array_obj = &operands[operands.len - 2];
        const phase = numericObjectValue(&operands[operands.len - 1]) orelse 0;
        if (dash_array_obj.* != .array) return;
        current_dash.clearRetainingCapacity();
        for (dash_array_obj.array) |item| {
            const value = numericObjectValue(&item) orelse continue;
            if (value > 0) try current_dash.append(alloc, value);
        }
        dash_phase.* = phase;
        return;
    }
    if (std.mem.eql(u8, op, "m") and operands.len >= 2) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0));
        return;
    }
    if (std.mem.eql(u8, op, "l") and operands.len >= 2) {
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0));
        return;
    }
    if (std.mem.eql(u8, op, "c") and operands.len >= 6 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 6]) orelse 0, numericObjectValue(&operands[operands.len - 5]) orelse 0);
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "v") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c2 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, start, c2, end);
        return;
    }
    if (std.mem.eql(u8, op, "y") and operands.len >= 4 and current_path.items.len > 0) {
        const start = current_path.items[current_path.items.len - 1];
        const c1 = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 4]) orelse 0, numericObjectValue(&operands[operands.len - 3]) orelse 0);
        const end = applyMatrixToPoint(state.matrix, numericObjectValue(&operands[operands.len - 2]) orelse 0, numericObjectValue(&operands[operands.len - 1]) orelse 0);
        try appendFlattenedCubicBezier(alloc, current_path, start, c1, end, end);
        return;
    }
    if (std.mem.eql(u8, op, "h")) {
        current_path_closed.* = true;
        return;
    }
    if (std.mem.eql(u8, op, "re") and operands.len >= 4) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = true;
        const x = numericObjectValue(&operands[operands.len - 4]) orelse 0;
        const y = numericObjectValue(&operands[operands.len - 3]) orelse 0;
        const w = numericObjectValue(&operands[operands.len - 2]) orelse 0;
        const h = numericObjectValue(&operands[operands.len - 1]) orelse 0;
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x + w, y + h));
        try current_path.append(alloc, applyMatrixToPoint(state.matrix, x, y + h));
        return;
    }
    if (std.mem.eql(u8, op, "W") or std.mem.eql(u8, op, "W*")) {
        if (current_path.items.len >= 2) {
            const next_clip = pathBounds(current_path.items);
            state.clip_box = if (state.clip_box) |current_clip| intersectPageBoxes(current_clip, next_clip) else next_clip;
            if (current_clip_points.items.len == 0) {
                try current_clip_points.appendSlice(alloc, current_path.items);
                current_clip_fill_rule.* = if (std.mem.eql(u8, op, "W*")) .even_odd else .nonzero;
            } else {
                if (try tryIntersectRectClipPoints(alloc, current_clip_points.items, current_path.items)) |intersection| {
                    defer alloc.free(intersection);
                    current_clip_points.clearRetainingCapacity();
                    try current_clip_points.appendSlice(alloc, intersection);
                    current_clip_fill_rule.* = .nonzero;
                } else {
                    current_clip_points.clearRetainingCapacity();
                }
            }
        }
        return;
    }
    if (std.mem.eql(u8, op, "f") or std.mem.eql(u8, op, "F") or std.mem.eql(u8, op, "f*")) {
        if (current_path.items.len >= 3) {
            if (state.fill_pattern_name) |pattern_name| {
                const pattern = findPagePattern(patterns, pattern_name) orelse {
                    current_path.clearRetainingCapacity();
                    current_path_closed.* = false;
                    return;
                };
                if (pattern.kind == .tiling) {
                    try out.append(alloc, try buildPatternRunAlloc(alloc, pattern, state.*, current_path.items, .fill, true, if (std.mem.eql(u8, op, "f*")) .even_odd else .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*, paint_order.*));
                } else if (pattern.kind == .shading) {
                    try out.append(alloc, try buildShadingPatternRunAlloc(alloc, pattern, state.*, current_path.items, .fill, true, if (std.mem.eql(u8, op, "f*")) .even_odd else .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*, paint_order.*));
                }
                paint_order.* += 1;
            }
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "S") or std.mem.eql(u8, op, "s")) {
        if (std.mem.eql(u8, op, "s")) current_path_closed.* = true;
        if (current_path.items.len >= 2) {
            if (state.stroke_pattern_name) |pattern_name| {
                const pattern = findPagePattern(patterns, pattern_name) orelse {
                    current_path.clearRetainingCapacity();
                    current_path_closed.* = false;
                    return;
                };
                try out.append(alloc, try buildPatternRunAlloc(alloc, pattern, state.*, current_path.items, .stroke, current_path_closed.*, .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*, paint_order.*));
                paint_order.* += 1;
            }
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "B") or std.mem.eql(u8, op, "B*") or std.mem.eql(u8, op, "b") or std.mem.eql(u8, op, "b*")) {
        const closed = current_path_closed.* or std.mem.eql(u8, op, "b") or std.mem.eql(u8, op, "b*");
        if (current_path.items.len >= 3) {
            if (state.fill_pattern_name) |fill_pattern_name| {
                if (findPagePattern(patterns, fill_pattern_name)) |pattern| {
                    if (pattern.kind == .tiling) {
                        try out.append(alloc, try buildPatternRunAlloc(alloc, pattern, state.*, current_path.items, .fill, true, if (std.mem.eql(u8, op, "B*") or std.mem.eql(u8, op, "b*")) .even_odd else .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*, paint_order.*));
                    } else if (pattern.kind == .shading) {
                        try out.append(alloc, try buildShadingPatternRunAlloc(alloc, pattern, state.*, current_path.items, .fill, true, if (std.mem.eql(u8, op, "B*") or std.mem.eql(u8, op, "b*")) .even_odd else .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*, paint_order.*));
                    }
                }
            }
        }
        if (current_path.items.len >= 2) {
            if (state.stroke_pattern_name) |stroke_pattern_name| {
                if (findPagePattern(patterns, stroke_pattern_name)) |pattern| {
                    try out.append(alloc, try buildPatternRunAlloc(alloc, pattern, state.*, current_path.items, .stroke, closed, .nonzero, current_dash.items, dash_phase.*, current_clip_points.items, current_clip_fill_rule.*, paint_order.*));
                }
            }
        }
        if ((state.fill_pattern_name != null and current_path.items.len >= 3) or (state.stroke_pattern_name != null and current_path.items.len >= 2)) {
            paint_order.* += 1;
        }
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
        return;
    }
    if (std.mem.eql(u8, op, "n")) {
        current_path.clearRetainingCapacity();
        current_path_closed.* = false;
    }
}

fn decodeShapeColorOperands(color_space: []const u8, operands: []const syntax.Object) !?[4]u8 {
    if (std.mem.eql(u8, color_space, "DeviceGray")) {
        if (operands.len < 1) return null;
        return grayColor(numericObjectValue(&operands[operands.len - 1]) orelse 0);
    }
    if (std.mem.eql(u8, color_space, "DeviceRGB")) {
        if (operands.len < 3) return null;
        return rgbColor(
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
    }
    if (std.mem.eql(u8, color_space, "DeviceCMYK")) {
        if (operands.len < 4) return null;
        return cmykColor(
            numericObjectValue(&operands[operands.len - 4]) orelse 0,
            numericObjectValue(&operands[operands.len - 3]) orelse 0,
            numericObjectValue(&operands[operands.len - 2]) orelse 0,
            numericObjectValue(&operands[operands.len - 1]) orelse 0,
        );
    }
    return null;
}

fn emitPatternShadingRun(
    alloc: Allocator,
    out: *std.ArrayList(ShadingRun),
    paint_order: usize,
    state: GraphicsState,
    path_points: []const [2]f64,
    path_fill_rule: FillRule,
    clip_points: []const [2]f64,
    clip_fill_rule: FillRule,
    p0: [2]f64,
    p1: [2]f64,
    shading: PageShading,
    scale: f64,
) !void {
    try out.append(alloc, .{
        .kind = shading.kind,
        .paint_order = paint_order,
        .blend_mode = state.blend_mode,
        .group_id = state.group_id,
        .group_parent_id = state.group_parent_id,
        .group_isolated = state.group_isolated,
        .clip_box = if (state.clip_box) |clip_box| intersectPageBoxes(clip_box, pathBounds(path_points)) else pathBounds(path_points),
        .clip_points = if (clip_points.len > 0)
            try alloc.dupe([2]f64, clip_points)
        else
            try alloc.dupe([2]f64, path_points),
        .clip_fill_rule = if (clip_points.len > 0) clip_fill_rule else path_fill_rule,
        .x0 = p0[0],
        .y0 = p0[1],
        .r0 = shading.r0 * scale,
        .x1 = p1[0],
        .y1 = p1[1],
        .r1 = shading.r1 * scale,
        .c0 = colorWithAlpha(shading.c0, state.fill_alpha),
        .c1 = colorWithAlpha(shading.c1, state.fill_alpha),
        .extend_start = shading.extend_start,
        .extend_end = shading.extend_end,
    });
}

fn decodePatternBaseColorOperands(operands: []const syntax.Object) ?[4]u8 {
    return switch (operands.len) {
        1 => grayColor(numericObjectValue(&operands[0]) orelse return null),
        3 => rgbColor(
            numericObjectValue(&operands[0]) orelse return null,
            numericObjectValue(&operands[1]) orelse return null,
            numericObjectValue(&operands[2]) orelse return null,
        ),
        4 => cmykColor(
            numericObjectValue(&operands[0]) orelse return null,
            numericObjectValue(&operands[1]) orelse return null,
            numericObjectValue(&operands[2]) orelse return null,
            numericObjectValue(&operands[3]) orelse return null,
        ),
        else => null,
    };
}

fn appendTextOperand(
    alloc: Allocator,
    out: *std.ArrayList(u8),
    state: *TextExtractionState,
    fonts: []const PageFont,
    obj: *const syntax.Object,
) !void {
    if (obj.* != .string) return;
    try appendDecodedString(alloc, out, state, fonts, obj.string);
}

fn appendDecodedString(
    alloc: Allocator,
    out: *std.ArrayList(u8),
    state: *TextExtractionState,
    fonts: []const PageFont,
    raw: []const u8,
) !void {
    const decoded = if (state.current_font_index) |font_idx|
        try fonts[font_idx].decoder.decodeAlloc(alloc, raw)
    else
        try text_encoding.pdfDocDecodeAlloc(alloc, raw);
    defer alloc.free(decoded);
    try out.appendSlice(alloc, decoded);
}

fn appendTextRunOperand(
    alloc: Allocator,
    out: *std.ArrayList(TextRun),
    state: *TextRunState,
    current_clip_points: []const [2]f64,
    current_clip_fill_rule: @FieldType(ShapeRun, "fill_rule"),
    fonts: []const PageFont,
    paint_order: usize,
    obj: *const syntax.Object,
) !void {
    if (obj.* != .string) return;
    try appendTextRunDecodedString(alloc, out, state, current_clip_points, current_clip_fill_rule, fonts, paint_order, obj.string);
}

fn appendTextRunDecodedString(
    alloc: Allocator,
    out: *std.ArrayList(TextRun),
    state: *TextRunState,
    current_clip_points: []const [2]f64,
    current_clip_fill_rule: @FieldType(ShapeRun, "fill_rule"),
    fonts: []const PageFont,
    paint_order: usize,
    raw: []const u8,
) !void {
    const decoded = if (state.current_font_index) |font_idx|
        try fonts[font_idx].decoder.decodeAlloc(alloc, raw)
    else
        try text_encoding.pdfDocDecodeAlloc(alloc, raw);
    defer alloc.free(decoded);
    if (decoded.len == 0) return;

    const position = applyMatrixToPoint(
        state.matrix,
        state.x + state.rise * state.text_c,
        state.y + state.rise * state.text_d,
    );
    const basis_x = applyMatrixToVector(state.matrix, state.text_a, state.text_b);
    const basis_y = applyMatrixToVector(state.matrix, state.text_c, state.text_d);
    const metrics = if (state.current_font_index) |font_idx|
        measureFontVerticalMetrics(fonts, font_idx, state.*)
    else
        defaultTextVerticalMetrics(state.*);
    const advance_width = if (state.current_font_index) |font_idx|
        try measureFontAdvanceAlloc(alloc, fonts, font_idx, raw, decoded, state.*)
    else
        estimateDecodedAdvance(decoded, state.*);
    if (state.render_mode != 3) {
        const vectorizable = if (state.current_font_index) |font_idx|
            fonts[font_idx].type3 != null or
                fonts[font_idx].type1 != null or
                fonts[font_idx].truetype != null or
                fonts[font_idx].cff_otf != null
        else
            false;
        try out.append(alloc, .{
            .text = try alloc.dupe(u8, decoded),
            .raw_text = try alloc.dupe(u8, raw),
            .font_index = if (state.current_font_index) |idx| @intCast(idx) else null,
            .vectorizable = vectorizable,
            .x = position[0],
            .y = position[1],
            .font_size = state.font_size,
            .a = basis_x[0],
            .b = basis_x[1],
            .c = basis_y[0],
            .d = basis_y[1],
            .alpha = state.alpha,
            .stroke_alpha = state.stroke_alpha,
            .render_mode = state.render_mode,
            .fill_color = state.fill_color,
            .stroke_color = state.stroke_color,
            .stroke_width = state.stroke_width,
            .horizontal_scale = state.horizontal_scale,
            .char_spacing = state.char_spacing,
            .word_spacing = state.word_spacing,
            .advance_width = advance_width,
            .ascent = metrics.ascent,
            .descent = metrics.descent,
            .paint_order = paint_order,
            .blend_mode = state.blend_mode,
            .group_id = state.group_id,
            .group_parent_id = state.group_parent_id,
            .group_isolated = state.group_isolated,
            .group_knockout = state.group_knockout,
            .fill_pattern_name = state.fill_pattern_name,
            .stroke_pattern_name = state.stroke_pattern_name,
            .clip_box = state.clip_box,
            .clip_points = if (current_clip_points.len > 0) try alloc.dupe([2]f64, current_clip_points) else null,
            .clip_fill_rule = current_clip_fill_rule,
        });
    }

    state.x += advance_width * state.text_a;
    state.y += advance_width * state.text_b;
}

fn estimateDecodedAdvance(decoded: []const u8, state: TextRunState) f64 {
    var advance: f64 = 0;
    var it = std.unicode.Utf8View.init(decoded) catch {
        for (decoded) |b| {
            advance += estimateCodepointAdvance(if (b == ' ') ' ' else 0xfffd, state);
        }
        return advance;
    };
    var iter = it.iterator();
    while (iter.nextCodepoint()) |cp| {
        advance += estimateCodepointAdvance(cp, state);
    }
    return advance;
}

fn estimateCodepointAdvance(cp: u21, state: TextRunState) f64 {
    const glyph = state.font_size * 0.6;
    const spacing = state.char_spacing + if (cp == ' ') state.word_spacing else 0.0;
    return (glyph + spacing) * state.horizontal_scale;
}

fn defaultTextVerticalMetrics(state: TextRunState) TextVerticalMetrics {
    return .{
        .ascent = state.font_size * 0.8,
        .descent = state.font_size * 0.2,
    };
}

fn measureFontVerticalMetrics(fonts: []const PageFont, font_idx: usize, state: TextRunState) TextVerticalMetrics {
    if (font_idx >= fonts.len) return defaultTextVerticalMetrics(state);
    const font = fonts[font_idx];
    if (font.truetype) |truetype| {
        const hhea = truetype.font.hhea() catch return defaultTextVerticalMetrics(state);
        const scale = if (truetype.units_per_em == 0) 1.0 else state.font_size / @as(f64, @floatFromInt(truetype.units_per_em));
        return .{
            .ascent = @max(scale, @as(f64, @floatFromInt(@max(hhea.ascender, 0))) * scale),
            .descent = @max(0.0, @as(f64, @floatFromInt(@max(-hhea.descender, 0))) * scale),
        };
    }
    if (font.cff_otf) |cff_otf| {
        const hhea = cff_otf.sfnt.hhea() catch return defaultTextVerticalMetrics(state);
        const scale = if (cff_otf.units_per_em == 0) 1.0 else state.font_size / @as(f64, @floatFromInt(cff_otf.units_per_em));
        return .{
            .ascent = @max(scale, @as(f64, @floatFromInt(@max(hhea.ascender, 0))) * scale),
            .descent = @max(0.0, @as(f64, @floatFromInt(@max(-hhea.descender, 0))) * scale),
        };
    }
    return defaultTextVerticalMetrics(state);
}

fn measureFontAdvanceAlloc(
    alloc: Allocator,
    fonts: []const PageFont,
    font_idx: usize,
    raw: []const u8,
    decoded: []const u8,
    state: TextRunState,
) !f64 {
    if (font_idx >= fonts.len) return estimateDecodedAdvance(decoded, state);
    const font = fonts[font_idx];
    if (font.type3) |type3| {
        var advance: f64 = 0;
        for (raw) |code| {
            const glyph = type3.glyphForCode(code);
            advance += (if (glyph) |g| g.advance_x * state.font_size else state.font_size * 0.6) * state.horizontal_scale;
            advance += state.char_spacing;
            if (code == ' ') advance += state.word_spacing;
        }
        return advance;
    }
    if (font.type1) |type1| {
        var advance: f64 = 0;
        const scale = state.font_size / 1000.0;
        for (raw) |code| {
            const glyph = type1.glyphForCode(code);
            advance += (if (glyph) |g| g.advance * scale else state.font_size * 0.6) * state.horizontal_scale;
            advance += state.char_spacing;
            if (code == ' ') advance += state.word_spacing;
        }
        return advance;
    }
    if (font.truetype) |truetype| {
        return try measureSfntAdvanceAlloc(alloc, decoded, state, truetype.font, truetype.units_per_em);
    }
    if (font.cff_otf) |cff_otf| {
        return try measureSfntAdvanceAlloc(alloc, decoded, state, cff_otf.sfnt, cff_otf.units_per_em);
    }
    return estimateDecodedAdvance(decoded, state);
}

fn measureSfntAdvanceAlloc(
    alloc: Allocator,
    decoded: []const u8,
    state: TextRunState,
    font: font_lib.sfnt.Font,
    units_per_em: u16,
) !f64 {
    _ = alloc;
    const scale = if (units_per_em == 0) 1.0 else state.font_size / @as(f64, @floatFromInt(units_per_em));
    var advance: f64 = 0;
    var view = std.unicode.Utf8View.init(decoded) catch return estimateDecodedAdvance(decoded, state);
    var iter = view.iterator();
    var prev_glyph: ?u16 = null;
    while (iter.nextCodepoint()) |cp| {
        const glyph_index = try font.cmapGlyphIndex(cp) orelse {
            advance += estimateCodepointAdvance(cp, state);
            prev_glyph = null;
            continue;
        };
        if (prev_glyph) |left| {
            const kern = try font.horizontalKerning(left, glyph_index);
            advance += @as(f64, @floatFromInt(kern)) * scale * state.horizontal_scale;
        }
        const width = try font.advanceWidth(glyph_index);
        advance += (@as(f64, @floatFromInt(width)) * scale + state.char_spacing + if (cp == ' ') state.word_spacing else 0.0) * state.horizontal_scale;
        prev_glyph = glyph_index;
    }
    return advance;
}

fn appendNewline(alloc: Allocator, out: *std.ArrayList(u8)) !void {
    if (out.items.len == 0) return;
    if (out.items[out.items.len - 1] == '\n') return;
    try out.append(alloc, '\n');
}

fn isContentObjectStartKeyword(keyword: []const u8) bool {
    return std.mem.eql(u8, keyword, "[") or std.mem.eql(u8, keyword, "<<");
}

fn contentMayContainShapePaintOperator(bytes: []const u8) bool {
    const operators = [_][]const u8{ "f", "F", "f*", "S", "s", "B", "B*", "b", "b*" };
    for (operators) |op| {
        if (contentMayContainOperator(bytes, op)) return true;
    }
    return false;
}

fn contentMayContainOperator(bytes: []const u8, operator: []const u8) bool {
    var index: usize = 0;
    while (std.mem.indexOfPos(u8, bytes, index, operator)) |match| {
        const end = match + operator.len;
        if ((match == 0 or isContentTokenBoundary(bytes[match - 1])) and
            (end == bytes.len or isContentTokenBoundary(bytes[end])))
        {
            return true;
        }
        index = match + 1;
    }
    return false;
}

fn isContentTokenBoundary(byte: u8) bool {
    return switch (byte) {
        '\x00', '\t', '\n', '\x0c', '\r', ' ', '<', '>', '(', ')', '[', ']', '{', '}', '/', '%' => true,
        else => false,
    };
}

fn numericObjectValue(obj: *const syntax.Object) ?f64 {
    return switch (obj.*) {
        .integer => |value| @floatFromInt(value),
        .real => |value| value,
        else => null,
    };
}

fn alphaByteFromNumber(alpha: f64) u8 {
    return floatChannel(alpha);
}

fn parseBlendModeObject(obj: *const syntax.Object) BlendMode {
    return switch (obj.*) {
        .name => parseBlendModeName(obj.name),
        .array => if (obj.array.len > 0) parseBlendModeObject(&obj.array[0]) else .normal,
        else => .normal,
    };
}

fn parseBlendModeName(name: []const u8) BlendMode {
    if (std.mem.eql(u8, name, "Multiply")) return .multiply;
    if (std.mem.eql(u8, name, "Screen")) return .screen;
    if (std.mem.eql(u8, name, "Overlay")) return .overlay;
    if (std.mem.eql(u8, name, "Darken")) return .darken;
    if (std.mem.eql(u8, name, "Lighten")) return .lighten;
    return .normal;
}

fn colorWithAlpha(color: [4]u8, alpha: u8) [4]u8 {
    return .{
        color[0],
        color[1],
        color[2],
        @intCast((@as(u16, color[3]) * @as(u16, alpha) + 127) / 255),
    };
}

fn findExtGState(gstates: []const PageExtGState, name: []const u8) ?PageExtGState {
    for (gstates) |gstate| {
        if (std.mem.eql(u8, gstate.name, name)) return gstate;
    }
    return null;
}

fn findPageForm(forms: []const PageForm, name: []const u8) ?*const PageForm {
    for (forms) |*form| {
        if (std.mem.eql(u8, form.name, name)) return form;
    }
    return null;
}

fn findPagePattern(patterns: []const PagePattern, name: []const u8) ?*const PagePattern {
    for (patterns) |*pattern| {
        if (std.mem.eql(u8, pattern.name, name)) return pattern;
    }
    return null;
}

fn buildFormGraphicsState(state: GraphicsState, form: *const PageForm) GraphicsState {
    var next = state;
    next.matrix = multiplyGraphicsMatrix(state.matrix, form.matrix);
    if (form.bbox) |bbox| {
        const transformed = transformPageBox(bbox, next.matrix);
        next.clip_box = if (next.clip_box) |current| intersectPageBoxes(current, transformed) else transformed;
    }
    return next;
}

fn buildFormTextState(state: TextRunState, form: *const PageForm) TextRunState {
    var next = TextRunState{
        .current_font_index = null,
        .font_size = state.font_size,
        .alpha = state.alpha,
        .stroke_alpha = state.stroke_alpha,
        .text_a = 1,
        .text_b = 0,
        .text_c = 0,
        .text_d = 1,
        .horizontal_scale = state.horizontal_scale,
        .char_spacing = state.char_spacing,
        .word_spacing = state.word_spacing,
        .rise = 0,
        .render_mode = state.render_mode,
        .fill_color = state.fill_color,
        .stroke_color = state.stroke_color,
        .stroke_width = state.stroke_width,
        .blend_mode = state.blend_mode,
        .group_id = state.group_id,
        .group_parent_id = state.group_parent_id,
        .group_isolated = state.group_isolated,
        .group_knockout = state.group_knockout,
        .fill_color_space = state.fill_color_space,
        .stroke_color_space = state.stroke_color_space,
        .fill_pattern_name = state.fill_pattern_name,
        .stroke_pattern_name = state.stroke_pattern_name,
        .x = 0,
        .y = 0,
        .line_x = 0,
        .line_y = 0,
        .leading = state.leading,
        .matrix = multiplyGraphicsMatrix(state.matrix, form.matrix),
        .clip_box = state.clip_box,
    };
    if (form.bbox) |bbox| {
        const transformed = transformPageBox(bbox, next.matrix);
        next.clip_box = if (next.clip_box) |current| intersectPageBoxes(current, transformed) else transformed;
    }
    return next;
}

fn buildPatternTextState(state: GraphicsState) TextRunState {
    return .{
        .alpha = state.fill_alpha,
        .stroke_alpha = state.stroke_alpha,
        .fill_color = state.fill_color,
        .stroke_color = state.stroke_color,
        .stroke_width = state.stroke_width,
        .blend_mode = state.blend_mode,
        .group_id = state.group_id,
        .group_parent_id = state.group_parent_id,
        .group_isolated = state.group_isolated,
        .group_knockout = state.group_knockout,
        .fill_color_space = state.fill_color_space,
        .stroke_color_space = state.stroke_color_space,
        .fill_pattern_name = null,
        .stroke_pattern_name = null,
    };
}

fn buildPatternGraphicsState(state: GraphicsState) GraphicsState {
    var next = state;
    next.matrix = .{};
    next.group_isolated = state.group_isolated;
    next.group_knockout = state.group_knockout;
    next.group_parent_id = state.group_parent_id;
    next.fill_pattern_name = null;
    next.stroke_pattern_name = null;
    next.clip_box = null;
    return next;
}

fn buildPatternRunFromShapeAlloc(
    alloc: Allocator,
    pattern: *const PagePattern,
    shape: ShapeRun,
) !PatternRun {
    if (pattern.kind == .shading) {
        const shading = pattern.shading orelse return error.UnsupportedPdfRendering;
        return .{
            .kind = shape.kind,
            .mode = .shading,
            .paint_order = shape.paint_order,
            .blend_mode = shape.blend_mode,
            .group_id = shape.group_id,
            .group_parent_id = shape.group_parent_id,
            .group_isolated = shape.group_isolated,
            .group_knockout = shape.group_knockout,
            .fill_rule = shape.fill_rule,
            .line_cap = shape.line_cap,
            .line_join = shape.line_join,
            .miter_limit = shape.miter_limit,
            .dash_array = if (shape.dash_array) |dash| try alloc.dupe(f64, dash) else null,
            .dash_phase = shape.dash_phase,
            .stroke_width = shape.stroke_width,
            .closed = shape.closed,
            .clip_box = shape.clip_box,
            .clip_points = if (shape.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
            .clip_fill_rule = shape.clip_fill_rule,
            .points = try alloc.dupe([2]f64, shape.points),
            .pattern_matrix = pattern.matrix,
            .pattern_bbox = pattern.bbox,
            .pattern_x_step = pattern.x_step,
            .pattern_y_step = pattern.y_step,
            .base_color = null,
            .shading = .{
                .kind = shading.kind,
                .paint_order = shape.paint_order,
                .blend_mode = shape.blend_mode,
                .group_id = shape.group_id,
                .group_parent_id = shape.group_parent_id,
                .group_isolated = shape.group_isolated,
                .group_knockout = shape.group_knockout,
                .clip_box = null,
                .clip_points = null,
                .clip_fill_rule = shape.fill_rule,
                .x0 = shading.x0,
                .y0 = shading.y0,
                .r0 = shading.r0,
                .x1 = shading.x1,
                .y1 = shading.y1,
                .r1 = shading.r1,
                .c0 = shading.c0,
                .c1 = shading.c1,
                .extend_start = shading.extend_start,
                .extend_end = shading.extend_end,
            },
        };
    }

    const state = GraphicsState{
        .fill_color = shape.color,
        .stroke_color = shape.color,
        .fill_alpha = shape.color[3],
        .stroke_alpha = shape.color[3],
        .blend_mode = shape.blend_mode,
        .group_id = shape.group_id,
        .group_parent_id = shape.group_parent_id,
        .group_isolated = shape.group_isolated,
        .group_knockout = shape.group_knockout,
        .line_cap = shape.line_cap,
        .line_join = shape.line_join,
        .miter_limit = shape.miter_limit,
        .stroke_width = shape.stroke_width,
        .clip_box = shape.clip_box,
    };
    return try buildPatternRunAlloc(
        alloc,
        pattern,
        state,
        shape.points,
        switch (shape.kind) {
            .fill => .fill,
            .stroke => .stroke,
        },
        shape.closed,
        shape.fill_rule,
        if (shape.dash_array) |dash| dash else &.{},
        shape.dash_phase,
        if (shape.clip_points) |clip| clip else &.{},
        shape.clip_fill_rule,
        shape.paint_order,
    );
}

fn buildPatternRunAlloc(
    alloc: Allocator,
    pattern: *const PagePattern,
    state: GraphicsState,
    points: []const [2]f64,
    kind: @FieldType(PatternRun, "kind"),
    closed: bool,
    fill_rule: FillRule,
    dash_array: []const f64,
    dash_phase: f64,
    clip_points: []const [2]f64,
    clip_fill_rule: FillRule,
    paint_order: usize,
) anyerror!PatternRun {
    var tile_paint_order: usize = 0;
    var tile_group_id: u32 = 1;

    var tile_text_list = std.ArrayList(TextRun).empty;
    defer tile_text_list.deinit(alloc);
    const text_state = buildPatternTextState(state);
    try extractTextRunsFromContentAppendWithState(alloc, &tile_text_list, pattern.content, pattern.fonts, pattern.gstates, pattern.forms, text_state, &.{}, .nonzero, &tile_paint_order, &tile_group_id);

    var tile_pattern_list = std.ArrayList(PatternRun).empty;
    defer tile_pattern_list.deinit(alloc);
    try Reader.appendVectorTextPatternRunsAlloc(alloc, &tile_pattern_list, pattern.fonts, pattern.patterns, tile_text_list.items);

    tile_paint_order = 0;
    tile_group_id = 1;
    var tile_image_list = std.ArrayList(ImageRun).empty;
    defer tile_image_list.deinit(alloc);
    const image_state = buildPatternGraphicsState(state);
    try extractImageRunsFromContentAppendWithState(alloc, &tile_image_list, pattern.content, pattern.images, pattern.gstates, pattern.forms, image_state, &.{}, .nonzero, &tile_paint_order, &tile_group_id);

    tile_paint_order = 0;
    tile_group_id = 1;
    var tile_shading_list = std.ArrayList(ShadingRun).empty;
    defer tile_shading_list.deinit(alloc);
    const shading_state = buildPatternGraphicsState(state);
    try extractShadingRunsFromContentAppendWithState(alloc, &tile_shading_list, pattern.content, pattern.shadings, pattern.gstates, pattern.forms, shading_state, &.{}, .nonzero, &tile_paint_order, &tile_group_id);

    tile_paint_order = 0;
    tile_group_id = 1;
    const pattern_state = buildPatternGraphicsState(state);
    try extractPatternRunsFromContentAppendWithState(alloc, &tile_pattern_list, pattern.content, pattern.patterns, pattern.gstates, pattern.forms, pattern_state, &.{}, .nonzero, &.{}, 0, &tile_paint_order, &tile_group_id);

    tile_paint_order = 0;
    tile_group_id = 1;
    var tile_shape_list = std.ArrayList(ShapeRun).empty;
    defer tile_shape_list.deinit(alloc);
    const shape_state = buildPatternGraphicsState(state);
    try extractShapeRunsFromContentAppendWithState(alloc, &tile_shape_list, pattern.content, pattern.gstates, pattern.forms, shape_state, &.{}, .nonzero, &.{}, 0, &tile_paint_order, &tile_group_id);

    return .{
        .kind = kind,
        .mode = .tiling,
        .paint_order = paint_order,
        .blend_mode = state.blend_mode,
        .group_id = state.group_id,
        .group_parent_id = state.group_parent_id,
        .group_isolated = state.group_isolated,
        .group_knockout = state.group_knockout,
        .fill_rule = fill_rule,
        .line_cap = state.line_cap,
        .line_join = state.line_join,
        .miter_limit = state.miter_limit,
        .dash_array = if (kind == .stroke and dash_array.len > 0) try scaleDashArrayAlloc(alloc, dash_array, state.matrix) else null,
        .dash_phase = if (kind == .stroke) dash_phase * graphicsStrokeScale(state.matrix) else 0,
        .stroke_width = state.stroke_width * graphicsStrokeScale(state.matrix),
        .closed = closed,
        .clip_box = state.clip_box,
        .clip_points = if (clip_points.len > 0) try alloc.dupe([2]f64, clip_points) else null,
        .clip_fill_rule = clip_fill_rule,
        .points = try alloc.dupe([2]f64, points),
        .pattern_matrix = multiplyGraphicsMatrix(state.matrix, pattern.matrix),
        .pattern_bbox = pattern.bbox,
        .pattern_x_step = pattern.x_step,
        .pattern_y_step = pattern.y_step,
        .base_color = if (pattern.colored)
            null
        else if (kind == .fill)
            colorWithAlpha(state.fill_color, state.fill_alpha)
        else
            colorWithAlpha(state.stroke_color, state.stroke_alpha),
        .tile_text_runs = try tile_text_list.toOwnedSlice(alloc),
        .tile_image_runs = try tile_image_list.toOwnedSlice(alloc),
        .tile_shading_runs = try tile_shading_list.toOwnedSlice(alloc),
        .tile_pattern_runs = try tile_pattern_list.toOwnedSlice(alloc),
        .tile_shape_runs = try tile_shape_list.toOwnedSlice(alloc),
    };
}

fn buildShadingPatternRunAlloc(
    alloc: Allocator,
    pattern: *const PagePattern,
    state: GraphicsState,
    points: []const [2]f64,
    kind: @FieldType(PatternRun, "kind"),
    closed: bool,
    fill_rule: FillRule,
    dash_array: []const f64,
    dash_phase: f64,
    clip_points: []const [2]f64,
    clip_fill_rule: FillRule,
    paint_order: usize,
) !PatternRun {
    const shading = pattern.shading orelse return error.UnsupportedPdfRendering;
    const shading_matrix = multiplyGraphicsMatrix(state.matrix, pattern.matrix);
    const p0 = applyMatrixToPoint(shading_matrix, shading.x0, shading.y0);
    const p1 = applyMatrixToPoint(shading_matrix, shading.x1, shading.y1);
    return .{
        .kind = kind,
        .mode = .shading,
        .paint_order = paint_order,
        .blend_mode = state.blend_mode,
        .group_id = state.group_id,
        .group_parent_id = state.group_parent_id,
        .group_isolated = state.group_isolated,
        .group_knockout = state.group_knockout,
        .fill_rule = fill_rule,
        .line_cap = state.line_cap,
        .line_join = state.line_join,
        .miter_limit = state.miter_limit,
        .dash_array = if (kind == .stroke and dash_array.len > 0) try scaleDashArrayAlloc(alloc, dash_array, state.matrix) else null,
        .dash_phase = if (kind == .stroke) dash_phase * graphicsStrokeScale(state.matrix) else 0,
        .stroke_width = state.stroke_width * graphicsStrokeScale(state.matrix),
        .closed = closed,
        .clip_box = state.clip_box,
        .clip_points = if (clip_points.len > 0) try alloc.dupe([2]f64, clip_points) else null,
        .clip_fill_rule = clip_fill_rule,
        .points = try alloc.dupe([2]f64, points),
        .pattern_matrix = pattern.matrix,
        .pattern_bbox = pattern.bbox,
        .pattern_x_step = pattern.x_step,
        .pattern_y_step = pattern.y_step,
        .shading = .{
            .kind = shading.kind,
            .paint_order = paint_order,
            .blend_mode = state.blend_mode,
            .group_id = state.group_id,
            .group_parent_id = state.group_parent_id,
            .group_isolated = state.group_isolated,
            .group_knockout = state.group_knockout,
            .clip_box = null,
            .clip_points = null,
            .clip_fill_rule = fill_rule,
            .x0 = p0[0],
            .y0 = p0[1],
            .r0 = shading.r0 * graphicsStrokeScale(shading_matrix),
            .x1 = p1[0],
            .y1 = p1[1],
            .r1 = shading.r1 * graphicsStrokeScale(shading_matrix),
            .c0 = colorWithAlpha(shading.c0, state.fill_alpha),
            .c1 = colorWithAlpha(shading.c1, state.fill_alpha),
            .extend_start = shading.extend_start,
            .extend_end = shading.extend_end,
        },
    };
}

fn multiplyGraphicsMatrix(lhs: GraphicsMatrix, rhs: GraphicsMatrix) GraphicsMatrix {
    return .{
        .a = lhs.a * rhs.a + lhs.b * rhs.c,
        .b = lhs.a * rhs.b + lhs.b * rhs.d,
        .c = lhs.c * rhs.a + lhs.d * rhs.c,
        .d = lhs.c * rhs.b + lhs.d * rhs.d,
        .e = lhs.e * rhs.a + lhs.f * rhs.c + rhs.e,
        .f = lhs.e * rhs.b + lhs.f * rhs.d + rhs.f,
    };
}

fn transformType3Point(point: [2]f64, run: TextRun, type3: Type3Font, cursor_x: f64, cursor_y: f64) [2]f64 {
    const tx = point[0] * type3.font_matrix[0] + point[1] * type3.font_matrix[2] + type3.font_matrix[4] + cursor_x;
    const ty = point[0] * type3.font_matrix[1] + point[1] * type3.font_matrix[3] + type3.font_matrix[5] + cursor_y;
    return .{
        run.x + tx * run.a + ty * run.c,
        run.y + tx * run.b + ty * run.d,
    };
}

fn scaleType3StrokeWidth(stroke_width: f64, run: TextRun, type3: Type3Font) f64 {
    const ux = type3.font_matrix[0];
    const uy = type3.font_matrix[1];
    const basis_world_x = ux * run.a;
    const basis_world_y = ux * run.b;
    const basis_world2_x = uy * run.c;
    const basis_world2_y = uy * run.d;
    const scale = @sqrt((basis_world_x + basis_world2_x) * (basis_world_x + basis_world2_x) + (basis_world_y + basis_world2_y) * (basis_world_y + basis_world2_y));
    return @max(0.1, stroke_width * run.font_size * @max(scale, 0.0001));
}

fn estimateType3MissingAdvance(run: TextRun) f64 {
    return (run.font_size * 0.6) * run.horizontal_scale + run.char_spacing;
}

fn estimateType1MissingAdvance(run: TextRun) f64 {
    return (run.font_size * 0.6) * run.horizontal_scale + run.char_spacing;
}

fn estimateRenderedRunCodepointAdvance(run: TextRun, cp: u21) f64 {
    const glyph = run.font_size * 0.6;
    const spacing = run.char_spacing + if (cp == ' ') run.word_spacing else 0.0;
    return (glyph + spacing) * run.horizontal_scale;
}

fn flattenTrueTypeContourAlloc(
    alloc: Allocator,
    points: []const font_lib.sfnt.GlyphPoint,
    run: TextRun,
    cursor_x: f64,
    scale: f64,
) ![][2]f64 {
    if (points.len == 0) return try alloc.alloc([2]f64, 0);

    var local = std.ArrayList([2]f64).empty;
    defer local.deinit(alloc);

    var start_index: usize = 0;
    var current = points[0];
    if (!current.on_curve) {
        const last = points[points.len - 1];
        if (last.on_curve) {
            start_index = points.len - 1;
            current = last;
        } else {
            current = .{
                .x = (current.x + last.x) * 0.5,
                .y = (current.y + last.y) * 0.5,
                .on_curve = true,
            };
        }
    }
    try local.append(alloc, .{ current.x, current.y });

    var i: usize = 1;
    while (i <= points.len) {
        const p1 = points[(start_index + i) % points.len];
        if (p1.on_curve) {
            try local.append(alloc, .{ p1.x, p1.y });
            current = p1;
            i += 1;
            continue;
        }

        const p2 = points[(start_index + i + 1) % points.len];
        const end_point = if (p2.on_curve)
            p2
        else
            font_lib.sfnt.GlyphPoint{
                .x = (p1.x + p2.x) * 0.5,
                .y = (p1.y + p2.y) * 0.5,
                .on_curve = true,
            };
        try appendQuadraticPointsAlloc(alloc, &local, .{ current.x, current.y }, .{ p1.x, p1.y }, .{ end_point.x, end_point.y }, 8);
        current = end_point;
        i += if (p2.on_curve) 2 else 1;
    }

    while (local.items.len > 1 and pointsAlmostEqual(local.items[0], local.items[local.items.len - 1])) {
        _ = local.pop();
    }

    const out = try alloc.alloc([2]f64, local.items.len);
    for (local.items, 0..) |point, idx| {
        const tx = cursor_x + point[0] * scale;
        const ty = point[1] * scale;
        out[idx] = .{
            run.x + tx * run.a + ty * run.c,
            run.y + tx * run.b + ty * run.d,
        };
    }
    return out;
}

fn appendQuadraticPointsAlloc(
    alloc: Allocator,
    out: *std.ArrayList([2]f64),
    start: [2]f64,
    control: [2]f64,
    end_point: [2]f64,
    steps: usize,
) !void {
    var step: usize = 1;
    while (step <= steps) : (step += 1) {
        const t = @as(f64, @floatFromInt(step)) / @as(f64, @floatFromInt(steps));
        const omt = 1.0 - t;
        try out.append(alloc, .{
            omt * omt * start[0] + 2.0 * omt * t * control[0] + t * t * end_point[0],
            omt * omt * start[1] + 2.0 * omt * t * control[1] + t * t * end_point[1],
        });
    }
}

fn pointsAlmostEqual(a: [2]f64, b: [2]f64) bool {
    return @abs(a[0] - b[0]) < 0.0001 and @abs(a[1] - b[1]) < 0.0001;
}

fn emitPolygonShapeRun(
    alloc: Allocator,
    out: *std.ArrayList(ShapeRun),
    paint_order: usize,
    state: GraphicsState,
    points: []const [2]f64,
    kind: @FieldType(ShapeRun, "kind"),
    closed: bool,
    fill_rule: FillRule,
    dash_array: []const f64,
    dash_phase: f64,
    clip_points: []const [2]f64,
    clip_fill_rule: FillRule,
) !void {
    try out.append(alloc, .{
        .paint_order = paint_order,
        .blend_mode = state.blend_mode,
        .kind = kind,
        .group_id = state.group_id,
        .group_parent_id = state.group_parent_id,
        .group_isolated = state.group_isolated,
        .group_knockout = state.group_knockout,
        .fill_rule = fill_rule,
        .line_cap = state.line_cap,
        .line_join = state.line_join,
        .miter_limit = state.miter_limit,
        .dash_array = if (kind == .stroke and dash_array.len > 0) try scaleDashArrayAlloc(alloc, dash_array, state.matrix) else null,
        .dash_phase = if (kind == .stroke) dash_phase * graphicsStrokeScale(state.matrix) else 0,
        .color = blk: {
            var color = if (kind == .fill) state.fill_color else state.stroke_color;
            color[3] = if (kind == .fill) state.fill_alpha else state.stroke_alpha;
            break :blk color;
        },
        .stroke_width = state.stroke_width * graphicsStrokeScale(state.matrix),
        .closed = closed,
        .clip_box = state.clip_box,
        .clip_points = if (clip_points.len > 0) try alloc.dupe([2]f64, clip_points) else null,
        .clip_fill_rule = clip_fill_rule,
        .points = try alloc.dupe([2]f64, points),
    });
}

fn graphicsStrokeScale(matrix: GraphicsMatrix) f64 {
    const sx = @sqrt(matrix.a * matrix.a + matrix.b * matrix.b);
    const sy = @sqrt(matrix.c * matrix.c + matrix.d * matrix.d);
    return @max(0.0001, (sx + sy) / 2.0);
}

fn scaleDashArrayAlloc(alloc: Allocator, dash_array: []const f64, matrix: GraphicsMatrix) ![]f64 {
    const scale = graphicsStrokeScale(matrix);
    var out = try alloc.alloc(f64, dash_array.len);
    for (dash_array, 0..) |value, i| out[i] = value * scale;
    return out;
}

fn appendFlattenedCubicBezier(
    alloc: Allocator,
    points: *std.ArrayList([2]f64),
    start: [2]f64,
    c1: [2]f64,
    c2: [2]f64,
    end: [2]f64,
) !void {
    const segments: usize = 12;
    var i: usize = 1;
    while (i <= segments) : (i += 1) {
        const t = @as(f64, @floatFromInt(i)) / @as(f64, @floatFromInt(segments));
        const mt = 1.0 - t;
        const x = mt * mt * mt * start[0] +
            3.0 * mt * mt * t * c1[0] +
            3.0 * mt * t * t * c2[0] +
            t * t * t * end[0];
        const y = mt * mt * mt * start[1] +
            3.0 * mt * mt * t * c1[1] +
            3.0 * mt * t * t * c2[1] +
            t * t * t * end[1];
        try points.append(alloc, .{ x, y });
    }
}

fn applyMatrixToPoint(m: GraphicsMatrix, x: f64, y: f64) [2]f64 {
    return .{
        m.a * x + m.c * y + m.e,
        m.b * x + m.d * y + m.f,
    };
}

fn parseFormMatrix(obj: *const syntax.Object) GraphicsMatrix {
    const matrix_obj = obj.get("Matrix") orelse return .{};
    if (matrix_obj.* != .array or matrix_obj.array.len < 6) return .{};
    return .{
        .a = numericObjectValue(&matrix_obj.array[0]) orelse 1,
        .b = numericObjectValue(&matrix_obj.array[1]) orelse 0,
        .c = numericObjectValue(&matrix_obj.array[2]) orelse 0,
        .d = numericObjectValue(&matrix_obj.array[3]) orelse 1,
        .e = numericObjectValue(&matrix_obj.array[4]) orelse 0,
        .f = numericObjectValue(&matrix_obj.array[5]) orelse 0,
    };
}

fn transformPageBox(box: PageBox, matrix: GraphicsMatrix) PageBox {
    const p0 = applyMatrixToPoint(matrix, box.min_x, box.min_y);
    const p1 = applyMatrixToPoint(matrix, box.max_x, box.min_y);
    const p2 = applyMatrixToPoint(matrix, box.max_x, box.max_y);
    const p3 = applyMatrixToPoint(matrix, box.min_x, box.max_y);
    return .{
        .min_x = @min(@min(p0[0], p1[0]), @min(p2[0], p3[0])),
        .min_y = @min(@min(p0[1], p1[1]), @min(p2[1], p3[1])),
        .max_x = @max(@max(p0[0], p1[0]), @max(p2[0], p3[0])),
        .max_y = @max(@max(p0[1], p1[1]), @max(p2[1], p3[1])),
    };
}

fn applyMatrixToVector(m: GraphicsMatrix, x: f64, y: f64) [2]f64 {
    return .{
        m.a * x + m.c * y,
        m.b * x + m.d * y,
    };
}

fn pathBounds(points: []const [2]f64) PageBox {
    var min_x = points[0][0];
    var max_x = points[0][0];
    var min_y = points[0][1];
    var max_y = points[0][1];
    for (points[1..]) |point| {
        min_x = @min(min_x, point[0]);
        max_x = @max(max_x, point[0]);
        min_y = @min(min_y, point[1]);
        max_y = @max(max_y, point[1]);
    }
    return .{ .min_x = min_x, .min_y = min_y, .max_x = max_x, .max_y = max_y };
}

fn grayColor(gray: f64) [4]u8 {
    const c = floatChannel(gray);
    return .{ c, c, c, 0xff };
}

fn rgbColor(r: f64, g: f64, b: f64) [4]u8 {
    return .{ floatChannel(r), floatChannel(g), floatChannel(b), 0xff };
}

fn cmykColor(c: f64, m: f64, y: f64, k: f64) [4]u8 {
    const ck = std.math.clamp(c, 0.0, 1.0);
    const mk = std.math.clamp(m, 0.0, 1.0);
    const yk = std.math.clamp(y, 0.0, 1.0);
    const kk = std.math.clamp(k, 0.0, 1.0);
    return rgbColor((1.0 - ck) * (1.0 - kk), (1.0 - mk) * (1.0 - kk), (1.0 - yk) * (1.0 - kk));
}

fn labColor(l: f64, a: f64, b: f64, white: [3]f64) [4]u8 {
    const fy = (l + 16.0) / 116.0;
    const fx = fy + a / 500.0;
    const fz = fy - b / 200.0;

    const xr = labFInv(fx);
    const yr = labFInv(fy);
    const zr = labFInv(fz);

    const x = xr * white[0];
    const y = yr * white[1];
    const z = zr * white[2];

    var r = 3.2406 * x - 1.5372 * y - 0.4986 * z;
    var g = -0.9689 * x + 1.8758 * y + 0.0415 * z;
    var blue = 0.0557 * x - 0.2040 * y + 1.0570 * z;

    r = linearToSrgb(r);
    g = linearToSrgb(g);
    blue = linearToSrgb(blue);
    return rgbColor(r, g, blue);
}

fn labFInv(t: f64) f64 {
    const delta = 6.0 / 29.0;
    if (t > delta) return t * t * t;
    return 3.0 * delta * delta * (t - 4.0 / 29.0);
}

fn linearToSrgb(v: f64) f64 {
    const clamped = @max(0.0, v);
    if (clamped <= 0.0031308) return 12.92 * clamped;
    return 1.055 * std.math.pow(f64, clamped, 1.0 / 2.4) - 0.055;
}

fn mapUnitToRange(unit: f64, min: f64, max: f64) f64 {
    return min + std.math.clamp(unit, 0.0, 1.0) * (max - min);
}

fn parseLabRange(dict: []const syntax.DictEntry, key: []const u8, base: usize, default_min: f64, default_max: f64) [2]f64 {
    for (dict) |entry| {
        if (!std.mem.eql(u8, entry.key, key)) continue;
        if (entry.value != .array or entry.value.array.len < base + 2) break;
        return .{
            numericObjectValue(&entry.value.array[base]) orelse default_min,
            numericObjectValue(&entry.value.array[base + 1]) orelse default_max,
        };
    }
    return .{ default_min, default_max };
}

fn parseLabWhitePoint(dict: []const syntax.DictEntry) [3]f64 {
    for (dict) |entry| {
        if (!std.mem.eql(u8, entry.key, "WhitePoint")) continue;
        if (entry.value != .array or entry.value.array.len < 3) break;
        return .{
            numericObjectValue(&entry.value.array[0]) orelse 0.95047,
            numericObjectValue(&entry.value.array[1]) orelse 1.0,
            numericObjectValue(&entry.value.array[2]) orelse 1.08883,
        };
    }
    return .{ 0.95047, 1.0, 1.08883 };
}

fn dictGetNumber(dict: []const syntax.DictEntry, key: []const u8) ?f64 {
    for (dict) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return numericObjectValue(&entry.value);
    }
    return null;
}

fn dictGetInteger(dict: []const syntax.DictEntry, key: []const u8) ?i64 {
    for (dict) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value.asInteger();
    }
    return null;
}

fn dictGetArray(dict: []const syntax.DictEntry, key: []const u8) ?[]const syntax.Object {
    for (dict) |entry| {
        if (std.mem.eql(u8, entry.key, key) and entry.value == .array) return entry.value.array;
    }
    return null;
}

fn dictGetObject(dict: []const syntax.DictEntry, key: []const u8) ?*const syntax.Object {
    for (dict) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return &entry.value;
    }
    return null;
}

fn colorFromComponents(color_space_name: []const u8, components: [4]u8, count: usize) ?[4]u8 {
    if (std.mem.eql(u8, color_space_name, "DeviceGray") and count >= 1) {
        return .{ components[0], components[0], components[0], 0xff };
    }
    if (std.mem.eql(u8, color_space_name, "DeviceRGB") and count >= 3) {
        return .{ components[0], components[1], components[2], 0xff };
    }
    if (std.mem.eql(u8, color_space_name, "DeviceCMYK") and count >= 4) {
        return cmykColor(
            @as(f64, @floatFromInt(components[0])) / 255.0,
            @as(f64, @floatFromInt(components[1])) / 255.0,
            @as(f64, @floatFromInt(components[2])) / 255.0,
            @as(f64, @floatFromInt(components[3])) / 255.0,
        );
    }
    return null;
}

fn evalExponentialTintComponent(transform: *const ExponentialTintTransform, tint: f64, component: usize) f64 {
    const t = std.math.clamp(tint, 0.0, 1.0);
    return std.math.clamp(
        transform.c0[component] + std.math.pow(f64, t, transform.n) * (transform.c1[component] - transform.c0[component]),
        0.0,
        1.0,
    );
}

fn applyDecodeByte(value: u8, decode_obj: ?*const syntax.Object, component_index: usize) u8 {
    return floatChannel(applyDecodeUnit(value, decode_obj, component_index));
}

fn applyDecodeUnit(value: u8, decode_obj: ?*const syntax.Object, component_index: usize) f64 {
    const unit = @as(f64, @floatFromInt(value)) / 255.0;
    const range = decodeRange(decode_obj, component_index) orelse return unit;
    return std.math.clamp(range.min + unit * (range.max - range.min), 0.0, 1.0);
}

fn applyIndexedDecode(value: u8, decode_obj: ?*const syntax.Object, palette_entries: usize) !u8 {
    if (palette_entries == 0) return error.UnsupportedPdfRendering;
    const max_index = @as(f64, @floatFromInt(palette_entries - 1));
    const unit = @as(f64, @floatFromInt(value)) / 255.0;
    const range = decodeRange(decode_obj, 0) orelse return value;
    const mapped = std.math.clamp(range.min + unit * (range.max - range.min), 0.0, max_index);
    return @intFromFloat(@round(mapped));
}

fn parseMaskDecodeInvert(decode_obj: ?*const syntax.Object) bool {
    const range = decodeRange(decode_obj, 0) orelse return false;
    return range.min > range.max;
}

fn decodeRange(decode_obj: ?*const syntax.Object, component_index: usize) ?struct { min: f64, max: f64 } {
    const obj = decode_obj orelse return null;
    if (obj.* != .array) return null;
    const array = obj.array;
    const base = component_index * 2;
    if (array.len < base + 2) return null;
    const min = numericObjectValue(&array[base]) orelse return null;
    const max = numericObjectValue(&array[base + 1]) orelse return null;
    return .{ .min = min, .max = max };
}

fn applyColorKeyMask(rgba: []u8, mask_array: []const syntax.Object) void {
    if (mask_array.len != 2 and mask_array.len != 6) return;

    const r0 = numericObjectValue(&mask_array[0]) orelse return;
    const r1 = numericObjectValue(&mask_array[1]) orelse return;
    const gray_only = mask_array.len == 2;
    const g0 = if (gray_only) r0 else numericObjectValue(&mask_array[2]) orelse return;
    const g1 = if (gray_only) r1 else numericObjectValue(&mask_array[3]) orelse return;
    const b0 = if (gray_only) r0 else numericObjectValue(&mask_array[4]) orelse return;
    const b1 = if (gray_only) r1 else numericObjectValue(&mask_array[5]) orelse return;

    const r0u = floatChannel(r0 / 255.0);
    const r1u = floatChannel(r1 / 255.0);
    const g0u = floatChannel(g0 / 255.0);
    const g1u = floatChannel(g1 / 255.0);
    const b0u = floatChannel(b0 / 255.0);
    const b1u = floatChannel(b1 / 255.0);

    const Vec16 = @Vector(16, u8);
    const Vec4 = @Vector(4, u8);
    const lanes = 16;
    var i: usize = 0;
    while (i + lanes <= rgba.len) : (i += lanes) {
        const block: [16]u8 = rgba[i..][0..16].*;
        const vec: Vec16 = @bitCast(block);
        const rv: Vec4 = @shuffle(u8, vec, undefined, [_]i32{ 0, 4, 8, 12 });
        const gv: Vec4 = @shuffle(u8, vec, undefined, [_]i32{ 1, 5, 9, 13 });
        const bv: Vec4 = @shuffle(u8, vec, undefined, [_]i32{ 2, 6, 10, 14 });
        const matches = (rv >= @as(Vec4, @splat(r0u))) &
            (rv <= @as(Vec4, @splat(r1u))) &
            (gv >= @as(Vec4, @splat(g0u))) &
            (gv <= @as(Vec4, @splat(g1u))) &
            (bv >= @as(Vec4, @splat(b0u))) &
            (bv <= @as(Vec4, @splat(b1u)));
        const match_arr: [4]bool = matches;
        inline for (0..4) |lane| {
            if (match_arr[lane]) rgba[i + lane * 4 + 3] = 0;
        }
    }
    while (i < rgba.len) : (i += 4) {
        const r = rgba[i + 0];
        const g = rgba[i + 1];
        const b = rgba[i + 2];
        if (r >= r0u and r <= r1u and g >= g0u and g <= g1u and b >= b0u and b <= b1u) {
            rgba[i + 3] = 0;
        }
    }
}

fn applySoftMaskAlpha(rgba: []u8, smask_rgba: []const u8) void {
    const Vec16 = @Vector(16, u8);
    const Vec4 = @Vector(4, u8);
    const lanes = 16;
    var i: usize = 0;
    while (i + lanes <= rgba.len and i + lanes <= smask_rgba.len) : (i += lanes) {
        var dst_block: [16]u8 = rgba[i..][0..16].*;
        const src_block: [16]u8 = smask_rgba[i..][0..16].*;
        const src_vec: Vec16 = @bitCast(src_block);
        const alpha_vec: Vec4 = @shuffle(u8, src_vec, undefined, [_]i32{ 0, 4, 8, 12 });
        const alpha_arr: [4]u8 = @bitCast(alpha_vec);
        inline for (0..4) |lane| {
            dst_block[lane * 4 + 3] = alpha_arr[lane];
        }
        rgba[i..][0..16].* = dst_block;
    }
    while (i + 3 < rgba.len and i < smask_rgba.len) : (i += 4) {
        rgba[i + 3] = smask_rgba[i];
    }
}

fn applyExplicitMaskAlpha(rgba: []u8, mask_rgba: []const u8, mask_is_stencil: bool) void {
    const Vec16 = @Vector(16, u8);
    const Vec4 = @Vector(4, u8);
    const lanes = 16;
    var i: usize = 0;
    while (i + lanes <= rgba.len and i + lanes <= mask_rgba.len) : (i += lanes) {
        var dst_block: [16]u8 = rgba[i..][0..16].*;
        const src_block: [16]u8 = mask_rgba[i..][0..16].*;
        const src_vec: Vec16 = @bitCast(src_block);
        const alpha_vec: Vec4 = @shuffle(u8, src_vec, undefined, [_]i32{ 3, 7, 11, 15 });
        const gray_vec: Vec4 = @shuffle(u8, src_vec, undefined, [_]i32{ 0, 4, 8, 12 });
        const use_alpha_vec = @as(@Vector(4, bool), @splat(mask_is_stencil));
        const effective_vec = @select(u8, use_alpha_vec, alpha_vec, gray_vec);
        const effective_arr: [4]u8 = @bitCast(effective_vec);
        inline for (0..4) |lane| {
            dst_block[lane * 4 + 3] = effective_arr[lane];
        }
        rgba[i..][0..16].* = dst_block;
    }
    while (i + 3 < rgba.len and i + 3 < mask_rgba.len) : (i += 4) {
        rgba[i + 3] = if (mask_is_stencil) mask_rgba[i + 3] else mask_rgba[i];
    }
}

fn intersectPageBoxes(lhs: PageBox, rhs: PageBox) PageBox {
    return .{
        .min_x = @max(lhs.min_x, rhs.min_x),
        .min_y = @max(lhs.min_y, rhs.min_y),
        .max_x = @min(lhs.max_x, rhs.max_x),
        .max_y = @min(lhs.max_y, rhs.max_y),
    };
}

fn tryIntersectRectClipPoints(
    alloc: Allocator,
    lhs_points: []const [2]f64,
    rhs_points: []const [2]f64,
) !?[]const [2]f64 {
    if (lhs_points.len < 3 or rhs_points.len < 3) return null;
    const clipped = try clipConvexPolygonAlloc(alloc, lhs_points, rhs_points);
    errdefer alloc.free(clipped);
    return clipped;
}

fn clipConvexPolygonAlloc(alloc: Allocator, subject: []const [2]f64, clip_polygon: []const [2]f64) ![]const [2]f64 {
    var output = std.ArrayList([2]f64).empty;
    defer output.deinit(alloc);
    try output.appendSlice(alloc, subject);

    const orientation = polygonSignedArea(clip_polygon);
    if (@abs(orientation) <= 0.000001) return try alloc.alloc([2]f64, 0);

    var i: usize = 0;
    while (i < clip_polygon.len) : (i += 1) {
        const a = clip_polygon[i];
        const b = clip_polygon[(i + 1) % clip_polygon.len];
        const input = try output.toOwnedSlice(alloc);
        defer alloc.free(input);
        output.clearRetainingCapacity();
        if (input.len == 0) break;

        var prev = input[input.len - 1];
        var prev_inside = pointInsideClipEdge(prev, a, b, orientation);
        for (input) |curr| {
            const curr_inside = pointInsideClipEdge(curr, a, b, orientation);
            if (curr_inside) {
                if (!prev_inside) {
                    if (lineIntersectionPoint(prev, curr, a, b)) |point| try output.append(alloc, point);
                }
                try output.append(alloc, curr);
            } else if (prev_inside) {
                if (lineIntersectionPoint(prev, curr, a, b)) |point| try output.append(alloc, point);
            }
            prev = curr;
            prev_inside = curr_inside;
        }
    }

    return try output.toOwnedSlice(alloc);
}

fn polygonSignedArea(points: []const [2]f64) f64 {
    var area: f64 = 0;
    var i: usize = 0;
    while (i < points.len) : (i += 1) {
        const a = points[i];
        const b = points[(i + 1) % points.len];
        area += a[0] * b[1] - b[0] * a[1];
    }
    return area * 0.5;
}

fn pointInsideClipEdge(point: [2]f64, edge_a: [2]f64, edge_b: [2]f64, orientation: f64) bool {
    const cross = (edge_b[0] - edge_a[0]) * (point[1] - edge_a[1]) - (edge_b[1] - edge_a[1]) * (point[0] - edge_a[0]);
    return if (orientation > 0) cross >= -0.000001 else cross <= 0.000001;
}

fn lineIntersectionPoint(a1: [2]f64, a2: [2]f64, b1: [2]f64, b2: [2]f64) ?[2]f64 {
    const dax = a2[0] - a1[0];
    const day = a2[1] - a1[1];
    const dbx = b2[0] - b1[0];
    const dby = b2[1] - b1[1];
    const denom = dax * dby - day * dbx;
    if (@abs(denom) < 0.000001) return null;
    const dx = b1[0] - a1[0];
    const dy = b1[1] - a1[1];
    const t = (dx * dby - dy * dbx) / denom;
    return .{ a1[0] + t * dax, a1[1] + t * day };
}

fn floatChannel(v: f64) u8 {
    const clamped = std.math.clamp(v, 0.0, 1.0);
    return @intFromFloat(@round(clamped * 255.0));
}

fn parsePageBox(obj: *const syntax.Object) !PageBox {
    if (obj.* != .array or obj.array.len < 4) return error.InvalidPageBox;
    const x0 = numericObjectValue(&obj.array[0]) orelse return error.InvalidPageBox;
    const y0 = numericObjectValue(&obj.array[1]) orelse return error.InvalidPageBox;
    const x1 = numericObjectValue(&obj.array[2]) orelse return error.InvalidPageBox;
    const y1 = numericObjectValue(&obj.array[3]) orelse return error.InvalidPageBox;
    return .{
        .min_x = @min(x0, x1),
        .min_y = @min(y0, y1),
        .max_x = @max(x0, x1),
        .max_y = @max(y0, y1),
    };
}

test "reader init parses basic header and startxref" {
    const alloc = std.testing.allocator;
    const prefix =
        "%PDF-1.7\n" ++
        "1 0 obj\n<< /Type /Catalog >>\nendobj\n" ++
        "xref\n" ++
        "0 2\n" ++
        "0000000000 65535 f \n" ++
        "0000000009 00000 n \n" ++
        "trailer\n" ++
        "<< /Size 2 /Root 1 0 R >>\n";
    const xref_offset = std.mem.indexOf(u8, prefix, "xref").?;
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u8, 7), reader.version_minor);
    try std.testing.expectEqual(xref_offset, reader.startxref_offset);
    try std.testing.expectEqual(@as(usize, 2), reader.xref_entries.len);
    try std.testing.expect(reader.trailerGet("Root") != null);
}

test "reader init rejects missing eof" {
    const sample = "%PDF-1.7\nstartxref\n123\n";
    try std.testing.expectError(error.MissingPdfEof, Reader.init(std.testing.allocator, sample));
}

test "reader init rejects missing startxref" {
    const sample = "%PDF-1.7\n1 0 obj\n<<>>\nendobj\n%%EOF\n";
    try std.testing.expectError(error.MissingStartXref, Reader.init(std.testing.allocator, sample));
}

test "reader can load indirect object from xref table" {
    const alloc = std.testing.allocator;
    const prefix =
        "%PDF-1.7\n" ++
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n" ++
        "2 0 obj\n<< /Type /Pages /Count 0 >>\nendobj\n" ++
        "xref\n" ++
        "0 3\n" ++
        "0000000000 65535 f \n" ++
        "0000000009 00000 n \n" ++
        "0000000058 00000 n \n" ++
        "trailer\n" ++
        "<< /Size 3 /Root 1 0 R >>\n";
    const xref_offset = std.mem.indexOf(u8, prefix, "xref").?;
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    var obj = try reader.readIndirectObject(.{ .id = 1, .gen = 0 });
    defer obj.deinit(alloc);
    try std.testing.expect(obj == .dict);
    try std.testing.expectEqualStrings("Catalog", obj.get("Type").?.asName().?);
    try std.testing.expect(obj.get("Pages").?.* == .obj_ref);
}

test "reader can decode flated stream object" {
    const alloc = std.testing.allocator;
    const prefix =
        "%PDF-1.7\n" ++
        "1 0 obj\n<< /Length 13 /Filter /FlateDecode >>\nstream\n" ++
        "x\x9c\xcbH\xcd\xc9\xc9\x07\x00\x06,\x02\x15" ++
        "\nendstream\nendobj\n" ++
        "xref\n" ++
        "0 2\n" ++
        "0000000000 65535 f \n" ++
        "0000000009 00000 n \n" ++
        "trailer\n" ++
        "<< /Size 2 /Root 1 0 R >>\n";
    const xref_offset = std.mem.indexOf(u8, prefix, "xref").?;
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    var obj = try reader.readIndirectObject(.{ .id = 1, .gen = 0 });
    defer obj.deinit(alloc);
    try std.testing.expect(obj == .stream);

    const decoded = try reader.readDecodedStreamData(&obj);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "reader can decode ascii hex stream object through filter array" {
    const alloc = std.testing.allocator;
    const prefix =
        "%PDF-1.7\n" ++
        "1 0 obj\n<< /Length 11 /Filter [/ASCIIHexDecode] >>\nstream\n" ++
        "68656c6c6f>" ++
        "\nendstream\nendobj\n" ++
        "xref\n" ++
        "0 2\n" ++
        "0000000000 65535 f \n" ++
        "0000000009 00000 n \n" ++
        "trailer\n" ++
        "<< /Size 2 /Root 1 0 R >>\n";
    const xref_offset = std.mem.indexOf(u8, prefix, "xref").?;
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    var obj = try reader.readIndirectObject(.{ .id = 1, .gen = 0 });
    defer obj.deinit(alloc);
    try std.testing.expect(obj == .stream);

    const decoded = try reader.readDecodedStreamData(&obj);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "reader can decode ascii85 stream object" {
    const alloc = std.testing.allocator;
    const prefix =
        "%PDF-1.7\n" ++
        "1 0 obj\n<< /Length 9 /Filter /ASCII85Decode >>\nstream\n" ++
        "BOu!rDZ~>" ++
        "\nendstream\nendobj\n" ++
        "xref\n" ++
        "0 2\n" ++
        "0000000000 65535 f \n" ++
        "0000000009 00000 n \n" ++
        "trailer\n" ++
        "<< /Size 2 /Root 1 0 R >>\n";
    const xref_offset = std.mem.indexOf(u8, prefix, "xref").?;
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    var obj = try reader.readIndirectObject(.{ .id = 1, .gen = 0 });
    defer obj.deinit(alloc);
    const decoded = try reader.readDecodedStreamData(&obj);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "reader applies png up predictor after flate decode" {
    const alloc = std.testing.allocator;
    const raw_predicted = &.{ 2, 'h', 'e', 'l', 'l', 'o' };
    var comp = std.ArrayList(u8).empty;
    defer comp.deinit(alloc);
    {
        var hist: [std.compress.flate.max_window_len]u8 = undefined;
        var out_buf: [256]u8 = undefined;
        var out: std.Io.Writer = .fixed(&out_buf);
        var zw = try std.compress.flate.Compress.init(&out, hist[0..], .zlib, .default);
        try zw.writer.writeAll(raw_predicted);
        try zw.finish();
        try comp.appendSlice(alloc, out.buffered());
    }

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    const obj_offset = prefix.items.len;
    const obj_head = try std.fmt.allocPrint(
        alloc,
        "1 0 obj\n<< /Length {d} /Filter /FlateDecode /DecodeParms << /Predictor 12 /Columns 5 >> >>\nstream\n",
        .{comp.items.len},
    );
    defer alloc.free(obj_head);
    try prefix.appendSlice(alloc, obj_head);
    try prefix.appendSlice(alloc, comp.items);
    try prefix.appendSlice(alloc, "\nendstream\nendobj\n");
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n" ++
        "0 2\n" ++
        "0000000000 65535 f \n");
    const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{obj_offset});
    defer alloc.free(line);
    try prefix.appendSlice(alloc, line);
    try prefix.appendSlice(alloc, "trailer\n<< /Size 2 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    var stream = try reader.readIndirectObject(.{ .id = 1, .gen = 0 });
    defer stream.deinit(alloc);
    const decoded = try reader.readDecodedStreamData(&stream);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "reader can decode lzw stream object" {
    const alloc = std.testing.allocator;
    const encoded = &.{ 0x80, 0x0b, 0x60, 0x50, 0x22, 0x0c, 0x0c, 0x85, 0x01 };
    const prefix = try std.fmt.allocPrint(
        alloc,
        "%PDF-1.7\n1 0 obj\n<< /Length {d} /Filter /LZWDecode >>\nstream\n{s}\nendstream\nendobj\nxref\n0 2\n0000000000 65535 f \n0000000009 00000 n \ntrailer\n<< /Size 2 /Root 1 0 R >>\n",
        .{ encoded.len, encoded },
    );
    defer alloc.free(prefix);
    const xref_offset = std.mem.indexOf(u8, prefix, "xref").?;
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    var obj = try reader.readIndirectObject(.{ .id = 1, .gen = 0 });
    defer obj.deinit(alloc);
    const decoded = try reader.readDecodedStreamData(&obj);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("-----A---B", decoded);
}

test "reader can decode run length stream object" {
    const alloc = std.testing.allocator;
    const encoded = &.{ 2, 'A', 'B', 'C', 254, 'Z', 128 };
    const prefix = try std.fmt.allocPrint(
        alloc,
        "%PDF-1.7\n1 0 obj\n<< /Length {d} /Filter /RunLengthDecode >>\nstream\n{s}\nendstream\nendobj\nxref\n0 2\n0000000000 65535 f \n0000000009 00000 n \ntrailer\n<< /Size 2 /Root 1 0 R >>\n",
        .{ encoded.len, encoded },
    );
    defer alloc.free(prefix);
    const xref_offset = std.mem.indexOf(u8, prefix, "xref").?;
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    var obj = try reader.readIndirectObject(.{ .id = 1, .gen = 0 });
    defer obj.deinit(alloc);
    const decoded = try reader.readDecodedStreamData(&obj);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("ABCZZZ", decoded);
}

test "reader can extract plain text from simple page content" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello World) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets, 0..) |off, i| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        _ = i;
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), try reader.pageCount());
    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello World\n", text);
}

test "reader can extract plain text from content stream with indirect length" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello Again) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\nendobj\n",
        "4 0 obj\n<< /Length 5 0 R >>\nstream\n",
        content,
        "endstream\nendobj\n",
        try std.fmt.allocPrint(alloc, "5 0 obj\n{d}\nendobj\n", .{content.len}),
    };
    defer alloc.free(objects[6]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [5]usize = undefined;
    offsets[0] = prefix.items.len;
    try prefix.appendSlice(alloc, objects[0]);
    offsets[1] = prefix.items.len;
    try prefix.appendSlice(alloc, objects[1]);
    offsets[2] = prefix.items.len;
    try prefix.appendSlice(alloc, objects[2]);
    offsets[3] = prefix.items.len;
    try prefix.appendSlice(alloc, objects[3]);
    try prefix.appendSlice(alloc, objects[4]);
    try prefix.appendSlice(alloc, objects[5]);
    offsets[4] = prefix.items.len;
    try prefix.appendSlice(alloc, objects[6]);

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello Again\n", text);
}

test "reader uses WinAnsi font encoding for text extraction" {
    const alloc = std.testing.allocator;
    const content = "BT\n/F1 12 Tf\n(\x80) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /Encoding /WinAnsiEncoding >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("\u{20ac}\n", text);
}

test "reader uses ToUnicode cmap for text extraction" {
    const alloc = std.testing.allocator;
    const content = "BT\n/F1 12 Tf\n<0001> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 beginbfchar\n" ++
        "<0001> <0041>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type0 /ToUnicode 6 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "6 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap }),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[5]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("A\n", text);
}

test "reader uses ToUnicode codespacerange for multibyte text extraction" {
    const alloc = std.testing.allocator;
    const content = "BT\n/F1 12 Tf\n<0102> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "1 beginbfchar\n" ++
        "<0102> <03A9>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type0 /ToUnicode 6 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "6 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap }),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[5]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("\u{03A9}\n", text);
}

test "reader uses multiline ToUnicode bfrange arrays" {
    const alloc = std.testing.allocator;
    const content = "BT\n/F1 12 Tf\n<00010002> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "1 beginbfrange\n" ++
        "<0001> <0002>\n" ++
        "[\n" ++
        "<0041>\n" ++
        "<0042>\n" ++
        "]\n" ++
        "endbfrange\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type0 /ToUnicode 6 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "6 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap }),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[5]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("AB\n", text);
}

test "reader extracts positioned text runs from text matrix operators" {
    const alloc = std.testing.allocator;
    const content = "BT\n/F1 12 Tf\n1 0 0 1 72 720 Tm\n(Top) Tj\n0 -24 Td\n(Bottom) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /Encoding /WinAnsiEncoding >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 2), runs.len);
    try std.testing.expectEqualStrings("Top", runs[0].text);
    try std.testing.expectEqualStrings("Bottom", runs[1].text);
    try std.testing.expect(runs[0].y > runs[1].y);
    try std.testing.expectApproxEqAbs(@as(f64, 72), runs[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1), runs[0].a, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), runs[0].b, 0.001);
}

test "reader preserves rotated text transform on runs" {
    const alloc = std.testing.allocator;
    const content = "BT\n/F1 12 Tf\n0 1 -1 0 72 720 Tm\n(Rotated) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /Encoding /WinAnsiEncoding >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqualStrings("Rotated", runs[0].text);
    try std.testing.expectApproxEqAbs(@as(f64, 0), runs[0].a, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1), runs[0].b, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, -1), runs[0].c, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), runs[0].d, 0.001);
}

test "reader applies Td in rotated text space" {
    const alloc = std.testing.allocator;
    const content = "BT\n/F1 12 Tf\n0 1 -1 0 72 720 Tm\n(A) Tj\n10 0 Td\n(B) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /Encoding /WinAnsiEncoding >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 2), runs.len);
    try std.testing.expectApproxEqAbs(runs[0].x, runs[1].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[1].y - runs[0].y, 0.001);
}

test "reader applies ExtGState alpha to text runs" {
    const alloc = std.testing.allocator;
    const content = "q\n/GS1 gs\nBT\n/F1 12 Tf\n1 0 0 1 72 720 Tm\n(Faded) Tj\nET\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> /ExtGState << /GS1 6 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /Encoding /WinAnsiEncoding >>\nendobj\n",
        "6 0 obj\n<< /ca 0.5 >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 128), runs[0].alpha);
}

test "reader applies ExtGState blend mode to text runs" {
    const alloc = std.testing.allocator;
    const content = "q\n/GS1 gs\nBT\n/F1 12 Tf\n1 0 0 1 72 720 Tm\n(Blend) Tj\nET\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> /ExtGState << /GS1 6 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /Encoding /WinAnsiEncoding >>\nendobj\n",
        "6 0 obj\n<< /BM /Multiply >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(BlendMode.multiply, runs[0].blend_mode);
}

test "reader preserves clip box on text runs" {
    const alloc = std.testing.allocator;
    const content = "10 10 20 20 re\nW\nn\nBT\n/F1 12 Tf\n15 25 Td\n(Clipped) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /Encoding /WinAnsiEncoding >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].clip_box != null);
    const clip = runs[0].clip_box.?;
    try std.testing.expectApproxEqAbs(@as(f64, 10), clip.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), clip.min_y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 30), clip.max_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 30), clip.max_y, 0.001);
}

test "reader applies text state operators to positioned runs" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 10 Tf\n" ++
        "2 Tc\n" ++
        "5 Tw\n" ++
        "150 Tz\n" ++
        "4 Ts\n" ++
        "10 20 Td\n" ++
        "(A B) Tj\n" ++
        "ET\n";

    const fonts = [_]PageFont{
        .{
            .name = try alloc.dupe(u8, "F1"),
            .decoder = .{},
        },
    };
    defer {
        var font = fonts[0];
        font.deinit(alloc);
    }

    var runs = std.ArrayList(TextRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractTextRunsFromContentAppend(alloc, &runs, content, &fonts, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expectEqualStrings("A B", runs.items[0].text);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs.items[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 24), runs.items[0].y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), runs.items[0].horizontal_scale, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 2), runs.items[0].char_spacing, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 5), runs.items[0].word_spacing, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 40.5), runs.items[0].advance_width, 0.001);
}

test "reader measures type1 text run advance width" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 10 Tf\n" ++
        "10 20 Td\n" ++
        "(AB) Tj\n" ++
        "ET\n";

    const glyphs = try alloc.alloc(Type1Glyph, 2);
    glyphs[0] = .{
        .code = 'A',
        .name = try alloc.dupe(u8, "A"),
        .charstring = try alloc.dupe(u8, &.{ 139, 139, 21, 14 }),
        .advance = 1000,
    };
    glyphs[1] = .{
        .code = 'B',
        .name = try alloc.dupe(u8, "B"),
        .charstring = try alloc.dupe(u8, &.{ 139, 139, 21, 14 }),
        .advance = 1000,
    };

    const fonts = [_]PageFont{
        .{
            .name = try alloc.dupe(u8, "F1"),
            .decoder = .{},
            .type1 = .{
                .bytes = try alloc.dupe(u8, ""),
                .local_subrs = try alloc.alloc([]u8, 0),
                .glyphs = glyphs,
            },
        },
    };
    defer {
        var font = fonts[0];
        font.deinit(alloc);
    }

    var runs = std.ArrayList(TextRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractTextRunsFromContentAppend(alloc, &runs, content, &fonts, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expectApproxEqAbs(@as(f64, 20), runs.items[0].advance_width, 0.001);
}

test "reader applies quote operator spacing and skips invisible runs" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 10 Tf\n" ++
        "12 TL\n" ++
        "10 20 Td\n" ++
        "3 Tr\n" ++
        "(Hide) Tj\n" ++
        "0 Tr\n" ++
        "5 2 (A B) \"\n" ++
        "ET\n";

    const fonts = [_]PageFont{
        .{
            .name = try alloc.dupe(u8, "F1"),
            .decoder = .{},
        },
    };
    defer {
        var font = fonts[0];
        font.deinit(alloc);
    }

    var runs = std.ArrayList(TextRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractTextRunsFromContentAppend(alloc, &runs, content, &fonts, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expectEqualStrings("A B", runs.items[0].text);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs.items[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 8), runs.items[0].y, 0.001);
    try std.testing.expectEqual(@as(i64, 0), runs.items[0].render_mode);
    try std.testing.expectApproxEqAbs(@as(f64, 2), runs.items[0].char_spacing, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 5), runs.items[0].word_spacing, 0.0001);
}

test "reader preserves stroke render mode on positioned runs" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 10 Tf\n" ++
        "10 20 Td\n" ++
        "1 Tr\n" ++
        "(Outline) Tj\n" ++
        "ET\n";

    const fonts = [_]PageFont{
        .{
            .name = try alloc.dupe(u8, "F1"),
            .decoder = .{},
        },
    };
    defer {
        var font = fonts[0];
        font.deinit(alloc);
    }

    var runs = std.ArrayList(TextRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractTextRunsFromContentAppend(alloc, &runs, content, &fonts, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expectEqual(@as(i64, 1), runs.items[0].render_mode);
}

test "reader preserves text fill and stroke colors on positioned runs" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 10 Tf\n" ++
        "1 0 0 rg\n" ++
        "0 0 1 RG\n" ++
        "10 20 Td\n" ++
        "2 Tr\n" ++
        "(Color) Tj\n" ++
        "ET\n";

    const fonts = [_]PageFont{
        .{
            .name = try alloc.dupe(u8, "F1"),
            .decoder = .{},
        },
    };
    defer {
        var font = fonts[0];
        font.deinit(alloc);
    }

    var runs = std.ArrayList(TextRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractTextRunsFromContentAppend(alloc, &runs, content, &fonts, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expectEqual(@as(i64, 2), runs.items[0].render_mode);
    try std.testing.expectEqual([4]u8{ 0xff, 0x00, 0x00, 0xff }, runs.items[0].fill_color);
    try std.testing.expectEqual([4]u8{ 0x00, 0x00, 0xff, 0xff }, runs.items[0].stroke_color);
}

test "reader preserves text stroke alpha and width on positioned runs" {
    const alloc = std.testing.allocator;
    const content =
        "q\n" ++
        "/GS1 gs\n" ++
        "3 w\n" ++
        "BT\n" ++
        "/F1 10 Tf\n" ++
        "10 20 Td\n" ++
        "1 Tr\n" ++
        "(Outline) Tj\n" ++
        "ET\n" ++
        "Q\n";

    const fonts = [_]PageFont{
        .{
            .name = try alloc.dupe(u8, "F1"),
            .decoder = .{},
        },
    };
    defer {
        var font = fonts[0];
        font.deinit(alloc);
    }

    const gstates = [_]PageExtGState{
        .{
            .name = try alloc.dupe(u8, "GS1"),
            .fill_alpha = 0x40,
            .stroke_alpha = 0x80,
        },
    };
    defer {
        var gstate = gstates[0];
        gstate.deinit(alloc);
    }

    var runs = std.ArrayList(TextRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractTextRunsFromContentAppend(alloc, &runs, content, &fonts, &gstates);
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expectEqual(@as(u8, 0x40), runs.items[0].alpha);
    try std.testing.expectEqual(@as(u8, 0x80), runs.items[0].stroke_alpha);
    try std.testing.expectApproxEqAbs(@as(f64, 3), runs.items[0].stroke_width, 0.0001);
}

test "reader extracts Type3 text glyph shapes" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 20 Tf\n" ++
        "1 0 0 rg\n" ++
        "10 20 Td\n" ++
        "(A) Tj\n" ++
        "ET\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 =
        "5 0 obj\n" ++
        "<< /Type /Font /Subtype /Type3 /PaintType 2 /FontMatrix [0.001 0 0 0.001 0 0] /Encoding << /Differences [65 /A] >> /FirstChar 65 /LastChar 65 /Widths [1000] /CharProcs << /A 6 0 R >> >>\n" ++
        "endobj\n";
    const glyph_content = "0 0 1000 1000 re\nf\n";
    const obj6 = try std.fmt.allocPrint(alloc, "6 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ glyph_content.len, glyph_content });
    defer alloc.free(obj6);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.4\n");
    const offsets = [_]usize{
        prefix.items.len,
        blk: {
            try prefix.appendSlice(alloc, obj1);
            break :blk prefix.items.len;
        },
        blk: {
            try prefix.appendSlice(alloc, obj2);
            break :blk prefix.items.len;
        },
        blk: {
            try prefix.appendSlice(alloc, obj3);
            break :blk prefix.items.len;
        },
        blk: {
            try prefix.appendSlice(alloc, obj4);
            break :blk prefix.items.len;
        },
        blk: {
            try prefix.appendSlice(alloc, obj5);
            break :blk prefix.items.len;
        },
    };
    _ = offsets;
    const obj1_offset: usize = "%PDF-1.4\n".len;
    const obj2_offset = obj1_offset + obj1.len;
    const obj3_offset = obj2_offset + obj2.len;
    const obj4_offset = obj3_offset + obj3.len;
    const obj5_offset = obj4_offset + obj4.len;
    const obj6_offset = obj5_offset + obj5.len;
    try prefix.appendSlice(alloc, obj6);
    const xref_offset = prefix.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 7\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset },
    );
    defer alloc.free(xref);
    try prefix.appendSlice(alloc, xref);
    try prefix.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 7 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try prefix.appendSlice(alloc, startxref);
    try prefix.appendSlice(alloc, "%%EOF\n");

    var parsed = try Reader.init(alloc, prefix.items);
    defer parsed.deinit();
    const runs = try parsed.extractPageType3TextShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expect(runs.len > 0);
    try std.testing.expect(runs[0].kind == .fill);
    try std.testing.expectEqual([4]u8{ 0xff, 0x00, 0x00, 0xff }, runs[0].color);
    const bounds = pathBounds(runs[0].points);
    try std.testing.expectApproxEqAbs(@as(f64, 10), bounds.min_x, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 20), bounds.min_y, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 30), bounds.max_x, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 40), bounds.max_y, 0.01);
}

test "parse Type3 glyph advance from d1" {
    const advance = parseType3GlyphAdvance("1000 0 0 0 1000 1000 d1\n0 0 1000 1000 re\nf\n").?;
    try std.testing.expectApproxEqAbs(@as(f64, 1000), advance[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), advance[1], 0.001);
}

test "reader prefers Type3 charproc advance over widths array" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 20 Tf\n" ++
        "10 20 Td\n" ++
        "(AA) Tj\n" ++
        "ET\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 =
        "5 0 obj\n" ++
        "<< /Type /Font /Subtype /Type3 /PaintType 2 /FontMatrix [0.001 0 0 0.001 0 0] /Encoding << /Differences [65 /A] >> /FirstChar 65 /LastChar 65 /Widths [500] /CharProcs << /A 6 0 R >> >>\n" ++
        "endobj\n";
    const glyph_content = "1000 0 d0\n0 0 1000 1000 re\nf\n";
    const obj6 = try std.fmt.allocPrint(alloc, "6 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ glyph_content.len, glyph_content });
    defer alloc.free(obj6);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj1);
    const obj2_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj2);
    const obj3_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj3);
    const obj4_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj4);
    const obj5_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj5);
    const obj6_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj6);
    const xref_offset = prefix.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 7\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset },
    );
    defer alloc.free(xref);
    try prefix.appendSlice(alloc, xref);
    try prefix.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 7 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try prefix.appendSlice(alloc, startxref);
    try prefix.appendSlice(alloc, "%%EOF\n");

    var parsed = try Reader.init(alloc, prefix.items);
    defer parsed.deinit();
    const runs = try parsed.extractPageType3TextShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expect(runs.len >= 2);
    const first = pathBounds(runs[0].points);
    const second = pathBounds(runs[1].points);
    try std.testing.expectApproxEqAbs(@as(f64, 10), first.min_x, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 30), first.max_x, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 30), second.min_x, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 50), second.max_x, 0.01);
}

test "reader preserves polygon clip on text runs" {
    const alloc = std.testing.allocator;
    const content =
        "q\n" ++
        "10 10 m\n" ++
        "30 10 l\n" ++
        "20 30 l\n" ++
        "W n\n" ++
        "BT\n" ++
        "/F1 12 Tf\n" ++
        "20 20 Td\n" ++
        "(A) Tj\n" ++
        "ET\n" ++
        "Q\n";

    const fonts = [_]PageFont{
        .{
            .name = try alloc.dupe(u8, "F1"),
            .decoder = .{},
        },
    };
    defer {
        var font = fonts[0];
        font.deinit(alloc);
    }

    var runs = std.ArrayList(TextRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractTextRunsFromContentAppend(alloc, &runs, content, &fonts, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expect(runs.items[0].clip_points != null);
    try std.testing.expectEqual(@as(usize, 3), runs.items[0].clip_points.?.len);
    try std.testing.expectEqual(FillRule.nonzero, runs.items[0].clip_fill_rule);
}

test "reader extracts image runs from simple image xobject draw" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n10 0 0 10 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u32, 1), runs[0].width);
    try std.testing.expectEqual(@as(u32, 1), runs[0].height);
    try std.testing.expectApproxEqAbs(@as(f64, 20), runs[0].x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 30), runs[0].y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].draw_width, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].draw_height, 0.001);
}

test "reader decodes DeviceCMYK image xobject draw" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0, 0 };
    const content = "q\n1 0 0 1 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceCMYK /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[2]);
}

test "reader decodes ICCBased alternate image xobject draw" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const profile_data = &.{0};
    const content = "q\n10 0 0 10 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 6 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /N 3 /Alternate /DeviceRGB /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ profile_data.len, profile_data },
        ),
        try std.fmt.allocPrint(
            alloc,
            "6 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace [/ICCBased 5 0 R] /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);
    defer alloc.free(objects[5]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[2]);
}

test "reader decodes CalRGB image xobject draw" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n10 0 0 10 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace [/CalRGB << /WhitePoint [1 1 1] >>] /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[2]);
}

test "reader decodes CalGray image xobject draw" {
    const alloc = std.testing.allocator;
    const image_data = &.{127};
    const content = "q\n10 0 0 10 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace [/CalGray << /WhitePoint [1 1 1] >>] /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 127), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 127), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 127), runs[0].rgba[2]);
}

test "reader decodes indexed image xobject draw" {
    const alloc = std.testing.allocator;
    const image_data = &.{1};
    const lookup = &.{ 0, 0, 0, 255, 0, 0 };
    const content = "q\n1 0 0 1 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace [/Indexed /DeviceRGB 1 <{x}>] /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ lookup, image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[2]);
}

test "reader decodes Lab image xobject draw" {
    const alloc = std.testing.allocator;
    const pixel = [_]u8{ 200, 128, 128 };
    const content = "q\n1 0 0 1 10 10 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace [/Lab << /WhitePoint [0.95047 1 1.08883] /Range [-128 127 -128 127] >>] /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ pixel.len, &pixel },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var pdf = try Reader.init(alloc, sample);
    defer pdf.deinit();

    const runs = try pdf.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].rgba[0] > 0);
    try std.testing.expect(runs[0].rgba[1] > 0);
    try std.testing.expect(runs[0].rgba[2] > 0);
}

test "reader decodes Separation image xobject draw" {
    const alloc = std.testing.allocator;
    const pixel = [_]u8{255};
    const content = "q\n1 0 0 1 10 10 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace [/Separation /Spot /DeviceRGB 6 0 R] /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ pixel.len, &pixel },
        ),
        "6 0 obj\n<< /FunctionType 2 /Domain [0 1] /C0 [1 1 1] /C1 [1 0 0] /N 1 >>\nendobj\n",
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var pdf = try Reader.init(alloc, sample);
    defer pdf.deinit();

    const runs = try pdf.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[2]);
}

test "reader decodes one-component DeviceN image xobject draw" {
    const alloc = std.testing.allocator;
    const pixel = [_]u8{255};
    const content = "q\n1 0 0 1 10 10 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace [/DeviceN [/Spot] /DeviceGray 6 0 R] /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ pixel.len, &pixel },
        ),
        "6 0 obj\n<< /FunctionType 2 /Domain [0 1] /C0 [1] /C1 [0] /N 1 >>\nendobj\n",
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var pdf = try Reader.init(alloc, sample);
    defer pdf.deinit();

    const runs = try pdf.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[2]);
}

test "reader decodes DCTDecode image xobject draw" {
    const alloc = std.testing.allocator;
    const jpeg_base64 =
        "/9j/4AAQSkZJRgABAgAAAQABAAD//gAQTGF2YzYyLjI4LjEwMAD/2wBDAAgEBAQEBAUFBQUFBQYGBgYGBgYGBgYGBgYHBwcICAgHBwcGBgcHCAgICAkJCQgICAgJCQoKCgwMCwsODg4RERT/xABLAAEBAAAAAAAAAAAAAAAAAAAABwEBAAAAAAAAAAAAAAAAAAAAABABAAAAAAAAAAAAAAAAAAAAABEBAAAAAAAAAAAAAAAAAAAAAP/AABEIAAEAAgMBIgACEQADEQD/2gAMAwEAAhEDEQA/AL+AD//Z";
    const jpeg_size = try std.base64.standard.Decoder.calcSizeForSlice(jpeg_base64);
    const jpeg_bytes = try alloc.alloc(u8, jpeg_size);
    defer alloc.free(jpeg_bytes);
    try std.base64.standard.Decoder.decode(jpeg_bytes, jpeg_base64);

    const content = "q\n2 0 0 1 10 10 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 2 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ jpeg_bytes.len, jpeg_bytes },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var pdf = try Reader.init(alloc, sample);
    defer pdf.deinit();

    const runs = try pdf.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u32, 2), runs[0].width);
    try std.testing.expectEqual(@as(u32, 1), runs[0].height);
    try std.testing.expect(runs[0].rgba[0] > 200);
    try std.testing.expect(runs[0].rgba[1] > 200);
    try std.testing.expect(runs[0].rgba[2] > 200);
}

test "reader decodes JPXDecode image xobject draw" {
    const alloc = std.testing.allocator;
    const pixels = [_]u8{
        255, 0,   0,
        0,   255, 0,
    };
    const params = image_lib.jpeg2000.EncodeParams{
        .width = 2,
        .height = 1,
        .components = 3,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .jp2,
    };
    const jp2_bytes = try image_lib.jpeg2000.encodeU8Bytes(alloc, pixels[0..], &params);
    defer alloc.free(jp2_bytes);

    const content = "q\n2 0 0 1 10 10 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 2 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /JPXDecode /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ jp2_bytes.len, jp2_bytes },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var pdf = try Reader.init(alloc, sample);
    defer pdf.deinit();

    const runs = try pdf.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u32, 2), runs[0].width);
    try std.testing.expectEqual(@as(u32, 1), runs[0].height);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[2]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[4]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[5]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[6]);
}

test "reader applies ExtGState alpha to image runs" {
    const alloc = std.testing.allocator;
    const image_data = [_]u8{ 0, 0, 0 };
    const content = "q\n/GS1 gs\n1 0 0 1 10 10 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /ExtGState << /GS1 6 0 R >> /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, &image_data },
        ),
        "6 0 obj\n<< /ca 0.25 >>\nendobj\n",
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var pdf = try Reader.init(alloc, sample);
    defer pdf.deinit();

    const runs = try pdf.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 64), runs[0].alpha);
}

test "reader applies ExtGState blend mode to image runs" {
    const alloc = std.testing.allocator;
    const image_data = [_]u8{ 0, 0, 0 };
    const content = "q\n/GS1 gs\n1 0 0 1 10 10 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /ExtGState << /GS1 6 0 R >> /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, &image_data },
        ),
        "6 0 obj\n<< /BM /Screen >>\nendobj\n",
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var pdf = try Reader.init(alloc, sample);
    defer pdf.deinit();

    const runs = try pdf.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(BlendMode.screen, runs[0].blend_mode);
}

fn packBitsMsbAlloc(alloc: Allocator, bits: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, @divFloor(bits.len + 7, 8));
    @memset(out, 0);
    for (bits, 0..) |bit, i| {
        if (bit != '1') continue;
        out[i / 8] |= @as(u8, 1) << @intCast(7 - (i % 8));
    }
    return out;
}

test "reader decodes CCITTFaxDecode image xobject draw" {
    const alloc = std.testing.allocator;
    const bits =
        "000000000001" ++
        "0111" ++
        "11" ++
        "0111" ++
        "11" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001";
    const image_data = try packBitsMsbAlloc(alloc, bits);
    defer alloc.free(image_data);
    const content = "q\n1 0 0 1 20 30 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 8 /Height 1 /ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /CCITTFaxDecode /DecodeParms << /K 0 /Columns 8 /Rows 1 >> /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u32, 8), runs[0].width);
    try std.testing.expectEqual(@as(u32, 1), runs[0].height);
    try std.testing.expectEqual(@as(u8, 0xff), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0xff), runs[0].rgba[4]);
    try std.testing.expectEqual(@as(u8, 0x00), runs[0].rgba[8]);
    try std.testing.expectEqual(@as(u8, 0x00), runs[0].rgba[12]);
}

test "reader decodes mixed-mode CCITTFaxDecode image xobject draw" {
    const alloc = std.testing.allocator;
    const bits =
        "000000000001" ++
        "1" ++
        "0111" ++
        "11" ++
        "0111" ++
        "11" ++
        "000000000001" ++
        "0" ++
        "1" ++
        "1" ++
        "1" ++
        "1" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001";
    const image_data = try packBitsMsbAlloc(alloc, bits);
    defer alloc.free(image_data);
    const content = "q\n1 0 0 1 20 30 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 8 /Height 2 /ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /CCITTFaxDecode /DecodeParms << /K 2 /Columns 8 /Rows 2 >> /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u32, 8), runs[0].width);
    try std.testing.expectEqual(@as(u32, 2), runs[0].height);
    try std.testing.expectEqual(@as(u8, 0xff), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0x00), runs[0].rgba[8]);
    try std.testing.expectEqual(@as(u8, 0xff), runs[0].rgba[32]);
    try std.testing.expectEqual(@as(u8, 0x00), runs[0].rgba[40]);
}

test "reader decodes CCITTFaxDecode BlackIs1 image xobject draw" {
    const alloc = std.testing.allocator;
    const bits =
        "000000000001" ++
        "0111" ++
        "11" ++
        "0111" ++
        "11" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001";
    const image_data = try packBitsMsbAlloc(alloc, bits);
    defer alloc.free(image_data);
    const content = "q\n1 0 0 1 20 30 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 8 /Height 1 /ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /CCITTFaxDecode /DecodeParms << /K 0 /Columns 8 /Rows 1 /BlackIs1 true >> /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 0x00), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0xff), runs[0].rgba[8]);
}

test "reader applies Decode array to grayscale image xobject" {
    const alloc = std.testing.allocator;
    const image_data = &.{0};
    const content = "q\n1 0 0 1 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceGray /BitsPerComponent 8 /Decode [1 0] /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[2]);
}

test "reader decodes 1-bit image mask xobject" {
    const alloc = std.testing.allocator;
    const image_data = &.{0b10000000};
    const content = "q\n1 0 0 1 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /ImageMask true /Width 1 /Height 1 /BitsPerComponent 1 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[2]);
    try std.testing.expectEqual(@as(u8, 0xff), runs[0].rgba[3]);
}

test "reader applies soft mask image alpha" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const smask_data = &.{128};
    const content = "q\n1 0 0 1 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /SMask 6 0 R /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
        try std.fmt.allocPrint(
            alloc,
            "6 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceGray /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ smask_data.len, smask_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);
    defer alloc.free(objects[5]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 255), runs[0].rgba[0]);
    try std.testing.expectEqual(@as(u8, 128), runs[0].rgba[3]);
}

test "reader applies color key mask array" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n1 0 0 1 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Mask [255 255 0 0 0 0] /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 0), runs[0].rgba[3]);
}

test "reader applies image mask stream alpha" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const mask_data = &.{0b10000000};
    const content = "q\n1 0 0 1 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Mask 6 0 R /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
        try std.fmt.allocPrint(
            alloc,
            "6 0 obj\n<< /Type /XObject /Subtype /Image /ImageMask true /Width 1 /Height 1 /BitsPerComponent 1 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ mask_data.len, mask_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);
    defer alloc.free(objects[5]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 0xff), runs[0].rgba[3]);
}

test "apply soft mask alpha updates multiple pixels" {
    var rgba = [_]u8{
        10,  20,  30,  255,
        40,  50,  60,  255,
        70,  80,  90,  255,
        100, 110, 120, 255,
    };
    const smask = [_]u8{
        1, 1, 1, 255,
        2, 2, 2, 255,
        3, 3, 3, 255,
        4, 4, 4, 255,
    };
    applySoftMaskAlpha(&rgba, &smask);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, &.{ rgba[3], rgba[7], rgba[11], rgba[15] });
}

test "apply color key mask zeroes matching alpha across vector lane block" {
    var rgba = [_]u8{
        255, 0,   0,   255,
        0,   255, 0,   255,
        255, 0,   0,   255,
        0,   0,   255, 255,
    };
    const mask_array = [_]syntax.Object{
        .{ .integer = 255 },
        .{ .integer = 255 },
        .{ .integer = 0 },
        .{ .integer = 0 },
        .{ .integer = 0 },
        .{ .integer = 0 },
    };
    applyColorKeyMask(&rgba, &mask_array);
    try std.testing.expectEqual(@as(u8, 0), rgba[3]);
    try std.testing.expectEqual(@as(u8, 255), rgba[7]);
    try std.testing.expectEqual(@as(u8, 0), rgba[11]);
    try std.testing.expectEqual(@as(u8, 255), rgba[15]);
}

test "apply explicit mask alpha uses mask alpha or grayscale channel" {
    var rgba = [_]u8{
        10,  20,  30,  255,
        40,  50,  60,  255,
        70,  80,  90,  255,
        100, 110, 120, 255,
    };
    const mask = [_]u8{
        11, 11, 11, 255,
        22, 22, 22, 255,
        33, 33, 33, 128,
        44, 44, 44, 0,
    };

    applyExplicitMaskAlpha(&rgba, &mask, false);

    try std.testing.expectEqual(@as(u8, 11), rgba[3]);
    try std.testing.expectEqual(@as(u8, 22), rgba[7]);
    try std.testing.expectEqual(@as(u8, 33), rgba[11]);
    try std.testing.expectEqual(@as(u8, 44), rgba[15]);
}

test "reader preserves rotated image xobject transform" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n0 10 -10 0 20 30 cm\n/Im1 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ image_data.len, image_data },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");

    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }

    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    const runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectApproxEqAbs(@as(f64, 0), runs[0].a, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].b, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, -10), runs[0].c, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), runs[0].d, 0.001);
}

test "reader extracts page box from mediabox" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello) Tj\nET\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [10 20 210 320] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const box = try reader.extractPageBox(1);
    try std.testing.expectApproxEqAbs(@as(f64, 10), box.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 20), box.min_y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 210), box.max_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 320), box.max_y, 0.001);
}

test "reader extracts filled rectangle shape runs" {
    const alloc = std.testing.allocator;
    const content = "0.5 g\n10 20 30 40 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].kind == .fill);
    try std.testing.expectEqual(@as(u8, 128), runs[0].color[0]);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].points[0][0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 20), runs[0].points[0][1], 0.001);
}

test "reader extracts shape fill color from cs sc operators" {
    const alloc = std.testing.allocator;
    const content = "/DeviceRGB cs\n1 0 0 sc\n10 20 30 40 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 255), runs[0].color[0]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].color[1]);
    try std.testing.expectEqual(@as(u8, 0), runs[0].color[2]);
}

test "reader extracts shape stroke color from CS SC operators" {
    const alloc = std.testing.allocator;
    const content = "/DeviceCMYK CS\n1 0 0 0 SC\n10 20 m\n30 40 l\nS\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].kind == .stroke);
    try std.testing.expectEqual(@as(u8, 0), runs[0].color[0]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].color[1]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].color[2]);
}

test "reader preserves rectangle clip on shape runs" {
    const alloc = std.testing.allocator;
    const content = "10 10 20 20 re\nW\nn\n0 0 1 rg\n0 0 50 50 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].clip_box != null);
    const clip = runs[0].clip_box.?;
    try std.testing.expectApproxEqAbs(@as(f64, 10), clip.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 30), clip.max_x, 0.001);
}

test "reader preserves polygon clip on shape runs" {
    const alloc = std.testing.allocator;
    const content =
        "q\n" ++
        "10 10 m\n" ++
        "30 10 l\n" ++
        "20 30 l\n" ++
        "W* n\n" ++
        "0 0 0 rg\n" ++
        "12 12 12 12 re\n" ++
        "f\n" ++
        "Q\n";

    var runs = std.ArrayList(ShapeRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractShapeRunsFromContentAppend(alloc, &runs, content, &.{}, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expect(runs.items[0].clip_points != null);
    try std.testing.expectEqual(@as(usize, 3), runs.items[0].clip_points.?.len);
    try std.testing.expectEqual(FillRule.even_odd, runs.items[0].clip_fill_rule);
}

test "reader intersects repeated clips on shape runs" {
    const alloc = std.testing.allocator;
    const content =
        "10 10 40 40 re\nW\nn\n" ++
        "20 0 40 60 re\nW\nn\n" ++
        "0 0 1 rg\n0 0 60 60 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].clip_box != null);
    const clip = runs[0].clip_box.?;
    try std.testing.expectApproxEqAbs(@as(f64, 20), clip.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), clip.min_y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 50), clip.max_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 50), clip.max_y, 0.001);
}

test "reader preserves repeated rectangular clips as polygon clip" {
    const alloc = std.testing.allocator;
    const content =
        "q\n" ++
        "10 10 40 40 re\n" ++
        "W n\n" ++
        "20 20 40 40 re\n" ++
        "W n\n" ++
        "0 0 0 rg\n" ++
        "20 20 10 10 re\n" ++
        "f\n" ++
        "Q\n";

    var runs = std.ArrayList(ShapeRun).empty;
    defer {
        for (runs.items) |*run| run.deinit(alloc);
        runs.deinit(alloc);
    }

    try extractShapeRunsFromContentAppend(alloc, &runs, content, &.{}, &.{});
    try std.testing.expectEqual(@as(usize, 1), runs.items.len);
    try std.testing.expect(runs.items[0].clip_points != null);
    try std.testing.expectEqual(@as(usize, 4), runs.items[0].clip_points.?.len);
    const clip = pathBounds(runs.items[0].clip_points.?);
    try std.testing.expectApproxEqAbs(@as(f64, 20), clip.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 20), clip.min_y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 50), clip.max_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 50), clip.max_y, 0.001);
}

test "reader intersects repeated polygon clips on shape runs" {
    const alloc = std.testing.allocator;
    const content =
        "10 10 m\n50 10 l\n30 50 l\nW n\n" ++
        "20 5 m\n55 25 l\n15 45 l\nW n\n" ++
        "0 0 40 60 re\nf\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj1);
    const obj2_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj2);
    const obj3_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj3);
    const obj4_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj4);
    const xref_offset = prefix.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try prefix.appendSlice(alloc, xref);
    try prefix.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try prefix.appendSlice(alloc, startxref);
    try prefix.appendSlice(alloc, "%%EOF\n");

    var parsed = try Reader.init(alloc, prefix.items);
    defer parsed.deinit();
    const runs = try parsed.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].clip_points != null);
    try std.testing.expect(runs[0].clip_points.?.len >= 3);
    const clip = pathBounds(runs[0].clip_points.?);
    try std.testing.expect(clip.min_x >= 15);
    try std.testing.expect(clip.max_x <= 50);
    try std.testing.expect(clip.min_y >= 10);
    try std.testing.expect(clip.max_y <= 45);
}

test "reader applies cmyk fill color to shape runs" {
    const alloc = std.testing.allocator;
    const content = "1 0 0 0 k\n10 20 30 40 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 0), runs[0].color[0]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].color[1]);
    try std.testing.expectEqual(@as(u8, 255), runs[0].color[2]);
}

test "reader applies ExtGState alpha to shape runs" {
    const alloc = std.testing.allocator;
    const content = "/GS1 gs\n1 0 0 rg\n10 20 30 40 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /ExtGState << /GS1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /ca 0.5 /CA 0.25 >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(u8, 128), runs[0].color[3]);
}

test "reader applies ExtGState blend mode to shape runs" {
    const alloc = std.testing.allocator;
    const content = "/GS1 gs\n1 0 0 rg\n10 20 30 40 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /ExtGState << /GS1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /BM /Darken >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(BlendMode.darken, runs[0].blend_mode);
}

test "reader extracts polygon shape runs from moveto lineto closepath" {
    const alloc = std.testing.allocator;
    const content = "1 0 0 rg\n10 10 m\n40 10 l\n25 35 l\nh\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].kind == .fill);
    try std.testing.expectEqual(@as(usize, 3), runs[0].points.len);
    try std.testing.expectApproxEqAbs(@as(f64, 25), runs[0].points[2][0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 35), runs[0].points[2][1], 0.001);
}

test "reader maps f* to even-odd fill rule" {
    const alloc = std.testing.allocator;
    const content = "0 1 0 rg\n10 10 m\n40 10 l\n40 40 l\n10 40 l\n10 10 l\n40 10 l\n40 40 l\n10 40 l\nh\nf*\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].kind == .fill);
    try std.testing.expect(runs[0].fill_rule == .even_odd);
}

test "reader extracts open stroked path runs" {
    const alloc = std.testing.allocator;
    const content = "0 0 1 RG\n4 w\n10 10 m\n90 90 l\nS\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].kind == .stroke);
    try std.testing.expect(!runs[0].closed);
    try std.testing.expectEqual(@as(usize, 2), runs[0].points.len);
    try std.testing.expectApproxEqAbs(@as(f64, 4), runs[0].stroke_width, 0.001);
}

test "reader preserves stroke cap and join state on runs" {
    const alloc = std.testing.allocator;
    const content = "2 J\n1 j\n7 M\n0 0 1 RG\n4 w\n10 10 m\n90 90 l\nS\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].line_cap == .square);
    try std.testing.expect(runs[0].line_join == .round);
    try std.testing.expectApproxEqAbs(@as(f64, 7), runs[0].miter_limit, 0.001);
}

test "reader preserves dash state on stroked runs" {
    const alloc = std.testing.allocator;
    const content = "[6 3] 2 d\n0 0 1 RG\n4 w\n10 10 m\n90 10 l\nS\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].dash_array != null);
    try std.testing.expectEqual(@as(usize, 2), runs[0].dash_array.?.len);
    try std.testing.expectApproxEqAbs(@as(f64, 6), runs[0].dash_array.?[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 3), runs[0].dash_array.?[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2), runs[0].dash_phase, 0.001);
}

test "reader scales stroke width through graphics matrix" {
    const alloc = std.testing.allocator;
    const content = "q\n2 0 0 2 0 0 cm\n2 w\n10 10 m\n20 10 l\nS\nQ\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj1);
    const obj2_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj2);
    const obj3_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj3);
    const obj4_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj4);
    const xref_offset = prefix.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try prefix.appendSlice(alloc, xref);
    try prefix.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try prefix.appendSlice(alloc, startxref);
    try prefix.appendSlice(alloc, "%%EOF\n");

    var parsed = try Reader.init(alloc, prefix.items);
    defer parsed.deinit();
    const runs = try parsed.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectApproxEqAbs(@as(f64, 4), runs[0].stroke_width, 0.001);
}

test "reader scales dash pattern through graphics matrix" {
    const alloc = std.testing.allocator;
    const content = "q\n2 0 0 2 0 0 cm\n[3 1] 2 d\n2 w\n10 10 m\n20 10 l\nS\nQ\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj1);
    const obj2_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj2);
    const obj3_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj3);
    const obj4_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj4);
    const xref_offset = prefix.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try prefix.appendSlice(alloc, xref);
    try prefix.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try prefix.appendSlice(alloc, startxref);
    try prefix.appendSlice(alloc, "%%EOF\n");

    var parsed = try Reader.init(alloc, prefix.items);
    defer parsed.deinit();
    const runs = try parsed.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].dash_array != null);
    try std.testing.expectApproxEqAbs(@as(f64, 6), runs[0].dash_array.?[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2), runs[0].dash_array.?[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 4), runs[0].dash_phase, 0.001);
}

test "reader extracts fill and stroke runs from closed bezier path" {
    const alloc = std.testing.allocator;
    const content = "0 1 0 rg\n1 0 0 RG\n10 10 m\n20 40 40 40 50 10 c\nb\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 5 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 2), runs.len);
    try std.testing.expect(runs[0].kind == .fill);
    try std.testing.expect(runs[1].kind == .stroke);
    try std.testing.expect(runs[1].closed);
    try std.testing.expect(runs[0].points.len > 6);
}

test "reader can extract plain text through xref stream" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello XRef Stream) Tj\nET\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    const obj1_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj1);
    const obj2_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj2);
    const obj3_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj3);
    const obj4_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj4);
    const obj5_offset = prefix.items.len;

    var xref_stream = std.ArrayList(u8).empty;
    defer xref_stream.deinit(alloc);

    const XrefWriter = struct {
        fn appendRecord(out: *std.ArrayList(u8), alloc_inner: Allocator, ty: u8, field2: u32, field3: u16) !void {
            try out.append(alloc_inner, ty);
            try out.append(alloc_inner, @intCast((field2 >> 24) & 0xff));
            try out.append(alloc_inner, @intCast((field2 >> 16) & 0xff));
            try out.append(alloc_inner, @intCast((field2 >> 8) & 0xff));
            try out.append(alloc_inner, @intCast(field2 & 0xff));
            try out.append(alloc_inner, @intCast((field3 >> 8) & 0xff));
            try out.append(alloc_inner, @intCast(field3 & 0xff));
        }
    };

    try XrefWriter.appendRecord(&xref_stream, alloc, 0, 0, 65535);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj1_offset), 0);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj2_offset), 0);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj3_offset), 0);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj4_offset), 0);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj5_offset), 0);

    const obj5 = try std.fmt.allocPrint(
        alloc,
        "5 0 obj\n<< /Type /XRef /Size 6 /Root 1 0 R /Index [0 6] /W [1 4 2] /Length {d} >>\nstream\n",
        .{xref_stream.items.len},
    );
    defer alloc.free(obj5);
    try prefix.appendSlice(alloc, obj5);
    try prefix.appendSlice(alloc, xref_stream.items);
    try prefix.appendSlice(alloc, "\nendstream\nendobj\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, obj5_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), try reader.pageCount());
    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello XRef Stream\n", text);
}

test "reader can extract plain text through xref stream with object stream entries" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello ObjStm) Tj\nET\n";
    const obj1_body = "<< /Type /Catalog /Pages 2 0 R >>";
    const obj2_body = "<< /Type /Pages /Count 1 /Kids [3 0 R] >>";
    const obj3_body = "<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);

    const obj2_offset_in_stream = obj1_body.len + 1;
    const obj3_offset_in_stream = obj2_offset_in_stream + obj2_body.len + 1;
    const obj6_header_data = try std.fmt.allocPrint(alloc, "1 0 2 {d} 3 {d} ", .{ obj2_offset_in_stream, obj3_offset_in_stream });
    defer alloc.free(obj6_header_data);
    const obj6_stream_data = try std.mem.concat(alloc, u8, &.{ obj6_header_data, obj1_body, " ", obj2_body, " ", obj3_body });
    defer alloc.free(obj6_stream_data);
    const obj6 = try std.fmt.allocPrint(
        alloc,
        "6 0 obj\n<< /Type /ObjStm /N 3 /First {d} /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
        .{ obj6_header_data.len, obj6_stream_data.len, obj6_stream_data },
    );
    defer alloc.free(obj6);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    const obj4_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj4);
    const obj6_offset = prefix.items.len;
    try prefix.appendSlice(alloc, obj6);
    const obj5_offset = prefix.items.len;

    var xref_stream = std.ArrayList(u8).empty;
    defer xref_stream.deinit(alloc);

    const XrefWriter = struct {
        fn appendRecord(out: *std.ArrayList(u8), alloc_inner: Allocator, ty: u8, field2: u32, field3: u16) !void {
            try out.append(alloc_inner, ty);
            try out.append(alloc_inner, @intCast((field2 >> 24) & 0xff));
            try out.append(alloc_inner, @intCast((field2 >> 16) & 0xff));
            try out.append(alloc_inner, @intCast((field2 >> 8) & 0xff));
            try out.append(alloc_inner, @intCast(field2 & 0xff));
            try out.append(alloc_inner, @intCast((field3 >> 8) & 0xff));
            try out.append(alloc_inner, @intCast(field3 & 0xff));
        }
    };

    try XrefWriter.appendRecord(&xref_stream, alloc, 0, 0, 65535);
    try XrefWriter.appendRecord(&xref_stream, alloc, 2, 6, 0);
    try XrefWriter.appendRecord(&xref_stream, alloc, 2, 6, 1);
    try XrefWriter.appendRecord(&xref_stream, alloc, 2, 6, 2);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj4_offset), 0);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj5_offset), 0);
    try XrefWriter.appendRecord(&xref_stream, alloc, 1, @intCast(obj6_offset), 0);

    const obj5 = try std.fmt.allocPrint(
        alloc,
        "5 0 obj\n<< /Type /XRef /Size 7 /Root 1 0 R /Index [0 7] /W [1 4 2] /Length {d} >>\nstream\n",
        .{xref_stream.items.len},
    );
    defer alloc.free(obj5);
    try prefix.appendSlice(alloc, obj5);
    try prefix.appendSlice(alloc, xref_stream.items);
    try prefix.appendSlice(alloc, "\nendstream\nendobj\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, obj5_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), try reader.pageCount());
    const text = try reader.extractPlainTextAlloc();
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello ObjStm\n", text);
}

test "reader preserves comparable paint order across images shapes and text" {
    const alloc = std.testing.allocator;
    const image_data = [_]u8{ 0xff, 0, 0 };
    const content =
        "q\n" ++
        "1 0 0 1 10 10 cm\n" ++
        "/Im1 Do\n" ++
        "Q\n" ++
        "10 10 20 20 re\n" ++
        "f\n" ++
        "BT\n" ++
        "1 0 0 1 40 40 Tm\n" ++
        "(Hi) Tj\n" ++
        "ET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = try std.fmt.allocPrint(
        alloc,
        "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
        .{ image_data.len, image_data },
    );
    defer alloc.free(obj5);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 6\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 6 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    var reader = try Reader.init(alloc, out.items);
    defer reader.deinit();

    const image_runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (image_runs) |*run| run.deinit(alloc);
        alloc.free(image_runs);
    }
    const shape_runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (shape_runs) |*run| run.deinit(alloc);
        alloc.free(shape_runs);
    }
    const text_runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (text_runs) |*run| run.deinit(alloc);
        alloc.free(text_runs);
    }

    try std.testing.expectEqual(@as(usize, 1), image_runs.len);
    try std.testing.expectEqual(@as(usize, 1), shape_runs.len);
    try std.testing.expectEqual(@as(usize, 1), text_runs.len);
    try std.testing.expect(image_runs[0].paint_order < shape_runs[0].paint_order);
    try std.testing.expect(shape_runs[0].paint_order < text_runs[0].paint_order);
}

test "reader extracts text and shapes through form xobject" {
    const alloc = std.testing.allocator;
    const form_content =
        "10 10 20 20 re\n" ++
        "f\n" ++
        "BT\n/F1 12 Tf\n1 0 0 1 40 40 Tm\n(Hi) Tj\nET\n";
    const page_content = "q\n1 0 0 1 5 5 cm\n/Fm1 Do\nQ\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /XObject << /Fm1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content });
    defer alloc.free(obj4);
    const obj5 = try std.fmt.allocPrint(
        alloc,
        "5 0 obj\n<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] /Resources << /Font << /F1 6 0 R >> >> /Length {d} >>\nstream\n{s}endstream\nendobj\n",
        .{ form_content.len, form_content },
    );
    defer alloc.free(obj5);
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /StandardEncoding >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 7\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 7 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    var reader = try Reader.init(alloc, out.items);
    defer reader.deinit();

    const text_runs = try reader.extractPageTextRunsAlloc(1);
    defer {
        for (text_runs) |*run| run.deinit(alloc);
        alloc.free(text_runs);
    }
    const shape_runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (shape_runs) |*run| run.deinit(alloc);
        alloc.free(shape_runs);
    }

    try std.testing.expectEqual(@as(usize, 1), text_runs.len);
    try std.testing.expectEqualStrings("Hi", text_runs[0].text);
    try std.testing.expectEqual(@as(usize, 1), shape_runs.len);
    try std.testing.expect(shape_runs[0].paint_order < text_runs[0].paint_order);
}

test "reader extracts images through form xobject" {
    const alloc = std.testing.allocator;
    const image_data = [_]u8{ 0xff, 0, 0 };
    const form_content = "q\n1 0 0 1 10 10 cm\n/Im1 Do\nQ\n";
    const page_content = "q\n/Fm1 Do\nQ\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /XObject << /Fm1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content });
    defer alloc.free(obj4);
    const obj5 = try std.fmt.allocPrint(
        alloc,
        "5 0 obj\n<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] /Resources << /XObject << /Im1 6 0 R >> >> /Length {d} >>\nstream\n{s}endstream\nendobj\n",
        .{ form_content.len, form_content },
    );
    defer alloc.free(obj5);
    const obj6 = try std.fmt.allocPrint(
        alloc,
        "6 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
        .{ image_data.len, image_data },
    );
    defer alloc.free(obj6);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 7\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 7 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    var reader = try Reader.init(alloc, out.items);
    defer reader.deinit();

    const image_runs = try reader.extractPageImageRunsAlloc(1);
    defer {
        for (image_runs) |*run| run.deinit(alloc);
        alloc.free(image_runs);
    }
    try std.testing.expectEqual(@as(usize, 1), image_runs.len);
}

test "reader extracts axial shading runs" {
    const alloc = std.testing.allocator;
    const content = "/Sh1 sh\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Shading << /Sh1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        "5 0 obj\n<< /ShadingType 2 /ColorSpace /DeviceRGB /Coords [0 0 100 0] /Function 6 0 R /Extend [true true] >>\nendobj\n",
        "6 0 obj\n<< /FunctionType 2 /Domain [0 1] /C0 [1 0 0] /C1 [0 0 1] /N 1 >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShadingRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@FieldType(ShadingRun, "kind").axial, runs[0].kind);
    try std.testing.expectEqual([4]u8{ 0xff, 0x00, 0x00, 0xff }, runs[0].c0);
    try std.testing.expectEqual([4]u8{ 0x00, 0x00, 0xff, 0xff }, runs[0].c1);
}

test "reader assigns group ids for isolated transparency form runs" {
    const alloc = std.testing.allocator;
    const form_content = "1 0 0 rg\n10 10 20 20 re\nf\n";
    const page_content = "/Fm1 Do\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /XObject << /Fm1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] /Group << /S /Transparency /I true >> /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ form_content.len, form_content },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var reader = try Reader.init(alloc, sample);
    defer reader.deinit();
    const runs = try reader.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].group_id != null);
}

test "reader extracts colored tiling pattern runs" {
    const alloc = std.testing.allocator;
    const pattern_content = "1 0 0 rg\n0 0 5 10 re\nf\n";
    const page_content = "/Pattern cs\n/P1 scn\n0 0 40 20 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Pattern << /P1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 10 10] /XStep 10 /YStep 10 /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ pattern_content.len, pattern_content },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPagePatternRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(usize, 1), runs[0].tile_shape_runs.len);
    try std.testing.expectApproxEqAbs(@as(f64, 0), runs[0].pattern_bbox.min_x, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0), runs[0].pattern_bbox.min_y, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].pattern_bbox.max_x, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].pattern_bbox.max_y, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].pattern_x_step, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), runs[0].pattern_y_step, 0.0001);
}

test "reader extracts uncolored tiling pattern base color" {
    const alloc = std.testing.allocator;
    const pattern_content = "0 0 5 10 re\nf\n";
    const page_content = "/Pattern cs\n0 1 0 /P1 scn\n0 0 40 20 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Pattern << /P1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /Pattern /PatternType 1 /PaintType 2 /TilingType 1 /BBox [0 0 10 10] /XStep 10 /YStep 10 /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ pattern_content.len, pattern_content },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPagePatternRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].base_color != null);
    try std.testing.expectEqual([4]u8{ 0x00, 0xff, 0x00, 0xff }, runs[0].base_color.?);
}

test "reader extracts vector text pattern runs" {
    const alloc = std.testing.allocator;
    const page_content =
        "BT\n" ++
        "/F1 20 Tf\n" ++
        "/Pattern cs\n" ++
        "/P1 scn\n" ++
        "10 10 Td\n" ++
        "(A) Tj\n" ++
        "ET\n";
    const pattern_content = "1 0 0 rg\n0 0 5 10 re\nf\n";
    const glyph_content = "1000 0 d0\n0 0 1000 1000 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> /Pattern << /P1 6 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        "5 0 obj\n<< /Type /Font /Subtype /Type3 /PaintType 2 /FontMatrix [0.001 0 0 0.001 0 0] /Encoding << /Differences [65 /A] >> /FirstChar 65 /LastChar 65 /Widths [1000] /CharProcs << /A 7 0 R >> >>\nendobj\n",
        try std.fmt.allocPrint(
            alloc,
            "6 0 obj\n<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 10 10] /XStep 10 /YStep 10 /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ pattern_content.len, pattern_content },
        ),
        try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ glyph_content.len, glyph_content }),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[5]);
    defer alloc.free(objects[6]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 8\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 8 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPageVectorTextPatternRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(PatternRun.kind.fill, runs[0].kind);
    try std.testing.expectEqual(PatternRun.mode.tiling, runs[0].mode);
    try std.testing.expect(runs[0].points.len >= 3);
    try std.testing.expectEqual(@as(usize, 1), runs[0].tile_shape_runs.len);
}

test "reader preserves nested tiling patterns" {
    const alloc = std.testing.allocator;
    const page_content = "/Pattern cs\n/P1 scn\n0 0 40 40 re\nf\n";
    const outer_content = "/Pattern cs\n/P2 scn\n0 0 20 20 re\nf\n";
    const inner_content = "0 0 5 10 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Pattern << /P1 5 0 R /P2 6 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 20 20] /XStep 20 /YStep 20 /Resources << /Pattern << /P2 6 0 R >> >> /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ outer_content.len, outer_content },
        ),
        try std.fmt.allocPrint(
            alloc,
            "6 0 obj\n<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 10 10] /XStep 10 /YStep 10 /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ inner_content.len, inner_content },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);
    defer alloc.free(objects[5]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 7\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 7 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPagePatternRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(usize, 1), runs[0].tile_pattern_runs.len);
}

test "reader preserves vector text pattern paint inside tiling patterns" {
    const alloc = std.testing.allocator;
    const page_content = "/Pattern cs\n/P1 scn\n0 0 40 40 re\nf\n";
    const outer_content =
        "BT\n" ++
        "/F1 20 Tf\n" ++
        "/Pattern cs\n" ++
        "/P2 scn\n" ++
        "0 0 Td\n" ++
        "(A) Tj\n" ++
        "ET\n";
    const inner_content = "1 0 0 rg\n0 0 5 10 re\nf\n";
    const glyph_content = "1000 0 d0\n0 0 1000 1000 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Pattern << /P1 5 0 R /P2 6 0 R >> /Font << /F1 7 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 20 20] /XStep 20 /YStep 20 /Resources << /Pattern << /P2 6 0 R >> /Font << /F1 7 0 R >> >> /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ outer_content.len, outer_content },
        ),
        try std.fmt.allocPrint(
            alloc,
            "6 0 obj\n<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 10 10] /XStep 10 /YStep 10 /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ inner_content.len, inner_content },
        ),
        "7 0 obj\n<< /Type /Font /Subtype /Type3 /PaintType 2 /FontMatrix [0.001 0 0 0.001 0 0] /Encoding << /Differences [65 /A] >> /FirstChar 65 /LastChar 65 /Widths [1000] /CharProcs << /A 8 0 R >> >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "8 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ glyph_content.len, glyph_content }),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);
    defer alloc.free(objects[5]);
    defer alloc.free(objects[7]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 9\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 9 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPagePatternRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectEqual(@as(usize, 1), runs[0].tile_pattern_runs.len);
}

test "reader emits shading runs for shading patterns" {
    const alloc = std.testing.allocator;
    const page_content = "/Pattern cs\n/P1 scn\n0 0 40 20 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Pattern << /P1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        "5 0 obj\n<< /Type /Pattern /PatternType 2 /Matrix [1 0 0 1 5 0] /Shading 6 0 R >>\nendobj\n",
        "6 0 obj\n<< /ShadingType 2 /ColorSpace /DeviceRGB /Coords [0 0 10 0] /Function 7 0 R /Extend [true true] >>\nendobj\n",
        "7 0 obj\n<< /FunctionType 2 /Domain [0 1] /C0 [1 0 0] /C1 [0 0 1] /N 1 >>\nendobj\n",
    };
    defer alloc.free(objects[3]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 8\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 8 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPageShadingRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expectApproxEqAbs(@as(f64, 5), runs[0].x0, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 15), runs[0].x1, 0.0001);
}

test "reader assigns non isolated transparency form group flag" {
    const alloc = std.testing.allocator;
    const form_content = "1 0 0 rg\n10 10 20 20 re\nf\n";
    const page_content = "/Fm1 Do\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /XObject << /Fm1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] /Group << /S /Transparency /I false >> /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ form_content.len, form_content },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].group_id != null);
    try std.testing.expect(!runs[0].group_isolated);
}

test "reader assigns knockout transparency form group flag" {
    const alloc = std.testing.allocator;
    const form_content = "1 0 0 rg\n10 10 20 20 re\nf\n";
    const page_content = "/Fm1 Do\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /XObject << /Fm1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ page_content.len, page_content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] /Group << /S /Transparency /I true /K true >> /Length {d} >>\nstream\n{s}endstream\nendobj\n",
            .{ form_content.len, form_content },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");
    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    defer alloc.free(sample);

    var parsed = try Reader.init(alloc, sample);
    defer parsed.deinit();
    const runs = try parsed.extractPageShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }

    try std.testing.expectEqual(@as(usize, 1), runs.len);
    try std.testing.expect(runs[0].group_id != null);
    try std.testing.expect(runs[0].group_isolated);
    try std.testing.expect(runs[0].group_knockout);
}
