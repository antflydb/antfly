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

pub const text_encoding = @import("text_encoding.zig");
pub const reader = @import("reader.zig");
pub const syntax = @import("syntax.zig");
pub const render = @import("render.zig");

const Allocator = std.mem.Allocator;

pub const Backend = struct {
    ptr: *const anyopaque,
    extract_text_fn: *const fn (ptr: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) anyerror![]u8,
    render_first_page_png_fn: *const fn (ptr: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) anyerror![]u8,

    pub fn extractText(self: Backend, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
        return try self.extract_text_fn(self.ptr, alloc, pdf_bytes);
    }

    pub fn renderFirstPagePng(self: Backend, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
        return try self.render_first_page_png_fn(self.ptr, alloc, pdf_bytes);
    }

    pub fn system() Backend {
        // Keep the existing call sites stable while the backend implementation
        // pivots to pure Zig.
        return native();
    }

    pub fn native() Backend {
        return .{
            .ptr = &native_backend,
            .extract_text_fn = extractTextNative,
            .render_first_page_png_fn = renderFirstPagePngNative,
        };
    }
};

const native_backend: u8 = 0;

fn extractTextNative(_: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
    var parsed = try reader.Reader.init(alloc, pdf_bytes);
    defer parsed.deinit();
    return try parsed.extractPlainTextAlloc();
}

fn renderFirstPagePngNative(_: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
    var parsed = try reader.Reader.init(alloc, pdf_bytes);
    defer parsed.deinit();
    var render_runs = try parsed.extractPageRenderRunsAlloc(1);
    defer render_runs.deinit(alloc);
    const page_box = render_runs.page_box;
    const runs = render_runs.text_runs;
    const image_runs = render_runs.image_runs;
    const shading_runs = render_runs.shading_runs;
    const pattern_runs = render_runs.pattern_runs;
    const shape_runs = render_runs.shape_runs;
    var text_pattern_runs: []reader.PatternRun = &.{};
    var text_shape_runs: []reader.ShapeRun = &.{};
    defer {
        for (text_pattern_runs) |*run| run.deinit(alloc);
        if (text_pattern_runs.len > 0) alloc.free(text_pattern_runs);
        for (text_shape_runs) |*run| run.deinit(alloc);
        if (text_shape_runs.len > 0) alloc.free(text_shape_runs);
    }
    var plain_runs = std.ArrayList(reader.TextRun).empty;
    defer plain_runs.deinit(alloc);
    var needs_vector_text_patterns = false;
    var needs_vector_text_shapes = false;
    for (runs) |run| {
        const has_pattern = run.fill_pattern_name != null or run.stroke_pattern_name != null;
        if (has_pattern) {
            needs_vector_text_patterns = true;
        }
        if (run.vectorizable) {
            needs_vector_text_shapes = true;
        }
        if (has_pattern or run.vectorizable) continue;
        try plain_runs.append(alloc, run);
    }
    if (needs_vector_text_patterns) text_pattern_runs = try parsed.extractPageVectorTextPatternRunsAlloc(1);
    if (needs_vector_text_shapes) text_shape_runs = try parsed.extractPageVectorTextShapeRunsAlloc(1);
    var all_shape_runs = std.ArrayList(reader.ShapeRun).empty;
    defer {
        for (all_shape_runs.items) |*run| run.deinit(alloc);
        all_shape_runs.deinit(alloc);
    }
    for (shape_runs) |run| {
        try all_shape_runs.append(alloc, .{
            .paint_order = run.paint_order,
            .blend_mode = run.blend_mode,
            .group_id = run.group_id,
            .group_parent_id = run.group_parent_id,
            .group_isolated = run.group_isolated,
            .group_knockout = run.group_knockout,
            .kind = run.kind,
            .fill_rule = run.fill_rule,
            .line_cap = run.line_cap,
            .line_join = run.line_join,
            .miter_limit = run.miter_limit,
            .dash_array = if (run.dash_array) |dash| try alloc.dupe(f64, dash) else null,
            .dash_phase = run.dash_phase,
            .color = run.color,
            .stroke_width = run.stroke_width,
            .closed = run.closed,
            .clip_box = run.clip_box,
            .clip_points = if (run.clip_points) |pts| try alloc.dupe([2]f64, pts) else null,
            .clip_fill_rule = run.clip_fill_rule,
            .points = try alloc.dupe([2]f64, run.points),
        });
    }
    for (text_shape_runs) |run| {
        try all_shape_runs.append(alloc, .{
            .paint_order = run.paint_order,
            .blend_mode = run.blend_mode,
            .group_id = run.group_id,
            .group_parent_id = run.group_parent_id,
            .group_isolated = run.group_isolated,
            .group_knockout = run.group_knockout,
            .kind = run.kind,
            .fill_rule = run.fill_rule,
            .line_cap = run.line_cap,
            .line_join = run.line_join,
            .miter_limit = run.miter_limit,
            .dash_array = if (run.dash_array) |dash| try alloc.dupe(f64, dash) else null,
            .dash_phase = run.dash_phase,
            .color = run.color,
            .stroke_width = run.stroke_width,
            .closed = run.closed,
            .clip_box = run.clip_box,
            .clip_points = if (run.clip_points) |pts| try alloc.dupe([2]f64, pts) else null,
            .clip_fill_rule = run.clip_fill_rule,
            .points = try alloc.dupe([2]f64, run.points),
        });
    }
    var all_pattern_runs = std.ArrayList(reader.PatternRun).empty;
    defer {
        for (all_pattern_runs.items) |*run| run.deinit(alloc);
        all_pattern_runs.deinit(alloc);
    }
    for (pattern_runs) |run| try all_pattern_runs.append(alloc, try dupPatternRunAlloc(alloc, run));
    for (text_pattern_runs) |run| try all_pattern_runs.append(alloc, try dupPatternRunAlloc(alloc, run));
    std.mem.sort(reader.TextRun, plain_runs.items, {}, struct {
        fn lessThan(_: void, a: reader.TextRun, b: reader.TextRun) bool {
            return a.paint_order < b.paint_order;
        }
    }.lessThan);
    std.mem.sort(reader.ShapeRun, all_shape_runs.items, {}, struct {
        fn lessThan(_: void, a: reader.ShapeRun, b: reader.ShapeRun) bool {
            return a.paint_order < b.paint_order;
        }
    }.lessThan);
    std.mem.sort(reader.PatternRun, all_pattern_runs.items, {}, struct {
        fn lessThan(_: void, a: reader.PatternRun, b: reader.PatternRun) bool {
            return a.paint_order < b.paint_order;
        }
    }.lessThan);
    if (plain_runs.items.len > 0 or image_runs.len > 0 or shading_runs.len > 0 or all_pattern_runs.items.len > 0 or all_shape_runs.items.len > 0) return try render.renderPageContentPngInBox(alloc, page_box, plain_runs.items, image_runs, shading_runs, all_pattern_runs.items, all_shape_runs.items);
    const text = try parsed.extractPageTextAlloc(1);
    defer alloc.free(text);
    return try render.renderTextPreviewPng(alloc, text);
}

fn dupPatternRunAlloc(alloc: Allocator, run: reader.PatternRun) !reader.PatternRun {
    var out = reader.PatternRun{
        .kind = run.kind,
        .mode = run.mode,
        .paint_order = run.paint_order,
        .blend_mode = run.blend_mode,
        .group_id = run.group_id,
        .group_parent_id = run.group_parent_id,
        .group_isolated = run.group_isolated,
        .group_knockout = run.group_knockout,
        .fill_rule = run.fill_rule,
        .line_cap = run.line_cap,
        .line_join = run.line_join,
        .miter_limit = run.miter_limit,
        .dash_array = if (run.dash_array) |dash| try alloc.dupe(f64, dash) else null,
        .dash_phase = run.dash_phase,
        .stroke_width = run.stroke_width,
        .closed = run.closed,
        .clip_box = run.clip_box,
        .clip_points = if (run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
        .clip_fill_rule = run.clip_fill_rule,
        .points = try alloc.dupe([2]f64, run.points),
        .pattern_matrix = run.pattern_matrix,
        .pattern_bbox = run.pattern_bbox,
        .pattern_x_step = run.pattern_x_step,
        .pattern_y_step = run.pattern_y_step,
        .base_color = run.base_color,
        .shading = null,
        .tile_text_runs = &.{},
        .tile_image_runs = &.{},
        .tile_shading_runs = &.{},
        .tile_pattern_runs = &.{},
        .tile_shape_runs = &.{},
    };
    if (run.shading) |shading| {
        out.shading = .{
            .kind = shading.kind,
            .paint_order = shading.paint_order,
            .blend_mode = shading.blend_mode,
            .group_id = shading.group_id,
            .group_parent_id = shading.group_parent_id,
            .group_isolated = shading.group_isolated,
            .group_knockout = shading.group_knockout,
            .clip_box = shading.clip_box,
            .clip_points = if (shading.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
            .clip_fill_rule = shading.clip_fill_rule,
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
        };
    }
    if (run.tile_text_runs.len > 0) {
        var list = std.ArrayList(reader.TextRun).empty;
        defer list.deinit(alloc);
        for (run.tile_text_runs) |text| {
            try list.append(alloc, .{
                .text = try alloc.dupe(u8, text.text),
                .raw_text = if (text.raw_text) |raw| try alloc.dupe(u8, raw) else null,
                .font_index = text.font_index,
                .vectorizable = text.vectorizable,
                .x = text.x,
                .y = text.y,
                .font_size = text.font_size,
                .a = text.a,
                .b = text.b,
                .c = text.c,
                .d = text.d,
                .alpha = text.alpha,
                .stroke_alpha = text.stroke_alpha,
                .render_mode = text.render_mode,
                .fill_color = text.fill_color,
                .stroke_color = text.stroke_color,
                .stroke_width = text.stroke_width,
                .horizontal_scale = text.horizontal_scale,
                .char_spacing = text.char_spacing,
                .word_spacing = text.word_spacing,
                .advance_width = text.advance_width,
                .ascent = text.ascent,
                .descent = text.descent,
                .paint_order = text.paint_order,
                .blend_mode = text.blend_mode,
                .group_id = text.group_id,
                .group_parent_id = text.group_parent_id,
                .group_isolated = text.group_isolated,
                .group_knockout = text.group_knockout,
                .fill_pattern_name = text.fill_pattern_name,
                .stroke_pattern_name = text.stroke_pattern_name,
                .clip_box = text.clip_box,
                .clip_points = if (text.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                .clip_fill_rule = text.clip_fill_rule,
            });
        }
        out.tile_text_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_image_runs.len > 0) {
        var list = std.ArrayList(reader.ImageRun).empty;
        defer list.deinit(alloc);
        for (run.tile_image_runs) |image_run| {
            try list.append(alloc, .{
                .rgba = try alloc.dupe(u8, image_run.rgba),
                .width = image_run.width,
                .height = image_run.height,
                .alpha = image_run.alpha,
                .paint_order = image_run.paint_order,
                .blend_mode = image_run.blend_mode,
                .group_id = image_run.group_id,
                .group_parent_id = image_run.group_parent_id,
                .group_isolated = image_run.group_isolated,
                .group_knockout = image_run.group_knockout,
                .clip_box = image_run.clip_box,
                .clip_points = if (image_run.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                .clip_fill_rule = image_run.clip_fill_rule,
                .a = image_run.a,
                .b = image_run.b,
                .c = image_run.c,
                .d = image_run.d,
                .e = image_run.e,
                .f = image_run.f,
                .x = image_run.x,
                .y = image_run.y,
                .draw_width = image_run.draw_width,
                .draw_height = image_run.draw_height,
            });
        }
        out.tile_image_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_shading_runs.len > 0) {
        var list = std.ArrayList(reader.ShadingRun).empty;
        defer list.deinit(alloc);
        for (run.tile_shading_runs) |shading| {
            try list.append(alloc, .{
                .kind = shading.kind,
                .paint_order = shading.paint_order,
                .blend_mode = shading.blend_mode,
                .group_id = shading.group_id,
                .group_parent_id = shading.group_parent_id,
                .group_isolated = shading.group_isolated,
                .group_knockout = shading.group_knockout,
                .clip_box = shading.clip_box,
                .clip_points = if (shading.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                .clip_fill_rule = shading.clip_fill_rule,
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
            });
        }
        out.tile_shading_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_pattern_runs.len > 0) {
        var list = std.ArrayList(reader.PatternRun).empty;
        defer list.deinit(alloc);
        for (run.tile_pattern_runs) |pattern_run| try list.append(alloc, try dupPatternRunAlloc(alloc, pattern_run));
        out.tile_pattern_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_shape_runs.len > 0) {
        var list = std.ArrayList(reader.ShapeRun).empty;
        defer list.deinit(alloc);
        for (run.tile_shape_runs) |shape| {
            try list.append(alloc, .{
                .kind = shape.kind,
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
                .color = shape.color,
                .stroke_width = shape.stroke_width,
                .closed = shape.closed,
                .clip_box = shape.clip_box,
                .clip_points = if (shape.clip_points) |clip| try alloc.dupe([2]f64, clip) else null,
                .clip_fill_rule = shape.clip_fill_rule,
                .points = try alloc.dupe([2]f64, shape.points),
            });
        }
        out.tile_shape_runs = try list.toOwnedSlice(alloc);
    }
    return out;
}

fn encryptType1EexecAlloc(alloc: Allocator, plain: []const u8) ![]u8 {
    const prefix = [_]u8{ 0, 0, 0, 0 };
    var out = try alloc.alloc(u8, prefix.len + plain.len);
    var r: u16 = 55665;
    for (prefix, 0..) |value, i| {
        const cipher = value ^ @as(u8, @truncate(r >> 8));
        out[i] = cipher;
        r = @truncate((@as(u32, cipher) + r) * 52845 + 22719);
    }
    for (plain, 0..) |value, i| {
        const cipher = value ^ @as(u8, @truncate(r >> 8));
        out[prefix.len + i] = cipher;
        r = @truncate((@as(u32, cipher) + r) * 52845 + 22719);
    }
    return out;
}

test "mock pdf backend interface compiles" {
    const Mock = struct {
        fn extract(_: *const anyopaque, alloc: Allocator, _: []const u8) ![]u8 {
            return try alloc.dupe(u8, "pdf text");
        }

        fn render(_: *const anyopaque, alloc: Allocator, _: []const u8) ![]u8 {
            return try alloc.dupe(u8, "png");
        }
    };

    const backend = Backend{
        .ptr = undefined,
        .extract_text_fn = Mock.extract,
        .render_first_page_png_fn = Mock.render,
    };

    const alloc = std.testing.allocator;
    const text = try backend.extractText(alloc, "pdf");
    defer alloc.free(text);
    const png = try backend.renderFirstPagePng(alloc, "pdf");
    defer alloc.free(png);

    try std.testing.expectEqualStrings("pdf text", text);
    try std.testing.expectEqualStrings("png", png);
}

test "native backend extracts simple pdf text" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello World) Tj\nET\n";

    const obj1 =
        "1 0 obj\n" ++
        "<< /Type /Catalog /Pages 2 0 R >>\n" ++
        "endobj\n";
    const obj2 =
        "2 0 obj\n" ++
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>\n" ++
        "endobj\n";
    const obj3 =
        "3 0 obj\n" ++
        "<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\n" ++
        "endobj\n";
    const obj4_prefix =
        "4 0 obj\n" ++
        "<< /Length ";
    const obj4_suffix =
        " >>\n" ++
        "stream\n";
    const obj4_end =
        "endstream\n" ++
        "endobj\n";

    const len_str = try std.fmt.allocPrint(alloc, "{d}", .{content.len});
    defer alloc.free(len_str);
    const obj4 = try std.mem.concat(alloc, u8, &.{ obj4_prefix, len_str, obj4_suffix, content, obj4_end });
    defer alloc.free(obj4);

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

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n" ++
        "<< /Root 1 0 R /Size 5 >>\n" ++
        "startxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const text = try backend.extractText(alloc, out.items);
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello World\n", text);
}

test "native backend extracts text from embedded fixture pdf" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    const backend = Backend.native();
    const text = try backend.extractText(alloc, fixture);
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello Fixture\n", text);
}

test "native backend renders simple pdf first page png" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello World) Tj\nET\n";

    const obj1 =
        "1 0 obj\n" ++
        "<< /Type /Catalog /Pages 2 0 R >>\n" ++
        "endobj\n";
    const obj2 =
        "2 0 obj\n" ++
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>\n" ++
        "endobj\n";
    const obj3 =
        "3 0 obj\n" ++
        "<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\n" ++
        "endobj\n";
    const obj4 = try std.fmt.allocPrint(
        alloc,
        "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n",
        .{ content.len, content },
    );
    defer alloc.free(obj4);

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

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded fixture pdf first page png" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, fixture);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders type1 cleartext fixture pdf first page png" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/type1_cleartext_fixture.pdf");
    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, fixture);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders type1 eexec fixture pdf first page png" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/type1_eexec_fixture.pdf");
    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, fixture);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders simple image xobject pdf first page png" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n10 0 0 10 20 30 cm\n/Im1 Do\nQ\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
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

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders Type3 text glyphs through shape path" {
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

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders rotated image xobject pdf first page png" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n0 10 -10 0 20 30 cm\n/Im1 Do\nQ\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
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

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders simple filled rectangle pdf first page png" {
    const alloc = std.testing.allocator;
    const content = "1 0 0 rg\n10 20 30 40 re\nf\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);

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
    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

fn appendU16Be(alloc: Allocator, out: *std.ArrayList(u8), value: u16) !void {
    try out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToBig(u16, value)));
}

fn appendI16Be(alloc: Allocator, out: *std.ArrayList(u8), value: i16) !void {
    try appendU16Be(alloc, out, @bitCast(value));
}

fn appendU32Be(alloc: Allocator, out: *std.ArrayList(u8), value: u32) !void {
    try out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToBig(u32, value)));
}

fn pad4(alloc: Allocator, out: *std.ArrayList(u8)) !void {
    while ((out.items.len % 4) != 0) try out.append(alloc, 0);
}

fn buildSimpleTrueTypeFontAlloc(alloc: Allocator) ![]u8 {
    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 3);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 3);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1600);
    try appendI16Be(alloc, &hmtx, 0);

    var glyph = std.ArrayList(u8).empty;
    defer glyph.deinit(alloc);
    try appendI16Be(alloc, &glyph, 1);
    try appendI16Be(alloc, &glyph, 0);
    try appendI16Be(alloc, &glyph, 0);
    try appendI16Be(alloc, &glyph, 1000);
    try appendI16Be(alloc, &glyph, 1000);
    try appendU16Be(alloc, &glyph, 2);
    try appendU16Be(alloc, &glyph, 0);
    try glyph.appendSlice(alloc, &.{ 0x31, 0x21, 0x01 });
    try appendI16Be(alloc, &glyph, 1000);
    try appendI16Be(alloc, &glyph, -500);
    try appendI16Be(alloc, &glyph, 1000);
    if ((glyph.items.len % 2) != 0) try glyph.append(alloc, 0);

    var composite = std.ArrayList(u8).empty;
    defer composite.deinit(alloc);
    try appendI16Be(alloc, &composite, -1);
    try appendI16Be(alloc, &composite, 0);
    try appendI16Be(alloc, &composite, 0);
    try appendI16Be(alloc, &composite, 1600);
    try appendI16Be(alloc, &composite, 1000);
    try appendU16Be(alloc, &composite, 0x0023);
    try appendU16Be(alloc, &composite, 1);
    try appendI16Be(alloc, &composite, 0);
    try appendI16Be(alloc, &composite, 0);
    try appendU16Be(alloc, &composite, 0x0003);
    try appendU16Be(alloc, &composite, 1);
    try appendI16Be(alloc, &composite, 600);
    try appendI16Be(alloc, &composite, 0);
    if ((composite.items.len % 2) != 0) try composite.append(alloc, 0);

    var loca = std.ArrayList(u8).empty;
    defer loca.deinit(alloc);
    try appendU16Be(alloc, &loca, 0);
    try appendU16Be(alloc, &loca, 0);
    try appendU16Be(alloc, &loca, @intCast(glyph.items.len / 2));
    try appendU16Be(alloc, &loca, @intCast((glyph.items.len + composite.items.len) / 2));

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 66);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'g', 'l', 'y', 'f' }, .bytes = &.{} },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'l', 'o', 'c', 'a' }, .bytes = loca.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try appendU32Be(alloc, &out, 0x00010000);
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        if (std.mem.eql(u8, &table.tag, "glyf")) {
            try out.appendSlice(alloc, glyph.items);
            try out.appendSlice(alloc, composite.items);
        } else {
            try out.appendSlice(alloc, table.bytes);
        }
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = if (std.mem.eql(u8, &table.tag, "glyf"))
            @intCast(glyph.items.len + composite.items.len)
        else
            @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

fn buildSimpleOpenTypeCffFontAlloc(alloc: Allocator) ![]u8 {
    const cff_bytes = &[_]u8{
        1,   0,   4,   1,
        0,   1,   1,   1,
        5,   'T', 'e', 's',
        't', 0,   1,   1,
        1,   5,   190, 15,
        165, 17,  0,   0,
        0,   0,   0,   2,
        1,   1,   2,   20,
        14,  139, 139, 21,
        247, 124, 139, 5,
        251, 124, 250, 124,
        5,   251, 124, 251,
        124, 5,   14,  0,
        0,   1,
    };

    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 2);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 2);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'C', 'F', 'F', ' ' }, .bytes = cff_bytes },
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "OTTO");
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        try out.appendSlice(alloc, table.bytes);
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

fn appendCffInt(alloc: Allocator, out: *std.ArrayList(u8), value: i32) !void {
    if (value >= -107 and value <= 107) {
        try out.append(alloc, @intCast(value + 139));
        return;
    }
    if (value >= -32768 and value <= 32767) {
        try out.append(alloc, 28);
        try appendU16Be(alloc, out, @bitCast(@as(i16, @intCast(value))));
        return;
    }
    return error.OutOfMemory;
}

fn appendCffIndex(alloc: Allocator, out: *std.ArrayList(u8), objects: []const []const u8) !void {
    try appendU16Be(alloc, out, @intCast(objects.len));
    if (objects.len == 0) return;
    try out.append(alloc, 1);
    var offset: usize = 1;
    try out.append(alloc, @intCast(offset));
    for (objects) |obj| {
        offset += obj.len;
        try out.append(alloc, @intCast(offset));
    }
    for (objects) |obj| try out.appendSlice(alloc, obj);
}

fn buildFdSelectOpenTypeCffFontAlloc(alloc: Allocator) ![]u8 {
    var name_index = std.ArrayList(u8).empty;
    defer name_index.deinit(alloc);
    try appendCffIndex(alloc, &name_index, &.{"Test"});

    const top_dict_len: usize = 18;
    const top_dict_index_len: usize = 2 + 1 + 2 + top_dict_len;
    const prefix_len = 4 + name_index.items.len + top_dict_index_len + 2 + 2;

    const charset_offset: i32 = @intCast(prefix_len);
    const fdselect_offset: i32 = charset_offset + 5;
    const fdarray_offset: i32 = fdselect_offset + 4;
    const charstrings_offset: i32 = fdarray_offset + 20;
    const private0_offset: i32 = charstrings_offset + 20;
    const local0_offset: i32 = private0_offset + 2;
    const private1_offset: i32 = local0_offset + 9;

    var top_dict = std.ArrayList(u8).empty;
    defer top_dict.deinit(alloc);
    try appendCffInt(alloc, &top_dict, charset_offset);
    try top_dict.append(alloc, 15);
    try appendCffInt(alloc, &top_dict, fdselect_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 37 });
    try appendCffInt(alloc, &top_dict, fdarray_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 36 });
    try appendCffInt(alloc, &top_dict, charstrings_offset);
    try top_dict.append(alloc, 17);

    var top_dict_index = std.ArrayList(u8).empty;
    defer top_dict_index.deinit(alloc);
    try appendCffIndex(alloc, &top_dict_index, &.{top_dict.items});

    var fd0_dict = std.ArrayList(u8).empty;
    defer fd0_dict.deinit(alloc);
    try appendCffInt(alloc, &fd0_dict, 2);
    try appendCffInt(alloc, &fd0_dict, private0_offset);
    try fd0_dict.append(alloc, 18);

    var fd1_dict = std.ArrayList(u8).empty;
    defer fd1_dict.deinit(alloc);
    try appendCffInt(alloc, &fd1_dict, 2);
    try appendCffInt(alloc, &fd1_dict, private1_offset);
    try fd1_dict.append(alloc, 18);

    var fdarray_index = std.ArrayList(u8).empty;
    defer fdarray_index.deinit(alloc);
    try appendCffIndex(alloc, &fdarray_index, &.{ fd0_dict.items, fd1_dict.items });

    const glyph0 = [_]u8{14};
    const glyph1 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    const glyph2 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    var charstrings_index = std.ArrayList(u8).empty;
    defer charstrings_index.deinit(alloc);
    try appendCffIndex(alloc, &charstrings_index, &.{ &glyph0, &glyph1, &glyph2 });

    const charset = [_]u8{
        0,
        0,
        1,
        0,
        2,
    };
    const fdselect = [_]u8{
        0,
        0,
        0,
        1,
    };
    const private_dict = [_]u8{ 141, 19 };
    var local0_index = std.ArrayList(u8).empty;
    defer local0_index.deinit(alloc);
    const local0_subr = [_]u8{ 189, 139, 5, 11 };
    try appendCffIndex(alloc, &local0_index, &.{&local0_subr});
    var local1_index = std.ArrayList(u8).empty;
    defer local1_index.deinit(alloc);
    const local1_subr = [_]u8{ 139, 189, 5, 11 };
    try appendCffIndex(alloc, &local1_index, &.{&local1_subr});

    var cff = std.ArrayList(u8).empty;
    defer cff.deinit(alloc);
    try cff.appendSlice(alloc, &.{ 1, 0, 4, 1 });
    try cff.appendSlice(alloc, name_index.items);
    try cff.appendSlice(alloc, top_dict_index.items);
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &charset);
    try cff.appendSlice(alloc, &fdselect);
    try cff.appendSlice(alloc, fdarray_index.items);
    try cff.appendSlice(alloc, charstrings_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local0_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local1_index.items);

    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 3);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 3);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 66);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'C', 'F', 'F', ' ' }, .bytes = cff.items },
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "OTTO");
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        try out.appendSlice(alloc, table.bytes);
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

fn buildFdSelectFormat3OpenTypeCffFontAlloc(alloc: Allocator) ![]u8 {
    var name_index = std.ArrayList(u8).empty;
    defer name_index.deinit(alloc);
    try appendCffIndex(alloc, &name_index, &.{"Test"});

    const top_dict_len: usize = 18;
    const top_dict_index_len: usize = 2 + 1 + 2 + top_dict_len;
    const prefix_len = 4 + name_index.items.len + top_dict_index_len + 2 + 2;

    const charset_offset: i32 = @intCast(prefix_len);
    const fdselect_offset: i32 = charset_offset + 5;
    const fdarray_offset: i32 = fdselect_offset + 12;
    const charstrings_offset: i32 = fdarray_offset + 20;
    const private0_offset: i32 = charstrings_offset + 20;
    const local0_offset: i32 = private0_offset + 2;
    const private1_offset: i32 = local0_offset + 9;

    var top_dict = std.ArrayList(u8).empty;
    defer top_dict.deinit(alloc);
    try appendCffInt(alloc, &top_dict, charset_offset);
    try top_dict.append(alloc, 15);
    try appendCffInt(alloc, &top_dict, fdselect_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 37 });
    try appendCffInt(alloc, &top_dict, fdarray_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 36 });
    try appendCffInt(alloc, &top_dict, charstrings_offset);
    try top_dict.append(alloc, 17);

    var top_dict_index = std.ArrayList(u8).empty;
    defer top_dict_index.deinit(alloc);
    try appendCffIndex(alloc, &top_dict_index, &.{top_dict.items});

    var fd0_dict = std.ArrayList(u8).empty;
    defer fd0_dict.deinit(alloc);
    try appendCffInt(alloc, &fd0_dict, 2);
    try appendCffInt(alloc, &fd0_dict, private0_offset);
    try fd0_dict.append(alloc, 18);

    var fd1_dict = std.ArrayList(u8).empty;
    defer fd1_dict.deinit(alloc);
    try appendCffInt(alloc, &fd1_dict, 2);
    try appendCffInt(alloc, &fd1_dict, private1_offset);
    try fd1_dict.append(alloc, 18);

    var fdarray_index = std.ArrayList(u8).empty;
    defer fdarray_index.deinit(alloc);
    try appendCffIndex(alloc, &fdarray_index, &.{ fd0_dict.items, fd1_dict.items });

    const glyph0 = [_]u8{14};
    const glyph1 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    const glyph2 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    var charstrings_index = std.ArrayList(u8).empty;
    defer charstrings_index.deinit(alloc);
    try appendCffIndex(alloc, &charstrings_index, &.{ &glyph0, &glyph1, &glyph2 });

    const charset = [_]u8{
        0,
        0,
        1,
        0,
        2,
    };
    const fdselect = [_]u8{
        3,
        0,
        3,
        0,
        0,
        0,
        0,
        1,
        1,
        0,
        2,
        0,
        0,
        3,
    };
    const private_dict = [_]u8{ 141, 19 };
    var local0_index = std.ArrayList(u8).empty;
    defer local0_index.deinit(alloc);
    const local0_subr = [_]u8{ 189, 139, 5, 11 };
    try appendCffIndex(alloc, &local0_index, &.{&local0_subr});
    var local1_index = std.ArrayList(u8).empty;
    defer local1_index.deinit(alloc);
    const local1_subr = [_]u8{ 139, 189, 5, 11 };
    try appendCffIndex(alloc, &local1_index, &.{&local1_subr});

    var cff = std.ArrayList(u8).empty;
    defer cff.deinit(alloc);
    try cff.appendSlice(alloc, &.{ 1, 0, 4, 1 });
    try cff.appendSlice(alloc, name_index.items);
    try cff.appendSlice(alloc, top_dict_index.items);
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &charset);
    try cff.appendSlice(alloc, &fdselect);
    try cff.appendSlice(alloc, fdarray_index.items);
    try cff.appendSlice(alloc, charstrings_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local0_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local1_index.items);

    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 3);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 3);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 66);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'C', 'F', 'F', ' ' }, .bytes = cff.items },
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "OTTO");
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        try out.appendSlice(alloc, table.bytes);
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

test "native backend renders embedded FontFile2 true type glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /TrueType /BaseFont /TestTT /FirstChar 65 /LastChar 66 /Widths [1000 1600] /Encoding /WinAnsiEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestTT /FontFile2 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile2 composite glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(B) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /TrueType /BaseFont /TestTT /FirstChar 65 /LastChar 66 /Widths [1000 1600] /Encoding /WinAnsiEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestTT /FontFile2 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_program =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n";

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding << /Differences [65 /A] >> /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 glyph pdf with standard encoding" {
    const alloc = std.testing.allocator;
    const font_program =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n";

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 RD charstrings pdf first page png" {
    const alloc = std.testing.allocator;
    const subr = [_]u8{ 189, 139, 5, 11 };
    const glyph = [_]u8{ 139, 139, 21, 139, 10, 14 };

    var font_program = std.ArrayList(u8).empty;
    defer font_program.deinit(alloc);
    try font_program.appendSlice(
        alloc,
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
            "/FontName /TestT1 def\n" ++
            "/lenIV -1 def\n" ++
            "/Private 1 dict dup begin\n" ++
            "/Subrs 1 array\n" ++
            "dup 0 4 RD ",
    );
    try font_program.appendSlice(alloc, &subr);
    try font_program.appendSlice(alloc, " ND\nend readonly def\n/CharStrings 2 dict dup begin\ndup /.notdef 4 RD ");
    try font_program.appendSlice(alloc, &[_]u8{ 139, 139, 21, 14 });
    try font_program.appendSlice(alloc, " ND\ndup /A 6 RD ");
    try font_program.appendSlice(alloc, &glyph);
    try font_program.appendSlice(alloc, " ND\nend readonly def\n");

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.items.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program.items);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 eexec glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const eexec_plain =
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n" ++
        "cleartomark\n";
    const encrypted = try encryptType1EexecAlloc(alloc, eexec_plain);
    defer alloc.free(encrypted);

    var hex_payload = std.ArrayList(u8).empty;
    defer hex_payload.deinit(alloc);
    for (encrypted) |b| {
        const piece = try std.fmt.allocPrint(alloc, "{X:0>2}", .{b});
        defer alloc.free(piece);
        try hex_payload.appendSlice(alloc, piece);
    }

    var font_program = std.ArrayList(u8).empty;
    defer font_program.deinit(alloc);
    try font_program.appendSlice(
        alloc,
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
            "/FontName /TestT1 def\n" ++
            "/Encoding /StandardEncoding def\n" ++
            "currentfile eexec\n",
    );
    try font_program.appendSlice(alloc, hex_payload.items);
    try font_program.appendSlice(alloc, "\n");

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.items.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program.items);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 pfb eexec glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const eexec_plain =
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n" ++
        "cleartomark\n";
    const encrypted = try encryptType1EexecAlloc(alloc, eexec_plain);
    defer alloc.free(encrypted);

    var font_program = std.ArrayList(u8).empty;
    defer font_program.deinit(alloc);
    const ascii_segment =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/Encoding /StandardEncoding def\n" ++
        "currentfile eexec\n";
    try font_program.appendSlice(alloc, &.{ 0x80, 0x01 });
    try font_program.appendSlice(alloc, &.{
        @intCast(ascii_segment.len & 0xff),
        @intCast((ascii_segment.len >> 8) & 0xff),
        @intCast((ascii_segment.len >> 16) & 0xff),
        @intCast((ascii_segment.len >> 24) & 0xff),
    });
    try font_program.appendSlice(alloc, ascii_segment);
    try font_program.appendSlice(alloc, &.{ 0x80, 0x02 });
    try font_program.appendSlice(alloc, &.{
        @intCast(encrypted.len & 0xff),
        @intCast((encrypted.len >> 8) & 0xff),
        @intCast((encrypted.len >> 16) & 0xff),
        @intCast((encrypted.len >> 24) & 0xff),
    });
    try font_program.appendSlice(alloc, encrypted);
    try font_program.appendSlice(alloc, &.{ 0x80, 0x03 });

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.items.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program.items);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "reader extracts vector text shapes for embedded FontFile type1 seac glyph" {
    const alloc = std.testing.allocator;
    const font_program =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/Encoding /StandardEncoding def\n" ++
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 4 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "/period <8B8B15938B058B93058D8B058B8D050E> def\n" ++
        "/Aperiod <8BF75CF7C0CCB90C060E> def\n" ++
        "end readonly def\n";

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding << /Differences [65 /Aperiod] >> /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

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
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    var parsed = try reader.Reader.init(alloc, out.items);
    defer parsed.deinit();
    const runs = try parsed.extractPageVectorTextShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expect(runs.len > 0);
}

test "native backend renders embedded Type0 CIDFontType2 glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<0041> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "1 beginbfchar\n" ++
        "<0041> <0041>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H /DescendantFonts [6 0 R] /ToUnicode 8 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /CIDFontType2 /BaseFont /TestCID /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /DW 1000 /W [65 [1000]] /FontDescriptor 7 0 R >>\nendobj\n";
    const obj7 = "7 0 obj\n<< /Type /FontDescriptor /FontName /TestCID /FontFile2 9 0 R >>\nendobj\n";
    const obj8 = try std.fmt.allocPrint(alloc, "8 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap });
    defer alloc.free(obj8);

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
    const obj7_offset = out.items.len;
    try out.appendSlice(alloc, obj7);
    const obj8_offset = out.items.len;
    try out.appendSlice(alloc, obj8);
    const obj9_offset = out.items.len;
    const obj9_head = try std.fmt.allocPrint(alloc, "9 0 obj\n<< /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj9_head);
    try out.appendSlice(alloc, obj9_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 10\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset, obj8_offset, obj9_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 10 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded Type0 CIDFontType0 OpenType CFF glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleOpenTypeCffFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<0041> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "1 beginbfchar\n" ++
        "<0041> <0041>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H /DescendantFonts [6 0 R] /ToUnicode 8 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /CIDFontType0 /BaseFont /TestCID /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /FontDescriptor 7 0 R >>\nendobj\n";
    const obj7 = "7 0 obj\n<< /Type /FontDescriptor /FontName /TestCID /FontFile3 9 0 R >>\nendobj\n";
    const obj8 = try std.fmt.allocPrint(alloc, "8 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap });
    defer alloc.free(obj8);

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
    const obj7_offset = out.items.len;
    try out.appendSlice(alloc, obj7);
    const obj8_offset = out.items.len;
    try out.appendSlice(alloc, obj8);
    const obj9_offset = out.items.len;
    const obj9_head = try std.fmt.allocPrint(alloc, "9 0 obj\n<< /Subtype /OpenType /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj9_head);
    try out.appendSlice(alloc, obj9_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 10\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset, obj8_offset, obj9_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 10 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded Type0 CIDFontType0 OpenType CFF fdselect glyphs pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildFdSelectOpenTypeCffFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<00410042> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "2 beginbfchar\n" ++
        "<0041> <0041>\n" ++
        "<0042> <0042>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H /DescendantFonts [6 0 R] /ToUnicode 8 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /CIDFontType0 /BaseFont /TestCID /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /FontDescriptor 7 0 R >>\nendobj\n";
    const obj7 = "7 0 obj\n<< /Type /FontDescriptor /FontName /TestCID /FontFile3 9 0 R >>\nendobj\n";
    const obj8 = try std.fmt.allocPrint(alloc, "8 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap });
    defer alloc.free(obj8);

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
    const obj7_offset = out.items.len;
    try out.appendSlice(alloc, obj7);
    const obj8_offset = out.items.len;
    try out.appendSlice(alloc, obj8);
    const obj9_offset = out.items.len;
    const obj9_head = try std.fmt.allocPrint(alloc, "9 0 obj\n<< /Length {d} /Subtype /OpenType >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj9_head);
    try out.appendSlice(alloc, obj9_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 10\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset, obj8_offset, obj9_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 10 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test {
    _ = text_encoding;
    _ = reader;
    _ = syntax;
    _ = render;
}
