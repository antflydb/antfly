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
const reader = @import("reader.zig");
const image = @import("antfly_image");

const Allocator = std.mem.Allocator;

pub fn renderTextPreviewPng(alloc: Allocator, text: []const u8) ![]u8 {
    var max_cols: usize = 0;
    var lines: usize = 1;
    var current_cols: usize = 0;
    for (text) |ch| {
        if (ch == '\n') {
            max_cols = @max(max_cols, current_cols);
            current_cols = 0;
            lines += 1;
        } else {
            current_cols += 1;
        }
    }
    max_cols = @max(max_cols, current_cols);

    const margin: usize = 8;
    const cell_w: usize = 8;
    const cell_h: usize = 12;
    const width: usize = @max(32, margin * 2 + max_cols * cell_w);
    const height: usize = @max(32, margin * 2 + lines * cell_h);

    const rgba = try alloc.alloc(u8, width * height * 4);
    defer alloc.free(rgba);
    @memset(rgba, 0xff);

    var x = margin;
    var y = margin;
    for (text) |ch| {
        switch (ch) {
            '\n' => {
                x = margin;
                y += cell_h;
            },
            ' ' => x += cell_w,
            else => {
                drawGlyphBox(rgba, width, height, x, y);
                x += cell_w;
            },
        }
    }

    return try image.png.encodeRgba(alloc, @intCast(width), @intCast(height), rgba);
}

pub fn renderTextRunsPng(alloc: Allocator, runs: []const reader.TextRun) ![]u8 {
    return try renderPageContentPng(alloc, runs, &.{});
}

pub fn renderPageContentPngInBox(
    alloc: Allocator,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
) ![]u8 {
    const raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs);
    defer alloc.free(raw.rgba);
    return try image.png.encodeRgba(alloc, @intCast(raw.width), @intCast(raw.height), raw.rgba);
}

const RawPageCanvas = struct {
    rgba: []u8,
    width: usize,
    height: usize,
};

fn finite(value: f64) bool {
    return value == value and value != std.math.inf(f64) and value != -std.math.inf(f64);
}

fn ceilPositiveToUsize(value: f64, minimum: usize) usize {
    if (!finite(value) or value <= 0.0) return minimum;
    const max_usize_f: f64 = @floatFromInt(std.math.maxInt(usize));
    if (value >= max_usize_f) return std.math.maxInt(usize);
    return @max(minimum, @as(usize, @intFromFloat(@ceil(value))));
}

fn floorToCanvas(value: f64, limit: usize) usize {
    if (!finite(value) or value <= 0.0) return 0;
    const limit_f: f64 = @floatFromInt(limit);
    if (value >= limit_f) return limit;
    return @as(usize, @intFromFloat(@floor(value)));
}

fn ceilToCanvas(value: f64, limit: usize) usize {
    if (!finite(value) or value <= 0.0) return 0;
    const limit_f: f64 = @floatFromInt(limit);
    if (value >= limit_f) return limit;
    return @as(usize, @intFromFloat(@ceil(value)));
}

fn pixelWorldX(min_x: f64, margin: usize, px: usize) f64 {
    return min_x + (@as(f64, @floatFromInt(px)) - @as(f64, @floatFromInt(margin)) + 0.5);
}

fn pixelWorldY(max_y: f64, margin: usize, py: usize) f64 {
    return max_y - (@as(f64, @floatFromInt(py)) - @as(f64, @floatFromInt(margin)) + 0.5);
}

const GroupMeta = struct {
    id: u32,
    parent_id: ?u32,
    isolated: bool,
    knockout: bool,
    min_paint_order: usize,
};

const RenderChoice = union(enum) {
    text: usize,
    image: usize,
    shading: usize,
    pattern: usize,
    shape: usize,
    group: usize,
};

fn choiceOrder(
    choice: RenderChoice,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    groups: []const GroupMeta,
) usize {
    return switch (choice) {
        .text => |idx| text_runs[idx].paint_order,
        .image => |idx| image_runs[idx].paint_order,
        .shading => |idx| shading_runs[idx].paint_order,
        .pattern => |idx| pattern_runs[idx].paint_order,
        .shape => |idx| shape_runs[idx].paint_order,
        .group => |idx| groups[idx].min_paint_order,
    };
}

fn addOrUpdateGroupMeta(
    alloc: Allocator,
    groups: *std.ArrayList(GroupMeta),
    id: u32,
    parent_id: ?u32,
    isolated: bool,
    knockout: bool,
    paint_order: usize,
) anyerror!void {
    for (groups.items) |*group| {
        if (group.id != id) continue;
        group.parent_id = parent_id;
        group.isolated = isolated;
        group.knockout = knockout;
        group.min_paint_order = @min(group.min_paint_order, paint_order);
        return;
    }
    try groups.append(alloc, .{
        .id = id,
        .parent_id = parent_id,
        .isolated = isolated,
        .knockout = knockout,
        .min_paint_order = paint_order,
    });
}

fn collectGroupMetasAlloc(
    alloc: Allocator,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
) ![]GroupMeta {
    var groups = std.ArrayList(GroupMeta).empty;
    errdefer groups.deinit(alloc);

    for (text_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.paint_order);
    for (image_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.paint_order);
    for (shading_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.paint_order);
    for (pattern_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.paint_order);
    for (shape_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.paint_order);

    std.mem.sort(GroupMeta, groups.items, {}, struct {
        fn lessThan(_: void, a: GroupMeta, b: GroupMeta) bool {
            return a.min_paint_order < b.min_paint_order;
        }
    }.lessThan);
    return try groups.toOwnedSlice(alloc);
}

fn nextRenderChoice(
    current_group: ?u32,
    after_order: ?usize,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    groups: []const GroupMeta,
) ?RenderChoice {
    const min_after = after_order orelse 0;
    var best_order: ?usize = null;
    var best: ?RenderChoice = null;

    for (text_runs, 0..) |run, idx| {
        if (run.group_id != current_group) continue;
        if (after_order != null and run.paint_order <= min_after) continue;
        if (best_order == null or run.paint_order < best_order.?) {
            best_order = run.paint_order;
            best = .{ .text = idx };
        }
    }
    for (image_runs, 0..) |run, idx| {
        if (run.group_id != current_group) continue;
        if (after_order != null and run.paint_order <= min_after) continue;
        if (best_order == null or run.paint_order < best_order.?) {
            best_order = run.paint_order;
            best = .{ .image = idx };
        }
    }
    for (shading_runs, 0..) |run, idx| {
        if (run.group_id != current_group) continue;
        if (after_order != null and run.paint_order <= min_after) continue;
        if (best_order == null or run.paint_order < best_order.?) {
            best_order = run.paint_order;
            best = .{ .shading = idx };
        }
    }
    for (pattern_runs, 0..) |run, idx| {
        if (run.group_id != current_group) continue;
        if (after_order != null and run.paint_order <= min_after) continue;
        if (best_order == null or run.paint_order < best_order.?) {
            best_order = run.paint_order;
            best = .{ .pattern = idx };
        }
    }
    for (shape_runs, 0..) |run, idx| {
        if (run.group_id != current_group) continue;
        if (after_order != null and run.paint_order <= min_after) continue;
        if (best_order == null or run.paint_order < best_order.?) {
            best_order = run.paint_order;
            best = .{ .shape = idx };
        }
    }
    for (groups, 0..) |group, idx| {
        if (group.parent_id != current_group) continue;
        if (after_order != null and group.min_paint_order <= min_after) continue;
        if (best_order == null or group.min_paint_order < best_order.?) {
            best_order = group.min_paint_order;
            best = .{ .group = idx };
        }
    }

    return best;
}

fn renderChildGroupAlloc(
    alloc: Allocator,
    target: []u8,
    width: usize,
    height: usize,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    groups: []const GroupMeta,
    group_index: usize,
) anyerror!void {
    const meta = groups[group_index];
    const child = try alloc.alloc(u8, width * height * 4);
    defer alloc.free(child);
    if (meta.isolated) {
        @memset(child, 0);
    } else {
        @memcpy(child, target);
    }
    try renderGroupChildrenAlloc(alloc, child, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, groups, meta.id, meta.knockout);
    if (meta.isolated) {
        compositeGroupCanvas(target, child);
    } else {
        @memcpy(target, child);
    }
}

fn renderChoiceAlloc(
    alloc: Allocator,
    canvas: []u8,
    width: usize,
    height: usize,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    groups: []const GroupMeta,
    choice: RenderChoice,
) !void {
    switch (choice) {
        .text => |idx| drawTextRun(canvas, width, height, 0, page_box.min_x, page_box.max_y, text_runs[idx]),
        .image => |idx| drawImageRun(canvas, width, height, 0, page_box.min_x, page_box.max_y, image_runs[idx]),
        .shading => |idx| drawShadingRun(canvas, width, height, page_box.min_x, page_box.max_y, shading_runs[idx]),
        .pattern => |idx| try drawPatternRun(alloc, canvas, width, height, page_box.min_x, page_box.max_y, pattern_runs[idx]),
        .shape => |idx| drawShapeRun(canvas, width, height, page_box.min_x, page_box.max_y, shape_runs[idx]),
        .group => |idx| try renderChildGroupAlloc(alloc, canvas, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, groups, idx),
    }
}

fn renderGroupChildrenAlloc(
    alloc: Allocator,
    canvas: []u8,
    width: usize,
    height: usize,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    groups: []const GroupMeta,
    current_group: ?u32,
    knockout: bool,
) anyerror!void {
    const backdrop = if (knockout) try alloc.dupe(u8, canvas) else null;
    defer if (backdrop) |buf| alloc.free(buf);

    var after_order: ?usize = null;
    while (nextRenderChoice(current_group, after_order, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, groups)) |choice| {
        if (knockout) {
            const scratch = try alloc.dupe(u8, backdrop orelse canvas);
            defer alloc.free(scratch);
            try renderChoiceAlloc(alloc, scratch, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, groups, choice);
            replaceCanvasWhereChanged(canvas, scratch, backdrop orelse canvas);
        } else {
            try renderChoiceAlloc(alloc, canvas, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, groups, choice);
        }
        after_order = choiceOrder(choice, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, groups);
    }
}

fn renderPageContentRgbaInBoxAlloc(
    alloc: Allocator,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
) !RawPageCanvas {
    const page_w = @max(1.0, page_box.max_x - page_box.min_x);
    const page_h = @max(1.0, page_box.max_y - page_box.min_y);
    const width = ceilPositiveToUsize(page_w, 1);
    const height = ceilPositiveToUsize(page_h, 1);

    const rgba = try alloc.alloc(u8, width * height * 4);
    @memset(rgba, 0xff);
    const groups = try collectGroupMetasAlloc(alloc, text_runs, image_runs, shading_runs, pattern_runs, shape_runs);
    defer alloc.free(groups);
    try renderGroupChildrenAlloc(alloc, rgba, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, groups, null, false);

    return .{ .rgba = rgba, .width = width, .height = height };
}

pub fn renderPageContentPng(alloc: Allocator, text_runs: []const reader.TextRun, image_runs: []const reader.ImageRun) ![]u8 {
    if (text_runs.len == 0 and image_runs.len == 0) return try renderTextPreviewPng(alloc, "");

    var initialized = false;
    var min_x: f64 = 0;
    var max_x: f64 = 0;
    var min_y: f64 = 0;
    var max_y: f64 = 0;

    for (text_runs) |run| {
        const bounds = textRunBounds(run);
        if (!initialized) {
            min_x = bounds.min_x;
            max_x = bounds.max_x;
            min_y = bounds.min_y;
            max_y = bounds.max_y;
            initialized = true;
        } else {
            min_x = @min(min_x, bounds.min_x);
            max_x = @max(max_x, bounds.max_x);
            min_y = @min(min_y, bounds.min_y);
            max_y = @max(max_y, bounds.max_y);
        }
    }

    for (image_runs) |run| {
        const bounds = imageRunBounds(run);
        if (!initialized) {
            min_x = bounds.min_x;
            max_x = bounds.max_x;
            min_y = bounds.min_y;
            max_y = bounds.max_y;
            initialized = true;
        } else {
            min_x = @min(min_x, bounds.min_x);
            max_x = @max(max_x, bounds.max_x);
            min_y = @min(min_y, bounds.min_y);
            max_y = @max(max_y, bounds.max_y);
        }
    }

    const margin: usize = 8;
    const width: usize = @max(32, margin * 2 + ceilPositiveToUsize(max_x - min_x + 1, 1));
    const height: usize = @max(32, margin * 2 + ceilPositiveToUsize(max_y - min_y + 1, 1));

    const rgba = try alloc.alloc(u8, width * height * 4);
    defer alloc.free(rgba);
    @memset(rgba, 0xff);

    for (image_runs) |run| {
        drawImageRun(rgba, width, height, margin, min_x, max_y, run);
    }

    for (text_runs) |run| {
        drawTextRun(rgba, width, height, margin, min_x, max_y, run);
    }

    return try image.png.encodeRgba(alloc, @intCast(width), @intCast(height), rgba);
}

pub fn renderTextRunsPngLegacy(alloc: Allocator, runs: []const reader.TextRun) ![]u8 {
    if (runs.len == 0) return try renderTextPreviewPng(alloc, "");

    var min_x = runs[0].x;
    var max_x = runs[0].x;
    var min_y = runs[0].y;
    var max_y = runs[0].y;
    for (runs) |run| {
        const bounds = textRunBounds(run);
        min_x = @min(min_x, bounds.min_x);
        max_x = @max(max_x, bounds.max_x);
        min_y = @min(min_y, bounds.min_y);
        max_y = @max(max_y, bounds.max_y);
    }

    const margin: usize = 8;
    const width: usize = @max(32, margin * 2 + ceilPositiveToUsize(max_x - min_x + 1, 1));
    const height: usize = @max(32, margin * 2 + ceilPositiveToUsize(max_y - min_y + 1, 1));

    const rgba = try alloc.alloc(u8, width * height * 4);
    defer alloc.free(rgba);
    @memset(rgba, 0xff);

    for (runs) |run| {
        drawTextRun(rgba, width, height, margin, min_x, max_y, run);
    }

    return try image.png.encodeRgba(alloc, @intCast(width), @intCast(height), rgba);
}

fn drawGlyphBox(rgba: []u8, width: usize, height: usize, x: usize, y: usize) void {
    const outer_w: usize = 6;
    const outer_h: usize = 9;
    const inner_w: usize = 4;
    const inner_h: usize = 7;
    var row: usize = 0;
    while (row < outer_h and y + row < height) : (row += 1) {
        var col: usize = 0;
        while (col < outer_w and x + col < width) : (col += 1) {
            const border = row == 0 or row + 1 == outer_h or col == 0 or col + 1 == outer_w;
            const fill = row >= 1 and row < 1 + inner_h and col >= 1 and col < 1 + inner_w and ((row + col) % 2 == 0);
            if (border or fill) {
                const idx = ((y + row) * width + (x + col)) * 4;
                rgba[idx + 0] = 0;
                rgba[idx + 1] = 0;
                rgba[idx + 2] = 0;
                rgba[idx + 3] = 0xff;
            }
        }
    }
}

fn drawTextRun(
    rgba: []u8,
    width: usize,
    height: usize,
    margin: usize,
    min_x: f64,
    max_y: f64,
    run: reader.TextRun,
) void {
    var cursor: f64 = 0;
    const advance_scale = estimatedRunAdvanceScale(run);
    var view = std.unicode.Utf8View.init(run.text) catch {
        for (run.text) |ch| {
            const advance = estimatedRunCodepointAdvance(run, if (ch == ' ') ' ' else 0xfffd, advance_scale);
            switch (ch) {
                ' ' => {},
                '\n', '\r' => {},
                else => drawAffineGlyphBox(rgba, width, height, margin, min_x, max_y, run, cursor, advance),
            }
            cursor += advance;
        }
        return;
    };
    var iter = view.iterator();
    while (iter.nextCodepoint()) |cp| {
        const advance = estimatedRunCodepointAdvance(run, cp, advance_scale);
        switch (cp) {
            ' ' => {},
            '\n', '\r' => {},
            else => drawAffineGlyphBox(rgba, width, height, margin, min_x, max_y, run, cursor, advance),
        }
        cursor += advance;
    }
}

fn drawImageRun(canvas: []u8, canvas_w: usize, canvas_h: usize, margin: usize, min_x: f64, max_y: f64, run: reader.ImageRun) void {
    const det = run.a * run.d - run.b * run.c;
    if (@abs(det) < 0.000001) return;

    const inv_a = run.d / det;
    const inv_b = -run.b / det;
    const inv_c = -run.c / det;
    const inv_d = run.a / det;
    const bounds = imageRunBounds(run);

    const margin_f: f64 = @floatFromInt(margin);
    const x0 = floorToCanvas(margin_f + bounds.min_x - min_x, canvas_w);
    const x1 = ceilToCanvas(margin_f + bounds.max_x - min_x, canvas_w);
    const y0 = floorToCanvas(margin_f + max_y - bounds.max_y, canvas_h);
    const y1 = ceilToCanvas(margin_f + max_y - bounds.min_y, canvas_h);
    const has_clip = run.clip_box != null or run.clip_points != null;

    var py = y0;
    while (py < y1) : (py += 1) {
        var px = x0;
        while (px < x1) : (px += 1) {
            const world_x = pixelWorldX(min_x, margin, px);
            const world_y = pixelWorldY(max_y, margin, py);
            if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
            const dx = world_x - run.e;
            const dy = world_y - run.f;
            const u = inv_a * dx + inv_c * dy;
            const v = inv_b * dx + inv_d * dy;
            if (!finite(u) or !finite(v) or u < 0 or u > 1 or v < 0 or v > 1) continue;

            const sx = @min(run.width - 1, @as(u32, @intFromFloat(@floor(u * @as(f64, @floatFromInt(run.width))))));
            const sy = @min(run.height - 1, @as(u32, @intFromFloat(@floor((1.0 - v) * @as(f64, @floatFromInt(run.height))))));
            const src = (@as(usize, sy) * @as(usize, run.width) + @as(usize, sx)) * 4;
            const dst = (py * canvas_w + px) * 4;
            const alpha = @as(u8, @intCast((@as(u16, run.rgba[src + 3]) * @as(u16, run.alpha) + 127) / 255));
            blendPixelMode(canvas, dst, .{ run.rgba[src + 0], run.rgba[src + 1], run.rgba[src + 2], alpha }, run.blend_mode);
        }
    }
}

fn imageRunBounds(run: reader.ImageRun) struct { min_x: f64, max_x: f64, min_y: f64, max_y: f64 } {
    const x0 = run.e;
    const y0 = run.f;
    const x1 = run.a + run.e;
    const y1 = run.b + run.f;
    const x2 = run.c + run.e;
    const y2 = run.d + run.f;
    const x3 = run.a + run.c + run.e;
    const y3 = run.b + run.d + run.f;
    return .{
        .min_x = @min(@min(x0, x1), @min(x2, x3)),
        .max_x = @max(@max(x0, x1), @max(x2, x3)),
        .min_y = @min(@min(y0, y1), @min(y2, y3)),
        .max_y = @max(@max(y0, y1), @max(y2, y3)),
    };
}

fn drawShapeRun(canvas: []u8, canvas_w: usize, canvas_h: usize, min_x: f64, max_y: f64, run: reader.ShapeRun) void {
    const bounds = shapeRunBounds(run);
    const x0 = floorToCanvas(bounds.min_x - min_x, canvas_w);
    const x1 = ceilToCanvas(bounds.max_x - min_x, canvas_w);
    const y0 = floorToCanvas(max_y - bounds.max_y, canvas_h);
    const y1 = ceilToCanvas(max_y - bounds.min_y, canvas_h);
    const has_clip = run.clip_box != null or run.clip_points != null;

    var py = y0;
    while (py < y1) : (py += 1) {
        var px = x0;
        while (px < x1) : (px += 1) {
            const world_x = min_x + (@as(f64, @floatFromInt(px)) + 0.5);
            const world_y = max_y - (@as(f64, @floatFromInt(py)) + 0.5);
            if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
            const dst = (py * canvas_w + px) * 4;
            if (run.kind == .fill) {
                if (pointInShape(world_x, world_y, run)) {
                    blendPixelMode(canvas, dst, run.color, run.blend_mode);
                }
            } else {
                if (pointInStrokeShape(world_x, world_y, run)) {
                    blendPixelMode(canvas, dst, run.color, run.blend_mode);
                }
            }
        }
    }
}

fn drawShadingRun(canvas: []u8, canvas_w: usize, canvas_h: usize, min_x: f64, max_y: f64, run: reader.ShadingRun) void {
    const has_clip = run.clip_box != null or run.clip_points != null;
    var py: usize = 0;
    while (py < canvas_h) : (py += 1) {
        var px: usize = 0;
        while (px < canvas_w) : (px += 1) {
            const world_x = min_x + (@as(f64, @floatFromInt(px)) + 0.5);
            const world_y = max_y - (@as(f64, @floatFromInt(py)) + 0.5);
            if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
            const t_opt = switch (run.kind) {
                .axial => axialShadingT(world_x, world_y, run),
                .radial => radialShadingT(world_x, world_y, run),
            };
            const t = t_opt orelse continue;
            const color = lerpColor(run.c0, run.c1, t);
            blendPixelMode(canvas, (py * canvas_w + px) * 4, color, run.blend_mode);
        }
    }
}

fn drawPatternRun(
    alloc: Allocator,
    canvas: []u8,
    canvas_w: usize,
    canvas_h: usize,
    min_x: f64,
    max_y: f64,
    run: reader.PatternRun,
) anyerror!void {
    const ShapeKind = @FieldType(reader.ShapeRun, "kind");
    const bounds = shapeRunBounds(.{
        .kind = if (run.kind == .fill) ShapeKind.fill else ShapeKind.stroke,
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
        .dash_array = run.dash_array,
        .dash_phase = run.dash_phase,
        .color = .{ 0, 0, 0, 0 },
        .stroke_width = run.stroke_width,
        .closed = run.closed,
        .clip_box = run.clip_box,
        .clip_points = run.clip_points,
        .clip_fill_rule = run.clip_fill_rule,
        .points = run.points,
    });
    const x0 = floorToCanvas(bounds.min_x - min_x, canvas_w);
    const x1 = ceilToCanvas(bounds.max_x - min_x, canvas_w);
    const y0 = floorToCanvas(max_y - bounds.max_y, canvas_h);
    const y1 = ceilToCanvas(max_y - bounds.min_y, canvas_h);
    const has_clip = run.clip_box != null or run.clip_points != null;

    if (run.mode == .shading) {
        const shading = run.shading orelse return;
        var py: usize = y0;
        while (py < y1) : (py += 1) {
            var px: usize = x0;
            while (px < x1) : (px += 1) {
                const world_x = min_x + (@as(f64, @floatFromInt(px)) + 0.5);
                const world_y = max_y - (@as(f64, @floatFromInt(py)) + 0.5);
                if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
                const target_hit = if (run.kind == .fill)
                    switch (run.fill_rule) {
                        .even_odd => pointInPolygonEvenOdd(world_x, world_y, run.points),
                        .nonzero => pointInPolygonNonZero(world_x, world_y, run.points),
                    }
                else blk: {
                    const tmp: reader.ShapeRun = .{
                        .kind = .stroke,
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
                        .dash_array = run.dash_array,
                        .dash_phase = run.dash_phase,
                        .color = .{ 0, 0, 0, 0 },
                        .stroke_width = run.stroke_width,
                        .closed = run.closed,
                        .clip_box = run.clip_box,
                        .clip_points = run.clip_points,
                        .clip_fill_rule = run.clip_fill_rule,
                        .points = run.points,
                    };
                    break :blk pointInStrokeShape(world_x, world_y, tmp);
                };
                if (!target_hit) continue;
                const t_opt = switch (shading.kind) {
                    .axial => axialShadingT(world_x, world_y, shading),
                    .radial => radialShadingT(world_x, world_y, shading),
                };
                const t = t_opt orelse continue;
                const color = lerpColor(shading.c0, shading.c1, t);
                blendPixelMode(canvas, (py * canvas_w + px) * 4, color, run.blend_mode);
            }
        }
        return;
    }

    const tile = try renderPatternTileCanvasAlloc(alloc, run);
    defer alloc.free(tile.rgba);
    const det = run.pattern_matrix.a * run.pattern_matrix.d - run.pattern_matrix.b * run.pattern_matrix.c;
    if (@abs(det) < 0.000001) return;
    const inv_a = run.pattern_matrix.d / det;
    const inv_b = -run.pattern_matrix.b / det;
    const inv_c = -run.pattern_matrix.c / det;
    const inv_d = run.pattern_matrix.a / det;

    var py = y0;
    while (py < y1) : (py += 1) {
        var px = x0;
        while (px < x1) : (px += 1) {
            const world_x = min_x + (@as(f64, @floatFromInt(px)) + 0.5);
            const world_y = max_y - (@as(f64, @floatFromInt(py)) + 0.5);
            if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
            const target_hit = if (run.kind == .fill)
                switch (run.fill_rule) {
                    .even_odd => pointInPolygonEvenOdd(world_x, world_y, run.points),
                    .nonzero => pointInPolygonNonZero(world_x, world_y, run.points),
                }
            else blk: {
                const tmp: reader.ShapeRun = .{
                    .kind = .stroke,
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
                    .dash_array = run.dash_array,
                    .dash_phase = run.dash_phase,
                    .color = .{ 0, 0, 0, 0 },
                    .stroke_width = run.stroke_width,
                    .closed = run.closed,
                    .clip_box = run.clip_box,
                    .clip_points = run.clip_points,
                    .clip_fill_rule = run.clip_fill_rule,
                    .points = run.points,
                };
                break :blk pointInStrokeShape(world_x, world_y, tmp);
            };
            if (!target_hit) continue;

            const dx = world_x - run.pattern_matrix.e;
            const dy = world_y - run.pattern_matrix.f;
            const pattern_x = inv_a * dx + inv_c * dy;
            const pattern_y = inv_b * dx + inv_d * dy;
            if (!finite(pattern_x) or !finite(pattern_y)) continue;
            const local_x = positiveModulo(pattern_x - run.pattern_bbox.min_x, run.pattern_x_step);
            const local_y = positiveModulo(pattern_y - run.pattern_bbox.min_y, run.pattern_y_step);
            const sample_x = run.pattern_bbox.min_x + local_x;
            const sample_y = run.pattern_bbox.min_y + local_y;
            if (!finite(sample_x) or !finite(sample_y)) continue;
            if (sample_x < run.pattern_bbox.min_x or sample_x > run.pattern_bbox.max_x or sample_y < run.pattern_bbox.min_y or sample_y > run.pattern_bbox.max_y) continue;
            const sx = floorToCanvas(sample_x - run.pattern_bbox.min_x, tile.width);
            const sy = floorToCanvas(run.pattern_bbox.max_y - sample_y, tile.height);
            if (sx >= tile.width or sy >= tile.height) continue;
            const src = (sy * tile.width + sx) * 4;
            var color: [4]u8 = .{ tile.rgba[src + 0], tile.rgba[src + 1], tile.rgba[src + 2], tile.rgba[src + 3] };
            if (run.base_color) |base_color| {
                color = .{
                    base_color[0],
                    base_color[1],
                    base_color[2],
                    @intCast((@as(u16, base_color[3]) * @as(u16, color[3]) + 127) / 255),
                };
            }
            blendPixelMode(canvas, (py * canvas_w + px) * 4, color, run.blend_mode);
        }
    }
}

const RawTile = struct {
    rgba: []u8,
    width: usize,
    height: usize,
};

fn renderPatternTileCanvasAlloc(alloc: Allocator, run: reader.PatternRun) anyerror!RawTile {
    const raw = try renderPageContentRgbaInBoxAlloc(
        alloc,
        run.pattern_bbox,
        run.tile_text_runs,
        run.tile_image_runs,
        run.tile_shading_runs,
        run.tile_pattern_runs,
        run.tile_shape_runs,
    );
    return .{ .rgba = raw.rgba, .width = raw.width, .height = raw.height };
}

fn positiveModulo(value: f64, modulus: f64) f64 {
    if (@abs(modulus) < 0.000001) return 0;
    const m = @mod(value, modulus);
    return if (m < 0) m + modulus else m;
}

fn axialShadingT(x: f64, y: f64, run: reader.ShadingRun) ?f64 {
    const dx = run.x1 - run.x0;
    const dy = run.y1 - run.y0;
    const len2 = dx * dx + dy * dy;
    if (len2 <= 0.000001) return null;
    var t = ((x - run.x0) * dx + (y - run.y0) * dy) / len2;
    if (!run.extend_start and t < 0.0) return null;
    if (!run.extend_end and t > 1.0) return null;
    t = std.math.clamp(t, 0.0, 1.0);
    return t;
}

fn radialShadingT(x: f64, y: f64, run: reader.ShadingRun) ?f64 {
    const dcx = run.x1 - run.x0;
    const dcy = run.y1 - run.y0;
    const dr = run.r1 - run.r0;
    const fx = x - run.x0;
    const fy = y - run.y0;
    const a = dcx * dcx + dcy * dcy - dr * dr;
    const b = -2.0 * (fx * dcx + fy * dcy + run.r0 * dr);
    const c = fx * fx + fy * fy - run.r0 * run.r0;

    var t: f64 = undefined;
    if (@abs(a) <= 0.000001) {
        if (@abs(b) <= 0.000001) return null;
        t = -c / b;
    } else {
        const disc = b * b - 4.0 * a * c;
        if (disc < 0) return null;
        const sqrt_disc = @sqrt(disc);
        const t0 = (-b - sqrt_disc) / (2.0 * a);
        const t1 = (-b + sqrt_disc) / (2.0 * a);
        t = if (t0 >= 0.0 and t0 <= 1.0) t0 else t1;
    }
    if (!run.extend_start and t < 0.0) return null;
    if (!run.extend_end and t > 1.0) return null;
    return std.math.clamp(t, 0.0, 1.0);
}

fn lerpColor(a: [4]u8, b: [4]u8, t: f64) [4]u8 {
    const clamped = std.math.clamp(t, 0.0, 1.0);
    return .{
        @intCast(@as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(a[0])) * (1.0 - clamped) + @as(f64, @floatFromInt(b[0])) * clamped)))),
        @intCast(@as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(a[1])) * (1.0 - clamped) + @as(f64, @floatFromInt(b[1])) * clamped)))),
        @intCast(@as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(a[2])) * (1.0 - clamped) + @as(f64, @floatFromInt(b[2])) * clamped)))),
        @intCast(@as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(a[3])) * (1.0 - clamped) + @as(f64, @floatFromInt(b[3])) * clamped)))),
    };
}

fn shapeRunBounds(run: reader.ShapeRun) struct { min_x: f64, max_x: f64, min_y: f64, max_y: f64 } {
    var min_x = run.points[0][0];
    var max_x = run.points[0][0];
    var min_y = run.points[0][1];
    var max_y = run.points[0][1];
    for (run.points[1..]) |point| {
        min_x = @min(min_x, point[0]);
        max_x = @max(max_x, point[0]);
        min_y = @min(min_y, point[1]);
        max_y = @max(max_y, point[1]);
    }
    return .{ .min_x = min_x, .max_x = max_x, .min_y = min_y, .max_y = max_y };
}

fn pointInShape(x: f64, y: f64, run: reader.ShapeRun) bool {
    return switch (run.fill_rule) {
        .even_odd => pointInPolygonEvenOdd(x, y, run.points),
        .nonzero => pointInPolygonNonZero(x, y, run.points),
    };
}

fn pointInPolygonEvenOdd(x: f64, y: f64, points: []const [2]f64) bool {
    var inside = false;
    var j = points.len - 1;
    for (points, 0..) |point, i| {
        const prev = points[j];
        const intersects = ((point[1] > y) != (prev[1] > y)) and
            (x < (prev[0] - point[0]) * (y - point[1]) / (prev[1] - point[1] + 0.000000001) + point[0]);
        if (intersects) inside = !inside;
        j = i;
    }
    return inside;
}

fn pointInPolygonNonZero(x: f64, y: f64, points: []const [2]f64) bool {
    var winding: i32 = 0;
    var j = points.len - 1;
    for (points, 0..) |point, i| {
        const prev = points[j];
        if (prev[1] <= y) {
            if (point[1] > y and isLeft(prev, point, x, y) > 0) winding += 1;
        } else {
            if (point[1] <= y and isLeft(prev, point, x, y) < 0) winding -= 1;
        }
        j = i;
    }
    return winding != 0;
}

fn isLeft(a: [2]f64, b: [2]f64, x: f64, y: f64) f64 {
    return (b[0] - a[0]) * (y - a[1]) - (x - a[0]) * (b[1] - a[1]);
}

fn polygonEdgeDistance(x: f64, y: f64, points: []const [2]f64, closed: bool) f64 {
    var best = std.math.inf(f64);
    if (points.len < 2) return best;
    var i: usize = 0;
    while (i + 1 < points.len) : (i += 1) {
        const point = points[i];
        const next = points[i + 1];
        best = @min(best, pointSegmentDistance(x, y, point, next));
    }
    if (closed and points.len > 2) {
        best = @min(best, pointSegmentDistance(x, y, points[points.len - 1], points[0]));
    }
    return best;
}

fn pointInStrokeShape(x: f64, y: f64, run: reader.ShapeRun) bool {
    const radius = run.stroke_width / 2.0;
    if (strokeContainsPoint(x, y, run, radius)) return true;

    if (run.points.len > 2) {
        const limit = if (run.closed) run.points.len else run.points.len - 1;
        var i: usize = if (run.closed) 0 else 1;
        while (i < limit) : (i += 1) {
            const prev = if (i == 0) run.points[run.points.len - 1] else run.points[i - 1];
            const curr = run.points[i];
            const next = if (i + 1 == run.points.len) run.points[0] else run.points[i + 1];
            switch (run.line_join) {
                .round => {
                    if (pointDistance(x, y, curr) <= radius) return true;
                },
                .bevel => {
                    if (pointInBevelJoin(x, y, prev, curr, next, radius)) return true;
                },
                .miter => {
                    if (pointInMiterJoin(x, y, prev, curr, next, radius, run.miter_limit)) return true;
                },
            }
        }
    }
    return false;
}

fn strokeContainsPoint(x: f64, y: f64, run: reader.ShapeRun, radius: f64) bool {
    if (run.dash_array == null or run.dash_array.?.len == 0) {
        const edge_dist = polygonEdgeDistanceWithCap(x, y, run.points, run.closed, run.line_cap, radius);
        return edge_dist <= radius;
    }

    const dash = run.dash_array.?;
    const cycle = dashCycleLength(dash);
    if (cycle <= 0.000001) {
        const edge_dist = polygonEdgeDistanceWithCap(x, y, run.points, run.closed, run.line_cap, radius);
        return edge_dist <= radius;
    }

    var offset = -run.dash_phase;
    if (offset < 0) {
        offset = @mod(offset, cycle);
        if (offset < 0) offset += cycle;
    }

    var i: usize = 0;
    while (i + 1 < run.points.len) : (i += 1) {
        const a = run.points[i];
        const b = run.points[i + 1];
        const hit = pointSegmentDistanceAndAlong(x, y, a, b);
        if (hit.distance <= radius and dashIsOn(offset + hit.along, dash)) return true;
        offset += hit.length;
    }
    if (run.closed and run.points.len > 2) {
        const hit = pointSegmentDistanceAndAlong(x, y, run.points[run.points.len - 1], run.points[0]);
        if (hit.distance <= radius and dashIsOn(offset + hit.along, dash)) return true;
    }
    return false;
}

fn polygonEdgeDistanceWithCap(
    x: f64,
    y: f64,
    points: []const [2]f64,
    closed: bool,
    line_cap: @FieldType(reader.ShapeRun, "line_cap"),
    radius: f64,
) f64 {
    var best = std.math.inf(f64);
    if (points.len < 2) return best;

    var i: usize = 0;
    while (i + 1 < points.len) : (i += 1) {
        const point = points[i];
        const next = points[i + 1];
        var extend_start: f64 = 0.0;
        var extend_end: f64 = 0.0;
        if (!closed and line_cap == .square) {
            if (i == 0) extend_start = radius;
            if (i + 2 == points.len) extend_end = radius;
        }
        best = @min(best, pointSegmentDistanceExtended(x, y, point, next, extend_start, extend_end));
    }
    if (closed and points.len > 2) {
        best = @min(best, pointSegmentDistanceExtended(x, y, points[points.len - 1], points[0], 0, 0));
    }
    if (!closed and line_cap == .round) {
        best = @min(best, pointDistance(x, y, points[0]));
        best = @min(best, pointDistance(x, y, points[points.len - 1]));
    }
    return best;
}

fn dashCycleLength(dash: []const f64) f64 {
    var total: f64 = 0;
    for (dash) |value| total += @max(0.0, value);
    return total;
}

fn dashIsOn(pos: f64, dash: []const f64) bool {
    if (dash.len == 0) return true;
    const cycle = dashCycleLength(dash);
    if (cycle <= 0.000001) return true;
    var p = @mod(pos, cycle);
    if (p < 0) p += cycle;
    var on = true;
    for (dash) |value| {
        const span = @max(0.0, value);
        if (p < span) return on;
        p -= span;
        on = !on;
    }
    return true;
}

fn pointSegmentDistanceAndAlong(x: f64, y: f64, a: [2]f64, b: [2]f64) struct { distance: f64, along: f64, length: f64 } {
    const vx = b[0] - a[0];
    const vy = b[1] - a[1];
    const wx = x - a[0];
    const wy = y - a[1];
    const vv = vx * vx + vy * vy;
    if (vv <= 0.000001) {
        return .{ .distance = pointDistance(x, y, a), .along = 0, .length = 0 };
    }
    const len = @sqrt(vv);
    const t = std.math.clamp((wx * vx + wy * vy) / vv, 0.0, 1.0);
    const px = a[0] + t * vx;
    const py = a[1] + t * vy;
    const dx = x - px;
    const dy = y - py;
    return .{
        .distance = @sqrt(dx * dx + dy * dy),
        .along = t * len,
        .length = len,
    };
}

fn pointSegmentDistanceExtended(x: f64, y: f64, a: [2]f64, b: [2]f64, extend_start: f64, extend_end: f64) f64 {
    const vx = b[0] - a[0];
    const vy = b[1] - a[1];
    const vv = vx * vx + vy * vy;
    if (vv <= 0.000001) return pointDistance(x, y, a);

    const len = @sqrt(vv);
    const ux = vx / len;
    const uy = vy / len;
    const ax = a[0] - ux * extend_start;
    const ay = a[1] - uy * extend_start;
    const bx = b[0] + ux * extend_end;
    const by = b[1] + uy * extend_end;
    return pointSegmentDistance(x, y, .{ ax, ay }, .{ bx, by });
}

fn pointSegmentDistance(x: f64, y: f64, a: [2]f64, b: [2]f64) f64 {
    const vx = b[0] - a[0];
    const vy = b[1] - a[1];
    const wx = x - a[0];
    const wy = y - a[1];
    const vv = vx * vx + vy * vy;
    if (vv <= 0.000001) return @sqrt(wx * wx + wy * wy);
    const t = std.math.clamp((wx * vx + wy * vy) / vv, 0.0, 1.0);
    const px = a[0] + t * vx;
    const py = a[1] + t * vy;
    const dx = x - px;
    const dy = y - py;
    return @sqrt(dx * dx + dy * dy);
}

fn pointDistance(x: f64, y: f64, p: [2]f64) f64 {
    const dx = x - p[0];
    const dy = y - p[1];
    return @sqrt(dx * dx + dy * dy);
}

fn pointInBevelJoin(x: f64, y: f64, prev: [2]f64, curr: [2]f64, next: [2]f64, radius: f64) bool {
    const wedge = joinOuterWedge(prev, curr, next, radius) orelse return false;
    return pointInTriangle(x, y, curr, wedge.a, wedge.b);
}

fn pointInMiterJoin(x: f64, y: f64, prev: [2]f64, curr: [2]f64, next: [2]f64, radius: f64, miter_limit: f64) bool {
    const wedge = joinOuterWedge(prev, curr, next, radius) orelse return false;
    const miter = computeMiterPoint(prev, curr, next, radius, miter_limit) orelse {
        return pointInTriangle(x, y, curr, wedge.a, wedge.b);
    };
    return pointInConvexQuad(x, y, curr, wedge.a, miter, wedge.b);
}

fn joinOuterWedge(prev: [2]f64, curr: [2]f64, next: [2]f64, radius: f64) ?struct { a: [2]f64, b: [2]f64 } {
    const unit_prev = normalizedSegment(prev, curr) orelse return null;
    const unit_next = normalizedSegment(curr, next) orelse return null;
    const turn = unit_prev[0] * unit_next[1] - unit_prev[1] * unit_next[0];
    if (@abs(turn) < 0.000001) return null;
    const n1 = if (turn > 0) [2]f64{ unit_prev[1], -unit_prev[0] } else [2]f64{ -unit_prev[1], unit_prev[0] };
    const n2 = if (turn > 0) [2]f64{ unit_next[1], -unit_next[0] } else [2]f64{ -unit_next[1], unit_next[0] };
    return .{
        .a = .{ curr[0] + n1[0] * radius, curr[1] + n1[1] * radius },
        .b = .{ curr[0] + n2[0] * radius, curr[1] + n2[1] * radius },
    };
}

fn computeMiterPoint(prev: [2]f64, curr: [2]f64, next: [2]f64, radius: f64, miter_limit: f64) ?[2]f64 {
    const unit_prev = normalizedSegment(prev, curr) orelse return null;
    const unit_next = normalizedSegment(curr, next) orelse return null;
    const turn = unit_prev[0] * unit_next[1] - unit_prev[1] * unit_next[0];
    if (@abs(turn) < 0.000001) return null;
    const n1 = if (turn > 0) [2]f64{ unit_prev[1], -unit_prev[0] } else [2]f64{ -unit_prev[1], unit_prev[0] };
    const n2 = if (turn > 0) [2]f64{ unit_next[1], -unit_next[0] } else [2]f64{ -unit_next[1], unit_next[0] };
    const p1 = [2]f64{ curr[0] + n1[0] * radius, curr[1] + n1[1] * radius };
    const p2 = [2]f64{ curr[0] + n2[0] * radius, curr[1] + n2[1] * radius };
    const intersection = lineIntersection(
        p1,
        .{ p1[0] + unit_prev[0], p1[1] + unit_prev[1] },
        p2,
        .{ p2[0] + unit_next[0], p2[1] + unit_next[1] },
    ) orelse return null;
    if (pointDistance(intersection[0], intersection[1], curr) > radius * miter_limit) return null;
    return intersection;
}

fn normalizedSegment(a: [2]f64, b: [2]f64) ?[2]f64 {
    const dx = b[0] - a[0];
    const dy = b[1] - a[1];
    const len = @sqrt(dx * dx + dy * dy);
    if (len <= 0.000001) return null;
    return .{ dx / len, dy / len };
}

fn lineIntersection(a1: [2]f64, a2: [2]f64, b1: [2]f64, b2: [2]f64) ?[2]f64 {
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

fn pointInTriangle(x: f64, y: f64, a: [2]f64, b: [2]f64, c: [2]f64) bool {
    return pointInPolygonEvenOdd(x, y, &.{ a, b, c });
}

fn pointInConvexQuad(x: f64, y: f64, a: [2]f64, b: [2]f64, c: [2]f64, d: [2]f64) bool {
    return pointInPolygonEvenOdd(x, y, &.{ a, b, c, d });
}

fn drawScaledGlyphBox(
    rgba: []u8,
    width: usize,
    height: usize,
    x: usize,
    y: usize,
    glyph_w: usize,
    glyph_h: usize,
    margin: usize,
    min_x: f64,
    max_y: f64,
    clip_box: ?reader.PageBox,
    clip_points: ?[]const [2]f64,
    clip_fill_rule: @FieldType(reader.ShapeRun, "fill_rule"),
) void {
    const inner_w = if (glyph_w > 2) glyph_w - 2 else glyph_w;
    const inner_h = if (glyph_h > 2) glyph_h - 2 else glyph_h;
    const has_clip = clip_box != null or clip_points != null;
    var row: usize = 0;
    while (row < glyph_h and y + row < height) : (row += 1) {
        var col: usize = 0;
        while (col < glyph_w and x + col < width) : (col += 1) {
            const border = row == 0 or row + 1 == glyph_h or col == 0 or col + 1 == glyph_w;
            const fill = row >= 1 and row < 1 + inner_h and col >= 1 and col < 1 + inner_w and ((row + col) % 2 == 0);
            if (border or fill) {
                const world_x = pixelWorldX(min_x, margin, x + col);
                const world_y = pixelWorldY(max_y, margin, y + row);
                if (has_clip and !pointPassesClip(world_x, world_y, clip_box, clip_points, clip_fill_rule)) continue;
                const idx = ((y + row) * width + (x + col)) * 4;
                rgba[idx + 0] = 0;
                rgba[idx + 1] = 0;
                rgba[idx + 2] = 0;
                rgba[idx + 3] = 0xff;
            }
        }
    }
}

fn drawAffineGlyphBox(
    rgba: []u8,
    width: usize,
    height: usize,
    margin: usize,
    min_x: f64,
    max_y: f64,
    run: reader.TextRun,
    local_x: f64,
    local_w: f64,
) void {
    if (local_w <= 0 or run.font_size <= 0) return;

    const det = run.a * run.d - run.b * run.c;
    if (@abs(det) < 0.000001) return;

    const corners = [_][2]f64{
        .{ run.x + run.a * local_x - run.c * run.descent, run.y + run.b * local_x - run.d * run.descent },
        .{ run.x + run.a * (local_x + local_w) - run.c * run.descent, run.y + run.b * (local_x + local_w) - run.d * run.descent },
        .{ run.x + run.a * local_x + run.c * run.ascent, run.y + run.b * local_x + run.d * run.ascent },
        .{ run.x + run.a * (local_x + local_w) + run.c * run.ascent, run.y + run.b * (local_x + local_w) + run.d * run.ascent },
    };

    var min_world_x = corners[0][0];
    var max_world_x = corners[0][0];
    var min_world_y = corners[0][1];
    var max_world_y = corners[0][1];
    for (corners[1..]) |corner| {
        min_world_x = @min(min_world_x, corner[0]);
        max_world_x = @max(max_world_x, corner[0]);
        min_world_y = @min(min_world_y, corner[1]);
        max_world_y = @max(max_world_y, corner[1]);
    }

    const margin_f: f64 = @floatFromInt(margin);
    const x0 = floorToCanvas(margin_f + min_world_x - min_x, width);
    const x1 = ceilToCanvas(margin_f + max_world_x - min_x, width);
    const y0 = floorToCanvas(margin_f + max_y - max_world_y, height);
    const y1 = ceilToCanvas(margin_f + max_y - min_world_y, height);

    const inv_a = run.d / det;
    const inv_b = -run.b / det;
    const inv_c = -run.c / det;
    const inv_d = run.a / det;
    const has_clip = run.clip_box != null or run.clip_points != null;

    var py = y0;
    while (py < y1) : (py += 1) {
        var px = x0;
        while (px < x1) : (px += 1) {
            const world_x = pixelWorldX(min_x, margin, px);
            const world_y = pixelWorldY(max_y, margin, py);
            if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;

            const dx = world_x - run.x;
            const dy = world_y - run.y;
            const lx = inv_a * dx + inv_c * dy;
            const ly = inv_b * dx + inv_d * dy;
            if (lx < local_x or lx > local_x + local_w or ly < -run.descent or ly > run.ascent) continue;
            if (glyphModeColor(run, local_x, local_w, lx, ly)) |color| {
                blendPixelMode(rgba, (py * width + px) * 4, color, run.blend_mode);
            }
        }
    }
}

fn glyphModeColor(run: reader.TextRun, local_x: f64, local_w: f64, lx: f64, ly: f64) ?[4]u8 {
    const mode = @mod(run.render_mode, 8);
    return switch (mode) {
        0, 4 => colorWithAlpha(run.fill_color, run.alpha),
        1, 5 => if (glyphPointIsStroke(run, local_x, local_w, lx, ly)) colorWithAlpha(run.stroke_color, run.stroke_alpha) else null,
        2, 6 => if (glyphPointIsStroke(run, local_x, local_w, lx, ly)) colorWithAlpha(run.stroke_color, run.stroke_alpha) else colorWithAlpha(run.fill_color, run.alpha),
        3, 7 => null,
        else => colorWithAlpha(run.fill_color, run.alpha),
    };
}

fn glyphPointIsStroke(run: reader.TextRun, local_x: f64, local_w: f64, lx: f64, ly: f64) bool {
    const usable_w = @max(1.0, local_w);
    const usable_h = @max(1.0, run.ascent + run.descent);
    const basis_x = @sqrt(run.a * run.a + run.b * run.b);
    const basis_y = @sqrt(run.c * run.c + run.d * run.d);
    const avg_scale = @max(0.000001, (basis_x + basis_y) / 2.0);
    const stroke_from_width = run.stroke_width / avg_scale;
    const stroke = std.math.clamp(@max(stroke_from_width, @min(usable_w, usable_h) * 0.12), 0.5, @min(usable_w, usable_h) / 2.0);
    const left = lx - local_x;
    const right = local_x + local_w - lx;
    const bottom = ly + run.descent;
    const top = run.ascent - ly;
    return left <= stroke or right <= stroke or bottom <= stroke or top <= stroke;
}

fn blendChannel(mode: reader.BlendMode, src: u8, dst: u8) u8 {
    return switch (mode) {
        .normal => src,
        .multiply => @intCast((@as(u16, src) * @as(u16, dst) + 127) / 255),
        .screen => @intCast(255 - ((@as(u16, 255 - src) * @as(u16, 255 - dst) + 127) / 255)),
        .overlay => if (dst < 128)
            @intCast((2 * @as(u16, src) * @as(u16, dst) + 127) / 255)
        else
            @intCast(255 - ((2 * @as(u16, 255 - src) * @as(u16, 255 - dst) + 127) / 255)),
        .darken => @min(src, dst),
        .lighten => @max(src, dst),
    };
}

fn blendPixelMode(canvas: []u8, dst: usize, src: [4]u8, mode: reader.BlendMode) void {
    const sa = @as(u32, src[3]);
    if (sa == 0) return;

    if (mode == .normal and sa == 255) {
        canvas[dst + 0] = src[0];
        canvas[dst + 1] = src[1];
        canvas[dst + 2] = src[2];
        canvas[dst + 3] = 0xff;
        return;
    }

    if (mode == .normal) {
        const inv_sa = 255 - sa;
        canvas[dst + 0] = @intCast((@as(u32, src[0]) * sa + @as(u32, canvas[dst + 0]) * inv_sa + 127) / 255);
        canvas[dst + 1] = @intCast((@as(u32, src[1]) * sa + @as(u32, canvas[dst + 1]) * inv_sa + 127) / 255);
        canvas[dst + 2] = @intCast((@as(u32, src[2]) * sa + @as(u32, canvas[dst + 2]) * inv_sa + 127) / 255);
        canvas[dst + 3] = 0xff;
        return;
    }

    const blended_r = blendChannel(mode, src[0], canvas[dst + 0]);
    const blended_g = blendChannel(mode, src[1], canvas[dst + 1]);
    const blended_b = blendChannel(mode, src[2], canvas[dst + 2]);
    const inv_sa = 255 - sa;
    canvas[dst + 0] = @intCast((@as(u32, blended_r) * sa + @as(u32, canvas[dst + 0]) * inv_sa + 127) / 255);
    canvas[dst + 1] = @intCast((@as(u32, blended_g) * sa + @as(u32, canvas[dst + 1]) * inv_sa + 127) / 255);
    canvas[dst + 2] = @intCast((@as(u32, blended_b) * sa + @as(u32, canvas[dst + 2]) * inv_sa + 127) / 255);
    canvas[dst + 3] = 0xff;
}

fn compositeGroupCanvas(canvas: []u8, group_canvas: []const u8) void {
    var i: usize = 0;
    while (i + 3 < group_canvas.len) : (i += 4) {
        if (group_canvas[i + 3] == 0) continue;
        blendPixelMode(canvas, i, .{ group_canvas[i + 0], group_canvas[i + 1], group_canvas[i + 2], group_canvas[i + 3] }, .normal);
    }
}

fn clearCanvasWhereOpaque(canvas: []u8, mask: []const u8) void {
    var i: usize = 0;
    while (i + 3 < mask.len) : (i += 4) {
        if (mask[i + 3] == 0) continue;
        canvas[i + 0] = 0;
        canvas[i + 1] = 0;
        canvas[i + 2] = 0;
        canvas[i + 3] = 0;
    }
}

fn replaceCanvasWhereChanged(canvas: []u8, next: []const u8, backdrop: []const u8) void {
    var i: usize = 0;
    while (i + 3 < next.len) : (i += 4) {
        if (next[i + 0] == backdrop[i + 0] and
            next[i + 1] == backdrop[i + 1] and
            next[i + 2] == backdrop[i + 2] and
            next[i + 3] == backdrop[i + 3]) continue;
        canvas[i + 0] = next[i + 0];
        canvas[i + 1] = next[i + 1];
        canvas[i + 2] = next[i + 2];
        canvas[i + 3] = next[i + 3];
    }
}

fn colorWithAlpha(color: [4]u8, alpha: u8) [4]u8 {
    return .{
        color[0],
        color[1],
        color[2],
        @intCast((@as(u16, color[3]) * @as(u16, alpha) + 127) / 255),
    };
}

fn pointPassesClip(
    x: f64,
    y: f64,
    clip_box: ?reader.PageBox,
    clip_points: ?[]const [2]f64,
    clip_fill_rule: @FieldType(reader.ShapeRun, "fill_rule"),
) bool {
    if (clip_box) |clip| {
        if (x < clip.min_x or x > clip.max_x or y < clip.min_y or y > clip.max_y) return false;
    }
    if (clip_points) |points| {
        if (points.len < 3) return false;
        return switch (clip_fill_rule) {
            .even_odd => pointInPolygonEvenOdd(x, y, points),
            .nonzero => pointInPolygonNonZero(x, y, points),
        };
    }
    return true;
}

test "blend pixel mode multiply darkens with backdrop" {
    var canvas = [_]u8{ 0xff, 0x00, 0x00, 0xff };
    blendPixelMode(&canvas, 0, .{ 0x00, 0x00, 0xff, 0xff }, .multiply);
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0x00, 0xff }, &canvas);
}

test "blend pixel mode screen combines source and backdrop" {
    var canvas = [_]u8{ 0xff, 0x00, 0x00, 0xff };
    blendPixelMode(&canvas, 0, .{ 0x00, 0x00, 0xff, 0xff }, .screen);
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0x00, 0xff, 0xff }, &canvas);
}

test "blend pixel mode normal opaque replaces backdrop" {
    var canvas = [_]u8{ 0x20, 0x40, 0x60, 0xff };
    blendPixelMode(&canvas, 0, .{ 0x80, 0x90, 0xa0, 0xff }, .normal);
    try std.testing.expectEqualSlices(u8, &.{ 0x80, 0x90, 0xa0, 0xff }, &canvas);
}

test "blend pixel mode normal alpha blends without blend channel math" {
    var canvas = [_]u8{ 0x20, 0x40, 0x60, 0xff };
    blendPixelMode(&canvas, 0, .{ 0x80, 0x90, 0xa0, 0x80 }, .normal);
    try std.testing.expectEqualSlices(u8, &.{ 0x50, 0x68, 0x80, 0xff }, &canvas);
}

test "knockout groups remove prior sibling contribution before compositing" {
    const alloc = std.testing.allocator;
    const page_box: reader.PageBox = .{ .min_x = 0, .min_y = 0, .max_x = 4, .max_y = 4 };
    const shape_runs = [_]reader.ShapeRun{
        .{
            .kind = .fill,
            .paint_order = 0,
            .group_id = 1,
            .group_isolated = true,
            .group_knockout = true,
            .color = .{ 0xff, 0x00, 0x00, 0xff },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 0, 0 }, .{ 4, 0 }, .{ 4, 4 }, .{ 0, 4 } }),
        },
        .{
            .kind = .fill,
            .paint_order = 1,
            .group_id = 1,
            .group_isolated = true,
            .group_knockout = true,
            .color = .{ 0x00, 0x00, 0xff, 0x80 },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 1, 1 }, .{ 3, 1 }, .{ 3, 3 }, .{ 1, 3 } }),
        },
    };

    const raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, &.{}, &.{}, &.{}, &.{}, &shape_runs);
    defer alloc.free(raw.rgba);

    const corner = (0 * raw.width + 0) * 4;
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0x00, 0x00, 0xff }, raw.rgba[corner .. corner + 4]);

    const center = (1 * raw.width + 1) * 4;
    try std.testing.expect(raw.rgba[center + 1] > 0);
    try std.testing.expect(raw.rgba[center + 2] > raw.rgba[center + 0]);
    try std.testing.expect(raw.rgba[center + 0] > 0);
}

test "non isolated knockout groups preserve backdrop while replacing sibling overlap" {
    const alloc = std.testing.allocator;
    const page_box: reader.PageBox = .{ .min_x = 0, .min_y = 0, .max_x = 4, .max_y = 4 };
    const shape_runs = [_]reader.ShapeRun{
        .{
            .kind = .fill,
            .paint_order = 0,
            .group_id = null,
            .color = .{ 0x00, 0xff, 0x00, 0xff },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 0, 0 }, .{ 4, 0 }, .{ 4, 4 }, .{ 0, 4 } }),
        },
        .{
            .kind = .fill,
            .paint_order = 1,
            .group_id = 7,
            .group_isolated = false,
            .group_knockout = true,
            .color = .{ 0xff, 0x00, 0x00, 0xff },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 0, 0 }, .{ 4, 0 }, .{ 4, 4 }, .{ 0, 4 } }),
        },
        .{
            .kind = .fill,
            .paint_order = 2,
            .group_id = 7,
            .group_isolated = false,
            .group_knockout = true,
            .color = .{ 0x00, 0x00, 0xff, 0x80 },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 1, 1 }, .{ 3, 1 }, .{ 3, 3 }, .{ 1, 3 } }),
        },
    };

    const raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, &.{}, &.{}, &.{}, &.{}, &shape_runs);
    defer alloc.free(raw.rgba);

    const corner = (0 * raw.width + 0) * 4;
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0x00, 0x00, 0xff }, raw.rgba[corner .. corner + 4]);

    const center = (1 * raw.width + 1) * 4;
    try std.testing.expect(raw.rgba[center + 1] > 0);
    try std.testing.expect(raw.rgba[center + 2] > raw.rgba[center + 0]);
}

test "nested non isolated groups composite against parent backdrop" {
    const alloc = std.testing.allocator;
    const page_box: reader.PageBox = .{ .min_x = 0, .min_y = 0, .max_x = 4, .max_y = 4 };
    const shape_runs = [_]reader.ShapeRun{
        .{
            .kind = .fill,
            .paint_order = 0,
            .group_id = null,
            .color = .{ 0x00, 0xff, 0x00, 0xff },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 0, 0 }, .{ 4, 0 }, .{ 4, 4 }, .{ 0, 4 } }),
        },
        .{
            .kind = .fill,
            .paint_order = 1,
            .group_id = 1,
            .group_parent_id = null,
            .group_isolated = false,
            .color = .{ 0xff, 0x00, 0x00, 0xff },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 0, 0 }, .{ 4, 0 }, .{ 4, 4 }, .{ 0, 4 } }),
        },
        .{
            .kind = .fill,
            .paint_order = 2,
            .group_id = 2,
            .group_parent_id = 1,
            .group_isolated = false,
            .color = .{ 0x00, 0x00, 0xff, 0x80 },
            .stroke_width = 0,
            .closed = true,
            .points = @constCast(&[_][2]f64{ .{ 1, 1 }, .{ 3, 1 }, .{ 3, 3 }, .{ 1, 3 } }),
        },
    };

    const raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, &.{}, &.{}, &.{}, &.{}, &shape_runs);
    defer alloc.free(raw.rgba);

    const center = (1 * raw.width + 1) * 4;
    try std.testing.expect(raw.rgba[center + 0] > 0);
    try std.testing.expectEqual(@as(u8, 0), raw.rgba[center + 1]);
    try std.testing.expect(raw.rgba[center + 2] > 0);
}

test "draw axial shading run interpolates colors" {
    var canvas: [8 * 4 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShadingRun(&canvas, 8, 4, 0, 4, .{
        .kind = .axial,
        .x0 = 0,
        .y0 = 2,
        .x1 = 8,
        .y1 = 2,
        .c0 = .{ 0xff, 0x00, 0x00, 0xff },
        .c1 = .{ 0x00, 0x00, 0xff, 0xff },
    });
    const left = (1 * 8 + 1) * 4;
    const right = (1 * 8 + 6) * 4;
    try std.testing.expect(canvas[left + 0] > canvas[left + 2]);
    try std.testing.expect(canvas[right + 2] > canvas[right + 0]);
}

fn estimateRunWidth(run: reader.TextRun) f64 {
    if (run.advance_width > 0) return run.advance_width;
    return estimateRunWidthFallback(run);
}

fn estimateRunWidthFallback(run: reader.TextRun) f64 {
    var width_est: f64 = 0;
    var it = std.unicode.Utf8View.init(run.text) catch {
        for (run.text) |ch| width_est += baseRunCodepointAdvance(run, if (ch == ' ') ' ' else 0xfffd);
        return width_est;
    };
    var iter = it.iterator();
    while (iter.nextCodepoint()) |cp| width_est += baseRunCodepointAdvance(run, cp);
    return width_est;
}

fn baseRunCodepointAdvance(run: reader.TextRun, cp: u21) f64 {
    const glyph = run.font_size * 0.6;
    const spacing = run.char_spacing + if (cp == ' ') run.word_spacing else 0.0;
    return (glyph + spacing) * run.horizontal_scale;
}

fn estimatedRunAdvanceScale(run: reader.TextRun) f64 {
    const fallback_total = estimateRunWidthFallback(run);
    if (run.advance_width > 0 and fallback_total > 0.000001) {
        return run.advance_width / fallback_total;
    }
    return 1.0;
}

fn estimatedRunCodepointAdvance(run: reader.TextRun, cp: u21, advance_scale: f64) f64 {
    return baseRunCodepointAdvance(run, cp) * advance_scale;
}

fn textRunBounds(run: reader.TextRun) struct { min_x: f64, max_x: f64, min_y: f64, max_y: f64 } {
    const width_est = estimateRunWidth(run);
    const corners = [_][2]f64{
        .{ run.x - run.c * run.descent, run.y - run.d * run.descent },
        .{ run.x + run.a * width_est - run.c * run.descent, run.y + run.b * width_est - run.d * run.descent },
        .{ run.x + run.c * run.ascent, run.y + run.d * run.ascent },
        .{ run.x + run.a * width_est + run.c * run.ascent, run.y + run.b * width_est + run.d * run.ascent },
    };

    var min_x = corners[0][0];
    var max_x = corners[0][0];
    var min_y = corners[0][1];
    var max_y = corners[0][1];
    for (corners[1..]) |corner| {
        min_x = @min(min_x, corner[0]);
        max_x = @max(max_x, corner[0]);
        min_y = @min(min_y, corner[1]);
        max_y = @max(max_y, corner[1]);
    }
    return .{ .min_x = min_x, .max_x = max_x, .min_y = min_y, .max_y = max_y };
}

test "render text preview writes png signature" {
    const alloc = std.testing.allocator;
    const png = try renderTextPreviewPng(alloc, "Hello\nPDF");
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "render text runs writes png signature" {
    const alloc = std.testing.allocator;
    const runs = [_]reader.TextRun{
        .{ .text = try alloc.dupe(u8, "Hello"), .x = 10, .y = 20, .font_size = 12 },
        .{ .text = try alloc.dupe(u8, "World"), .x = 10, .y = 40, .font_size = 12 },
    };
    defer {
        for (runs) |run| alloc.free(run.text);
    }
    const png = try renderTextRunsPng(alloc, &runs);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "render page content in box uses page dimensions" {
    const alloc = std.testing.allocator;
    const runs = [_]reader.TextRun{
        .{ .text = try alloc.dupe(u8, "Hello"), .x = 20, .y = 80, .font_size = 12 },
    };
    defer {
        for (runs) |run| alloc.free(run.text);
    }

    const png = try renderPageContentPngInBox(
        alloc,
        .{ .min_x = 10, .min_y = 20, .max_x = 210, .max_y = 120 },
        &runs,
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
    try std.testing.expectEqual(@as(u32, 200), std.mem.readInt(u32, png[16..20], .big));
    try std.testing.expectEqual(@as(u32, 100), std.mem.readInt(u32, png[20..24], .big));
}

test "estimate run width includes spacing and horizontal scale" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "A B");
    defer alloc.free(text);
    const run = reader.TextRun{
        .text = text,
        .x = 0,
        .y = 0,
        .font_size = 10,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .horizontal_scale = 1.5,
        .char_spacing = 2,
        .word_spacing = 5,
    };
    const width = estimateRunWidth(run);
    try std.testing.expectApproxEqAbs(@as(f64, 40.5), width, 0.001);
}

test "estimate run width prefers measured advance width" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "AB");
    defer alloc.free(text);
    const run = reader.TextRun{
        .text = text,
        .x = 0,
        .y = 0,
        .font_size = 10,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .advance_width = 12,
    };
    try std.testing.expectApproxEqAbs(@as(f64, 12), estimateRunWidth(run), 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 6), estimatedRunCodepointAdvance(run, 'A', estimatedRunAdvanceScale(run)), 0.001);
}

test "text run bounds respect ascent and descent" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "A");
    defer alloc.free(text);
    const bounds = textRunBounds(.{
        .text = text,
        .x = 10,
        .y = 20,
        .font_size = 12,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .advance_width = 8,
        .ascent = 9,
        .descent = 3,
    });
    try std.testing.expectApproxEqAbs(@as(f64, 10), bounds.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 18), bounds.max_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 17), bounds.min_y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 29), bounds.max_y, 0.001);
}

test "draw shape run renders open stroked path pixels" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 2, 2 }, .{ 18, 18 } });
    defer alloc.free(points);

    var canvas: [20 * 20 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 20, 20, 0, 20, .{
        .kind = .stroke,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 2,
        .closed = false,
        .points = points,
    });

    var changed = false;
    for (canvas, 0..) |b, i| {
        if (@mod(i, 4) == 3) continue;
        if (b != 0xff) {
            changed = true;
            break;
        }
    }
    try std.testing.expect(changed);
}

test "draw shape run distinguishes nonzero and even-odd fill" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{
        .{ 2, 2 },
        .{ 18, 2 },
        .{ 18, 18 },
        .{ 2, 18 },
        .{ 2, 2 },
        .{ 18, 2 },
        .{ 18, 18 },
        .{ 2, 18 },
    });
    defer alloc.free(points);

    var canvas_nonzero: [20 * 20 * 4]u8 = undefined;
    @memset(&canvas_nonzero, 0xff);
    drawShapeRun(&canvas_nonzero, 20, 20, 0, 20, .{
        .kind = .fill,
        .fill_rule = .nonzero,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 1,
        .closed = true,
        .points = points,
    });

    var canvas_evenodd: [20 * 20 * 4]u8 = undefined;
    @memset(&canvas_evenodd, 0xff);
    drawShapeRun(&canvas_evenodd, 20, 20, 0, 20, .{
        .kind = .fill,
        .fill_rule = .even_odd,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 1,
        .closed = true,
        .points = points,
    });

    const center = ((10 * 20) + 10) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas_nonzero[center]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas_evenodd[center]);
}

test "draw shape run respects clip box" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 2, 2 }, .{ 18, 2 }, .{ 18, 18 }, .{ 2, 18 } });
    defer alloc.free(points);

    var canvas: [20 * 20 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 20, 20, 0, 20, .{
        .kind = .fill,
        .fill_rule = .nonzero,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 1,
        .closed = true,
        .clip_box = .{ .min_x = 2, .min_y = 2, .max_x = 10, .max_y = 10 },
        .points = points,
    });

    const inside = ((5 * 20) + 5) * 4;
    const outside = ((15 * 20) + 15) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[inside]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[outside]);
}

test "draw shape run respects polygon clip" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 2, 2 }, .{ 18, 2 }, .{ 18, 18 }, .{ 2, 18 } });
    defer alloc.free(points);
    const clip = try alloc.dupe([2]f64, &.{ .{ 2, 2 }, .{ 18, 2 }, .{ 2, 18 } });
    defer alloc.free(clip);

    var canvas: [20 * 20 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 20, 20, 0, 20, .{
        .kind = .fill,
        .fill_rule = .nonzero,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 1,
        .closed = true,
        .clip_box = .{ .min_x = 2, .min_y = 2, .max_x = 18, .max_y = 18 },
        .clip_points = clip,
        .clip_fill_rule = .nonzero,
        .points = points,
    });

    const inside = ((5 * 20) + 5) * 4;
    const outside = ((5 * 20) + 15) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[inside]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[outside]);
}

test "draw text run respects clip box" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "A");
    defer alloc.free(text);

    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 10,
        .y = 20,
        .font_size = 12,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .clip_box = .{ .min_x = 0, .min_y = 0, .max_x = 8, .max_y = 32 },
    });

    var changed = false;
    for (canvas, 0..) |byte, i| {
        if (@mod(i, 4) == 3) continue;
        if (byte != 0xff) {
            changed = true;
            break;
        }
    }
    try std.testing.expect(!changed);
}

test "draw text run respects polygon clip" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "A");
    defer alloc.free(text);
    const clip = try alloc.dupe([2]f64, &.{ .{ 0, 0 }, .{ 32, 0 }, .{ 0, 32 } });
    defer alloc.free(clip);

    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 20,
        .y = 20,
        .font_size = 12,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .clip_box = .{ .min_x = 0, .min_y = 0, .max_x = 32, .max_y = 32 },
        .clip_points = clip,
        .clip_fill_rule = .nonzero,
    });

    var changed = false;
    for (canvas, 0..) |byte, i| {
        if (@mod(i, 4) == 3) continue;
        if (byte != 0xff) {
            changed = true;
            break;
        }
    }
    try std.testing.expect(!changed);
}

test "text run bounds follow affine transform" {
    const bounds = textRunBounds(.{
        .text = "I",
        .x = 8,
        .y = 8,
        .font_size = 8,
        .a = 0,
        .b = 1,
        .c = -1,
        .d = 0,
    });
    try std.testing.expect(bounds.min_x < bounds.max_x);
    try std.testing.expect(bounds.min_y < bounds.max_y);
    try std.testing.expectApproxEqAbs(@as(f64, 0), bounds.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 8), bounds.max_y, 0.001);
}

test "draw text run applies affine rotation" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 16,
        .y = 16,
        .font_size = 8,
        .a = 0,
        .b = 1,
        .c = -1,
        .d = 0,
    });

    var min_x: usize = 32;
    var max_x: usize = 0;
    var min_y: usize = 32;
    var max_y: usize = 0;
    for (0..32) |py| {
        for (0..32) |px| {
            if (canvas[(py * 32 + px) * 4] != 0xff) {
                min_x = @min(min_x, px);
                max_x = @max(max_x, px);
                min_y = @min(min_y, py);
                max_y = @max(max_y, py);
            }
        }
    }
    try std.testing.expect(min_x < max_x);
    try std.testing.expect(min_y < max_y);
    try std.testing.expect((max_y - min_y) > (max_x - min_x));
}

test "draw text run blends alpha" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .alpha = 0x80,
    });
    const idx = ((12 * 32) + 10) * 4;
    try std.testing.expect(canvas[idx + 0] > 0);
    try std.testing.expect(canvas[idx + 0] < 0xff);
    try std.testing.expectEqual(canvas[idx + 0], canvas[idx + 1]);
    try std.testing.expectEqual(canvas[idx + 1], canvas[idx + 2]);
}

test "draw text run stroke-only mode leaves interior white" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 1,
    });
    const edge = ((4 * 32) + 8) * 4;
    const interior = ((4 * 32) + 10) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[edge + 0]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[interior + 0]);
}

test "draw text run stroke-only mode uses stroke color" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 1,
        .stroke_color = .{ 0x00, 0x00, 0xff, 0xff },
    });
    const edge = ((4 * 32) + 8) * 4;
    try std.testing.expectEqual(@as(u8, 0x00), canvas[edge + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[edge + 1]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[edge + 2]);
}

test "draw text run fill-stroke mode still fills interior" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 2,
    });
    const interior = ((4 * 32) + 10) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[interior + 0]);
}

test "draw text run fill-stroke mode uses fill and stroke colors" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 2,
        .fill_color = .{ 0xff, 0x00, 0x00, 0xff },
        .stroke_color = .{ 0x00, 0x00, 0xff, 0xff },
    });
    const edge = ((4 * 32) + 8) * 4;
    const interior = ((4 * 32) + 10) * 4;
    try std.testing.expectEqual(@as(u8, 0x00), canvas[edge + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[edge + 1]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[edge + 2]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[interior + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[interior + 1]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[interior + 2]);
}

test "draw text run stroke-only mode uses stroke alpha" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 1,
        .stroke_alpha = 0x80,
    });
    const edge = ((4 * 32) + 8) * 4;
    try std.testing.expect(canvas[edge + 0] > 0);
    try std.testing.expect(canvas[edge + 0] < 0xff);
}

test "draw text run stroke width changes outline thickness" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);

    var thin: [32 * 32 * 4]u8 = undefined;
    @memset(&thin, 0xff);
    drawTextRun(&thin, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 1,
        .stroke_width = 1,
    });

    var thick: [32 * 32 * 4]u8 = undefined;
    @memset(&thick, 0xff);
    drawTextRun(&thick, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 1,
        .stroke_width = 3,
    });

    const near_interior = ((4 * 32) + 9) * 4;
    try std.testing.expectEqual(@as(u8, 0xff), thin[near_interior + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), thick[near_interior + 0]);
}

test "draw text run invisible mode skips rendering" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);
    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 32, 32, 0, 0, 32, .{
        .text = text,
        .x = 8,
        .y = 24,
        .font_size = 8,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 3,
    });
    for (canvas) |byte| {
        try std.testing.expectEqual(@as(u8, 0xff), byte);
    }
}

test "draw image run respects polygon clip" {
    const alloc = std.testing.allocator;
    const rgba = try alloc.dupe(u8, &.{
        0, 0, 0, 0xff,
        0, 0, 0, 0xff,
        0, 0, 0, 0xff,
        0, 0, 0, 0xff,
    });
    defer alloc.free(rgba);
    const clip = try alloc.dupe([2]f64, &.{ .{ 2, 2 }, .{ 10, 2 }, .{ 2, 10 } });
    defer alloc.free(clip);

    var canvas: [16 * 16 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawImageRun(&canvas, 16, 16, 0, 0, 16, .{
        .rgba = rgba,
        .width = 2,
        .height = 2,
        .clip_box = .{ .min_x = 2, .min_y = 2, .max_x = 10, .max_y = 10 },
        .clip_points = clip,
        .clip_fill_rule = .nonzero,
        .a = 8,
        .b = 0,
        .c = 0,
        .d = 8,
        .e = 2,
        .f = 2,
        .x = 2,
        .y = 2,
        .draw_width = 8,
        .draw_height = 8,
    });

    const inside = ((10 * 16) + 4) * 4;
    const outside = ((10 * 16) + 8) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[inside]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[outside]);
}

test "draw shape run round cap paints endpoint beyond segment" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 10, 10 }, .{ 10, 10 } });
    defer alloc.free(points);

    var canvas: [24 * 24 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 24, 24, 0, 24, .{
        .kind = .stroke,
        .fill_rule = .nonzero,
        .line_cap = .round,
        .line_join = .miter,
        .miter_limit = 10,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 6,
        .closed = false,
        .points = points,
    });

    const endpoint_pixel = ((13 * 24) + 10) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[endpoint_pixel]);
}

test "draw shape run square cap extends beyond endpoint" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 5, 10 }, .{ 15, 10 } });
    defer alloc.free(points);

    var canvas: [24 * 24 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 24, 24, 0, 24, .{
        .kind = .stroke,
        .fill_rule = .nonzero,
        .line_cap = .square,
        .line_join = .miter,
        .miter_limit = 10,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 4,
        .closed = false,
        .points = points,
    });

    const beyond_start = ((14 * 24) + 3) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[beyond_start]);
}

test "draw shape run bevel join paints outer corner wedge" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 6, 6 }, .{ 18, 6 }, .{ 18, 18 } });
    defer alloc.free(points);

    var canvas: [28 * 28 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 28, 28, 0, 28, .{
        .kind = .stroke,
        .fill_rule = .nonzero,
        .line_cap = .butt,
        .line_join = .bevel,
        .miter_limit = 10,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 6,
        .closed = false,
        .points = points,
    });

    const outer_corner = ((20 * 28) + 20) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[outer_corner]);
}

test "draw shape run miter join extends beyond bevel corner" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 6, 6 }, .{ 18, 6 }, .{ 18, 18 } });
    defer alloc.free(points);

    var canvas: [32 * 32 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 32, 32, 0, 32, .{
        .kind = .stroke,
        .fill_rule = .nonzero,
        .line_cap = .butt,
        .line_join = .miter,
        .miter_limit = 10,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 6,
        .closed = false,
        .points = points,
    });

    const miter_pixel = ((22 * 32) + 22) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[miter_pixel]);
}

test "draw shape run respects dash pattern" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 2, 10 }, .{ 18, 10 } });
    defer alloc.free(points);
    const dash = try alloc.dupe(f64, &.{ 4.0, 4.0 });
    defer alloc.free(dash);

    var canvas: [24 * 24 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 24, 24, 0, 24, .{
        .kind = .stroke,
        .fill_rule = .nonzero,
        .line_cap = .butt,
        .line_join = .miter,
        .miter_limit = 10,
        .dash_array = dash,
        .dash_phase = 0,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 2,
        .closed = false,
        .points = points,
    });

    const on_px = ((14 * 24) + 3) * 4;
    const off_px = ((14 * 24) + 7) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[on_px]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[off_px]);
}

test "draw pattern run tiles colored cell content" {
    const alloc = std.testing.allocator;
    const tile_points = try alloc.dupe([2]f64, &.{ .{ 0, 0 }, .{ 2, 0 }, .{ 2, 4 }, .{ 0, 4 } });
    defer alloc.free(tile_points);
    const target_points = try alloc.dupe([2]f64, &.{ .{ 0, 0 }, .{ 8, 0 }, .{ 8, 4 }, .{ 0, 4 } });
    defer alloc.free(target_points);

    var tile_shapes = [_]reader.ShapeRun{
        .{
            .kind = .fill,
            .color = .{ 0xff, 0x00, 0x00, 0xff },
            .stroke_width = 1,
            .closed = true,
            .points = tile_points,
        },
    };

    const run: reader.PatternRun = .{
        .kind = .fill,
        .points = target_points,
        .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 4, .max_y = 4 },
        .pattern_x_step = 4,
        .pattern_y_step = 4,
        .tile_shape_runs = tile_shapes[0..],
    };

    var canvas: [8 * 4 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    try drawPatternRun(alloc, &canvas, 8, 4, 0, 4, run);

    const left_red = ((2 * 8) + 1) * 4;
    const right_red = ((2 * 8) + 5) * 4;
    const white_gap = ((2 * 8) + 3) * 4;
    try std.testing.expectEqual(@as(u8, 0xff), canvas[left_red + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[left_red + 1]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[right_red + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[right_red + 1]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[white_gap + 0]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[white_gap + 1]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[white_gap + 2]);
}

test "draw pattern run recolors uncolored cell content" {
    const alloc = std.testing.allocator;
    const tile_points = try alloc.dupe([2]f64, &.{ .{ 0, 0 }, .{ 2, 0 }, .{ 2, 4 }, .{ 0, 4 } });
    defer alloc.free(tile_points);
    const target_points = try alloc.dupe([2]f64, &.{ .{ 0, 0 }, .{ 4, 0 }, .{ 4, 4 }, .{ 0, 4 } });
    defer alloc.free(target_points);

    var tile_shapes = [_]reader.ShapeRun{
        .{
            .kind = .fill,
            .color = .{ 0xff, 0x00, 0x00, 0x80 },
            .stroke_width = 1,
            .closed = true,
            .points = tile_points,
        },
    };

    const run: reader.PatternRun = .{
        .kind = .fill,
        .points = target_points,
        .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 4, .max_y = 4 },
        .pattern_x_step = 4,
        .pattern_y_step = 4,
        .base_color = .{ 0x00, 0xff, 0x00, 0xff },
        .tile_shape_runs = tile_shapes[0..],
    };

    var canvas: [4 * 4 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    try drawPatternRun(alloc, &canvas, 4, 4, 0, 4, run);

    const green_px = ((2 * 4) + 1) * 4;
    try std.testing.expectEqual(@as(u8, 0x00), canvas[green_px + 0]);
    try std.testing.expect(canvas[green_px + 1] > 0x80);
}
