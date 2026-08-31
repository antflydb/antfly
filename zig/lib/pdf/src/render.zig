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

pub const PageRotation = enum(u16) {
    none = 0,
    clockwise_90 = 90,
    clockwise_180 = 180,
    clockwise_270 = 270,
};

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
    return try renderPageContentPngInBoxRotated(alloc, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, .none);
}

pub fn renderPageContentPngInBoxRotated(
    alloc: Allocator,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    rotation: PageRotation,
) ![]u8 {
    return try renderPageContentPngInBoxRotatedCancelable(alloc, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, rotation, .{});
}

pub fn renderPageContentPngInBoxRotatedCancelable(
    alloc: Allocator,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    rotation: PageRotation,
    cancellation: reader.CancellationProbe,
) ![]u8 {
    try cancellation.check();
    var raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, cancellation);
    defer alloc.free(raw.rgba);
    try cancellation.check();
    try rotateRawPageCanvasAlloc(alloc, &raw, rotation, cancellation);
    try cancellation.check();
    return try image.png.encodeRgbaWithCancellation(
        alloc,
        @intCast(raw.width),
        @intCast(raw.height),
        raw.rgba,
        .{ .context = cancellation.context, .is_cancelled_fn = cancellation.is_cancelled_fn },
    );
}

const RawPageCanvas = struct {
    rgba: []u8,
    width: usize,
    height: usize,
};

const BilevelSampleBudget = struct {
    const minimum_samples: u64 = 262_144;
    const maximum_samples: u64 = 32_000_000;
    const samples_per_canvas_pixel: u64 = 8;

    remaining_samples: u64,

    fn init(canvas_pixels: usize) BilevelSampleBudget {
        const scaled = std.math.mul(u64, canvas_pixels, samples_per_canvas_pixel) catch maximum_samples;
        return .{ .remaining_samples = @min(maximum_samples, @max(minimum_samples, scaled)) };
    }

    fn reserve(self: *BilevelSampleBudget, samples: u64) bool {
        if (samples > self.remaining_samples) return false;
        self.remaining_samples -= samples;
        return true;
    }

    fn takeUpTo(self: *BilevelSampleBudget, requested: u64) u64 {
        const granted = @min(requested, self.remaining_samples);
        self.remaining_samples -= granted;
        return granted;
    }
};

const BilevelCancellationPoller = struct {
    const work_per_check: u64 = 4096;

    cancellation: reader.CancellationProbe,
    work_since_check: u64 = 0,

    fn init(cancellation: reader.CancellationProbe) BilevelCancellationPoller {
        return .{ .cancellation = cancellation };
    }

    /// Account source-sample work across the complete image run. Keeping this
    /// state outside the per-destination-pixel sampler avoids a deadline probe
    /// (and, in production, a monotonic-clock read) for every output pixel.
    fn complete(self: *BilevelCancellationPoller, work: u64) !void {
        self.work_since_check +|= work;
        if (self.work_since_check < work_per_check) return;
        try self.cancellation.check();
        self.work_since_check %= work_per_check;
    }
};

fn rotateRawPageCanvasAlloc(alloc: Allocator, raw: *RawPageCanvas, rotation: PageRotation, cancellation: reader.CancellationProbe) !void {
    switch (rotation) {
        .none => return,
        .clockwise_180 => {
            const pixel_count = raw.width * raw.height;
            var left: usize = 0;
            var right = pixel_count - 1;
            while (left < right) : ({
                left += 1;
                right -= 1;
            }) {
                if (left & 16_383 == 0) try cancellation.check();
                const left_offset = left * 4;
                const right_offset = right * 4;
                for (0..4) |channel| {
                    std.mem.swap(u8, &raw.rgba[left_offset + channel], &raw.rgba[right_offset + channel]);
                }
            }
        },
        .clockwise_90, .clockwise_270 => {
            const rotated = try alloc.alloc(u8, raw.rgba.len);
            errdefer alloc.free(rotated);
            // Traverse the destination in row-major order so the full-page
            // write remains cache-friendly. Source reads are necessarily
            // column-strided for a quarter turn.
            for (0..raw.width) |dst_y| {
                if (dst_y & 31 == 0) try cancellation.check();
                for (0..raw.height) |dst_x| {
                    const src_x = if (rotation == .clockwise_90) dst_y else raw.width - 1 - dst_y;
                    const src_y = if (rotation == .clockwise_90) raw.height - 1 - dst_x else dst_x;
                    const src_offset = (src_y * raw.width + src_x) * 4;
                    const dst_offset = (dst_y * raw.height + dst_x) * 4;
                    @memcpy(rotated[dst_offset .. dst_offset + 4], raw.rgba[src_offset .. src_offset + 4]);
                }
            }
            alloc.free(raw.rgba);
            raw.rgba = rotated;
            std.mem.swap(usize, &raw.width, &raw.height);
        },
    }
}

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
    parent_index: ?usize = null,
    isolated: bool,
    knockout: bool,
    alpha: u8,
    blend_mode: reader.BlendMode,
    min_paint_order: usize,
    min_paint_phase: usize,
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
    group_indices: *std.AutoHashMapUnmanaged(u32, usize),
    id: u32,
    parent_id: ?u32,
    isolated: bool,
    knockout: bool,
    alpha: u8,
    blend_mode: reader.BlendMode,
    paint_order: usize,
    paint_phase: usize,
) anyerror!void {
    if (group_indices.get(id)) |idx| {
        const group = &groups.items[idx];
        group.parent_id = parent_id;
        group.isolated = isolated;
        group.knockout = knockout;
        if (group.alpha != alpha or group.blend_mode != blend_mode)
            return error.InvalidRenderGroup;
        if (paint_order < group.min_paint_order) {
            group.min_paint_order = paint_order;
            group.min_paint_phase = paint_phase;
        } else if (paint_order == group.min_paint_order) {
            group.min_paint_phase = @min(group.min_paint_phase, paint_phase);
        }
        return;
    }
    const idx = groups.items.len;
    try groups.append(alloc, .{
        .id = id,
        .parent_id = parent_id,
        .isolated = isolated,
        .knockout = knockout,
        .alpha = alpha,
        .blend_mode = blend_mode,
        .min_paint_order = paint_order,
        .min_paint_phase = paint_phase,
    });
    try group_indices.put(alloc, id, idx);
}

const RenderSchedule = std.ArrayListUnmanaged(RenderChoice);

const RenderPlan = struct {
    groups: []GroupMeta,
    schedules: []RenderSchedule,
    peak_canvas_count: usize,

    fn deinit(self: *RenderPlan, alloc: Allocator) void {
        for (self.schedules) |*schedule| schedule.deinit(alloc);
        alloc.free(self.schedules);
        alloc.free(self.groups);
        self.* = undefined;
    }
};

fn peakRenderCanvasCountAlloc(alloc: Allocator, groups: []const GroupMeta) !usize {
    const VisitState = enum { unseen, visiting, complete };
    const states = try alloc.alloc(VisitState, groups.len);
    defer alloc.free(states);
    @memset(states, .unseen);
    const counts = try alloc.alloc(usize, groups.len);
    defer alloc.free(counts);

    const Visitor = struct {
        fn visit(all_groups: []const GroupMeta, all_states: []VisitState, all_counts: []usize, index: usize) !usize {
            switch (all_states[index]) {
                .complete => return all_counts[index],
                .visiting => return error.InvalidRenderGroup,
                .unseen => {},
            }
            all_states[index] = .visiting;
            const parent_count = if (all_groups[index].parent_index) |parent|
                try visit(all_groups, all_states, all_counts, parent)
            else
                1; // The page canvas.
            const boundary_coverage_canvas: usize = if (!all_groups[index].isolated and
                (all_groups[index].alpha != 0xff or all_groups[index].blend_mode != .normal)) 1 else 0;
            const group_canvases: usize = 1 + boundary_coverage_canvas + if (all_groups[index].knockout) @as(usize, 2) else 0;
            const count = std.math.add(usize, parent_count, group_canvases) catch return error.RenderedPageTooLarge;
            all_counts[index] = count;
            all_states[index] = .complete;
            return count;
        }
    };

    var peak: usize = 1;
    for (groups, 0..) |_, index| peak = @max(peak, try Visitor.visit(groups, states, counts, index));
    return peak;
}

fn renderChoiceKindOrder(choice: RenderChoice) u8 {
    return switch (choice) {
        .text => 0,
        .image => 1,
        .shading => 2,
        .pattern => 3,
        .shape => 4,
        .group => 5,
    };
}

fn renderChoiceIndex(choice: RenderChoice) usize {
    return switch (choice) {
        inline else => |idx| idx,
    };
}

fn renderChoicePhase(
    choice: RenderChoice,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    groups: []const GroupMeta,
) usize {
    return switch (choice) {
        .text => |idx| text_runs[idx].paint_phase,
        .image => |idx| image_runs[idx].paint_phase,
        .shading => |idx| shading_runs[idx].paint_phase,
        .pattern => |idx| pattern_runs[idx].paint_phase,
        .shape => |idx| shape_runs[idx].paint_phase,
        .group => |idx| groups[idx].min_paint_phase,
    };
}

const RenderChoiceSortContext = struct {
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    groups: []const GroupMeta,

    fn lessThan(ctx: @This(), a: RenderChoice, b: RenderChoice) bool {
        const a_order = choiceOrder(a, ctx.text_runs, ctx.image_runs, ctx.shading_runs, ctx.pattern_runs, ctx.shape_runs, ctx.groups);
        const b_order = choiceOrder(b, ctx.text_runs, ctx.image_runs, ctx.shading_runs, ctx.pattern_runs, ctx.shape_runs, ctx.groups);
        if (a_order != b_order) return a_order < b_order;
        const a_phase = renderChoicePhase(a, ctx.text_runs, ctx.image_runs, ctx.shading_runs, ctx.pattern_runs, ctx.shape_runs, ctx.groups);
        const b_phase = renderChoicePhase(b, ctx.text_runs, ctx.image_runs, ctx.shading_runs, ctx.pattern_runs, ctx.shape_runs, ctx.groups);
        if (a_phase != b_phase) return a_phase < b_phase;
        const a_kind = renderChoiceKindOrder(a);
        const b_kind = renderChoiceKindOrder(b);
        if (a_kind != b_kind) return a_kind < b_kind;
        return renderChoiceIndex(a) < renderChoiceIndex(b);
    }
};

fn buildRenderPlanAlloc(
    alloc: Allocator,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
) !RenderPlan {
    var groups = std.ArrayList(GroupMeta).empty;
    errdefer groups.deinit(alloc);
    var group_indices = std.AutoHashMapUnmanaged(u32, usize).empty;
    defer group_indices.deinit(alloc);

    for (text_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, &group_indices, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.group_alpha, run.group_blend_mode, run.paint_order, run.paint_phase);
    for (image_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, &group_indices, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.group_alpha, run.group_blend_mode, run.paint_order, run.paint_phase);
    for (shading_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, &group_indices, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.group_alpha, run.group_blend_mode, run.paint_order, run.paint_phase);
    for (pattern_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, &group_indices, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.group_alpha, run.group_blend_mode, run.paint_order, run.paint_phase);
    for (shape_runs) |run| if (run.group_id) |id| try addOrUpdateGroupMeta(alloc, &groups, &group_indices, id, run.group_parent_id, run.group_isolated, run.group_knockout, run.group_alpha, run.group_blend_mode, run.paint_order, run.paint_phase);

    // A parent group may contain only nested transparency groups. Propagate
    // descendant minima so its position in its own parent remains the true
    // earliest `(paint_order, paint_phase)` in the whole subtree.
    var pass: usize = 0;
    while (pass < groups.items.len) : (pass += 1) {
        var changed = false;
        for (groups.items) |child| if (child.parent_id) |parent_id| {
            const parent_idx = group_indices.get(parent_id) orelse continue;
            const parent = &groups.items[parent_idx];
            if (child.min_paint_order < parent.min_paint_order or
                (child.min_paint_order == parent.min_paint_order and child.min_paint_phase < parent.min_paint_phase))
            {
                parent.min_paint_order = child.min_paint_order;
                parent.min_paint_phase = child.min_paint_phase;
                changed = true;
            }
        };
        if (!changed) break;
    }

    const owned_groups = try groups.toOwnedSlice(alloc);
    errdefer alloc.free(owned_groups);
    for (owned_groups) |*group| if (group.parent_id) |parent_id| {
        const parent_index = group_indices.get(parent_id) orelse continue;
        if (parent_index == group_indices.get(group.id).?) return error.InvalidRenderGroup;
        group.parent_index = parent_index;
    };
    const peak_canvas_count = try peakRenderCanvasCountAlloc(alloc, owned_groups);
    const schedules = try alloc.alloc(RenderSchedule, owned_groups.len + 1);
    errdefer alloc.free(schedules);
    for (schedules) |*schedule| schedule.* = .empty;
    var initialized_schedules: usize = schedules.len;
    errdefer for (schedules[0..initialized_schedules]) |*schedule| schedule.deinit(alloc);

    for (text_runs, 0..) |run, idx| try schedules[if (run.group_id) |id| (group_indices.get(id) orelse return error.InvalidRenderGroup) + 1 else 0].append(alloc, .{ .text = idx });
    for (image_runs, 0..) |run, idx| try schedules[if (run.group_id) |id| (group_indices.get(id) orelse return error.InvalidRenderGroup) + 1 else 0].append(alloc, .{ .image = idx });
    for (shading_runs, 0..) |run, idx| try schedules[if (run.group_id) |id| (group_indices.get(id) orelse return error.InvalidRenderGroup) + 1 else 0].append(alloc, .{ .shading = idx });
    for (pattern_runs, 0..) |run, idx| try schedules[if (run.group_id) |id| (group_indices.get(id) orelse return error.InvalidRenderGroup) + 1 else 0].append(alloc, .{ .pattern = idx });
    for (shape_runs, 0..) |run, idx| try schedules[if (run.group_id) |id| (group_indices.get(id) orelse return error.InvalidRenderGroup) + 1 else 0].append(alloc, .{ .shape = idx });
    for (owned_groups, 0..) |group, idx| {
        const parent_schedule = if (group.parent_id) |parent_id|
            if (group_indices.get(parent_id)) |parent_idx| parent_idx + 1 else 0
        else
            0;
        try schedules[parent_schedule].append(alloc, .{ .group = idx });
    }

    const sort_context = RenderChoiceSortContext{
        .text_runs = text_runs,
        .image_runs = image_runs,
        .shading_runs = shading_runs,
        .pattern_runs = pattern_runs,
        .shape_runs = shape_runs,
        .groups = owned_groups,
    };
    for (schedules) |schedule| std.mem.sort(RenderChoice, schedule.items, sort_context, RenderChoiceSortContext.lessThan);
    initialized_schedules = 0;
    return .{ .groups = owned_groups, .schedules = schedules, .peak_canvas_count = peak_canvas_count };
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
    plan: *const RenderPlan,
    group_index: usize,
    cancellation: reader.CancellationProbe,
    bilevel_sample_budget: *BilevelSampleBudget,
) anyerror!void {
    try cancellation.check();
    const meta = plan.groups[group_index];
    const child = try alloc.alloc(u8, width * height * 4);
    defer alloc.free(child);
    const nonisolated_boundary = !meta.isolated and (meta.alpha != 0xff or meta.blend_mode != .normal);
    if (meta.isolated) {
        @memset(child, 0);
    } else {
        @memcpy(child, target);
    }
    const coverage_budget_start = bilevel_sample_budget.*;
    try renderGroupChildrenAlloc(alloc, child, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, plan, group_index + 1, meta.knockout, cancellation, bilevel_sample_budget);
    if (nonisolated_boundary) {
        // A non-isolated group must see the real backdrop while its children
        // blend. Render its coverage independently, then remove the backdrop
        // contribution from the result before applying boundary alpha/blend.
        const coverage = try alloc.alloc(u8, width * height * 4);
        defer alloc.free(coverage);
        @memset(coverage, 0);
        var coverage_bilevel_budget = coverage_budget_start;
        try renderGroupChildrenAlloc(alloc, coverage, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, plan, group_index + 1, meta.knockout, cancellation, &coverage_bilevel_budget);
        try compositeNonisolatedGroupCanvasModeCancelable(target, child, coverage, meta.alpha, meta.blend_mode, cancellation);
    } else if (meta.isolated) {
        try compositeGroupCanvasModeCancelable(target, child, meta.alpha, meta.blend_mode, cancellation);
    } else {
        try copyCanvasCancelable(target, child, width, height, cancellation);
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
    plan: *const RenderPlan,
    choice: RenderChoice,
    cancellation: reader.CancellationProbe,
    bilevel_sample_budget: *BilevelSampleBudget,
) !void {
    try cancellation.check();
    switch (choice) {
        .text => |idx| try drawTextRunCancelable(canvas, width, height, 0, page_box.min_x, page_box.max_y, text_runs[idx], cancellation),
        .image => |idx| try drawImageRunCancelable(canvas, width, height, 0, page_box.min_x, page_box.max_y, image_runs[idx], cancellation, bilevel_sample_budget),
        .shading => |idx| try drawShadingRunCancelable(canvas, width, height, page_box.min_x, page_box.max_y, shading_runs[idx], cancellation),
        .pattern => |idx| try drawPatternRunCancelable(alloc, canvas, width, height, page_box.min_x, page_box.max_y, pattern_runs[idx], cancellation, bilevel_sample_budget),
        .shape => |idx| try drawShapeRunAllocCancelable(alloc, canvas, width, height, page_box.min_x, page_box.max_y, shape_runs[idx], cancellation),
        .group => |idx| try renderChildGroupAlloc(alloc, canvas, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, plan, idx, cancellation, bilevel_sample_budget),
    }
}

const PixelRect = struct {
    x0: usize,
    y0: usize,
    x1: usize,
    y1: usize,

    fn full(width: usize, height: usize) PixelRect {
        return .{ .x0 = 0, .y0 = 0, .x1 = width, .y1 = height };
    }

    fn empty(self: PixelRect) bool {
        return self.x0 >= self.x1 or self.y0 >= self.y1;
    }
};

fn boundsPixelRect(bounds: anytype, page_box: reader.PageBox, width: usize, height: usize, padding: f64) PixelRect {
    return .{
        .x0 = floorToCanvas(bounds.min_x - page_box.min_x - padding, width),
        .x1 = ceilToCanvas(bounds.max_x - page_box.min_x + padding, width),
        .y0 = floorToCanvas(page_box.max_y - bounds.max_y - padding, height),
        .y1 = ceilToCanvas(page_box.max_y - bounds.min_y + padding, height),
    };
}

fn patternRunBounds(run: reader.PatternRun) struct { min_x: f64, max_x: f64, min_y: f64, max_y: f64 } {
    if (run.points.len == 0) return .{ .min_x = 0, .max_x = 0, .min_y = 0, .max_y = 0 };
    var min_x = run.points[0][0];
    var max_x = min_x;
    var min_y = run.points[0][1];
    var max_y = min_y;
    for (run.points[1..]) |point| {
        min_x = @min(min_x, point[0]);
        max_x = @max(max_x, point[0]);
        min_y = @min(min_y, point[1]);
        max_y = @max(max_y, point[1]);
    }
    if (run.kind == .stroke) {
        const radius = run.stroke_width / 2.0;
        const padding = if (run.line_join == .miter) radius * @max(1.0, run.miter_limit) else radius;
        min_x -= padding;
        max_x += padding;
        min_y -= padding;
        max_y += padding;
    }
    return .{ .min_x = min_x, .max_x = max_x, .min_y = min_y, .max_y = max_y };
}

fn renderChoicePixelRect(
    choice: RenderChoice,
    width: usize,
    height: usize,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
) PixelRect {
    return switch (choice) {
        .text => |idx| boundsPixelRect(textRunBounds(text_runs[idx]), page_box, width, height, @max(1.0, text_runs[idx].stroke_width)),
        .image => |idx| boundsPixelRect(imageRunBounds(image_runs[idx]), page_box, width, height, 1.0),
        // Shadings and nested transparency groups can affect the whole clip;
        // retaining the full-page conservative region preserves semantics.
        .shading, .group => PixelRect.full(width, height),
        .pattern => |idx| boundsPixelRect(patternRunBounds(pattern_runs[idx]), page_box, width, height, 1.0),
        .shape => |idx| boundsPixelRect(shapeRunBounds(shape_runs[idx]), page_box, width, height, 1.0),
    };
}

fn copyCanvasRect(dst: []u8, src: []const u8, canvas_width: usize, rect: PixelRect) void {
    var y = rect.y0;
    while (y < rect.y1) : (y += 1) {
        const start = (y * canvas_width + rect.x0) * 4;
        const end = (y * canvas_width + rect.x1) * 4;
        @memcpy(dst[start..end], src[start..end]);
    }
}

fn copyCanvasRectCancelable(dst: []u8, src: []const u8, canvas_width: usize, rect: PixelRect, cancellation: reader.CancellationProbe) !void {
    var y = rect.y0;
    while (y < rect.y1) : (y += 1) {
        if ((y - rect.y0) & 31 == 0) try cancellation.check();
        const start = (y * canvas_width + rect.x0) * 4;
        const end = (y * canvas_width + rect.x1) * 4;
        @memcpy(dst[start..end], src[start..end]);
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
    plan: *const RenderPlan,
    schedule_index: usize,
    knockout: bool,
    cancellation: reader.CancellationProbe,
    bilevel_sample_budget: *BilevelSampleBudget,
) anyerror!void {
    const backdrop = if (knockout) try alloc.dupe(u8, canvas) else null;
    defer if (backdrop) |buf| alloc.free(buf);
    const scratch = if (knockout) try alloc.dupe(u8, canvas) else null;
    defer if (scratch) |buf| alloc.free(buf);

    for (plan.schedules[schedule_index].items) |choice| {
        try cancellation.check();
        if (knockout) {
            const rect = renderChoicePixelRect(choice, width, height, page_box, text_runs, image_runs, pattern_runs, shape_runs);
            if (rect.empty()) continue;
            try copyCanvasRectCancelable(scratch.?, backdrop.?, width, rect, cancellation);
            try renderChoiceAlloc(alloc, scratch.?, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, plan, choice, cancellation, bilevel_sample_budget);
            try replaceCanvasWhereChangedRectCancelable(canvas, scratch.?, backdrop.?, width, rect, cancellation);
        } else {
            try renderChoiceAlloc(alloc, canvas, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, plan, choice, cancellation, bilevel_sample_budget);
        }
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
    cancellation: reader.CancellationProbe,
) !RawPageCanvas {
    return try renderPageContentRgbaInBoxAllocWithBudget(alloc, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, cancellation, null, .opaque_white);
}

const CanvasBackground = enum { opaque_white, transparent };

fn renderPageContentRgbaInBoxAllocWithBudget(
    alloc: Allocator,
    page_box: reader.PageBox,
    text_runs: []const reader.TextRun,
    image_runs: []const reader.ImageRun,
    shading_runs: []const reader.ShadingRun,
    pattern_runs: []const reader.PatternRun,
    shape_runs: []const reader.ShapeRun,
    cancellation: reader.CancellationProbe,
    shared_bilevel_sample_budget: ?*BilevelSampleBudget,
    background: CanvasBackground,
) !RawPageCanvas {
    try cancellation.check();
    const page_w = @max(1.0, page_box.max_x - page_box.min_x);
    const page_h = @max(1.0, page_box.max_y - page_box.min_y);
    const width = ceilPositiveToUsize(page_w, 1);
    const height = ceilPositiveToUsize(page_h, 1);
    const pixel_count = std.math.mul(usize, width, height) catch return error.RenderedPageTooLarge;
    if (pixel_count > 100_000_000) return error.RenderedPageTooLarge;
    const rgba_len = std.math.mul(usize, pixel_count, 4) catch return error.RenderedPageTooLarge;

    var plan = try buildRenderPlanAlloc(alloc, text_runs, image_runs, shading_runs, pattern_runs, shape_runs);
    defer plan.deinit(alloc);
    const planned_pixels = std.math.mul(usize, pixel_count, plan.peak_canvas_count) catch return error.RenderedPageTooLarge;
    if (planned_pixels > 200_000_000) return error.RenderedPageTooLarge;

    const rgba = try alloc.alloc(u8, rgba_len);
    errdefer alloc.free(rgba);
    @memset(rgba, if (background == .opaque_white) 0xff else 0x00);
    var local_bilevel_sample_budget = BilevelSampleBudget.init(pixel_count);
    const bilevel_sample_budget = shared_bilevel_sample_budget orelse &local_bilevel_sample_budget;
    try renderGroupChildrenAlloc(alloc, rgba, width, height, page_box, text_runs, image_runs, shading_runs, pattern_runs, shape_runs, &plan, 0, false, cancellation, bilevel_sample_budget);

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
    drawTextRunCancelable(rgba, width, height, margin, min_x, max_y, run, .{}) catch unreachable;
}

fn drawTextRunCancelable(
    rgba: []u8,
    width: usize,
    height: usize,
    margin: usize,
    min_x: f64,
    max_y: f64,
    run: reader.TextRun,
    cancellation: reader.CancellationProbe,
) !void {
    var cursor: f64 = 0;
    const advance_scale = estimatedRunAdvanceScale(run);
    var view = std.unicode.Utf8View.init(run.text) catch {
        for (run.text, 0..) |ch, index| {
            if (index & 31 == 0) try cancellation.check();
            const advance = estimatedRunCodepointAdvance(run, if (ch == ' ') ' ' else 0xfffd, advance_scale);
            switch (ch) {
                ' ' => {},
                '\n', '\r' => {},
                else => try drawAffineFallbackGlyphCancelable(rgba, width, height, margin, min_x, max_y, run, ch, cursor, advance, cancellation),
            }
            cursor += advance;
        }
        return;
    };
    var iter = view.iterator();
    var codepoint_index: usize = 0;
    while (iter.nextCodepoint()) |cp| {
        if (codepoint_index & 31 == 0) try cancellation.check();
        codepoint_index += 1;
        const advance = estimatedRunCodepointAdvance(run, cp, advance_scale);
        switch (cp) {
            ' ' => {},
            '\n', '\r' => {},
            else => try drawAffineFallbackGlyphCancelable(rgba, width, height, margin, min_x, max_y, run, cp, cursor, advance, cancellation),
        }
        cursor += advance;
    }
}

fn drawImageRun(canvas: []u8, canvas_w: usize, canvas_h: usize, margin: usize, min_x: f64, max_y: f64, run: reader.ImageRun) void {
    drawImageRunCancelable(canvas, canvas_w, canvas_h, margin, min_x, max_y, run, .{}, null) catch unreachable;
}

fn drawImageRunCancelable(
    canvas: []u8,
    canvas_w: usize,
    canvas_h: usize,
    margin: usize,
    min_x: f64,
    max_y: f64,
    run: reader.ImageRun,
    cancellation: reader.CancellationProbe,
    bilevel_sample_budget: ?*BilevelSampleBudget,
) !void {
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
    // PDF interpolation is opt-in. In particular, preserve hard sample
    // boundaries for bilevel scans and line art when /Interpolate is absent
    // or false, even when the image is minified.
    const filtered = run.interpolate;
    const projected_width = @sqrt(run.a * run.a + run.b * run.b);
    const projected_height = @sqrt(run.c * run.c + run.d * run.d);
    const coverage_minify = run.ocr_coverage_minify and run.bilevel and
        (@as(f64, @floatFromInt(run.width)) > projected_width or
            @as(f64, @floatFromInt(run.height)) > projected_height);
    var bilevel_cancellation = BilevelCancellationPoller.init(cancellation);

    var py = y0;
    while (py < y1) : (py += 1) {
        if ((py - y0) & 31 == 0) try cancellation.check();
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

            const dst = (py * canvas_w + px) * 4;
            var sample: [4]u8 = undefined;
            if (coverage_minify) {
                sample = try coveragePreservingBilevelSample(run, world_x, world_y, inv_a, inv_b, inv_c, inv_d, &bilevel_cancellation, bilevel_sample_budget);
            } else if (filtered) {
                sample = bilinearImageSample(run, u, 1.0 - v);
            } else {
                const sx = @min(run.width - 1, @as(u32, @intFromFloat(@floor(u * @as(f64, @floatFromInt(run.width))))));
                const sy = @min(run.height - 1, @as(u32, @intFromFloat(@floor((1.0 - v) * @as(f64, @floatFromInt(run.height))))));
                const src = (@as(usize, sy) * @as(usize, run.width) + @as(usize, sx)) * 4;
                sample = .{ run.rgba[src], run.rgba[src + 1], run.rgba[src + 2], run.rgba[src + 3] };
            }
            if (run.stencil_color) |color| {
                sample[0] = color[0];
                sample[1] = color[1];
                sample[2] = color[2];
                sample[3] = @intCast((@as(u16, sample[3]) * @as(u16, color[3]) + 127) / 255);
            }
            if (run.opacity_mask_rgba != null) {
                const mask_alpha = imageRunOpacityMaskSample(run, u, 1.0 - v, run.opacity_mask_interpolate);
                sample[3] = @intCast((@as(u16, sample[3]) * @as(u16, mask_alpha) + 127) / 255);
            }
            sample[3] = @intCast((@as(u16, sample[3]) * @as(u16, run.alpha) + 127) / 255);
            blendPixelMode(canvas, dst, sample, run.blend_mode);
        }
    }
}

fn imageRunOpacityMaskSample(run: reader.ImageRun, u: f64, v: f64, filtered: bool) u8 {
    const rgba = run.opacity_mask_rgba orelse return 0xff;
    const width = run.opacity_mask_width;
    const height = run.opacity_mask_height;
    if (width == 0 or height == 0 or rgba.len != @as(usize, width) * @as(usize, height) * 4) return 0;
    if (!filtered) {
        const sx = @min(width - 1, @as(u32, @intFromFloat(@floor(u * @as(f64, @floatFromInt(width))))));
        const sy = @min(height - 1, @as(u32, @intFromFloat(@floor(v * @as(f64, @floatFromInt(height))))));
        return softMaskPixel(rgba, (@as(usize, sy) * width + sx) * 4, run.opacity_mask_luminosity);
    }

    const width_f: f64 = @floatFromInt(width);
    const height_f: f64 = @floatFromInt(height);
    const x = std.math.clamp(u * width_f - 0.5, 0.0, @max(0.0, width_f - 1.0));
    const y = std.math.clamp(v * height_f - 0.5, 0.0, @max(0.0, height_f - 1.0));
    const x0: u32 = @intFromFloat(@floor(x));
    const y0: u32 = @intFromFloat(@floor(y));
    const x1 = @min(width - 1, x0 + 1);
    const y1 = @min(height - 1, y0 + 1);
    const tx = x - @as(f64, @floatFromInt(x0));
    const ty = y - @as(f64, @floatFromInt(y0));
    const weights = [4]f64{ (1.0 - tx) * (1.0 - ty), tx * (1.0 - ty), (1.0 - tx) * ty, tx * ty };
    const indices = [4]usize{
        (@as(usize, y0) * width + x0) * 4,
        (@as(usize, y0) * width + x1) * 4,
        (@as(usize, y1) * width + x0) * 4,
        (@as(usize, y1) * width + x1) * 4,
    };
    var value: f64 = 0;
    for (weights, indices) |weight, index|
        value += weight * @as(f64, @floatFromInt(softMaskPixel(rgba, index, run.opacity_mask_luminosity)));
    return @intFromFloat(@round(std.math.clamp(value, 0.0, 255.0)));
}

fn softMaskPixel(rgba: []const u8, index: usize, luminosity: bool) u8 {
    const alpha: u32 = rgba[index + 3];
    if (!luminosity) return @intCast(alpha);
    const luminance = (@as(u32, rgba[index]) * 77 + @as(u32, rgba[index + 1]) * 150 + @as(u32, rgba[index + 2]) * 29 + 128) / 256;
    return @intCast((luminance * alpha + 127) / 255);
}

test "image-backed luminosity soft mask samples normalized coverage" {
    var image_rgba = [_]u8{ 0, 0, 0, 255 } ** 2;
    var mask = [_]u8{ 0, 0, 0, 255, 255, 255, 255, 255 };
    const run = reader.ImageRun{
        .rgba = &image_rgba,
        .width = 2,
        .height = 1,
        .opacity_mask_rgba = &mask,
        .opacity_mask_width = 2,
        .opacity_mask_height = 1,
        .opacity_mask_luminosity = true,
        .a = 2,
        .b = 0,
        .c = 0,
        .d = 1,
        .e = 0,
        .f = 0,
        .x = 0,
        .y = 0,
        .draw_width = 2,
        .draw_height = 1,
    };
    try std.testing.expectEqual(@as(u8, 0), imageRunOpacityMaskSample(run, 0.25, 0.5, false));
    try std.testing.expectEqual(@as(u8, 255), imageRunOpacityMaskSample(run, 0.75, 0.5, false));
    const midpoint = imageRunOpacityMaskSample(run, 0.5, 0.5, true);
    try std.testing.expect(midpoint >= 127 and midpoint <= 129);
}

const SourceFootprint = struct {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,

    fn area(self: SourceFootprint) f64 {
        return @max(0.0, self.max_x - self.min_x) * @max(0.0, self.max_y - self.min_y);
    }
};

fn imageSourceFootprint(run: reader.ImageRun, world_x: f64, world_y: f64, inv_a: f64, inv_b: f64, inv_c: f64, inv_d: f64) ?SourceFootprint {
    var footprint = SourceFootprint{
        .min_x = std.math.inf(f64),
        .max_x = -std.math.inf(f64),
        .min_y = std.math.inf(f64),
        .max_y = -std.math.inf(f64),
    };
    const corners = [_][2]f64{
        .{ -0.5, -0.5 },
        .{ 0.5, -0.5 },
        .{ -0.5, 0.5 },
        .{ 0.5, 0.5 },
    };
    for (corners) |corner| {
        const dx = world_x + corner[0] - run.e;
        const dy = world_y + corner[1] - run.f;
        const u = inv_a * dx + inv_c * dy;
        const v = inv_b * dx + inv_d * dy;
        if (!finite(u) or !finite(v)) return null;
        const source_x = u * @as(f64, @floatFromInt(run.width));
        const source_y = (1.0 - v) * @as(f64, @floatFromInt(run.height));
        footprint.min_x = @min(footprint.min_x, source_x);
        footprint.max_x = @max(footprint.max_x, source_x);
        footprint.min_y = @min(footprint.min_y, source_y);
        footprint.max_y = @max(footprint.max_y, source_y);
    }
    return if (footprint.area() > 0) footprint else null;
}

fn channelByte(value: f64) u8 {
    return @intFromFloat(@round(std.math.clamp(value, 0.0, 255.0)));
}

/// Exact premultiplied box integration for axis-aligned and quarter-turned
/// image matrices. The source rectangles of adjacent destination pixels form
/// a partition during ordinary minification, so total work remains linear in
/// the source image instead of growing with the reduction ratio.
fn boxFilteredBilevelSample(run: reader.ImageRun, footprint: SourceFootprint, cancellation: *BilevelCancellationPoller) ![4]u8 {
    const total_area = footprint.area();
    if (total_area <= 0 or !finite(total_area)) return .{ 0, 0, 0, 0 };
    const width_f: f64 = @floatFromInt(run.width);
    const height_f: f64 = @floatFromInt(run.height);
    const min_x = std.math.clamp(footprint.min_x, 0.0, width_f);
    const max_x = std.math.clamp(footprint.max_x, 0.0, width_f);
    const min_y = std.math.clamp(footprint.min_y, 0.0, height_f);
    const max_y = std.math.clamp(footprint.max_y, 0.0, height_f);
    if (min_x >= max_x or min_y >= max_y) return .{ 0, 0, 0, 0 };

    const x0: usize = @intFromFloat(@floor(min_x));
    const x1: usize = @intFromFloat(@ceil(max_x));
    const y0: usize = @intFromFloat(@floor(min_y));
    const y1: usize = @intFromFloat(@ceil(max_y));
    var alpha_area: f64 = 0;
    var premultiplied_area: [3]f64 = .{ 0, 0, 0 };
    var sy = y0;
    while (sy < y1) : (sy += 1) {
        const y_weight = @max(0.0, @min(max_y, @as(f64, @floatFromInt(sy + 1))) - @max(min_y, @as(f64, @floatFromInt(sy))));
        var sx = x0;
        while (sx < x1) : (sx += 1) {
            try cancellation.complete(1);
            const x_weight = @max(0.0, @min(max_x, @as(f64, @floatFromInt(sx + 1))) - @max(min_x, @as(f64, @floatFromInt(sx))));
            const weight = x_weight * y_weight;
            if (weight <= 0) continue;
            const src = (sy * @as(usize, run.width) + sx) * 4;
            const alpha: f64 = @floatFromInt(run.rgba[src + 3]);
            alpha_area += alpha * weight;
            for (0..3) |channel| premultiplied_area[channel] += @as(f64, @floatFromInt(run.rgba[src + channel])) * alpha * weight;
        }
    }
    const averaged_alpha = alpha_area / total_area;
    var result: [4]u8 = .{ 0, 0, 0, channelByte(averaged_alpha) };
    if (alpha_area > 0) {
        for (0..3) |channel| result[channel] = channelByte(premultiplied_area[channel] / alpha_area);
    }
    return result;
}

/// Bounded adaptive fallback for genuinely sheared/rotated footprints. Sample
/// density follows the source footprint up to 16 strata per destination axis,
/// avoiding the fixed 4x4 alias pattern without permitting adversarial affine
/// transforms to multiply work without limit.
fn adaptiveAffineBilevelSample(run: reader.ImageRun, world_x: f64, world_y: f64, inv_a: f64, inv_b: f64, inv_c: f64, inv_d: f64, footprint: SourceFootprint, max_samples: u64, cancellation: *BilevelCancellationPoller) ![4]u8 {
    const desired_x: usize = @intFromFloat(@min(16.0, @max(1.0, @ceil(footprint.max_x - footprint.min_x))));
    const desired_y: usize = @intFromFloat(@min(16.0, @max(1.0, @ceil(footprint.max_y - footprint.min_y))));
    const sample_limit: usize = @intCast(@max(1, @min(max_samples, desired_x * desired_y)));
    var samples_x: usize = 1;
    var samples_y: usize = 1;
    // Grow a balanced grid without exceeding the page-wide probe grant.
    // Endpoint-inclusive strata conservatively retain rules on footprint
    // boundaries when exact area integration is no longer affordable.
    while (samples_x * samples_y < sample_limit) {
        if (samples_x < desired_x and (samples_x <= samples_y or samples_y == desired_y)) {
            if ((samples_x + 1) * samples_y > sample_limit) break;
            samples_x += 1;
        } else if (samples_y < desired_y) {
            if (samples_x * (samples_y + 1) > sample_limit) break;
            samples_y += 1;
        } else break;
    }
    const sample_count = samples_x * samples_y;
    const sample_count_u32: u32 = @intCast(sample_count);
    var alpha_sum: u32 = 0;
    var premultiplied_sum: [3]u64 = .{ 0, 0, 0 };
    for (0..samples_y) |sample_y_index| for (0..samples_x) |sample_x_index| {
        try cancellation.complete(1);
        const ox = if (samples_x == 1) 0.5 else @as(f64, @floatFromInt(sample_x_index)) / @as(f64, @floatFromInt(samples_x - 1));
        const oy = if (samples_y == 1) 0.5 else @as(f64, @floatFromInt(sample_y_index)) / @as(f64, @floatFromInt(samples_y - 1));
        const sample_world_x = world_x + ox - 0.5;
        const sample_world_y = world_y - oy + 0.5;
        const dx = sample_world_x - run.e;
        const dy = sample_world_y - run.f;
        const u = inv_a * dx + inv_c * dy;
        const v = inv_b * dx + inv_d * dy;
        if (!finite(u) or !finite(v) or u < 0 or u > 1 or v < 0 or v > 1) continue;
        const sx = @min(run.width - 1, @as(u32, @intFromFloat(@floor(u * @as(f64, @floatFromInt(run.width))))));
        const sy = @min(run.height - 1, @as(u32, @intFromFloat(@floor((1.0 - v) * @as(f64, @floatFromInt(run.height))))));
        const src = (@as(usize, sy) * @as(usize, run.width) + @as(usize, sx)) * 4;
        const alpha = run.rgba[src + 3];
        alpha_sum += alpha;
        for (0..3) |channel| premultiplied_sum[channel] += @as(u64, run.rgba[src + channel]) * alpha;
    };
    const averaged_alpha: u8 = @intCast((alpha_sum + sample_count_u32 / 2) / sample_count_u32);
    var result: [4]u8 = .{ 0, 0, 0, averaged_alpha };
    if (alpha_sum != 0) for (0..3) |channel| {
        result[channel] = @intCast((premultiplied_sum[channel] + alpha_sum / 2) / alpha_sum);
    };
    return result;
}

fn boxFilterSourceSampleCount(run: reader.ImageRun, footprint: SourceFootprint) u64 {
    const width_f: f64 = @floatFromInt(run.width);
    const height_f: f64 = @floatFromInt(run.height);
    const min_x = std.math.clamp(footprint.min_x, 0.0, width_f);
    const max_x = std.math.clamp(footprint.max_x, 0.0, width_f);
    const min_y = std.math.clamp(footprint.min_y, 0.0, height_f);
    const max_y = std.math.clamp(footprint.max_y, 0.0, height_f);
    if (min_x >= max_x or min_y >= max_y) return 0;
    const columns = @as(usize, @intFromFloat(@ceil(max_x))) - @as(usize, @intFromFloat(@floor(min_x)));
    const rows = @as(usize, @intFromFloat(@ceil(max_y))) - @as(usize, @intFromFloat(@floor(min_y)));
    return std.math.mul(u64, columns, rows) catch std.math.maxInt(u64);
}

fn coveragePreservingBilevelSample(
    run: reader.ImageRun,
    world_x: f64,
    world_y: f64,
    inv_a: f64,
    inv_b: f64,
    inv_c: f64,
    inv_d: f64,
    cancellation: *BilevelCancellationPoller,
    bilevel_sample_budget: ?*BilevelSampleBudget,
) ![4]u8 {
    const footprint = imageSourceFootprint(run, world_x, world_y, inv_a, inv_b, inv_c, inv_d) orelse return .{ 0, 0, 0, 0 };
    const epsilon = 0.000000001;
    const orthogonal = (@abs(inv_b) <= epsilon and @abs(inv_c) <= epsilon) or
        (@abs(inv_a) <= epsilon and @abs(inv_d) <= epsilon);
    if (orthogonal) {
        const source_samples = boxFilterSourceSampleCount(run, footprint);
        if (bilevel_sample_budget == null or bilevel_sample_budget.?.reserve(source_samples))
            return try boxFilteredBilevelSample(run, footprint, cancellation);
    }
    if (run.bilevel_fallback) |fallback| {
        // The summary represents average coverage over the source image, not
        // over this destination pixel. Use it only when the transformed image
        // is wholly inside the pixel, and scale its alpha by the transformed
        // image area. Otherwise a tiny mask could become a fully opaque blob.
        if (imageContainedByDestinationPixel(run, world_x, world_y)) {
            const image_area = std.math.clamp(@abs(run.a * run.d - run.b * run.c), 0.0, 1.0);
            // This is exact for the full-image contribution: the cached color
            // is alpha-weighted over the source and the determinant is its
            // area in destination pixels. Returning it directly also avoids a
            // single adaptive probe turning a tiny image fully opaque.
            var covered = fallback;
            covered[3] = channelByte(@as(f64, @floatFromInt(fallback[3])) * image_area);
            return covered;
        }
    }
    const desired_adaptive_samples: u64 = @as(u64, @intFromFloat(@min(16.0, @max(1.0, @ceil(footprint.max_x - footprint.min_x))))) *
        @as(u64, @intFromFloat(@min(16.0, @max(1.0, @ceil(footprint.max_y - footprint.min_y)))));
    const granted_samples = if (bilevel_sample_budget) |budget|
        budget.takeUpTo(desired_adaptive_samples)
    else
        desired_adaptive_samples;
    // Every adaptive probe is charged to the shared page budget. Once it is
    // empty, one deterministic center probe is the O(1) per-destination-pixel
    // floor, so repeated XObjects cannot recover an unbounded source scan.
    return try adaptiveAffineBilevelSample(run, world_x, world_y, inv_a, inv_b, inv_c, inv_d, footprint, granted_samples, cancellation);
}

fn imageContainedByDestinationPixel(run: reader.ImageRun, world_x: f64, world_y: f64) bool {
    const min_x = world_x - 0.5;
    const max_x = world_x + 0.5;
    const min_y = world_y - 0.5;
    const max_y = world_y + 0.5;
    const corners = [_][2]f64{
        .{ 0, 0 },
        .{ 1, 0 },
        .{ 0, 1 },
        .{ 1, 1 },
    };
    for (corners) |corner| {
        const x = run.e + run.a * corner[0] + run.c * corner[1];
        const y = run.f + run.b * corner[0] + run.d * corner[1];
        if (!finite(x) or !finite(y) or x < min_x or x > max_x or y < min_y or y > max_y) return false;
    }
    return true;
}

fn bilinearImageSample(run: reader.ImageRun, u: f64, v: f64) [4]u8 {
    const width_f: f64 = @floatFromInt(run.width);
    const height_f: f64 = @floatFromInt(run.height);
    const x = std.math.clamp(u * width_f - 0.5, 0.0, @max(0.0, width_f - 1.0));
    const y = std.math.clamp(v * height_f - 0.5, 0.0, @max(0.0, height_f - 1.0));
    const x0: u32 = @intFromFloat(@floor(x));
    const y0: u32 = @intFromFloat(@floor(y));
    const x1 = @min(run.width - 1, x0 + 1);
    const y1 = @min(run.height - 1, y0 + 1);
    const tx = x - @as(f64, @floatFromInt(x0));
    const ty = y - @as(f64, @floatFromInt(y0));
    const weights = [4]f64{ (1.0 - tx) * (1.0 - ty), tx * (1.0 - ty), (1.0 - tx) * ty, tx * ty };
    const indices = [4]usize{
        (@as(usize, y0) * @as(usize, run.width) + @as(usize, x0)) * 4,
        (@as(usize, y0) * @as(usize, run.width) + @as(usize, x1)) * 4,
        (@as(usize, y1) * @as(usize, run.width) + @as(usize, x0)) * 4,
        (@as(usize, y1) * @as(usize, run.width) + @as(usize, x1)) * 4,
    };
    var alpha: f64 = 0;
    for (weights, indices) |weight, index| alpha += weight * @as(f64, @floatFromInt(run.rgba[index + 3]));
    var out: [4]u8 = .{ 0, 0, 0, @intFromFloat(@round(std.math.clamp(alpha, 0.0, 255.0))) };
    if (alpha > 0.000001) {
        for (0..3) |channel| {
            var premultiplied: f64 = 0;
            for (weights, indices) |weight, index| {
                premultiplied += weight * @as(f64, @floatFromInt(run.rgba[index + channel])) * @as(f64, @floatFromInt(run.rgba[index + 3]));
            }
            out[channel] = @intFromFloat(@round(std.math.clamp(premultiplied / alpha, 0.0, 255.0)));
        }
    }
    return out;
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

const FillIntersection = struct {
    x: f64,
    winding_delta: i8,
};

/// Font outlines are antialiased fills with potentially thousands of edges.
/// Testing every edge at every pixel is quadratic in glyph detail and made
/// valid traced/CJK fonts indistinguishable from hostile input. Build the
/// crossings once per subpixel scanline, then fill spans in raster order.
fn drawShapeRunAlloc(
    alloc: Allocator,
    canvas: []u8,
    canvas_w: usize,
    canvas_h: usize,
    min_x: f64,
    max_y: f64,
    run: reader.ShapeRun,
) !void {
    return try drawShapeRunAllocCancelable(alloc, canvas, canvas_w, canvas_h, min_x, max_y, run, .{});
}

fn drawShapeRunAllocCancelable(
    alloc: Allocator,
    canvas: []u8,
    canvas_w: usize,
    canvas_h: usize,
    min_x: f64,
    max_y: f64,
    run: reader.ShapeRun,
    cancellation: reader.CancellationProbe,
) !void {
    const scanline_fill = run.kind == .fill and run.antialias and run.closed and
        run.clip_points == null and run.points.len >= 3;
    if (!scanline_fill) {
        try drawShapeRunCancelable(canvas, canvas_w, canvas_h, min_x, max_y, run, cancellation);
        return;
    }

    const bounds = shapeRunBounds(run);
    const x0 = floorToCanvas(bounds.min_x - min_x, canvas_w);
    const x1 = ceilToCanvas(bounds.max_x - min_x, canvas_w);
    const y0 = floorToCanvas(max_y - bounds.max_y, canvas_h);
    const y1 = ceilToCanvas(max_y - bounds.min_y, canvas_h);
    if (x0 >= x1 or y0 >= y1) return;

    const coverage = try alloc.alloc(u8, x1 - x0);
    defer alloc.free(coverage);
    var intersections = std.ArrayList(FillIntersection).empty;
    defer intersections.deinit(alloc);
    try intersections.ensureTotalCapacity(alloc, run.points.len);

    const implicit_starts = [_]usize{0};
    const starts: []const usize = run.subpath_starts orelse &implicit_starts;
    const y_offsets = [_]f64{ 0.25, 0.75 };
    const x_offsets = [_]f64{ 0.25, 0.75 };
    var py = y0;
    while (py < y1) : (py += 1) {
        if ((py - y0) & 31 == 0) try cancellation.check();
        @memset(coverage, 0);
        for (y_offsets) |oy| {
            intersections.clearRetainingCapacity();
            const sample_y = max_y - (@as(f64, @floatFromInt(py)) + oy);
            if (run.clip_box) |clip| if (sample_y < clip.min_y or sample_y > clip.max_y) continue;
            for (starts, 0..) |start, subpath_index| {
                const end = if (subpath_index + 1 < starts.len) starts[subpath_index + 1] else run.points.len;
                if (end <= start + 2) continue;
                var previous = run.points[end - 1];
                for (run.points[start..end]) |point| {
                    const low_y = @min(previous[1], point[1]);
                    const high_y = @max(previous[1], point[1]);
                    if (sample_y >= low_y and sample_y < high_y and high_y > low_y) {
                        const t = (sample_y - previous[1]) / (point[1] - previous[1]);
                        intersections.appendAssumeCapacity(.{
                            .x = previous[0] + t * (point[0] - previous[0]),
                            .winding_delta = if (point[1] > previous[1]) 1 else -1,
                        });
                    }
                    previous = point;
                }
            }
            if (intersections.items.len < 2) continue;
            std.mem.sort(FillIntersection, intersections.items, {}, struct {
                fn lessThan(_: void, left: FillIntersection, right: FillIntersection) bool {
                    return left.x < right.x;
                }
            }.lessThan);

            var winding: i32 = 0;
            var parity = false;
            var crossing: usize = 0;
            while (crossing < intersections.items.len) {
                const left_x = intersections.items[crossing].x;
                var next = crossing;
                var delta: i32 = 0;
                while (next < intersections.items.len and @abs(intersections.items[next].x - left_x) <= 0.000000001) : (next += 1) {
                    delta += intersections.items[next].winding_delta;
                }
                if (run.fill_rule == .even_odd) parity = parity != ((next - crossing) % 2 == 1) else winding += delta;
                if (next >= intersections.items.len) break;
                const right_x = intersections.items[next].x;
                const inside = if (run.fill_rule == .even_odd) parity else winding != 0;
                if (inside and right_x > left_x) {
                    const px_start = @max(x0, floorToCanvas(left_x - min_x, canvas_w));
                    const px_end = @min(x1, ceilToCanvas(right_x - min_x, canvas_w));
                    var px = px_start;
                    while (px < px_end) : (px += 1) for (x_offsets) |ox| {
                        const sample_x = min_x + @as(f64, @floatFromInt(px)) + ox;
                        const inside_clip = if (run.clip_box) |clip| sample_x >= clip.min_x and sample_x <= clip.max_x else true;
                        if (inside_clip and sample_x >= left_x and sample_x < right_x) coverage[px - x0] += 1;
                    };
                }
                crossing = next;
            }
        }

        for (coverage, 0..) |sample_coverage, local_x| {
            if (sample_coverage == 0) continue;
            var color = run.color;
            color[3] = @intCast((@as(u16, color[3]) * sample_coverage + 2) / 4);
            blendPixelMode(canvas, (py * canvas_w + x0 + local_x) * 4, color, run.blend_mode);
        }
    }
}

fn drawShapeRun(canvas: []u8, canvas_w: usize, canvas_h: usize, min_x: f64, max_y: f64, run: reader.ShapeRun) void {
    drawShapeRunCancelable(canvas, canvas_w, canvas_h, min_x, max_y, run, .{}) catch unreachable;
}

fn drawShapeRunCancelable(canvas: []u8, canvas_w: usize, canvas_h: usize, min_x: f64, max_y: f64, run: reader.ShapeRun, cancellation: reader.CancellationProbe) !void {
    const bounds = shapeRunBounds(run);
    const x0 = floorToCanvas(bounds.min_x - min_x, canvas_w);
    const x1 = ceilToCanvas(bounds.max_x - min_x, canvas_w);
    const y0 = floorToCanvas(max_y - bounds.max_y, canvas_h);
    const y1 = ceilToCanvas(max_y - bounds.min_y, canvas_h);
    const has_clip = run.clip_box != null or run.clip_points != null;

    var py = y0;
    while (py < y1) : (py += 1) {
        if ((py - y0) & 31 == 0) try cancellation.check();
        var px = x0;
        while (px < x1) : (px += 1) {
            const world_x = min_x + (@as(f64, @floatFromInt(px)) + 0.5);
            const world_y = max_y - (@as(f64, @floatFromInt(py)) + 0.5);
            if (has_clip and !run.antialias and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
            const dst = (py * canvas_w + px) * 4;
            if (run.antialias) {
                const coverage = shapeCoverage4x(px, py, min_x, max_y, run);
                if (coverage > 0) {
                    var color = run.color;
                    color[3] = @intCast((@as(u16, color[3]) * coverage + 2) / 4);
                    blendPixelMode(canvas, dst, color, run.blend_mode);
                }
                continue;
            }
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

fn shapeCoverage4x(px: usize, py: usize, min_x: f64, max_y: f64, run: reader.ShapeRun) u16 {
    const offsets = [2]f64{ 0.25, 0.75 };
    var coverage: u16 = 0;
    for (offsets) |oy| {
        for (offsets) |ox| {
            const x = min_x + @as(f64, @floatFromInt(px)) + ox;
            const y = max_y - (@as(f64, @floatFromInt(py)) + oy);
            if (run.clip_box != null or run.clip_points != null) {
                if (!pointPassesClip(x, y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
            }
            const inside = if (run.kind == .fill) pointInShape(x, y, run) else pointInStrokeShape(x, y, run);
            if (inside) coverage += 1;
        }
    }
    return coverage;
}

fn drawShadingRun(canvas: []u8, canvas_w: usize, canvas_h: usize, min_x: f64, max_y: f64, run: reader.ShadingRun) void {
    drawShadingRunCancelable(canvas, canvas_w, canvas_h, min_x, max_y, run, .{}) catch unreachable;
}

fn drawShadingRunCancelable(canvas: []u8, canvas_w: usize, canvas_h: usize, min_x: f64, max_y: f64, run: reader.ShadingRun, cancellation: reader.CancellationProbe) !void {
    const has_clip = run.clip_box != null or run.clip_points != null;
    var py: usize = 0;
    while (py < canvas_h) : (py += 1) {
        if (py & 31 == 0) try cancellation.check();
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
            const color = shadingColorAt(run, t);
            blendPixelMode(canvas, (py * canvas_w + px) * 4, color, run.blend_mode);
        }
    }
}

fn shadingColorAt(run: reader.ShadingRun, t: f64) [4]u8 {
    if (run.color_sample_count < 2) return lerpColor(run.c0, run.c1, t);
    const count: usize = run.color_sample_count;
    const position = std.math.clamp(t, 0.0, 1.0);
    var lower: usize = 0;
    // Advance across equal-position samples so an exact stitching boundary
    // selects its right-hand function, while interpolation approaching it
    // terminates at the left-hand sample.
    while (lower + 1 < count and run.color_sample_positions[lower + 1] <= position) : (lower += 1) {}
    if (lower + 1 >= count) return run.color_samples[count - 1];
    const upper = lower + 1;
    const lower_position = run.color_sample_positions[lower];
    const upper_position = run.color_sample_positions[upper];
    const fraction = if (upper_position > lower_position)
        (position - lower_position) / (upper_position - lower_position)
    else
        0;
    return lerpColor(run.color_samples[lower], run.color_samples[upper], fraction);
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
    return try drawPatternRunCancelable(alloc, canvas, canvas_w, canvas_h, min_x, max_y, run, .{}, null);
}

fn patternTargetShape(run: reader.PatternRun) reader.ShapeRun {
    return .{
        .kind = if (run.kind == .fill) .fill else .stroke,
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
        .subpath_starts = run.subpath_starts,
        .subpath_closed = run.subpath_closed,
    };
}

const ImageAlphaSampler = struct {
    run: reader.ImageRun,
    inv_a: f64,
    inv_b: f64,
    inv_c: f64,
    inv_d: f64,
    coverage_minify: bool,
    cancellation: BilevelCancellationPoller,

    fn init(run: reader.ImageRun, cancellation: reader.CancellationProbe) ?ImageAlphaSampler {
        const det = run.a * run.d - run.b * run.c;
        if (@abs(det) < 0.000001) return null;
        const projected_width = @sqrt(run.a * run.a + run.b * run.b);
        const projected_height = @sqrt(run.c * run.c + run.d * run.d);
        return .{
            .run = run,
            .inv_a = run.d / det,
            .inv_b = -run.b / det,
            .inv_c = -run.c / det,
            .inv_d = run.a / det,
            .coverage_minify = run.ocr_coverage_minify and run.bilevel and
                (@as(f64, @floatFromInt(run.width)) > projected_width or
                    @as(f64, @floatFromInt(run.height)) > projected_height),
            .cancellation = BilevelCancellationPoller.init(cancellation),
        };
    }

    fn alphaAt(self: *ImageAlphaSampler, world_x: f64, world_y: f64, bilevel_sample_budget: ?*BilevelSampleBudget) !u8 {
        const dx = world_x - self.run.e;
        const dy = world_y - self.run.f;
        const u = self.inv_a * dx + self.inv_c * dy;
        const v = self.inv_b * dx + self.inv_d * dy;
        if (!finite(u) or !finite(v) or u < 0 or u > 1 or v < 0 or v > 1) return 0;
        if (self.coverage_minify) {
            const sample = try coveragePreservingBilevelSample(
                self.run,
                world_x,
                world_y,
                self.inv_a,
                self.inv_b,
                self.inv_c,
                self.inv_d,
                &self.cancellation,
                bilevel_sample_budget,
            );
            return sample[3];
        }
        if (self.run.interpolate) return bilinearImageSample(self.run, u, 1.0 - v)[3];
        const sx = @min(self.run.width - 1, @as(u32, @intFromFloat(@floor(u * @as(f64, @floatFromInt(self.run.width))))));
        const sy = @min(self.run.height - 1, @as(u32, @intFromFloat(@floor((1.0 - v) * @as(f64, @floatFromInt(self.run.height))))));
        return self.run.rgba[(@as(usize, sy) * @as(usize, self.run.width) + @as(usize, sx)) * 4 + 3];
    }
};

fn patternTargetAlpha(
    run: reader.PatternRun,
    mask_sampler: *?ImageAlphaSampler,
    world_x: f64,
    world_y: f64,
    bilevel_sample_budget: ?*BilevelSampleBudget,
) !u8 {
    if (mask_sampler.*) |*sampler| return try sampler.alphaAt(world_x, world_y, bilevel_sample_budget);
    const shape = patternTargetShape(run);
    const hit = if (run.kind == .fill)
        pointInShape(world_x, world_y, shape)
    else
        pointInStrokeShape(world_x, world_y, shape);
    return if (hit) 0xff else 0;
}

fn drawPatternRunCancelable(
    alloc: Allocator,
    canvas: []u8,
    canvas_w: usize,
    canvas_h: usize,
    min_x: f64,
    max_y: f64,
    run: reader.PatternRun,
    cancellation: reader.CancellationProbe,
    bilevel_sample_budget: ?*BilevelSampleBudget,
) anyerror!void {
    var mask_sampler = if (run.stencil_mask) |mask| ImageAlphaSampler.init(mask, cancellation) else null;
    if (run.stencil_mask != null and mask_sampler == null) return;
    const bounds: reader.PageBox = if (run.stencil_mask) |mask| blk: {
        const value = imageRunBounds(mask);
        break :blk .{ .min_x = value.min_x, .min_y = value.min_y, .max_x = value.max_x, .max_y = value.max_y };
    } else blk: {
        const value = shapeRunBounds(patternTargetShape(run));
        break :blk .{ .min_x = value.min_x, .min_y = value.min_y, .max_x = value.max_x, .max_y = value.max_y };
    };
    const x0 = floorToCanvas(bounds.min_x - min_x, canvas_w);
    const x1 = ceilToCanvas(bounds.max_x - min_x, canvas_w);
    const y0 = floorToCanvas(max_y - bounds.max_y, canvas_h);
    const y1 = ceilToCanvas(max_y - bounds.min_y, canvas_h);
    const has_clip = run.clip_box != null or run.clip_points != null;

    if (run.mode == .shading) {
        const shading = run.shading orelse return;
        var py: usize = y0;
        while (py < y1) : (py += 1) {
            if ((py - y0) & 31 == 0) try cancellation.check();
            var px: usize = x0;
            while (px < x1) : (px += 1) {
                const world_x = min_x + (@as(f64, @floatFromInt(px)) + 0.5);
                const world_y = max_y - (@as(f64, @floatFromInt(py)) + 0.5);
                if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
                const target_alpha = try patternTargetAlpha(run, &mask_sampler, world_x, world_y, bilevel_sample_budget);
                if (target_alpha == 0) continue;
                const t_opt = switch (shading.kind) {
                    .axial => axialShadingT(world_x, world_y, shading),
                    .radial => radialShadingT(world_x, world_y, shading),
                };
                const t = t_opt orelse continue;
                var color = lerpColor(shading.c0, shading.c1, t);
                color[3] = @intCast((@as(u16, color[3]) * @as(u16, target_alpha) + 127) / 255);
                blendPixelMode(canvas, (py * canvas_w + px) * 4, color, run.blend_mode);
            }
        }
        return;
    }

    const tile = try renderPatternTileCanvasAlloc(alloc, run, cancellation, bilevel_sample_budget);
    defer alloc.free(tile.rgba);
    const det = run.pattern_matrix.a * run.pattern_matrix.d - run.pattern_matrix.b * run.pattern_matrix.c;
    if (@abs(det) < 0.000001) return;
    const inv_a = run.pattern_matrix.d / det;
    const inv_b = -run.pattern_matrix.b / det;
    const inv_c = -run.pattern_matrix.c / det;
    const inv_d = run.pattern_matrix.a / det;

    var py = y0;
    while (py < y1) : (py += 1) {
        if ((py - y0) & 31 == 0) try cancellation.check();
        var px = x0;
        while (px < x1) : (px += 1) {
            const world_x = min_x + (@as(f64, @floatFromInt(px)) + 0.5);
            const world_y = max_y - (@as(f64, @floatFromInt(py)) + 0.5);
            if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;
            const target_alpha = try patternTargetAlpha(run, &mask_sampler, world_x, world_y, bilevel_sample_budget);
            if (target_alpha == 0) continue;

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
            color[3] = @intCast((@as(u16, color[3]) * @as(u16, target_alpha) + 127) / 255);
            blendPixelMode(canvas, (py * canvas_w + px) * 4, color, run.blend_mode);
        }
    }
}

const RawTile = struct {
    rgba: []u8,
    width: usize,
    height: usize,
};

fn renderPatternTileCanvasAlloc(
    alloc: Allocator,
    run: reader.PatternRun,
    cancellation: reader.CancellationProbe,
    bilevel_sample_budget: ?*BilevelSampleBudget,
) anyerror!RawTile {
    const raw = try renderPageContentRgbaInBoxAllocWithBudget(
        alloc,
        run.pattern_bbox,
        run.tile_text_runs,
        run.tile_image_runs,
        run.tile_shading_runs,
        run.tile_pattern_runs,
        run.tile_shape_runs,
        cancellation,
        bilevel_sample_budget,
        .transparent,
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
    if (run.kind == .stroke) {
        const radius = run.stroke_width / 2.0;
        // Stroke caps and joins extend outside the centerline path. Include a
        // conservative miter envelope so those pixels are actually visited.
        const padding = if (run.line_join == .miter)
            radius * @max(1.0, run.miter_limit)
        else
            radius;
        min_x -= padding;
        max_x += padding;
        min_y -= padding;
        max_y += padding;
    }
    return .{ .min_x = min_x, .max_x = max_x, .min_y = min_y, .max_y = max_y };
}

fn pointInShape(x: f64, y: f64, run: reader.ShapeRun) bool {
    if (run.subpath_starts) |starts| {
        if (run.fill_rule == .even_odd) {
            var inside = false;
            for (starts, 0..) |start, i| {
                const end = if (i + 1 < starts.len) starts[i + 1] else run.points.len;
                if (end > start + 2 and pointInPolygonEvenOdd(x, y, run.points[start..end])) inside = !inside;
            }
            return inside;
        }
        var winding: i32 = 0;
        for (starts, 0..) |start, i| {
            const end = if (i + 1 < starts.len) starts[i + 1] else run.points.len;
            if (end > start + 2) winding += polygonWindingNumber(x, y, run.points[start..end]);
        }
        return winding != 0;
    }
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
    return polygonWindingNumber(x, y, points) != 0;
}

fn polygonWindingNumber(x: f64, y: f64, points: []const [2]f64) i32 {
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
    return winding;
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
    if (run.subpath_starts) |starts| {
        for (starts, 0..) |start, i| {
            const end = if (i + 1 < starts.len) starts[i + 1] else run.points.len;
            if (end <= start + 1) continue;
            var subpath = run;
            subpath.points = run.points[start..end];
            subpath.subpath_starts = null;
            subpath.subpath_closed = null;
            subpath.closed = if (run.subpath_closed) |closed| closed[i] else run.closed;
            if (pointInStrokeShapeSingle(x, y, subpath)) return true;
        }
        return false;
    }
    return pointInStrokeShapeSingle(x, y, run);
}

fn pointInStrokeShapeSingle(x: f64, y: f64, run: reader.ShapeRun) bool {
    const radius = run.stroke_width / 2.0;
    if (strokeContainsPoint(x, y, run, radius)) return true;

    if (run.points.len > 2) {
        const limit = if (run.closed) run.points.len else run.points.len - 1;
        var i: usize = if (run.closed) 0 else 1;
        while (i < limit) : (i += 1) {
            const prev = if (i == 0) run.points[run.points.len - 1] else run.points[i - 1];
            const curr = run.points[i];
            const next = if (i + 1 == run.points.len) run.points[0] else run.points[i + 1];
            if (segmentLengthSquared(prev, curr) <= 0.000001 or
                segmentLengthSquared(curr, next) <= 0.000001) continue;
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
        // A zero-length segment has no direction and a butt cap contributes no
        // area. Treating its endpoint distance as a stroked segment turns PDF
        // producer cleanup paths into large discs when the line width is high.
        if (hit.length <= 0.000001 and run.line_cap != .round) continue;
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
        const dx = next[0] - point[0];
        const dy = next[1] - point[1];
        if (dx * dx + dy * dy <= 0.000001) continue;
        var extend_start: f64 = 0.0;
        var extend_end: f64 = 0.0;
        if (!closed and line_cap == .square) {
            if (i == 0) extend_start = radius;
            if (i + 2 == points.len) extend_end = radius;
        }
        best = @min(best, pointSegmentDistanceExtended(x, y, point, next, extend_start, extend_end));
    }
    if (closed and points.len > 2) {
        const last = points[points.len - 1];
        const first = points[0];
        if (segmentLengthSquared(last, first) > 0.000001) {
            best = @min(best, pointSegmentDistanceExtended(x, y, last, first, 0, 0));
        }
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

fn segmentLengthSquared(a: [2]f64, b: [2]f64) f64 {
    const dx = b[0] - a[0];
    const dy = b[1] - a[1];
    return dx * dx + dy * dy;
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

/// Draws a bounded, allocation-free 5x7 glyph when an embedded outline cannot
/// be admitted. The previous solid affine rectangle destroyed character
/// identity precisely when a page hit its vector-work limit. This compact
/// native fallback keeps dense numbers OCR-readable and follows the original
/// text matrix, clipping, colors, alpha, blend mode, and render mode.
fn drawAffineFallbackGlyph(
    rgba: []u8,
    width: usize,
    height: usize,
    margin: usize,
    min_x: f64,
    max_y: f64,
    run: reader.TextRun,
    codepoint: u21,
    local_x: f64,
    local_w: f64,
) void {
    drawAffineFallbackGlyphCancelable(rgba, width, height, margin, min_x, max_y, run, codepoint, local_x, local_w, .{}) catch unreachable;
}

fn drawAffineFallbackGlyphCancelable(
    rgba: []u8,
    width: usize,
    height: usize,
    margin: usize,
    min_x: f64,
    max_y: f64,
    run: reader.TextRun,
    codepoint: u21,
    local_x: f64,
    local_w: f64,
    cancellation: reader.CancellationProbe,
) !void {
    if (local_w <= 0 or run.font_size <= 0) return;

    const det = run.a * run.d - run.b * run.c;
    if (@abs(det) < 0.000001) return;
    const ascent = effectiveRunAscent(run);
    const descent = effectiveRunDescent(run);
    const glyph_x = if (run.vertical) -local_w / 2.0 else local_x;
    const glyph_y = if (run.vertical) -local_x else 0;

    const corners = [_][2]f64{
        .{ run.x + run.a * glyph_x + run.c * (glyph_y - descent), run.y + run.b * glyph_x + run.d * (glyph_y - descent) },
        .{ run.x + run.a * (glyph_x + local_w) + run.c * (glyph_y - descent), run.y + run.b * (glyph_x + local_w) + run.d * (glyph_y - descent) },
        .{ run.x + run.a * glyph_x + run.c * (glyph_y + ascent), run.y + run.b * glyph_x + run.d * (glyph_y + ascent) },
        .{ run.x + run.a * (glyph_x + local_w) + run.c * (glyph_y + ascent), run.y + run.b * (glyph_x + local_w) + run.d * (glyph_y + ascent) },
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
        if ((py - y0) & 31 == 0) try cancellation.check();
        var px = x0;
        while (px < x1) : (px += 1) {
            const world_x = pixelWorldX(min_x, margin, px);
            const world_y = pixelWorldY(max_y, margin, py);
            if (has_clip and !pointPassesClip(world_x, world_y, run.clip_box, run.clip_points, run.clip_fill_rule)) continue;

            const dx = world_x - run.x;
            const dy = world_y - run.y;
            const lx = inv_a * dx + inv_c * dy;
            const ly = inv_b * dx + inv_d * dy - glyph_y;
            if (lx < glyph_x or lx >= glyph_x + local_w or ly <= -descent or ly > ascent) continue;
            if (fallbackGlyphModeColor(run, codepoint, glyph_x, local_w, lx, ly)) |color| {
                blendPixelMode(rgba, (py * width + px) * 4, color, run.blend_mode);
            }
        }
    }
}

fn fallbackGlyphModeColor(run: reader.TextRun, codepoint: u21, local_x: f64, local_w: f64, lx: f64, ly: f64) ?[4]u8 {
    const mode = @mod(run.render_mode, 8);
    const filled = fallbackGlyphContains(run, codepoint, local_x, local_w, lx, ly);
    return switch (mode) {
        0, 4 => if (filled) colorWithAlpha(run.fill_color, run.alpha) else null,
        1, 5 => if (fallbackGlyphStrokeContains(run, codepoint, local_x, local_w, lx, ly)) colorWithAlpha(run.stroke_color, run.stroke_alpha) else null,
        2, 6 => if (fallbackGlyphStrokeContains(run, codepoint, local_x, local_w, lx, ly)) colorWithAlpha(run.stroke_color, run.stroke_alpha) else if (filled) colorWithAlpha(run.fill_color, run.alpha) else null,
        3, 7 => null,
        else => if (filled) colorWithAlpha(run.fill_color, run.alpha) else null,
    };
}

fn fallbackGlyphContains(run: reader.TextRun, codepoint: u21, local_x: f64, local_w: f64, lx: f64, ly: f64) bool {
    const ascent = effectiveRunAscent(run);
    const descent = effectiveRunDescent(run);
    const normalized_x = (lx - local_x) / local_w;
    const normalized_y = (ascent - ly) / @max(0.000001, ascent + descent);
    if (normalized_x < 0 or normalized_x >= 1 or normalized_y < 0 or normalized_y >= 1) return false;
    const column: u3 = @intFromFloat(@min(4.0, @floor(normalized_x * 5.0)));
    const row: u3 = @intFromFloat(@min(6.0, @floor(normalized_y * 7.0)));
    return fallbackGlyphRow(codepoint, row) & (@as(u5, 0b10000) >> column) != 0;
}

fn fallbackGlyphStrokeContains(run: reader.TextRun, codepoint: u21, local_x: f64, local_w: f64, lx: f64, ly: f64) bool {
    const ascent = effectiveRunAscent(run);
    const descent = effectiveRunDescent(run);
    const glyph_h = @max(0.000001, ascent + descent);
    const basis_x = @sqrt(run.a * run.a + run.b * run.b);
    const basis_y = @sqrt(run.c * run.c + run.d * run.d);
    const avg_scale = @max(0.000001, (basis_x + basis_y) / 2.0);
    const stroke = @max(run.stroke_width / avg_scale, @min(local_w / 5.0, glyph_h / 7.0) * 0.35);
    if (!fallbackGlyphContains(run, codepoint, local_x, local_w, lx, ly)) {
        return fallbackGlyphContains(run, codepoint, local_x, local_w, lx - stroke, ly) or
            fallbackGlyphContains(run, codepoint, local_x, local_w, lx + stroke, ly) or
            fallbackGlyphContains(run, codepoint, local_x, local_w, lx, ly - stroke) or
            fallbackGlyphContains(run, codepoint, local_x, local_w, lx, ly + stroke);
    }
    return !fallbackGlyphContains(run, codepoint, local_x, local_w, lx - stroke, ly) or
        !fallbackGlyphContains(run, codepoint, local_x, local_w, lx + stroke, ly) or
        !fallbackGlyphContains(run, codepoint, local_x, local_w, lx, ly - stroke) or
        !fallbackGlyphContains(run, codepoint, local_x, local_w, lx, ly + stroke);
}

fn fallbackGlyphRow(codepoint: u21, row: u3) u5 {
    const cp: u21 = if (codepoint >= 'a' and codepoint <= 'z') codepoint - ('a' - 'A') else codepoint;
    const rows: [7]u5 = switch (cp) {
        '0' => .{ 0b01110, 0b10001, 0b10011, 0b10101, 0b11001, 0b10001, 0b01110 },
        '1' => .{ 0b00100, 0b01100, 0b00100, 0b00100, 0b00100, 0b00100, 0b01110 },
        '2' => .{ 0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0b01000, 0b11111 },
        '3' => .{ 0b11110, 0b00001, 0b00001, 0b01110, 0b00001, 0b00001, 0b11110 },
        '4' => .{ 0b00010, 0b00110, 0b01010, 0b10010, 0b11111, 0b00010, 0b00010 },
        '5' => .{ 0b11111, 0b10000, 0b10000, 0b11110, 0b00001, 0b00001, 0b11110 },
        '6' => .{ 0b01110, 0b10000, 0b10000, 0b11110, 0b10001, 0b10001, 0b01110 },
        '7' => .{ 0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b01000, 0b01000 },
        '8' => .{ 0b01110, 0b10001, 0b10001, 0b01110, 0b10001, 0b10001, 0b01110 },
        '9' => .{ 0b01110, 0b10001, 0b10001, 0b01111, 0b00001, 0b00001, 0b01110 },
        'A' => .{ 0b01110, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001 },
        'B' => .{ 0b11110, 0b10001, 0b10001, 0b11110, 0b10001, 0b10001, 0b11110 },
        'C' => .{ 0b01111, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b01111 },
        'D' => .{ 0b11110, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b11110 },
        'E' => .{ 0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b11111 },
        'F' => .{ 0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b10000 },
        'G' => .{ 0b01111, 0b10000, 0b10000, 0b10111, 0b10001, 0b10001, 0b01111 },
        'H' => .{ 0b10001, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001 },
        'I' => .{ 0b01110, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b01110 },
        'J' => .{ 0b00111, 0b00010, 0b00010, 0b00010, 0b10010, 0b10010, 0b01100 },
        'K' => .{ 0b10001, 0b10010, 0b10100, 0b11000, 0b10100, 0b10010, 0b10001 },
        'L' => .{ 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b11111 },
        'M' => .{ 0b10001, 0b11011, 0b10101, 0b10101, 0b10001, 0b10001, 0b10001 },
        'N' => .{ 0b10001, 0b11001, 0b10101, 0b10011, 0b10001, 0b10001, 0b10001 },
        'O' => .{ 0b01110, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01110 },
        'P' => .{ 0b11110, 0b10001, 0b10001, 0b11110, 0b10000, 0b10000, 0b10000 },
        'Q' => .{ 0b01110, 0b10001, 0b10001, 0b10001, 0b10101, 0b10010, 0b01101 },
        'R' => .{ 0b11110, 0b10001, 0b10001, 0b11110, 0b10100, 0b10010, 0b10001 },
        'S' => .{ 0b01111, 0b10000, 0b10000, 0b01110, 0b00001, 0b00001, 0b11110 },
        'T' => .{ 0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100 },
        'U' => .{ 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01110 },
        'V' => .{ 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01010, 0b00100 },
        'W' => .{ 0b10001, 0b10001, 0b10001, 0b10101, 0b10101, 0b10101, 0b01010 },
        'X' => .{ 0b10001, 0b10001, 0b01010, 0b00100, 0b01010, 0b10001, 0b10001 },
        'Y' => .{ 0b10001, 0b10001, 0b01010, 0b00100, 0b00100, 0b00100, 0b00100 },
        'Z' => .{ 0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b10000, 0b11111 },
        '.' => .{ 0, 0, 0, 0, 0, 0b00110, 0b00110 },
        ',' => .{ 0, 0, 0, 0, 0b00110, 0b00110, 0b00100 },
        ':' => .{ 0, 0b00110, 0b00110, 0, 0b00110, 0b00110, 0 },
        ';' => .{ 0, 0b00110, 0b00110, 0, 0b00110, 0b00110, 0b00100 },
        '-' => .{ 0, 0, 0, 0b11111, 0, 0, 0 },
        '_' => .{ 0, 0, 0, 0, 0, 0, 0b11111 },
        '=' => .{ 0, 0, 0b11111, 0, 0b11111, 0, 0 },
        '+' => .{ 0, 0b00100, 0b00100, 0b11111, 0b00100, 0b00100, 0 },
        '/' => .{ 0b00001, 0b00010, 0b00010, 0b00100, 0b01000, 0b01000, 0b10000 },
        '\\' => .{ 0b10000, 0b01000, 0b01000, 0b00100, 0b00010, 0b00010, 0b00001 },
        '(' => .{ 0b00010, 0b00100, 0b01000, 0b01000, 0b01000, 0b00100, 0b00010 },
        ')' => .{ 0b01000, 0b00100, 0b00010, 0b00010, 0b00010, 0b00100, 0b01000 },
        '[' => .{ 0b01110, 0b01000, 0b01000, 0b01000, 0b01000, 0b01000, 0b01110 },
        ']' => .{ 0b01110, 0b00010, 0b00010, 0b00010, 0b00010, 0b00010, 0b01110 },
        '<' => .{ 0b00010, 0b00100, 0b01000, 0b10000, 0b01000, 0b00100, 0b00010 },
        '>' => .{ 0b01000, 0b00100, 0b00010, 0b00001, 0b00010, 0b00100, 0b01000 },
        '!' => .{ 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0, 0b00100 },
        '?' => .{ 0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0, 0b00100 },
        '\'' => .{ 0b00100, 0b00100, 0b00010, 0, 0, 0, 0 },
        '"' => .{ 0b01010, 0b01010, 0b00100, 0, 0, 0, 0 },
        '#' => .{ 0b01010, 0b11111, 0b01010, 0b01010, 0b11111, 0b01010, 0 },
        '&' => .{ 0b01100, 0b10010, 0b10100, 0b01000, 0b10101, 0b10010, 0b01101 },
        '*' => .{ 0, 0b10101, 0b01110, 0b11111, 0b01110, 0b10101, 0 },
        '|' => .{ 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100 },
        '$' => .{ 0b00100, 0b01111, 0b10100, 0b01110, 0b00101, 0b11110, 0b00100 },
        '%' => .{ 0b11001, 0b11010, 0b00100, 0b01000, 0b10110, 0b00110, 0 },
        else => .{ 0b11111, 0b10001, 0b00010, 0b00100, 0b00000, 0b00100, 0b00100 },
    };
    return rows[row];
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

    // Normal source-over is the overwhelmingly common path. Keep its compact
    // integer formulation so transparency correctness does not add work to
    // ordinary antialiased text, vectors, and images.
    if (mode == .normal) {
        const da = @as(u32, canvas[dst + 3]);
        const inv_sa = 255 - sa;
        const out_a = sa + (da * inv_sa + 127) / 255;
        if (out_a == 0) return;
        inline for (0..3) |channel| {
            const src_p = @as(u32, src[channel]) * sa;
            const dst_p = (@as(u32, canvas[dst + channel]) * da * inv_sa + 127) / 255;
            canvas[dst + channel] = @intCast((src_p + dst_p + out_a / 2) / out_a);
        }
        canvas[dst + 3] = @intCast(out_a);
        return;
    }

    const da = @as(u64, canvas[dst + 3]);
    if (da == 0) {
        canvas[dst + 0] = src[0];
        canvas[dst + 1] = src[1];
        canvas[dst + 2] = src[2];
        canvas[dst + 3] = src[3];
        return;
    }
    if (da == 255) {
        const inv_sa = 255 - sa;
        inline for (0..3) |channel| {
            const blended = blendChannel(mode, src[channel], canvas[dst + channel]);
            canvas[dst + channel] = @intCast((@as(u32, blended) * sa + @as(u32, canvas[dst + channel]) * inv_sa + 127) / 255);
        }
        canvas[dst + 3] = 0xff;
        return;
    }
    const sa64 = @as(u64, sa);
    const out_alpha_numerator = sa64 * 255 + da * (255 - sa64);
    if (out_alpha_numerator == 0) return;
    inline for (0..3) |channel| {
        const source = @as(u64, src[channel]);
        const backdrop = @as(u64, canvas[dst + channel]);
        const blended = @as(u64, blendChannel(mode, src[channel], canvas[dst + channel]));
        const color_numerator = source * sa64 * (255 - da) +
            backdrop * da * (255 - sa64) +
            blended * sa64 * da;
        canvas[dst + channel] = @intCast((color_numerator + out_alpha_numerator / 2) / out_alpha_numerator);
    }
    canvas[dst + 3] = @intCast((out_alpha_numerator + 127) / 255);
}

fn compositeGroupCanvas(canvas: []u8, group_canvas: []const u8) void {
    compositeGroupCanvasCancelable(canvas, group_canvas, .{}) catch unreachable;
}

fn compositeGroupCanvasCancelable(canvas: []u8, group_canvas: []const u8, cancellation: reader.CancellationProbe) !void {
    return try compositeGroupCanvasModeCancelable(canvas, group_canvas, 0xff, .normal, cancellation);
}

fn compositeGroupCanvasModeCancelable(canvas: []u8, group_canvas: []const u8, alpha: u8, blend_mode: reader.BlendMode, cancellation: reader.CancellationProbe) !void {
    var i: usize = 0;
    while (i + 3 < group_canvas.len) : (i += 4) {
        if (i & 65_535 == 0) try cancellation.check();
        if (group_canvas[i + 3] == 0) continue;
        const source_alpha: u8 = @intCast((@as(u16, group_canvas[i + 3]) * @as(u16, alpha) + 127) / 255);
        blendPixelMode(canvas, i, .{ group_canvas[i + 0], group_canvas[i + 1], group_canvas[i + 2], source_alpha }, blend_mode);
    }
}

fn compositeNonisolatedGroupCanvasModeCancelable(
    canvas: []u8,
    group_result: []const u8,
    group_coverage: []const u8,
    alpha: u8,
    blend_mode: reader.BlendMode,
    cancellation: reader.CancellationProbe,
) !void {
    std.debug.assert(canvas.len == group_result.len and canvas.len == group_coverage.len);
    var i: usize = 0;
    while (i + 3 < canvas.len) : (i += 4) {
        if (i & 65_535 == 0) try cancellation.check();
        const coverage_alpha = group_coverage[i + 3];
        if (coverage_alpha == 0) continue;
        const group_alpha = @as(f64, @floatFromInt(coverage_alpha)) / 255.0;
        const backdrop_alpha = @as(f64, @floatFromInt(canvas[i + 3])) / 255.0;
        const result_alpha = @as(f64, @floatFromInt(group_result[i + 3])) / 255.0;
        var source: [4]u8 = undefined;
        inline for (0..3) |channel| {
            const result_premultiplied = (@as(f64, @floatFromInt(group_result[i + channel])) / 255.0) * result_alpha;
            const backdrop_premultiplied = (@as(f64, @floatFromInt(canvas[i + channel])) / 255.0) * backdrop_alpha;
            const recovered = (result_premultiplied - backdrop_premultiplied * (1.0 - group_alpha)) / group_alpha;
            source[channel] = @intFromFloat(@round(std.math.clamp(recovered, 0.0, 1.0) * 255.0));
        }
        source[3] = @intCast((@as(u16, coverage_alpha) * @as(u16, alpha) + 127) / 255);
        blendPixelMode(canvas, i, source, blend_mode);
    }
}

fn copyCanvasCancelable(dst: []u8, src: []const u8, width: usize, height: usize, cancellation: reader.CancellationProbe) !void {
    const row_bytes = std.math.mul(usize, width, 4) catch return error.RenderedPageTooLarge;
    for (0..height) |row| {
        if (row & 31 == 0) try cancellation.check();
        const start = row * row_bytes;
        @memcpy(dst[start .. start + row_bytes], src[start .. start + row_bytes]);
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

fn replaceCanvasWhereChangedRect(canvas: []u8, next: []const u8, backdrop: []const u8, canvas_width: usize, rect: PixelRect) void {
    replaceCanvasWhereChangedRectCancelable(canvas, next, backdrop, canvas_width, rect, .{}) catch unreachable;
}

fn replaceCanvasWhereChangedRectCancelable(canvas: []u8, next: []const u8, backdrop: []const u8, canvas_width: usize, rect: PixelRect, cancellation: reader.CancellationProbe) !void {
    var y = rect.y0;
    while (y < rect.y1) : (y += 1) {
        if ((y - rect.y0) & 31 == 0) try cancellation.check();
        var i = (y * canvas_width + rect.x0) * 4;
        const end = (y * canvas_width + rect.x1) * 4;
        while (i < end) : (i += 4) {
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
}

test "knockout dirty replacement never scans or mutates outside its bounds" {
    var backdrop = [_]u8{0} ** (4 * 4 * 4);
    var canvas = backdrop;
    var next = backdrop;
    next[0] = 99;
    const inside = (1 * 4 + 1) * 4;
    next[inside] = 42;
    next[inside + 3] = 255;
    replaceCanvasWhereChangedRect(&canvas, &next, &backdrop, 4, .{ .x0 = 1, .y0 = 1, .x1 = 2, .y1 = 2 });
    try std.testing.expectEqual(@as(u8, 0), canvas[0]);
    try std.testing.expectEqual(@as(u8, 42), canvas[inside]);
    try std.testing.expectEqual(@as(u8, 255), canvas[inside + 3]);
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

test "render plan preserves paint order ties and nested group schedules" {
    const alloc = std.testing.allocator;
    const text_runs = [_]reader.TextRun{
        .{ .text = "root", .x = 0, .y = 0, .font_size = 12, .paint_order = 4 },
        .{ .text = "child", .x = 0, .y = 0, .font_size = 12, .paint_order = 6, .group_id = 1, .group_knockout = true },
        .{ .text = "nested", .x = 0, .y = 0, .font_size = 12, .paint_order = 8, .group_id = 2, .group_parent_id = 1, .group_knockout = true },
    };
    const shape_runs = [_]reader.ShapeRun{
        .{ .kind = .fill, .paint_order = 4, .color = .{ 0, 0, 0, 0xff }, .stroke_width = 0, .closed = true, .points = @constCast(&[_][2]f64{}) },
        .{ .kind = .fill, .paint_order = 7, .group_id = 1, .group_knockout = true, .color = .{ 0, 0, 0, 0xff }, .stroke_width = 0, .closed = true, .points = @constCast(&[_][2]f64{}) },
    };

    var plan = try buildRenderPlanAlloc(alloc, &text_runs, &.{}, &.{}, &.{}, &shape_runs);
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), plan.groups.len);
    // Page + one child/backdrop/scratch trio for each active knockout group.
    try std.testing.expectEqual(@as(usize, 7), plan.peak_canvas_count);
    try std.testing.expectEqual(@as(usize, 3), plan.schedules[0].items.len);
    try std.testing.expect(plan.schedules[0].items[0] == .text);
    try std.testing.expect(plan.schedules[0].items[1] == .shape);
    try std.testing.expect(plan.schedules[0].items[2] == .group);

    const child_group_index = switch (plan.schedules[0].items[2]) {
        .group => |idx| idx,
        else => unreachable,
    };
    const child_schedule = plan.schedules[child_group_index + 1].items;
    try std.testing.expectEqual(@as(usize, 3), child_schedule.len);
    try std.testing.expect(child_schedule[0] == .text);
    try std.testing.expect(child_schedule[1] == .shape);
    try std.testing.expect(child_schedule[2] == .group);
}

test "render plan schedules transparency groups by earliest paint phase" {
    const alloc = std.testing.allocator;
    const shape_runs = [_]reader.ShapeRun{
        .{ .kind = .fill, .paint_order = 10, .paint_phase = 4, .color = .{ 0, 0, 0, 0xff }, .stroke_width = 0, .closed = true, .points = @constCast(&[_][2]f64{}) },
        .{ .kind = .fill, .paint_order = 10, .paint_phase = 8, .group_id = 7, .color = .{ 0, 0, 0, 0xff }, .stroke_width = 0, .closed = true, .points = @constCast(&[_][2]f64{}) },
        .{ .kind = .fill, .paint_order = 10, .paint_phase = 12, .color = .{ 0, 0, 0, 0xff }, .stroke_width = 0, .closed = true, .points = @constCast(&[_][2]f64{}) },
    };
    var plan = try buildRenderPlanAlloc(alloc, &.{}, &.{}, &.{}, &.{}, &shape_runs);
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), plan.schedules[0].items.len);
    try std.testing.expect(plan.schedules[0].items[0] == .shape);
    try std.testing.expect(plan.schedules[0].items[1] == .group);
    try std.testing.expect(plan.schedules[0].items[2] == .shape);
    try std.testing.expectEqual(@as(usize, 8), plan.groups[0].min_paint_phase);
}

test "render plan schedules a large page exactly once per choice" {
    const alloc = std.testing.allocator;
    const count = 10_000;
    const text_runs = try alloc.alloc(reader.TextRun, count);
    defer alloc.free(text_runs);
    for (text_runs, 0..) |*run, idx| run.* = .{
        .text = "",
        .x = 0,
        .y = 0,
        .font_size = 12,
        .paint_order = count - idx,
    };

    var plan = try buildRenderPlanAlloc(alloc, text_runs, &.{}, &.{}, &.{}, &.{});
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, count), plan.schedules[0].items.len);

    const seen = try alloc.alloc(bool, count);
    defer alloc.free(seen);
    @memset(seen, false);
    var previous_order: usize = 0;
    for (plan.schedules[0].items, 0..) |choice, position| {
        const idx = switch (choice) {
            .text => |text_idx| text_idx,
            else => return error.TestUnexpectedResult,
        };
        try std.testing.expect(!seen[idx]);
        seen[idx] = true;
        const order = text_runs[idx].paint_order;
        if (position > 0) try std.testing.expect(previous_order <= order);
        previous_order = order;
    }
    for (seen) |was_seen| try std.testing.expect(was_seen);
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

test "blend modes preserve source color over a transparent backdrop" {
    var canvas = [_]u8{0} ** 4;
    blendPixelMode(&canvas, 0, .{ 0x30, 0x60, 0x90, 0x80 }, .multiply);
    try std.testing.expectEqualSlices(u8, &.{ 0x30, 0x60, 0x90, 0x80 }, &canvas);
}

test "blend modes compose partially transparent source and backdrop" {
    var canvas = [_]u8{ 0x80, 0x80, 0x80, 0x80 };
    blendPixelMode(&canvas, 0, .{ 0x80, 0x80, 0x80, 0x80 }, .multiply);
    try std.testing.expectEqualSlices(u8, &.{ 0x6b, 0x6b, 0x6b, 0xc0 }, &canvas);
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

test "transparency group boundary applies alpha and blend mode once" {
    var canvas = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    const group = [_]u8{ 0x00, 0x00, 0x00, 0xff };
    try compositeGroupCanvasModeCancelable(&canvas, &group, 0x80, .multiply, .{});
    try std.testing.expectEqualSlices(u8, &.{ 0x7f, 0x7f, 0x7f, 0xff }, &canvas);
}

test "non-isolated group boundary removes backdrop before applying alpha" {
    // Red at 50% was painted into an opaque blue non-isolated backdrop.
    // Applying 50% group alpha must make the recovered red source 25%
    // effective coverage; treating the purple result as an isolated source
    // would incorrectly count blue twice.
    var canvas = [_]u8{ 0x00, 0x00, 0xff, 0xff };
    const group_result = [_]u8{ 0x80, 0x00, 0x7f, 0xff };
    const coverage = [_]u8{ 0xff, 0x00, 0x00, 0x80 };
    try compositeNonisolatedGroupCanvasModeCancelable(&canvas, &group_result, &coverage, 0x80, .normal, .{});
    try std.testing.expect(@abs(@as(i16, canvas[0]) - 0x40) <= 1);
    try std.testing.expectEqual(@as(u8, 0), canvas[1]);
    try std.testing.expect(@abs(@as(i16, canvas[2]) - 0xbf) <= 1);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[3]);
}

test "shading samples select the right side of a stitching discontinuity" {
    var run = reader.ShadingRun{
        .kind = .axial,
        .x0 = 0,
        .y0 = 0,
        .x1 = 1,
        .y1 = 0,
        .c0 = .{ 0, 0, 0, 0xff },
        .c1 = .{ 0xff, 0xff, 0xff, 0xff },
        .color_sample_count = 4,
    };
    run.color_sample_positions[0..4].* = .{ 0, 0.5, 0.5, 1 };
    run.color_samples[0..4].* = .{
        .{ 0, 0, 0, 0xff },
        .{ 0xff, 0, 0, 0xff },
        .{ 0, 0, 0xff, 0xff },
        .{ 0xff, 0xff, 0xff, 0xff },
    };
    try std.testing.expectEqual([4]u8{ 0, 0, 0xff, 0xff }, shadingColorAt(run, 0.5));
    const left = shadingColorAt(run, 0.499);
    try std.testing.expect(left[0] > 0xf0 and left[2] == 0);
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

    const raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, &.{}, &.{}, &.{}, &.{}, &shape_runs, .{});
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

    const raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, &.{}, &.{}, &.{}, &.{}, &shape_runs, .{});
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

    const raw = try renderPageContentRgbaInBoxAlloc(alloc, page_box, &.{}, &.{}, &.{}, &.{}, &shape_runs, .{});
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
    if (run.vertical) return run.font_size + run.char_spacing;
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

pub fn textRunBounds(run: reader.TextRun) struct { min_x: f64, max_x: f64, min_y: f64, max_y: f64 } {
    const width_est = estimateRunWidth(run);
    const ascent = effectiveRunAscent(run);
    const descent = effectiveRunDescent(run);
    const local_min_x = if (run.vertical) -run.font_size / 2.0 else 0;
    const local_max_x = if (run.vertical) run.font_size / 2.0 else width_est;
    const local_min_y = if (run.vertical) -width_est - descent else -descent;
    const local_max_y = ascent;
    const corners = [_][2]f64{
        .{ run.x + run.a * local_min_x + run.c * local_min_y, run.y + run.b * local_min_x + run.d * local_min_y },
        .{ run.x + run.a * local_max_x + run.c * local_min_y, run.y + run.b * local_max_x + run.d * local_min_y },
        .{ run.x + run.a * local_min_x + run.c * local_max_y, run.y + run.b * local_min_x + run.d * local_max_y },
        .{ run.x + run.a * local_max_x + run.c * local_max_y, run.y + run.b * local_max_x + run.d * local_max_y },
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

fn effectiveRunAscent(run: reader.TextRun) f64 {
    return if (run.ascent > 0 or run.descent > 0) run.ascent else run.font_size * 0.8;
}

fn effectiveRunDescent(run: reader.TextRun) f64 {
    return if (run.ascent > 0 or run.descent > 0) run.descent else run.font_size * 0.2;
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

test "raw page rotation normalizes dimensions and pixel orientation" {
    const alloc = std.testing.allocator;
    const Case = struct {
        rotation: PageRotation,
        width: usize,
        height: usize,
        expected: []const u8,
    };
    const cases = [_]Case{
        .{ .rotation = .clockwise_90, .width = 3, .height = 2, .expected = &.{ 4, 2, 0, 5, 3, 1 } },
        .{ .rotation = .clockwise_180, .width = 2, .height = 3, .expected = &.{ 5, 4, 3, 2, 1, 0 } },
        .{ .rotation = .clockwise_270, .width = 3, .height = 2, .expected = &.{ 1, 3, 5, 0, 2, 4 } },
    };

    for (cases) |case| {
        var raw = RawPageCanvas{
            .rgba = try alloc.alloc(u8, 2 * 3 * 4),
            .width = 2,
            .height = 3,
        };
        defer alloc.free(raw.rgba);
        for (0..6) |pixel| {
            @memset(raw.rgba[pixel * 4 .. pixel * 4 + 4], @intCast(pixel));
        }

        try rotateRawPageCanvasAlloc(alloc, &raw, case.rotation, .{});
        try std.testing.expectEqual(case.width, raw.width);
        try std.testing.expectEqual(case.height, raw.height);
        for (case.expected, 0..) |expected, pixel| {
            try std.testing.expectEqual(expected, raw.rgba[pixel * 4]);
        }
    }
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
    try std.testing.expectApproxEqAbs(@as(f64, 43.5), width, 0.001);
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

test "vertical text run bounds follow its negative y advance" {
    const bounds = textRunBounds(.{
        .text = @constCast("AB"),
        .x = 10,
        .y = 30,
        .font_size = 10,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .vertical = true,
        .advance_width = 20,
        .ascent = 8,
        .descent = 2,
    });
    try std.testing.expectApproxEqAbs(@as(f64, 5), bounds.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 15), bounds.max_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 8), bounds.min_y, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 38), bounds.max_y, 0.001);
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

test "scanline antialias fill matches bounded point sampling" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{
        .{ 2, 2 }, .{ 18, 2 }, .{ 18, 18 }, .{ 2, 18 },
        .{ 6, 6 }, .{ 6, 14 }, .{ 14, 14 }, .{ 14, 6 },
    });
    defer alloc.free(points);
    const starts = try alloc.dupe(usize, &.{ 0, 4 });
    defer alloc.free(starts);

    const run = reader.ShapeRun{
        .kind = .fill,
        .fill_rule = .nonzero,
        .color = .{ 17, 31, 47, 0xd9 },
        .stroke_width = 0,
        .closed = true,
        .clip_box = .{ .min_x = 3.25, .min_y = 4.25, .max_x = 16.75, .max_y = 17.75 },
        .points = points,
        .subpath_starts = starts,
        .antialias = true,
    };
    var expected: [20 * 20 * 4]u8 = undefined;
    var actual: [20 * 20 * 4]u8 = undefined;
    @memset(&expected, 0xff);
    @memset(&actual, 0xff);
    drawShapeRun(&expected, 20, 20, 0, 20, run);
    try drawShapeRunAlloc(alloc, &actual, 20, 20, 0, 20, run);
    try std.testing.expectEqualSlices(u8, &expected, &actual);
}

test "page raster cancellation propagates before paint" {
    const Cancelled = struct {
        fn check(_: ?*const anyopaque) bool {
            return true;
        }
    };
    try std.testing.expectError(error.Canceled, renderPageContentPngInBoxRotatedCancelable(
        std.testing.allocator,
        .{ .min_x = 0, .min_y = 0, .max_x = 10, .max_y = 10 },
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        .none,
        .{ .is_cancelled_fn = Cancelled.check },
    ));
}

test "fallback text raster observes cancellation" {
    const Cancelled = struct {
        fn check(_: ?*const anyopaque) bool {
            return true;
        }
    };
    var canvas = [_]u8{0xff} ** (32 * 32 * 4);
    try std.testing.expectError(error.Canceled, drawTextRunCancelable(
        &canvas,
        32,
        32,
        0,
        0,
        32,
        .{ .text = @constCast("fallback"), .x = 1, .y = 20, .font_size = 12 },
        .{ .is_cancelled_fn = Cancelled.check },
    ));
}

test "group canvas operations observe cancellation" {
    const Cancelled = struct {
        fn check(_: ?*const anyopaque) bool {
            return true;
        }
    };
    const cancellation: reader.CancellationProbe = .{ .is_cancelled_fn = Cancelled.check };
    var canvas = [_]u8{0xff} ** (4 * 4 * 4);
    var next = [_]u8{0} ** (4 * 4 * 4);
    try std.testing.expectError(error.Canceled, compositeGroupCanvasCancelable(&canvas, &next, cancellation));
    try std.testing.expectError(error.Canceled, copyCanvasCancelable(&canvas, &next, 4, 4, cancellation));
    try std.testing.expectError(error.Canceled, copyCanvasRectCancelable(&canvas, &next, 4, PixelRect.full(4, 4), cancellation));
    try std.testing.expectError(error.Canceled, replaceCanvasWhereChangedRectCancelable(&canvas, &next, &canvas, 4, PixelRect.full(4, 4), cancellation));
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

    const inside = ((14 * 20) + 5) * 4;
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

    const inside = ((14 * 20) + 5) * 4;
    const outside = ((14 * 20) + 15) * 4;
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
    try std.testing.expectApproxEqAbs(@as(f64, 1.6), bounds.min_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 12.8), bounds.max_y, 0.001);
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
    try std.testing.expect((max_x - min_x) > (max_y - min_y));
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
    const idx = ((4 * 32) + 10) * 4;
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
    var black: usize = 0;
    var white: usize = 0;
    for (canvas[0..], 0..) |channel, i| if (@mod(i, 4) == 0) {
        if (channel == 0) black += 1 else if (channel == 0xff) white += 1;
    };
    try std.testing.expect(black > 0);
    try std.testing.expect(white > black);
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
    var found_blue = false;
    for (0..canvas.len / 4) |i| {
        const px = i * 4;
        if (canvas[px] == 0 and canvas[px + 1] == 0 and canvas[px + 2] == 0xff) found_blue = true;
    }
    try std.testing.expect(found_blue);
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
    var found_blue = false;
    var found_red = false;
    for (0..canvas.len / 4) |i| {
        const px = i * 4;
        if (canvas[px] == 0 and canvas[px + 1] == 0 and canvas[px + 2] == 0xff) found_blue = true;
        if (canvas[px] == 0xff and canvas[px + 1] == 0 and canvas[px + 2] == 0) found_red = true;
    }
    try std.testing.expect(found_blue);
    try std.testing.expect(found_red);
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
    var found_translucent_stroke = false;
    for (canvas[0..], 0..) |channel, i| if (@mod(i, 4) == 0 and channel > 0 and channel < 0xff) {
        found_translucent_stroke = true;
    };
    try std.testing.expect(found_translucent_stroke);
}

test "draw text run stroke width changes outline thickness" {
    const alloc = std.testing.allocator;
    const text = try alloc.dupe(u8, "I");
    defer alloc.free(text);

    var thin: [64 * 64 * 4]u8 = undefined;
    @memset(&thin, 0xff);
    drawTextRun(&thin, 64, 64, 0, 0, 64, .{
        .text = text,
        .x = 12,
        .y = 52,
        .font_size = 28,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 1,
        .stroke_width = 1,
    });

    var thick: [64 * 64 * 4]u8 = undefined;
    @memset(&thick, 0xff);
    drawTextRun(&thick, 64, 64, 0, 0, 64, .{
        .text = text,
        .x = 12,
        .y = 52,
        .font_size = 28,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .render_mode = 1,
        .stroke_width = 5,
    });

    var thin_pixels: usize = 0;
    var thick_pixels: usize = 0;
    for (thin[0..], thick[0..], 0..) |thin_channel, thick_channel, i| if (@mod(i, 4) == 0) {
        if (thin_channel != 0xff) thin_pixels += 1;
        if (thick_channel != 0xff) thick_pixels += 1;
    };
    try std.testing.expect(thick_pixels > thin_pixels);
}

test "fallback glyphs preserve numeric character identity" {
    try std.testing.expect(fallbackGlyphRow('1', 0) != fallbackGlyphRow('8', 0));
    try std.testing.expect(fallbackGlyphRow('1', 3) != fallbackGlyphRow('8', 3));

    var canvas: [48 * 24 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawTextRun(&canvas, 48, 24, 0, 0, 24, .{
        .text = @constCast("18"),
        .x = 4,
        .y = 20,
        .font_size = 14,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
    });
    var changed: usize = 0;
    for (canvas[0..], 0..) |channel, i| if (@mod(i, 4) == 0 and channel != 0xff) {
        changed += 1;
    };
    try std.testing.expect(changed > 10);
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

test "image minification honors explicit interpolation policy" {
    var rgba = [_]u8{
        0xff, 0x00, 0x00, 0xff,
        0x00, 0x00, 0xff, 0xff,
    };
    const base: reader.ImageRun = .{
        .rgba = &rgba,
        .width = 2,
        .height = 1,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .e = 0,
        .f = 0,
        .x = 0,
        .y = 0,
        .draw_width = 1,
        .draw_height = 1,
    };

    var nearest = [_]u8{0xff} ** 4;
    drawImageRun(&nearest, 1, 1, 0, 0, 1, base);
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0xff, 0xff }, &nearest);

    var filtered = [_]u8{0xff} ** 4;
    var interpolated = base;
    interpolated.interpolate = true;
    drawImageRun(&filtered, 1, 1, 0, 0, 1, interpolated);
    try std.testing.expect(filtered[0] > 0 and filtered[2] > 0);
    try std.testing.expectEqual(@as(u8, 0), filtered[1]);
    try std.testing.expectEqual(@as(u8, 0xff), filtered[3]);
}

test "image mask stencil paints decoded coverage with its occurrence color" {
    var rgba = [_]u8{ 0, 0, 0, 0xff };
    const run: reader.ImageRun = .{
        .rgba = &rgba,
        .width = 1,
        .height = 1,
        .stencil_color = .{ 51, 102, 153, 255 },
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .e = 0,
        .f = 0,
        .x = 0,
        .y = 0,
        .draw_width = 1,
        .draw_height = 1,
    };

    var canvas = [_]u8{0xff} ** 4;
    drawImageRun(&canvas, 1, 1, 0, 0, 1, run);
    try std.testing.expectEqualSlices(u8, &.{ 51, 102, 153, 255 }, &canvas);
}

test "OCR bilevel minification preserves source ink coverage" {
    var rgba = [_]u8{
        0x00, 0x00, 0x00, 0xff,
        0xff, 0xff, 0xff, 0xff,
    };
    const exact: reader.ImageRun = .{
        .rgba = &rgba,
        .width = 2,
        .height = 1,
        .bilevel = true,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .e = 0,
        .f = 0,
        .x = 0,
        .y = 0,
        .draw_width = 1,
        .draw_height = 1,
    };

    var exact_canvas = [_]u8{0xff} ** 4;
    drawImageRun(&exact_canvas, 1, 1, 0, 0, 1, exact);
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0xff, 0xff, 0xff }, &exact_canvas);

    var ocr_canvas = [_]u8{0xff} ** 4;
    var ocr = exact;
    ocr.ocr_coverage_minify = true;
    drawImageRun(&ocr_canvas, 1, 1, 0, 0, 1, ocr);
    try std.testing.expect(ocr_canvas[0] >= 120 and ocr_canvas[0] <= 135);
    try std.testing.expectEqual(ocr_canvas[0], ocr_canvas[1]);
    try std.testing.expectEqual(ocr_canvas[0], ocr_canvas[2]);
    try std.testing.expectEqual(@as(u8, 0xff), ocr_canvas[3]);
}

test "OCR pattern stencil minification preserves sparse mask coverage" {
    const alloc = std.testing.allocator;
    var tile_points = [_][2]f64{ .{ 0, 0 }, .{ 1, 0 }, .{ 1, 1 }, .{ 0, 1 } };
    var tile_shapes = [_]reader.ShapeRun{.{
        .kind = .fill,
        .color = .{ 0xff, 0, 0, 0xff },
        .stroke_width = 1,
        .closed = true,
        .points = &tile_points,
    }};
    var mask_rgba = [_]u8{0} ** (8 * 4);
    mask_rgba[0..4].* = .{ 0xff, 0xff, 0xff, 0xff };
    const run: reader.PatternRun = .{
        .kind = .fill,
        .points = &.{},
        .stencil_mask = .{
            .rgba = &mask_rgba,
            .width = 8,
            .height = 1,
            .bilevel = true,
            .ocr_coverage_minify = true,
            .a = 1,
            .b = 0,
            .c = 0,
            .d = 1,
            .e = 0,
            .f = 0,
            .x = 0,
            .y = 0,
            .draw_width = 1,
            .draw_height = 1,
        },
        .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 1, .max_y = 1 },
        .pattern_x_step = 1,
        .pattern_y_step = 1,
        .tile_shape_runs = &tile_shapes,
    };

    var canvas = [_]u8{0xff} ** 4;
    try drawPatternRun(alloc, &canvas, 1, 1, 0, 1, run);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[0]);
    try std.testing.expect(canvas[1] >= 222 and canvas[1] <= 224);
    try std.testing.expectEqual(canvas[1], canvas[2]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[3]);
}

test "OCR bilevel area filter retains thin rules beyond four-to-one minification" {
    var rgba = [_]u8{0xff} ** (8 * 4);
    rgba[0] = 0;
    rgba[1] = 0;
    rgba[2] = 0;
    const run: reader.ImageRun = .{
        .rgba = &rgba,
        .width = 8,
        .height = 1,
        .bilevel = true,
        .ocr_coverage_minify = true,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .e = 0,
        .f = 0,
        .x = 0,
        .y = 0,
        .draw_width = 1,
        .draw_height = 1,
    };

    var canvas = [_]u8{0xff} ** 4;
    drawImageRun(&canvas, 1, 1, 0, 0, 1, run);
    try std.testing.expect(canvas[0] >= 222 and canvas[0] <= 224);
    try std.testing.expectEqual(canvas[0], canvas[1]);
    try std.testing.expectEqual(canvas[0], canvas[2]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[3]);
}

test "OCR bilevel exact integration uses cached full-source summary when unaffordable" {
    var rgba = [_]u8{0xff} ** (1024 * 4);
    rgba[511 * 4] = 0;
    rgba[511 * 4 + 1] = 0;
    rgba[511 * 4 + 2] = 0;
    const run: reader.ImageRun = .{
        .rgba = &rgba,
        .width = 1024,
        .height = 1,
        .bilevel = true,
        .bilevel_fallback = .{ 0xfe, 0xfe, 0xfe, 0xff },
        .ocr_coverage_minify = true,
        .a = 1,
        .b = 0,
        .c = 0,
        .d = 1,
        .e = 0,
        .f = 0,
        .x = 0,
        .y = 0,
        .draw_width = 1,
        .draw_height = 1,
    };
    var budget = BilevelSampleBudget{ .remaining_samples = 8 };
    var cancellation = BilevelCancellationPoller.init(.{});
    const sample = try coveragePreservingBilevelSample(run, 0.5, 0.5, 1, 0, 0, 1, &cancellation, &budget);
    try std.testing.expectEqual(@as(u64, 8), budget.remaining_samples);
    try std.testing.expectEqual(@as(u8, 0xfe), sample[0]);
    try std.testing.expectEqual(sample[0], sample[1]);
    try std.testing.expectEqual(sample[0], sample[2]);
    try std.testing.expectEqual(@as(u8, 0xff), sample[3]);
}

test "OCR bilevel fallback scales alpha by transformed source coverage" {
    var rgba = [_]u8{ 0, 0, 0, 0xff };
    const run: reader.ImageRun = .{
        .rgba = &rgba,
        .width = 1,
        .height = 1,
        .bilevel = true,
        .bilevel_fallback = .{ 0, 0, 0, 0xff },
        .ocr_coverage_minify = true,
        .a = 0.1,
        .b = 0,
        .c = 0,
        .d = 0.1,
        .e = 0.1,
        .f = 0.1,
        .x = 0.1,
        .y = 0.1,
        .draw_width = 0.1,
        .draw_height = 0.1,
    };
    var budget = BilevelSampleBudget{ .remaining_samples = 0 };
    var cancellation = BilevelCancellationPoller.init(.{});
    const sample = try coveragePreservingBilevelSample(run, 0.5, 0.5, 10, 0, 0, 10, &cancellation, &budget);
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 3 }, &sample);
}

test "bilevel cancellation polling is amortized across destination pixels" {
    const ProbeState = struct {
        checks: usize = 0,

        fn isCancelled(context: ?*const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(context.?)));
            self.checks += 1;
            return false;
        }
    };

    var state = ProbeState{};
    var cancellation = BilevelCancellationPoller.init(.{
        .context = &state,
        .is_cancelled_fn = ProbeState.isCancelled,
    });
    for (0..BilevelCancellationPoller.work_per_check * 3 + 17) |_| try cancellation.complete(1);

    try std.testing.expectEqual(@as(usize, 3), state.checks);
    try std.testing.expectEqual(@as(u64, 17), cancellation.work_since_check);
}

test "legacy shape raster path observes cancellation" {
    const Cancelled = struct {
        fn check(_: ?*const anyopaque) bool {
            return true;
        }
    };
    var canvas = [_]u8{0xff} ** (32 * 32 * 4);
    const points = [_][2]f64{ .{ 1, 1 }, .{ 31, 31 } };
    try std.testing.expectError(error.Canceled, drawShapeRunAllocCancelable(
        std.testing.allocator,
        &canvas,
        32,
        32,
        0,
        32,
        .{
            .kind = .stroke,
            .color = .{ 0, 0, 0, 0xff },
            .stroke_width = 1,
            .closed = false,
            .points = @constCast(&points),
            .antialias = true,
        },
        .{ .is_cancelled_fn = Cancelled.check },
    ));
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

    const endpoint_pixel = ((11 * 24) + 10) * 4;
    try std.testing.expectEqual(@as(u8, 0), canvas[endpoint_pixel]);
}

test "draw shape run butt cap ignores zero-length segments" {
    const alloc = std.testing.allocator;
    const points = try alloc.dupe([2]f64, &.{ .{ 10, 10 }, .{ 10, 10 } });
    defer alloc.free(points);

    var canvas: [24 * 24 * 4]u8 = undefined;
    @memset(&canvas, 0xff);
    drawShapeRun(&canvas, 24, 24, 0, 24, .{
        .kind = .stroke,
        .fill_rule = .nonzero,
        .line_cap = .butt,
        .line_join = .miter,
        .miter_limit = 10,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 20,
        .closed = false,
        .points = points,
    });

    for (canvas) |channel| try std.testing.expectEqual(@as(u8, 0xff), channel);
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

    const outer_corner = ((23 * 28) + 20) * 4;
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

    const miter_pixel = ((28 * 32) + 20) * 4;
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
    for (0..canvas.len / 4) |pixel| canvas[pixel * 4 ..][0..4].* = .{ 0x00, 0x00, 0xff, 0xff };
    try drawPatternRun(alloc, &canvas, 8, 4, 0, 4, run);

    const left_red = ((2 * 8) + 1) * 4;
    const right_red = ((2 * 8) + 5) * 4;
    const transparent_gap = ((2 * 8) + 3) * 4;
    try std.testing.expectEqual(@as(u8, 0xff), canvas[left_red + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[left_red + 1]);
    try std.testing.expectEqual(@as(u8, 0xff), canvas[right_red + 0]);
    try std.testing.expectEqual(@as(u8, 0x00), canvas[right_red + 1]);
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0xff, 0xff }, canvas[transparent_gap .. transparent_gap + 4]);
}

test "draw pattern run clips paint through image stencil coverage" {
    const alloc = std.testing.allocator;
    var tile_points = [_][2]f64{ .{ 0, 0 }, .{ 1, 0 }, .{ 1, 1 }, .{ 0, 1 } };
    var tile_shapes = [_]reader.ShapeRun{.{
        .kind = .fill,
        .color = .{ 0xff, 0x00, 0x00, 0xff },
        .stroke_width = 1,
        .closed = true,
        .points = &tile_points,
    }};
    var mask_rgba = [_]u8{
        0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0x00,
    };
    const run: reader.PatternRun = .{
        .kind = .fill,
        .points = &.{},
        .stencil_mask = .{
            .rgba = &mask_rgba,
            .width = 2,
            .height = 1,
            .a = 4,
            .b = 0,
            .c = 0,
            .d = 2,
            .e = 0,
            .f = 0,
            .x = 0,
            .y = 0,
            .draw_width = 4,
            .draw_height = 2,
        },
        .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 1, .max_y = 1 },
        .pattern_x_step = 1,
        .pattern_y_step = 1,
        .tile_shape_runs = &tile_shapes,
    };

    var canvas: [4 * 2 * 4]u8 = undefined;
    for (0..canvas.len / 4) |pixel| canvas[pixel * 4 ..][0..4].* = .{ 0x00, 0x00, 0xff, 0xff };
    try drawPatternRun(alloc, &canvas, 4, 2, 0, 2, run);

    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0x00, 0x00, 0xff }, canvas[0..4]);
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0xff, 0xff }, canvas[12..16]);
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
    const transparent_gap = ((2 * 4) + 3) * 4;
    try std.testing.expect(canvas[green_px + 0] >= 0x7f and canvas[green_px + 0] <= 0x80);
    try std.testing.expect(canvas[green_px + 1] > 0x80);
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0xff, 0xff, 0xff }, canvas[transparent_gap .. transparent_gap + 4]);
}

test "reader and renderer preserve uncolored tiling pattern cell geometry" {
    const alloc = std.testing.allocator;
    const pattern_content = "0 0 5 10 re\nf\n";
    const page_content = "/CS1 cs\n0 1 0 /P1 scn\n0 0 20 20 re\nf\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 20 20] /Resources << /ColorSpace << /CS1 [/Pattern /DeviceRGB] >> /Pattern << /P1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
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

    var parsed = try reader.Reader.init(alloc, sample);
    defer parsed.deinit();
    var runs = try parsed.extractPageRenderRunsAlloc(1);
    defer runs.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), runs.shape_runs.len);
    try std.testing.expectEqual(@as(usize, 1), runs.pattern_runs.len);
    try std.testing.expectEqual(@as(usize, 1), runs.pattern_runs[0].tile_shape_runs.len);
    const raw = try renderPageContentRgbaInBoxAlloc(
        alloc,
        runs.page_box,
        runs.text_runs,
        runs.image_runs,
        runs.shading_runs,
        runs.pattern_runs,
        runs.shape_runs,
        .{},
    );
    defer alloc.free(raw.rgba);

    var green_pixels: usize = 0;
    for (0..raw.width * raw.height) |pixel| {
        const offset = pixel * 4;
        if (raw.rgba[offset] < 0x40 and raw.rgba[offset + 1] > 0xc0 and raw.rgba[offset + 2] < 0x40)
            green_pixels += 1;
    }
    try std.testing.expect(green_pixels > 0);
    try std.testing.expect(green_pixels < raw.width * raw.height);
}

test "nonzero glyph paths preserve counter contours" {
    var points = [_][2]f64{
        .{ 0, 0 }, .{ 10, 0 }, .{ 10, 10 }, .{ 0, 10 },
        .{ 3, 3 }, .{ 3, 7 },  .{ 7, 7 },   .{ 7, 3 },
    };
    var starts = [_]usize{ 0, 4 };
    const run: reader.ShapeRun = .{
        .kind = .fill,
        .color = .{ 0, 0, 0, 255 },
        .stroke_width = 0,
        .closed = true,
        .points = &points,
        .subpath_starts = &starts,
    };
    try std.testing.expect(pointInShape(1, 1, run));
    try std.testing.expect(!pointInShape(5, 5, run));
}

test "render plan preserves text fill before stroke across backing kinds" {
    var points = [_][2]f64{ .{ 0, 0 }, .{ 1, 0 }, .{ 1, 1 } };
    const patterns = [_]reader.PatternRun{.{
        .kind = .stroke,
        .paint_order = 4,
        .paint_phase = 1,
        .points = &points,
        .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 1, .max_y = 1 },
        .pattern_x_step = 1,
        .pattern_y_step = 1,
    }};
    const shapes = [_]reader.ShapeRun{.{
        .kind = .fill,
        .paint_order = 4,
        .paint_phase = 0,
        .color = .{ 0, 0, 0, 255 },
        .stroke_width = 0,
        .closed = true,
        .points = &points,
    }};
    const context = RenderChoiceSortContext{
        .text_runs = &.{},
        .image_runs = &.{},
        .shading_runs = &.{},
        .pattern_runs = &patterns,
        .shape_runs = &shapes,
        .groups = &.{},
    };
    try std.testing.expect(context.lessThan(.{ .shape = 0 }, .{ .pattern = 0 }));
    try std.testing.expect(!context.lessThan(.{ .pattern = 0 }, .{ .shape = 0 }));
}
