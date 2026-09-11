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

//! Pure, allocation-bounded Qwen3-VL request planning.
//!
//! This module deliberately contains no backend calls. The same checked plan is
//! consumed by preprocessing, Metal admission, M-RoPE, DeepStack injection, and
//! cache identity so those paths cannot disagree about token counts or spans.

const std = @import("std");

pub const text_modality: u8 = 0;
pub const image_modality: u8 = 1;
pub const video_modality: u8 = 2;

pub const VisionGrid = struct {
    temporal: u32,
    height: u32,
    width: u32,

    pub fn mergedTokenCount(self: VisionGrid, spatial_merge_size: u32) !usize {
        if (self.temporal == 0 or self.height == 0 or self.width == 0 or spatial_merge_size == 0)
            return error.InvalidVisionGrid;
        if (self.height % spatial_merge_size != 0 or self.width % spatial_merge_size != 0)
            return error.InvalidVisionGrid;
        const spatial = std.math.mul(
            usize,
            @as(usize, self.height / spatial_merge_size),
            @as(usize, self.width / spatial_merge_size),
        ) catch return error.RequestTooLarge;
        return std.math.mul(usize, @as(usize, self.temporal), spatial) catch
            return error.RequestTooLarge;
    }
};

pub const VisualSpan = struct {
    token_offset: usize,
    token_count: usize,
    grid: VisionGrid,
};

pub const RequestPlan = struct {
    allocator: std.mem.Allocator,
    /// Axis-major [temporal][height][width], each axis having token_count items.
    mrope_positions: []u32,
    /// Ordinary monotonically increasing token positions used by the causal mask.
    causal_positions: []u32,
    visual_token_mask: []bool,
    visual_spans: []VisualSpan,
    /// Added to the ordinary next-token position for cached generation.
    mrope_position_delta: i64,

    pub fn deinit(self: *RequestPlan) void {
        self.allocator.free(self.mrope_positions);
        self.allocator.free(self.causal_positions);
        self.allocator.free(self.visual_token_mask);
        self.allocator.free(self.visual_spans);
        self.* = undefined;
    }

    pub fn tokenCount(self: RequestPlan) usize {
        return self.causal_positions.len;
    }

    pub fn axis(self: RequestPlan, index: usize) []const u32 {
        std.debug.assert(index < 3);
        const token_count = self.tokenCount();
        return self.mrope_positions[index * token_count ..][0..token_count];
    }

    pub fn nextDecodePosition(self: RequestPlan) !u32 {
        const next: i64 = @as(i64, @intCast(self.tokenCount())) + self.mrope_position_delta;
        if (next < 0 or next > std.math.maxInt(u32)) return error.RequestTooLarge;
        return @intCast(next);
    }
};

pub const Limits = struct {
    max_input_tokens: usize,
    max_images: usize,
    max_visual_tokens: usize,
};

/// Build the exact image/text position plan used by the Qwen3-VL reference
/// implementation. Video is intentionally rejected by this release contract.
pub fn buildRequestPlan(
    allocator: std.mem.Allocator,
    modality_ids: []const u8,
    image_grids: []const VisionGrid,
    spatial_merge_size: u32,
    limits: Limits,
) !RequestPlan {
    if (modality_ids.len == 0) return error.EmptyPrompt;
    if (modality_ids.len > limits.max_input_tokens) return error.InputTokenLimitExceeded;
    if (image_grids.len > limits.max_images) return error.ImageLimitExceeded;
    if (spatial_merge_size == 0) return error.InvalidVisionGrid;

    const positions_len = std.math.mul(usize, modality_ids.len, 3) catch
        return error.RequestTooLarge;
    const positions = try allocator.alloc(u32, positions_len);
    errdefer allocator.free(positions);
    @memset(positions, 0);
    const causal_positions = try allocator.alloc(u32, modality_ids.len);
    errdefer allocator.free(causal_positions);
    const visual_mask = try allocator.alloc(bool, modality_ids.len);
    errdefer allocator.free(visual_mask);
    @memset(visual_mask, false);
    var spans = std.ArrayListUnmanaged(VisualSpan).empty;
    errdefer spans.deinit(allocator);
    try spans.ensureTotalCapacity(allocator, image_grids.len);

    for (causal_positions, 0..) |*position, token_index| {
        position.* = std.math.cast(u32, token_index) orelse return error.RequestTooLarge;
    }

    var current_position: u64 = 0;
    var image_index: usize = 0;
    var visual_tokens: usize = 0;
    var token_offset: usize = 0;
    while (token_offset < modality_ids.len) {
        const modality = modality_ids[token_offset];
        var run_end = token_offset + 1;
        while (run_end < modality_ids.len and modality_ids[run_end] == modality) : (run_end += 1) {}
        const run_len = run_end - token_offset;

        switch (modality) {
            text_modality => {
                for (0..run_len) |relative| {
                    const position = std.math.add(u64, current_position, relative) catch
                        return error.RequestTooLarge;
                    const value = std.math.cast(u32, position) orelse return error.RequestTooLarge;
                    setAllAxes(positions, modality_ids.len, token_offset + relative, value, value, value);
                }
                current_position = std.math.add(u64, current_position, run_len) catch
                    return error.RequestTooLarge;
            },
            image_modality => {
                if (image_index >= image_grids.len) return error.ImagePlaceholderCountMismatch;
                const grid = image_grids[image_index];
                const expected_tokens = try grid.mergedTokenCount(spatial_merge_size);
                if (run_len != expected_tokens) return error.ImageTokenCountMismatch;
                visual_tokens = std.math.add(usize, visual_tokens, run_len) catch
                    return error.RequestTooLarge;
                if (visual_tokens > limits.max_visual_tokens) return error.VisualTokenLimitExceeded;
                try spans.append(allocator, .{
                    .token_offset = token_offset,
                    .token_count = run_len,
                    .grid = grid,
                });
                for (0..run_len) |relative| {
                    const merged_h = grid.height / spatial_merge_size;
                    const merged_w = grid.width / spatial_merge_size;
                    const spatial_plane: usize = @as(usize, merged_h) * merged_w;
                    const temporal = relative / spatial_plane;
                    const within_plane = relative % spatial_plane;
                    const height = within_plane / merged_w;
                    const width = within_plane % merged_w;
                    const temporal_pos = std.math.add(u64, current_position, temporal) catch
                        return error.RequestTooLarge;
                    const height_pos = std.math.add(u64, current_position, height) catch
                        return error.RequestTooLarge;
                    const width_pos = std.math.add(u64, current_position, width) catch
                        return error.RequestTooLarge;
                    setAllAxes(
                        positions,
                        modality_ids.len,
                        token_offset + relative,
                        std.math.cast(u32, temporal_pos) orelse return error.RequestTooLarge,
                        std.math.cast(u32, height_pos) orelse return error.RequestTooLarge,
                        std.math.cast(u32, width_pos) orelse return error.RequestTooLarge,
                    );
                    visual_mask[token_offset + relative] = true;
                }
                current_position = std.math.add(
                    u64,
                    current_position,
                    @max(grid.height, grid.width) / spatial_merge_size,
                ) catch return error.RequestTooLarge;
                image_index += 1;
            },
            video_modality => return error.VideoUnsupported,
            else => return error.InvalidModality,
        }
        token_offset = run_end;
    }

    if (image_index != image_grids.len) return error.ImagePlaceholderCountMismatch;
    var max_position: u32 = 0;
    for (positions) |position| max_position = @max(max_position, position);
    const delta: i64 = @as(i64, max_position) + 1 - @as(i64, @intCast(modality_ids.len));
    return .{
        .allocator = allocator,
        .mrope_positions = positions,
        .causal_positions = causal_positions,
        .visual_token_mask = visual_mask,
        .visual_spans = try spans.toOwnedSlice(allocator),
        .mrope_position_delta = delta,
    };
}

pub fn buildImageModalityIds(
    allocator: std.mem.Allocator,
    token_ids: []const i64,
    image_token_id: i32,
) ![]u8 {
    if (image_token_id < 0) return error.InvalidImageToken;
    const result = try allocator.alloc(u8, token_ids.len);
    for (token_ids, result) |token_id, *modality| {
        modality.* = if (token_id == image_token_id) image_modality else text_modality;
    }
    return result;
}

pub fn validateDeepstackFeatureCount(plan: RequestPlan, feature_counts: []const usize) !void {
    for (feature_counts) |feature_count| {
        if (feature_count != plan.visual_spans.len) return error.DeepstackImageCountMismatch;
    }
}

fn setAllAxes(positions: []u32, token_count: usize, index: usize, temporal: u32, height: u32, width: u32) void {
    positions[index] = temporal;
    positions[token_count + index] = height;
    positions[2 * token_count + index] = width;
}

test "Qwen3-VL request plan matches official mixed text image M-RoPE semantics" {
    const allocator = std.testing.allocator;
    const modality_ids = [_]u8{
        text_modality,
        text_modality,
        image_modality,
        image_modality,
        image_modality,
        image_modality,
        text_modality,
        text_modality,
    };
    var plan = try buildRequestPlan(
        allocator,
        &modality_ids,
        &.{.{ .temporal = 1, .height = 4, .width = 4 }},
        2,
        .{ .max_input_tokens = 32, .max_images = 4, .max_visual_tokens = 16 },
    );
    defer plan.deinit();

    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 2, 2, 2, 4, 5 }, plan.axis(0));
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 2, 3, 3, 4, 5 }, plan.axis(1));
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 3, 2, 3, 4, 5 }, plan.axis(2));
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 2, 3, 4, 5, 6, 7 }, plan.causal_positions);
    try std.testing.expectEqual(@as(i64, -2), plan.mrope_position_delta);
    try std.testing.expectEqual(@as(u32, 6), try plan.nextDecodePosition());
    try std.testing.expectEqual(@as(usize, 1), plan.visual_spans.len);
    try std.testing.expectEqual(@as(usize, 2), plan.visual_spans[0].token_offset);
    try std.testing.expectEqual(@as(usize, 4), plan.visual_spans[0].token_count);
}

test "Qwen3-VL request plan fails closed on mismatched images video and admission" {
    const allocator = std.testing.allocator;
    const limits = Limits{ .max_input_tokens = 8, .max_images = 1, .max_visual_tokens = 4 };
    try std.testing.expectError(
        error.ImageTokenCountMismatch,
        buildRequestPlan(allocator, &.{ image_modality, image_modality }, &.{.{ .temporal = 1, .height = 4, .width = 4 }}, 2, limits),
    );
    try std.testing.expectError(
        error.VideoUnsupported,
        buildRequestPlan(allocator, &.{video_modality}, &.{}, 2, limits),
    );
    try std.testing.expectError(
        error.InputTokenLimitExceeded,
        buildRequestPlan(allocator, &.{ text_modality, text_modality, text_modality }, &.{}, 2, .{ .max_input_tokens = 2, .max_images = 0, .max_visual_tokens = 0 }),
    );
}
