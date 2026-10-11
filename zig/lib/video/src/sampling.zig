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

const std = @import("std");
const media = @import("antfly_media");
pub const Overflow = enum { uniform, truncate };
pub const Metadata = struct { total_frames: usize, fps: ?f64 = null, duration_seconds: ?f64 = null };
pub const Options = struct { fps: ?f64 = 1, max_frames: ?usize = 32, overflow: ?Overflow = .uniform, max_output: usize = 4096 };

/// Reproduces EmbeddingGemma2VideoProcessor.sample_frames index/FPS semantics.
/// Missing rate/duration disables FPS sampling; the configured cap still applies.
/// Allocate only the capped output, never every FPS candidate in a long source.
/// Caller must pin this policy with its processor revision; VFR timestamp-based
/// selection is a distinct API below, not a replacement for upstream parity.
pub fn embeddingGemma2(allocator: std.mem.Allocator, metadata: Metadata, options: Options) ![]usize {
    if (metadata.total_frames == 0) return error.EmptyVideo;
    if (options.max_output == 0 or (options.max_frames != null and options.max_frames.? == 0)) return error.InvalidSamplingPolicy;
    if (options.overflow != null and options.max_frames == null) return error.InvalidSamplingPolicy;
    if (options.fps) |fps| if (!std.math.isFinite(fps) or fps <= 0) return error.InvalidSamplingPolicy;
    if (metadata.fps) |fps| if (!std.math.isFinite(fps) or fps <= 0) return error.InvalidSamplingMetadata;
    if (metadata.duration_seconds) |duration| if (!std.math.isFinite(duration) or duration < 0) return error.InvalidSamplingMetadata;
    const rate_based = options.fps != null and metadata.fps != null and metadata.duration_seconds != null;
    var candidates = metadata.total_frames;
    var step: f64 = 1;
    if (rate_based) {
        const raw = metadata.duration_seconds.? * options.fps.?;
        if (!std.math.isFinite(raw) or raw >= 9007199254740992.0 or raw >= @as(f64, @floatFromInt(std.math.maxInt(usize)))) return error.SamplingOverflow;
        candidates = @max(1, @as(usize, @intFromFloat(raw)));
        step = metadata.fps.? / options.fps.?;
        if (!std.math.isFinite(step)) return error.SamplingOverflow;
    }
    if (@as(u64, candidates) > 9007199254740991 or @as(u64, metadata.total_frames) > 9007199254740991) return error.SamplingOverflow;
    const count = if (options.overflow != null) @min(candidates, options.max_frames.?) else candidates;
    if (count > options.max_output) return error.ResourceLimitExceeded;
    const output = try allocator.alloc(usize, count);
    for (output, 0..) |*index, i| {
        // NumPy linspace computes the step first and fixes the final endpoint.
        const candidate = if (count < candidates and options.overflow == .uniform and count > 1)
            if (i == count - 1) candidates - 1 else @as(usize, @intFromFloat(@as(f64, @floatFromInt(i)) * (@as(f64, @floatFromInt(candidates - 1)) / @as(f64, @floatFromInt(count - 1)))))
        else
            i;
        const frame = @as(f64, @floatFromInt(candidate)) * step;
        index.* = if (frame >= @as(f64, @floatFromInt(metadata.total_frames - 1))) metadata.total_frames - 1 else @intFromFloat(frame);
    }
    return output;
}

pub const Interval = struct { start: i64, end: i64 };
/// Native timestamp policy v1: each tick target chooses the earliest presented
/// picture at or after that target in [start,end); duplicate IDs are omitted.
/// Stable ties use decode index. This deliberately differs from HF FPS sampling.
/// Returns decode indexes in presentation order, preserving VFR and edit mapping.
pub fn timestamps(allocator: std.mem.Allocator, packets: []const media.mp4.Packet, interval: Interval, step: u64, max_output: usize, control: media.source.Control) ![]usize {
    if (interval.start >= interval.end or step == 0 or max_output == 0) return error.InvalidSamplingPolicy;
    const span: u128 = @intCast(@as(i128, interval.end) - interval.start);
    if ((span + step - 1) / step > max_output) return error.ResourceLimitExceeded;
    var output: std.ArrayList(usize) = .empty;
    errdefer output.deinit(allocator);
    var target: i128 = interval.start;
    while (target < interval.end) : (target += step) {
        try control.check();
        var chosen: ?usize = null;
        for (packets, 0..) |packet, i| {
            if (i % 256 == 0) try control.check();
            if (packet.pts < target or packet.pts >= interval.end) continue;
            if (chosen == null or packet.pts < packets[chosen.?].pts) chosen = i;
        }
        if (chosen) |index| {
            if (output.items.len == 0 or output.items[output.items.len - 1] != index) try output.append(allocator, index);
        }
    }
    return output.toOwnedSlice(allocator);
}

test "sampling uniform cap spans the video rather than its first 32 seconds" {
    const a = std.testing.allocator;
    const frames = try embeddingGemma2(a, .{ .total_frames = 1800, .fps = 30, .duration_seconds = 60 }, .{});
    defer a.free(frames);
    try std.testing.expectEqual(@as(usize, 32), frames.len);
    try std.testing.expectEqual(@as(usize, 0), frames[0]);
    try std.testing.expectEqual(@as(usize, 1770), frames[31]);
    const short = try embeddingGemma2(a, .{ .total_frames = 15, .fps = 30, .duration_seconds = 0.5 }, .{});
    defer a.free(short);
    try std.testing.expectEqualSlices(usize, &.{0}, short);
    const missing = try embeddingGemma2(a, .{ .total_frames = 5 }, .{ .max_frames = 3 });
    defer a.free(missing);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2, 4 }, missing);
    try std.testing.expectError(error.EmptyVideo, embeddingGemma2(a, .{ .total_frames = 0 }, .{}));
    try std.testing.expectError(error.InvalidSamplingPolicy, embeddingGemma2(a, .{ .total_frames = 5 }, .{ .fps = 0 }));
    try std.testing.expectError(error.ResourceLimitExceeded, embeddingGemma2(a, .{ .total_frames = 5 }, .{ .max_output = 2 }));
}

test "native timestamp selection respects reordered PTS, gaps and half-open interval" {
    var packets = [_]media.mp4.Packet{
        .{ .offset = 0, .size = 1, .dts = 0, .media_pts = 0, .pts = 0, .duration = 1, .sync = true },
        .{ .offset = 0, .size = 1, .dts = 1, .media_pts = 3, .pts = 3, .duration = 1, .sync = false },
        .{ .offset = 0, .size = 1, .dts = 2, .media_pts = 1, .pts = 1, .duration = 1, .sync = false },
    };
    const selected = try timestamps(std.testing.allocator, &packets, .{ .start = 0, .end = 3 }, 1, 3, .{});
    defer std.testing.allocator.free(selected);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2 }, selected);
}

/// Logical picture metadata, independent of packet/slice/field transport. The
/// decoder/indexer supplies one entry for each displayed picture.
pub const Picture = struct { id: usize, pts: i64, duration: u64 };
pub const GapPolicy = enum { skip, hold_previous };
pub const PresentationOptions = struct {
    step: u64,
    max_output: usize = 4096,
    gaps: GapPolicy = .skip,
    preserve_repeats: bool = false,
};

/// Samples pictures active at each timestamp, including one starting before the
/// window. Unlike timestamps(), this never selects a future frame across a gap.
/// Unsorted input and overlapping durations are valid; the most recent active
/// picture wins, with stable input order resolving equal timestamps.
pub fn presentation(allocator: std.mem.Allocator, pictures: []const Picture, interval: Interval, options: PresentationOptions, control: media.source.Control) ![]usize {
    if (interval.end <= interval.start or options.step == 0 or options.max_output == 0) return error.InvalidSamplingPolicy;
    const span: u128 = @intCast(@as(i128, interval.end) - interval.start);
    if ((span + options.step - 1) / options.step > options.max_output) return error.ResourceLimitExceeded;
    for (pictures) |picture| if (picture.duration == 0) return error.InvalidSamplingMetadata;
    var output: std.ArrayList(usize) = .empty;
    errdefer output.deinit(allocator);
    var target: i128 = interval.start;
    while (target < interval.end) : (target += options.step) {
        try control.check();
        var active: ?usize = null;
        var previous: ?usize = null;
        for (pictures, 0..) |picture, i| {
            if (i % 256 == 0) try control.check();
            if (picture.pts > target) continue;
            if (previous == null or picture.pts > pictures[previous.?].pts) previous = i;
            if (target >= @as(i128, picture.pts) + picture.duration) continue;
            if (active == null or picture.pts > pictures[active.?].pts) active = i;
        }
        const chosen = active orelse if (options.gaps == .hold_previous) previous orelse continue else continue;
        const id = pictures[chosen].id;
        if (options.preserve_repeats or output.items.len == 0 or output.items[output.items.len - 1] != id) try output.append(allocator, id);
    }
    return output.toOwnedSlice(allocator);
}

test "presentation sampling handles VFR gaps overlapping pictures and repeated cadence" {
    const a = std.testing.allocator;
    const pictures = [_]Picture{
        .{ .id = 12, .pts = 8, .duration = 2 },
        .{ .id = 10, .pts = 0, .duration = 4 },
        .{ .id = 11, .pts = 2, .duration = 1 },
    };
    const skipped = try presentation(a, &pictures, .{ .start = 1, .end = 10 }, .{ .step = 1 }, .{});
    defer a.free(skipped);
    try std.testing.expectEqualSlices(usize, &.{ 10, 11, 10, 12 }, skipped);
    const held = try presentation(a, &pictures, .{ .start = 1, .end = 10 }, .{ .step = 1, .gaps = .hold_previous, .preserve_repeats = true }, .{});
    defer a.free(held);
    try std.testing.expectEqualSlices(usize, &.{ 10, 11, 10, 11, 11, 11, 11, 12, 12 }, held);
    try std.testing.expectError(error.ResourceLimitExceeded, presentation(a, &pictures, .{ .start = std.math.minInt(i64), .end = std.math.maxInt(i64) }, .{ .step = 1 }, .{}));
    try std.testing.expectError(error.InvalidSamplingMetadata, presentation(a, &.{.{ .id = 0, .pts = 0, .duration = 0 }}, .{ .start = 0, .end = 1 }, .{ .step = 1 }, .{}));
}
