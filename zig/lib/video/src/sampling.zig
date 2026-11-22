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

pub fn hidden(pic: anytype) bool {
    return @hasField(@TypeOf(pic), "hidden") and pic.hidden;
}

const EditOpening = struct {
    undated: ?i64 = null,
    reference: ?i64 = null,

    fn inspect(pictures: anytype, edit: media.timeline.Edit, control: media.source.Control) !EditOpening {
        if (edit.source_duration == null) return .{};
        var latest: ?i64 = null;
        var result = EditOpening{};
        for (pictures, 0..) |pic, i| {
            if (i % 256 == 0) try control.check();
            if (hidden(pic) or pic.pts > edit.source_start) continue;
            if (pic.pts == edit.source_start) return .{ .reference = pic.pts };
            latest = @max(latest orelse pic.pts, pic.pts);
            if (@as(?u64, pic.duration)) |d| {
                if (@as(i128, pic.pts) + d > edit.source_start)
                    result.reference = @max(result.reference orelse pic.pts, pic.pts);
            } else result.undated = @max(result.undated orelse pic.pts, pic.pts);
        }
        // An implicit duration ends at the next distinct visible source PTS.
        if (result.undated != latest) result.undated = null;
        if (result.undated) |pts| result.reference = pts;
        return result;
    }

    fn intersects(self: EditOpening, pts: i64, duration: ?u64, edit: media.timeline.Edit) bool {
        const length = edit.source_duration orelse return true;
        if (pts >= @as(i128, edit.source_start) + length) return false;
        if (pts >= edit.source_start) return true;
        return if (duration) |d| @as(i128, pts) + d > edit.source_start else pts == self.undated;
    }
};

/// Return original picture ordinals in the reference processor's presentation
/// order. Keep decode preroll in the reader but omit it from an explicit edit.
pub fn referencePictures(a: std.mem.Allocator, pictures: anytype, scale: u32, edit: media.timeline.Edit, control: media.source.Control) ![]usize {
    if (scale == 0 or pictures.len == 0) return error.EmptyVideo;
    const order = try a.alloc(usize, pictures.len);
    defer a.free(order);
    var count: usize = 0;
    var bounds = Bounds{};
    const opening = try EditOpening.inspect(pictures, edit, control);
    for (pictures, 0..) |picture, id| {
        if (id % 256 == 0) try control.check();
        if (hidden(picture) or !opening.intersects(picture.pts, picture.duration, edit)) continue;
        // Reference FPS sampling counts a single opening presentation group.
        // Timestamp sampling indexes every intersecting interval for later holds.
        if (edit.source_duration != null and picture.pts < edit.source_start and picture.pts != opening.reference) continue;
        bounds.include(picture.pts, picture.duration);
        order[count] = id;
        count += 1;
    }
    if (count == 0) return error.EmptyVideo;
    const Less = struct {
        fn less(items: @TypeOf(pictures), l: usize, r: usize) bool {
            if (items[l].pts != items[r].pts) return items[l].pts < items[r].pts;
            if (@hasField(@TypeOf(items[l]), "first_packet") and items[l].first_packet != items[r].first_packet) return items[l].first_packet < items[r].first_packet;
            return l < r;
        }
    };
    std.mem.sort(usize, order[0..count], pictures, Less.less);
    const duration: i128 = edit.source_duration orelse (bounds.end(null) - bounds.first);
    const ids = try referenceOrdinals(a, count, duration, scale);
    for (ids) |*id| id.* = order[id.*];
    return ids;
}

pub fn referenceOrdinals(a: std.mem.Allocator, frames: usize, ticks: i128, scale: u32) ![]usize {
    if (ticks <= 0 or scale == 0) return error.InvalidSamplingMetadata;
    const seconds = @as(f64, @floatFromInt(ticks)) / @as(f64, @floatFromInt(scale));
    return embeddingGemma2(a, .{ .total_frames = frames, .fps = @as(f64, @floatFromInt(frames)) / seconds, .duration_seconds = seconds }, .{});
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
            if (packet.hidden or packet.pts < target or packet.pts >= interval.end) continue;
            if (chosen == null or packet.pts < packets[chosen.?].pts) chosen = i;
        }
        if (chosen) |index| {
            if (output.items.len == 0 or output.items[output.items.len - 1] != index) try output.append(allocator, index);
        }
    }
    return output.toOwnedSlice(allocator);
}

/// Native presentation policy v1. Times are nanoseconds relative to the first
/// logical presentation picture; end is exclusive. A window is one such interval.
pub const TimestampPolicy = struct {
    start_ns: i64 = 0,
    end_ns: ?i64 = null,
    step_ns: u64 = 1_000_000_000,
    max_frames: usize = 32,
    overflow: Overflow = .uniform,
    /// Select the picture displayed at each target, retaining VFR holds.
    hold: bool = false,
    pub fn validate(self: TimestampPolicy) !void {
        if (self.start_ns < 0 or self.step_ns == 0 or self.max_frames == 0 or self.max_frames > 32 or (self.end_ns != null and self.end_ns.? <= self.start_ns)) return error.InvalidSamplingPolicy;
    }
};
pub const TimedPicture = struct { pts: i64, duration: ?u64, hidden: bool = false };
/// Presentation bounds for reordered pictures, including a trailing picture
/// whose duration can be inferred from the preceding distinct timestamp.
pub const Bounds = struct {
    first: i64 = std.math.maxInt(i64),
    latest: ?i64 = null,
    previous: ?i64 = null,
    last: i128 = std.math.minInt(i128),
    unknown_tail: bool = false,

    pub fn include(self: *Bounds, pts: i64, duration: ?u64) void {
        self.first = @min(self.first, pts);
        self.last = @max(self.last, @as(i128, pts) + (duration orelse 0));
        if (self.latest == null or pts > self.latest.?) {
            self.previous = self.latest;
            self.latest = pts;
            self.unknown_tail = duration == null;
        } else if (pts == self.latest.?) {
            self.unknown_tail = self.unknown_tail or duration == null;
        } else if (self.previous == null or pts > self.previous.?) self.previous = pts;
    }

    pub fn end(self: Bounds, declared_end: ?i64) i128 {
        if (self.unknown_tail) {
            if (declared_end) |stop| return @max(self.last, stop);
            if (self.previous) |before| return @max(self.last, 2 * @as(i128, self.latest.?) - before);
        }
        return self.last;
    }
};
/// Immutable presentation index. Build once per source; selections are O(K log N).
/// The augmented tree retains overlapping holds and stable decode-order ties.
pub const Timeline = struct {
    const Entry = struct { id: usize, pts: i64, stop: i128 };
    allocator: std.mem.Allocator,
    entries: []Entry,
    maxima: []i128,
    base: usize,
    first: i64,
    last: i128,
    edited: bool,
    pub fn storageBytes(count: usize) !usize {
        const base = try std.math.ceilPowerOfTwo(usize, @max(@as(usize, 1), count));
        return std.math.add(usize, try std.math.mul(usize, count, @sizeOf(Entry)), try std.math.mul(usize, base, 2 * @sizeOf(i128)));
    }
    pub fn init(a: std.mem.Allocator, pictures: []const TimedPicture, control: media.source.Control) !Timeline {
        return initEdited(a, pictures, control, .{});
    }

    /// Preserve original logical picture IDs for decoding preroll while indexing
    /// pictures that start inside the edit or remain displayed across its start.
    pub fn initEdited(a: std.mem.Allocator, pictures: []const TimedPicture, control: media.source.Control, edit: media.timeline.Edit) !Timeline {
        const stop: ?i128 = if (edit.source_duration) |d| @as(i128, edit.source_start) + d else null;
        const opening = try EditOpening.inspect(pictures, edit, control);
        const count = blk: {
            var n: usize = 0;
            for (pictures, 0..) |pic, i| {
                if (i % 256 == 0) try control.check();
                if (!hidden(pic) and opening.intersects(pic.pts, pic.duration, edit)) n += 1;
            }
            break :blk n;
        };
        if (count == 0) return error.EmptyVideo;
        const entries = try a.alloc(Entry, count);
        errdefer a.free(entries);
        var bounds = Bounds{};
        var i: usize = 0;
        for (pictures, 0..) |picture, id| {
            if (id % 256 == 0) try control.check();
            if (hidden(picture) or !opening.intersects(picture.pts, picture.duration, edit)) continue;
            bounds.include(picture.pts, picture.duration);
            entries[i] = .{ .id = id, .pts = picture.pts, .stop = @min(stop orelse std.math.maxInt(i128), if (picture.duration) |d| @as(i128, picture.pts) + d else std.math.maxInt(i128)) };
            i += 1;
        }
        std.mem.sort(Entry, entries, {}, struct {
            fn less(_: void, x: Entry, y: Entry) bool {
                return x.pts < y.pts or (x.pts == y.pts and x.id > y.id);
            }
        }.less);
        // Missing durations end at the next distinct presentation time.
        var group_end = entries.len;
        var next_pts: i128 = stop orelse std.math.maxInt(i128);
        while (group_end != 0) {
            try control.check();
            const pts = entries[group_end - 1].pts;
            while (group_end != 0 and entries[group_end - 1].pts == pts) {
                group_end -= 1;
                if (pictures[entries[group_end].id].duration == null) entries[group_end].stop = next_pts;
            }
            next_pts = pts;
        }
        try control.check();
        const base = try std.math.ceilPowerOfTwo(usize, entries.len);
        const maxima = try a.alloc(i128, try std.math.mul(usize, base, 2));
        @memset(maxima, std.math.minInt(i128));
        for (entries, 0..) |entry, index| maxima[base + index] = entry.stop;
        var node = base;
        while (node > 1) {
            node -= 1;
            maxima[node] = @max(maxima[node * 2], maxima[node * 2 + 1]);
        }
        return .{ .allocator = a, .entries = entries, .maxima = maxima, .base = base, .edited = stop != null, .first = if (stop != null) edit.source_start else bounds.first, .last = stop orelse bounds.end(null) };
    }
    pub fn deinit(self: *Timeline) void {
        self.allocator.free(self.entries);
        self.allocator.free(self.maxima);
    }
    fn active(self: *const Timeline, node: usize, lo: usize, hi: usize, bound: usize, target: i128) ?usize {
        if (lo >= bound or self.maxima[node] <= target) return null;
        if (hi - lo == 1) return lo;
        const mid = lo + (hi - lo) / 2;
        return self.active(node * 2 + 1, mid, hi, bound, target) orelse self.active(node * 2, lo, mid, bound, target);
    }
    pub fn select(self: *const Timeline, a: std.mem.Allocator, timescale: u32, policy: TimestampPolicy, control: media.source.Control, relative: bool) ![]usize {
        try policy.validate();
        if (timescale == 0) return error.EmptyVideo;
        const origin: i128 = if (relative) self.first else 0;
        const inferred = @divFloor((self.last - origin) * 1_000_000_000 + timescale - 1, timescale);
        const end: i128 = policy.end_ns orelse (std.math.cast(i64, inferred) orelse return error.TimestampOverflow);
        const start_ticks = @as(i128, policy.start_ns) * timescale;
        const stop_ticks = end * timescale;
        const begin = if (self.edited) @max(start_ticks, (@as(i128, self.first) - origin) * std.time.ns_per_s) else start_ticks;
        const stop = if (self.edited) @min(stop_ticks, (self.last - origin) * std.time.ns_per_s) else stop_ticks;
        if (stop <= begin) return error.EmptyVideoWindow;
        // Preserve the request's cadence and include a held edit boundary once.
        // Apply the uniform cap only after discarding out-of-edit targets.
        const step_ticks = @as(i128, policy.step_ns) * timescale;
        const first_slot: u128 = @intCast(@divFloor(begin - start_ticks, step_ticks));
        const after_last: u128 = @intCast(@divFloor(stop - start_ticks - 1, step_ticks) + 1);
        const candidates = after_last - first_slot;
        const count: usize = @intCast(@min(candidates, policy.max_frames));
        var output: std.ArrayList(usize) = .empty;
        errdefer output.deinit(a);
        for (0..count) |i| {
            try control.check();
            const ordinal: u128 = if (policy.overflow == .uniform and count > 1 and candidates > count) @as(u128, i) * (candidates - 1) / (count - 1) else i;
            const requested = start_ticks + @as(i128, @intCast(first_slot + ordinal)) * step_ticks;
            const target = origin * std.time.ns_per_s + @max(begin, requested);
            var lo: usize = 0;
            var hi = self.entries.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (@as(i128, self.entries[mid].pts) * 1_000_000_000 <= target) lo = mid + 1 else hi = mid;
            }
            var chosen: ?usize = null;
            if (policy.hold or (self.edited and target == @as(i128, self.first) * std.time.ns_per_s)) chosen = self.active(1, 0, self.base, lo, @divFloor(target, 1_000_000_000));
            if (chosen == null) {
                lo = 0;
                hi = self.entries.len;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (@as(i128, self.entries[mid].pts) * 1_000_000_000 < target) lo = mid + 1 else hi = mid;
                }
                if (lo < self.entries.len and (@as(i128, self.entries[lo].pts) - origin) * 1_000_000_000 < end * timescale) {
                    const pts = self.entries[lo].pts;
                    hi = self.entries.len;
                    while (lo < hi) {
                        const mid = lo + (hi - lo) / 2;
                        if (self.entries[mid].pts <= pts) lo = mid + 1 else hi = mid;
                    }
                    chosen = lo - 1;
                }
            }
            if (chosen) |index| {
                const id = self.entries[index].id;
                if (output.items.len == 0 or output.items[output.items.len - 1] != id) try output.append(a, id);
            }
        }
        if (output.items.len == 0) return error.EmptyVideoWindow;
        return output.toOwnedSlice(a);
    }
};

/// Select logical IDs with exact rational time comparisons. A sparse VFR source
/// can map multiple targets to one picture; duplicates are omitted. Uniform caps
/// span the entire interval without allocating every target of a long video.
pub fn cappedTimestamps(a: std.mem.Allocator, pictures: []const TimedPicture, timescale: u32, policy: TimestampPolicy, control: media.source.Control) ![]usize {
    return cappedAtOrigin(a, pictures, timescale, policy, control, true);
}
/// Absolute source-clock selection, preserving rational tick comparisons. This
/// is independent of which preroll pictures remain retained in a live window.
pub fn cappedSourceTimestamps(a: std.mem.Allocator, pictures: []const TimedPicture, timescale: u32, policy: TimestampPolicy, control: media.source.Control) ![]usize {
    return cappedAtOrigin(a, pictures, timescale, policy, control, false);
}
fn cappedAtOrigin(a: std.mem.Allocator, pictures: []const TimedPicture, timescale: u32, policy: TimestampPolicy, control: media.source.Control, relative: bool) ![]usize {
    try policy.validate();
    if (pictures.len == 0 or timescale == 0) return error.EmptyVideo;
    var bounds = Bounds{};
    for (pictures, 0..) |picture, i| {
        if (i % 256 == 0) try control.check();
        if (picture.hidden) continue;
        bounds.include(picture.pts, picture.duration);
    }
    if (bounds.latest == null) return error.EmptyVideo;
    const first = if (relative) bounds.first else 0;
    const last = bounds.end(null);
    const inferred = @divFloor((last - first) * 1_000_000_000 + timescale - 1, timescale);
    const end: i128 = policy.end_ns orelse (std.math.cast(i64, inferred) orelse return error.TimestampOverflow);
    if (end <= policy.start_ns) return error.EmptyVideoWindow;
    const candidates: u128 = @intCast(@divFloor(end - policy.start_ns + policy.step_ns - 1, policy.step_ns));
    const count: usize = @intCast(@min(candidates, policy.max_frames));
    var output: std.ArrayList(usize) = .empty;
    errdefer output.deinit(a);
    for (0..count) |i| {
        try control.check();
        const ordinal: u128 = if (policy.overflow == .uniform and count > 1 and candidates > count) @as(u128, i) * (candidates - 1) / (count - 1) else i;
        const target: i128 = policy.start_ns + @as(i128, @intCast(ordinal * policy.step_ns));
        var chosen: ?usize = null;
        if (policy.hold) {
            var latest: ?i64 = null;
            var unknown: ?usize = null;
            for (pictures, 0..) |picture, id| {
                if (id % 256 == 0) try control.check();
                if (picture.hidden) continue;
                const time = (@as(i128, picture.pts) - first) * std.time.ns_per_s;
                if (time > target * timescale) continue;
                latest = @max(latest orelse picture.pts, picture.pts);
                if (picture.duration) |duration| {
                    if (time + @as(i128, duration) * std.time.ns_per_s <= target * timescale) continue;
                    if (chosen == null or picture.pts > pictures[chosen.?].pts) chosen = id;
                } else if (unknown == null or picture.pts > pictures[unknown.?].pts) unknown = id;
            }
            if (unknown) |id| {
                const pts = pictures[id].pts;
                if (pts == latest.? and (chosen == null or pts > pictures[chosen.?].pts or (pts == pictures[chosen.?].pts and id < chosen.?))) chosen = id;
            }
        }
        if (chosen != null) {
            const id = chosen.?;
            if (output.items.len == 0 or output.items[output.items.len - 1] != id) try output.append(a, id);
            continue;
        }
        for (pictures, 0..) |picture, id| {
            if (id % 256 == 0) try control.check();
            if (picture.hidden) continue;
            const time = (@as(i128, picture.pts) - first) * 1_000_000_000;
            if (time < target * timescale or time >= end * timescale) continue;
            if (chosen == null or picture.pts < pictures[chosen.?].pts) chosen = id;
        }
        if (chosen) |id| if (output.items.len == 0 or output.items[output.items.len - 1] != id) try output.append(a, id);
    }
    if (output.items.len == 0) return error.EmptyVideoWindow;
    return output.toOwnedSlice(a);
}

test "presentation policy samples logical VFR windows and uniformly caps long videos" {
    const a = std.testing.allocator;
    const pictures = [_]TimedPicture{ .{ .pts = 100, .duration = 10 }, .{ .pts = 105, .duration = 5 }, .{ .pts = 140, .duration = 10 }, .{ .pts = 150, .duration = 10 } };
    const selected = try cappedTimestamps(a, &pictures, 10, .{ .start_ns = 500_000_000, .end_ns = 5_000_000_000 }, .{});
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, selected);
    var long: [100]TimedPicture = undefined;
    for (&long, 0..) |*picture, i| picture.* = .{ .pts = @intCast(i), .duration = 1 };
    const capped = try cappedTimestamps(a, &long, 1, .{}, .{});
    defer a.free(capped);
    try std.testing.expectEqual(@as(usize, 32), capped.len);
    try std.testing.expectEqual(@as(usize, 0), capped[0]);
    try std.testing.expectEqual(@as(usize, 99), capped[31]);
    const window = try cappedTimestamps(a, &long, 1, .{ .start_ns = 60_000_000_000, .end_ns = 90_000_000_000 }, .{});
    defer a.free(window);
    try std.testing.expectEqual(@as(usize, 60), window[0]);
    try std.testing.expectEqual(@as(usize, 89), window[29]);
    try std.testing.expectError(error.InvalidSamplingPolicy, cappedTimestamps(a, &long, 1, .{ .step_ns = 0 }, .{}));
    try std.testing.expectError(error.EmptyVideoWindow, cappedTimestamps(a, &long, 1, .{ .start_ns = 101_000_000_000 }, .{}));
}

test "timestamp windows infer unknown trailing duration and keep exact rational boundaries" {
    const a = std.testing.allocator;
    // Unsorted decode order, tied PTS, and absent WebM block durations.
    const pictures = [_]TimedPicture{ .{ .pts = 3003, .duration = null }, .{ .pts = 0, .duration = null }, .{ .pts = 2002, .duration = null }, .{ .pts = 1001, .duration = null }, .{ .pts = 2002, .duration = null } };
    const selected = try cappedTimestamps(a, &pictures, 30000, .{ .step_ns = 33_366_667 }, .{});
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2, 0 }, selected);
    // End is exclusive even when it lands exactly on a timestamp.
    const excluded = try cappedTimestamps(a, &pictures, 30000, .{ .end_ns = 100_100_000, .step_ns = 50_000_000 }, .{});
    defer a.free(excluded);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, excluded);
    const lone = [_]TimedPicture{.{ .pts = 5, .duration = null }};
    try std.testing.expectError(error.EmptyVideoWindow, cappedTimestamps(a, &lone, 1, .{}, .{}));
    const bounded = try cappedTimestamps(a, &lone, 1, .{ .end_ns = 1 }, .{});
    defer a.free(bounded);
    try std.testing.expectEqualSlices(usize, &.{0}, bounded);
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

test "source timestamp sampling retains rational VFR boundaries across preroll eviction" {
    const a = std.testing.allocator;
    const frames = [_]TimedPicture{ .{ .pts = 1, .duration = 1 }, .{ .pts = 2, .duration = 1 }, .{ .pts = 4, .duration = 1 } };
    const policy = TimestampPolicy{ .start_ns = 1_333_333_333, .end_ns = 1_666_666_667, .step_ns = 1_000_000_000 };
    const all = try cappedSourceTimestamps(a, &frames, 3, policy, .{});
    defer a.free(all);
    const tail = try cappedSourceTimestamps(a, frames[1..], 3, policy, .{});
    defer a.free(tail);
    try std.testing.expectEqualSlices(usize, &.{2}, all);
    try std.testing.expectEqualSlices(usize, &.{1}, tail);
}

test "model aware media VFR sampling retains displayed pictures across window boundaries" {
    const a = std.testing.allocator;
    const pictures = [_]TimedPicture{ .{ .pts = 0, .duration = 60 }, .{ .pts = 60, .duration = 2 } };
    const held = try cappedTimestamps(a, &pictures, 1, .{ .start_ns = 30_000_000_000, .end_ns = 32_000_000_000, .hold = true }, .{});
    defer a.free(held);
    try std.testing.expectEqualSlices(usize, &.{0}, held);
    try std.testing.expectError(error.EmptyVideoWindow, cappedTimestamps(a, &pictures, 1, .{ .start_ns = 30_000_000_000, .end_ns = 32_000_000_000 }, .{}));
    const boundary = try cappedTimestamps(a, &pictures, 1, .{ .start_ns = 59_000_000_000, .end_ns = 61_000_000_000, .hold = true }, .{});
    defer a.free(boundary);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, boundary);
}

test "indexed timestamp selection matches linear oracle for reordered overlapping VFR and unknown durations" {
    const a = std.testing.allocator;
    var rng = std.Random.DefaultPrng.init(87931);
    const random = rng.random();
    var pictures: [73]TimedPicture = undefined;
    for (0..100) |_| {
        for (&pictures) |*picture| picture.* = .{ .pts = random.intRangeLessThan(i64, -20, 100), .duration = if (random.boolean()) random.intRangeLessThan(u64, 0, 40) else null };
        var timeline = try Timeline.init(a, &pictures, .{});
        defer timeline.deinit();
        for ([_]bool{ false, true }) |hold| for ([_]bool{ false, true }) |relative| for ([_]Overflow{ .uniform, .truncate }) |overflow| {
            const policy = TimestampPolicy{ .start_ns = random.intRangeLessThan(i64, 0, 20_000_000_000), .end_ns = 50_000_000_000, .step_ns = 123456789, .max_frames = 32, .hold = hold, .overflow = overflow };
            const expected = cappedAtOrigin(a, &pictures, 3, policy, .{}, relative) catch |err| {
                try std.testing.expectError(err, timeline.select(a, 3, policy, .{}, relative));
                continue;
            };
            defer a.free(expected);
            const actual = try timeline.select(a, 3, policy, .{}, relative);
            defer a.free(actual);
            try std.testing.expectEqualSlices(usize, expected, actual);
        };
    }
}
fn timelineAllocationCheck(a: std.mem.Allocator) !void {
    const pictures = [_]TimedPicture{ .{ .pts = -3, .duration = 2 }, .{ .pts = 1, .duration = null }, .{ .pts = -3, .duration = 40 } };
    var timeline = try Timeline.init(a, &pictures, .{});
    defer timeline.deinit();
    const selected = try timeline.select(a, 3, .{ .hold = true }, .{}, true);
    defer a.free(selected);
}
test "indexed timestamp ownership releases every partial allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, timelineAllocationCheck, .{});
}

test "source timestamp sampling preserves edit origin and visible logical IDs" {
    const a = std.testing.allocator;
    var timeline = try Timeline.initEdited(a, &.{ .{ .pts = 0, .duration = 12 }, .{ .pts = 20, .duration = 4 }, .{ .pts = 12, .duration = 4 }, .{ .pts = 25, .duration = 10 } }, .{}, .{ .source_start = 10, .source_duration = 15 });
    defer timeline.deinit();
    try std.testing.expectEqual(@as(i64, 10), timeline.first);
    try std.testing.expectEqual(@as(i128, 25), timeline.last);
    for ([_]bool{ true, false }) |hold| {
        try std.testing.expectError(error.EmptyVideoWindow, timeline.select(a, 1, .{ .start_ns = 0, .end_ns = 5 * std.time.ns_per_s, .hold = hold }, .{}, false));
        const crossed = try timeline.select(a, 1, .{ .start_ns = 9 * std.time.ns_per_s, .end_ns = 11 * std.time.ns_per_s, .hold = hold }, .{}, false);
        defer a.free(crossed);
        try std.testing.expectEqualSlices(usize, &.{0}, crossed);
    }
    const ids = try timeline.select(a, 10, .{ .step_ns = std.time.ns_per_s, .hold = true }, .{}, true);
    defer a.free(ids);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, ids);
}

test "source timestamp sampling never revives a superseded undated picture" {
    const a = std.testing.allocator;
    const pictures = [_]TimedPicture{ .{ .pts = 6, .duration = 2 }, .{ .pts = 0, .duration = null }, .{ .pts = 2, .duration = 1 } };
    const policy = TimestampPolicy{ .end_ns = 4 * std.time.ns_per_s, .hold = true };
    var timeline = try Timeline.init(a, &pictures, .{});
    defer timeline.deinit();
    const indexed = try timeline.select(a, 2, policy, .{}, false);
    defer a.free(indexed);
    const linear = try cappedSourceTimestamps(a, &pictures, 2, policy, .{});
    defer a.free(linear);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2, 0 }, indexed);
    try std.testing.expectEqualSlices(usize, indexed, linear);
}

test "reference and indexed edit sampling retain tied undated outputs until the next distinct PTS" {
    const a = std.testing.allocator;
    const pictures = [_]TimedPicture{ .{ .pts = 2, .duration = null }, .{ .pts = 2, .duration = null }, .{ .pts = 8, .duration = 2 } };
    for ([_]i64{ 4, 8 }) |begin| {
        const edit = media.timeline.Edit{ .source_start = begin, .source_duration = 2 };
        const selected = try referencePictures(a, &pictures, 1, edit, .{});
        defer a.free(selected);
        var timeline = try Timeline.initEdited(a, &pictures, .{}, edit);
        defer timeline.deinit();
        if (begin == 4) {
            try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, selected);
            try std.testing.expectEqual(2, timeline.entries.len);
        } else {
            try std.testing.expectEqualSlices(usize, &.{ 2, 2 }, selected);
            try std.testing.expectEqual(1, timeline.entries.len);
        }
    }
}

test "edited timestamp selection preserves overlapping holds after the opening picture expires" {
    const a = std.testing.allocator;
    const pictures = [_]TimedPicture{
        .{ .pts = 0, .duration = null },
        .{ .pts = 0, .duration = 10 },
        .{ .pts = 4, .duration = 2 },
    };
    for ([_]i64{ 4, 5 }) |start| {
        var timeline = try Timeline.initEdited(a, &pictures, .{}, .{ .source_start = start, .source_duration = 10 - start });
        defer timeline.deinit();
        const ids = try timeline.select(a, 1, .{ .start_ns = 5 * std.time.ns_per_s, .end_ns = 7 * std.time.ns_per_s, .hold = true }, .{}, false);
        defer a.free(ids);
        try std.testing.expectEqualSlices(usize, &.{ 2, 1 }, ids);
    }
}
