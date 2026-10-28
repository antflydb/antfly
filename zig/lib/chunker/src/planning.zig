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

//! Model-aware, allocation-bounded chunk planning. Adapters own source materialization.
const std = @import("std");
pub const Kind = enum { text, pdf, image, gif, audio, video };
pub const PictureBasis = enum { logical, packet_assembly };
pub const Coordinates = struct {
    kind: Kind,
    start_byte: ?u64 = null,
    end_byte: ?u64 = null,
    page: ?u32 = null,
    region: ?[4]u32 = null,
    raster_width: ?u32 = null,
    raster_height: ?u32 = null,
    start_ns: ?i64 = null,
    end_ns: ?i64 = null,
    first_picture: ?u64 = null,
    picture_count: ?u32 = null,
    selected_pictures: ?[]const u64 = null,
    picture_basis: ?PictureBasis = null,
    start_sample: ?u64 = null,
    end_sample: ?u64 = null,
    sample_rate: ?u32 = null,
    start_tick: ?i64 = null,
    end_tick: ?i64 = null,
    timescale: ?u32 = null,
    page_box: ?[4]f64 = null,
    page_rotation: ?i32 = null,
    speaker_index: ?u32 = null,

    pub fn validate(self: Coordinates) !void {
        if (self.speaker_index) |index| if (self.kind != .audio or index >= 64) return error.InvalidChunkCoordinates;
        if ((self.raster_width == null) != (self.raster_height == null)) return error.InvalidChunkCoordinates;
        if (self.raster_width) |width| if (width == 0 or self.raster_height.? == 0) return error.InvalidChunkCoordinates;
        if (self.region) |box| {
            if (box[0] >= box[2] or box[1] >= box[3]) return error.InvalidChunkCoordinates;
            if (self.raster_width) |width| if (box[2] > width or box[3] > self.raster_height.?) return error.InvalidChunkCoordinates;
        }
        if ((self.start_byte == null) != (self.end_byte == null)) return error.InvalidChunkCoordinates;
        if (self.start_byte) |start| if (start >= self.end_byte.?) return error.InvalidChunkCoordinates;
        if ((self.start_ns == null) != (self.end_ns == null)) return error.InvalidChunkCoordinates;
        if (self.start_ns) |start| if (start < 0 or start > self.end_ns.? or (self.kind != .gif and start == self.end_ns.?)) return error.InvalidChunkCoordinates;
        if ((self.start_sample == null) != (self.end_sample == null)) return error.InvalidChunkCoordinates;
        if (self.start_sample) |start| if (start >= self.end_sample.? or self.sample_rate == null or self.sample_rate.? == 0) return error.InvalidChunkCoordinates;
        if ((self.start_tick == null) != (self.end_tick == null)) return error.InvalidChunkCoordinates;
        if (self.start_tick) |start| if (start >= self.end_tick.? or self.timescale == null or self.timescale.? == 0) return error.InvalidChunkCoordinates;
        if (self.page) |page| if (page == 0) return error.InvalidChunkCoordinates;
        if (self.page_box) |box| {
            for (box) |number| if (!std.math.isFinite(number)) return error.InvalidChunkCoordinates;
            if (box[0] >= box[2] or box[1] >= box[3]) return error.InvalidChunkCoordinates;
        }
        if (self.selected_pictures) |pictures| {
            if (pictures.len == 0 or pictures.len > 32 or self.picture_basis == null) return error.InvalidChunkCoordinates;
            if (self.picture_count) |count| if (count != pictures.len) return error.InvalidChunkCoordinates;
        }
    }
};
pub const Descriptor = struct {
    ordinal: u64,
    coordinates: Coordinates,
    input_tokens: usize,
};
pub const Model = struct {
    /// Identity of tokenizer, wrappers, geometry and supported input limits.
    planning_identity: []const u8,
    context_tokens: usize,
    max_text_bytes: usize = 16 * 1024 * 1024,
    reserved_tokens: usize = 0,
    max_frames: usize = 32,
    max_audio_ns: i64 = 30 * std.time.ns_per_s,
    min_audio_ns: i64 = 0,
    pub fn capacity(self: Model) !usize {
        if (self.planning_identity.len == 0 or self.context_tokens == 0 or self.max_text_bytes == 0 or self.reserved_tokens >= self.context_tokens or self.max_frames == 0 or self.max_frames > 4096 or self.max_audio_ns <= 0 or self.min_audio_ns < 0 or self.min_audio_ns > self.max_audio_ns) return error.InvalidChunkModel;
        return self.context_tokens - self.reserved_tokens;
    }
};
pub const Policy = struct {
    max_text_bytes: usize = 1024 * 1024,
    target_tokens: usize = 500,
    overlap_tokens: usize = 50,
    window_ns: i64 = 32 * std.time.ns_per_s,
    overlap_ns: i64 = 0,
    step_ns: i64 = std.time.ns_per_s,
    max_frames: usize = 32,
    video_hold: u8 = 1,
    /// Zero means a single prepared image; tiling is explicit.
    tile_width: u32 = 0,
    tile_height: u32 = 0,
    tile_overlap: u32 = 0,
    audio_speaker_turns: bool = false,
    audio_pause_ns: i64 = 0,
    audio_pause_search_ns: i64 = 5 * std.time.ns_per_s,
    audio_silence_milli: u16 = 10,
    pub fn validate(self: Policy) !void {
        if (self.audio_speaker_turns and (self.overlap_ns != 0 or self.audio_pause_ns != 0)) return error.InvalidChunkPolicy;
        if (self.video_hold > 1) return error.InvalidChunkPolicy;
        if (self.max_text_bytes < 4 or self.max_text_bytes > 16 * 1024 * 1024) return error.InvalidChunkPolicy;
        if (self.audio_pause_ns < 0 or self.audio_pause_search_ns < 0 or self.audio_silence_milli > 1000) return error.InvalidChunkPolicy;
        if (self.target_tokens == 0 or self.target_tokens > 1_000_000 or self.overlap_tokens >= self.target_tokens or self.window_ns <= 0 or self.overlap_ns < 0 or self.overlap_ns >= self.window_ns or self.step_ns <= 0 or self.max_frames == 0 or self.max_frames > 4096) return error.InvalidChunkPolicy;
        if ((self.tile_width == 0) != (self.tile_height == 0)) return error.InvalidChunkPolicy;
        if (self.tile_width != 0 and (self.tile_overlap >= self.tile_width or self.tile_overlap >= self.tile_height)) return error.InvalidChunkPolicy;
        if (self.tile_width == 0 and self.tile_overlap != 0) return error.InvalidChunkPolicy;
    }
};
pub const Counter = struct {
    context: *anyopaque,
    /// Exact count for the downstream model, including its wrapping.
    count: *const fn (*anyopaque, []const u8) anyerror!usize,
    count_overlap: ?*const fn (*anyopaque, []const u8) anyerror!usize = null,
};
pub const Control = struct {
    context: ?*const anyopaque = null,
    check_fn: ?*const fn (?*const anyopaque) anyerror!void = null,
    pub fn check(self: Control) !void {
        if (self.check_fn) |f| try f(self.context);
    }
};
pub const Identity = struct {
    source: [32]u8,
    plan: [32]u8,
    pub fn init(bytes: []const u8, mime_type: []const u8, version: []const u8, model: Model, policy: Policy) !Identity {
        try policy.validate();
        _ = try model.capacity();
        var source = std.crypto.hash.sha2.Sha256.init(.{});
        source.update("antfly-chunk-source-v1");
        field(&source, mime_type);
        field(&source, bytes);
        return fromFingerprint(source.finalResult(), version, model, policy);
    }
    /// Version-pinned providers establish immutable identity without downloading
    /// an entire object. Their range adapter must enforce that version per read.
    pub fn initPinned(resource: []const u8, mime: []const u8, version: []const u8, model: Model, policy: Policy) !Identity {
        if (resource.len == 0 or version.len == 0) return error.ImmutableVersionRequired;
        var source = std.crypto.hash.sha2.Sha256.init(.{});
        source.update("antfly-chunk-pinned-source-v1");
        field(&source, resource);
        field(&source, mime);
        field(&source, version);
        return fromFingerprint(source.finalResult(), version, model, policy);
    }
    pub fn fromFingerprint(fingerprint: [32]u8, version: []const u8, model: Model, policy: Policy) !Identity {
        try policy.validate();
        _ = try model.capacity();
        var plan = std.crypto.hash.sha2.Sha256.init(.{});
        plan.update("antfly-chunk-plan-v1");
        plan.update(&fingerprint);
        const content_version = std.fmt.bytesToHex(fingerprint, .lower);
        field(&plan, if (version.len != 0) version else &content_version);
        field(&plan, model.planning_identity);
        number(&plan, model.context_tokens);
        number(&plan, model.max_text_bytes);
        number(&plan, model.reserved_tokens);
        number(&plan, model.max_frames);
        number(&plan, @intCast(model.max_audio_ns));
        number(&plan, @intCast(model.min_audio_ns));
        inline for (@typeInfo(Policy).@"struct".field_names) |name| {
            if (comptime std.mem.eql(u8, name, "audio_speaker_turns")) {
                if (policy.audio_speaker_turns) field(&plan, "speaker-turns-v1");
            } else number(&plan, @intCast(@field(policy, name)));
        }
        return .{ .source = fingerprint, .plan = plan.finalResult() };
    }
    fn field(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
        number(hash, bytes.len);
        hash.update(bytes);
    }
    fn number(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .little);
        hash.update(&bytes);
    }
    pub fn cursor(self: Identity, ordinal: u64, out: *[85]u8) []const u8 {
        const hex = std.fmt.bytesToHex(self.plan, .lower);
        @memcpy(out[0..64], &hex);
        return std.fmt.bufPrint(out, "{s}:{d}", .{ hex, ordinal }) catch unreachable;
    }
    pub fn parseContinuation(self: Identity, value: []const u8) !u64 {
        if (value.len < 66 or value.len > 85 or value[64] != ':') return error.InvalidChunkContinuation;
        const hex = std.fmt.bytesToHex(self.plan, .lower);
        if (!std.mem.eql(u8, value[0..64], &hex)) return error.StaleChunkContinuation;
        const digits = value[65..];
        if (digits.len > 1 and digits[0] == '0') return error.InvalidChunkContinuation;
        for (digits) |c| if (!std.ascii.isDigit(c)) return error.InvalidChunkContinuation;
        return std.fmt.parseInt(u64, digits, 10) catch error.InvalidChunkContinuation;
    }
};
/// Owns no text or token arrays; each invocation bounds tokenizer output in its adapter.
pub const Text = struct {
    text: []const u8,
    policy: Policy,
    model: Model,
    counter: Counter,
    control: Control = .{},
    offset: usize = 0,
    ordinal: u64 = 0,
    page: ?u32 = null,
    validated: bool = false,
    fn boundary(self: Text, end: usize) usize {
        var p = @min(end, self.text.len);
        while (p > self.offset and p < self.text.len and self.text[p] & 0xc0 == 0x80) p -= 1;
        return p;
    }
    pub fn next(self: *Text) !?Descriptor {
        try self.control.check();
        try self.policy.validate();
        const capacity = @min(try self.model.capacity(), self.policy.target_tokens);
        if (!self.validated) {
            if (!std.unicode.utf8ValidateSlice(self.text)) return error.InvalidUtf8;
            self.validated = true;
        }
        if (self.offset >= self.text.len) return null;
        const start = self.offset;
        var lo = start;
        const ceiling = start + @min(self.text.len - start, @min(self.policy.max_text_bytes, self.model.max_text_bytes));
        var hi = self.boundary(start + @min(ceiling - start, @max(@as(usize, 256), capacity * 4)));
        while (hi < ceiling) {
            try self.control.check();
            if (try self.counter.count(self.counter.context, self.text[start..hi]) > capacity) break;
            const expanded = start + @min(ceiling - start, (hi - start) * 2);
            const next_boundary = self.boundary(expanded);
            if (next_boundary <= hi) break;
            hi = next_boundary;
        }
        var best = start;
        var best_tokens: usize = 0;
        while (lo < hi) {
            try self.control.check();
            var end = self.boundary(lo + (hi - lo + 1) / 2);
            if (end == lo) {
                end = @min(self.text.len, lo + (std.unicode.utf8ByteSequenceLength(self.text[lo]) catch return error.InvalidUtf8));
            }
            const tokens = try self.counter.count(self.counter.context, self.text[start..end]);
            if (tokens <= capacity) {
                best = end;
                best_tokens = tokens;
                lo = end;
            } else {
                if (end == 0) break;
                hi = self.boundary(end - 1);
            }
        }
        if (best == start) return error.ChunkUnitTooLarge;
        self.offset = best;
        // An overlapping suffix must itself leave room for new content. Check
        // exact counts rather than deriving source offsets from token lengths.
        if (best < self.text.len and self.policy.overlap_tokens != 0) {
            var overlap = best;
            var retained: usize = 0;
            while (overlap > start) {
                var previous = overlap - 1;
                while (previous > start and self.text[previous] & 0xc0 == 0x80) previous -= 1;
                const cost = try (self.counter.count_overlap orelse self.counter.count)(self.counter.context, self.text[previous..best]);
                if (cost > self.policy.overlap_tokens or cost >= capacity or previous == start) break;
                retained = cost;
                overlap = previous;
            }
            if (retained != 0) self.offset = overlap;
        }
        const value = Descriptor{ .ordinal = self.ordinal, .coordinates = .{ .kind = if (self.page != null) .pdf else .text, .start_byte = start, .end_byte = best, .page = self.page }, .input_tokens = best_tokens };
        self.ordinal = try std.math.add(u64, self.ordinal, 1);
        return value;
    }
};
pub const Temporal = struct {
    kind: Kind,
    end_ns: i64,
    policy: Policy,
    model: Model,
    /// Model geometry cost for one selected visual picture, zero for audio.
    picture_tokens: usize,
    offset_ns: i64 = 0,
    ordinal: u64 = 0,
    pub fn next(self: *Temporal) !?Descriptor {
        try self.policy.validate();
        const budget = try self.model.capacity();
        if (self.kind != .audio and self.kind != .video and self.kind != .gif) return error.InvalidChunkPolicy;
        if (self.end_ns < 0 or self.offset_ns < 0) return error.InvalidChunkPolicy;
        if (self.offset_ns >= self.end_ns) return null;
        var duration = self.policy.window_ns;
        var count: usize = 0;
        if (self.kind == .audio) {
            duration = @min(duration, self.model.max_audio_ns);
        } else {
            if (self.picture_tokens == 0 or self.picture_tokens > budget) return error.ChunkUnitTooLarge;
            count = @min(@min(self.model.max_frames, self.policy.max_frames), (budget -| 2) / self.picture_tokens);
            if (count == 0) return error.ChunkUnitTooLarge;
            duration = @min(duration, std.math.mul(i64, @intCast(count), self.policy.step_ns) catch return error.InvalidChunkPolicy);
        }
        if (self.policy.overlap_ns >= duration) return error.InvalidChunkPolicy;
        var stop = @min(self.end_ns, std.math.add(i64, self.offset_ns, duration) catch self.end_ns);
        if (self.kind == .audio and stop < self.end_ns and self.end_ns - stop < self.model.min_audio_ns) {
            stop = self.end_ns - self.model.min_audio_ns;
            if (stop <= self.offset_ns + self.policy.overlap_ns) return error.ChunkUnitTooLarge;
        }
        if (self.kind != .audio) count = @intCast(@divFloor(stop - self.offset_ns - 1, self.policy.step_ns) + 1);
        const result = Descriptor{ .ordinal = self.ordinal, .coordinates = .{ .kind = self.kind, .start_ns = self.offset_ns, .end_ns = stop, .picture_count = if (self.kind == .audio) null else @intCast(count) }, .input_tokens = try std.math.mul(usize, count, self.picture_tokens) };
        self.offset_ns = if (stop == self.end_ns) stop else stop - self.policy.overlap_ns;
        self.ordinal = try std.math.add(u64, self.ordinal, 1);
        return result;
    }
};
pub const Image = struct {
    width: u32,
    height: u32,
    policy: Policy,
    x: u32 = 0,
    y: u32 = 0,
    ordinal: u64 = 0,
    pub fn next(self: *Image) !?Descriptor {
        try self.policy.validate();
        if (self.width == 0 or self.height == 0) return error.InvalidChunkGeometry;
        if (self.y >= self.height) return null;
        const w = if (self.policy.tile_width == 0) self.width else self.policy.tile_width;
        const h = if (self.policy.tile_height == 0) self.height else self.policy.tile_height;
        const right = @min(@as(u64, self.x) + w, self.width);
        const bottom = @min(@as(u64, self.y) + h, self.height);
        const result = Descriptor{ .ordinal = self.ordinal, .coordinates = .{ .kind = .image, .region = .{ self.x, self.y, @intCast(right), @intCast(bottom) } }, .input_tokens = 0 };
        if (right == self.width) {
            self.x = 0;
            self.y = if (bottom == self.height) self.height else @intCast(bottom - self.policy.tile_overlap);
        } else self.x = @intCast(right - self.policy.tile_overlap);
        self.ordinal = try std.math.add(u64, self.ordinal, 1);
        return result;
    }
};
test "model aware text windows preserve UTF8 spans and enforce actual downstream counts" {
    const Count = struct {
        fn count(_: *anyopaque, text: []const u8) !usize {
            return std.unicode.utf8CountCodepoints(text);
        }
    };
    var dummy: u8 = 0;
    const input = "hello 世界 today";
    var planner = Text{ .text = input, .model = .{ .planning_identity = "test", .context_tokens = 5 }, .policy = .{ .target_tokens = 8, .overlap_tokens = 1 }, .counter = .{ .context = &dummy, .count = Count.count } };
    var previous: usize = 0;
    var count: usize = 0;
    while (try planner.next()) |part| {
        const c = part.coordinates;
        try std.testing.expect(c.start_byte.? <= previous);
        try std.testing.expect(c.end_byte.? > previous);
        try std.testing.expect(try std.unicode.utf8CountCodepoints(input[c.start_byte.?..c.end_byte.?]) <= 5);
        previous = @intCast(c.end_byte.?);
        count += 1;
    }
    try std.testing.expectEqual(input.len, previous);
    try std.testing.expect(count > 1);
}
test "time windows retain cadence rather than sparsifying long clips and cover the tail" {
    var planner = Temporal{ .kind = .video, .end_ns = 70 * std.time.ns_per_s, .model = .{ .planning_identity = "test", .context_tokens = 8192 }, .policy = .{}, .picture_tokens = 140 };
    const first = (try planner.next()).?;
    try std.testing.expectEqual(@as(i64, 32 * std.time.ns_per_s), first.coordinates.end_ns.?);
    try std.testing.expectEqual(@as(usize, 4480), first.input_tokens);
    _ = try planner.next();
    try std.testing.expectEqual(@as(i64, 70 * std.time.ns_per_s), (try planner.next()).?.coordinates.end_ns.?);
    try std.testing.expectEqual(@as(?Descriptor, null), try planner.next());
    var audio = Temporal{ .kind = .audio, .end_ns = 31 * std.time.ns_per_s, .model = planner.model, .policy = .{}, .picture_tokens = 0 };
    try std.testing.expectEqual(@as(i64, 30 * std.time.ns_per_s), (try audio.next()).?.coordinates.end_ns.?);
}
test "continuation binds source model and semantic policy while tiling covers original regions" {
    const model = Model{ .planning_identity = "tokenizer+geometry-v1", .context_tokens = 8192 };
    const original = try Identity.init("source", "video/mp4", "version1", model, .{});
    var bytes: [85]u8 = undefined;
    const cursor_value = original.cursor(123, &bytes);
    try std.testing.expectEqual(@as(u64, 123), try original.parseContinuation(cursor_value));
    const automatic = try Identity.init("source", "video/mp4", "", model, .{});
    const automatic_version = std.fmt.bytesToHex(automatic.source, .lower);
    const explicit_content_version = try Identity.init("source", "video/mp4", &automatic_version, model, .{});
    try std.testing.expectEqualSlices(u8, &automatic.plan, &explicit_content_version.plan);
    const changed = try Identity.init("source", "video/mp4", "version2", model, .{});
    try std.testing.expectError(error.StaleChunkContinuation, changed.parseContinuation(cursor_value));
    const different_policy = try Identity.init("source", "video/mp4", "version1", model, .{ .step_ns = 2 * std.time.ns_per_s });
    try std.testing.expectError(error.StaleChunkContinuation, different_policy.parseContinuation(cursor_value));
    var image = Image{ .width = 10, .height = 7, .policy = .{ .tile_width = 6, .tile_height = 4, .tile_overlap = 1 } };
    var coverage: [7][10]bool = @splat(@splat(false));
    while (try image.next()) |part| {
        const box = part.coordinates.region.?;
        for (box[1]..box[3]) |y| for (box[0]..box[2]) |x| {
            coverage[y][x] = true;
        };
    }
    for (coverage) |row| for (row) |covered| try std.testing.expect(covered);
}

test "model chunk coordinates retain the full raster basis and reject inconsistent source spans" {
    var coordinates = Coordinates{ .kind = .pdf, .page = 1, .region = .{ 32, 16, 64, 48 }, .raster_width = 128, .raster_height = 96, .page_box = .{ 0, 0, 64, 48 } };
    try coordinates.validate();
    coordinates.raster_width = 63;
    try std.testing.expectError(error.InvalidChunkCoordinates, coordinates.validate());
    coordinates.raster_width = null;
    try std.testing.expectError(error.InvalidChunkCoordinates, coordinates.validate());
    try std.testing.expectError(error.InvalidChunkCoordinates, (Coordinates{ .kind = .text, .start_byte = 9, .end_byte = 8 }).validate());
    try std.testing.expectError(error.InvalidChunkCoordinates, (Coordinates{ .kind = .audio, .start_sample = 0, .end_sample = 100 }).validate());
}
