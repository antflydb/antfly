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

//! Immutable container indexes shared by paged requests. Entries own every
//! metadata slice; sources, provider contexts, decoder state and controls are
//! always rebound to the current request. Unpinned ranges bypass the cache.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const audio_seek = @import("media_audio_seek.zig");
const video_owner = @import("media_chunking_video.zig");
const admitted = @import("../backends/admitted_allocator.zig");
pub const Cache = struct {
    pub const max_bytes = 64 * 1024 * 1024;
    pub const max_entries = 8;
    mutex: std.atomic.Mutex = .unlocked,
    entries: [max_entries]?*Entry = @splat(null),
    bytes: usize = 0,
    clock: u64 = 0,
    pub const Admission = struct {
        context: *anyopaque,
        acquire: *const fn (*anyopaque, usize) anyerror!@import("../runtime/tier/memory.zig").AdmissionLease,
    };
    pub const Input = union(enum) { mp4: media.mp4.Reader, webm: media.webm.Reader, audio: audio_seek.Index };
    pub const Data = union(enum) {
        audio: audio_seek.Index,
        mp4: struct {
            track: media.mp4.Track,
            packets: []media.mp4.Packet,
            limits: media.mp4.Limits,
            pub fn reader(self: @This(), a: std.mem.Allocator, source: *media.source.Source) media.mp4.Reader {
                return .{ .allocator = a, .input = source, .metadata = .{ .bytes = &.{}, .source = null }, .track = self.track, .packets = self.packets, .limits = self.limits };
            }
        },
        webm: struct {
            track: media.webm.Track,
            packets: []media.webm.Packet,
            cues: []media.webm.Cue,
            timestamp_scale_ns: u64,
            duration_seconds: ?f64,
            pub fn reader(self: @This(), a: std.mem.Allocator, source: *media.source.Source) media.webm.Reader {
                return .{ .allocator = a, .input = source, .metadata = .{ .bytes = &.{}, .source = null }, .track = self.track, .packets = self.packets, .cues = self.cues, .timestamp_scale_ns = self.timestamp_scale_ns, .duration_seconds = self.duration_seconds };
            }
        },
    };
    pub const WebmSnapshot = struct {
        index: video.webm_pictures.Index,
        source_bound: bool,
        timeline: video.sampling.Timeline,
        origin: i64,
        duration: i128,
        fn copy(a: std.mem.Allocator, value: WebmSnapshot) !WebmSnapshot {
            var result = value;
            result.index.reader = null;
            result.index.reservation = .{};
            result.index.allocator = a;
            result.index.pictures = try a.dupe(video.webm_pictures.Picture, value.index.pictures);
            result.index.restarts = try a.dupe(video.webm_pictures.Restart, value.index.restarts);
            result.timeline.allocator = a;
            result.timeline.entries = try a.dupe(@typeInfo(@TypeOf(value.timeline.entries)).pointer.child, value.timeline.entries);
            result.timeline.maxima = try a.dupe(i128, value.timeline.maxima);
            return result;
        }
    };
    pub const Planning = union(enum) { mp4: *const video_owner.Owner, webm: WebmSnapshot };
    pub const Entry = struct {
        allocation: admitted.AdmittedAllocator,
        arena: std.heap.ArenaAllocator,
        key: [32]u8,
        data: Data,
        mp4_plan: ?video_owner.Snapshot = null,
        webm_plan: ?WebmSnapshot = null,
        refs: usize = 1,
        used: u64 = 0,
        bytes: usize = 0,
        fn destroy(self: *Entry) void {
            const a = self.allocation.backing;
            self.arena.deinit();
            self.allocation.deinit();
            a.destroy(self);
        }
    };
    pub fn key(source: *const media.source.Source, webm: bool, max_packets: usize, max_working_bytes: usize) ?[32]u8 {
        const binding = switch (source.storage) {
            .range => |range| range.immutable_binding orelse return null,
            .borrowed => return null,
        };
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("media-container-index-v1");
        hash.update(&binding);
        hash.update(source.identity);
        hash.update(&.{@intFromBool(webm)});
        for ([_]u64{ source.length(), max_packets, max_working_bytes }) |n| {
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &raw, n, .little);
            hash.update(&raw);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn audioKey(source: *const media.source.Source, max_packets: usize, max_bytes_: usize) ?[32]u8 {
        const base = key(source, false, max_packets, max_bytes_) orelse return null;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("media-audio-seek-v1");
        hash.update(&base);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn get(self: *Cache, identity: [32]u8) ?*Entry {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |slot| if (slot) |entry| {
            if (std.mem.eql(u8, &identity, &entry.key)) {
                entry.refs += 1;
                self.clock +%= 1;
                entry.used = self.clock;
                return entry;
            }
        };
        return null;
    }
    /// Best-effort retention after admitted parsing. The cache holds its own
    /// node host-memory reservation rather than borrowing a request's permit.
    pub fn remember(self: *Cache, a: std.mem.Allocator, identity: [32]u8, data: Input, admission: Admission) void {
        self.rememberPlanned(a, identity, data, admission, null);
    }
    pub fn rememberPlanned(self: *Cache, a: std.mem.Allocator, identity: [32]u8, data: Input, admission: Admission, planning: ?Planning) void {
        const estimate = switch (data) {
            .audio => |index| index.estimatedBytes(),
            .mp4 => |r| std.math.mul(usize, r.packets.len, @sizeOf(media.mp4.Packet)) catch return,
            .webm => |r| (std.math.mul(usize, r.packets.len, @sizeOf(media.webm.Packet)) catch return) +| (std.math.mul(usize, r.cues.len, @sizeOf(media.webm.Cue)) catch return),
        };
        if (estimate > max_bytes) return;
        const entry = a.create(Entry) catch return;
        entry.* = .{ .allocation = .{ .backing = a, .admission_context = admission.context, .admit = admission.acquire }, .arena = undefined, .key = identity, .data = undefined, .refs = 0 };
        entry.arena = .init(entry.allocation.allocator());
        self.copy(entry, data) catch {
            entry.destroy();
            return;
        };
        if (planning) |value| switch (value) {
            .mp4 => |owner| {
                // In-band parameter registries own mutable request context.
                if (owner.registry == null) entry.mp4_plan = video_owner.Snapshot.copy(entry.arena.allocator(), owner) catch {
                    entry.destroy();
                    return;
                };
            },
            .webm => |snapshot| entry.webm_plan = WebmSnapshot.copy(entry.arena.allocator(), snapshot) catch {
                entry.destroy();
                return;
            },
        };
        entry.bytes = entry.allocation.reserved_bytes + @sizeOf(Entry);
        if (entry.bytes > max_bytes) {
            entry.destroy();
            return;
        }
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |slot| if (slot) |old| {
            if (std.mem.eql(u8, &old.key, &identity)) {
                entry.destroy();
                return;
            }
        };
        while (true) {
            var empty: ?usize = null;
            var victim: ?usize = null;
            for (self.entries, 0..) |slot, i| {
                if (slot) |old| {
                    if (old.refs == 0 and (victim == null or old.used < self.entries[victim.?].?.used)) victim = i;
                } else empty = i;
            }
            if (empty != null and entry.bytes <= max_bytes - self.bytes) {
                self.clock +%= 1;
                entry.used = self.clock;
                self.entries[empty.?] = entry;
                self.bytes += entry.bytes;
                return;
            }
            const index = victim orelse {
                entry.destroy();
                return;
            };
            const old = self.entries[index].?;
            self.bytes -= old.bytes;
            self.entries[index] = null;
            old.destroy();
        }
    }
    fn copy(_: *Cache, entry: *Entry, data: Input) !void {
        const a = entry.arena.allocator();
        switch (data) {
            .audio => |index| entry.data = .{ .audio = try index.copy(a) },
            .mp4 => |original| {
                var track = original.track;
                track.avcc = try a.dupe(u8, track.avcc);
                track.color_info = try a.dupe(u8, track.color_info);
                entry.data = .{ .mp4 = .{ .track = track, .packets = try a.dupe(media.mp4.Packet, original.packets), .limits = original.limits } };
            },
            .webm => |original| {
                var track = original.track;
                track.codec_private = try a.dupe(u8, track.codec_private);
                track.presentation.projection_private = try a.dupe(u8, track.presentation.projection_private);
                track.colour.mastering = try a.dupe(u8, track.colour.mastering);
                entry.data = .{ .webm = .{ .track = track, .packets = try a.dupe(media.webm.Packet, original.packets), .cues = try a.dupe(media.webm.Cue, original.cues), .timestamp_scale_ns = original.timestamp_scale_ns, .duration_seconds = original.duration_seconds } };
            },
        }
    }
    pub fn reclaim(raw: *anyopaque, target_bytes: usize) usize {
        const self: *Cache = @ptrCast(@alignCast(raw));
        if (!self.mutex.tryLock()) return 0;
        defer self.mutex.unlock();
        var released: usize = 0;
        while (released < target_bytes) {
            var victim: ?usize = null;
            for (self.entries, 0..) |slot, index| if (slot) |entry| {
                if (entry.refs == 0 and (victim == null or entry.used < self.entries[victim.?].?.used)) victim = index;
            };
            const index = victim orelse break;
            const entry = self.entries[index].?;
            self.entries[index] = null;
            self.bytes -= entry.bytes;
            released += entry.bytes;
            entry.destroy();
        }
        return released;
    }
    pub fn release(self: *Cache, entry: *Entry) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(entry.refs > 0);
        entry.refs -= 1;
    }
    pub fn deinit(self: *Cache) void {
        for (self.entries) |slot| if (slot) |entry| {
            std.debug.assert(entry.refs == 0);
            entry.destroy();
        };
        self.entries = @splat(null);
        self.bytes = 0;
    }
};

test "model aware media immutable container cache owns metadata and fences source versions and parser limits" {
    const a = std.testing.allocator;
    const Provider = struct {
        controller: @import("../runtime/tier/memory.zig").AdmissionController = .{},
        fn acquire(raw: *anyopaque, bytes: usize) !@import("../runtime/tier/memory.zig").AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = bytes }, false);
        }
        fn read(_: *anyopaque, _: u64, _: []u8, _: media.source.Control) !usize {
            return error.UnexpectedRead;
        }
    };
    var provider: Provider = .{};
    var source = media.source.Source{ .allocator = a, .identity = "object/version", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = 100, .immutable_binding = @splat(1) } } };
    const identity = Cache.key(&source, false, 100, 1024).?;
    var private = [_]u8{ 1, 2, 3 };
    var packets = [_]media.mp4.Packet{.{ .offset = 20, .size = 4, .dts = 0, .media_pts = 0, .pts = 0, .duration = 1, .sync = true }};
    const reader = media.mp4.Reader{ .allocator = a, .input = &source, .metadata = .{ .bytes = &.{}, .source = null }, .track = .{ .id = 1, .timescale = 1, .width = 1, .height = 1, .avcc = &private, .nal_length_bytes = 4, .display_matrix = @splat(0) }, .packets = &packets, .limits = .{} };
    var cache: Cache = .{};
    defer cache.deinit();
    cache.remember(a, identity, .{ .mp4 = reader }, .{ .context = &provider, .acquire = Provider.acquire });
    @memset(&private, 0);
    packets[0].offset = 99;
    {
        const hit = cache.get(identity).?;
        defer cache.release(hit);
        try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, hit.data.mp4.track.avcc);
        try std.testing.expectEqual(@as(u64, 20), hit.data.mp4.packets[0].offset);
        source.storage.range.immutable_binding = @splat(2);
        try std.testing.expect(cache.get(Cache.key(&source, false, 100, 1024).?) == null);
        source.storage.range.immutable_binding = @splat(1);
        try std.testing.expect(cache.get(Cache.key(&source, false, 99, 1024).?) == null);
        try std.testing.expect(cache.get(Cache.key(&source, true, 100, 1024).?) == null);
        source.storage.range.immutable_binding = @splat(3);
        const webm_identity = Cache.key(&source, true, 100, 1024).?;
        private = .{ 4, 5, 6 };
        const webm_reader = media.webm.Reader{ .allocator = a, .input = &source, .metadata = .{ .bytes = &.{}, .source = null }, .track = .{ .number = 1, .codec = .vp9, .width = 1, .height = 1, .codec_private = &private, .presentation = .{ .projection_private = &private }, .colour = .{ .mastering = &private } }, .packets = &.{}, .cues = &.{}, .timestamp_scale_ns = 1_000_000 };
        cache.remember(a, webm_identity, .{ .webm = webm_reader }, .{ .context = &provider, .acquire = Provider.acquire });
        @memset(&private, 0);
        const webm_hit = cache.get(webm_identity).?;
        defer cache.release(webm_hit);
        try std.testing.expectEqualSlices(u8, &.{ 4, 5, 6 }, webm_hit.data.webm.track.codec_private);
        try std.testing.expectEqualSlices(u8, &.{ 4, 5, 6 }, webm_hit.data.webm.track.presentation.projection_private);
        try std.testing.expectEqualSlices(u8, &.{ 4, 5, 6 }, webm_hit.data.webm.track.colour.mastering);
        source.storage.range.immutable_binding = null;
        try std.testing.expect(Cache.key(&source, false, 100, 1024) == null);
        try std.testing.expect(cache.bytes <= Cache.max_bytes);
        try std.testing.expectEqual(@as(usize, 0), Cache.reclaim(&cache, Cache.max_bytes));
    }
    try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) > 0);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try std.testing.expectEqual(@as(usize, 0), provider.controller.snapshot().hostTotalBytes());
}

test "model aware media cached planning allocation failures release every host lease" {
    const a = std.testing.allocator;
    const memory = @import("../runtime/tier/memory.zig");
    const Admission = struct {
        fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const controller: *memory.AdmissionController = @ptrCast(@alignCast(raw));
            return controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    var source = media.source.Source{ .allocator = a, .identity = "immutable", .storage = .{ .borrowed = &.{} } };
    var packets = [_]media.mp4.Packet{.{ .offset = 0, .size = 1, .dts = 0, .media_pts = 0, .pts = 0, .duration = 1, .sync = true }};
    var reader = media.mp4.Reader{ .allocator = a, .input = &source, .metadata = .{ .bytes = &.{}, .source = null }, .track = .{ .id = 1, .codec = .mjpeg, .timescale = 1, .width = 1, .height = 1, .avcc = &.{}, .nal_length_bytes = 4, .display_matrix = @splat(0) }, .packets = &packets, .limits = .{} };
    var pool: media.admission.Pool = .{ .limits = .{ .host_bytes = 1024 * 1024 } };
    var owner = try video_owner.Owner.init(a, &reader, &pool, .{});
    defer owner.deinit();
    for (0..32) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        var controller: memory.AdmissionController = .{};
        var cache: Cache = .{};
        cache.rememberPlanned(failing.allocator(), @splat(1), .{ .mp4 = reader }, .{ .context = &controller, .acquire = Admission.acquire }, .{ .mp4 = &owner });
        cache.deinit();
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
    }
}

test "model aware media audio index clones own payloads and release admission under allocation faults" {
    const a = std.testing.allocator;
    const memory = @import("../runtime/tier/memory.zig");
    const Admission = struct {
        fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const controller: *memory.AdmissionController = @ptrCast(@alignCast(raw));
            return controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    for ([_][]const u8{ "tone.mp3", "codec-corpus/tone-stereo.flac" }) |name| {
        const path = try std.fmt.allocPrint(a, "../../lib/audio/testdata/{s}", .{name});
        defer a.free(path);
        const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, a, .limited(1024 * 1024));
        defer a.free(bytes);
        var index = try audio_seek.Index.init(a, bytes, 100000);
        var controller: memory.AdmissionController = .{};
        var cache: Cache = .{};
        defer cache.deinit();
        cache.remember(a, @splat(1), .{ .audio = index }, .{ .context = &controller, .acquire = Admission.acquire });
        const samples = index.samples();
        index.deinit(); // Every cached slice must survive its request's arena.
        const hit = cache.get(@splat(1)).?;
        try std.testing.expectEqual(@as(usize, 0), Cache.reclaim(&cache, Cache.max_bytes));
        var reader = audio_seek.Reader{ .index = &hit.data.audio, .bytes = bytes };
        var window = try reader.provider().read(&reader, a, samples / 2, samples / 2 + 100);
        window.deinit(a);
        // Best-effort insertion absorbs OOM, but every partial clone and lease
        // must still be released. Stop after the first successful allocation run.
        var fail_index: usize = 0;
        while (fail_index < 256) : (fail_index += 1) {
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
            var fault_cache: Cache = .{};
            fault_cache.remember(failing.allocator(), @splat(2), .{ .audio = hit.data.audio }, .{ .context = &controller, .acquire = Admission.acquire });
            const retained = fault_cache.bytes != 0;
            fault_cache.deinit();
            try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
            if (retained) break;
        }
        try std.testing.expect(fail_index < 256);
        cache.release(hit);
        try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) > 0);
        try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
    }
}
