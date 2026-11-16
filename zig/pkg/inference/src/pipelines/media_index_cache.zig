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

//! Immutable container and planning indexes shared by paged requests. Entries own every
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
    // A metadata-abuse ceiling; admitted bytes normally bound occupancy first.
    pub const max_entries = 4096;
    const bucket_count = 1024;
    mutex: std.atomic.Mutex = .unlocked,
    audio_builds: std.atomic.Value(u64) = .init(0),
    builds: @import("media_speaker_flights.zig").Registry = .{},
    buckets: [bucket_count]?*Entry = @splat(null),
    oldest: ?*Entry = null,
    newest: ?*Entry = null,
    count: usize = 0,
    bytes: usize = 0,
    pub fn acquireBuild(self: *Cache, identity: [32]u8, control: @import("../execution_control.zig").InferenceExecutionControl) !@import("media_speaker_flights.zig").Registry.Lease {
        return self.builds.acquire(std.heap.smp_allocator, identity, control);
    }
    pub const Admission = struct {
        context: *anyopaque,
        acquire: *const fn (*anyopaque, usize) anyerror!@import("../runtime/tier/memory.zig").AdmissionLease,
    };
    pub const Input = union(enum) { speaker_analysis: @import("media_speaker_state.zig").Snapshot, audio_plan: @import("media_audio_plan.zig").Snapshot, mp4: media.mp4.Reader, webm: media.webm.Reader, audio: audio_seek.Index, aac: @import("media_chunking_audio_source.zig").Mp4 };
    pub const Data = union(enum) {
        speaker_analysis: @import("media_speaker_state.zig").Snapshot,
        audio_plan: @import("media_audio_plan.zig").Snapshot,
        aac: @import("media_chunking_audio_source.zig").Mp4.Snapshot,
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
        header_lease: @import("../runtime/tier/memory.zig").AdmissionLease,
        arena: std.heap.ArenaAllocator,
        key: [32]u8,
        data: Data,
        mp4_plan: ?video_owner.Snapshot = null,
        webm_plan: ?WebmSnapshot = null,
        refs: usize = 1,
        audio_plan_digest: ?[32]u8 = null,
        speaker_blocks_owned: bool = false,
        opus_owned: bool = false,
        speaker_disk: ?@import("media_speaker_state.zig").DiskCursor = null,
        hash_next: ?*Entry = null,
        older: ?*Entry = null,
        newer: ?*Entry = null,
        bytes: usize = 0,
        fn destroy(self: *Entry) void {
            const a = self.allocation.backing;
            if (self.opus_owned) switch (self.data.audio) {
                inline .opus, .flac => |*index| index.deinit(),
                .webm => |*index| index.codec.opus.deinit(),
                else => unreachable,
            };
            if (self.speaker_blocks_owned) self.data.speaker_analysis.shared.?.releaseBlocks();
            self.arena.deinit();
            self.allocation.deinit();
            self.header_lease.release();
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
    pub fn inlineAudioKey(content: [32]u8, aac: bool, max_packets: usize, max_bytes_: usize) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("media-inline-audio-seek-v1");
        hash.update(&content);
        hash.update(&.{@intFromBool(aac)});
        for ([_]u64{ max_packets, max_bytes_ }) |n| {
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
    pub fn aacKey(source: *const media.source.Source, max_packets: usize, max_working_bytes: usize) ?[32]u8 {
        const base = key(source, false, max_packets, max_working_bytes) orelse return null;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("media-aac-window-index-v1");
        hash.update(&base);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn bucket(identity: [32]u8) usize {
        return @intCast(std.hash.Wyhash.hash(0, &identity) % bucket_count);
    }
    fn find(self: *Cache, identity: [32]u8) ?*Entry {
        var next = self.buckets[bucket(identity)];
        while (next) |entry| : (next = entry.hash_next) {
            if (std.mem.eql(u8, &identity, &entry.key)) return entry;
        }
        return null;
    }
    fn unlinkLru(self: *Cache, entry: *Entry) void {
        if (entry.older) |older| older.newer = entry.newer else self.oldest = entry.newer;
        if (entry.newer) |newer| newer.older = entry.older else self.newest = entry.older;
    }
    fn touch(self: *Cache, entry: *Entry) void {
        entry.older = self.newest;
        entry.newer = null;
        if (self.newest) |previous| previous.newer = entry else self.oldest = entry;
        self.newest = entry;
    }
    fn evictable(self: *Cache) ?*Entry {
        var next = self.oldest;
        while (next) |entry| : (next = entry.newer) if (entry.refs == 0) return entry;
        return null;
    }
    fn remove(self: *Cache, entry: *Entry) void {
        var link = &self.buckets[bucket(entry.key)];
        while (link.*) |candidate| {
            if (candidate == entry) {
                link.* = entry.hash_next;
                break;
            }
            link = &candidate.hash_next;
        }
        self.unlinkLru(entry);
        self.bytes -= entry.bytes;
        self.count -= 1;
        entry.destroy();
    }
    pub fn get(self: *Cache, identity: [32]u8) ?*Entry {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.find(identity) orelse return null;
        entry.refs += 1;
        self.unlinkLru(entry);
        self.touch(entry);
        return entry;
    }
    pub fn speakerDisk(self: *Cache, identity: [32]u8) ?@import("media_speaker_state.zig").DiskCursor {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.find(identity) orelse return null;
        return entry.speaker_disk;
    }
    pub fn recordSpeakerDisk(self: *Cache, identity: [32]u8, cursor: @import("media_speaker_state.zig").DiskCursor) void {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.find(identity) orelse return;
        if (entry.data != .speaker_analysis or entry.data.speaker_analysis.next_window < cursor.window) return;
        if (entry.speaker_disk) |old| if (old.window > cursor.window) return;
        entry.speaker_disk = cursor;
    }
    /// Best-effort retention after admitted parsing. The cache holds its own
    /// node host-memory reservation rather than borrowing a request's permit.
    pub fn remember(self: *Cache, a: std.mem.Allocator, identity: [32]u8, data: Input, admission: Admission) void {
        self.rememberPlanned(a, identity, data, admission, null);
    }
    pub fn rememberPlanned(self: *Cache, a: std.mem.Allocator, identity: [32]u8, data: Input, admission: Admission, planning: ?Planning) void {
        if (data == .speaker_analysis) {
            const snapshot = data.speaker_analysis;
            snapshot.validate() catch return;
            const entry = self.prepareEntry(a, identity, data, admission, null) orelse return;
            self.lock();
            defer self.mutex.unlock();
            if (self.find(identity)) |old| {
                if (old.data != .speaker_analysis or old.refs != 0 or !snapshot.extends(old.data.speaker_analysis) or (snapshot.next_window == old.data.speaker_analysis.next_window and snapshot.complete == old.data.speaker_analysis.complete)) {
                    entry.destroy();
                    return;
                }
                entry.speaker_disk = old.speaker_disk;
                self.remove(old);
            }
            self.insert(entry);
            return;
        }
        if (data == .audio_plan) {
            self.rememberAudioPlan(a, identity, data.audio_plan, admission);
            return;
        }
        const entry = self.prepareEntry(a, identity, data, admission, planning) orelse return;
        self.lock();
        defer self.mutex.unlock();
        if (self.find(identity) != null) {
            entry.destroy();
            return;
        }
        self.insert(entry);
    }
    fn prepareEntry(self: *Cache, a: std.mem.Allocator, identity: [32]u8, data: Input, admission: Admission, planning: ?Planning) ?*Entry {
        const estimate = switch (data) {
            .speaker_analysis => |value| @sizeOf(@import("inference_chunker").diarization.Registry) + value.spanCount() * @sizeOf(@import("inference_chunker").diarization.Span),
            .audio_plan => |plan| plan.points.len * @sizeOf(@import("media_audio_plan.zig").Point) + plan.hot_ordinals.len * @sizeOf(u64),
            .aac => |owner| @import("media_chunking_audio_source.zig").Mp4.Snapshot.estimatedBytes(owner),
            .audio => |index| index.estimatedBytes(),
            .mp4 => |r| std.math.mul(usize, r.packets.len, @sizeOf(media.mp4.Packet)) catch return null,
            .webm => |r| (std.math.mul(usize, r.packets.len, @sizeOf(media.webm.Packet)) catch return null) +| (std.math.mul(usize, r.cues.len, @sizeOf(media.webm.Cue)) catch return null),
        };
        if (estimate > max_bytes) return null;
        var header_lease = admission.acquire(admission.context, @sizeOf(Entry)) catch return null;
        const entry = a.create(Entry) catch {
            header_lease.release();
            return null;
        };
        entry.* = .{ .header_lease = header_lease, .allocation = .{ .backing = a, .admission_context = admission.context, .admit = admission.acquire }, .arena = undefined, .key = identity, .data = undefined, .refs = 0 };
        entry.arena = .init(entry.allocation.allocator());
        self.copy(entry, data) catch {
            entry.destroy();
            return null;
        };
        if (planning) |value| switch (value) {
            .mp4 => |owner| {
                // In-band parameter registries own mutable request context.
                if (owner.registry == null) entry.mp4_plan = video_owner.Snapshot.copy(entry.arena.allocator(), owner) catch {
                    entry.destroy();
                    return null;
                };
            },
            .webm => |snapshot| entry.webm_plan = WebmSnapshot.copy(entry.arena.allocator(), snapshot) catch {
                entry.destroy();
                return null;
            },
        };
        entry.bytes = entry.allocation.reserved_bytes + @sizeOf(Entry);
        if (entry.opus_owned) entry.bytes += entry.data.audio.estimatedBytes();
        if (entry.speaker_blocks_owned) entry.bytes += entry.data.speaker_analysis.shared.?.blocks.len * @sizeOf(@import("media_speaker_timeline.zig").Block);
        if (entry.bytes > max_bytes) {
            entry.destroy();
            return null;
        }
        return entry;
    }
    fn insert(self: *Cache, entry: *Entry) void {
        while (self.count >= max_entries or entry.bytes > max_bytes - self.bytes) {
            const victim = self.evictable() orelse {
                entry.destroy();
                return;
            };
            self.remove(victim);
        }
        const index = bucket(entry.key);
        entry.hash_next = self.buckets[index];
        self.buckets[index] = entry;
        self.touch(entry);
        self.count += 1;
        self.bytes += entry.bytes;
    }
    /// Same-tail repair anchors are improvements too. Merge off-lock, then
    /// compare content identities before replacing; concurrent writers retry
    /// rather than overwriting a newer frontier or another repair's anchors.
    fn rememberAudioPlan(self: *Cache, a: std.mem.Allocator, identity: [32]u8, snapshot: @import("media_audio_plan.zig").Snapshot, admission: Admission) void {
        self.rememberAudioPlanUpdate(a, identity, snapshot, admission, null);
    }
    pub fn rememberAudioPlanUpdate(self: *Cache, a: std.mem.Allocator, identity: [32]u8, snapshot: @import("media_audio_plan.zig").Snapshot, admission: Admission, base_digest: ?[32]u8) void {
        snapshot.validate() catch return;
        for (0..3) |_| {
            var merged = @import("media_audio_plan.zig").Buffer{};
            var observed: ?[32]u8 = null;
            var reuse = false;
            if (self.get(identity)) |old| {
                defer self.release(old);
                if (old.data != .audio_plan) return;
                observed = old.audio_plan_digest;
                reuse = base_digest != null and std.mem.eql(u8, &base_digest.?, &observed.?);
                if (!reuse) merged.merge(old.data.audio_plan) catch return;
            }
            if (!reuse) merged.merge(snapshot) catch return;
            const candidate = if (reuse) snapshot else merged.snapshot();
            const digest = candidate.digest();
            if (observed) |old| if (std.mem.eql(u8, &old, &digest)) return;
            const entry = self.prepareEntry(a, identity, .{ .audio_plan = candidate }, admission, null) orelse return;
            entry.audio_plan_digest = digest;
            self.lock();
            const current = self.find(identity);
            const matches = if (current) |old| old.data == .audio_plan and observed != null and std.mem.eql(u8, &old.audio_plan_digest.?, &observed.?) else observed == null;
            if (!matches) {
                self.mutex.unlock();
                entry.destroy();
                continue;
            }
            if (current) |old| {
                if (old.refs != 0) {
                    self.mutex.unlock();
                    entry.destroy();
                    return;
                }
                self.remove(old);
            }
            self.insert(entry);
            self.mutex.unlock();
            return;
        }
    }
    fn copy(_: *Cache, entry: *Entry, data: Input) !void {
        const a = entry.arena.allocator();
        switch (data) {
            .speaker_analysis => |value| {
                entry.data = .{ .speaker_analysis = try value.copy(a) };
                entry.speaker_blocks_owned = entry.data.speaker_analysis.shared != null;
            },
            .audio_plan => |plan| entry.data = .{ .audio_plan = .{ .points = try a.dupe(@import("media_audio_plan.zig").Point, plan.points), .stride = plan.stride, .hot_ordinals = try a.dupe(u64, plan.hot_ordinals) } },
            .aac => |owner| entry.data = .{ .aac = try @import("media_chunking_audio_source.zig").Mp4.Snapshot.copy(a, owner) },
            .audio => |index| {
                entry.data = .{ .audio = try index.copy(a) };
                entry.opus_owned = index == .opus or index == .flac or (index == .webm and index.webm.codec == .opus);
            },
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
            const entry = self.evictable() orelse break;
            released += entry.bytes;
            self.remove(entry);
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
        self.builds.deinit(std.heap.smp_allocator);
        while (self.oldest) |entry| {
            std.debug.assert(entry.refs == 0);
            self.remove(entry);
        }
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
    for ([_][]const u8{ "tone.mp3", "codec-corpus/tone-stereo.flac", "codec-corpus/tone-stereo.opus", "codec-corpus/tone-stereo.oga" }) |name| {
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
        while (fail_index < 4096) : (fail_index += 1) {
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
            var fault_cache: Cache = .{};
            fault_cache.remember(failing.allocator(), @splat(2), .{ .audio = hit.data.audio }, .{ .context = &controller, .acquire = Admission.acquire });
            const retained = fault_cache.bytes != 0;
            fault_cache.deinit();
            try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
            if (retained) break;
        }
        try std.testing.expect(fail_index < 4096);
        cache.release(hit);
        try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) > 0);
        try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
    }
}

test "model aware media hashed LRU resolves collisions and preserves pinned entries under pressure" {
    const a = std.testing.allocator;
    const Provider = struct {
        controller: @import("../runtime/tier/memory.zig").AdmissionController = .{},
        fn acquire(raw: *anyopaque, bytes: usize) !@import("../runtime/tier/memory.zig").AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    var provider: Provider = .{};
    var source = media.source.Source{ .allocator = a, .identity = "unused", .storage = .{ .borrowed = "" } };
    const reader = media.mp4.Reader{ .allocator = a, .input = &source, .metadata = .{ .bytes = &.{}, .source = null }, .track = .{ .id = 1, .timescale = 1, .width = 1, .height = 1, .avcc = "config", .nal_length_bytes = 4, .display_matrix = @splat(0) }, .packets = &.{}, .limits = .{} };
    var cache: Cache = .{};
    defer cache.deinit();
    var keys: [32][32]u8 = undefined;
    var candidate: u64 = 0;
    for (&keys) |*key_| {
        while (true) : (candidate += 1) {
            key_.* = @splat(0);
            std.mem.writeInt(u64, key_[0..8], candidate, .little);
            if (Cache.bucket(key_.*) == 0) break;
        }
        candidate += 1;
        cache.remember(a, key_.*, .{ .mp4 = reader }, .{ .context = &provider, .acquire = Provider.acquire });
    }
    try std.testing.expectEqual(@as(usize, 32), cache.count);
    const pinned = cache.get(keys[1]).?;
    const touched = cache.get(keys[0]).?;
    cache.release(touched);
    // 0 was touched, 1 is pinned: 2 is the oldest reclaimable entry.
    try std.testing.expect(Cache.reclaim(&cache, 1) > 0);
    try std.testing.expect(cache.get(keys[2]) == null);
    const still_present = cache.get(keys[0]).?;
    cache.release(still_present);
    _ = Cache.reclaim(&cache, Cache.max_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.count);
    try std.testing.expectEqualStrings("config", pinned.data.mp4.track.avcc);
    cache.release(pinned);
    _ = Cache.reclaim(&cache, Cache.max_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.count);
    try std.testing.expectEqual(@as(usize, 0), provider.controller.snapshot().hostTotalBytes());
}

test "model aware media pause plan cache advances without regressing or destroying borrowed snapshots" {
    const memory = @import("../runtime/tier/memory.zig");
    const Admission = struct {
        fn acquire(raw: *anyopaque, amount: usize) !memory.AdmissionLease {
            const controller: *memory.AdmissionController = @ptrCast(@alignCast(raw));
            return controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = amount }, false);
        }
    };
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    var cache = Cache{};
    defer cache.deinit();
    const admission = Cache.Admission{ .context = &controller, .acquire = Admission.acquire };
    const identity: [32]u8 = @splat(9);
    const points = [_]@import("media_audio_plan.zig").Point{ .{ .ordinal = 0, .offset_ns = 0 }, .{ .ordinal = 1, .offset_ns = 1000 }, .{ .ordinal = 2, .offset_ns = 2000 } };
    cache.remember(a, identity, .{ .audio_plan = .{ .points = points[0..2] } }, admission);
    const borrowed = cache.get(identity).?;
    cache.remember(a, identity, .{ .audio_plan = .{ .points = &points } }, admission);
    try std.testing.expectEqual(@as(usize, 2), borrowed.data.audio_plan.points.len);
    try std.testing.expectEqual(@as(usize, 0), Cache.reclaim(&cache, Cache.max_bytes));
    cache.release(borrowed);
    cache.remember(a, identity, .{ .audio_plan = .{ .points = &points } }, admission);
    cache.remember(a, identity, .{ .audio_plan = .{ .points = points[0..2] } }, admission);
    const latest = cache.get(identity).?;
    try std.testing.expectEqual(@as(u64, 2), latest.data.audio_plan.nearest(2).?.ordinal);
    cache.release(latest);
    try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) > 0);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "model aware media concurrent checkpoint updates preserve same-tail repair anchors" {
    const Plan = @import("media_audio_plan.zig");
    const Fixture = struct {
        controller: @import("../runtime/tier/memory.zig").AdmissionController = .{},
        cache: *Cache,
        race: bool = false,
        identity: [32]u8 = @splat(8),
        fn acquire(raw: *anyopaque, amount: usize) !@import("../runtime/tier/memory.zig").AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.race) {
                self.race = false;
                self.cache.remember(std.testing.allocator, self.identity, .{ .audio_plan = .{ .points = &.{.{ .ordinal = 1000, .offset_ns = 1000000 }}, .hot_ordinals = &.{1000} } }, .{ .context = self, .acquire = acquire });
            }
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = amount }, false);
        }
    };
    var cache = Cache{};
    defer cache.deinit();
    var fixture = Fixture{ .cache = &cache };
    const admission = Cache.Admission{ .context = &fixture, .acquire = Fixture.acquire };
    cache.remember(std.testing.allocator, fixture.identity, .{ .audio_plan = .{ .points = &.{ .{ .ordinal = 0, .offset_ns = 0 }, .{ .ordinal = 100000, .offset_ns = 100000000 } } } }, admission);
    const base = cache.get(fixture.identity).?;
    const base_digest = base.audio_plan_digest.?;
    var candidate = Plan.Buffer{};
    try candidate.merge(base.data.audio_plan);
    cache.release(base);
    try candidate.append(.{ .ordinal = 500, .offset_ns = 500000 }, true);
    fixture.race = true;
    // Race while preparing a caller-merged candidate. Its base digest matches
    // initially, then must take the retry/merge path after the competing write.
    cache.rememberAudioPlanUpdate(std.testing.allocator, fixture.identity, candidate.snapshot(), admission, base_digest);
    {
        const hit = cache.get(fixture.identity).?;
        defer cache.release(hit);
        const snapshot: Plan.Snapshot = hit.data.audio_plan;
        try snapshot.validate();
        try std.testing.expectEqual(snapshot.digest(), hit.audio_plan_digest.?);
        try std.testing.expectEqual(@as(u64, 100000), snapshot.points[snapshot.points.len - 1].ordinal);
        try std.testing.expectEqual(@as(u64, 500), snapshot.nearest(500).?.ordinal);
        try std.testing.expectEqual(@as(u64, 1000), snapshot.nearest(1000).?.ordinal);
    }
    try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) != 0);
    try std.testing.expectEqual(@as(usize, 0), fixture.controller.snapshot().host_scratch_bytes);
}

test "model aware media speaker cache owns immutable progress and cannot regress a newer timeline" {
    const a = std.testing.allocator;
    const Speaker = @import("media_speaker_state.zig");
    const D = @import("inference_chunker").diarization;
    const Fixture = struct {
        controller: @import("../runtime/tier/memory.zig").AdmissionController = .{},
        fn acquire(raw: *anyopaque, bytes: usize) !@import("../runtime/tier/memory.zig").AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    var fixture = Fixture{};
    const admission = Cache.Admission{ .context = &fixture, .acquire = Fixture.acquire };
    var cache = Cache{};
    defer cache.deinit();
    const registry = try a.create(D.Registry);
    defer a.destroy(registry);
    registry.* = .{ .count = 1 };
    registry.sums[0][0] = 1;
    const initial = Speaker.Snapshot{ .registry = registry, .spans = &.{.{ .start_ns = 0, .end_ns = 1500 * std.time.ns_per_ms, .speaker_index = 0 }}, .sample_rate = 16000, .next_window = 1, .covered_samples = 24000, .total_samples = 48000 };
    const advanced = Speaker.Snapshot{ .registry = registry, .spans = &.{.{ .start_ns = 0, .end_ns = 3 * std.time.ns_per_s, .speaker_index = 0 }}, .sample_rate = 16000, .next_window = 2, .covered_samples = 48000, .total_samples = 48000, .complete = true };
    cache.remember(a, @splat(1), .{ .speaker_analysis = initial }, admission);
    const pinned = cache.get(@splat(1)).?;
    cache.remember(a, @splat(1), .{ .speaker_analysis = advanced }, admission);
    try std.testing.expectEqual(@as(u64, 1), pinned.data.speaker_analysis.next_window);
    try std.testing.expectEqual(@as(usize, 0), Cache.reclaim(&cache, Cache.max_bytes));
    cache.release(pinned);
    cache.remember(a, @splat(1), .{ .speaker_analysis = advanced }, admission);
    cache.remember(a, @splat(1), .{ .speaker_analysis = initial }, admission);
    const complete = cache.get(@splat(1)).?;
    try std.testing.expect(complete.data.speaker_analysis.complete);
    registry.sums[0][0] = 0.25;
    try std.testing.expectEqual(@as(f32, 1), complete.data.speaker_analysis.registry.sums[0][0]);
    cache.release(complete);
    try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) > 0);
    try std.testing.expectEqual(@as(usize, 0), fixture.controller.snapshot().hostTotalBytes());
}
