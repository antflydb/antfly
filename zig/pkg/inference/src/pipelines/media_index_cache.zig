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
    pub const Data = union(enum) { mp4: media.mp4.Reader, webm: media.webm.Reader };
    pub const Entry = struct {
        allocation: admitted.AdmittedAllocator,
        arena: std.heap.ArenaAllocator,
        key: [32]u8,
        data: Data,
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
    pub fn remember(self: *Cache, a: std.mem.Allocator, identity: [32]u8, data: Data, admission: Admission) void {
        const estimate = switch (data) {
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
    fn copy(_: *Cache, entry: *Entry, data: Data) !void {
        const a = entry.arena.allocator();
        switch (data) {
            .mp4 => |original| {
                var r = original;
                r.input = undefined;
                r.allocator = a;
                r.metadata = .{ .bytes = &.{}, .source = null };
                r.index_reservation = .{};
                r.track.avcc = try a.dupe(u8, r.track.avcc);
                r.track.color_info = try a.dupe(u8, r.track.color_info);
                r.packets = try a.dupe(media.mp4.Packet, r.packets);
                entry.data = .{ .mp4 = r };
            },
            .webm => |original| {
                var r = original;
                r.input = undefined;
                r.allocator = a;
                r.metadata = .{ .bytes = &.{}, .source = null };
                r.index_reservation = .{};
                r.track.codec_private = try a.dupe(u8, r.track.codec_private);
                r.packets = try a.dupe(media.webm.Packet, r.packets);
                r.cues = try a.dupe(media.webm.Cue, r.cues);
                entry.data = .{ .webm = r };
            },
        }
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
    const hit = cache.get(identity).?;
    defer cache.release(hit);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, hit.data.mp4.track.avcc);
    try std.testing.expectEqual(@as(u64, 20), hit.data.mp4.packets[0].offset);
    source.storage.range.immutable_binding = @splat(2);
    try std.testing.expect(cache.get(Cache.key(&source, false, 100, 1024).?) == null);
    source.storage.range.immutable_binding = @splat(1);
    try std.testing.expect(cache.get(Cache.key(&source, false, 99, 1024).?) == null);
    try std.testing.expect(cache.get(Cache.key(&source, true, 100, 1024).?) == null);
    source.storage.range.immutable_binding = null;
    try std.testing.expect(Cache.key(&source, false, 100, 1024) == null);
    try std.testing.expect(cache.bytes <= Cache.max_bytes);
}
