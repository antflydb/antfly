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

//! Optional bounded reuse of strictly reconstructed live pictures. Pins own
//! pixels independently of the coordinator/DPB; pressure is always a cache miss.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const capacity = 128;
const spill_capacity = 32;
const Budget = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    used: std.atomic.Value(usize) = .init(0),
    limit: usize,
    reservation: media.admission.Token,
    fn release(self: *Budget) void {
        if (self.references.fetchSub(1, .acq_rel) == 1) {
            std.debug.assert(self.used.load(.acquire) == 0);
            var reservation = self.reservation;
            self.allocator.destroy(self);
            reservation.deinit();
        }
    }
};
const Entry = struct {
    budget: *Budget,
    references: std.atomic.Value(usize) = .init(1),
    packet: usize,
    frame: h264.Frame,
    fn retain(self: *Entry) *Entry {
        _ = self.references.fetchAdd(1, .monotonic);
        return self;
    }
    fn release(self: *Entry) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const budget = self.budget;
        const bytes = self.frame.nv12.len + @sizeOf(Entry);
        self.frame.deinit();
        budget.allocator.destroy(self);
        _ = budget.used.fetchSub(bytes, .acq_rel);
        budget.release();
    }
};
pub const View = struct {
    entries: [capacity + spill_capacity]?*Entry = @splat(null),
    pub fn deinit(self: *View) void {
        for (&self.entries) |*entry| if (entry.*) |owner| owner.release();
        self.* = .{};
    }
    pub fn get(self: *const View, packet: usize) ?*const h264.Frame {
        for (self.entries) |entry| if (entry) |owner| if (owner.packet == packet) return &owner.frame;
        return null;
    }
};
const Target = struct {
    ns: i128,
    end: i128,
    entry: ?*Entry = null,
    fn prefers(self: Target, packet: usize, pts: i64, scale: u32) bool {
        const time = @as(i128, pts) * std.time.ns_per_s;
        if (time < self.ns * scale or time >= self.end * scale) return false;
        return self.entry == null or pts < self.entry.?.frame.pts or (pts == self.entry.?.frame.pts and packet < self.entry.?.packet);
    }
};
pub const Cache = struct {
    allocator: std.mem.Allocator,
    pool: ?*media.admission.Pool,
    budget: *Budget,
    targets: [capacity]Target = undefined,
    /// Predictive pictures can decode before B pictures in earlier windows.
    /// Preserve a bounded reorder tail until the sampling horizon advances.
    spill: [spill_capacity]?*Entry = @splat(null),
    horizon: i128 = 0,
    count: usize = 0,
    base: usize = 0,
    scale: u32 = 1,
    hits: usize = 0,
    declined: usize = 0,
    pub fn init(a: std.mem.Allocator, pool: ?*media.admission.Pool, limit: usize) !Cache {
        var reservation = if (pool) |shared| try shared.acquire(.{ .host_bytes = @sizeOf(Budget) }) else media.admission.Token{};
        errdefer reservation.deinit();
        const budget = try a.create(Budget);
        budget.* = .{ .allocator = a, .limit = limit, .reservation = reservation };
        return .{ .allocator = a, .pool = pool, .budget = budget };
    }
    pub fn clear(self: *Cache) void {
        for (self.targets[0..self.count]) |target| if (target.entry) |entry| entry.release();
        for (&self.spill) |*slot| {
            if (slot.*) |entry| entry.release();
            slot.* = null;
        }
        self.horizon = 0;
        self.count = 0;
    }
    pub fn deinit(self: *Cache) void {
        self.clear();
        self.budget.release();
        self.* = undefined;
    }
    /// Four ready/near-future windows, without reading headers or waiting for
    /// future transport. Exact ID lookup makes a provisional candidate harmless.
    pub fn configure(self: *Cache, start: i64, duration: i64, advance: u64, step: u64, max_frames: usize, base: usize, scale: u32) void {
        var old = self.pin();
        defer old.deinit();
        self.clear();
        self.base = base;
        self.scale = scale;
        const candidates: u128 = @intCast(@divFloor(@as(i128, duration) + step - 1, step));
        const count: usize = @intCast(@min(candidates, max_frames));
        for (0..4) |window| {
            const begin = @as(i128, start) + @as(i128, @intCast(window)) * advance;
            for (0..count) |i| {
                const ordinal: u128 = if (count > 1 and candidates > count) @as(u128, i) * (candidates - 1) / (count - 1) else i;
                var target = Target{ .ns = begin + @as(i128, @intCast(ordinal * step)), .end = begin + duration };
                for (old.entries) |entry| if (entry) |owner| if (target.prefers(owner.packet, owner.frame.pts, scale)) {
                    if (target.entry) |previous| previous.release();
                    target.entry = owner.retain();
                };
                self.horizon = @max(self.horizon, target.end);
                self.targets[self.count] = target;
                self.count += 1;
            }
        }
        var spill_count: usize = 0;
        for (old.entries) |entry| if (entry) |owner| {
            if (@as(i128, owner.frame.pts) * std.time.ns_per_s < self.horizon * scale) continue;
            if (spill_count == self.spill.len) break;
            var duplicate = false;
            for (self.spill[0..spill_count]) |existing| if (existing.?.packet == owner.packet) {
                duplicate = true;
                break;
            };
            if (duplicate) continue;
            self.spill[spill_count] = owner.retain();
            spill_count += 1;
        };
    }
    pub fn pin(self: *const Cache) View {
        var view = View{};
        for (self.targets[0..self.count], 0..) |target, i| if (target.entry) |entry| {
            view.entries[i] = entry.retain();
        };
        for (self.spill, capacity..) |entry, i| if (entry) |owner| {
            view.entries[i] = owner.retain();
        };
        return view;
    }
    /// A prepared window retains only its own picture candidates. Keeping all
    /// four horizons pinned would prevent consumed windows releasing byte quota
    /// while later validation fills the next horizon.
    pub fn pinWindow(self: *const Cache, start: i64, end: i64) View {
        var view = View{};
        var count: usize = 0;
        for (self.targets[0..self.count]) |target| if (target.entry) |entry| {
            const pts = @as(i128, entry.frame.pts) * std.time.ns_per_s;
            if (pts < @as(i128, start) * self.scale or pts >= @as(i128, end) * self.scale) continue;
            view.entries[count] = entry.retain();
            count += 1;
        };
        for (self.spill) |entry| if (entry) |owner| {
            const pts = @as(i128, owner.frame.pts) * std.time.ns_per_s;
            if (pts < @as(i128, start) * self.scale or pts >= @as(i128, end) * self.scale) continue;
            view.entries[count] = owner.retain();
            count += 1;
        };
        return view;
    }
    pub fn observer(self: *Cache) h264.FrameObserver {
        return .{ .context = self, .wants = wants, .accept = accept };
    }
    fn wants(raw: *anyopaque, picture: h264.Picture) bool {
        const self: *Cache = @ptrCast(@alignCast(raw));
        // Independent colour planes are published separately; the ordinary
        // decoder remains authoritative until their combined picture is cached.
        if (picture.independent_plane != null) return false;
        const packet = std.math.add(usize, self.base, picture.first_packet) catch return false;
        for (self.targets[0..self.count]) |target| if (target.prefers(packet, picture.pts, self.scale)) return true;
        if (self.count != 0 and @as(i128, picture.pts) * std.time.ns_per_s >= self.horizon * self.scale) {
            for (self.spill) |entry| if (entry) |owner| {
                if (owner.packet == packet) return false;
            };
            for (self.spill) |entry| if (entry == null) return true;
        }
        return false;
    }
    fn accept(raw: *anyopaque, picture: h264.Picture, frame: *const h264.Frame) void {
        const self: *Cache = @ptrCast(@alignCast(raw));
        self.capture(picture, frame) catch {
            self.declined += 1;
        };
    }
    fn capture(self: *Cache, picture: h264.Picture, frame: *const h264.Frame) !void {
        const bytes = try std.math.add(usize, frame.nv12.len, @sizeOf(Entry));
        const packet = try std.math.add(usize, self.base, picture.first_packet);
        // Retire superseded provisional candidates before requesting replacement
        // storage. External pins keep their bytes charged and immutable.
        for (self.targets[0..self.count]) |*target| if (target.prefers(packet, frame.pts, self.scale)) {
            if (target.entry) |previous| previous.release();
            target.entry = null;
        };
        const future = self.count != 0 and @as(i128, frame.pts) * std.time.ns_per_s >= self.horizon * self.scale;
        const spill_slot = if (future) std.mem.indexOfScalar(?*Entry, &self.spill, null) orelse return error.ResourceLimitExceeded else null;
        // Speculative reorder pictures must never evict an imminent sampling
        // target. Bound their share even when no target has arrived yet.
        if (future) {
            var spill_bytes: usize = 0;
            for (self.spill) |entry| if (entry) |owner| {
                spill_bytes += owner.frame.nv12.len + @sizeOf(Entry);
            };
            if (bytes > self.budget.limit / 4 -| spill_bytes) return error.ResourceLimitExceeded;
        } else {
            // Reclaim speculative owners before declining a selected picture.
            // External immutable pins remain charged and cannot be reclaimed.
            for (&self.spill) |*entry| {
                if (bytes <= self.budget.limit -| self.budget.used.load(.acquire)) break;
                if (entry.*) |owner| owner.release();
                entry.* = null;
            }
            var priority: i128 = std.math.maxInt(i128);
            for (self.targets[0..self.count]) |target| if (target.prefers(packet, frame.pts, self.scale)) {
                priority = @min(priority, target.ns);
            };
            // Farthest-window candidates yield to nearer selections. Release
            // every target reference to a victim so duplicate targets do not
            // accidentally keep its pixels resident.
            var cursor = self.count;
            while (cursor > 0 and bytes > self.budget.limit -| self.budget.used.load(.acquire)) {
                cursor -= 1;
                const victim = self.targets[cursor].entry orelse continue;
                var nearest: i128 = std.math.maxInt(i128);
                for (self.targets[0..self.count]) |target| if (target.entry == victim) {
                    nearest = @min(nearest, target.ns);
                };
                if (nearest <= priority) continue;
                for (self.targets[0..self.count]) |*target| if (target.entry == victim) {
                    target.entry = null;
                    victim.release();
                };
            }
        }
        if (bytes > self.budget.limit -| self.budget.used.load(.acquire)) return error.ResourceLimitExceeded;
        var reservation = if (self.pool) |pool| try pool.acquire(.{ .host_bytes = bytes }) else media.admission.Token{};
        errdefer reservation.deinit();
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        const pixels = try self.allocator.dupe(u8, frame.nv12);
        errdefer self.allocator.free(pixels);
        entry.* = .{ .budget = self.budget, .packet = try std.math.add(usize, self.base, picture.first_packet), .frame = frame.* };
        entry.frame.allocator = self.allocator;
        entry.frame.nv12 = pixels;
        entry.frame.reservation = reservation;
        _ = self.budget.references.fetchAdd(1, .monotonic);
        _ = self.budget.used.fetchAdd(bytes, .acq_rel);
        defer entry.release();
        for (self.targets[0..self.count]) |*target| if (target.prefers(entry.packet, frame.pts, self.scale)) {
            if (target.entry) |previous| previous.release();
            target.entry = entry.retain();
        };
        if (spill_slot) |slot| self.spill[slot] = entry.retain();
        self.hits += 1;
    }
};

test "live coordinator frame cache pressure preserves pinned native depth pixels" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 4096 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var pixels = [_]u8{ 1, 0, 2, 0, 3, 0, 4, 0 };
    const frame = h264.Frame{ .allocator = a, .nv12 = &pixels, .width = 2, .height = 2, .pts = 0, .duration = 1, .timescale = 1, .full_range = true, .color = .{}, .decode_high_water = 0, .decoded_packets = 1, .payload_bytes = 1, .bit_depth = 10, .chroma_format = 0 };
    var cache = try Cache.init(a, &pool, @sizeOf(Entry) + pixels.len);
    defer cache.deinit();
    cache.configure(0, std.time.ns_per_s, std.time.ns_per_s, std.time.ns_per_s, 1, 100, 1);
    const picture = h264.Picture{ .first_packet = 0, .pts = 0, .duration = 1, .width = 2, .height = 2, .bit_depth = 10, .chroma_format = 0 };
    const observer = cache.observer();
    try std.testing.expect(observer.wants(observer.context, picture));
    observer.accept(observer.context, picture, &frame);
    var pins = cache.pin();
    defer pins.deinit();
    cache.clear();
    try std.testing.expectEqualSlices(u8, &pixels, pins.get(100).?.nv12);
    try std.testing.expectEqual(@as(u8, 10), pins.get(100).?.bit_depth);
    cache.configure(0, std.time.ns_per_s, std.time.ns_per_s, std.time.ns_per_s, 1, 100, 1);
    observer.accept(observer.context, picture, &frame);
    try std.testing.expectEqual(@as(usize, 1), cache.declined);
    try std.testing.expectEqualSlices(u8, &pixels, pins.get(100).?.nv12);
}

test "live coordinator cache replaces unpinned reordered candidates at its byte limit" {
    const a = std.testing.allocator;
    var pixels = [_]u8{ 1, 2, 3, 4, 5, 6 };
    var frame = h264.Frame{ .allocator = a, .nv12 = &pixels, .width = 2, .height = 2, .pts = 1, .duration = 1, .timescale = 1, .full_range = true, .color = .{}, .decode_high_water = 0, .decoded_packets = 1, .payload_bytes = 1 };
    var cache = try Cache.init(a, null, @sizeOf(Entry) + pixels.len);
    defer cache.deinit();
    cache.configure(0, 2 * std.time.ns_per_s, 2 * std.time.ns_per_s, 2 * std.time.ns_per_s, 1, 0, 1);
    const observer = cache.observer();
    var picture = h264.Picture{ .first_packet = 1, .pts = 1, .duration = 1, .width = 2, .height = 2, .bit_depth = 8, .chroma_format = 1 };
    observer.accept(observer.context, picture, &frame);
    picture.first_packet = 0;
    picture.pts = 0;
    frame.pts = 0;
    observer.accept(observer.context, picture, &frame);
    var pin = cache.pin();
    defer pin.deinit();
    try std.testing.expect(pin.get(0) != null);
    try std.testing.expectEqual(@as(usize, 0), cache.declined);
}

test "live coordinator cache preserves predictive pictures beyond four windows until reorder drains" {
    const a = std.testing.allocator;
    var pixels = [_]u8{ 1, 2, 3, 4, 5, 6 };
    const frame = h264.Frame{ .allocator = a, .nv12 = &pixels, .width = 2, .height = 2, .pts = 4, .duration = 1, .timescale = 1, .full_range = true, .color = .{}, .decode_high_water = 0, .decoded_packets = 1, .payload_bytes = 1 };
    var cache = try Cache.init(a, null, 4096);
    defer cache.deinit();
    cache.configure(0, std.time.ns_per_s, std.time.ns_per_s, std.time.ns_per_s, 1, 0, 1);
    const picture = h264.Picture{ .first_packet = 40, .pts = 4, .duration = 1, .width = 2, .height = 2, .bit_depth = 8, .chroma_format = 1 };
    const observer = cache.observer();
    try std.testing.expect(observer.wants(observer.context, picture));
    observer.accept(observer.context, picture, &frame);
    cache.configure(0, std.time.ns_per_s, std.time.ns_per_s, std.time.ns_per_s, 1, 0, 1);
    cache.configure(2 * std.time.ns_per_s, std.time.ns_per_s, std.time.ns_per_s, std.time.ns_per_s, 1, 0, 1);
    var pin = cache.pinWindow(4 * std.time.ns_per_s, 5 * std.time.ns_per_s);
    defer pin.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, pin.get(40).?.nv12);
}

test "live coordinator cache prioritizes near selections over speculative and distant pixels" {
    const a = std.testing.allocator;
    const pixels: [256]u8 = @splat(1);
    const bytes = pixels.len + @sizeOf(Entry);
    var cache = try Cache.init(a, null, bytes * 4);
    defer cache.deinit();
    cache.configure(0, std.time.ns_per_s, std.time.ns_per_s, std.time.ns_per_s, 1, 0, 1);
    var frame = h264.Frame{ .allocator = a, .nv12 = @constCast(&pixels), .width = 16, .height = 16, .pts = 4, .duration = 1, .timescale = 1, .full_range = true, .color = .{}, .decode_high_water = 0, .decoded_packets = 1, .payload_bytes = 1 };
    const picture = h264.Picture{ .first_packet = 4, .pts = 4, .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 };
    try cache.capture(picture, &frame);
    // Spill consumes at most a quarter of this budget.
    frame.pts = 5;
    try std.testing.expectError(error.ResourceLimitExceeded, cache.capture(.{ .first_packet = 5, .pts = 5, .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 }, &frame));
    for ([_]i64{ 3, 2, 1, 0 }) |pts| {
        frame.pts = pts;
        try cache.capture(.{ .first_packet = @intCast(pts), .pts = pts, .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 }, &frame);
    }
    var view = cache.pin();
    defer view.deinit();
    try std.testing.expect(view.get(0) != null);
    try std.testing.expect(view.get(4) == null);
    try std.testing.expect(cache.budget.used.load(.acquire) <= bytes * 4);
}
