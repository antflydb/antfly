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
    entries: [capacity]?*Entry = @splat(null),
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
                self.targets[self.count] = target;
                self.count += 1;
            }
        }
    }
    pub fn pin(self: *const Cache) View {
        var view = View{};
        for (self.targets[0..self.count], 0..) |target, i| if (target.entry) |entry| {
            view.entries[i] = entry.retain();
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
        const used = self.budget.used.load(.acquire);
        if (bytes > self.budget.limit -| used) return error.ResourceLimitExceeded;
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
