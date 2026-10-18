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

//! Bounded, disposable plans for immutable sources. Cache ownership and query
//! leases are independent, so eviction never invalidates an active worker.
const std = @import("std");
const A = std.mem.Allocator;
const Region = @import("regions.zig").Region;
const Input = @import("decision.zig").Input;
pub const Plan = struct {
    backing: A,
    owner: ?*Cache = null,
    budget: @import("antfly_regex").Budget,
    arena: std.heap.ArenaAllocator,
    refs: std.atomic.Value(usize) = .init(1),
    key: [32]u8,
    regions: []Region = &.{},
    ids: [][]const u8 = &.{},
    keys: [][64]u8 = &.{},
    path: []const u8 = "",
    context: usize = 0,
    identity: ?[64]u8 = null,
    mutex: std.Io.Mutex = .init,
    pub fn release(self: *Plan) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.backing;
        if (self.owner) |cache| _ = cache.physical.fetchSub(self.budget.live + @sizeOf(Plan), .monotonic);
        self.arena.deinit();
        a.destroy(self);
    }
    pub fn prepare(self: *Plan, client: *@import("decision.zig").Client, bytes: []const u8, lines: []const @import("source_cache.zig").Line) !bool {
        self.mutex.lockUncancelable(client.io);
        defer self.mutex.unlock(client.io);
        if (client.options.no_cache or client.options.cache_dir == null or client.options.cache_ttl_seconds == 0) return false;
        // The empty-input identity includes all query, policy, model, stage and
        // document fields. Full on-disk keys remain byte-for-byte compatible.
        const identity = client.key(.{ .id = "", .input = "" });
        if (self.identity == null or !std.mem.eql(u8, &identity, &self.identity.?)) {
            self.identity = null;
            var scratch = std.heap.ArenaAllocator.init(client.a);
            defer scratch.deinit();
            for (self.keys, 0..) |*key, i| {
                defer _ = scratch.reset(.{ .retain_with_limit = 1024 * 1024 });
                var materialized = self.input(i, bytes, lines, false);
                materialized.input = try materialized.deferred.?.render(scratch.allocator());
                key.* = client.key(materialized);
            }
            self.identity = identity;
        }
        return true;
    }
    pub fn input(self: *const Plan, i: usize, bytes: []const u8, lines: []const @import("source_cache.zig").Line, caching: bool) Input {
        return .{ .id = self.ids[i], .input = "", .derived_key = if (caching) self.keys[i] else null, .deferred = .{ .path = self.path, .bytes = bytes, .lines = lines, .region = self.regions[i], .context = self.context } };
    }
    fn queryInputs(self: *Plan, a: A, client: *@import("decision.zig").Client, bytes: []const u8, lines: []const @import("source_cache.zig").Line) ![]Input {
        const caching = try self.prepare(client, bytes, lines);
        const inputs = try a.alloc(Input, self.regions.len);
        for (inputs, 0..) |*item, i| item.* = self.input(i, bytes, lines, caching);
        return inputs;
    }
};
pub const Cache = struct {
    a: A,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    slots: [16384]?*Plan = @splat(null),
    bytes: usize = 0,
    physical: std.atomic.Value(usize) = .init(0),
    clock: usize = 0,
    pub const limit = 32 * 1024 * 1024;
    const plan_limit = 4 * 1024 * 1024;
    pub fn deinit(self: *Cache) void {
        for (self.slots) |plan| if (plan) |value| value.release();
    }
    pub fn get(self: *Cache, path: []const u8, revision: [64]u8, bytes: []const u8, lines: []const @import("source_cache.zig").Line, index: @import("source.zig").Index, chunk: usize, context: usize) ?*Plan {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&revision);
        var size: [8]u8 = undefined;
        for ([_]usize{ path.len, chunk, context }) |n| {
            std.mem.writeInt(u64, &size, n, .little);
            hash.update(&size);
        }
        hash.update(path);
        var key: [32]u8 = undefined;
        hash.final(&key);
        const slot = std.mem.readInt(u16, key[0..2], .little) % self.slots.len;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.slots[slot]) |plan| if (std.mem.eql(u8, &plan.key, &key)) {
            _ = plan.refs.fetchAdd(1, .monotonic);
            return plan;
        };
        if (self.slots[slot]) |old| {
            self.bytes -= old.budget.live;
            old.release();
            self.slots[slot] = null;
        }
        // Reserve construction headroom, including evicted but still leased
        // plans. Construction is serialized; active leases may release freely.
        var visited: usize = 0;
        while (limit -| self.physical.load(.monotonic) < plan_limit + @sizeOf(Plan) and visited < self.slots.len) : (visited += 1) {
            const victim = self.clock % self.slots.len;
            self.clock +%= 1;
            if (self.slots[victim]) |old| {
                self.bytes -= old.budget.live;
                old.release();
                self.slots[victim] = null;
            }
        }
        const available = limit -| self.physical.load(.monotonic);
        if (available <= @sizeOf(Plan)) return null;
        const plan = self.build(key, path, bytes, lines, index, chunk, context, @min(plan_limit, available - @sizeOf(Plan))) catch return null;
        _ = self.physical.fetchAdd(plan.budget.live + @sizeOf(Plan), .monotonic);
        plan.owner = self;
        self.slots[slot] = plan;
        self.bytes += plan.budget.live;
        _ = plan.refs.fetchAdd(1, .monotonic);
        return plan;
    }
    fn build(self: *Cache, key: [32]u8, path: []const u8, bytes: []const u8, lines: []const @import("source_cache.zig").Line, index: @import("source.zig").Index, chunk: usize, context: usize, allocation_limit: usize) !*Plan {
        const plan = try self.a.create(Plan);
        plan.* = .{ .backing = self.a, .budget = .{ .backing = self.a, .limit = allocation_limit }, .arena = undefined, .key = key };
        plan.arena = .init(plan.budget.allocator());
        errdefer plan.release();
        const a = plan.arena.allocator();
        plan.regions = try @import("regions.zig").buildPrepared(a, bytes, lines, chunk, index);
        plan.ids = try a.alloc([]const u8, plan.regions.len);
        plan.keys = try a.alloc([64]u8, plan.regions.len);
        plan.path = try a.dupe(u8, path);
        plan.context = context;
        for (plan.regions, plan.ids, 0..) |*region, *id, i| {
            region.label = try a.dupe(u8, region.label);
            id.* = try std.fmt.allocPrint(a, "region-{d}", .{i});
        }
        return plan;
    }
};

test "plan leases survive eviction and derived keys track the complete query identity" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var cache: Cache = .{ .a = a, .io = io };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const bytes = "def retry():\n    STOP()\n";
    const lines = try @import("source_cache.zig").lines(temp, bytes, '\n', false);
    const index = try @import("source.zig").parse(temp, "a.py", bytes);
    const plan = cache.get("a.py", @splat('a'), bytes, lines, index, 80, 8).?;
    defer plan.release();
    var options: @import("options.zig").Options = .{ .ask = "retry", .model = "fixture", .cache_dir = "unused" };
    defer options.deinit(a);
    var client = @import("decision.zig").Client.init(a, io, &options);
    defer client.deinit();
    client.stage = .region;
    const first = try plan.queryInputs(temp, &client, bytes, lines);
    const original = first[0].derived_key.?;
    var unplanned = first[0];
    unplanned.derived_key = null;
    unplanned.input = try unplanned.deferred.?.render(temp);
    try std.testing.expectEqualSlices(u8, &client.key(unplanned), &original);
    options.ask = "different";
    const second = try plan.queryInputs(temp, &client, bytes, lines);
    try std.testing.expect(!std.mem.eql(u8, &original, &second[0].derived_key.?));
    try options.only.append(a, "policy");
    const third = try plan.queryInputs(temp, &client, bytes, lines);
    try std.testing.expect(!std.mem.eql(u8, &second[0].derived_key.?, &third[0].derived_key.?));
    options.no_cache = true;
    const uncached = try plan.queryInputs(temp, &client, bytes, lines);
    try std.testing.expect(uncached[0].derived_key == null);
    cache.deinit();
    try std.testing.expectEqualStrings("retry", plan.regions[0].label);
    try std.testing.expect(std.mem.indexOf(u8, try first[0].deferred.?.render(temp), "STOP") != null);
}
