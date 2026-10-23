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

//! Server-owned immutable accepted-WAL corpora. Exact cuts and recipes isolate
//! entries; query masks and request metadata remain private. Four 64 MiB lanes
//! bound retained state and builds. Saturation uses the ordinary bounded path.
const std = @import("std");
const local = @import("antfly_local_sources");
const overlay_api = @import("lake_search_overlay.zig");
const Source = local.storage_db_query_search_exec.PinnedTextSource;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
pub const Cache = struct {
    const Entry = struct { key: [32]u8, family: [32]u8, pin: ?Source = null, used: u64 = 0 };
    mutex: std.atomic.Mutex = .unlocked,
    entries: [4]?Entry = @splat(null),
    tick: u64 = 0,
    builds: usize = 0,
    heap: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 256 * 1024 * 1024 },
    managed: ?local.storage_resource_manager.BudgetedAllocator = null,
    pub fn attach(self: *Cache, manager: *local.storage_resource_manager.ResourceManager) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.managed != null) return;
        std.debug.assert(self.heap.live == 0);
        self.managed = local.storage_resource_manager.BudgetedAllocator.init(manager, .full_text_segment_residency, @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), 1);
        self.heap.backing = self.managed.?.allocator();
    }
    fn private(self: *Cache, overlay: *const overlay_api.Overlay, name: []const u8, analysis: local.introducer.TextAnalysisConfig, schema: ?local.storage_schema.TableSchema, field: ?[]const u8, context: Context) !Source {
        return overlay.recent(self.heap.backing, name, analysis, schema, field, context);
    }
    fn lock(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    pub fn deinit(self: *Cache) void {
        for (&self.entries) |*slot| if (slot.*) |*entry| {
            std.debug.assert(entry.pin != null);
            entry.pin.?.deinit();
            slot.* = null;
        };
        std.debug.assert(self.heap.live == 0);
        if (self.managed) |*budget| budget.deinit();
    }
    pub fn family(scope: [32]u8, table_id: u64, object_generation: u64, source: []const u8, snapshot: []const u8, schema: []const u8, config: []const u8, name: []const u8) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&scope);
        var table: [16]u8 = undefined;
        std.mem.writeInt(u64, table[0..8], table_id, .little);
        std.mem.writeInt(u64, table[8..16], object_generation, .little);
        hash.update(&table);
        for ([_][]const u8{ source, snapshot, schema, config, name }) |part| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, part.len, .little);
            hash.update(&length);
            hash.update(part);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn key(scope: [32]u8, table_id: u64, object_generation: u64, source: []const u8, snapshot: []const u8, schema: []const u8, config: []const u8, name: []const u8, overlay: *const overlay_api.Overlay) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&family(scope, table_id, object_generation, source, snapshot, schema, config, name));
        hash.update(&overlay.contentIdentity());
        var cut: [16]u8 = undefined;
        std.mem.writeInt(u64, cut[0..8], overlay.pending.base_lsn, .little);
        std.mem.writeInt(u64, cut[8..16], overlay.pending.lsn, .little);
        hash.update(&cut);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn acquire(self: *Cache, identity: [32]u8, overlay: *const overlay_api.Overlay, name: []const u8, analysis: local.introducer.TextAnalysisConfig, schema: ?local.storage_schema.TableSchema, field: ?[]const u8, context: Context) !Source {
        return self.acquireFamily(identity, identity, overlay, name, analysis, schema, field, context);
    }
    pub fn acquireFamily(self: *Cache, identity: [32]u8, authority: [32]u8, overlay: *const overlay_api.Overlay, name: []const u8, analysis: local.introducer.TextAnalysisConfig, schema: ?local.storage_schema.TableSchema, field: ?[]const u8, context: Context) !Source {
        while (true) {
            try context.ensureActive();
            self.lock();
            self.tick +%= 1;
            var waiting = false;
            for (&self.entries) |*slot| if (slot.*) |*entry| {
                if (std.mem.eql(u8, &entry.key, &identity)) {
                    if (entry.pin) |pin| {
                        var result = pin.clone() catch unreachable;
                        entry.used = self.tick;
                        self.mutex.unlock();
                        result.name = name;
                        result.text_analysis = analysis;
                        result.runtime_schema = schema;
                        result.selected_field = field;
                        return result;
                    }
                    waiting = true;
                    break;
                }
            };
            if (waiting) {
                self.mutex.unlock();
                if (context.io) |io| {
                    try io.sleep(.fromMilliseconds(1), .awake);
                    continue;
                }
                return self.private(overlay, name, analysis, schema, field, context);
            }
            var target: ?usize = null;
            var oldest: u64 = std.math.maxInt(u64);
            for (self.entries, 0..) |slot, i| {
                if (slot == null) {
                    target = i;
                    break;
                }
                if (slot.?.pin != null and slot.?.used <= oldest) {
                    oldest = slot.?.used;
                    target = i;
                }
            }
            const index = target orelse {
                self.mutex.unlock();
                return self.private(overlay, name, analysis, schema, field, context);
            };
            var previous: ?Source = null;
            var used: u64 = 0;
            for (self.entries) |slot| if (slot) |entry| if (entry.pin) |pin| {
                if (std.mem.eql(u8, &entry.family, &authority) and entry.used >= used) {
                    if (previous) |*held| held.deinit();
                    previous = pin.clone() catch unreachable;
                    used = entry.used;
                }
            };
            defer if (previous) |*held| held.deinit();
            var retired = self.entries[index];
            self.entries[index] = .{ .key = identity, .family = authority, .used = self.tick };
            self.builds += 1;
            self.mutex.unlock();
            if (retired) |*entry| if (entry.pin) |*pin| pin.deinit();
            var pin = overlay.recentIncremental(self.heap.allocator(), name, analysis, schema, field, context, previous) catch |err| {
                self.lock();
                self.entries[index] = null;
                self.mutex.unlock();
                if (err == error.OutOfMemory or err == error.LakeOverlayTooLarge) return self.private(overlay, name, analysis, schema, field, context);
                return err;
            };
            errdefer {
                pin.deinit();
                self.lock();
                self.entries[index] = null;
                self.mutex.unlock();
            }
            // The cached owner contains no request-arena metadata or capability.
            pin.name = "";
            pin.text_analysis = .{};
            pin.runtime_schema = null;
            pin.selected_field = null;
            pin = try pin.share(std.heap.page_allocator);
            self.lock();
            self.entries[index].?.pin = pin;
            var result = pin.clone() catch unreachable;
            self.mutex.unlock();
            result.name = name;
            result.text_analysis = analysis;
            result.runtime_schema = schema;
            result.selected_field = field;
            return result;
        }
    }
};

test "external lake global frontier recent corpora share exact cuts and own no request metadata" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var overlay: overlay_api.Overlay = .{ .pending = .{ .base_lsn = 4, .lsn = 8, .key_fields = &.{"id"}, .changes = &.{} }, .key_filter = "" };
    try overlay.rows.put(sa, "wal1:row", try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"shared recent text\"}", .{}));
    var cache: Cache = .{};
    defer cache.deinit();
    const identity = Cache.key(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text", &overlay);
    const reset_queue = Cache.key(@splat(1), 7, 1, "table", "snapshot", "schema", "recipe", "text", &overlay);
    const different_base = Cache.key(@splat(1), 7, 0, "table", "other-snapshot", "schema", "recipe", "text", &overlay);
    try std.testing.expect(!std.mem.eql(u8, &identity, &reset_queue));
    try std.testing.expect(!std.mem.eql(u8, &identity, &different_base));
    var first = try cache.acquire(identity, &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
    defer first.deinit();
    var second = try cache.acquire(identity, &overlay, "different borrowed name", .{}, null, "body", .{ .io = std.testing.io });
    defer second.deinit();
    try std.testing.expect(first.snapshot == second.snapshot);
    try std.testing.expectEqual(@as(usize, 1), cache.builds);
    try std.testing.expectEqualStrings("different borrowed name", second.name);
    // A reset queue may reuse the same LSNs and stable keys for different
    // accepted images. Verify that a new generation builds its own corpus.
    var reset: overlay_api.Overlay = .{ .pending = overlay.pending, .key_filter = "" };
    try reset.rows.put(sa, "wal1:row", try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"changed state\"}", .{}));
    var reset_pin = try cache.acquire(reset_queue, &reset, "text", .{}, null, "body", .{ .io = std.testing.io });
    defer reset_pin.deinit();
    try std.testing.expect(first.snapshot != reset_pin.snapshot);
    const query: local.search_search.SearchQuery = .{ .match = .{ .field = "body", .text = "shared" } };
    var old_matches = try local.search_search.execute(a, first.snapshot, .{ .query = query, .k = 10, .include_stored = false });
    defer old_matches.deinit();
    var reset_matches = try local.search_search.execute(a, reset_pin.snapshot, .{ .query = query, .k = 10, .include_stored = false });
    defer reset_matches.deinit();
    try std.testing.expectEqual(@as(usize, 1), old_matches.hits.len);
    try std.testing.expectEqual(@as(usize, 0), reset_matches.hits.len);
    overlay.pending.base_lsn = 5;
    const other = Cache.key(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text", &overlay);
    try std.testing.expect(!std.mem.eql(u8, &identity, &other));
    var third = try cache.acquire(other, &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
    defer third.deinit();
    try std.testing.expect(third.snapshot != first.snapshot);
    for (0..6) |i| {
        overlay.pending.lsn = 10 + i;
        const next = Cache.key(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text", &overlay);
        var pin = try cache.acquire(next, &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
        pin.deinit();
    }
    try std.testing.expectEqual(@as(usize, 1), first.snapshot.segments.len);
    try first.snapshot.checkScoringReadContext();
}

test "external lake global frontier concurrent recent admissions single flight and fall back under pressure" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var overlay: overlay_api.Overlay = .{ .pending = .{ .lsn = 9, .key_fields = &.{"id"}, .changes = &.{} }, .key_filter = "" };
    const row = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"common shared recent text\"}", .{});
    for (0..1000) |i| try overlay.rows.put(sa, try std.fmt.allocPrint(sa, "wal1:{d}", .{i}), row);
    var cache: Cache = .{};
    defer cache.deinit();
    const identity = Cache.key(@splat(2), 7, 0, "source", "snapshot", "schema", "recipe", "text", &overlay);
    const Worker = struct {
        fn run(shared: *Cache, key_: [32]u8, suffix: *const overlay_api.Overlay, context: Context) anyerror!Source {
            return shared.acquire(key_, suffix, "text", .{}, null, "body", context);
        }
    };
    var one = try io.concurrent(Worker.run, .{ &cache, identity, &overlay, Context{ .io = io } });
    var one_joined = false;
    defer if (!one_joined) {
        if (one.cancel(io)) |pin_| {
            var pin = pin_;
            pin.deinit();
        } else |_| {}
    };
    var two = try io.concurrent(Worker.run, .{ &cache, identity, &overlay, Context{ .io = io } });
    var two_joined = false;
    defer if (!two_joined) {
        if (two.cancel(io)) |pin_| {
            var pin = pin_;
            pin.deinit();
        } else |_| {}
    };
    one_joined = true;
    var first = try one.await(io);
    defer first.deinit();
    two_joined = true;
    var second = try two.await(io);
    defer second.deinit();
    try std.testing.expect(first.snapshot == second.snapshot);
    try std.testing.expectEqual(@as(usize, 1), cache.builds);
    try std.testing.expectError(error.DeadlineExceeded, cache.acquire(identity, &overlay, "text", .{}, null, "body", .{ .deadline_ns = 0 }));
    var pressure: Cache = .{};
    defer pressure.deinit();
    pressure.heap.limit = 1;
    var uncached = try pressure.acquire(identity, &overlay, "text", .{}, null, "body", .{ .io = io });
    defer uncached.deinit();
    try std.testing.expectEqual(@as(usize, 0), pressure.heap.live);
    for (pressure.entries) |entry| try std.testing.expect(entry == null);
    try std.testing.expectEqual(first.snapshot.liveDocCount(), uncached.snapshot.liveDocCount());
}

test "external lake global frontier recent cuts isolate restores and reuse unchanged prefix runs" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var cache: Cache = .{};
    defer cache.deinit();
    var overlay: overlay_api.Overlay = .{ .pending = .{ .base_lsn = 4, .lsn = 8, .key_fields = &.{"id"}, .changes = &.{} }, .key_filter = "" };
    const first = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"old shared text\"}", .{});
    const changed = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"replacement text\"}", .{});
    try overlay.rows.put(sa, "wal1:00first", first);
    try overlay.rows.put(sa, "wal1:fflast", first);
    const family_ = Cache.family(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text");
    const old_key = Cache.key(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text", &overlay);
    var old = try cache.acquireFamily(old_key, family_, &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
    defer old.deinit();
    try overlay.rows.put(sa, "wal1:00first", changed);
    const new_key = Cache.key(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text", &overlay);
    try std.testing.expect(!std.mem.eql(u8, &old_key, &new_key));
    var next = try cache.acquireFamily(new_key, family_, &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
    defer next.deinit();
    const metadata: *const overlay_api.Overlay.RecentMetadata = @ptrCast(@alignCast(next.provider_metadata.?));
    try std.testing.expectEqual(@as(usize, 1), metadata.rebuilt_rows);
    try std.testing.expectEqual(@as(usize, 1), metadata.reused_segments);
    const query: local.search_search.SearchQuery = .{ .match = .{ .field = "body", .text = "old" } };
    var old_matches = try local.search_search.execute(a, old.snapshot, .{ .query = query, .k = 10, .include_stored = false });
    defer old_matches.deinit();
    var next_matches = try local.search_search.execute(a, next.snapshot, .{ .query = query, .k = 10, .include_stored = false });
    defer next_matches.deinit();
    try std.testing.expectEqual(@as(usize, 2), old_matches.hits.len);
    try std.testing.expectEqual(@as(usize, 1), next_matches.hits.len);
    // Run owners retain only their own immutable data: many successive cuts
    // keep sharing the unchanged prefix without pinning a chain of old cuts.
    for (0..20) |i| {
        const row_json = try std.fmt.allocPrint(sa, "{{\"body\":\"replacement text {d}\"}}", .{i});
        try overlay.rows.put(sa, "wal1:00first", try std.json.parseFromSliceLeaky(std.json.Value, sa, row_json, .{}));
        overlay.pending.lsn += 1;
        const key_ = Cache.key(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text", &overlay);
        var current = try cache.acquireFamily(key_, family_, &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
        defer current.deinit();
        const reused: *const overlay_api.Overlay.RecentMetadata = @ptrCast(@alignCast(current.provider_metadata.?));
        try std.testing.expectEqual(@as(usize, 1), reused.reused_segments);
        try std.testing.expectEqual(@as(usize, 1), reused.rebuilt_rows);
    }
    try std.testing.expect(overlay.rows.remove("wal1:00first"));
    overlay.pending.lsn += 1;
    const delete_key = Cache.key(@splat(1), 7, 0, "table", "snapshot", "schema", "recipe", "text", &overlay);
    var deleted = try cache.acquireFamily(delete_key, family_, &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
    defer deleted.deinit();
    const deletion: *const overlay_api.Overlay.RecentMetadata = @ptrCast(@alignCast(deleted.provider_metadata.?));
    try std.testing.expectEqual(@as(usize, 0), deletion.rebuilt_rows);
    try std.testing.expectEqual(@as(usize, 1), deletion.reused_segments);
    try std.testing.expectEqual(@as(u32, 1), deleted.snapshot.liveDocCount());
    for (0..7) |i| {
        var held = try cache.acquireFamily(@splat(@intCast(i + 2)), @splat(@intCast(i + 2)), &overlay, "text", .{}, null, "body", .{ .io = std.testing.io });
        held.deinit();
    }
    try std.testing.expectEqual(@as(u32, 2), next.snapshot.liveDocCount());
}
