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

//! Expiry-first enumeration and bounded, restartable task/query cleanup.
//! Active discovery hints precede plan creation; GC enumerates actual roots.
const std = @import("std");
const state = @import("state.zig");
const A = std.mem.Allocator;
const exchange = @import("exchange.zig");
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
/// Only expiry-owned namespaces are deleted. Their content-addressed query ID
/// includes the expiry, and every claimant rejects an expired plan. No live
/// generation can reuse these names, so conditional DELETE is unnecessary.
/// Listing the actual expiry-first state root rediscovers delayed writes even
/// after the discovery marker has already been removed.
pub fn collect(a: A, store: state.Store, max_steps: usize) !usize {
    if (max_steps == 0 or max_steps > 256) return error.InvalidSqlExecutionState;
    var steps: usize = 0;
    for ([_][]const u8{ "sql-execution-v2", "sql-query-expiry" }) |lane| {
        if (steps == max_steps) break;
        steps += (try collectLane(a, store, lane, max_steps - steps, store.context)).deleted;
    }
    return steps;
}
fn collectLane(a: A, store: state.Store, lane: []const u8, max_keys: usize, parent: Context) !@import("exchange.zig").Collection {
    var client = store.client;
    client.allocator = a;
    const token = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&store.context);
    const prefix = try std.fmt.allocPrint(a, "{s}/{s}/", .{ store.prefix, lane });
    defer a.free(prefix);
    var listed = client.listObjects(store.bucket, .{ .prefix = prefix, .max_keys = @intCast(max_keys), .cancellation = token }) catch |err| {
        if (try exchange.collectionYield(parent, store.context, err)) return .{ .more = true };
        return err;
    };
    defer listed.deinit(a);
    var deleted: usize = 0;
    for (listed.entries) |entry| {
        store.context.ensureActive() catch |err| {
            if (try exchange.collectionYield(parent, store.context, err)) return .{ .deleted = deleted, .more = true };
            return err;
        };
        if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len < prefix.len + 21 + 64 or entry.key[prefix.len + 20] != '/') return error.InvalidSqlExecutionState;
        const expires = try std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10);
        try state.validateDigest(entry.key[prefix.len + 21 ..][0..64].*);
        if (expires +| 30_000 >= store.now_ms) break;
        client.deleteObject(store.bucket, entry.key, .{ .cancellation = token }) catch |err| switch (err) {
            error.FileNotFound, error.NotFound, error.ObjectNotFound => {},
            else => {
                if (try exchange.collectionYield(parent, store.context, err)) return .{ .deleted = deleted, .more = true };
                return err;
            },
        };
        deleted += 1;
    }
    return .{ .deleted = deleted, .more = deleted != 0 and deleted == listed.entries.len and listed.next_continuation_token != null };
}
pub const Progress = struct {
    state: exchange.Collection = .{},
    discovery: exchange.Collection = .{},
    exchange: @import("exchange.zig").Collection = .{},
    pub fn deleted(self: Progress) usize {
        return self.state.deleted + self.discovery.deleted + self.exchange.deleted;
    }
    pub fn more(self: Progress) bool {
        return self.state.more or self.discovery.more or self.exchange.more;
    }
};

pub const Collector = struct {
    jitter_seed: u64 = 0,
    state_store: state.Store,
    exchange_store: @import("exchange.zig").Store,
    pub fn run(self: Collector, a: A, context: @import("antfly_local_sources").serverless_query_lake_read_context.Context) !usize {
        return (try self.runProgress(a, context)).deleted();
    }
    /// Every namespace gets its own page, including when a sibling is full or
    /// fails. One turn uses at most three LISTs and 192 DELETEs. Errors retain
    /// retry semantics without starving healthy cleanup lanes.
    pub fn runProgress(self: Collector, a: A, context: @import("antfly_local_sources").serverless_query_lake_read_context.Context) !Progress {
        return self.runProgressFrom(a, context, 0);
    }
    pub fn runProgressFrom(self: Collector, a: A, context: Context, first_lane: u8) !Progress {
        if (first_lane >= 3) return error.InvalidSqlExecutionState;
        var parent = context;
        const now = @import("antfly_platform").time.monotonicNs();
        if (parent.deadline_ns == null) parent.deadline_ns = now +| 55 * std.time.ns_per_s;
        try parent.ensureActive();
        const wall_ms: u64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        var progress: Progress = .{};
        var failure: ?anyerror = null;
        for (0..3) |offset| {
            const lane = (@as(usize, first_lane) + offset) % 3;
            const start = @import("antfly_platform").time.monotonicNs();
            var child = parent;
            // Give every unvisited lane a share of the remaining budget and
            // reserve one share for publication. Quantum expiry returns partial
            // progress; it is not a provider failure and must not induce backoff.
            child.deadline_ns = start +| (parent.deadline_ns.? -| start) / (4 - offset);
            const value = if (lane == 2) collect_exchange: {
                var objects = self.exchange_store;
                objects.context = child;
                break :collect_exchange objects.collectPageWithin(a, wall_ms, 64, parent);
            } else collect_state: {
                var records = self.state_store;
                records.context = child;
                records.now_ms = wall_ms;
                break :collect_state collectLane(a, records, if (lane == 0) "sql-execution-v2" else "sql-query-expiry", 64, parent);
            };
            const done = value catch |err| failed: {
                if (failure == null) failure = err;
                break :failed exchange.Collection{};
            };
            switch (lane) {
                0 => progress.state = done,
                1 => progress.discovery = done,
                2 => progress.exchange = done,
                else => unreachable,
            }
        }
        try parent.ensureActive();
        if (failure) |err| return err;
        return progress;
    }
};

test "distributed SQL expiry roots rediscover writes arriving after discovery cleanup" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var store: state.Store = .{ .client = memory.client(), .bucket = "execution", .prefix = "queries", .now_ms = 0 };
    const query: state.Query = .{ .id = @splat('a'), .expires_ms = 1000 };
    const key = try store.key(sa, query, null, 0);
    try store.create(sa, key, query.id, query.expires_ms);
    var client = memory.client();
    var original = try client.getObject("execution", key, .{});
    defer original.deinit(a);
    store.now_ms = 31001;
    try std.testing.expectEqual(@as(usize, 1), try collect(sa, store, 1));
    // No discovery marker remains. An indeterminate earlier PUT arrives late.
    var late = try client.putObject("execution", key, original.body, .{ .if_none_match = true });
    late.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), try collect(sa, store, 1));
    try std.testing.expect((try store.load(sa, key)) == null);
}

/// Process-owned scheduling, separate from the legacy maintenance tick. Empty
/// walks back off while nonempty pages drain promptly; failed passes are retried.
pub const Schedule = struct {
    next_ms: u64 = 0,
    delay_ms: u32 = 100,
    next_lane: u8 = 0,
    pub fn waitMs(self: Schedule, now_ms: u64) u64 {
        return @max(@as(u64, 1), self.next_ms -| now_ms);
    }
    fn deferPass(self: *Schedule, collector: Collector, now_ms: u64) void {
        self.delay_ms = @min(@as(u32, 30_000), self.delay_ms * 2);
        const jitter = if (collector.jitter_seed == 0) 0 else std.hash.Wyhash.hash(collector.jitter_seed, std.mem.asBytes(&now_ms)) % @max(@as(u32, 1), self.delay_ms / 8);
        self.next_ms = now_ms +| self.delay_ms +| jitter;
    }
    pub fn runAt(self: *Schedule, a: A, collector: Collector, context: @import("antfly_local_sources").serverless_query_lake_read_context.Context, now_ms: u64) !usize {
        try context.ensureActive();
        if (now_ms < self.next_ms) return 0;
        errdefer self.deferPass(collector, now_ms);
        const first = self.next_lane;
        // Advance before I/O so an interrupted pass cannot pin the first lane.
        self.next_lane = (first + 1) % 3;
        const progress = try collector.runProgressFrom(a, context, first);
        const steps = progress.deleted();
        if (steps == 0 and !progress.more()) self.deferPass(collector, now_ms) else {
            self.delay_ms = 100;
            self.next_ms = now_ms +| 1;
        }
        return steps;
    }
};

test "distributed SQL collection reports independent lane continuation and preserves live objects" {
    const a = std.testing.allocator;
    for (0..4) |busy| {
        var memory = @import("objectstore").MemoryClient.init(a);
        defer memory.deinit();
        var client = memory.client();
        const prefixes = [_][]const u8{ "sql/sql-execution-v2/", "sql/sql-query-expiry/", "sql/sql-exchange/" };
        for (prefixes, 0..) |prefix, lane| {
            if (busy == lane or busy == 3) for (0..130) |id| {
                const key = try std.fmt.allocPrint(a, "{s}{d:0>20}/{s}/{d}.json", .{ prefix, @as(u64, 1), @as([64]u8, @splat('a')), id });
                defer a.free(key);
                var put = try client.putObject("execution", key, "{}", .{});
                put.deinit(a);
            };
            const live = try std.fmt.allocPrint(a, "{s}{d:0>20}/{s}/live.json", .{ prefix, std.math.maxInt(u64), @as([64]u8, @splat('b')) });
            defer a.free(live);
            var put = try client.putObject("execution", live, "{}", .{});
            put.deinit(a);
        }
        var source = try client.putObject("source", "artifacts/keep", "source", .{});
        source.deinit(a);
        const collector: Collector = .{ .state_store = .{ .client = client, .bucket = "execution", .prefix = "sql", .now_ms = 0 }, .exchange_store = .{ .client = client, .bucket = "execution", .prefix = "sql" } };
        var deleted: usize = 0;
        for (0..3) |turn| {
            memory.resetOperationCount();
            const progress = try collector.runProgress(a, .{});
            const lanes = [_]@import("exchange.zig").Collection{ progress.state, progress.discovery, progress.exchange };
            for (lanes, 0..) |lane, id| {
                const count: usize = if (busy == id or busy == 3) (if (turn < 2) 64 else 2) else 0;
                try std.testing.expectEqual(count, lane.deleted);
                try std.testing.expectEqual(count == 64, lane.more);
            }
            try std.testing.expectEqual(turn < 2, progress.more());
            try std.testing.expect(memory.operationCount() <= 195);
            deleted += progress.deleted();
        }
        try std.testing.expectEqual(@as(usize, if (busy == 3) 390 else 130), deleted);
        const idle = try collector.runProgress(a, .{});
        try std.testing.expectEqual(@as(usize, 0), idle.deleted());
        try std.testing.expect(!idle.more());
        var leftovers = try client.listObjects("execution", .{ .prefix = "sql/", .max_keys = 256 });
        defer leftovers.deinit(a);
        try std.testing.expectEqual(@as(usize, 3), leftovers.entries.len);
        var retained = try client.getObject("source", "artifacts/keep", .{});
        defer retained.deinit(a);
        try std.testing.expectEqualStrings("source", retained.body);
    }
}

test "distributed SQL collection attempts healthy lanes after an independent lane failure" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    for ([_][]const u8{ "sql-execution-v2", "sql-query-expiry", "sql-exchange" }) |lane| {
        const key = try std.fmt.allocPrint(a, "sql/{s}/{d:0>20}/{s}/expired.json", .{ lane, @as(u64, 1), @as([64]u8, @splat('a')) });
        defer a.free(key);
        var put = try client.putObject("execution", key, "{}", .{});
        put.deinit(a);
    }
    const Fault = struct {
        var base: @FieldType(@import("objectstore").Client.VTable, "delete_object") = undefined;
        fn remove(raw: *anyopaque, bucket: []const u8, key: []const u8, options: @import("objectstore").DeleteOptions) !void {
            if (std.mem.indexOf(u8, key, "/sql-execution-v2/") != null) return error.AccessDenied;
            return base(raw, bucket, key, options);
        }
    };
    Fault.base = client.vtable.delete_object;
    var table = client.vtable.*;
    table.delete_object = Fault.remove;
    client.vtable = &table;
    const collector: Collector = .{ .state_store = .{ .client = client, .bucket = "execution", .prefix = "sql", .now_ms = 0 }, .exchange_store = .{ .client = client, .bucket = "execution", .prefix = "sql" } };
    try std.testing.expectError(error.AccessDenied, collector.runProgress(a, .{}));
    var remaining = try client.listObjects("execution", .{ .prefix = "sql/", .max_keys = 256 });
    defer remaining.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), remaining.entries.len);
    try std.testing.expect(std.mem.indexOf(u8, remaining.entries[0].key, "/sql-execution-v2/") != null);
}

test "distributed SQL collection quantum yields preserve real cancellation and provider failures" {
    const now = @import("antfly_platform").time.monotonicNs();
    const parent: Context = .{ .deadline_ns = now + std.time.ns_per_s };
    const child: Context = .{ .deadline_ns = 0 };
    try std.testing.expect(try exchange.collectionYield(parent, child, error.Canceled));
    try std.testing.expect(!try exchange.collectionYield(parent, parent, error.Canceled));
    try std.testing.expect(!try exchange.collectionYield(parent, child, error.AccessDenied));
    var cancelled = std.atomic.Value(bool).init(true);
    const canceled_parent: Context = .{ .cancellation = @import("antfly_local_sources").storage_object_storage.CancellationToken.fromAtomic(&cancelled) };
    try std.testing.expectError(error.Canceled, exchange.collectionYield(canceled_parent, child, error.Canceled));
    try std.testing.expectError(error.DeadlineExceeded, exchange.collectionYield(child, child, error.Canceled));
}
