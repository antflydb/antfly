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
//! The marker precedes plan creation and survives until the last state object.
const std = @import("std");
const state = @import("state.zig");
const A = std.mem.Allocator;
/// Only expiry-owned namespaces are deleted. Their content-addressed query ID
/// includes the expiry, and every claimant rejects an expired plan. No live
/// generation can reuse these names, so conditional DELETE is unnecessary.
/// Listing the actual expiry-first state root rediscovers delayed writes even
/// after the discovery marker has already been removed.
pub fn collect(a: A, store: state.Store, max_steps: usize) !usize {
    if (max_steps == 0 or max_steps > 256) return error.InvalidSqlExecutionState;
    var client = store.client;
    client.allocator = a;
    const token = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&store.context);
    var steps: usize = 0;
    for ([_][]const u8{ "sql-execution-v2", "sql-query-expiry" }) |lane| {
        if (steps == max_steps) break;
        const prefix = try std.fmt.allocPrint(a, "{s}/{s}/", .{ store.prefix, lane });
        defer a.free(prefix);
        var listed = try client.listObjects(store.bucket, .{ .prefix = prefix, .max_keys = @intCast(max_steps - steps), .cancellation = token });
        defer listed.deinit(a);
        for (listed.entries) |entry| {
            try store.context.ensureActive();
            if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len < prefix.len + 21 + 64 or entry.key[prefix.len + 20] != '/') return error.InvalidSqlExecutionState;
            const expires = try std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10);
            try state.validateDigest(entry.key[prefix.len + 21 ..][0..64].*);
            if (expires +| 30_000 >= store.now_ms) break;
            try client.deleteObject(store.bucket, entry.key, .{ .cancellation = token });
            steps += 1;
        }
    }
    return steps;
}

pub const Collector = struct {
    state_store: state.Store,
    exchange_store: @import("exchange.zig").Store,
    pub fn run(self: Collector, a: A, context: @import("antfly_local_sources").serverless_query_lake_read_context.Context) !usize {
        var state_store = self.state_store;
        state_store.context = context;
        state_store.now_ms = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        const steps = try collect(a, state_store, 64);
        var exchange_store = self.exchange_store;
        exchange_store.context = context;
        return steps + try exchange_store.collect(a, state_store.now_ms, 64);
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
