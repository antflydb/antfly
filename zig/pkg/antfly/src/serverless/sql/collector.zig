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
const storage = @import("objectstore");
const state = @import("state.zig");
const plan = @import("plan.zig");
const digest = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.digestHex;
const A = std.mem.Allocator;
const Progress = struct { phase: u32 = 0, cursor: u32 = 0 };
pub fn collect(a: A, store: state.Store, max_steps: usize) !usize {
    if (max_steps == 0 or max_steps > 256) return error.InvalidSqlExecutionState;
    var client = store.client;
    client.allocator = a;
    const prefix = try std.fmt.allocPrint(a, "{s}/sql-query-expiry/", .{store.prefix});
    defer a.free(prefix);
    const token = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&store.context);
    var listed = try client.listObjects(store.bucket, .{ .prefix = prefix, .max_keys = 16, .cancellation = token });
    defer listed.deinit(a);
    var steps: usize = 0;
    for (listed.entries) |entry| {
        try store.context.ensureActive();
        if (steps == max_steps) break;
        if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len != prefix.len + 20 + 1 + 64 + 5) return error.InvalidSqlExecutionState;
        const expires = try std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10);
        if (expires +| 30_000 >= store.now_ms) break;
        const query = entry.key[prefix.len + 21 ..][0..64].*;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var marker = client.getObject(store.bucket, entry.key, .{ .max_response_bytes = 1024, .cancellation = token }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => continue,
            else => return err,
        };
        defer marker.deinit(a);
        var progress = try std.json.parseFromSliceLeaky(Progress, scratch, marker.body, .{});
        if (progress.phase > 10) return error.InvalidSqlExecutionState;
        const plan_key = try std.fmt.allocPrint(scratch, "{s}/sql-execution/{s}/plan.json", .{ store.prefix, query });
        var object = client.getObject(store.bucket, plan_key, .{ .max_response_bytes = 1024 * 1024, .cancellation = token }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
            else => return err,
        };
        defer if (object) |*value| value.deinit(a);
        const spec = if (object) |value| blk: {
            if (!std.mem.eql(u8, &query, &digest(value.body))) return error.InvalidSqlExecutionState;
            const parsed = try std.json.parseFromSliceLeaky(plan.Plan, scratch, value.body, .{});
            if (parsed.expires_ms != expires or parsed.left.splits > 65536 or parsed.right.splits > 65536 or parsed.partitions > 1024) return error.InvalidSqlExecutionState;
            break :blk parsed;
        } else null;
        const final_stage: u32 = if (spec) |value| value.finalStage() else 0;
        while (steps < max_steps and progress.phase <= final_stage) {
            const count: u32 = if (progress.phase == final_stage) 2 else if (spec) |value| try value.stageTasks(progress.phase) else 0;
            if (progress.cursor >= count) {
                progress.phase += 1;
                progress.cursor = 0;
                continue;
            }
            const key = if (progress.phase < final_stage) try store.key(scratch, query, progress.phase, progress.cursor) else if (progress.cursor == 0) try store.key(scratch, query, null, 0) else plan_key;
            try remove(client, store.bucket, key, token);
            progress.cursor += 1;
            steps += 1;
        }
        if (progress.phase > final_stage) {
            client.compareAndDelete(store.bucket, entry.key, marker.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = token }) catch |err| switch (err) {
                error.NotFound, error.ObjectNotFound, error.FileNotFound, error.PreconditionFailed => {},
                else => return err,
            };
        } else {
            var saved = client.compareAndPut(store.bucket, entry.key, try std.json.Stringify.valueAlloc(scratch, progress, .{}), marker.metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = token }) catch |err| switch (err) {
                error.PreconditionFailed => continue,
                else => return err,
            };
            saved.deinit(a);
        }
    }
    return steps;
}
fn remove(client_value: storage.Client, bucket: []const u8, key: []const u8, token: storage.CancellationToken) !void {
    var client = client_value;
    var metadata = client.statObjectWithOptions(bucket, key, .{ .cancellation = token }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return,
        else => return err,
    };
    defer metadata.deinit(client.allocator);
    client.compareAndDelete(bucket, key, metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = token }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
        else => return err,
    };
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
