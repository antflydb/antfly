// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Supervised bounded maintenance. Remote CAS state retains the exact request
//! across retries; foreground publication never depends on job success.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.serverless_external_source_mod.lake_catalog;
const configured = @import("../serverless/configured_object_store_support.zig");
const ingestion = @import("../serverless/lake_ingestion.zig");
const maintenance = @import("lake_maintenance.zig");
const A = std.mem.Allocator;
pub const Policy = struct {
    enabled: bool = false,
    interval_ms: u64 = 60 * 60 * 1000,
    compact: bool = true,
    wal_gc: bool = true,
    vacuum: bool = false,
    exclusive_ownership: bool = false,
    max_rows: u64 = 16384,
    max_bytes: u64 = 32 * 1024 * 1024,
    max_deleted: usize = 128,
    retain_ms: u64 = 7 * 24 * 60 * 60 * 1000,
    keep_latest: usize = 2,
    pub fn validate(self: Policy, rest: bool) !void {
        if (self.interval_ms < 60000 or self.interval_ms > 30 * 24 * 60 * 60 * 1000 or self.max_rows == 0 or self.max_rows > 16384 or self.max_bytes == 0 or self.max_bytes > 32 * 1024 * 1024 or self.max_deleted == 0 or self.max_deleted > 4096 or self.retain_ms < 600000 or self.keep_latest < 1 or self.keep_latest > 1024) return error.InvalidLakeMaintenanceLimits;
        // Automatic REST vacuum cannot prove external metadata writers quiesced.
        if (self.vacuum and (!self.exclusive_ownership or rest)) return error.LakeMaintenanceOwnershipRequired;
    }
};
const State = struct {
    sequence: u64 = 0,
    stage: u8 = 0,
    next_ms: u64 = 0,
    lease_until_ms: u64 = 0,
    request: ?[]const u8 = null,
    last_result: ?[]const u8 = null,
    failures: u32 = 0,
    last_error: ?[]const u8 = null,
};
pub fn run(a: A, server: *@import("http_server.zig").ApiHttpServer, table: local.common_topology_records.TableRecord, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, parent: catalog.types.Context) !bool {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var loaded = try configured.executeLakeCatalogAlloc(a, binding, options, parent, .load);
    defer loaded.deinit(a);
    const root = try catalog.metadata.parse(scratch, loaded.table.metadata_json);
    const property = (try catalog.metadata.get(root, "properties")).object.get("antfly.maintenance.policy") orelse return false;
    const policy = try std.json.parseFromSliceLeaky(Policy, scratch, try catalog.metadata.str(property), .{});
    try policy.validate(binding.catalog.?.type == .rest);
    if (!policy.enabled) return false;
    var opened = try ingestion.openQueue(a, binding, options);
    defer opened.deinit();
    var client = opened.client;
    const key = try std.fmt.allocPrint(scratch, "{s}/maintenance/scheduler.json", .{try ingestion.prefix(scratch, opened.prefix, binding, options)});
    var saved = client.getObject(opened.bucket, key, .{ .cancellation = catalog.types.contextCancellation(&parent) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    if (saved) |value| if (value.body.len > 65536) return error.InvalidLakeMaintenanceLimits;
    var state: State = if (saved) |value| try std.json.parseFromSliceLeaky(State, scratch, value.body, .{}) else .{};
    if (state.stage > 3 or (state.request != null and state.request.?.len > 16384)) return error.InvalidLakeMaintenanceLimits;
    if (state.request != null) {
        const enabled = switch (state.stage) {
            0 => policy.compact,
            1 => policy.wal_gc,
            2 => policy.vacuum,
            else => false,
        };
        if (!enabled) return false;
    }
    const now: u64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    if (state.lease_until_ms > now or state.next_ms > now) return false;
    if (state.request == null) {
        while (state.stage < 3) : (state.stage += 1) {
            const enabled = switch (state.stage) {
                0 => policy.compact,
                1 => policy.wal_gc,
                2 => policy.vacuum,
                else => unreachable,
            };
            if (enabled) break;
        }
        if (state.stage == 3) {
            state.stage = 0;
            state.sequence = try std.math.add(u64, state.sequence, 1);
            state.next_ms = now +| policy.interval_ms;
        } else {
            const action = switch (state.stage) {
                0 => "compact",
                1 => "wal_gc",
                2 => "vacuum",
                else => unreachable,
            };
            state.request = try std.json.Stringify.valueAlloc(scratch, .{ .action = action, .operation_id = try std.fmt.allocPrint(scratch, "scheduled-{d}-{s}", .{ state.sequence, action }), .dry_run = false, .exclusive_ownership = policy.exclusive_ownership, .max_rows = policy.max_rows, .max_bytes = policy.max_bytes, .max_deleted = policy.max_deleted, .retain_ms = policy.retain_ms, .keep_latest = policy.keep_latest }, .{});
        }
    }
    // Work has a shorter monotonic deadline than the ownership lease. Allow
    // clock skew using the same grace contract as reader leases.
    state.lease_until_ms = now +| 120000;
    var claimed = client.putObject(opened.bucket, key, try std.json.Stringify.valueAlloc(scratch, state, .{}), .{ .if_none_match = saved == null, .if_match_etag = if (saved) |value| value.metadata.etag orelse return error.MissingObjectEtag else null, .cancellation = catalog.types.contextCancellation(&parent) }) catch |err| switch (err) {
        error.PreconditionFailed, error.ConditionalCheckFailed => return false,
        else => return err,
    };
    defer claimed.deinit(client.allocator);
    var mutated = false;
    if (state.request) |request| {
        var context = parent;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
        context.deadline_ns = if (context.deadline_ns) |existing| @min(existing, deadline) else deadline;
        if (maintenance.execute(a, server, table, binding, options, context, .{}, request)) |result| {
            defer a.free(result.body);
            state.last_result = try scratch.dupe(u8, result.body);
            state.last_error = null;
            state.failures = 0;
            mutated = result.mutated;
            const completed = try std.json.parseFromSliceLeaky(std.json.Value, scratch, result.body, .{});
            const complete = if (completed.object.get("complete")) |value| value == .bool and value.bool else true;
            if (complete) {
                state.request = null;
                state.stage += 1;
            }
        } else |err| {
            state.failures +|= 1;
            state.last_error = @errorName(err);
            state.next_ms = now +| @min(@as(u64, 300000), @as(u64, 1000) << @intCast(@min(state.failures, 8)));
        }
    }
    state.lease_until_ms = 0;
    var completed = try client.putObject(opened.bucket, key, try std.json.Stringify.valueAlloc(scratch, state, .{}), .{ .if_match_etag = claimed.etag orelse return error.MissingObjectEtag, .cancellation = catalog.types.contextCancellation(&parent) });
    completed.deinit(client.allocator);
    return mutated;
}

test "external lake scheduler requires bounded policy and prohibits automatic REST vacuum" {
    try (Policy{}).validate(false);
    try std.testing.expectError(error.InvalidLakeMaintenanceLimits, (Policy{ .interval_ms = 1 }).validate(false));
    try std.testing.expectError(error.LakeMaintenanceOwnershipRequired, (Policy{ .vacuum = true }).validate(false));
    try std.testing.expectError(error.LakeMaintenanceOwnershipRequired, (Policy{ .vacuum = true, .exclusive_ownership = true }).validate(true));
    try (Policy{ .vacuum = true, .exclusive_ownership = true }).validate(false);
}

pub fn status(a: A, binding: local.serverless_external_source_catalog_binding.Binding, options: configured.BindingObjectStoreOpenOptions, context: catalog.types.Context) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var loaded = try configured.executeLakeCatalogAlloc(a, binding, options, context, .load);
    defer loaded.deinit(a);
    const root = try catalog.metadata.parse(scratch, loaded.table.metadata_json);
    const property = (try catalog.metadata.get(root, "properties")).object.get("antfly.maintenance.policy");
    const policy = if (property) |value| try std.json.parseFromSliceLeaky(Policy, scratch, try catalog.metadata.str(value), .{}) else Policy{};
    var opened = try ingestion.openQueue(a, binding, options);
    defer opened.deinit();
    var client = opened.client;
    const key = try std.fmt.allocPrint(scratch, "{s}/maintenance/scheduler.json", .{try ingestion.prefix(scratch, opened.prefix, binding, options)});
    var saved = client.getObject(opened.bucket, key, .{ .cancellation = catalog.types.contextCancellation(&context) }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    if (saved) |value| if (value.body.len > 65536) return error.InvalidLakeMaintenanceLimits;
    const state: ?State = if (saved) |value| try std.json.parseFromSliceLeaky(State, scratch, value.body, .{}) else null;
    return std.json.Stringify.valueAlloc(a, .{ .policy = policy, .state = state }, .{});
}
