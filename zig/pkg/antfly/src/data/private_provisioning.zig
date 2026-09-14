// Copyright 2026 Antfly, Inc.
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

//! Private placement proof validation. Results are request-arena owned and
//! never enter the public routing snapshot cache.
const std = @import("std");
const staging = @import("../metadata/restore_staging.zig");
const tables = @import("../metadata/table_manager.zig");
pub const Owner = struct { plan_id: [16]u8, plan_digest: [32]u8, table: tables.TableRecord, range: tables.RangeRecord, scope: @import("../storage/db/restore_staging.zig").Scope, cancel_recovery: bool = false };

pub fn validate(alloc: std.mem.Allocator, public_tables: []const tables.TableRecord, public_ranges: []const tables.RangeRecord, projection: staging.ProvisioningProjection) ![]Owner {
    if (projection.jobs_json.len > staging.max_active_attempts) return error.InvalidRestoreStaging;
    var owners = std.ArrayListUnmanaged(Owner).empty;
    var seen_groups: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (projection.jobs_json) |bytes| {
        if (bytes.len > staging.max_encoded_bytes) return error.InvalidRestoreStaging;
        const parsed = try std.json.parseFromSlice(staging.Job, alloc, bytes, .{ .allocate = .alloc_always });
        try parsed.value.plan.validate(alloc);
        if (!std.mem.eql(u8, &parsed.value.plan_digest, &(try parsed.value.plan.digest(alloc)))) return error.InvalidRestoreStaging;
        switch (parsed.value.state) {
            .importing, .validating, .cutover, .canceling => {},
            .published, .canceled => return error.InvalidRestoreStaging,
        }
        if (parsed.value.revision == 0) return error.InvalidRestoreStaging;
        var new_owners: usize = 0;
        var old_owners: usize = 0;
        for (parsed.value.plan.targets) |target| {
            new_owners += target.ranges.len;
            if (target.replace) |old| old_owners += old.ranges.len;
        }
        const maximum = switch (parsed.value.state) {
            .cutover => old_owners,
            .canceling => new_owners + old_owners,
            else => new_owners,
        };
        if (parsed.value.completed_owners > maximum) return error.InvalidRestoreStaging;
        for (parsed.value.plan.targets) |target| {
            var expected = target.table;
            expected.restore_backup_id = "";
            expected.restore_location = "";
            expected.replication_sources_json = "[]";
            const maybe_actual: ?tables.TableRecord = for (projection.tables) |table| {
                if (table.table_id == expected.table_id) break table;
            } else null;
            const actual = maybe_actual orelse continue;
            for (public_tables) |published| if (published.table_id == target.table.table_id) return error.InvalidRestoreStaging;
            if (!tables.tableDefinitionsEqual(expected, actual)) return error.InvalidRestoreStaging;
            for (target.ranges) |range| {
                var expected_range = try tables.cloneRange(alloc, range);
                try tables.clearOwnedRangeRestoreIntent(alloc, &expected_range);
                expected_range.completed_restore_fingerprint = tables.empty_restore_completion_fingerprint;
                const maybe_range: ?tables.RangeRecord = for (projection.ranges) |candidate| {
                    if (candidate.group_id == range.group_id) break candidate;
                } else null;
                const actual_range = maybe_range orelse continue;
                for (public_ranges) |published| if (published.group_id == actual_range.group_id) return error.InvalidRestoreStaging;
                if (!tables.rangeRecordsEqual(expected_range, actual_range)) return error.InvalidRestoreStaging;
                const inserted = try seen_groups.getOrPut(alloc, range.group_id);
                if (inserted.found_existing) return error.InvalidRestoreStaging;
                const scope = try staging.ownerScope(alloc, parsed.value.plan, parsed.value.plan_digest, target, range);
                try scope.validate();
                try owners.append(alloc, .{ .plan_id = parsed.value.plan.id, .plan_digest = parsed.value.plan_digest, .table = actual, .range = actual_range, .scope = scope, .cancel_recovery = parsed.value.state == .canceling });
            }
        }
    }
    var seen_tables: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (projection.tables) |table| {
        const entry = try seen_tables.getOrPut(alloc, table.table_id);
        if (entry.found_existing) return error.InvalidRestoreStaging;
        const published: ?tables.TableRecord = for (public_tables) |candidate| {
            if (candidate.table_id == table.table_id) break candidate;
        } else null;
        if (published) |expected| {
            if (!tables.tableDefinitionsEqual(expected, table)) return error.MetadataSnapshotHeadMismatch;
        } else {
            for (owners.items) |owner| {
                if (owner.table.table_id == table.table_id) break;
            } else return error.InvalidRestoreStaging;
        }
    }
    var projected_groups: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (projection.ranges) |range| {
        const entry = try projected_groups.getOrPut(alloc, range.group_id);
        if (entry.found_existing) return error.InvalidRestoreStaging;
        const published: ?tables.RangeRecord = for (public_ranges) |candidate| {
            if (candidate.group_id == range.group_id) break candidate;
        } else null;
        if (published) |expected| {
            if (!tables.rangeRecordsEqual(expected, range)) return error.MetadataSnapshotHeadMismatch;
        } else if (!seen_groups.contains(range.group_id)) return error.InvalidRestoreStaging;
    }
    return owners.toOwnedSlice(alloc);
}

pub fn find(owners: []const Owner, group_id: u64) ?Owner {
    for (owners) |owner| if (owner.range.group_id == group_id) return owner;
    return null;
}

/// Seed sidecars are immutable historical authority. Once an exact table ID
/// is published, routing takes over and its old hidden descriptor is retired.
/// This never relaxes validation of a live private provisioning response.
pub fn unpublishedProjection(alloc: std.mem.Allocator, public_tables: []const tables.TableRecord, projection: staging.ProvisioningProjection) !staging.ProvisioningProjection {
    var hidden_tables = std.ArrayListUnmanaged(tables.TableRecord).empty;
    var hidden_ranges = std.ArrayListUnmanaged(tables.RangeRecord).empty;
    for (projection.tables) |table| {
        for (public_tables) |published| {
            if (published.table_id == table.table_id) break;
        } else try hidden_tables.append(alloc, table);
    }
    for (projection.ranges) |range| {
        for (hidden_tables.items) |table| {
            if (table.table_id == range.table_id) {
                try hidden_ranges.append(alloc, range);
                break;
            }
        }
    }
    return .{ .tables = try hidden_tables.toOwnedSlice(alloc), .ranges = try hidden_ranges.toOwnedSlice(alloc), .jobs_json = projection.jobs_json };
}

test "private provisioning validates immutable hidden owners and rejects forged descriptors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const target: staging.Target = .{
        .source_table_id = 1,
        .table = .{ .table_id = 11, .name = "hidden", .schema_json = "{}" },
        .ranges = &.{.{ .table_id = 11, .group_id = 701, .range_id = 701, .doc_identity_shard_id = 701, .doc_identity_range_id = 701, .start_key = "" }},
        .source_artifacts = &.{.{ .target_group_id = 701, .source_namespace = .{ .table_id = 1, .shard_id = 1, .range_id = 1 }, .format = .native, .snapshot_path = "source", .artifact_size_bytes = 1, .artifact_sha256 = @splat(3) }},
    };
    const plan: staging.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{target} };
    const job: staging.Job = .{ .plan = plan, .plan_digest = try plan.digest(alloc) };
    const bytes = try std.json.Stringify.valueAlloc(alloc, job, .{});
    var table_values = [_]tables.TableRecord{target.table};
    var range_values = [_]tables.RangeRecord{target.ranges[0]};
    var projection: staging.ProvisioningProjection = .{ .tables = &table_values, .ranges = &range_values, .jobs_json = &.{bytes} };
    const owners = try validate(alloc, &.{}, &.{}, projection);
    try std.testing.expectEqual(@as(usize, 1), owners.len);
    try std.testing.expectEqual(@as(u64, 701), owners[0].range.group_id);
    try std.testing.expect(!owners[0].cancel_recovery);
    var cancel_job = job;
    cancel_job.state = .canceling;
    cancel_job.revision = 2;
    cancel_job.completed_owners = 1;
    const cancel_bytes = try std.json.Stringify.valueAlloc(alloc, cancel_job, .{});
    projection.jobs_json = &.{cancel_bytes};
    const cancel_owners = try validate(alloc, &.{}, &.{}, projection);
    try std.testing.expect(cancel_owners[0].cancel_recovery);
    const cancel_placement: @import("../raft/reconciler.zig").PlacementIntent = .{ .record = .{ .group_id = 701, .replica_id = 1, .local_node_id = 7 } };
    var scoped_cancel = try staging.scopeProvisioningForNode(std.testing.allocator, projection, 7, &.{cancel_placement});
    defer scoped_cancel.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(cancel_bytes, scoped_cancel.jobs_json[0]);
    try std.testing.expect((try validate(alloc, &.{}, &.{}, scoped_cancel))[0].cancel_recovery);
    for ([_]staging.State{ .published, .canceled }) |terminal| {
        var terminal_job = cancel_job;
        terminal_job.state = terminal;
        const terminal_bytes = try std.json.Stringify.valueAlloc(alloc, terminal_job, .{});
        projection.jobs_json = &.{terminal_bytes};
        try std.testing.expectError(error.InvalidRestoreStaging, validate(alloc, &.{}, &.{}, projection));
    }
    cancel_job.completed_owners = 2;
    const excessive_progress = try std.json.Stringify.valueAlloc(alloc, cancel_job, .{});
    projection.jobs_json = &.{excessive_progress};
    try std.testing.expectError(error.InvalidRestoreStaging, validate(alloc, &.{}, &.{}, projection));
    projection.jobs_json = &.{bytes};
    table_values[0].schema_json = "{\"version\":2}";
    try std.testing.expectError(error.InvalidRestoreStaging, validate(alloc, &.{}, &.{}, projection));
    table_values[0] = target.table;
    projection.jobs_json = &.{};
    try std.testing.expectError(error.InvalidRestoreStaging, validate(alloc, &.{}, &.{}, projection));
    projection.jobs_json = &.{bytes};
    const placement: @import("../raft/reconciler.zig").PlacementIntent = .{ .record = .{ .group_id = 701, .replica_id = 1, .local_node_id = 7 } };
    var owner_projection = try staging.scopeProvisioningForNode(std.testing.allocator, projection, 7, &.{placement});
    defer owner_projection.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owner_projection.tables.len);
    var other_projection = try staging.scopeProvisioningForNode(std.testing.allocator, projection, 8, &.{placement});
    defer other_projection.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), other_projection.tables.len);
    try std.testing.expectEqual(@as(usize, 0), other_projection.jobs_json.len);
}
