// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical joins for native generation retirement. Secondary keys are only
//! addresses: authority always comes from the admitted job and its irreversible
//! checkpoint in the same metadata transaction.
const std = @import("std");
const recovery = @import("sql_relation_recovery.zig");
const restore = @import("restore_staging.zig");
const tables = @import("table_manager.zig");
const scopes = @import("scope_retirement_ledger.zig");

fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}

pub fn read(a: std.mem.Allocator, reader: anytype, group: u64, incarnation: [32]u8, query: recovery.Query) !?recovery.RetiredGeneration {
    try query.validate();
    var key: [320]u8 = undefined;
    const pointer_bytes = (try optional(reader, try recovery.indexKey(&key, group, query.physical_pin))) orelse return null;
    const pointer = try recovery.PointerRecord.decode(pointer_bytes);
    const result: recovery.RetiredGeneration = switch (pointer) {
        .reset => |address| result: {
            const job_bytes = (try optional(reader, try restore.jobKey(&key, group, address.plan_id))) orelse return error.InvalidCatalogRecord;
            const job = try std.json.parseFromSliceLeaky(restore.Job, a, job_bytes, .{ .allocate = .alloc_always });
            if (!std.mem.eql(u8, &job.plan.id, &address.plan_id) or !std.mem.eql(u8, &job.plan_digest, &address.plan_digest) or
                !std.mem.eql(u8, &try job.plan.digest(a), &address.plan_digest)) return error.InvalidCatalogRecord;
            const target = for (job.plan.targets) |candidate| {
                if (candidate.source_table_id == address.source_table_id) break candidate;
            } else return error.InvalidCatalogRecord;
            const guard = target.native_reset orelse return error.InvalidCatalogRecord;
            const old = target.replace orelse return error.InvalidCatalogRecord;
            const descriptor = old.table.sql_persistence orelse return error.InvalidCatalogRecord;
            if (!target.empty_generation or target.population != null or target.rewrite != null or
                old.table.table_id != query.table_id or !descriptor.eql(guard.before.binding.descriptor) or
                target.source_table_id != query.table_id or old.ranges.len != 1 or
                old.ranges[0].group_id != guard.source_group_id or
                tables.rangeDocIdentityShardId(old.ranges[0]) != query.physical_pin.shard_id or
                tables.rangeDocIdentityRangeId(old.ranges[0]) != query.physical_pin.range_id) return error.InvalidCatalogRecord;
            const fence_bytes = (try optional(reader, try recovery.resetFenceKey(&key, group, address.plan_id, query.table_id))) orelse return error.InvalidCatalogRecord;
            const fence = try recovery.LossFenceRecord.decode(fence_bytes);
            try fence.validate();
            if (!std.mem.eql(u8, &try fence.source.digest(), &try guard.digest()) or
                !std.mem.eql(u8, &fence.plan_id, &address.plan_id) or !std.mem.eql(u8, &fence.plan_digest, &address.plan_digest)) return error.InvalidCatalogRecord;
            const progress_bytes = (try optional(reader, try restore.progressKey(&key, group, address.plan_id))) orelse return error.InvalidCatalogRecord;
            const progress = try std.json.parseFromSliceLeaky(restore.Progress, a, progress_bytes, .{});
            // A lost image cannot become usable again through cancellation.
            if (progress.state == .canceling or progress.state == .canceled or progress.revision < fence.revision) return error.InvalidCatalogRecord;
            break :result .{ .unlogged_loss_fenced = fence };
        },
        .scope => |address| result: {
            const child = (try scopes.child(reader, address.lookup, address.child_ordinal)) orelse return error.InvalidCatalogRecord;
            if (child.table != query.table_id) return error.InvalidCatalogRecord;
            const proof = (try @import("temporary_relation_retirement.zig").readDerived(a, reader, child)) orelse return error.InvalidCatalogRecord;
            if (!std.meta.eql(proof.scope, address.lookup) or proof.child_ordinal != address.child_ordinal) return error.InvalidCatalogRecord;
            break :result .{ .temporary_scope = proof };
        },
    };
    const source_group = switch (result) {
        .unlogged_loss_fenced => |value| value.source.source_group_id,
        .unlogged_reset => |value| value.source.source_group_id,
        .temporary_scope => |value| value.source_group_id,
    };
    try result.requireCaptured(group, incarnation, source_group, query.physical_pin);
    return result;
}

const TestReader = struct {
    key: []const u8,
    value: []const u8,
    pub fn get(self: *const @This(), key: []const u8) anyerror![]const u8 {
        if (std.mem.eql(u8, self.key, key)) return self.value;
        return error.NotFound;
    }
};

test "SQL native generation retirement never infers loss from missing physical state" {
    const query: recovery.Query = .{ .table_id = 7, .physical_pin = .{ .physical_root_incarnation = 11, .table_id = 7, .shard_id = 8, .range_id = 9, .data_generation = 1 } };
    const reader: TestReader = .{ .key = "unrelated", .value = "" };
    try std.testing.expectEqual(null, try read(std.testing.allocator, &reader, 1, "0123456789abcdef0123456789abcdef".*, query));
}

test "SQL native generation retirement index alone grants no lost-generation authority" {
    const query: recovery.Query = .{ .table_id = 7, .physical_pin = .{ .physical_root_incarnation = 11, .table_id = 7, .shard_id = 8, .range_id = 9, .data_generation = 1 } };
    var key: [320]u8 = undefined;
    const encoded = try recovery.PointerRecord.init(.{ .reset = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_table_id = 7 } });
    const reader: TestReader = .{ .key = try recovery.indexKey(&key, 1, query.physical_pin), .value = encoded.slice() };
    try std.testing.expectError(error.InvalidCatalogRecord, read(std.testing.allocator, &reader, 1, "0123456789abcdef0123456789abcdef".*, query));
}
test "SQL native generation retirement canonical fence survives repeated successor boots and rejects canceled or foreign history" {
    const attestation = @import("sql_relation_attestation.zig");
    const records = @import("antfly_local_sources").common_topology_records;
    const MapReader = struct {
        const Row = struct { key: []const u8, value: []const u8 };
        rows: []const Row,
        pub fn get(self: *const @This(), key: []const u8) anyerror![]const u8 {
            for (self.rows) |row| if (std.mem.eql(u8, row.key, key)) return row.value;
            return error.NotFound;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: @import("antfly_local_sources").storage_db_root_signing_identity.State = .{ .root_incarnation = 11, .seed = seed, .public_key = pair.public_key.toBytes() };
    const before = try attestation.SignedBinding.sign(.{
        .kind = .initialized,
        .metadata_group_id = 1,
        .boot_generation = 2,
        .identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key },
        .reporter_incarnation = 3,
        .binding = .{ .descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 7 }, .native_instance = @splat(1), .table_id = 7, .shard_id = 172, .range_id = 272, .physical_root_incarnation = std.math.maxInt(u128) - 1, .schema_version = 1, .schema_digest = @splat(5), .catalog_generation = 1, .catalog_fingerprint = @splat(6) },
    }, root);
    const guard: recovery.Guard = .{ .before = before, .source_group_id = 42, .lost_boot_generation = 3, .lost_native_instance = @splat(2), .catalog_revision = 13 };
    const old_ranges: [1]records.RangeRecord = .{.{ .group_id = 42, .table_id = 7, .start_key = "", .doc_identity_shard_id = 172, .doc_identity_range_id = 272 }};
    const targets: [1]restore.Target = .{.{ .table = .{ .table_id = 8, .name = "rows" }, .source_table_id = 7, .empty_generation = true, .native_reset = guard, .ranges = &.{}, .replace = .{ .table = .{ .table_id = 7, .name = "rows", .sql_persistence = before.binding.descriptor }, .ranges = &old_ranges } }};
    const plan: restore.Plan = .{ .id = @splat(1), .cohort_digest = @splat(3), .targets = &targets };
    const plan_digest = try plan.digest(a);
    var fence: recovery.LossFence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('a'), .plan_id = plan.id, .plan_digest = plan_digest, .source = guard, .revision = 1, .checkpoint_digest = @splat(0) };
    fence.checkpoint_digest = try fence.checkpointDigest();
    const pin = try records.RelationPersistence.PhysicalPin.fromBinding(before.binding);
    const encoded_pointer = try recovery.PointerRecord.init(.{ .reset = .{ .plan_id = plan.id, .plan_digest = plan_digest, .source_table_id = 7 } });
    const encoded_fence = try recovery.LossFenceRecord.init(fence);
    var keys: [4][320]u8 = undefined;
    var rows: [4]MapReader.Row = .{
        .{ .key = try recovery.indexKey(&keys[0], 1, pin), .value = encoded_pointer.slice() },
        .{ .key = try restore.jobKey(&keys[1], 1, plan.id), .value = try std.json.Stringify.valueAlloc(a, restore.Job{ .plan = plan, .plan_digest = plan_digest }, .{}) },
        .{ .key = try recovery.resetFenceKey(&keys[2], 1, plan.id, 7), .value = encoded_fence.slice() },
        .{ .key = try restore.progressKey(&keys[3], 1, plan.id), .value = try std.json.Stringify.valueAlloc(a, restore.Progress{ .state = .published, .revision = 9 }, .{}) },
    };
    const reader: MapReader = .{ .rows = &rows };
    const query: recovery.Query = .{ .table_id = 7, .physical_pin = pin };
    const retired = (try read(a, &reader, 1, @splat('a'), query)).?;
    try retired.requireCaptured(1, @splat('a'), 42, pin);
    // No successor Boot participates in consumption: generation N+2 cannot
    // invalidate irreversible retirement of the original image N.
    try std.testing.expectEqual(.unlogged_loss_fenced, std.meta.activeTag(retired));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, read(a, &reader, 1, @splat('b'), query));
    rows[3].value = try std.json.Stringify.valueAlloc(a, restore.Progress{ .state = .canceled, .revision = 10 }, .{});
    try std.testing.expectError(error.InvalidCatalogRecord, read(a, &reader, 1, @splat('a'), query));
}
