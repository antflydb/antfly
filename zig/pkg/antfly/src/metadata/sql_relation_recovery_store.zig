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

/// A counted participant can outlive its physical image without deciding
/// its named transaction. Only this plan's already-admitted irreversible loss
/// proof permits publication while that exact old image remains referenced.
/// The caller keeps every registry record, counter and permanent participant.
/// Job and expected_progress must be the actual separately stored records
/// loaded from this same writer cut. The caller verifies the full immutable
/// Plan digest once per counted phase before invoking this compact rejoin; this
/// private helper accepts no wire/request authority. Rejoin only compact keys
/// per target, rather than reparsing the whole canonical cohort per owner.
pub fn permitsRetainedParticipants(a: std.mem.Allocator, reader: anytype, group: u64, incarnation: [32]u8, job: restore.Job, expected_progress: restore.Progress, target: restore.Target) !bool {
    const guard = target.native_reset orelse return false;
    const old = target.replace orelse return false;
    const descriptor = old.table.sql_persistence orelse return false;
    const successor = target.table.sql_persistence orelse return false;
    try guard.validate();
    if (!target.empty_generation or target.schema_only_snapshot != null or target.population != null or target.rewrite != null or
        target.source_table_id != old.table.table_id or old.table.table_id != guard.before.binding.table_id or
        !descriptor.eql(guard.before.binding.descriptor) or old.ranges.len != 1) return false;
    var expected_successor = descriptor;
    expected_successor.generation_instance = guard.lost_native_instance;
    expected_successor.data_generation = std.math.add(u64, descriptor.data_generation, 1) catch return error.CatalogGenerationExhausted;
    if (!successor.eql(expected_successor)) return false;
    const pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(guard.before.binding);
    const range = old.ranges[0];
    if (range.table_id != pin.table_id or range.group_id != guard.source_group_id or
        tables.rangeDocIdentityShardId(range) != pin.shard_id or tables.rangeDocIdentityRangeId(range) != pin.range_id) return false;
    var key: [320]u8 = undefined;
    const pointer_bytes = (try optional(reader, try recovery.indexKey(&key, group, pin))) orelse return false;
    const pointer = try recovery.PointerRecord.decode(pointer_bytes);
    if (pointer != .reset) return false;
    const address = pointer.reset;
    if (address.source_table_id != old.table.table_id or !std.mem.eql(u8, &address.plan_id, &job.plan.id) or
        !std.mem.eql(u8, &address.plan_digest, &job.plan_digest)) return false;
    const fence_bytes = (try optional(reader, try recovery.resetFenceKey(&key, group, job.plan.id, old.table.table_id))) orelse return false;
    const fence = try recovery.LossFenceRecord.decode(fence_bytes);
    try fence.requireCaptured(group, incarnation, range.group_id, pin);
    if (!std.mem.eql(u8, &fence.plan_id, &job.plan.id) or !std.mem.eql(u8, &fence.plan_digest, &job.plan_digest) or
        !std.mem.eql(u8, &try fence.source.digest(), &try guard.digest())) return false;
    const progress_bytes = (try optional(reader, try restore.progressKey(&key, group, job.plan.id))) orelse return false;
    if (progress_bytes.len == 0 or progress_bytes.len > restore.max_encoded_bytes) return error.InvalidRestoreStaging;
    var progress = try std.json.parseFromSlice(restore.Progress, a, progress_bytes, .{});
    defer progress.deinit();
    if (progress.value.state == .canceling or progress.value.state == .canceled or progress.value.revision < fence.revision) return false;
    if (progress.value.state != expected_progress.state or progress.value.revision != expected_progress.revision or progress.value.completed_owners != expected_progress.completed_owners)
        return error.RestoreStagingProgressChanged;
    return true;
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

test "SQL native generation retirement cold loss evidence outlives public job expiry without deciding named custody" {
    const Observer = struct {
        pub fn check(_: anytype, _: restore.Job, _: restore.Progress) !void {}
    };
    try testRetainedNativeLossCustody(Observer);
}

/// Metadata-component harness shared with the writer's activation-floor test.
/// No physical engine, participant vote or native ACK is produced here.
pub fn testRetainedNativeLossCustody(comptime Observer: type) !void {
    const local = @import("antfly_local_sources");
    const attestation = @import("sql_relation_attestation.zig");
    const records = local.common_topology_records;
    const group = local.common_group_ids.main_metadata_group_id;
    const P = @import("../system_catalog/prepared_transactions.zig");
    const Store = @import("storage/raft_apply_store.zig").RaftApplyStore;
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/retained-native-loss", .{tmp.sub_path});
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: @import("antfly_local_sources").storage_db_root_signing_identity.State = .{ .root_incarnation = 11, .seed = seed, .public_key = pair.public_key.toBytes() };
    const before = try attestation.SignedBinding.sign(.{
        .kind = .initialized,
        .metadata_group_id = group,
        .boot_generation = 2,
        .identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key },
        .reporter_incarnation = 3,
        .binding = .{ .descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 7 }, .native_instance = @splat(1), .table_id = 7, .shard_id = 172, .range_id = 272, .physical_root_incarnation = std.math.maxInt(u128) - 1, .schema_version = 1, .schema_digest = @splat(5), .catalog_generation = 1, .catalog_fingerprint = @splat(6) },
    }, root);
    const guard: recovery.Guard = .{ .before = before, .source_group_id = 42, .lost_boot_generation = 3, .lost_native_instance = @splat(2), .catalog_revision = 13 };
    const old_ranges: [1]records.RangeRecord = .{.{ .group_id = 42, .table_id = 7, .start_key = "", .doc_identity_shard_id = 172, .doc_identity_range_id = 272 }};
    var successor = before.binding.descriptor;
    successor.data_generation += 1;
    successor.generation_instance = guard.lost_native_instance;
    const new_ranges: [1]records.RangeRecord = .{.{ .group_id = 43, .table_id = 8, .range_id = 43, .doc_identity_shard_id = 43, .doc_identity_range_id = 43, .start_key = "" }};
    const targets: [1]restore.Target = .{.{ .table = .{ .table_id = 8, .name = "rows", .schema_json = "{}", .sql_persistence = successor }, .source_table_id = 7, .empty_generation = true, .native_reset = guard, .ranges = &new_ranges, .replace = .{ .table = .{ .table_id = 7, .name = "rows", .schema_json = "{}", .sql_persistence = before.binding.descriptor }, .ranges = &old_ranges } }};
    const plan: restore.Plan = .{ .id = @splat(1), .cohort_digest = @splat(3), .targets = &targets };
    try plan.validate(a);
    const plan_digest = try plan.digest(a);
    const job: restore.Job = .{ .plan = plan, .plan_digest = plan_digest };
    const progress: restore.Progress = .{ .state = .activating, .revision = 9 };
    var fence: recovery.LossFence = .{ .metadata_group_id = group, .metadata_incarnation = @splat('a'), .plan_id = plan.id, .plan_digest = plan_digest, .source = guard, .revision = 1, .checkpoint_digest = @splat(0) };
    fence.checkpoint_digest = try fence.checkpointDigest();
    const pin = try records.RelationPersistence.PhysicalPin.fromBinding(before.binding);
    const encoded_pointer = try recovery.PointerRecord.init(.{ .reset = .{ .plan_id = plan.id, .plan_digest = plan_digest, .source_table_id = 7 } });
    const encoded_fence = try recovery.LossFenceRecord.init(fence);
    const participants: [2]P.Participant = .{
        .{ .table_name = "rows", .schema_version = 1, .route = .{ .metadata_group_id = group, .metadata_incarnation = @splat('a'), .catalog_revision = 13, .table_id = 7, .topology_epoch = 1, .native_participant = pin, .route = .{ .group_id = 42, .range_id = 272, .identity_namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .execution = .{ .native_local = before.binding.descriptor } } } },
        .{ .table_name = "permanent_rows", .schema_version = 1, .route = .{ .metadata_group_id = group, .metadata_incarnation = @splat('a'), .catalog_revision = 13, .table_id = 17, .topology_epoch = 1, .route = .{ .group_id = 52, .range_id = 282, .identity_namespace = .{ .table_id = 17, .shard_id = 182, .range_id = 282 } } } },
    };
    const identity: P.Identity = .{ .gid = "retained-native-loss", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(4), .operation_id = @splat(5), .metadata_incarnation = @splat('a'), .begin_timestamp = 10, .expires_at_unix_ns = 1000, .plan_digest = try P.planDigest("[]"), .manifest_digest = try P.manifestDigest(a, &participants, @splat('a')) };
    const logical_job = "\x00\x00__api_restore_jobs__:0000000000000007";
    var expected_record: []const u8 = undefined;
    for (0..3) |pass| {
        var store = try Store.init(alloc, .{ .root_dir = path });
        defer store.deinit();
        if (pass == 0) {
            try store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('a') });
            try store.applyStandaloneCommand(group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(a, @import("topology_protocol.zig").Activation{ .version = @import("topology_protocol.zig").sql_relation_persistence_version, .incarnation = @splat('a'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{}) });
            // Real registry admission, still PREPARING/votes=0. This metadata
            // component never manufactures a native prepare/row/ACK receipt.
            var txn = try store.store.beginWriteTxn();
            errdefer txn.abort();
            const admission: P.Mutation = .{ .admission = .{ .nonce = identity.operation_id, .head = 1 }, .incarnation = @splat(0xaa), .actor = .{ .session_user = .bootstrap, .credential = "" }, .database_id = 1, .operation = .{ .admit = .{ .identity = identity, .participants = &participants, .admitted_at_ns = 20 } } };
            const delta = try P.plan(a, &txn, group, 1, admission);
            try delta.apply(&txn);
            var key: [320]u8 = undefined;
            // Canonical already-captured loss evidence, with no invented
            // physical completion: the mutable progress is only ACTIVATING.
            try txn.put(try recovery.indexKey(&key, group, pin), encoded_pointer.slice());
            try txn.put(try restore.jobKey(&key, group, plan.id), try std.json.Stringify.valueAlloc(a, job, .{}));
            try txn.put(try recovery.resetFenceKey(&key, group, plan.id, 7), encoded_fence.slice());
            try txn.put(try restore.progressKey(&key, group, plan.id), try std.json.Stringify.valueAlloc(a, progress, .{}));
            expected_record = try std.json.Stringify.valueAlloc(a, (try P.readGid(a, &txn, group, identity.gid)).?, .{});
            try txn.commit();
            // Use the production public-job persistence/deletion commands;
            // they must not collect the separate canonical Plan/loss journal.
            try store.applyStandaloneCommand(group, .{ .upsert_restore_job = .{ .key = logical_job, .value = "{\"job_id\":7,\"phase\":\"failed\"}" } });
            try store.applyStandaloneCommand(group, .{ .remove_restore_job = .{ .key = logical_job } });
        }
        try Observer.check(&store, job, progress);
        var txn = try store.store.beginReadTxn();
        defer txn.abort();
        var key: [320]u8 = undefined;
        const public_key = try std.fmt.bufPrint(&key, "\x00\x00__metadata__:metadata_restore_job:{d}:{s}", .{ group, logical_job });
        try std.testing.expectError(error.NotFound, txn.get(public_key));
        const stored_job = try std.json.parseFromSliceLeaky(restore.Job, a, try txn.get(try restore.jobKey(&key, group, plan.id)), .{});
        const stored_progress = try std.json.parseFromSliceLeaky(restore.Progress, a, try txn.get(try restore.progressKey(&key, group, plan.id)), .{});
        // The real writer performs this full digest once per counted phase.
        try std.testing.expectEqualSlices(u8, &stored_job.plan_digest, &try stored_job.plan.digest(a));
        try std.testing.expect(try permitsRetainedParticipants(a, &txn, group, @splat('a'), stored_job, stored_progress, stored_job.plan.targets[0]));
        const retired = (try read(a, &txn, group, @splat('a'), .{ .table_id = 7, .physical_pin = pin })).?;
        try retired.requireCaptured(group, @splat('a'), 42, pin);
        const record = (try P.readGid(a, &txn, group, identity.gid)).?;
        try std.testing.expectEqualStrings(expected_record, try std.json.Stringify.valueAlloc(a, record, .{}));
        try std.testing.expectEqual(@as(u64, 1), try P.activeCount(a, &txn, group));
        try std.testing.expectEqual(@as(u64, 1), try P.tableCount(a, &txn, group, 7));
        try std.testing.expectEqual(@as(u64, 1), try P.tableCount(a, &txn, group, 17));
        try std.testing.expectEqual(@as(u64, 0), record.votes);
        try std.testing.expectEqual(@as(u64, 0), record.native_lock_releases);
        try std.testing.expectEqual(null, record.decision);
        var wrong_job = stored_job;
        wrong_job.plan.id = @splat(8);
        try std.testing.expect(!try permitsRetainedParticipants(a, &txn, group, @splat('a'), wrong_job, stored_progress, stored_job.plan.targets[0]));
        wrong_job = stored_job;
        wrong_job.plan_digest[0] ^= 1;
        try std.testing.expect(!try permitsRetainedParticipants(a, &txn, group, @splat('a'), wrong_job, stored_progress, stored_job.plan.targets[0]));
        var wrong_target = stored_job.plan.targets[0];
        wrong_target.native_reset = null;
        try std.testing.expect(!try permitsRetainedParticipants(a, &txn, group, @splat('a'), stored_job, stored_progress, wrong_target));
        wrong_target = stored_job.plan.targets[0];
        wrong_target.native_reset.?.before.binding.descriptor.data_generation += 1;
        // Signed binding substitution is refused, never inferred from absence.
        try std.testing.expectError(error.InvalidSqlRelationPersistence, permitsRetainedParticipants(a, &txn, group, @splat('a'), stored_job, stored_progress, wrong_target));
        wrong_target = stored_job.plan.targets[0];
        wrong_target.native_reset.?.catalog_revision += 1;
        try std.testing.expect(!try permitsRetainedParticipants(a, &txn, group, @splat('a'), stored_job, stored_progress, wrong_target));
        var wrong_progress = stored_progress;
        wrong_progress.revision += 1;
        try std.testing.expectError(error.RestoreStagingProgressChanged, permitsRetainedParticipants(a, &txn, group, @splat('a'), stored_job, wrong_progress, stored_job.plan.targets[0]));
        // A well-framed record with a corrupt irreversible checkpoint is
        // still refused. Abort this negative writer; no bad journal persists.
        var corrupt = try store.store.beginWriteTxn();
        defer corrupt.abort();
        var bad_fence = fence;
        bad_fence.checkpoint_digest[0] ^= 1;
        const bad_record = try recovery.LossFenceRecord.init(bad_fence);
        try corrupt.put(try recovery.resetFenceKey(&key, group, plan.id, 7), bad_record.slice());
        try std.testing.expectError(error.InvalidSqlRelationPersistence, permitsRetainedParticipants(a, &corrupt, group, @splat('a'), stored_job, stored_progress, stored_job.plan.targets[0]));
    }
}
