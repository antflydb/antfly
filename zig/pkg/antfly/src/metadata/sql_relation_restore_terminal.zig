// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A published restore can outlive its final physical delivery when a later
//! native generation is irreversibly lost. This is metadata delivery evidence,
//! never a fabricated native Progress.published marker or row/transaction ACK.
const std = @import("std");
const local = @import("antfly_local_sources");
const stages = @import("restore_staging.zig");
const images = @import("sql_relation_target_images.zig");
const attest = @import("sql_relation_attestation.zig");
const loss = @import("sql_relation_recovery.zig");
const A = std.mem.Allocator;
pub const Query = struct {
    creation_scope: attest.CreationScope,
    table_id: u64,
    group_id: u64,
    scope_digest: [32]u8,
    pub fn validate(self: Query) !void {
        try self.creation_scope.validate();
        if (self.table_id == 0 or self.group_id == 0 or std.mem.allEqual(u8, &self.scope_digest, 0)) return error.InvalidRestoreStaging;
    }
};
pub const Proof = struct {
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    creation_scope: attest.CreationScope,
    scope_digest: [32]u8,
    group_id: u64,
    publication_revision: u64,
    initialized: attest.SignedBinding,
    retirement: loss.RetiredGeneration,

    pub fn require(self: Proof, query: Query, descriptor: local.common_topology_records.RelationPersistence.Descriptor) !void {
        try query.validate();
        try self.initialized.verify();
        const binding = self.initialized.binding;
        const pin = try local.common_topology_records.RelationPersistence.PhysicalPin.fromBinding(binding);
        try self.retirement.requireCaptured(self.metadata_group_id, self.metadata_incarnation, self.group_id, pin);
        if (self.publication_revision == 0 or self.metadata_group_id != self.initialized.metadata_group_id or
            !std.mem.eql(u8, &self.metadata_incarnation, &self.initialized.identity.metadata_incarnation) or
            !std.meta.eql(self.creation_scope, query.creation_scope) or !std.meta.eql(self.initialized.creation_scope, @as(?attest.CreationScope, query.creation_scope)) or
            !std.mem.eql(u8, &self.scope_digest, &query.scope_digest) or binding.table_id != query.table_id or self.group_id != query.group_id or
            !binding.descriptor.eql(descriptor) or !self.retirement.beforeBinding().binding.descriptor.eql(descriptor)) return error.RestoreStagingScopeChanged;
    }
};
fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}
/// Read only through an actual metadata read cut. The immutable published job,
/// initialized image and canonical loss checkpoint must all agree together.
pub fn read(a: A, reader: anytype, group: u64, incarnation: [32]u8, query: Query) !?Proof {
    try query.validate();
    var buffer: [384]u8 = undefined;
    const raw = (try optional(reader, try stages.jobKey(&buffer, group, query.creation_scope.plan_id))) orelse return null;
    if (raw.len > stages.max_encoded_bytes) return error.InvalidRestoreStaging;
    const job = try std.json.parseFromSliceLeaky(stages.Job, a, raw, .{});
    if (!std.mem.eql(u8, &job.plan.id, &query.creation_scope.plan_id) or !std.mem.eql(u8, &job.plan_digest, &query.creation_scope.plan_digest) or
        !std.mem.eql(u8, &try job.plan.digest(a), &job.plan_digest)) return error.RestoreStagingScopeChanged;
    const target = for (job.plan.targets) |value| {
        if (value.table.table_id == query.table_id) break value;
    } else return error.RestoreStagingScopeChanged;
    const descriptor = target.table.sql_persistence orelse return null;
    const range = for (target.ranges) |value| {
        if (value.group_id == query.group_id) break value;
    } else return error.RestoreStagingScopeChanged;
    const scope = try stages.ownerScope(a, job.plan, job.plan_digest, target, range);
    if (!std.mem.eql(u8, &scope.digest(), &query.scope_digest)) return error.RestoreStagingScopeChanged;
    const progress_raw = (try optional(reader, try stages.progressKey(&buffer, group, job.plan.id))) orelse return error.InvalidRestoreStaging;
    const progress = try std.json.parseFromSliceLeaky(stages.Progress, a, progress_raw, .{});
    if (progress.state != .published) return null;
    const initialized = (try images.effectiveInitialized(a, reader, group, query.creation_scope, query.table_id)) orelse return error.InvalidSqlRelationPersistence;
    const pin = try local.common_topology_records.RelationPersistence.PhysicalPin.fromBinding(initialized.binding);
    const retired = (try @import("sql_relation_recovery_store.zig").read(a, reader, group, incarnation, .{ .table_id = query.table_id, .physical_pin = pin })) orelse return null;
    const result: Proof = .{ .metadata_group_id = group, .metadata_incarnation = incarnation, .creation_scope = query.creation_scope, .scope_digest = query.scope_digest, .group_id = query.group_id, .publication_revision = progress.revision, .initialized = initialized, .retirement = retired };
    try result.require(query, descriptor);
    return result;
}

test "SQL native restore terminal delivery rejoins published job and exact irreversible image loss across cold reopen" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const working = arena.allocator();
    const p = local.common_topology_records.RelationPersistence;
    const docstore = local.storage_docstore;
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .root_incarnation = 11, .seed = seed, .public_key = pair.public_key.toBytes() };
    const incarnation: [32]u8 = @splat('a');
    const descriptor: p.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 1 };
    const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 201, .shard_id = 202, .range_id = 202 };
    const schema_digest = try stages.runtimeSchemaDigestForTable(working, "{}");
    const table: local.common_topology_records.TableRecord = .{ .table_id = namespace.table_id, .name = "terminal-target", .schema_json = "{}", .sql_persistence = descriptor, .desired_replica_count = 1 };
    const ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = namespace.table_id, .group_id = 202, .range_id = namespace.range_id, .doc_identity_shard_id = namespace.shard_id, .doc_identity_range_id = namespace.range_id, .start_key = "" }};
    const targets = [_]stages.Target{.{ .source_table_id = 7, .empty_generation = true, .schema_only_snapshot = .{ .manifest_digest = @splat(3), .schema_digest = schema_digest, .source_table_id = 7 }, .table = table, .ranges = &ranges }};
    const plan: stages.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &targets };
    try plan.validate(working);
    const creation: attest.CreationScope = .{ .plan_id = plan.id, .plan_digest = try plan.digest(working) };
    const owner_scope = try stages.ownerScope(working, plan, creation.plan_digest, targets[0], ranges[0]);
    const query: Query = .{ .creation_scope = creation, .table_id = namespace.table_id, .group_id = 202, .scope_digest = owner_scope.digest() };
    const initialized = try attest.SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = 1, .boot_generation = 1, .identity = .{ .metadata_incarnation = incarnation, .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key }, .reporter_incarnation = 1, .creation_scope = creation, .binding = .{ .descriptor = descriptor, .native_instance = @splat(1), .table_id = namespace.table_id, .shard_id = namespace.shard_id, .range_id = namespace.range_id, .physical_root_incarnation = std.math.maxInt(u128) - 17, .schema_version = 1, .schema_digest = schema_digest, .catalog_generation = 1, .catalog_fingerprint = @splat(6) } }, root);
    const pin = try p.PhysicalPin.fromBinding(initialized.binding);
    const guard: loss.Guard = .{ .before = initialized, .source_group_id = 202, .lost_boot_generation = 2, .lost_native_instance = @splat(2), .catalog_revision = 1 };
    var successor = table;
    successor.table_id = 301;
    successor.name = table.name;
    successor.sql_persistence.?.data_generation = 2;
    successor.sql_persistence.?.generation_instance = @splat(2);
    const successor_ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 301, .group_id = 302, .range_id = 302, .doc_identity_shard_id = 302, .doc_identity_range_id = 302, .start_key = "" }};
    var reset_targets = [_]stages.Target{.{ .source_table_id = table.table_id, .empty_generation = true, .native_reset = guard, .table = successor, .ranges = &successor_ranges, .replace = .{ .table = table, .ranges = &ranges } }};
    try stages.prepareNativeLossGenerationsAlloc(working, &reset_targets);
    const reset: stages.Plan = .{ .id = @splat(3), .cohort_digest = @splat(4), .targets = &reset_targets };
    try reset.validate(working);
    const reset_digest = try reset.digest(working);
    var checkpoint: loss.LossFence = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .plan_id = reset.id, .plan_digest = reset_digest, .source = guard, .revision = 1, .checkpoint_digest = @splat(0) };
    checkpoint.checkpoint_digest = try checkpoint.checkpointDigest();
    try checkpoint.validate();
    const pointer = try loss.PointerRecord.init(.{ .reset = .{ .plan_id = reset.id, .plan_digest = reset_digest, .source_table_id = table.table_id } });
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/restore-terminal-proof", .{tmp.sub_path}, 0);
    defer a.free(path);
    var store = try docstore.DocStore.open(a, path.ptr, .{});
    var store_open = true;
    defer if (store_open) store.close();
    const ReadOnly = struct {
        txn: *docstore.DocStore.Txn,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.txn.get(key);
        }
    };
    var buffer: [384]u8 = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var cut: ReadOnly = .{ .txn = &txn };
        try std.testing.expectEqual(@as(?Proof, null), try read(working, &cut, 1, incarnation, query));
        try txn.put(try stages.jobKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, stages.Job{ .plan = plan, .plan_digest = creation.plan_digest }, .{}));
        try txn.put(try stages.progressKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, stages.Progress{}, .{}));
        try txn.put(try stages.nativeBindingKey(&buffer, 1, plan.id, namespace.table_id), try std.json.Stringify.valueAlloc(working, initialized, .{}));
        _ = try images.stageImage(&txn, 1, creation, initialized, 1, try images.originalAttemptDigest(creation, initialized));
        try std.testing.expectEqual(@as(?Proof, null), try read(working, &cut, 1, incarnation, query));
        try txn.put(try stages.progressKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, stages.Progress{ .state = .published, .revision = 3 }, .{}));
        try std.testing.expectEqual(@as(?Proof, null), try read(working, &cut, 1, incarnation, query)); // No loss inferred from publication.
        try txn.put(try stages.jobKey(&buffer, 1, reset.id), try std.json.Stringify.valueAlloc(working, stages.Job{ .plan = reset, .plan_digest = reset_digest }, .{}));
        try txn.put(try stages.progressKey(&buffer, 1, reset.id), try std.json.Stringify.valueAlloc(working, stages.Progress{}, .{}));
        try txn.put(try loss.indexKey(&buffer, 1, pin), pointer.slice());
        // An index pointer alone cannot certify irreversible source loss.
        try std.testing.expectError(error.InvalidCatalogRecord, read(working, &cut, 1, incarnation, query));
        const fenced = try loss.LossFenceRecord.init(checkpoint);
        try txn.put(try loss.resetFenceKey(&buffer, 1, reset.id, table.table_id), fenced.slice());
        const proof = (try read(working, &cut, 1, incarnation, query)).?;
        try proof.require(query, descriptor);
        try std.testing.expectEqual(@as(u64, 3), proof.publication_revision);
        var wrong = query;
        wrong.scope_digest[0] ^= 1;
        try std.testing.expectError(error.RestoreStagingScopeChanged, read(working, &cut, 1, incarnation, wrong));
        try txn.commit();
    }
    try store.sync(true);
    store.close();
    store_open = false;
    store = try docstore.DocStore.open(a, path.ptr, .{});
    store_open = true;
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var cut: ReadOnly = .{ .txn = &txn };
        const proof = (try read(working, &cut, 1, incarnation, query)).?;
        try proof.require(query, descriptor);
        var other = initialized;
        other.binding.physical_root_incarnation -= 1;
        other = try other.sign(root);
        try txn.put(try stages.nativeBindingKey(&buffer, 1, plan.id, namespace.table_id), try std.json.Stringify.valueAlloc(working, other, .{}));
        _ = try images.stageImage(&txn, 1, creation, other, 1, try images.originalAttemptDigest(creation, other));
        try std.testing.expectEqual(@as(?Proof, null), try read(working, &cut, 1, incarnation, query)); // Old loss cannot cover a different real image.
        const other_pin = try p.PhysicalPin.fromBinding(other.binding);
        try txn.put(try loss.indexKey(&buffer, 1, other_pin), pointer.slice());
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, read(working, &cut, 1, incarnation, query));
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var cut: ReadOnly = .{ .txn = &txn };
        try (try read(working, &cut, 1, incarnation, query)).?.require(query, descriptor);
    }
}
