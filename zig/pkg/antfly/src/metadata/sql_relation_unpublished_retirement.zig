// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit metadata authority to retire an unpublished canceled native target.
//! Its optional initialized proof is the actual admitted image identity, never
//! a claim that the old process flushed or sealed its current rows. It certifies no physical root,
//! native flush, Pin or owner completion. Physical deletion uses the existing
//! replica-retirement sidecar, under the shared producer/publication drain.
const std = @import("std");
const stages = @import("restore_staging.zig");
const boots = @import("sql_relation_boots.zig");
const records = @import("schema_validation_publication.zig");
const local = @import("antfly_local_sources");
const p = local.common_topology_records.RelationPersistence;
const A = std.mem.Allocator;

pub const Proof = struct {
    version: u8 = 1,
    metadata_group: u64,
    metadata_incarnation: [32]u8,
    plan: @import("sql_relation_attestation.zig").CreationScope,
    namespace: local.storage_db_doc_identity_namespace.Namespace,
    group: u64,
    descriptor: p.Descriptor,
    boot_generation: u64,
    native_instance: [16]u8,
    cancellation_revision: u64,
    initialized: ?@import("sql_relation_attestation.zig").SignedBinding = null,
    uninitialized_attempt: ?@import("sql_relation_empty_target.zig").Attempt = null,

    pub fn validate(self: Proof) !void {
        try self.plan.validate();
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0) return error.InvalidSqlRelationPersistence;
        try self.descriptor.validate();
        if (self.uninitialized_attempt) |attempt| try attempt.validate();
        if (self.uninitialized_attempt != null and self.initialized == null) return error.InvalidSqlRelationPersistence;
        if (self.initialized) |image| {
            try image.verify();
            if (image.metadata_group_id != self.metadata_group or !image.binding.descriptor.eql(self.descriptor) or !std.meta.eql(image.creation_scope, @as(?@import("sql_relation_attestation.zig").CreationScope, self.plan)) or image.binding.table_id != self.namespace.table_id or image.binding.shard_id != self.namespace.shard_id or image.binding.range_id != self.namespace.range_id) return error.InvalidSqlRelationPersistence;
        }
        if (self.version != 1 or self.metadata_group == 0 or self.group == 0 or self.boot_generation == 0 or self.cancellation_revision == 0 or
            std.mem.allEqual(u8, &self.native_instance, 0) or std.mem.allEqual(u8, &self.metadata_incarnation, 0)) return error.InvalidSqlRelationPersistence;
    }
    pub fn digest(self: Proof) ![32]u8 {
        try self.validate();
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try writer.writer.writeAll("antfly/native-unpublished-target-cancellation/v1\x00");
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: [32]u8 = undefined;
        writer.hasher.final(&result);
        return result;
    }
};
pub const Record = records.Record(Proof, "NUPX", 16384);

pub fn key(buffer: []u8, group: u64, plan: stages.Id, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native-unpublished-canceled:{s}:{x:0>16}", .{ group, std.fmt.bytesToHex(plan, .lower), table });
}
pub fn groupKey(buffer: []u8, group: u64, source_group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native-unpublished-group:{x:0>16}", .{ group, source_group });
}
fn optional(reader: anytype, selected: []const u8) !?[]const u8 {
    return reader.get(selected) catch |err| return if (err == error.NotFound) null else err;
}

/// The caller supplies the actual registered Boot from this same transaction,
/// never a user body. Declaration absence alone is not authority: every exact
/// initialized/import receipt and immutable hidden target is rejoined here.
pub fn capture(a: A, reader: anytype, group: u64, scope: @import("sql_relation_attestation.zig").CreationScope, target_id: u64, boot: boots.Record) !Proof {
    try scope.validate();
    try boot.validate();
    var buffer: [320]u8 = undefined;
    const encoded = (try optional(reader, try stages.jobKey(&buffer, group, scope.plan_id))) orelse return error.RestoreStagingScopeChanged;
    const job = try std.json.parseFromSliceLeaky(stages.Job, a, encoded, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &job.plan_digest, &scope.plan_digest)) return error.RestoreStagingScopeChanged;
    const raw_progress = (try optional(reader, try stages.progressKey(&buffer, group, scope.plan_id))) orelse return error.RestoreStagingScopeChanged;
    const progress = try std.json.parseFromSliceLeaky(stages.Progress, a, raw_progress, .{});
    if (progress.state != .canceling) return error.RestoreStagingProgressChanged;
    const target = for (job.plan.targets) |value| {
        if (value.table.table_id == target_id) break value;
    } else return error.RestoreStagingScopeChanged;
    const reservation = (try optional(reader, try stages.identityKey(&buffer, group, .table, target_id))) orelse return error.RestoreStagingScopeChanged;
    if (!std.mem.eql(u8, reservation, &scope.plan_id)) return error.RestoreStagingScopeChanged;
    const descriptor = target.table.sql_persistence orelse return error.InvalidSqlRelationPersistence;
    if (target.ranges.len != 1 or target.native_reset != null or descriptor.owner_node_id != boot.node_id or descriptor.owner_store_id != boot.identity.store_id) return error.InvalidSqlRelationPersistence;
    // An irreversible reset is not a cancellable unpublished query target.
    const images = @import("sql_relation_target_images.zig");
    const effective = try images.effectiveAttempt(a, reader, group, scope, target_id);
    const reserved = if (effective != null and effective.?.initialized == null) try images.currentAttempt(reader, group, scope, target_id) else null;
    const initialized = if (effective) |value| value.initialized orelse if (reserved) |attempt| attempt.previous_initialized else null else null;
    const range = target.ranges[0];
    if (initialized == null and (try optional(reader, try stages.receiptKey(&buffer, group, scope.plan_id, .importing, range.group_id)) != null or try optional(reader, try stages.receiptKey(&buffer, group, scope.plan_id, .validating, range.group_id)) != null)) return error.InvalidSqlRelationPersistence;
    const result: Proof = .{ .metadata_group = group, .metadata_incarnation = boot.identity.metadata_incarnation, .plan = scope, .namespace = .{ .table_id = target_id, .shard_id = @import("table_manager.zig").rangeDocIdentityShardId(range), .range_id = @import("table_manager.zig").rangeDocIdentityRangeId(range) }, .group = range.group_id, .descriptor = descriptor, .boot_generation = boot.generation, .native_instance = boot.instance, .cancellation_revision = progress.revision, .initialized = initialized, .uninitialized_attempt = if (reserved) |attempt| .{ .number = attempt.number, .digest = try attempt.digest() } else null };
    try result.validate();
    return result;
}

/// Persisted beside the existing cancellation receipt in the very same native
/// metadata transaction. The ordinary root-GC worker consumes this terminal
/// authority after restart without requiring the old native process to exist.
pub fn stage(a: A, txn: anytype, expected: Proof, boot: boots.Record) !void {
    try expected.validate();
    var buffer: [320]u8 = undefined;
    if (try optional(txn, try key(&buffer, expected.metadata_group, expected.plan.plan_id, expected.namespace.table_id))) |before| {
        if (!std.meta.eql(try Record.decode(before), expected)) return error.RestoreStagingScopeChanged;
        return;
    }
    const actual = try capture(a, txn, expected.metadata_group, expected.plan, expected.namespace.table_id, boot);
    if (!std.meta.eql(actual, expected)) return error.RestoreStagingScopeChanged;
    const encoded = try Record.init(actual);
    try txn.put(try key(&buffer, expected.metadata_group, expected.plan.plan_id, expected.namespace.table_id), encoded.slice());
    // Globally non-reused source group IDs make this a narrow navigation key,
    // not name-based authority or another queue/catalog.
    if (try optional(txn, try groupKey(&buffer, expected.metadata_group, expected.group))) |before| {
        if (!std.mem.eql(u8, before, encoded.slice())) return error.InvalidSqlRelationPersistence;
    } else try txn.put(try groupKey(&buffer, expected.metadata_group, expected.group), encoded.slice());
}

pub fn read(a: A, reader: anytype, group: u64, scope: @import("sql_relation_attestation.zig").CreationScope, table: u64) !?Proof {
    var buffer: [320]u8 = undefined;
    const bytes = (try optional(reader, try key(&buffer, group, scope.plan_id, table))) orelse return null;
    const value = try Record.decode(bytes);
    try value.validate();
    if (value.metadata_group != group or value.namespace.table_id != table or !std.meta.eql(value.plan, scope)) return error.InvalidSqlRelationPersistence;
    const reverse = (try optional(reader, try groupKey(&buffer, group, value.group))) orelse return error.InvalidSqlRelationPersistence;
    if (!std.mem.eql(u8, bytes, reverse)) return error.InvalidSqlRelationPersistence;
    // The compact inverse pointer alone is never a GC grant. Rejoin the
    // existing immutable job and its actual canceled-owner receipt, including
    // after finish_cancel removed the live hidden directory.
    const job_raw = (try optional(reader, try stages.jobKey(&buffer, group, scope.plan_id))) orelse return error.InvalidSqlRelationPersistence;
    const job = try std.json.parseFromSliceLeaky(stages.Job, a, job_raw, .{});
    if (!std.mem.eql(u8, &job.plan_digest, &scope.plan_digest)) return error.InvalidSqlRelationPersistence;
    const target = for (job.plan.targets) |candidate| {
        if (candidate.table.table_id == table) break candidate;
    } else return error.InvalidSqlRelationPersistence;
    if (target.table.sql_persistence == null or !target.table.sql_persistence.?.eql(value.descriptor) or target.ranges.len != 1 or
        target.ranges[0].group_id != value.group or @import("table_manager.zig").rangeDocIdentityShardId(target.ranges[0]) != value.namespace.shard_id or
        @import("table_manager.zig").rangeDocIdentityRangeId(target.ranges[0]) != value.namespace.range_id) return error.InvalidSqlRelationPersistence;
    const progress_raw = (try optional(reader, try stages.progressKey(&buffer, group, scope.plan_id))) orelse return error.InvalidSqlRelationPersistence;
    const progress = try std.json.parseFromSliceLeaky(stages.Progress, a, progress_raw, .{});
    if ((progress.state != .canceling and progress.state != .canceled) or progress.revision < value.cancellation_revision) return error.InvalidSqlRelationPersistence;
    const receipt = (try optional(reader, try stages.receiptKey(&buffer, group, scope.plan_id, .canceling, value.group))) orelse return error.InvalidSqlRelationPersistence;
    if (!std.mem.eql(u8, receipt, &try value.digest())) return error.InvalidSqlRelationPersistence;
    return value;
}
pub fn readGroup(a: A, reader: anytype, group: u64, source_group: u64) !?Proof {
    var buffer: [320]u8 = undefined;
    const bytes = (try optional(reader, try groupKey(&buffer, group, source_group))) orelse return null;
    const value = try Record.decode(bytes);
    if (value.group != source_group or value.metadata_group != group) return error.InvalidSqlRelationPersistence;
    return try read(a, reader, group, value.plan, value.namespace.table_id);
}

test "SQL native unpublished cancellation requires atomic owner receipt and rejects orphan or wrong group GC authority after cold reopen" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const working = arena.allocator();
    const docstore = local.storage_docstore;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/unpublished-cancel-metadata", .{tmp.sub_path}, 0);
    defer a.free(path);
    var store = try docstore.DocStore.open(a, path.ptr, .{});
    var store_open = true;
    defer if (store_open) store.close();
    const descriptor: p.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 1 };
    const ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 201, .group_id = 202, .range_id = 202, .doc_identity_shard_id = 202, .doc_identity_range_id = 202, .start_key = "" }};
    const targets = [_]stages.Target{.{ .source_table_id = 7, .empty_generation = true, .schema_only_snapshot = .{ .manifest_digest = @splat(3), .schema_digest = @splat(4), .source_table_id = 7 }, .table = .{ .table_id = 201, .name = "native-canceled", .schema_json = "{}", .sql_persistence = descriptor, .desired_replica_count = 1 }, .ranges = &ranges }};
    const plan: stages.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &targets };
    try plan.validate(working);
    const scope: @import("sql_relation_attestation.zig").CreationScope = .{ .plan_id = plan.id, .plan_digest = try plan.digest(working) };
    const job: stages.Job = .{ .plan = plan, .plan_digest = scope.plan_digest };
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const boot: boots.Record = .{ .node_id = 9, .identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = 11, .public_key = pair.public_key.toBytes() }, .reporter_incarnation = 2, .generation = 2, .instance = @splat(2), .claim_digest = @splat(8), .unclean_restart = true, .phase = .recovering };
    var buffer: [320]u8 = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(try stages.jobKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, job, .{}));
        try txn.put(try stages.progressKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, stages.Progress{}, .{}));
        try txn.put(try stages.identityKey(&buffer, 1, .table, 201), &plan.id);
        try std.testing.expectError(error.RestoreStagingProgressChanged, capture(working, &txn, 1, scope, 201, boot));
        try txn.put(try stages.progressKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, stages.Progress{ .state = .canceling, .revision = 2 }, .{}));
        try txn.commit();
    }
    var proof: Proof = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        proof = try capture(working, &txn, 1, scope, 201, boot);
        try std.testing.expect(proof.initialized == null); // No fabricated root or Pin.
        try stage(working, &txn, proof, boot);
        // Compact rows alone have no disposal authority, even in the exact
        // transaction about to install the existing canceled-owner position.
        try std.testing.expectError(error.InvalidSqlRelationPersistence, read(working, &txn, 1, scope, 201));
        try txn.put(try stages.receiptKey(&buffer, 1, plan.id, .canceling, 202), &try proof.digest());
        try txn.put(try stages.progressKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, stages.Progress{ .state = .canceling, .revision = 3, .completed_owners = 1 }, .{}));
        try std.testing.expectEqualDeep(proof, (try readGroup(working, &txn, 1, 202)).?);
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
        try std.testing.expectEqualDeep(proof, (try readGroup(working, &txn, 1, 202)).?);
        const record = try Record.init(proof);
        try txn.put(try groupKey(&buffer, 1, 203), record.slice());
        try std.testing.expectError(error.InvalidSqlRelationPersistence, readGroup(working, &txn, 1, 203));
        try txn.delete(try stages.receiptKey(&buffer, 1, plan.id, .canceling, 202));
        try std.testing.expectError(error.InvalidSqlRelationPersistence, readGroup(working, &txn, 1, 202));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(try stages.progressKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(working, stages.Progress{ .state = .canceled, .revision = 4, .completed_owners = 1 }, .{}));
        // Terminal job state retains the canonical owner receipt and compact
        // lookup so ordinary asynchronous root GC can recover after restart.
        try std.testing.expectEqualDeep(proof, (try readGroup(working, &txn, 1, 202)).?);
        try txn.commit();
    }
}
