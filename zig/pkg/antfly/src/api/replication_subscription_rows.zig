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

const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = @import("table_catalog.zig");
const codec = @import("../system_catalog/replication/snapshot_codec.zig");
const chain = @import("../system_catalog/replication/snapshot_chain.zig");
const registry = @import("../system_catalog/prepared_transactions.zig");
const Relation = local.system_catalog_logical_replication_origin.Relation;
const types = local.storage_db_types;
const A = std.mem.Allocator;

pub const Group = struct {
    participant: registry.Participant,
    intent: types.TransactionIntentRequest,
};

const Building = struct {
    route: catalog.CatalogGroupRoute,
    writes: std.ArrayList(types.BatchWrite) = .empty,
    deletes: std.ArrayList([]const u8) = .empty,
    predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty,
};

fn rowKey(a: A, scope: [32]u8, relation: u32, offset: u64) ![]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/subscription-copy-row/v1\x00");
    hash.update(&scope);
    var numbers: [12]u8 = undefined;
    std.mem.writeInt(u32, numbers[0..4], relation, .little);
    std.mem.writeInt(u64, numbers[4..12], offset, .little);
    hash.update(&numbers);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return std.fmt.allocPrint(a, "sql:{x}", .{digest[0..16]});
}

/// Keyed publishers use the same address in COPY and WAL regardless of page
/// boundaries or schema column order in the serialized row.
pub fn replicaKey(a: A, scope: [32]u8, relation: Relation, row: codec.Row) !?[]u8 {
    if (relation.replica_identity == .full or relation.replica_identity == .nothing) return null;
    if (row.value != .object) return error.InvalidReplicationSourceRow;
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
    try writer.writer.writeAll("antfly/subscription-replica-key/v1\x00");
    try writer.writer.writeAll(&scope);
    try writer.writer.writeInt(u32, relation.id, .little);
    var has_key = false;
    for (relation.columns) |column| {
        if (!column.key) continue;
        const cell = row.value.object.get(column.name) orelse return error.InvalidReplicationSourceRow;
        if (cell == .null and !containsField(row.json_null_fields, column.name))
            return error.InvalidReplicationSourceRow;
        if (column.type_oid == 1700) {
            // PostgreSQL NUMERIC equality ignores display scale. Keep it in
            // the row payload, while hashing the exact logical key value.
            if (cell != .number_string) return error.InvalidReplicationSourceRow;
            const numeric = local.sql_numeric_value;
            var context: numeric.Context = .{ .alloc = a };
            var value = try numeric.parse(&context, cell.number_string);
            defer value.deinit();
            try numeric.hash(&context, value.value, &writer.hasher);
        } else if (column.type_oid == 700) {
            try writer.writer.writeInt(u32, @bitCast(try replicaFloat(f32, cell)), .little);
        } else if (column.type_oid == 701) {
            try writer.writer.writeInt(u64, @bitCast(try replicaFloat(f64, cell)), .little);
        } else {
            try std.json.Stringify.value(cell, .{}, &writer.writer);
        }
        try writer.writer.writeByte(0);
        has_key = true;
    }
    if (!has_key) return null;
    var digest: [32]u8 = undefined;
    writer.hasher.final(&digest);
    return try std.fmt.allocPrint(a, "sql:{x}", .{digest[0..16]});
}

fn replicaFloat(comptime T: type, cell: std.json.Value) !T {
    const value: T = switch (cell) {
        .float => |number| @floatCast(number),
        .number_string => |text| try std.fmt.parseFloat(T, text),
        else => return error.InvalidReplicationSourceRow,
    };
    if (!std.math.isFinite(value)) return error.UnsupportedReplicationType;
    return if (value == 0) 0 else value;
}

fn containsField(fields: []const []const u8, name: []const u8) bool {
    for (fields) |field| if (std.mem.eql(u8, field, name)) return true;
    return false;
}

pub const PagePlan = struct { event: registry.ReplicationEvent, groups: []const Group };

pub fn planPage(a: A, routing: *const catalog.AuthoritativeTableRoutingSnapshot, table: []const u8, schema_version: u32, source_scope: [32]u8, source_point: u64, expected_page: chain.Digest, relation: Relation, page: codec.CapturePage, proof: chain.Page) !PagePlan {
    if (source_point == 0 or std.mem.allEqual(u8, &source_scope, 0) or routing.execution != .raft or
        relation.id != page.relation_id or page.relation_id != proof.relation or page.offset != proof.offset or page.rows.len != proof.count or
        !std.mem.eql(u8, &expected_page, &try proof.commitment())) return error.InvalidReplicationSnapshotCursor;

    var hashing = std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256).initHasher(std.crypto.hash.sha2.Sha256.init(.{}), &.{});
    try std.json.Stringify.value(page, .{}, &hashing.writer);
    var actual: [32]u8 = undefined;
    hashing.hasher.final(&actual);
    if (!std.mem.eql(u8, &actual, &proof.content_digest)) return error.InvalidReplicationSnapshotCursor;

    var groups: std.ArrayList(Building) = .empty;
    var index: std.AutoHashMapUnmanaged(u64, usize) = .empty;
    defer index.deinit(a);
    for (page.rows, 0..) |row, ordinal| {
        if (row.value != .object) return error.InvalidReplicationSourceRow;
        const key = (try replicaKey(a, source_scope, relation, row)) orelse try rowKey(a, source_scope, page.relation_id, page.offset + ordinal);
        const route = routing.resolveRouteForKey(key) orelse return error.CatalogGenerationChanged;
        const slot = try index.getOrPut(a, route.group_id);
        if (!slot.found_existing) {
            if (groups.items.len == registry.max_participants) return error.CatalogCommandTooLarge;
            slot.value_ptr.* = groups.items.len;
            try groups.append(a, .{ .route = route });
        }
        const group = &groups.items[slot.value_ptr.*];
        try group.writes.append(a, .{ .key = key, .value = try std.json.Stringify.valueAlloc(a, row.value, .{}), .json_null_fields = row.json_null_fields });
        try group.predicates.append(a, .{ .key = key, .expected_version = 0, .unique_absence = true, .row_policy_requirement = .insert });
    }

    defer groups.deinit(a);
    const result = try finishGroups(a, routing, table, schema_version, groups.items);
    return .{ .groups = result, .event = .{
        .kind = .snapshot_page,
        .source_point = source_point,
        .end_point = source_point,
        .offset = proof.offset,
        .next_offset = proof.offset + proof.count,
        .changes_digest = proof.content_digest,
        .snapshot_page = proof,
    } };
}

const foreign = @import("../foreign/source.zig");

pub const WalWork = struct {
    event: registry.ReplicationEvent,
    changes: []const foreign.ReplicationChange,
};

/// Consume a validated provider window one whole source transaction at a
/// time. The event starts at the durable apply cursor, which may precede
/// the next published commit LSN due to unrelated WAL.
pub fn nextWalTransaction(result: foreign.ReplicationPollResult, checkpoint: u64) !?WalWork {
    if (checkpoint == 0) return error.ReplicationCutoverAuthorityLost;
    const transactions = result.transactions orelse return error.InvalidReplicationTransaction;
    try result.validateTransactions();
    for (transactions) |txn| {
        if (txn.end_lsn <= checkpoint) continue;
        if (txn.commit_lsn < checkpoint) return error.InvalidReplicationTransaction;
        const changes = result.changes[txn.first_change .. txn.first_change + txn.change_count];
        const digest = try @import("../metadata/replication_digest.zig").digest("antfly/subscription-wal/v1\x00", .{
            .xid = txn.xid,
            .commit_lsn = txn.commit_lsn,
            .end_lsn = txn.end_lsn,
            .commit_timestamp_ms = txn.commit_timestamp_ms,
            .changes = changes,
        }, 64 * 1024 * 1024);
        return .{ .event = .{ .kind = .transaction, .source_point = checkpoint, .end_point = txn.end_lsn, .changes_digest = digest }, .changes = changes };
    }
    return null;
}

test "subscription capture handoff selects a complete WAL transaction at the durable cursor" {
    var transactions = [_]foreign.ReplicationTransaction{
        .{ .xid = 1, .final_lsn = 15, .commit_lsn = 15, .end_lsn = 16, .commit_timestamp_ms = 200, .first_change = 0, .change_count = 0, .checkpoint = @constCast("0/10") },
        .{ .xid = 2, .final_lsn = 31, .commit_lsn = 31, .end_lsn = 32, .commit_timestamp_ms = 201, .first_change = 0, .change_count = 1, .checkpoint = @constCast("0/20") },
    };
    var changes = [_]foreign.ReplicationChange{.{ .op = .insert, .checkpoint = @constCast("0/20"), .commit_timestamp_ms = 201 }};
    const result: foreign.ReplicationPollResult = .{ .transactions = &transactions, .changes = &changes, .checkpoint = @constCast("0/20") };
    const next = (try nextWalTransaction(result, 16)).?;
    try std.testing.expectEqual(@as(u64, 16), next.event.source_point);
    try std.testing.expectEqual(@as(u64, 32), next.event.end_point);
    try std.testing.expectEqual(@as(usize, 1), next.changes.len);
    try std.testing.expect((try nextWalTransaction(result, 32)) == null);
    changes[0].op = .delete;
    try std.testing.expect(!std.mem.eql(u8, &next.event.changes_digest, &(try nextWalTransaction(result, 16)).?.event.changes_digest));
    transactions[1].first_change = 1;
    try std.testing.expectError(error.InvalidReplicationTransaction, nextWalTransaction(result, 10));
}

pub const WalRow = struct {
    before_key: ?[]const u8,
    after_key: ?[]const u8,
    /// Missing fields represent unchanged TOAST values in UPDATE. The
    /// destination planner must overlay these on the guarded before image.
    after: ?codec.Row,
};

pub fn addressWalRow(a: A, scope: [32]u8, relation: Relation, change: foreign.ReplicationChange) !WalRow {
    const actual = change.relation orelse return error.InvalidReplicationSourceRow;
    if (actual.id != relation.id or !std.mem.eql(u8, actual.namespace, relation.namespace) or !std.mem.eql(u8, actual.name, relation.name))
        return error.ForeignProviderIdentityMismatch;
    const row: codec.Row = .{ .value = change.row orelse return error.InvalidReplicationSourceRow, .json_null_fields = change.json_null_fields };
    const key = (try replicaKey(a, scope, relation, row)) orelse return error.UnsupportedReplicationType;
    errdefer a.free(key);
    return switch (change.op) {
        .insert => .{ .before_key = null, .after_key = key, .after = row },
        .delete => .{ .before_key = key, .after_key = null, .after = null },
        .update => .{
            .before_key = if (change.old_row) |old|
                (try replicaKey(a, scope, relation, .{ .value = old, .json_null_fields = change.old_json_null_fields })) orelse return error.UnsupportedReplicationType
            else
                key,
            .after_key = key,
            .after = row,
        },
    };
}

/// Preserve both omitted TOAST fields and subscriber-only fields from the
/// version-guarded destination image. An explicit SQL null still overwrites.
pub fn materializeWalRow(a: A, patch: codec.Row, before: ?codec.Row) !codec.Row {
    return overlayWalRow(a, patch, before, false);
}

fn overlayWalRow(a: A, patch: codec.Row, before: ?codec.Row, reuse: bool) !codec.Row {
    if (patch.value != .object) return error.InvalidReplicationSourceRow;
    const previous = before orelse return patch;
    if (previous.value != .object) return error.InvalidReplicationSourceRow;
    var fields: std.ArrayList([]const u8) = .empty;
    errdefer fields.deinit(a);
    for (previous.json_null_fields) |name| {
        if (!patch.value.object.contains(name)) try fields.append(a, name);
    }
    try fields.appendSlice(a, patch.json_null_fields);
    const owned_fields = try fields.toOwnedSlice(a);
    errdefer a.free(owned_fields);
    var object: std.json.ObjectMap = if (reuse) previous.value.object else .empty;
    if (reuse) {
        try object.ensureUnusedCapacity(a, patch.value.object.count());
    } else {
        try object.ensureTotalCapacity(a, previous.value.object.count() + patch.value.object.count());
        var old = previous.value.object.iterator();
        while (old.next()) |entry| object.putAssumeCapacityNoClobber(entry.key_ptr.*, entry.value_ptr.*);
    }
    var updated = patch.value.object.iterator();
    while (updated.next()) |entry| object.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
    return .{ .value = .{ .object = object }, .json_null_fields = owned_fields };
}

test "subscription capture float replica identities canonicalize source precision and signed zero" {
    try std.testing.expectEqual(@as(f32, 0.1), try replicaFloat(f32, .{ .number_string = "0.1" }));
    try std.testing.expectEqual(@as(f32, 0.1), try replicaFloat(f32, .{ .float = @as(f32, 0.1) }));
    try std.testing.expectEqual(@as(u32, 0), @as(u32, @bitCast(try replicaFloat(f32, .{ .float = -0.0 }))));
    try std.testing.expectEqual(@as(u64, 0), @as(u64, @bitCast(try replicaFloat(f64, .{ .number_string = "-0" }))));
    try std.testing.expectError(error.UnsupportedReplicationType, replicaFloat(f64, .{ .number_string = "Infinity" }));
}

test "subscription capture numeric replica identity ignores display scale" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const relation: Relation = .{
        .id = 15,
        .namespace = "public",
        .name = "t",
        .replica_identity = .default,
        .columns = &.{.{ .name = "id", .type_oid = 1700, .type_modifier = -1, .key = true }},
    };
    const before = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":7.001}", .{ .parse_numbers = false });
    const after = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":7.0010}", .{ .parse_numbers = false });
    const different = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":7.0001}", .{ .parse_numbers = false });
    const key = (try replicaKey(a, @splat(1), relation, .{ .value = before, .json_null_fields = &.{} })).?;
    try std.testing.expectEqualStrings(key, (try replicaKey(a, @splat(1), relation, .{ .value = after, .json_null_fields = &.{} })).?);
    try std.testing.expect(!std.mem.eql(u8, key, (try replicaKey(a, @splat(1), relation, .{ .value = different, .json_null_fields = &.{} })).?));
}

test "subscription capture WAL key moves preserve omitted values and null provenance" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const relation: Relation = .{
        .id = 15,
        .namespace = "public",
        .name = "t",
        .replica_identity = .default,
        .columns = &.{
            .{ .name = "id", .type_oid = 20, .type_modifier = -1, .key = true },
            .{ .name = "payload", .type_oid = 3802, .type_modifier = -1, .key = false },
            .{ .name = "replace", .type_oid = 3802, .type_modifier = -1, .key = false },
        },
    };
    const previous = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":7,\"payload\":null,\"replace\":null,\"local\":\"keep\"}", .{});
    const patch = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":8,\"replace\":null}", .{});
    const old_key = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":7}", .{});
    var change: foreign.ReplicationChange = .{
        .op = .update,
        .relation = .{ .id = 15, .namespace = @constCast("public"), .name = @constCast("t") },
        .checkpoint = @constCast("0/20"),
        .row = patch,
        .old_row = old_key,
    };
    const scope: [32]u8 = @splat(3);
    const addressed = try addressWalRow(a, scope, relation, change);
    try std.testing.expect(!std.mem.eql(u8, addressed.before_key.?, addressed.after_key.?));
    const image: codec.Row = .{ .value = previous, .json_null_fields = &.{ "payload", "replace" } };
    try std.testing.expectEqualStrings((try replicaKey(a, scope, relation, image)).?, addressed.before_key.?);
    const merged = try materializeWalRow(a, addressed.after.?, image);
    try std.testing.expectEqualStrings("keep", merged.value.object.get("local").?.string);
    try std.testing.expectEqual(@as(i64, 8), merged.value.object.get("id").?.integer);
    try std.testing.expectEqualDeep(@as([]const []const u8, &.{"payload"}), merged.json_null_fields);
    const guarded = try guardWalChange(a, addressed, .{ .key = addressed.before_key.?, .row = image, .version = 42, .content_digest = @splat(9) });
    try std.testing.expectEqual(.delete, guarded[0].predicate.row_policy_requirement);
    try std.testing.expectEqual(@as(u64, 42), guarded[0].predicate.expected_version);
    try std.testing.expectEqual(.insert, guarded[1].predicate.row_policy_requirement);
    try std.testing.expect(guarded[1].predicate.unique_absence);
    const high = if (std.mem.lessThan(u8, addressed.before_key.?, addressed.after_key.?)) addressed.after_key.? else addressed.before_key.?;
    const Range = @import("../metadata/table_manager.zig").RangeRecord;
    const low_range: Range = .{ .group_id = 10, .range_id = 11, .table_id = 12, .start_key = "", .end_key = high };
    const high_range: Range = .{ .group_id = 20, .range_id = 21, .table_id = 12, .start_key = high };
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    const routing: catalog.AuthoritativeTableRoutingSnapshot = .{
        .snapshot = .{ .source = undefined, .value = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 8, .tables = &.{}, .ranges = &.{} } },
        .table_id = 12,
        .ranges = &.{ &low_range, &high_range },
        .topology_epoch = 1,
    };
    const destination: registry.ReplicationDestination = .{ .table_id = 12, .storage_name = "records", .schema_version = 2, .schema_digest = @splat(11) };
    const participants = try routeWalEffects(a, &routing, destination, guarded);
    try std.testing.expectEqual(@as(usize, 2), participants.len);
    try std.testing.expect(participants[0].participant.route.route.group_id != participants[1].participant.route.route.group_id);
    try std.testing.expectEqualStrings(addressed.before_key.?, participants[0].intent.deletes[0]);
    try std.testing.expectEqualStrings(addressed.after_key.?, participants[1].intent.writes[0].key);
    try std.testing.expectError(error.InvalidReplicationTransaction, routeWalEffects(a, &routing, destination, &.{ guarded[0], guarded[0] }));
    const Observe = struct {
        value: codec.Row,
        key: []const u8,

        fn read(raw: *anyopaque, _: A, key: []const u8) !?WalObserved {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, self.key, key)) return null;
            return .{ .key = key, .row = self.value, .version = 42, .content_digest = @splat(9) };
        }
    };
    var observe: Observe = .{ .value = image, .key = addressed.before_key.? };
    const event: registry.ReplicationEvent = .{ .kind = .transaction, .source_point = 16, .end_point = 32, .changes_digest = @splat(8) };
    const tables = [_]WalTable{.{
        .relation = relation,
        .destination = destination,
        .routing = &routing,
        .reader = .{ .ptr = &observe, .observe = Observe.read },
    }};
    const plan = try planWal(a, scope, .{ .event = event, .changes = &.{change} }, &tables);
    try std.testing.expectEqual(@as(usize, 2), plan.len);
    var dir = try local.common_test_directory.TestDirectory.init("subscription-wal-seal");
    defer dir.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, dir.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const admission = @import("replication_subscription_admission.zig");
    const attempt: admission.Attempt = .{
        .metadata_group = 1,
        .incarnation = incarnation,
        .owner = .{ .logical_subscription = .{ .database_id = 1, .subscription_id = 2 } },
        .birth = 2,
        .generation = 5,
        .origin = scope,
        .txn_id = @splat(7),
        .recipe_digest = @splat(6),
        .upload_token = 5,
        .begin_timestamp = 10,
        .admitted_at_ns = 20,
        .expires_at_ns = 100,
        .event = event,
        .wal_access = &.{.{
            .table_id = destination.table_id,
            .catalog_revision = routing.snapshot.value.catalog_revision,
            .schema_version = destination.schema_version,
            .schema_digest = destination.schema_digest,
            .policy_epoch = destination.policy_epoch,
            .rights = local.system_catalog_privileges.Access.bit(.update),
        }},
    };
    const command = try admission.sealWal(a, &store, attempt, plan, &.{ null, null }, .none);
    try std.testing.expectEqual(@as(usize, 2), command.intents.?.members.len);
    try std.testing.expect(command.page_proof == null);
    try std.testing.expectEqual(command.permits[0].reference, command.permits[1].reference);
    observe.key = "";
    const skipped = try planWal(a, scope, .{ .event = event, .changes = &.{change} }, &tables);
    try std.testing.expectEqual(@as(usize, 0), skipped.len);
    const empty = try admission.sealWal(a, &store, attempt, skipped, &.{}, .none);
    try std.testing.expect(empty.intents == null);
    try std.testing.expectEqualDeep(attempt.wal_access, empty.wal_access);
    var epoch: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&epoch, &incarnation);
    var mutation: registry.Mutation = .{
        .incarnation = epoch,
        .admission = .{ .nonce = @splat(18), .head = 0 },
        .database_id = 1,
        .actor = .{ .session_user = .bootstrap, .credential = "" },
        .operation = .{ .replication_admit = empty },
    };
    try mutation.validate();
    var unfenced = attempt.wal_access.?[0];
    unfenced.schema_version = 0;
    mutation.operation.replication_admit.wal_access = &.{unfenced};
    try mutation.validate();
    unfenced.catalog_revision = 0;
    mutation.operation.replication_admit.wal_access = &.{unfenced};
    try std.testing.expectError(error.InvalidCatalogMutation, mutation.validate());
    mutation.operation.replication_admit.wal_access = null;
    try std.testing.expectError(error.InvalidCatalogMutation, mutation.validate());
    try std.testing.expectEqual(@as(usize, 0), empty.permits.len);
    change.old_row = null;
    const unchanged = try addressWalRow(a, scope, relation, change);
    try std.testing.expectEqualStrings(unchanged.before_key.?, unchanged.after_key.?);
    change.relation.?.id += 1;
    try std.testing.expectError(error.ForeignProviderIdentityMismatch, addressWalRow(a, scope, relation, change));
}

pub const WalObserved = struct {
    key: []const u8,
    row: codec.Row,
    version: u64,
    content_digest: [32]u8,
};

pub const WalEffect = struct {
    predicate: types.TransactionVersionPredicate,
    // An effect with no write deletes the predicate's key.
    write: ?types.BatchWrite = null,
};

/// Produce the guarded effects of one addressed change. The transaction
/// planner must enroll all emitted effects in one source decision.
pub fn guardWalChange(a: A, change: WalRow, observed: ?WalObserved) ![]const WalEffect {
    const before_key = change.before_key;
    const after_key = change.after_key;
    if ((before_key == null) != (observed == null) or
        (after_key == null) != (change.after == null) or (before_key == null and after_key == null))
        return error.InvalidReplicationSourceRow;
    const same_key = before_key != null and after_key != null and std.mem.eql(u8, before_key.?, after_key.?);
    var effects: [2]WalEffect = undefined;
    var count: usize = 0;
    if (observed) |prior| {
        if (!std.mem.eql(u8, prior.key, before_key.?) or prior.version == 0 or std.mem.allEqual(u8, &prior.content_digest, 0))
            return error.ReplicationCutoverAuthorityLost;
        effects[count] = .{ .predicate = .{
            .key = prior.key,
            .expected_version = prior.version,
            .expected_content_digest = prior.content_digest,
            .row_policy_requirement = if (same_key) .update else .delete,
        } };
        count += 1;
    }
    if (change.after) |patch| {
        const row = try materializeWalRow(a, patch, if (observed) |prior| prior.row else null);
        if (!same_key) {
            effects[count] = .{ .predicate = .{
                .key = after_key.?,
                .expected_version = 0,
                .unique_absence = true,
                .row_policy_requirement = .insert,
            } };
            count += 1;
        }
        effects[count - 1].write = .{
            .key = after_key.?,
            .value = try std.json.Stringify.valueAlloc(a, row.value, .{}),
            .json_null_fields = row.json_null_fields,
        };
    }
    return a.dupe(WalEffect, effects[0..count]);
}

/// Group guarded effects by destination owner using one pinned table cut.
/// A repeated key needs transaction-local row folding before routing.
pub fn routeWalEffects(a: A, routing: *const catalog.AuthoritativeTableRoutingSnapshot, destination: registry.ReplicationDestination, effects: []const WalEffect) ![]const Group {
    if (routing.execution != .raft or routing.table_id != destination.table_id) return error.CatalogGenerationChanged;
    var owners: std.AutoHashMapUnmanaged(u64, usize) = .empty;
    defer owners.deinit(a);
    var keys: std.StringHashMapUnmanaged(void) = .empty;
    defer keys.deinit(a);
    var groups: std.ArrayList(Building) = .empty;
    defer groups.deinit(a);
    for (effects) |effect| {
        const key = effect.predicate.key;
        if (effect.write) |write| if (!std.mem.eql(u8, write.key, key)) return error.InvalidReplicationSourceRow;
        if ((try keys.getOrPut(a, key)).found_existing) return error.InvalidReplicationTransaction;
        const route = routing.resolveRouteForKey(key) orelse return error.CatalogGenerationChanged;
        const entry = try owners.getOrPut(a, route.group_id);
        if (!entry.found_existing) {
            if (groups.items.len == registry.max_participants) return error.CatalogCommandTooLarge;
            entry.value_ptr.* = groups.items.len;
            try groups.append(a, .{ .route = route });
        }
        const group = &groups.items[entry.value_ptr.*];
        try group.predicates.append(a, effect.predicate);
        if (effect.write) |write| try group.writes.append(a, write) else try group.deletes.append(a, key);
    }
    return finishGroups(a, routing, destination.storage_name, destination.schema_version, groups.items);
}

fn finishGroups(a: A, routing: *const catalog.AuthoritativeTableRoutingSnapshot, table: []const u8, schema_version: u32, groups: []Building) ![]const Group {
    const result = try a.alloc(Group, groups.len);
    for (groups, result) |*group, *out| {
        out.* = .{
            .participant = .{ .table_name = table, .route = routing.fenceForRoute(group.route), .schema_version = schema_version },
            .intent = .{
                .schema_version = schema_version,
                .writes = try group.writes.toOwnedSlice(a),
                .deletes = try group.deletes.toOwnedSlice(a),
                .predicates = try group.predicates.toOwnedSlice(a),
            },
        };
    }
    return result;
}

/// Fold one source transaction for a single destination table. The reader
/// must return the row and content digest from the same pinned owner cut.
pub const WalReader = struct {
    ptr: *anyopaque,
    observe: *const fn (*anyopaque, A, []const u8) anyerror!?WalObserved,
};

pub const WalFold = struct {
    allocator: A,
    reader: WalReader,
    rows: std.StringArrayHashMapUnmanaged(State) = .empty,

    const State = struct {
        original: ?WalObserved,
        current: ?codec.Row,
        owns_current: bool = false,
    };

    fn ensure(self: *WalFold, key: []const u8) !void {
        if (self.rows.contains(key)) return;
        const observed = try self.reader.observe(self.reader.ptr, self.allocator, key);
        if (observed) |row| {
            if (!std.mem.eql(u8, row.key, key) or row.version == 0 or std.mem.allEqual(u8, &row.content_digest, 0))
                return error.ReplicationCutoverAuthorityLost;
        }
        try self.rows.put(self.allocator, key, .{ .original = observed, .current = if (observed) |row| row.row else null });
    }

    pub fn add(self: *WalFold, change: WalRow) !void {
        if ((change.after_key == null) != (change.after == null) or
            (change.before_key == null and change.after_key == null)) return error.InvalidReplicationSourceRow;
        // Missing UPDATE/DELETE rows are skipped at the observed cut. A
        // skipped key-changing UPDATE must not probe or reserve its new key.
        if (change.before_key) |key| {
            try self.ensure(key);
            if (self.rows.get(key).?.current == null) return;
        }
        if (change.after_key) |key| try self.ensure(key);
        const before = if (change.before_key) |key| self.rows.getPtr(key).? else null;
        const after = if (change.after_key) |key| self.rows.getPtr(key).? else null;
        if (after) |state| if (before != after and state.current != null) return error.UniqueConstraintViolation;
        const image = if (change.after) |patch|
            try overlayWalRow(self.allocator, patch, if (before) |state| state.current else null, if (before) |state| state.owns_current else false)
        else
            null;
        if (before) |state| state.current = null;
        if (after) |state| {
            state.current = image;
            state.owns_current = before != null;
        }
    }

    pub fn finish(self: *const WalFold) ![]const WalEffect {
        var effects: std.ArrayList(WalEffect) = .empty;
        errdefer effects.deinit(self.allocator);
        for (self.rows.keys(), self.rows.values()) |key, state| {
            const original = state.original;
            const current = state.current;
            if (original == null and current == null) continue;
            var effect: WalEffect = .{ .predicate = .{
                .key = key,
                .expected_version = if (original) |row| row.version else 0,
                .expected_content_digest = if (original) |row| row.content_digest else null,
                .unique_absence = original == null,
                .row_policy_requirement = if (current == null) .delete else if (original == null) .insert else .update,
            } };
            if (current) |row| effect.write = .{
                .key = key,
                .value = try std.json.Stringify.valueAlloc(self.allocator, row.value, .{}),
                .json_null_fields = row.json_null_fields,
            };
            try effects.append(self.allocator, effect);
        }
        return effects.toOwnedSlice(self.allocator);
    }
};

test "subscription capture folds ordered repeated changes and reads each destination key once" {
    const Source = struct {
        original: codec.Row,
        calls: usize = 0,

        fn observe(raw: *anyopaque, _: A, key: []const u8) !?WalObserved {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (!std.mem.eql(u8, key, "first")) return null;
            return .{ .key = key, .row = self.original, .version = 41, .content_digest = @splat(9) };
        }
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":1,\"payload\":null,\"local\":\"preserve\"}", .{});
    const first_patch = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"status\":\"changed\"}", .{});
    const second_patch = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"id\":2}", .{});
    var source: Source = .{ .original = .{ .value = original, .json_null_fields = &.{"payload"} } };
    var fold: WalFold = .{ .allocator = a, .reader = .{ .ptr = &source, .observe = Source.observe } };
    try fold.add(.{ .before_key = "missing", .after_key = "first", .after = .{ .value = first_patch, .json_null_fields = &.{} } });
    try fold.add(.{ .before_key = "missing", .after_key = null, .after = null });
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    try std.testing.expectEqual(@as(usize, 0), (try fold.finish()).len);
    try std.testing.expectError(error.UniqueConstraintViolation, fold.add(.{ .before_key = null, .after_key = "first", .after = .{ .value = original, .json_null_fields = &.{} } }));
    try fold.add(.{ .before_key = "first", .after_key = "first", .after = .{ .value = first_patch, .json_null_fields = &.{} } });
    const repeated: WalRow = .{ .before_key = "first", .after_key = "first", .after = .{ .value = first_patch, .json_null_fields = &.{} } };
    try fold.add(repeated);
    const owned_keys = fold.rows.get("first").?.current.?.value.object.keys().ptr;
    try fold.add(repeated);
    try std.testing.expect(owned_keys == fold.rows.get("first").?.current.?.value.object.keys().ptr);
    try std.testing.expect(original.object.get("status") == null);
    try fold.add(.{ .before_key = "first", .after_key = "second", .after = .{ .value = second_patch, .json_null_fields = &.{} } });
    try fold.add(.{ .before_key = null, .after_key = "transient", .after = .{ .value = original, .json_null_fields = &.{"payload"} } });
    try fold.add(.{ .before_key = "transient", .after_key = null, .after = null });
    try std.testing.expectEqual(@as(usize, 4), source.calls);
    const effects = try fold.finish();
    try std.testing.expectEqual(@as(usize, 2), effects.len);
    try std.testing.expectEqualStrings("first", effects[0].predicate.key);
    try std.testing.expectEqual(@as(u64, 41), effects[0].predicate.expected_version);
    try std.testing.expectEqual(@as(?[32]u8, @splat(9)), effects[0].predicate.expected_content_digest);
    try std.testing.expect(effects[0].write == null);
    const inserted = effects[1].write.?;
    try std.testing.expectEqualStrings("second", inserted.key);
    try std.testing.expect(effects[1].predicate.unique_absence);
    try std.testing.expectEqualDeep(@as([]const []const u8, &.{"payload"}), inserted.json_null_fields);
    const image = try std.json.parseFromSliceLeaky(std.json.Value, a, inserted.value, .{});
    try std.testing.expectEqualStrings("preserve", image.object.get("local").?.string);
    try std.testing.expectEqualStrings("changed", image.object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 2), image.object.get("id").?.integer);
}

pub const WalTable = struct {
    relation: Relation,
    destination: registry.ReplicationDestination,
    routing: *const catalog.AuthoritativeTableRoutingSnapshot,
    reader: WalReader,
};

pub const WalMember = struct {
    policy_epoch: local.api_replication_source_apply.PolicyEpoch,
    group: Group,
    schema_digest: [32]u8,
};

/// All table cuts and point observations must belong to the same
/// coordinator attempt. Preserve source order within each destination.
pub fn planWal(a: A, scope: [32]u8, transaction: WalWork, tables: []const WalTable) ![]const WalMember {
    if (transaction.event.kind != .transaction) return error.InvalidReplicationTransaction;
    var bindings: std.AutoHashMapUnmanaged(u32, usize) = .empty;
    defer bindings.deinit(a);
    var physical: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer physical.deinit(a);
    const folds = try a.alloc(WalFold, tables.len);
    defer a.free(folds);
    for (tables, folds, 0..) |table, *fold, i| {
        if ((try bindings.getOrPut(a, table.relation.id)).found_existing or
            (try physical.getOrPut(a, table.destination.table_id)).found_existing)
            return error.InvalidReplicationTransaction;
        bindings.getPtr(table.relation.id).?.* = i;
        fold.* = .{ .allocator = a, .reader = table.reader };
    }
    for (transaction.changes) |change| {
        const observed = change.relation orelse return error.InvalidReplicationSourceRow;
        const i = bindings.get(observed.id) orelse return error.ForeignProviderIdentityMismatch;
        const addressed = try addressWalRow(a, scope, tables[i].relation, change);
        try folds[i].add(addressed);
    }
    var members: std.ArrayList(WalMember) = .empty;
    for (tables, folds) |table, *fold| {
        defer fold.rows.deinit(a);
        if (fold.rows.count() == 0) continue;
        const effects = try fold.finish();
        const groups = try routeWalEffects(a, table.routing, table.destination, effects);
        if (groups.len > registry.max_participants - members.items.len) return error.CatalogCommandTooLarge;
        for (groups) |group| try members.append(a, .{ .group = group, .schema_digest = table.destination.schema_digest, .policy_epoch = table.destination.policy_epoch });
    }
    return members.toOwnedSlice(a);
}
