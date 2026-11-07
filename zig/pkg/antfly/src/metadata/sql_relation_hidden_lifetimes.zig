// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-native-owner addresses into admitted hidden jobs. These are not copies
//! of schemas or another catalog. Their only purpose is bounded restart work
//! before a hidden target has a public table-lifetime directory entry.
const std = @import("std");
const staging = @import("restore_staging.zig");
const records = @import("schema_validation_publication.zig");
const attestation = @import("sql_relation_attestation.zig");
const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
const A = std.mem.Allocator;
pub const max_page = 16;
pub const Reference = struct {
    node: u64,
    table: u64,
    group: u64,
    plan: attestation.CreationScope,
    pub fn validate(self: Reference) !void {
        try self.plan.validate();
        if (self.node == 0 or self.table == 0 or self.group == 0) return error.InvalidSqlRelationPersistence;
    }
};
const Record = records.Record(Reference, "NRHJ", 1024);
pub const Entry = struct { reference: Reference, descriptor: persistence.Descriptor, initialized: ?attestation.SignedBinding };
pub const Page = struct { entries: []const Entry, next: ?u64 };

pub fn prefix(buffer: []u8, metadata_group: u64) ![]const u8 {
    if (metadata_group == 0) return error.InvalidSqlRelationPersistence;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sql-native-hidden:", .{metadata_group});
}
fn nodePrefix(buffer: []u8, metadata_group: u64, node: u64) ![]const u8 {
    if (metadata_group == 0 or node == 0) return error.InvalidSqlRelationPersistence;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sql-native-hidden:node:{x:0>16}:", .{ metadata_group, node });
}
fn nodeKey(buffer: []u8, metadata_group: u64, node: u64, table: u64) ![]const u8 {
    var selected_prefix: [192]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try nodePrefix(&selected_prefix, metadata_group, node), table });
}
fn tableKey(buffer: []u8, metadata_group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sql-native-hidden:table:{x:0>16}", .{ metadata_group, table });
}
fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}
fn reference(plan: staging.Plan, digest: [32]u8, target: staging.Target) !?Reference {
    const descriptor = target.table.sql_persistence orelse return null;
    try descriptor.validate();
    if (target.ranges.len != 1 or target.ranges[0].table_id != target.table.table_id) return error.InvalidSqlRelationPersistence;
    return .{ .node = descriptor.owner_node_id, .table = target.table.table_id, .group = target.ranges[0].group_id, .plan = .{ .plan_id = plan.id, .plan_digest = digest } };
}
pub fn stagePlan(txn: anytype, metadata_group: u64, plan: staging.Plan, digest: [32]u8) !void {
    for (plan.targets) |target| if (try reference(plan, digest, target)) |ref| {
        try ref.validate();
        try @import("sql_relation_boots.zig").rewindForInsertion(txn, metadata_group, ref.node, ref.table);
        const encoded = try Record.init(ref);
        var key: [256]u8 = undefined;
        const selected = try tableKey(&key, metadata_group, ref.table);
        if (try optional(txn, selected)) |before| if (!std.mem.eql(u8, before, encoded.slice())) return error.InvalidSqlRelationPersistence;
        try txn.put(selected, encoded.slice());
        try txn.put(try nodeKey(&key, metadata_group, ref.node, ref.table), encoded.slice());
    };
}

pub fn removePlan(txn: anytype, metadata_group: u64, plan: staging.Plan, digest: [32]u8) !void {
    for (plan.targets) |target| if (try reference(plan, digest, target)) |ref| {
        var key: [256]u8 = undefined;
        const selected = try tableKey(&key, metadata_group, ref.table);
        const before = (try optional(txn, selected)) orelse return error.InvalidSqlRelationPersistence;
        const decoded = try Record.decode(before);
        if (!std.meta.eql(decoded, ref)) return error.InvalidSqlRelationPersistence;
        try txn.delete(selected);
        const by_node = try nodeKey(&key, metadata_group, ref.node, ref.table);
        const indexed = (try optional(txn, by_node)) orelse return error.InvalidSqlRelationPersistence;
        if (!std.meta.eql(try Record.decode(indexed), ref)) return error.InvalidSqlRelationPersistence;
        try txn.delete(by_node);
    };
}

fn resolve(backing: A, reader: anytype, metadata_group: u64, ref: Reference) !Entry {
    // Entry is transitively POD. Release the entire immutable-plan decode
    // before visiting another address, rather than retaining page*plan memory.
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    try ref.validate();
    var key: [256]u8 = undefined;
    const primary = (try optional(reader, try tableKey(&key, metadata_group, ref.table))) orelse return error.InvalidSqlRelationPersistence;
    if (!std.meta.eql(try Record.decode(primary), ref)) return error.InvalidSqlRelationPersistence;
    const raw = (try optional(reader, try staging.jobKey(&key, metadata_group, ref.plan.plan_id))) orelse return error.InvalidSqlRelationPersistence;
    if (raw.len > staging.max_encoded_bytes) return error.InvalidSqlRelationPersistence;
    const job = try std.json.parseFromSliceLeaky(staging.Job, a, raw, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &job.plan_digest, &ref.plan.plan_digest) or !std.mem.eql(u8, &job.plan.id, &ref.plan.plan_id)) return error.InvalidSqlRelationPersistence;
    const target = for (job.plan.targets) |candidate| {
        if (candidate.table.table_id == ref.table) break candidate;
    } else return error.InvalidSqlRelationPersistence;
    const expected = (try reference(job.plan, job.plan_digest, target)) orelse return error.InvalidSqlRelationPersistence;
    if (!std.meta.eql(expected, ref)) return error.InvalidSqlRelationPersistence;
    const progress_raw = (try optional(reader, try staging.progressKey(&key, metadata_group, job.plan.id))) orelse return error.InvalidSqlRelationPersistence;
    const progress = try std.json.parseFromSliceLeaky(staging.Progress, a, progress_raw, .{});
    if (progress.state == .published or progress.state == .canceled) return error.InvalidSqlRelationPersistence;
    const initialized = try @import("sql_relation_target_images.zig").effectiveInitialized(a, reader, metadata_group, ref.plan, ref.table);
    if (initialized) |value| {
        try value.verify();
        if (value.metadata_group_id != metadata_group or value.binding.table_id != ref.table or value.creation_scope == null or !std.meta.eql(value.creation_scope.?, ref.plan) or !value.binding.descriptor.eql(target.table.sql_persistence.?)) return error.InvalidSqlRelationPersistence;
    }
    return .{ .reference = ref, .descriptor = target.table.sql_persistence.?, .initialized = initialized };
}

/// `a` belongs to the caller's bounded preparation arena. The limit caps both
/// pointer work and immutable-plan joins, independent of table/user row count.
pub fn listNode(a: A, reader: anytype, metadata_group: u64, node: u64, after: ?u64, limit: u16) !Page {
    if (limit == 0 or limit > max_page) return error.InvalidSqlRelationPersistence;
    var prefix_buffer: [192]u8 = undefined;
    const selected_prefix = try nodePrefix(&prefix_buffer, metadata_group, node);
    var seek_buffer: [256]u8 = undefined;
    const seek = if (after) |table| try nodeKey(&seek_buffer, metadata_group, node, table) else selected_prefix;
    var cursor = try reader.openCursor();
    defer cursor.close();
    var entries: std.ArrayList(Entry) = .empty;
    errdefer entries.deinit(a);
    var next = try cursor.seekAtOrAfter(seek);
    while (next) |row| : (next = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, selected_prefix)) break;
        if (row.key.len != selected_prefix.len + 16) return error.InvalidSqlRelationPersistence;
        const table = std.fmt.parseInt(u64, row.key[selected_prefix.len..], 16) catch return error.InvalidSqlRelationPersistence;
        if (after != null and table <= after.?) continue;
        if (entries.items.len == limit) {
            const continuation = entries.items[entries.items.len - 1].reference.table;
            return .{ .entries = try entries.toOwnedSlice(a), .next = continuation };
        }
        const ref = try Record.decode(row.value);
        var canonical: [256]u8 = undefined;
        if (ref.node != node or ref.table != table or !std.mem.eql(u8, row.key, try nodeKey(&canonical, metadata_group, node, table))) return error.InvalidSqlRelationPersistence;
        try entries.append(a, try resolve(a, reader, metadata_group, ref));
    }
    return .{ .entries = try entries.toOwnedSlice(a), .next = null };
}

/// Forward verification reuses the already-decoded immutable job projection
/// during cold/snapshot checks; it never decodes a full plan per index row.
pub fn requireProjected(a: A, reader: anytype, metadata_group: u64, ref: Reference, descriptor: persistence.Descriptor, namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace) !void {
    try ref.validate();
    try descriptor.validate();
    if (descriptor.owner_node_id != ref.node or namespace.table_id != ref.table) return error.InvalidSqlRelationPersistence;
    var key: [256]u8 = undefined;
    const primary = (try optional(reader, try tableKey(&key, metadata_group, ref.table))) orelse return error.InvalidSqlRelationPersistence;
    if (!std.meta.eql(try Record.decode(primary), ref)) return error.InvalidSqlRelationPersistence;
    const indexed = (try optional(reader, try nodeKey(&key, metadata_group, ref.node, ref.table))) orelse return error.InvalidSqlRelationPersistence;
    if (!std.meta.eql(try Record.decode(indexed), ref)) return error.InvalidSqlRelationPersistence;
    if (try optional(reader, try staging.nativeBindingKey(&key, metadata_group, ref.plan.plan_id, ref.table))) |raw| {
        var parsed = try std.json.parseFromSlice(attestation.SignedBinding, a, raw, .{});
        defer parsed.deinit();
        const proof = parsed.value;
        try proof.verify();
        if (proof.metadata_group_id != metadata_group or proof.creation_scope == null or !std.meta.eql(proof.creation_scope.?, ref.plan) or
            !proof.binding.descriptor.eql(descriptor) or proof.binding.table_id != namespace.table_id or
            proof.binding.shard_id != namespace.shard_id or proof.binding.range_id != namespace.range_id) return error.InvalidSqlRelationPersistence;
    }
}

/// Reverse accounting rejects orphaned or noncanonical secondary rows after
/// every expected job target has passed requireProjected in the same cut.
pub fn indexedRecord(group: u64, key: []const u8, value: []const u8) !bool {
    var prefix_buffer: [192]u8 = undefined;
    const selected_prefix = try std.fmt.bufPrint(&prefix_buffer, "\x00\x00__metadata__:system_catalog:{d}:sql-native-hidden:", .{group});
    if (!std.mem.startsWith(u8, key, selected_prefix)) return false;
    const ref = try Record.decode(value);
    try ref.validate();
    var canonical: [256]u8 = undefined;
    if (!std.mem.eql(u8, key, try nodeKey(&canonical, group, ref.node, ref.table)) and
        !std.mem.eql(u8, key, try tableKey(&canonical, group, ref.table))) return error.InvalidSqlRelationPersistence;
    return true;
}
