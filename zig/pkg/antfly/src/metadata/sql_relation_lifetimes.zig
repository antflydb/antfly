// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Indexed lifetime ownership beside authoritative table catalog rows. All
//! mutations run in that table's existing native metadata transaction. These
//! records contain ownership/digests, never user row payloads or a second SQL
//! catalog. Session close and node recovery enumerate bounded owner cuts.
const std = @import("std");
const local = @import("antfly_local_sources");
const persistence = local.common_topology_records.RelationPersistence;
const Table = local.common_topology_records.TableRecord;
const A = std.mem.Allocator;
pub const max_page = 128;
pub const max_record_bytes = 4096;

pub const Entry = struct {
    version: u8 = 1,
    table_id: u64,
    descriptor: persistence.Descriptor,
    initialized: ?@import("sql_relation_attestation.zig").SignedBinding = null,
    pub fn jsonStringify(self: Entry, writer: anytype) !void {
        if (self.initialized != null) return writer.write(.{ .version = self.version, .table_id = self.table_id, .descriptor = self.descriptor, .initialized = self.initialized });
        return writer.write(.{ .version = self.version, .table_id = self.table_id, .descriptor = self.descriptor });
    }
    pub fn validate(self: Entry) !void {
        if (self.version != 1 or self.table_id == 0) return error.InvalidSqlRelationPersistence;
        try self.descriptor.validate();
        if (self.initialized) |proof| {
            try proof.verify();
            if (proof.binding.table_id != self.table_id or !proof.binding.descriptor.eql(self.descriptor)) return error.InvalidSqlRelationPersistence;
        }
    }
};

pub fn prefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidSqlRelationPersistence;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sql_relation_lifetimes:", .{group});
}

pub fn tableKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    if (table == 0) return error.InvalidSqlRelationPersistence;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}table:{x:0>16}", .{ try prefix(&base, group), table });
}

pub fn nodePrefix(buffer: []u8, group: u64, node: u64) ![]const u8 {
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}node:{x:0>16}:", .{ try prefix(&base, group), node });
}

pub fn connectionPrefix(buffer: []u8, group: u64, owner: persistence.Owner) ![]const u8 {
    var base: [128]u8 = undefined;
    var name: [72]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}connection:{s}:", .{ try prefix(&base, group), try owner.namespace(&name) });
}

pub fn issuerPrefix(buffer: []u8, group: u64, node: u64) ![]const u8 {
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}issuer:{x:0>16}:", .{ try prefix(&base, group), node });
}

fn indexKey(buffer: []u8, parent: []const u8, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ parent, table });
}

fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}

pub fn read(a: A, reader: anytype, group: u64, table: u64) !?Entry {
    var key: [256]u8 = undefined;
    const raw = (try optional(reader, try tableKey(&key, group, table))) orelse return null;
    if (raw.len > max_record_bytes) return error.InvalidSqlRelationPersistence;
    var parsed = try std.json.parseFromSlice(Entry, a, raw, .{});
    defer parsed.deinit();
    const entry = parsed.value;
    try entry.validate();
    if (entry.table_id != table or (entry.initialized != null and entry.initialized.?.metadata_group_id != group)) return error.InvalidSqlRelationPersistence;
    return entry;
}

/// Publish only a root-authenticated native postimage after the caller has
/// rejoined its current Boot and actual table/range in the same transaction.
/// A different physical root never replaces an acknowledged initialized root.
pub fn stageInitialized(a: A, txn: anytype, group: u64, proof: @import("sql_relation_attestation.zig").SignedBinding) !void {
    try proof.verify();
    if (proof.metadata_group_id != group) return error.InvalidSqlRelationPersistence;
    var entry = (try read(a, txn, group, proof.binding.table_id)) orelse return error.SqlRelationOwnerChanged;
    if (!entry.descriptor.eql(proof.binding.descriptor)) return error.SqlRelationOwnerChanged;
    if (entry.initialized) |before| {
        if (!std.mem.eql(u8, &try before.digest(), &try proof.digest())) return error.SqlRelationOwnerChanged;
        return;
    }
    entry.initialized = proof;
    try entry.validate();
    var encoded: [max_record_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&encoded);
    try std.json.Stringify.value(entry, .{}, &writer);
    var buffer: [256]u8 = undefined;
    try txn.put(try tableKey(&buffer, group, entry.table_id), writer.buffered());
}

/// Clean process adoption replaces only the signed process observation. The
/// table definition, generation birth, root address and catalog stay intact.
pub fn requireAdoption(before: @import("sql_relation_attestation.zig").SignedBinding, after: @import("sql_relation_attestation.zig").SignedBinding) !void {
    try before.verify();
    try after.verify();
    if (after.kind != .adopted or !before.binding.descriptor.eql(after.binding.descriptor) or
        after.metadata_group_id != before.metadata_group_id or !std.meta.eql(before.creation_scope, after.creation_scope) or
        !std.mem.eql(u8, &try before.identity.digest(), &try after.identity.digest()) or
        after.boot_generation <= before.boot_generation or
        std.mem.eql(u8, &before.binding.native_instance, &after.binding.native_instance) or
        !std.mem.eql(u8, &after.previous_descriptor_fingerprint.?, &try before.binding.descriptor.fingerprint()) or
        !std.mem.eql(u8, &after.previous_binding_digest.?, &try before.digest()) or
        !(try persistence.PhysicalPin.fromBinding(before.binding)).eql(try persistence.PhysicalPin.fromBinding(after.binding)))
        return error.SqlRelationOwnerChanged;
}

pub fn stageAdopted(a: A, txn: anytype, group: u64, proof: @import("sql_relation_attestation.zig").SignedBinding) !void {
    var entry = (try read(a, txn, group, proof.binding.table_id)) orelse return error.SqlRelationOwnerChanged;
    const before = entry.initialized orelse return error.SqlRelationRecoveryIncomplete;
    if (std.mem.eql(u8, &try before.digest(), &try proof.digest())) return;
    try requireAdoption(before, proof);
    entry.initialized = proof;
    try entry.validate();
    var bytes: [max_record_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&bytes);
    try std.json.Stringify.value(entry, .{}, &writer);
    var buffer: [256]u8 = undefined;
    try txn.put(try tableKey(&buffer, group, entry.table_id), writer.buffered());
}

fn requireIndex(reader: anytype, key: []const u8, fingerprint: [32]u8) !void {
    const value = (try optional(reader, key)) orelse return error.InvalidSqlRelationPersistence;
    if (!std.mem.eql(u8, value, &fingerprint)) return error.InvalidSqlRelationPersistence;
}

/// Called by the same table writer for create/replace/drop. Caller owns the
/// ordinary catalog authorization and generation-CAS; changed process/data
/// epochs additionally require its authenticated recovery-lease proof.
pub fn stageTable(a: A, txn: anytype, group: u64, before: ?Table, after: ?Table) !void {
    const previous = if (before) |value| value.sql_persistence else null;
    const next = if (after) |value| value.sql_persistence else null;
    if (previous == null and next == null) return;
    const table = if (after) |value| value.table_id else before.?.table_id;
    if (table == 0 or (before != null and before.?.table_id != table)) return error.InvalidSqlRelationPersistence;
    if (previous) |value| {
        try value.validate();
        const entry = (try read(a, txn, group, table)) orelse return error.InvalidSqlRelationPersistence;
        if (!entry.descriptor.eql(value)) return error.InvalidSqlRelationPersistence;
        const fingerprint = try value.fingerprint();
        var buffer: [256]u8 = undefined;
        var parent: [256]u8 = undefined;
        try requireIndex(txn, try indexKey(&buffer, try nodePrefix(&parent, group, value.owner_node_id), table), fingerprint);
        if (value.owner) |owner| {
            try requireIndex(txn, try indexKey(&buffer, try connectionPrefix(&parent, group, owner), table), fingerprint);
            try requireIndex(txn, try indexKey(&buffer, try issuerPrefix(&parent, group, owner.issuer_node_id), table), fingerprint);
        }
    } else if (try read(a, txn, group, table) != null) return error.InvalidSqlRelationPersistence;
    if (next) |value| {
        try value.validate();
        if (previous) |old| {
            if (old.eql(value)) return;
            if (old.kind != value.kind or !std.meta.eql(old.owner, value.owner) or
                value.data_generation < old.data_generation or value.data_generation - old.data_generation > 1)
                return error.InvalidSqlRelationPersistence;
        }
    }
    var buffer: [256]u8 = undefined;
    var parent: [256]u8 = undefined;
    if (previous) |value| {
        try txn.delete(try indexKey(&buffer, try nodePrefix(&parent, group, value.owner_node_id), table));
        if (value.owner) |owner| {
            try txn.delete(try indexKey(&buffer, try connectionPrefix(&parent, group, owner), table));
            try txn.delete(try indexKey(&buffer, try issuerPrefix(&parent, group, owner.issuer_node_id), table));
        }
    }
    if (next) |value| {
        try @import("sql_relation_boots.zig").rewindForInsertion(txn, group, value.owner_node_id, table);
        const entry: Entry = .{ .table_id = table, .descriptor = value };
        var encoded: [max_record_bytes]u8 = undefined;
        var writer = std.Io.Writer.fixed(&encoded);
        try std.json.Stringify.value(entry, .{}, &writer);
        try txn.put(try tableKey(&buffer, group, table), writer.buffered());
        const fingerprint = try value.fingerprint();
        try txn.put(try indexKey(&buffer, try nodePrefix(&parent, group, value.owner_node_id), table), &fingerprint);
        if (value.owner) |owner| {
            try txn.put(try indexKey(&buffer, try connectionPrefix(&parent, group, owner), table), &fingerprint);
            try txn.put(try indexKey(&buffer, try issuerPrefix(&parent, group, owner.issuer_node_id), table), &fingerprint);
        }
    } else {
        try txn.delete(try tableKey(&buffer, group, table));
        if (previous != null and previous.?.kind == .temporary)
            try @import("temporary_relation_retirement.zig").removeSubject(txn, group, table);
    }
}

pub const Selection = union(enum) { node: u64, issuer: u64, connection: persistence.Owner };
pub const Page = struct { entries: []const Entry, next: ?u64 };
pub const PageRequest = struct {
    selection: Selection,
    after: ?u64 = null,
    limit: u16 = 128,
    include_hidden: bool = false,
    pub fn jsonStringify(self: PageRequest, writer: anytype) !void {
        if (self.include_hidden) return writer.write(.{ .selection = self.selection, .after = self.after, .limit = self.limit, .include_hidden = true });
        return writer.write(.{ .selection = self.selection, .after = self.after, .limit = self.limit });
    }
};

fn requireEntry(a: A, reader: anytype, group: u64, table: Table) !Entry {
    const descriptor = table.sql_persistence orelse return error.InvalidSqlRelationPersistence;
    const entry = (try read(a, reader, group, table.table_id)) orelse return error.InvalidSqlRelationPersistence;
    if (!entry.descriptor.eql(descriptor)) return error.InvalidSqlRelationPersistence;
    const fingerprint = try descriptor.fingerprint();
    var buffer: [256]u8 = undefined;
    var parent: [256]u8 = undefined;
    try requireIndex(reader, try indexKey(&buffer, try nodePrefix(&parent, group, descriptor.owner_node_id), table.table_id), fingerprint);
    if (descriptor.owner) |owner| {
        try requireIndex(reader, try indexKey(&buffer, try connectionPrefix(&parent, group, owner), table.table_id), fingerprint);
        try requireIndex(reader, try indexKey(&buffer, try issuerPrefix(&parent, group, owner.issuer_node_id), table.table_id), fingerprint);
    }
    return entry;
}

fn requireDirectoryKey(group: u64, key: []const u8, value: []const u8, base_len: usize, entry: Entry) !void {
    var buffer: [256]u8 = undefined;
    var parent: [256]u8 = undefined;
    const suffix = key[base_len..];
    if (std.mem.startsWith(u8, suffix, "table:")) {
        if (!std.mem.eql(u8, key, try tableKey(&buffer, group, entry.table_id))) return error.InvalidSqlRelationPersistence;
        return;
    }
    if (!std.mem.eql(u8, value, &try entry.descriptor.fingerprint())) return error.InvalidSqlRelationPersistence;
    const expected = if (std.mem.startsWith(u8, suffix, "node:"))
        try indexKey(&buffer, try nodePrefix(&parent, group, entry.descriptor.owner_node_id), entry.table_id)
    else if (std.mem.startsWith(u8, suffix, "connection:") and entry.descriptor.owner != null)
        try indexKey(&buffer, try connectionPrefix(&parent, group, entry.descriptor.owner.?), entry.table_id)
    else if (std.mem.startsWith(u8, suffix, "issuer:") and entry.descriptor.owner != null)
        try indexKey(&buffer, try issuerPrefix(&parent, group, entry.descriptor.owner.?.issuer_node_id), entry.table_id)
    else
        return error.InvalidSqlRelationPersistence;
    if (!std.mem.eql(u8, key, expected)) return error.InvalidSqlRelationPersistence;
}

/// Bounded streaming control work. Resume by table ID, not an allocation-sized
/// catalog snapshot; every index row rejoins its exact compact owner record.
pub fn list(a: A, reader: anytype, group: u64, selection: Selection, after: ?u64, limit: usize) !Page {
    if (limit == 0 or limit > max_page) return error.InvalidSqlRelationPersistence;
    var buffer: [256]u8 = undefined;
    const selected = switch (selection) {
        .node => |node| try nodePrefix(&buffer, group, node),
        .issuer => |node| try issuerPrefix(&buffer, group, node),
        .connection => |owner| try connectionPrefix(&buffer, group, owner),
    };
    var seek_buffer: [256]u8 = undefined;
    const seek = if (after) |table| try indexKey(&seek_buffer, selected, table) else selected;
    var cursor = try reader.openCursor();
    defer cursor.close();
    var values: std.ArrayList(Entry) = .empty;
    errdefer values.deinit(a);
    var row = try cursor.seekAtOrAfter(seek);
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, selected)) break;
        if (item.key.len != selected.len + 16 or item.value.len != 32) return error.InvalidSqlRelationPersistence;
        const table = std.fmt.parseInt(u64, item.key[selected.len..], 16) catch return error.InvalidSqlRelationPersistence;
        if (after != null and table <= after.?) continue;
        if (values.items.len == limit) {
            const next = values.items[values.items.len - 1].table_id;
            return .{ .entries = try values.toOwnedSlice(a), .next = next };
        }
        const expected = item.value[0..32].*;
        const entry = (try read(a, reader, group, table)) orelse return error.InvalidSqlRelationPersistence;
        switch (selection) {
            .node => |node| if (entry.descriptor.owner_node_id != node) return error.InvalidSqlRelationPersistence,
            .issuer => |node| if (entry.descriptor.owner == null or entry.descriptor.owner.?.issuer_node_id != node) return error.InvalidSqlRelationPersistence,
            .connection => |owner| if (entry.descriptor.owner == null or !entry.descriptor.owner.?.eql(owner)) return error.InvalidSqlRelationPersistence,
        }
        if (!std.mem.eql(u8, &expected, &try entry.descriptor.fingerprint())) return error.InvalidSqlRelationPersistence;
        try values.append(a, entry);
    }
    return .{ .entries = try values.toOwnedSlice(a), .next = null };
}

/// One streaming proof qualifies cold startup, native snapshot install and
/// standby metadata export. Missing companions are never silently rebuilt.
pub fn verifyDirectory(backing: A, reader: anytype, group: u64, rows: anytype, comptime table_key: anytype, comptime table_prefix_fn: anytype, comptime decode_table: anytype, cancelled: ?*const std.atomic.Value(bool)) !bool {
    var present = false;
    var base: [128]u8 = undefined;
    const selected = try prefix(&base, group);
    var table_buffer: [256]u8 = undefined;
    const tables = try table_prefix_fn(&table_buffer, group);
    try rows.reset();
    while (try rows.next()) |row| {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, selected) and !std.mem.startsWith(u8, row.key, tables)) continue;
        var region = std.heap.ArenaAllocator.init(backing);
        defer region.deinit();
        const a = region.allocator();
        if (std.mem.startsWith(u8, row.key, tables)) {
            const table = try decode_table(a, row.value);
            if (table.sql_persistence != null) {
                present = true;
                _ = try requireEntry(a, reader, group, table);
            }
            continue;
        }
        if (row.key.len < selected.len + 16) return error.InvalidSqlRelationPersistence;
        if (row.key.len > 256 or row.value.len > max_record_bytes) return error.InvalidSqlRelationPersistence;
        var saved_key: [256]u8 = undefined;
        var saved_value: [max_record_bytes]u8 = undefined;
        @memcpy(saved_key[0..row.key.len], row.key);
        @memcpy(saved_value[0..row.value.len], row.value);
        const key_bytes = saved_key[0..row.key.len];
        const value_bytes = saved_value[0..row.value.len];
        const id = std.fmt.parseInt(u64, row.key[row.key.len - 16 ..], 16) catch return error.InvalidSqlRelationPersistence;
        var key: [256]u8 = undefined;
        const table = try decode_table(a, try reader.get(try table_key(&key, group, id)));
        if (table.table_id != id) return error.InvalidSqlRelationPersistence;
        const entry = try requireEntry(a, reader, group, table);
        try requireDirectoryKey(group, key_bytes, value_bytes, selected.len, entry);
    }
    return present;
}

test "SQL relation lifetime clean adoption binds exact prior receipt and immutable image" {
    const attestation = @import("sql_relation_attestation.zig");
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
    var request = before;
    request.kind = .adopted;
    request.boot_generation += 1;
    request.reporter_incarnation += 1;
    request.binding.native_instance = @splat(2);
    request.binding.has_transaction_state = true;
    request.previous_binding_digest = try before.digest();
    request.previous_descriptor_fingerprint = try before.binding.descriptor.fingerprint();
    const adopted = try attestation.SignedBinding.sign(request, root);
    try requireAdoption(before, adopted);
    try std.testing.expect(adopted.binding.descriptor.eql(before.binding.descriptor));
    var wrong = request;
    wrong.binding.physical_root_incarnation -= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, requireAdoption(before, try attestation.SignedBinding.sign(wrong, root)));
    wrong = request;
    wrong.previous_binding_digest.?[0] ^= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, requireAdoption(before, try attestation.SignedBinding.sign(wrong, root)));
    wrong = request;
    wrong.binding.native_instance = before.binding.native_instance;
    try std.testing.expectError(error.SqlRelationOwnerChanged, requireAdoption(before, try attestation.SignedBinding.sign(wrong, root)));
    wrong = request;
    wrong.binding.descriptor.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, requireAdoption(before, try attestation.SignedBinding.sign(wrong, root)));
}
