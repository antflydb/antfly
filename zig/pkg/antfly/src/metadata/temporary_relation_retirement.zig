// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Irreversible lost-TEMP checkpoints in the existing ancestor retirement job.
//! The checkpoint retires an inaccessible generation, never manufactures a
//! native source receipt, catalog image, transaction vote or successor image.
const std = @import("std");
const local = @import("antfly_local_sources");
const scopes = @import("scope_retirement_ledger.zig");
const lifetimes = @import("sql_relation_lifetimes.zig");
const boots = @import("sql_relation_boots.zig");
const recovery = @import("sql_relation_recovery.zig");
const records = @import("schema_validation_publication.zig");
const A = std.mem.Allocator;

pub const Checkpoint = struct {
    child: scopes.Child,
    namespace_id: u64,
    source_group_id: u64,
    before: lifetimes.Entry,
    superseding_boot: boots.Record,
    sequence: u16 = 0,
    previous_digest: [32]u8 = @splat(0),

    pub fn validate(self: Checkpoint) !void {
        try self.child.validate();
        try self.before.validate();
        try self.superseding_boot.validate();
        const descriptor = self.before.descriptor;
        if (self.namespace_id == 0 or self.source_group_id == 0 or self.before.table_id != self.child.table or
            descriptor.kind != .temporary or descriptor.owner == null or
            self.superseding_boot.node_id != descriptor.owner_node_id or
            std.mem.eql(u8, &self.superseding_boot.instance, &descriptor.generation_instance) or
            (self.child.lookup.reference.kind == .namespace and self.child.lookup.reference.root != self.namespace_id))
            return error.InvalidSqlRelationPersistence;
        if (self.before.initialized) |proof| {
            if (proof.kind != .initialized or proof.metadata_group_id != self.child.lookup.group or
                self.superseding_boot.generation <= proof.boot_generation or
                !std.mem.eql(u8, &proof.binding.native_instance, &descriptor.generation_instance) or
                !std.mem.eql(u8, &proof.identity.metadata_incarnation, &self.superseding_boot.identity.metadata_incarnation))
                return error.InvalidSqlRelationPersistence;
        }
    }
    pub fn digest(self: Checkpoint) ![32]u8 {
        try self.validate();
        if (self.sequence == 0) return error.InvalidScopeRetirement;
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try writer.writer.writeAll("antfly/scoped-temporary-source-loss/v1\x00");
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: [32]u8 = undefined;
        writer.hasher.final(&result);
        return result;
    }
};
pub const CheckpointRecord = records.Record(Checkpoint, "STLF", 8192);
pub const SubjectRecord = records.Record(scopes.Child, "STLI", 2048);

pub fn prefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidScopeRetirement;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:scope-native-loss:", .{group});
}
pub fn checkpointKey(buffer: []u8, child: scopes.Child) ![]const u8 {
    try child.validate();
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}checkpoint:{s}:{x:0>4}", .{ try prefix(&base, child.lookup.group), std.fmt.bytesToHex(child.lookup.reference.id, .lower), child.ordinal });
}
pub fn sequenceKey(buffer: []u8, lookup: scopes.Lookup, sequence: u16) ![]const u8 {
    try lookup.validate();
    if (sequence == 0 or sequence > lookup.reference.tables) return error.InvalidScopeRetirement;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}sequence:{s}:{x:0>4}", .{ try prefix(&base, lookup.group), std.fmt.bytesToHex(lookup.reference.id, .lower), sequence });
}
pub fn subjectKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    if (table == 0) return error.InvalidScopeRetirement;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}subject:{x:0>16}", .{ try prefix(&base, group), table });
}
fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}
pub fn read(reader: anytype, child: scopes.Child) !?Checkpoint {
    var buffer: [320]u8 = undefined;
    const raw = (try optional(reader, try checkpointKey(&buffer, child))) orelse return null;
    const checkpoint = try CheckpointRecord.decode(raw);
    try checkpoint.validate();
    if (checkpoint.sequence == 0 or !std.meta.eql(checkpoint.child, child)) return error.InvalidScopeRetirement;
    return checkpoint;
}

fn requireHead(reader: anytype, checkpoint: Checkpoint) !scopes.Head {
    try checkpoint.validate();
    const child = checkpoint.child;
    const head = (try scopes.head(reader, child.lookup)) orelse return error.InvalidScopeRetirement;
    if (head.progress.phase == .canceled or head.native_losses == null or head.native_losses.?.count < checkpoint.sequence or
        !std.meta.eql((try scopes.child(reader, child.lookup, child.ordinal)) orelse return error.InvalidScopeRetirement, child) or
        !std.mem.eql(u8, &std.fmt.bytesToHex(head.metadata_incarnation, .lower), &checkpoint.superseding_boot.identity.metadata_incarnation))
        return error.InvalidScopeRetirement;
    if (head.temporary_owner) |owner| if (!owner.eql(checkpoint.before.descriptor.owner.?)) return error.InvalidScopeRetirement;
    if (head.active()) {
        const held = (try scopes.member(reader, child.lookup.group, .table, child.table)) orelse return error.InvalidScopeRetirement;
        if (held.intent != .retire or !std.meta.eql(held.lookup, child.lookup) or
            !std.mem.eql(u8, &held.metadata_incarnation, &head.metadata_incarnation)) return error.InvalidScopeRetirement;
    }
    return head;
}

/// The caller has already rejoined the registered superseding Boot, current
/// Entry, complete scope membership and table/range cut in this same writer.
/// Checkpoints and their discovery pointers share the existing job transaction.
pub fn stage(txn: anytype, before: scopes.Head, checkpoint: Checkpoint) !scopes.Head {
    try checkpoint.validate();
    if (!before.active() or !std.meta.eql(before.lookup, checkpoint.child.lookup) or
        !std.meta.eql((try scopes.head(txn, before.lookup)) orelse return error.ScopeRetirementChanged, before) or
        !std.meta.eql((try scopes.child(txn, before.lookup, checkpoint.child.ordinal)) orelse return error.InvalidScopeRetirement, checkpoint.child))
        return error.ScopeRetirementChanged;
    if (try read(txn, checkpoint.child)) |stored| {
        var candidate = checkpoint;
        candidate.sequence = stored.sequence;
        candidate.previous_digest = stored.previous_digest;
        if (!std.meta.eql(stored, candidate)) return error.ScopeRetirementChanged;
        _ = try requireHead(txn, stored);
        return before;
    }
    var buffer: [320]u8 = undefined;
    const subject = try subjectKey(&buffer, before.lookup.group, checkpoint.child.table);
    if (try optional(txn, subject) != null) return error.InvalidScopeRetirement;
    var totals: scopes.NativeLosses = before.native_losses orelse .{ .count = 0, .digest = @as([32]u8, @splat(0)) };
    var captured = checkpoint;
    captured.previous_digest = totals.digest;
    totals.count = std.math.add(u16, totals.count, 1) catch return error.InvalidScopeRetirement;
    captured.sequence = totals.count;
    const digest = try captured.digest();
    for (&totals.digest, digest) |*byte, value| byte.* ^= value;
    var after = before;
    after.native_losses = totals;
    try after.validate();
    const encoded = try CheckpointRecord.init(captured);
    const pointer = try SubjectRecord.init(checkpoint.child);
    try txn.put(subject, pointer.slice());
    try txn.put(try checkpointKey(&buffer, checkpoint.child), encoded.slice());
    const sequence_key = try sequenceKey(&buffer, before.lookup, captured.sequence);
    if (try optional(txn, sequence_key) != null) return error.InvalidScopeRetirement;
    try txn.put(sequence_key, pointer.slice());
    if (checkpoint.before.initialized) |proof| {
        const pin = try local.common_topology_records.RelationPersistence.PhysicalPin.fromBinding(proof.binding);
        const index: recovery.Pointer = .{ .scope = .{ .lookup = checkpoint.child.lookup, .child_ordinal = checkpoint.child.ordinal } };
        const index_record = try recovery.PointerRecord.init(index);
        const key = try recovery.indexKey(&buffer, before.lookup.group, pin);
        if (try optional(txn, key)) |raw| {
            if (!std.meta.eql(try recovery.PointerRecord.decode(raw), index)) return error.InvalidScopeRetirement;
        }
        try txn.put(key, index_record.slice());
    }
    const head_record = try scopes.HeadRecord.init(after);
    try txn.put(try scopes.key(&buffer, before.lookup, .head), head_record.slice());
    return after;
}

/// An uninitialized image has no acknowledged pin and grants no participant
/// retirement proof. Its metadata definition can still complete scope cleanup.
pub fn readDerived(a: A, reader: anytype, child: scopes.Child) !?recovery.TemporaryRetirement {
    _ = a;
    const checkpoint = (try read(reader, child)) orelse return null;
    _ = try requireHead(reader, checkpoint);
    const before = checkpoint.before.initialized orelse return null;
    const pin = try local.common_topology_records.RelationPersistence.PhysicalPin.fromBinding(before.binding);
    var buffer: [320]u8 = undefined;
    const raw = (try optional(reader, try recovery.indexKey(&buffer, child.lookup.group, pin))) orelse return error.InvalidScopeRetirement;
    const pointer = try recovery.PointerRecord.decode(raw);
    if (pointer != .scope or !std.meta.eql(pointer.scope.lookup, child.lookup) or pointer.scope.child_ordinal != child.ordinal) return error.InvalidScopeRetirement;
    const result: recovery.TemporaryRetirement = .{
        .before = before,
        .source_group_id = checkpoint.source_group_id,
        .namespace_id = checkpoint.namespace_id,
        .scope = child.lookup,
        .child_ordinal = child.ordinal,
        .child_id = child.id,
        .checkpoint = .{ .lost_source_fenced = .{ .sequence = checkpoint.sequence, .digest = try checkpoint.digest() } },
    };
    try result.validate();
    return result;
}

/// Boot recovery may advance past a retained TEMP directory row once its exact
/// old generation is irreversibly owned by scope cleanup. No row access follows.
pub fn requireEntryRetired(a: A, reader: anytype, group: u64, entry: lifetimes.Entry, current_boot: boots.Record) !bool {
    _ = a;
    try current_boot.validate();
    var buffer: [320]u8 = undefined;
    const raw = (try optional(reader, try subjectKey(&buffer, group, entry.table_id))) orelse return false;
    const child = try SubjectRecord.decode(raw);
    const checkpoint = (try read(reader, child)) orelse return error.InvalidScopeRetirement;
    _ = try requireHead(reader, checkpoint);
    if (child.lookup.group != group or !std.meta.eql(checkpoint.before, entry) or
        current_boot.node_id != checkpoint.superseding_boot.node_id or current_boot.generation < checkpoint.superseding_boot.generation or
        !std.mem.eql(u8, &current_boot.identity.metadata_incarnation, &checkpoint.superseding_boot.identity.metadata_incarnation) or
        std.mem.eql(u8, &current_boot.instance, &entry.descriptor.generation_instance)) return error.SqlRelationOwnerChanged;
    return true;
}

/// Removing a live directory row removes only its boot-discovery pointer. The
/// immutable checkpoint/pinned participant proof remains in the original job.
pub fn removeSubject(txn: anytype, group: u64, table: u64) !void {
    var buffer: [320]u8 = undefined;
    const key = try subjectKey(&buffer, group, table);
    if (try optional(txn, key) != null) try txn.delete(key);
}

/// Lost-TEMP children have no surviving physical parent namespace to mutate.
/// Every declared FK parent must be another child of the same admitted scope;
/// ordinary or external parents continue through genuine native retirement.
pub fn requireManifest(reader: anytype, checkpoint: Checkpoint, manifest: @import("table_retirement_manifest.zig").Manifest) !void {
    _ = try requireHead(reader, checkpoint);
    const child = checkpoint.child;
    if (!std.meta.eql(manifest.scope_child, @as(?scopes.Child, child)) or
        manifest.metadata_group_id != child.lookup.group or manifest.root.table_id != child.table or
        manifest.root.sql_persistence == null or !manifest.root.sql_persistence.?.eql(checkpoint.before.descriptor) or
        !std.mem.eql(u8, &manifest.metadata_incarnation, &checkpoint.superseding_boot.identity.metadata_incarnation) or
        manifest.root_ranges.len != 1 or manifest.root_ranges[0].group_id != checkpoint.source_group_id or
        manifest.root_ranges[0].table_id != child.table or manifest.survivors.len != 0 or manifest.view_backings.len != 0)
        return error.InvalidScopeRetirement;
    for (manifest.outgoing_parents) |parent| {
        const registered = for (manifest.retiring_parents) |item| {
            if (item.table == parent) break item;
        } else return error.InvalidScopeRetirement;
        if (!std.meta.eql(registered.lookup, child.lookup) or
            !std.meta.eql((try scopes.child(reader, child.lookup, registered.ordinal)) orelse return error.InvalidScopeRetirement, registered))
            return error.InvalidScopeRetirement;
        const descriptor = if (parent == child.table) checkpoint.before.descriptor else blk: {
            for (manifest.external_parents) |item| if (item.table.table_id == parent)
                break :blk item.table.sql_persistence orelse return error.InvalidScopeRetirement;
            return error.InvalidScopeRetirement;
        };
        try local.common_topology_records.RelationPersistence.requireForeignKeyClasses(checkpoint.before.descriptor, descriptor);
    }
}

fn sequenceCheckpoint(reader: anytype, lookup: scopes.Lookup, sequence: u16) !Checkpoint {
    var buffer: [320]u8 = undefined;
    const child = try SubjectRecord.decode((try optional(reader, try sequenceKey(&buffer, lookup, sequence))) orelse return error.InvalidScopeRetirement);
    if (!std.meta.eql(child.lookup, lookup)) return error.InvalidScopeRetirement;
    const checkpoint = (try read(reader, child)) orelse return error.InvalidScopeRetirement;
    if (checkpoint.sequence != sequence) return error.InvalidScopeRetirement;
    return checkpoint;
}

/// Historical scope outcomes may precede further loss checkpoints. Their
/// compact count/digest rejoins one immutable sequence point in constant time.
pub fn verifySummary(reader: anytype, lookup: scopes.Lookup, summary: ?scopes.NativeLosses) !void {
    const expected = summary orelse return;
    const checkpoint = try sequenceCheckpoint(reader, lookup, expected.count);
    var digest = checkpoint.previous_digest;
    for (&digest, try checkpoint.digest()) |*byte, value| byte.* ^= value;
    if (!std.mem.eql(u8, &digest, &expected.digest)) return error.InvalidScopeRetirement;
}

/// Cold verification streams the owned checkpoint chain once per scope.
/// It does not decode table schemas, plans or every participant manifest.
pub fn verifyHead(a: A, reader: anytype, head: scopes.Head) !void {
    const summary = head.native_losses orelse return;
    var digest: [32]u8 = @splat(0);
    for (1..@as(usize, summary.count) + 1) |ordinal| {
        const checkpoint = try sequenceCheckpoint(reader, head.lookup, @intCast(ordinal));
        _ = try requireHead(reader, checkpoint);
        if (!std.mem.eql(u8, &checkpoint.previous_digest, &digest)) return error.InvalidScopeRetirement;
        for (&digest, try checkpoint.digest()) |*byte, value| byte.* ^= value;
        if (checkpoint.before.initialized != null) _ = try readDerived(a, reader, checkpoint.child);
    }
    if (!std.mem.eql(u8, &digest, &summary.digest)) return error.InvalidScopeRetirement;
}

/// Reverse validation rejects orphan checkpoints/sequence/subject indexes.
/// The caller also verifies each scope's complete chain through verifyHead.
pub fn verifyDirectory(a: A, reader: anytype, group: u64, rows: anytype, canceled: ?*const std.atomic.Value(bool)) !void {
    var prefix_buffer: [128]u8 = undefined;
    const stem = try prefix(&prefix_buffer, group);
    var key_buffer: [320]u8 = undefined;
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, stem)) continue;
        if (row.key.len > 320) return error.InvalidScopeRetirement;
        var saved: [320]u8 = undefined;
        @memcpy(saved[0..row.key.len], row.key);
        const key = saved[0..row.key.len];
        const suffix = key[stem.len..];
        if (std.mem.startsWith(u8, suffix, "checkpoint:")) {
            const checkpoint = try CheckpointRecord.decode(row.value);
            try checkpoint.validate();
            if (checkpoint.sequence == 0 or checkpoint.child.lookup.group != group or
                !std.mem.eql(u8, key, try checkpointKey(&key_buffer, checkpoint.child))) return error.InvalidScopeRetirement;
            const head = try requireHead(reader, checkpoint);
            const sequence = try sequenceCheckpoint(reader, checkpoint.child.lookup, checkpoint.sequence);
            if (!std.meta.eql(sequence, checkpoint)) return error.InvalidScopeRetirement;
            if (head.active()) {
                const raw = (try optional(reader, try subjectKey(&key_buffer, group, checkpoint.child.table))) orelse return error.InvalidScopeRetirement;
                if (!std.meta.eql(try SubjectRecord.decode(raw), checkpoint.child)) return error.InvalidScopeRetirement;
            }
        } else if (std.mem.startsWith(u8, suffix, "sequence:")) {
            const child = try SubjectRecord.decode(row.value);
            const checkpoint = (try read(reader, child)) orelse return error.InvalidScopeRetirement;
            _ = try requireHead(reader, checkpoint);
            if (child.lookup.group != group or !std.mem.eql(u8, key, try sequenceKey(&key_buffer, child.lookup, checkpoint.sequence)))
                return error.InvalidScopeRetirement;
        } else if (std.mem.startsWith(u8, suffix, "subject:")) {
            const child = try SubjectRecord.decode(row.value);
            const checkpoint = (try read(reader, child)) orelse return error.InvalidScopeRetirement;
            const head = try requireHead(reader, checkpoint);
            const entry = (try lifetimes.read(a, reader, group, child.table)) orelse return error.InvalidScopeRetirement;
            if (!head.active() or child.lookup.group != group or !std.meta.eql(entry, checkpoint.before) or
                !std.mem.eql(u8, key, try subjectKey(&key_buffer, group, child.table))) return error.InvalidScopeRetirement;
        } else return error.InvalidScopeRetirement;
    }
}
