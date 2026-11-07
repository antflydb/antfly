// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Point-addressable native group custody after ordinary range aliases have
//! disappeared. Only the existing published retirement journal grants GC.
const std = @import("std");
const local = @import("antfly_local_sources");
const retirement = @import("table_retirement_ledger.zig");
const manifests = @import("table_retirement_manifest.zig");
const tables = @import("table_manager.zig");
const records = @import("schema_validation_publication.zig");
const A = std.mem.Allocator;
pub const Record = struct {
    lookup: retirement.Lookup,
    source_group: u64,
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    namespace: local.storage_db_doc_identity_namespace.Namespace,
    descriptor_fingerprint: [32]u8,

    pub fn validate(self: Record) !void {
        try self.lookup.validate();
        if (self.source_group == 0 or self.namespace.table_id != self.lookup.table or
            !local.metadata_incarnation.isValid(self.metadata_incarnation) or
            std.mem.allEqual(u8, &self.descriptor_fingerprint, 0)) return error.InvalidCatalogRecord;
    }
};
const Encoded = records.Record(Record, "TRNG", 2048);

pub fn prefix(buffer: []u8, group: u64) ![]const u8 {
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}native-group:", .{try retirement.prefix(&base, group)});
}
pub fn key(buffer: []u8, group: u64, source_group: u64) ![]const u8 {
    if (source_group == 0) return error.InvalidCatalogRecord;
    var base: [192]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try prefix(&base, group), source_group });
}
pub fn address(group: u64, bytes: []const u8) !?u64 {
    var buffer: [256]u8 = undefined;
    const start = try prefix(&buffer, group);
    if (!std.mem.startsWith(u8, bytes, start)) return null;
    const id = std.fmt.parseInt(u64, bytes[start.len..], 16) catch return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, bytes, try key(&buffer, group, id))) return error.InvalidCatalogRecord;
    return id;
}
fn requireManifest(head: retirement.Head, manifest: manifests.Manifest) !void {
    if (manifest.root.table_id != head.lookup.table or manifest.metadata_group_id != head.lookup.group or
        !std.meta.eql(manifest.scope_child, head.scope.parent) or
        !std.mem.eql(u8, &manifest.metadata_incarnation, &head.incarnation) or
        !std.meta.eql(try manifest.reference(), head.effects)) return error.InvalidCatalogRecord;
}
fn selected(head: retirement.Head, manifest: manifests.Manifest, range: local.common_topology_records.RangeRecord) !Record {
    const descriptor = manifest.root.sql_persistence orelse return error.InvalidCatalogRecord;
    try descriptor.validate();
    if (range.table_id != head.lookup.table) return error.InvalidCatalogRecord;
    const result: Record = .{
        .lookup = head.lookup,
        .source_group = range.group_id,
        .metadata_incarnation = head.incarnation,
        .namespace = .{ .table_id = range.table_id, .shard_id = tables.rangeDocIdentityShardId(range), .range_id = tables.rangeDocIdentityRangeId(range) },
        .descriptor_fingerprint = try descriptor.fingerprint(),
    };
    try result.validate();
    return result;
}
fn optional(reader: anytype, selected_key: []const u8) !?[]const u8 {
    return reader.get(selected_key) catch |err| return if (err == error.NotFound) null else err;
}
/// Called from transferReservations, in the transaction publishing the exact
/// table/scope postimage and terminal Head. Rollback withdraws every pointer.
pub fn stage(txn: anytype, head: retirement.Head, manifest: manifests.Manifest) !void {
    if (manifest.root.sql_persistence == null) return;
    try head.validate();
    if (head.phase != .physical_ready and head.phase != .validating_survivors) return error.GenerationPublicationChanged;
    try requireManifest(head, manifest);
    for (manifest.root_ranges) |range| {
        const value = try selected(head, manifest, range);
        const encoded = try Encoded.init(value);
        var buffer: [256]u8 = undefined;
        const selected_key = try key(&buffer, head.lookup.group, range.group_id);
        if (try optional(txn, selected_key)) |before| {
            if (!std.meta.eql(try Encoded.decode(before), value)) return error.InvalidCatalogRecord;
        } else try txn.put(selected_key, encoded.slice());
    }
}
/// A pointer is an index into the original immutable journal, not a second
/// terminal decision. Missing, active and mismatched journals grant no GC.
pub fn readPublished(a: A, reader: anytype, group: u64, source_group: u64) !?Record {
    var buffer: [256]u8 = undefined;
    const bytes = (try optional(reader, try key(&buffer, group, source_group))) orelse return null;
    const value = try Encoded.decode(bytes);
    try value.validate();
    if (value.lookup.group != group or value.source_group != source_group) return error.InvalidCatalogRecord;
    const head = (try retirement.head(reader, value.lookup)) orelse return error.InvalidCatalogRecord;
    try head.validate();
    if (head.phase != .published or (head.physical == null and head.native_loss == null) or
        !std.mem.eql(u8, &head.incarnation, &value.metadata_incarnation)) return error.InvalidCatalogRecord;
    var body = try manifests.Owned.parse(a, try reader.get(try retirement.key(&buffer, head.lookup, .effects)));
    defer body.deinit();
    try requireManifest(head, body.value);
    const range = for (body.value.root_ranges) |candidate| {
        if (candidate.group_id == source_group) break candidate;
    } else return error.InvalidCatalogRecord;
    if (!std.meta.eql(try selected(head, body.value, range), value)) return error.InvalidCatalogRecord;
    if (head.scope.parent) |parent| {
        _ = try @import("scope_retirement_children.zig").requirePublished(a, reader, parent);
    } else {
        _ = try retirement.verifyPhysical(a, reader, head, body.value);
    }
    return value;
}
/// Forward validation rejects a missing pointer; reverse directory validation
/// uses readPublished to reject orphaned, renamed and extra pointers.
pub fn requirePublished(reader: anytype, head: retirement.Head, manifest: manifests.Manifest) !void {
    if (manifest.root.sql_persistence == null) return;
    if (head.phase != .published) return error.InvalidCatalogRecord;
    try requireManifest(head, manifest);
    for (manifest.root_ranges) |range| {
        var buffer: [256]u8 = undefined;
        const bytes = (try optional(reader, try key(&buffer, head.lookup.group, range.group_id))) orelse return error.InvalidCatalogRecord;
        if (!std.meta.eql(try selected(head, manifest, range), try Encoded.decode(bytes))) return error.InvalidCatalogRecord;
    }
}

test "SQL relation lifetime retired native group pointers preserve full width namespace and reject noncanonical aliases" {
    const a = std.testing.allocator;
    const source_group: u64 = std.math.maxInt(u64) - 1;
    var buffer: [256]u8 = undefined;
    const exact = try a.dupe(u8, try key(&buffer, 7, source_group));
    defer a.free(exact);
    try std.testing.expectEqual(source_group, (try address(7, exact)).?);
    try std.testing.expectEqual(@as(?u64, null), try address(8, exact));
    const bad = try std.fmt.allocPrint(a, "{s}0", .{exact});
    defer a.free(bad);
    try std.testing.expectError(error.InvalidCatalogRecord, address(7, bad));
    const value: Record = .{ .lookup = .{ .group = 7, .table = source_group - 1, .id = @splat(3) }, .source_group = source_group, .metadata_incarnation = @splat('1'), .namespace = .{ .table_id = source_group - 1, .shard_id = source_group - 2, .range_id = source_group - 3 }, .descriptor_fingerprint = @splat(4) };
    try value.validate();
    const encoded = try Encoded.init(value);
    try std.testing.expectEqualDeep(value, try Encoded.decode(encoded.slice()));
    var changed = value;
    changed.namespace.table_id -= 1;
    try std.testing.expectError(error.InvalidCatalogRecord, changed.validate());
}

test "SQL relation lifetime retired native group pointers refuse active journals and never infer retirement from absence" {
    const value: Record = .{ .lookup = .{ .group = 7, .table = 8, .id = @splat(3) }, .source_group = 9, .metadata_incarnation = @splat('1'), .namespace = .{ .table_id = 8, .shard_id = 10, .range_id = 11 }, .descriptor_fingerprint = @splat(4) };
    const encoded = try Encoded.init(value);
    const active = try retirement.HeadRecord.init(.{ .lookup = value.lookup, .incarnation = value.metadata_incarnation, .effects = .{ .digest = @splat(5), .root_table = 8, .survivors = 0, .counters = 0, .views = 0 }, .scope = .{ .database = 1, .namespace = 2, .creator = .bootstrap }, .participant_count = 1, .range_count = 1, .participants_digest = @splat(6) });
    const Reader = struct {
        pointer: ?[]const u8,
        head: []const u8,
        pub fn get(self: @This(), selected_key: []const u8) ![]const u8 {
            if (std.mem.indexOf(u8, selected_key, ":native-group:") != null) return self.pointer orelse error.NotFound;
            if (std.mem.endsWith(u8, selected_key, ":head")) return self.head;
            return error.UnexpectedCatalogRead;
        }
    };
    try std.testing.expect((try readPublished(std.testing.allocator, Reader{ .pointer = null, .head = active.slice() }, 7, 9)) == null);
    try std.testing.expectError(error.InvalidCatalogRecord, readPublished(std.testing.allocator, Reader{ .pointer = encoded.slice(), .head = active.slice() }, 7, 9));
    try std.testing.expectError(error.InvalidCatalogRecord, readPublished(std.testing.allocator, Reader{ .pointer = encoded.slice(), .head = active.slice() }, 7, 10));
}
