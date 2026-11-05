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

//! Physical children borrow their ancestor's immutable custody. They own only
//! their source aliases and native owner receipts; logical publication remains
//! one root decision, including all surviving dependency postimages.
const std = @import("std");
const local = @import("antfly_local_sources");
const scopes = @import("scope_retirement_ledger.zig");
const tables = @import("table_retirement_ledger.zig");
const manifests = @import("table_retirement_manifest.zig");

fn optional(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| return if (err == error.NotFound) null else err;
}
pub fn requireParent(reader: anytype, child: scopes.Child) !scopes.Head {
    try child.validate();
    const parent = (try scopes.head(reader, child.lookup)) orelse return error.GenerationPublicationChanged;
    if ((parent.progress.phase != .retiring_tables and parent.progress.phase != .publishing_catalog) or
        child.ordinal > parent.progress.completed_tables or
        !std.meta.eql((try scopes.child(reader, child.lookup, child.ordinal)) orelse return error.InvalidScopeRetirement, child)) return error.GenerationPublicationChanged;
    const lease = (try scopes.member(reader, child.lookup.group, .table, child.table)) orelse return error.InvalidScopeRetirement;
    if (!std.meta.eql(lease.lookup, child.lookup) or lease.intent != .retire or
        !std.mem.eql(u8, &lease.metadata_incarnation, &parent.metadata_incarnation)) return error.InvalidScopeRetirement;
    return parent;
}
fn fingerprint(value: anytype, label: []const u8) ![32]u8 {
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
    try writer.writer.writeAll(label);
    try std.json.Stringify.value(value, .{}, &writer.writer);
    var digest: [32]u8 = undefined;
    writer.hasher.final(&digest);
    return digest;
}
pub fn requireCut(reader: anytype, value: tables.Head, manifest: manifests.Manifest, participants: []const u64) !void {
    const child = value.scope.parent orelse return error.InvalidScopeRetirement;
    const parent = try requireParent(reader, child);
    if (!std.meta.eql(manifest.scope_child, value.scope.parent) or !value.scope.creator.eql(parent.creator) or
        !std.mem.eql(u8, &std.fmt.bytesToHex(parent.metadata_incarnation, .lower), &value.incarnation) or
        !std.mem.eql(u8, &try fingerprint(manifest.root, "antfly/scope-retirement-table/v1\x00"), &child.definition) or
        !std.mem.eql(u8, &try fingerprint(manifest.root_ranges, "antfly/scope-retirement-ranges/v1\x00"), &child.ranges)) return error.InvalidScopeRetirement;
    for (manifest.retiring_parents) |retiring| {
        if (!std.meta.eql(retiring.lookup, child.lookup) or
            !std.meta.eql((try scopes.child(reader, child.lookup, retiring.ordinal)) orelse return error.InvalidScopeRetirement, retiring)) return error.InvalidScopeRetirement;
        const lease = (try scopes.member(reader, child.lookup.group, .table, retiring.table)) orelse return error.InvalidScopeRetirement;
        if (lease.intent != .retire) return error.InvalidScopeRetirement;
        const table = if (retiring.table == manifest.root.table_id) manifest.root else blk: {
            for (manifest.external_parents) |held| if (held.table.table_id == retiring.table) break :blk held.table;
            return error.InvalidScopeRetirement;
        };
        const ranges = if (retiring.table == manifest.root.table_id) manifest.root_ranges else blk: {
            for (manifest.external_parents) |held| if (held.table.table_id == retiring.table) break :blk held.ranges;
            return error.InvalidScopeRetirement;
        };
        if (!std.mem.eql(u8, &try fingerprint(table, "antfly/scope-retirement-table/v1\x00"), &retiring.definition) or
            !std.mem.eql(u8, &try fingerprint(ranges, "antfly/scope-retirement-ranges/v1\x00"), &retiring.ranges)) return error.InvalidScopeRetirement;
    }
    for (participants) |table| {
        const lease = (try scopes.member(reader, child.lookup.group, .table, table)) orelse return error.GenerationPublicationChanged;
        if (!std.meta.eql(lease.lookup, child.lookup) or !std.mem.eql(u8, &lease.metadata_incarnation, &parent.metadata_incarnation)) return error.GenerationPublicationChanged;
        if (try @import("table_lifecycle_lock.zig").read(reader, child.lookup.group, table) != null) return error.GenerationPublicationChanged;
    }
}
/// Parent ACKs borrow the ancestor's exact table lease. A shared parent may
/// itself be another deletion child, and its source alias belongs to that
/// child's journal rather than to the child acknowledging this FK transition.
pub fn requireOwner(reader: anytype, value: tables.Head, table: u64) !void {
    const child = value.scope.parent orelse return error.InvalidScopeRetirement;
    const parent = try requireParent(reader, child);
    const source = (try tables.member(reader, value.lookup.group, value.lookup.table)) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(source.root, value.lookup) or source.ordinal != 0) return error.InvalidCatalogRecord;
    const lease = (try scopes.member(reader, value.lookup.group, .table, table)) orelse return error.InvalidScopeRetirement;
    if (!std.meta.eql(lease.lookup, child.lookup) or !std.mem.eql(u8, &lease.metadata_incarnation, &parent.metadata_incarnation) or
        try @import("table_lifecycle_lock.zig").read(reader, value.lookup.group, table) != null) return error.InvalidScopeRetirement;
}

pub fn requireVacant(reader: anytype, prepared: *const tables.Prepared) !void {
    const parent = try requireParent(reader, prepared.head.scope.parent.?);
    if (prepared.head.scope.parent.?.ordinal != parent.progress.completed_tables or parent.progress.phase != .retiring_tables) return error.GenerationPublicationChanged;
    try requireCut(reader, prepared.head, prepared.value, prepared.tables);
    var buffer: [256]u8 = undefined;
    if (try optional(reader, try tables.key(&buffer, prepared.head.lookup, .active)) != null or
        try optional(reader, try tables.key(&buffer, prepared.head.lookup, .effects)) != null or
        try tables.member(reader, prepared.head.lookup.group, prepared.head.lookup.table) != null) return error.GenerationPublicationChanged;
    for (prepared.value.root_ranges) |range| if (try optional(reader, try tables.rangeKey(&buffer, prepared.head.lookup.group, range.group_id)) != null) return error.GenerationPublicationChanged;
}
pub fn stageBegin(txn: anytype, prepared: *const tables.Prepared) !void {
    const value = prepared.head;
    var buffer: [256]u8 = undefined;
    // The ancestor already excludes all conflicting schema, topology and
    // dependency work. Shared parent tables need no competing child locks.
    const member = try tables.MemberRecord.init(.{ .root = value.lookup, .table = value.lookup.table, .ordinal = 0 });
    try txn.put(try tables.memberKey(&buffer, value.lookup.group, value.lookup.table), member.slice());
    for (prepared.value.root_ranges) |range| {
        const alias = try tables.RangeRecord.init(.{ .root = value.lookup, .table = value.lookup.table, .group = range.group_id });
        try txn.put(try tables.rangeKey(&buffer, value.lookup.group, range.group_id), alias.slice());
    }
    try txn.put(try tables.key(&buffer, value.lookup, .effects), prepared.encoded);
    const head = try tables.HeadRecord.init(value);
    try txn.put(try tables.key(&buffer, value.lookup, .head), head.slice());
    const active = try tables.ActiveRecord.init(value.lookup);
    try txn.put(try tables.key(&buffer, value.lookup, .active), active.slice());
}
pub fn requireReservations(reader: anytype, value: tables.Head, manifest: manifests.Manifest, participants: []const u64) !void {
    try requireCut(reader, value, manifest, participants);
    var buffer: [256]u8 = undefined;
    const active = try tables.ActiveRecord.decode((try optional(reader, try tables.key(&buffer, value.lookup, .active))) orelse return error.InvalidCatalogRecord);
    const member = (try tables.member(reader, value.lookup.group, value.lookup.table)) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(active, value.lookup) or !std.meta.eql(member.root, value.lookup) or member.ordinal != 0) return error.InvalidCatalogRecord;
    for (manifest.root_ranges) |range| try tables.requireRange(reader, member, range.group_id);
}

/// A ready phase is joined to its exact native plan and signed receipt set.
/// Registration, a source fence alone, and a forged summary are insufficient.
pub fn requireReady(backing: std.mem.Allocator, reader: anytype, child: scopes.Child) !tables.Head {
    _ = try requireParent(reader, child);
    const value = (try tables.head(reader, .{ .group = child.lookup.group, .table = child.table, .id = child.id })) orelse return error.ScopeRetirementChanged;
    if (!std.meta.eql(value.scope.parent, @as(?scopes.Child, child)) or value.phase != .physical_ready or
        value.physical == null or value.physical.?.phase != .ready) return error.ScopeRetirementChanged;
    var buffer: [256]u8 = undefined;
    var body = try manifests.Owned.parse(backing, try reader.get(try tables.key(&buffer, value.lookup, .effects)));
    defer body.deinit();
    const participants = try tables.participantTables(body.arena.allocator(), body.value);
    try tables.requireReservations(reader, value, body.value, participants);
    _ = try tables.verifyPhysical(backing, reader, value, body.value);
    return value;
}

/// Cold publication verifies the retained physical proof, the root's exact
/// catalog decision and the immutable child registration. No live reservation
/// is required after that atomic decision has withdrawn the whole scope.
pub fn requirePublished(backing: std.mem.Allocator, reader: anytype, child: scopes.Child) !tables.Head {
    try child.validate();
    const parent = (try scopes.head(reader, child.lookup)) orelse return error.InvalidScopeRetirement;
    if (parent.progress.phase != .published or parent.progress.completed_tables != parent.lookup.reference.tables or
        !std.meta.eql((try scopes.child(reader, child.lookup, child.ordinal)) orelse return error.InvalidScopeRetirement, child)) return error.InvalidScopeRetirement;
    const value = (try tables.head(reader, .{ .group = child.lookup.group, .table = child.table, .id = child.id })) orelse return error.InvalidScopeRetirement;
    try value.validate();
    if (value.phase != .published or value.direct_catalog_revision != parent.progress.catalog_revision or
        !std.meta.eql(value.scope.parent, @as(?scopes.Child, child)) or !value.scope.creator.eql(parent.creator) or
        !std.mem.eql(u8, &value.incarnation, &std.fmt.bytesToHex(parent.metadata_incarnation, .lower))) return error.InvalidScopeRetirement;
    var buffer: [256]u8 = undefined;
    var body = try manifests.Owned.parse(backing, try reader.get(try tables.key(&buffer, value.lookup, .effects)));
    defer body.deinit();
    if (!std.meta.eql(body.value.scope_child, value.scope.parent) or !std.meta.eql(try body.value.reference(), value.effects) or
        !std.mem.eql(u8, &try fingerprint(body.value.root, "antfly/scope-retirement-table/v1\x00"), &child.definition) or
        !std.mem.eql(u8, &try fingerprint(body.value.root_ranges, "antfly/scope-retirement-ranges/v1\x00"), &child.ranges) or
        try optional(reader, try tables.key(&buffer, value.lookup, .active)) != null or
        try tables.member(reader, child.lookup.group, child.table) != null) return error.InvalidScopeRetirement;
    for (body.value.root_ranges) |range| if (try optional(reader, try tables.rangeKey(&buffer, value.lookup.group, range.group_id)) != null) return error.InvalidScopeRetirement;
    _ = try tables.verifyPhysical(backing, reader, value, body.value);
    return value;
}
