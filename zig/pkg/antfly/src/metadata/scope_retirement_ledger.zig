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

//! Native ancestor-retirement reservations. Membership bodies are written once;
//! progress and queue rows stay compact. The caller must revalidate the captured
//! catalog cut and physical proof inside the same writer before staging effects.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.system_catalog_domain.scope_retirement;
const records = @import("schema_validation_publication.zig");
const A = std.mem.Allocator;
pub const Lookup = struct {
    group: u64,
    reference: contract.Reference,
    pub fn validate(self: Lookup) !void {
        if (self.group == 0) return error.InvalidScopeRetirement;
        try self.reference.validate();
    }
};
pub const Head = struct {
    lookup: Lookup,
    metadata_incarnation: [16]u8,
    creator: local.system_catalog_privileges.Principal,
    target_digest: contract.Digest,
    force: bool = false,
    sequence: u64,
    progress: contract.Progress,
    pub fn validate(self: Head) !void {
        try self.lookup.validate();
        try self.creator.validate();
        try self.progress.validate();
        if (std.mem.allEqual(u8, &self.target_digest, 0) or self.creator.kind == .public or self.sequence == 0 or std.mem.allEqual(u8, &self.metadata_incarnation, 0) or
            !std.meta.eql(self.lookup.reference, self.progress.reference)) return error.InvalidScopeRetirement;
    }
    pub fn active(self: Head) bool {
        return self.progress.phase != .published and self.progress.phase != .canceled;
    }
};
pub const HeadRecord = records.Record(Head, "SRHD", 2048);
pub const Lease = struct {
    lookup: Lookup,
    metadata_incarnation: [16]u8,
    intent: @FieldType(contract.Dependency, "action") = .retire,
    pub fn validate(self: Lease) !void {
        try self.lookup.validate();
        if (std.mem.allEqual(u8, &self.metadata_incarnation, 0)) return error.InvalidScopeRetirement;
    }
};
pub const LeaseRecord = records.Record(Lease, "SRLS", 1024);
pub const QueueRecord = records.Record(Lookup, "SRQU", 1024);
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    manifest: contract.Manifest,
    lookup: Lookup,
    encoded: []const u8,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub fn prepare(backing: A, group: u64, id: contract.Id, bytes: []const u8) !Prepared {
    if (bytes.len == 0 or bytes.len > contract.max_manifest_bytes) return error.InvalidScopeRetirement;
    var arena: std.heap.ArenaAllocator = .init(backing);
    errdefer arena.deinit();
    const a = arena.allocator();
    const manifest = try std.json.parseFromSliceLeaky(contract.Manifest, a, bytes, .{ .allocate = .alloc_always });
    if (manifest.metadata_group_id != group) return error.InvalidScopeRetirement;
    const lookup: Lookup = .{ .group = group, .reference = try manifest.reference(a, id) };
    try lookup.validate();
    const encoded = try std.json.Stringify.valueAlloc(a, manifest, .{});
    return .{ .arena = arena, .manifest = manifest, .lookup = lookup, .encoded = encoded };
}
const Kind = enum { head, manifest, terminal_outcome, drain_outcome, source_cursor };
pub fn prefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidScopeRetirement;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:scope-retirement:", .{group});
}
pub fn key(buffer: []u8, lookup: Lookup, kind: Kind) ![]const u8 {
    try lookup.validate();
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{s}:{x:0>16}:{s}:{s}", .{ try prefix(&base, lookup.group), @tagName(lookup.reference.kind), lookup.reference.root, std.fmt.bytesToHex(lookup.reference.id, .lower), @tagName(kind) });
}
pub fn memberKey(buffer: []u8, group: u64, kind: local.system_catalog_domain.Kind, id: u64) ![]const u8 {
    return @import("scope_retirement_fence.zig").memberKey(buffer, group, @tagName(kind), id);
}
/// Every deletion child is registered atomically with root admission, before
/// any proposal to a data owner. A compact identity survives a lost child reply;
/// membership is never reconstructed from a mutable display name. Registration
/// is not a physical completion receipt or permission to skip owner barriers.
pub const Child = struct {
    lookup: Lookup,
    ordinal: u16,
    table: u64,
    id: contract.Id,
    definition: contract.Digest,
    ranges: contract.Digest,
    pub fn validate(self: Child) !void {
        try self.lookup.validate();
        if (self.ordinal >= self.lookup.reference.tables or self.table == 0 or
            !std.mem.eql(u8, &self.id, &try self.lookup.reference.childId(self.table)) or
            std.mem.allEqual(u8, &self.definition, 0) or std.mem.allEqual(u8, &self.ranges, 0)) return error.InvalidScopeRetirement;
    }
    fn fromTable(lookup: Lookup, ordinal: u16, table: contract.Table) !Child {
        const result: Child = .{ .lookup = lookup, .ordinal = ordinal, .table = table.id, .id = try lookup.reference.childId(table.id), .definition = table.definition, .ranges = table.ranges };
        try result.validate();
        return result;
    }
};
pub const ChildRecord = records.Record(Child, "SRCH", 2048);
pub fn childKey(buffer: []u8, lookup: Lookup, ordinal: u16) ![]const u8 {
    try lookup.validate();
    if (ordinal >= lookup.reference.tables) return error.InvalidScopeRetirement;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{s}:{x:0>16}:{s}:child:{x:0>4}", .{ try prefix(&base, lookup.group), @tagName(lookup.reference.kind), lookup.reference.root, std.fmt.bytesToHex(lookup.reference.id, .lower), ordinal });
}
pub fn child(reader: anytype, lookup: Lookup, ordinal: u16) !?Child {
    var buffer: [320]u8 = undefined;
    const bytes = (try optional(reader, try childKey(&buffer, lookup, ordinal))) orelse return null;
    const result = try ChildRecord.decode(bytes);
    try result.validate();
    if (!std.meta.eql(result.lookup, lookup) or result.ordinal != ordinal) return error.InvalidScopeRetirement;
    return result;
}
/// One compact feed response. A live job with remaining children always carries
/// its exact next registered identity; reading it does not load the manifest.
pub const SourceControl = struct { root: Lookup, expected_after: ?local.storage_pessimistic_lock_contract.Backend = null };
pub const SourceCursor = struct { lookup: Lookup, backend: local.storage_pessimistic_lock_contract.Backend };
pub const SourceCursorRecord = records.Record(SourceCursor, "SRSC", 1024);
pub fn sourceCursor(reader: anytype, lookup: Lookup) !?SourceCursor {
    var buffer: [320]u8 = undefined;
    const bytes = (try optional(reader, try key(&buffer, lookup, .source_cursor))) orelse return null;
    const value = try SourceCursorRecord.decode(bytes);
    try value.lookup.validate();
    try value.backend.validate();
    if (!std.meta.eql(value.lookup, lookup)) return error.InvalidScopeRetirement;
    return value;
}
pub fn stageSourceCursor(txn: anytype, control: SourceControl, backend: local.storage_pessimistic_lock_contract.Backend) !void {
    const current = try sourceCursor(txn, control.root);
    if (!std.meta.eql(if (current) |value| @as(?local.storage_pessimistic_lock_contract.Backend, value.backend) else null, control.expected_after)) return error.ScopeRetirementChanged;
    const value = try SourceCursorRecord.init(.{ .lookup = control.root, .backend = backend });
    var buffer: [320]u8 = undefined;
    try txn.put(try key(&buffer, control.root, .source_cursor), value.slice());
}
pub const Work = struct { head: Head, child: ?Child, source_after: ?local.storage_pessimistic_lock_contract.Backend = null };
pub fn nextOwnerWork(reader: anytype, group: u64, after: u64) !?Work {
    const value = (try nextWork(reader, group, after)) orelse return null;
    const selected = if (value.progress.completed_tables < value.lookup.reference.tables)
        (try child(reader, value.lookup, value.progress.completed_tables)) orelse return error.InvalidScopeRetirement
    else
        null;
    const source = try sourceCursor(reader, value.lookup);
    return .{ .head = value, .child = selected, .source_after = if (source) |cursor| cursor.backend else null };
}

pub fn queuePrefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidScopeRetirement;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:scope-retirement-queue:", .{group});
}
pub fn queueKey(buffer: []u8, group: u64, sequence: u64) ![]const u8 {
    if (sequence == 0) return error.InvalidScopeRetirement;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try queuePrefix(&base, group), sequence });
}
pub fn counterKey(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidScopeRetirement;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:scope-retirement-sequence", .{group});
}
fn optional(reader: anytype, name: []const u8) !?[]const u8 {
    return reader.get(name) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn head(reader: anytype, lookup: Lookup) !?Head {
    var buffer: [256]u8 = undefined;
    const bytes = (try optional(reader, try key(&buffer, lookup, .head))) orelse return null;
    const value = try HeadRecord.decode(bytes);
    try value.validate();
    if (!std.meta.eql(value.lookup, lookup)) return error.CatalogGenerationChanged;
    return value;
}
pub fn member(reader: anytype, group: u64, kind: local.system_catalog_domain.Kind, id: u64) !?Lease {
    var buffer: [256]u8 = undefined;
    const bytes = (try optional(reader, try memberKey(&buffer, group, kind, id))) orelse return null;
    const lease = try LeaseRecord.decode(bytes);
    try lease.validate();
    if (lease.lookup.group != group) return error.InvalidScopeRetirement;
    return lease;
}
/// One compact cyclic queue item. Queue membership and head progress share
/// the same native transaction; corrupt or orphaned work is never skipped.
pub fn nextWork(reader: anytype, group: u64, after: u64) !?Head {
    var prefix_buffer: [256]u8 = undefined;
    const base = try queuePrefix(&prefix_buffer, group);
    var seek_buffer: [256]u8 = undefined;
    const seek = if (after == std.math.maxInt(u64)) base else try queueKey(&seek_buffer, group, after + 1);
    var cursor = try reader.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(seek);
    if (row == null or !std.mem.startsWith(u8, row.?.key, base)) row = try cursor.seekAtOrAfter(base);
    const entry = row orelse return null;
    if (!std.mem.startsWith(u8, entry.key, base)) return null;
    if (entry.key.len != base.len + 16) return error.InvalidScopeRetirement;
    const sequence = std.fmt.parseInt(u64, entry.key[base.len..], 16) catch return error.InvalidScopeRetirement;
    const lookup = try QueueRecord.decode(entry.value);
    if (lookup.group != group) return error.InvalidScopeRetirement;
    const value = (try head(reader, lookup)) orelse return error.InvalidScopeRetirement;
    if (!value.active() or value.sequence != sequence) return error.InvalidScopeRetirement;
    return value;
}

fn rootKind(value: contract.Manifest) local.system_catalog_domain.Kind {
    return if (value.scope.kind == .database) .database else .namespace;
}
fn requireDefinitionVacant(reader: anytype, group: u64, kind: local.system_catalog_domain.Kind, id: u64) !void {
    switch (kind) {
        .view => try @import("../system_catalog/view_leases.zig").requireUnleased(reader, group, id),
        .sequence => {
            try @import("../system_catalog/sequence_leases.zig").requireUnleased(reader, group, id);
            if (try @import("schema_validation_ledger.zig").activeSequence(reader, group, id) != null) return error.CatalogPublicationProofPending;
        },
        .named_type => if (try @import("../system_catalog/named_type_dependencies.zig").retirementLock(reader, group, id) != null or
            try @import("schema_validation_ledger.zig").activeType(reader, group, id) != null) return error.CatalogPublicationProofPending,
        else => {},
    }
}

pub fn requireVacant(reader: anytype, prepared: *const Prepared) !void {
    const group = prepared.lookup.group;
    // Distinct namespaces can retire concurrently. A database retirement
    // intersects every namespace reservation, and blocks new namespace jobs.
    if (try member(reader, group, .database, prepared.manifest.scope.database.id) != null) return error.TableTransitionActive;
    if (try member(reader, group, rootKind(prepared.manifest), prepared.manifest.scope.root().id) != null) return error.TableTransitionActive;
    for (prepared.manifest.resources) |resource| if (try member(reader, group, resource.kind, resource.id) != null) return error.TableTransitionActive;
    for (prepared.manifest.dependencies) |dependency| if (try member(reader, group, dependency.kind, dependency.id) != null) return error.TableTransitionActive;
    for (prepared.manifest.resources) |resource| try requireDefinitionVacant(reader, group, resource.kind, resource.id);
    for (prepared.manifest.dependencies) |dependency| try requireDefinitionVacant(reader, group, dependency.kind, dependency.id);
    // Admission and competing lifecycle jobs use the same native transaction.
    // Check both directions: a scope cannot capture an existing job or a parent
    // leased by FK validation, and later jobs cannot pass a scope's member key.
    for (prepared.manifest.tables) |table| {
        if (try @import("table_lifecycle_lock.zig").read(reader, group, table.id) != null) return error.TableTransitionActive;
        @import("schema_validation_dependencies.zig").requireUnleased(reader, group, table.id) catch |err| switch (err) {
            error.GenerationPublicationChanged => return error.TableTransitionActive,
            else => return err,
        };
    }
    for (prepared.manifest.dependencies) |dependency| if (dependency.table) |table| {
        if (try @import("table_lifecycle_lock.zig").read(reader, group, table.id) != null) return error.TableTransitionActive;
        @import("schema_validation_dependencies.zig").requireUnleased(reader, group, table.id) catch |err| switch (err) {
            error.GenerationPublicationChanged => return error.TableTransitionActive,
            else => return err,
        };
    };
}
/// Exact replay is observed before checking current vacancy. A completed or
/// canceled operation cannot reacquire reservations or replace its manifest.
pub fn existing(reader: anytype, prepared: *const Prepared) !?Head {
    const previous = (try head(reader, prepared.lookup)) orelse return null;
    if (!std.mem.eql(u8, &previous.metadata_incarnation, &prepared.manifest.metadata_incarnation) or !previous.creator.eql(prepared.manifest.creator) or previous.force != prepared.manifest.force or !std.mem.eql(u8, &previous.target_digest, &prepared.manifest.scope.labelsDigest())) return error.CatalogGenerationChanged;
    var buffer: [256]u8 = undefined;
    const body = try optional(reader, try key(&buffer, prepared.lookup, .manifest));
    if (previous.active() or previous.progress.phase == .published) {
        if (body == null or !std.mem.eql(u8, body.?, prepared.encoded)) return error.InvalidScopeRetirement;
        if (previous.active()) try requireMembers(reader, prepared, previous);
    } else if (body != null) return error.InvalidScopeRetirement;
    return previous;
}
/// This stages native reservations only. Independent transaction-local scope,
/// catalog, decoder, SQL command and physical-preimage checks precede it.
pub fn stageBegin(txn: anytype, prepared: *const Prepared) !Head {
    if (try existing(txn, prepared)) |previous| return previous;
    try requireVacant(txn, prepared);
    var buffer: [256]u8 = undefined;
    if (try optional(txn, try key(&buffer, prepared.lookup, .manifest)) != null or
        try optional(txn, try key(&buffer, prepared.lookup, .terminal_outcome)) != null or
        try optional(txn, try key(&buffer, prepared.lookup, .drain_outcome)) != null or
        try optional(txn, try key(&buffer, prepared.lookup, .source_cursor)) != null) return error.InvalidScopeRetirement;
    const previous = if (try optional(txn, try counterKey(&buffer, prepared.lookup.group))) |bytes| blk: {
        if (bytes.len != 8) return error.InvalidScopeRetirement;
        break :blk std.mem.readInt(u64, bytes[0..8], .little);
    } else 0;
    const sequence = std.math.add(u64, previous, 1) catch return error.SqlProgramLimitExceeded;
    if (try optional(txn, try queueKey(&buffer, prepared.lookup.group, sequence)) != null) return error.InvalidScopeRetirement;
    const value: Head = .{ .lookup = prepared.lookup, .metadata_incarnation = prepared.manifest.metadata_incarnation, .creator = prepared.manifest.creator, .target_digest = prepared.manifest.scope.labelsDigest(), .force = prepared.manifest.force, .sequence = sequence, .progress = .{ .reference = prepared.lookup.reference } };
    try value.validate();
    for (0..prepared.lookup.reference.tables) |ordinal| {
        if (try child(txn, prepared.lookup, @intCast(ordinal)) != null) return error.InvalidScopeRetirement;
    }
    const encoded_head = try HeadRecord.init(value);
    const encoded_queue = try QueueRecord.init(prepared.lookup);
    const lease = try LeaseRecord.init(.{ .lookup = prepared.lookup, .metadata_incarnation = value.metadata_incarnation });
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, sequence, .little);
    // A caller aborts the entire native transaction on any staged-write error.
    try txn.put(try counterKey(&buffer, prepared.lookup.group), &count);
    try txn.put(try key(&buffer, prepared.lookup, .manifest), prepared.encoded);
    try txn.put(try key(&buffer, prepared.lookup, .head), encoded_head.slice());
    try txn.put(try queueKey(&buffer, prepared.lookup.group, sequence), encoded_queue.slice());
    try txn.put(try memberKey(&buffer, prepared.lookup.group, rootKind(prepared.manifest), prepared.manifest.scope.root().id), lease.slice());
    for (prepared.manifest.resources) |resource| try txn.put(try memberKey(&buffer, prepared.lookup.group, resource.kind, resource.id), lease.slice());
    for (prepared.manifest.dependencies) |dependency| {
        const participant = try LeaseRecord.init(.{ .lookup = prepared.lookup, .metadata_incarnation = value.metadata_incarnation, .intent = dependency.action });
        try txn.put(try memberKey(&buffer, prepared.lookup.group, dependency.kind, dependency.id), participant.slice());
    }
    var tables = prepared.manifest.retiringTables();
    var ordinal: u16 = 0;
    while (tables.next()) |table| : (ordinal += 1) {
        const registered = try ChildRecord.init(try Child.fromTable(prepared.lookup, ordinal, table));
        var child_buffer: [320]u8 = undefined;
        try txn.put(try childKey(&child_buffer, prepared.lookup, ordinal), registered.slice());
    }
    if (ordinal != prepared.lookup.reference.tables) return error.InvalidScopeRetirement;
    return value;
}
pub fn requireMembers(reader: anytype, prepared: *const Prepared, value: Head) !void {
    try value.validate();
    const expected: Lease = .{ .lookup = value.lookup, .metadata_incarnation = value.metadata_incarnation };
    const root = (try member(reader, value.lookup.group, rootKind(prepared.manifest), prepared.manifest.scope.root().id)) orelse return error.InvalidScopeRetirement;
    if (!std.meta.eql(root, expected)) return error.InvalidScopeRetirement;
    for (prepared.manifest.resources) |resource| {
        const lease = (try member(reader, value.lookup.group, resource.kind, resource.id)) orelse return error.InvalidScopeRetirement;
        if (!std.meta.eql(lease, expected)) return error.InvalidScopeRetirement;
    }
    for (prepared.manifest.dependencies) |dependency| {
        var participant = expected;
        participant.intent = dependency.action;
        const lease = (try member(reader, value.lookup.group, dependency.kind, dependency.id)) orelse return error.InvalidScopeRetirement;
        if (!std.meta.eql(lease, participant)) return error.InvalidScopeRetirement;
    }
    var tables = prepared.manifest.retiringTables();
    var ordinal: u16 = 0;
    while (tables.next()) |table| : (ordinal += 1) {
        const registered = (try child(reader, prepared.lookup, ordinal)) orelse return error.InvalidScopeRetirement;
        if (!std.meta.eql(registered, try Child.fromTable(prepared.lookup, ordinal, table))) return error.InvalidScopeRetirement;
    }
    if (ordinal != prepared.lookup.reference.tables) return error.InvalidScopeRetirement;
    var buffer: [256]u8 = undefined;
    const queued = (try optional(reader, try queueKey(&buffer, value.lookup.group, value.sequence))) orelse return error.InvalidScopeRetirement;
    if (!std.meta.eql(try QueueRecord.decode(queued), value.lookup)) return error.InvalidScopeRetirement;
}
/// The native apply owner verifies connection drainage, physical child receipts
/// or the final catalog publication independently before advancing this CAS.
pub fn stageAdvance(txn: anytype, prepared: *const Prepared, before: Head, next: contract.Progress) !Head {
    const current = (try existing(txn, prepared)) orelse return error.CatalogGenerationChanged;
    if (!std.meta.eql(current, before) or !before.active() or !std.meta.eql(next.reference, before.lookup.reference)) return error.CatalogGenerationChanged;
    const expected = switch (next.phase) {
        .draining_connections => return error.ScopeRetirementChanged,
        .retiring_tables => if (before.progress.phase == .draining_connections) try before.progress.drained() else try before.progress.tablePublished(before.progress.completed_tables),
        .publishing_catalog => if (before.progress.phase == .draining_connections) try before.progress.drained() else try before.progress.tablePublished(before.progress.completed_tables),
        .published => try before.progress.published(next.catalog_revision, prepared.manifest.catalog_revision),
        .canceled => try before.progress.cancelBeforeEffects(),
    };
    if (!std.meta.eql(next, expected)) return error.ScopeRetirementChanged;
    var result = before;
    result.progress = next;
    try result.validate();
    const encoded = try HeadRecord.init(result);
    var buffer: [256]u8 = undefined;
    if (try sourceCursor(txn, result.lookup) != null) try txn.delete(try key(&buffer, result.lookup, .source_cursor));
    if (!result.active()) {
        try txn.delete(try memberKey(&buffer, result.lookup.group, rootKind(prepared.manifest), prepared.manifest.scope.root().id));
        for (prepared.manifest.resources) |resource| try txn.delete(try memberKey(&buffer, result.lookup.group, resource.kind, resource.id));
        for (prepared.manifest.dependencies) |dependency| try txn.delete(try memberKey(&buffer, result.lookup.group, dependency.kind, dependency.id));
        try txn.delete(try queueKey(&buffer, result.lookup.group, result.sequence));
        // Publication retains its immutable cut and fixed child directory for
        // cold verification. Cancellation has no physical proof to preserve.
        if (result.progress.phase == .canceled) {
            try txn.delete(try key(&buffer, result.lookup, .manifest));
            var child_buffer: [320]u8 = undefined;
            for (0..result.lookup.reference.tables) |ordinal| try txn.delete(try childKey(&child_buffer, result.lookup, @intCast(ordinal)));
        }
    }
    try txn.put(try key(&buffer, result.lookup, .head), encoded.slice());
    return result;
}
