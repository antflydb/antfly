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

//! Durable root reservation for whole-table dependency retirement. The logical
//! effects are stored once, separately from compact progress. Reservation and
//! its cancellation grant no physical owner publication or deletion authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const manifests = @import("table_retirement_manifest.zig");
pub const physical = @import("table_retirement_physical.zig");
const records = @import("schema_validation_publication.zig");
const lifecycle = @import("table_lifecycle_lock.zig");
const counters = @import("../system_catalog/sequence_leases.zig");
const views = @import("../system_catalog/view_leases.zig");
const A = std.mem.Allocator;
pub const Id = [16]u8;
pub const max_tables = 1 + manifests.max_survivors + manifests.max_parents + manifests.max_views;
pub const Phase = enum { reserved, canceled, retiring, physical_ready, validating_survivors, published };
pub const Lookup = physical.Lookup;
pub const Scope = struct {
    database: u64,
    namespace: u64,
    creator: local.system_catalog_privileges.Principal,
    target_digest: [32]u8 = @splat(0),
    pub fn targetDigest(database: []const u8, namespace: []const u8, name: []const u8) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/table-retirement-label/v1");
        for ([_][]const u8{ database, namespace, name }) |part| {
            var size: [8]u8 = undefined;
            std.mem.writeInt(u64, &size, part.len, .little);
            hash.update(&size);
            hash.update(part);
        }
        var out: [32]u8 = undefined;
        hash.final(&out);
        return out;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        if (std.mem.allEqual(u8, &self.target_digest, 0)) return writer.write(.{ .database = self.database, .namespace = self.namespace, .creator = self.creator });
        return writer.write(.{ .database = self.database, .namespace = self.namespace, .creator = self.creator, .target_digest = self.target_digest });
    }
    pub fn validate(self: @This()) !void {
        try self.creator.validate();
        if (self.database == 0 or self.namespace == 0 or self.creator.kind == .public) return error.InvalidCatalogRecord;
    }
};
pub const Head = struct {
    lookup: Lookup,
    incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    effects: manifests.Reference,
    scope: Scope,
    participant_count: u16,
    range_count: u16,
    participants_digest: [32]u8,
    revision: u64 = 1,
    phase: Phase = .reserved,
    physical: ?physical.State = null,
    survivor: ?@import("table_retirement_authority.zig").Cohort = null,
    schema_attempts: u32 = 0,
    /// Logical catalog cut for publication with no surviving schemas.
    direct_catalog_revision: u64 = 0,
    pub fn active(self: @This()) bool {
        return self.phase != .canceled and self.phase != .published;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            const skip = (comptime std.mem.eql(u8, field, "physical")) and self.physical == null or
                (comptime std.mem.eql(u8, field, "survivor")) and self.survivor == null or
                (comptime std.mem.eql(u8, field, "schema_attempts")) and self.schema_attempts == 0 or
                (comptime std.mem.eql(u8, field, "direct_catalog_revision")) and self.direct_catalog_revision == 0;
            if (!skip) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: @This()) !void {
        try self.lookup.validate();
        try self.effects.validate();
        try self.scope.validate();
        if (!local.metadata_incarnation.isValid(self.incarnation) or self.effects.root_table != self.lookup.table or
            self.range_count > manifests.max_ranges or self.participant_count == 0 or self.participant_count > max_tables or std.mem.allEqual(u8, &self.participants_digest, 0) or
            (self.phase == .reserved and self.revision != 1) or (self.phase == .canceled and self.revision != 2) or
            ((self.phase != .reserved and self.phase != .canceled) != (self.physical != null))) return error.InvalidCatalogRecord;
        if (self.physical) |state| {
            try state.validate();
            var expected_revision: u64 = 2;
            for (state.counts) |count| expected_revision += count;
            expected_revision += @as(u64, self.schema_attempts) * 2;
            if (self.direct_catalog_revision != 0) expected_revision += 1;
            if (self.phase == .validating_survivors) {
                if (self.schema_attempts == 0) return error.InvalidCatalogRecord;
                expected_revision -= 1;
            }
            if (self.revision != expected_revision or (self.phase != .retiring) != (state.phase == .ready)) return error.InvalidCatalogRecord;
        }
        if (self.survivor) |candidate| {
            try candidate.validate();
            if (candidate.table == self.lookup.table or std.mem.eql(u8, &candidate.id, &self.lookup.id) or self.schema_attempts == 0 or
                (self.phase != .validating_survivors and self.phase != .published)) return error.InvalidCatalogRecord;
        } else if (self.phase == .validating_survivors or (self.phase == .published and self.schema_attempts != 0)) return error.InvalidCatalogRecord;
        // Exactly one publication proof: the common survivor cohort or the
        // direct logical cut after every physical owner barrier completed.
        if (self.direct_catalog_revision != 0) {
            if (self.phase != .published or self.effects.survivors != 0 or self.schema_attempts != 0 or self.survivor != null) return error.InvalidCatalogRecord;
        } else if (self.phase == .published and (self.schema_attempts == 0 or self.survivor == null)) return error.InvalidCatalogRecord;
        if (self.schema_attempts != 0 and self.effects.survivors == 0) return error.InvalidCatalogRecord;
        if (self.physical == null and (self.schema_attempts != 0 or self.survivor != null)) return error.InvalidCatalogRecord;
    }
    pub fn lease(self: @This()) counters.Owner {
        return .{ .table = self.lookup.table, .publication = self.lookup.id, .incarnation = self.incarnation, .effects = self.effects.digest };
    }
};
/// A bounded dispatcher read. The immutable plan is fetched by owner decision
/// reads, rather than copied back through every coordinator ACK.
pub const Work = struct { head: Head, owner: ?physical.Owner, owner_name: []const u8 = "", root_name: []const u8 = "" };
const HeadWire = records.Record(Head, "TRHD", 8192);
pub const HeadRecord = struct {
    wire: HeadWire,
    pub fn init(value: Head) !@This() {
        value.validate() catch return error.InvalidCatalogRecord;
        return .{ .wire = try HeadWire.init(value) };
    }
    pub fn slice(self: *const @This()) []const u8 {
        return self.wire.slice();
    }
    pub fn decode(bytes: []const u8) !Head {
        const value = try HeadWire.decode(bytes);
        value.validate() catch return error.InvalidCatalogRecord;
        return value;
    }
};
pub const ActiveRecord = records.Record(Lookup, "TRAC", 256);
pub fn prefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidCatalogRecord;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:table-retirement:", .{group});
}
pub const Kind = enum { head, effects, active };
pub fn key(buffer: []u8, lookup: Lookup, kind: Kind) ![]const u8 {
    try lookup.validate();
    var base: [128]u8 = undefined;
    return if (kind == .active)
        std.fmt.bufPrint(buffer, "{s}{x:0>16}:active", .{ try prefix(&base, lookup.group), lookup.table })
    else
        std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:{s}", .{ try prefix(&base, lookup.group), lookup.table, std.fmt.bytesToHex(lookup.id, .lower), @tagName(kind) });
}
pub const Address = struct { lookup: Lookup, kind: Kind };
pub fn address(group: u64, bytes: []const u8) !?Address {
    var buffer: [128]u8 = undefined;
    const start = try prefix(&buffer, group);
    if (!std.mem.startsWith(u8, bytes, start)) return null;
    var parts = std.mem.splitScalar(u8, bytes[start.len..], ':');
    const table = parts.next() orelse return error.InvalidCatalogRecord;
    const job = parts.next() orelse return error.InvalidCatalogRecord;
    var result: Address = .{ .lookup = .{ .group = group, .table = std.fmt.parseInt(u64, table, 16) catch return error.InvalidCatalogRecord, .id = @splat(1) }, .kind = .active };
    if (!std.mem.eql(u8, job, "active")) {
        _ = std.fmt.hexToBytes(&result.lookup.id, job) catch return error.InvalidCatalogRecord;
        const lane = parts.next() orelse return error.InvalidCatalogRecord;
        result.kind = if (std.mem.eql(u8, lane, "head")) .head else if (std.mem.eql(u8, lane, "effects")) .effects else return error.InvalidCatalogRecord;
    }
    if (parts.next() != null) return error.InvalidCatalogRecord;
    var canonical: [256]u8 = undefined;
    if (!std.mem.eql(u8, bytes, try key(&canonical, result.lookup, result.kind))) return error.InvalidCatalogRecord;
    return result;
}
fn optional(reader: anytype, selected: []const u8) !?[]const u8 {
    return reader.get(selected) catch |err| return if (err == error.NotFound) null else err;
}
pub fn head(reader: anytype, lookup: Lookup) !?Head {
    var buffer: [256]u8 = undefined;
    const bytes = (try optional(reader, try key(&buffer, lookup, .head))) orelse return null;
    const value = try HeadRecord.decode(bytes);
    if (!std.meta.eql(value.lookup, lookup)) return error.InvalidCatalogRecord;
    return value;
}
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    value: manifests.Manifest,
    encoded: []const u8,
    head: Head,
    tables: []u64,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
/// Decode/validate outside the metadata writer. The native writer independently
/// rejoins the transaction-local catalog before calling stageBegin.
pub fn prepare(backing: A, id: Id, scope: Scope, bytes: []const u8) !Prepared {
    var decoded = try manifests.Owned.parse(backing, bytes);
    errdefer decoded.deinit();
    return prepareOwned(&decoded, id, scope);
}
fn prepareOwned(decoded: *manifests.Owned, id: Id, scope: Scope) !Prepared {
    const a = decoded.arena.allocator();
    const tables = try participantTables(a, decoded.value);
    const value: Head = .{ .lookup = .{ .group = decoded.value.metadata_group_id, .table = decoded.value.root.table_id, .id = id }, .incarnation = decoded.value.metadata_incarnation, .effects = try decoded.value.reference(), .scope = scope, .participant_count = @intCast(tables.len), .range_count = @intCast(rangeCount(decoded.value)), .participants_digest = tableDigest(tables) };
    try value.validate();
    // Allocate the final body before copying the arena descriptor. Encoding
    // can grow its chunk list; copying it first would orphan that last chunk.
    const encoded = try decoded.value.encodeAlloc(a);
    return .{ .arena = decoded.arena, .value = decoded.value, .encoded = encoded, .head = value, .tables = tables };
}

pub const max_command_bytes = manifests.max_bytes + 4096;
pub const Command = union(enum) {
    begin: struct { id: Id, scope: Scope, manifest: manifests.Manifest },
    cancel: struct { lookup: Lookup, expected_revision: u64 },
    publish: struct { lookup: Lookup, expected_revision: u64 },
    seal: struct { lookup: Lookup, expected_revision: u64, plan: @import("fk_generation_publication.zig").Plan },
    ack: struct { lookup: Lookup, expected_revision: u64, ordinal: u16, step: physical.Step, evidence: physical.Evidence },
    pub fn encodeAlloc(self: @This(), a: A) ![]u8 {
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(self, .{}, &count.writer);
        if (count.fullCount() > max_command_bytes) return error.CatalogCommandTooLarge;
        const bytes = try a.alloc(u8, @intCast(count.fullCount()));
        errdefer a.free(bytes);
        var writer = std.Io.Writer.fixed(bytes);
        try std.json.Stringify.value(self, .{}, &writer);
        return bytes;
    }
};
pub const Input = struct {
    arena: ?std.heap.ArenaAllocator = null,
    command: Command,
    prepared: ?Prepared = null,
    prepared_physical: ?physical.Prepared = null,
    digest: [32]u8,
    pub fn parse(backing: A, bytes: []const u8) !@This() {
        if (bytes.len == 0 or bytes.len > max_command_bytes) return error.CatalogCommandTooLarge;
        var owned: manifests.Owned = .{ .arena = std.heap.ArenaAllocator.init(backing), .value = undefined };
        errdefer owned.arena.deinit();
        const command = try std.json.parseFromSliceLeaky(Command, owned.arena.allocator(), bytes, .{ .allocate = .alloc_always });
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &digest, .{});
        switch (command) {
            .begin => |request| {
                try request.manifest.validate(backing);
                owned.value = request.manifest;
                const prepared = try prepareOwned(&owned, request.id, request.scope);
                return .{ .command = command, .prepared = prepared, .digest = digest };
            },
            .seal => |request| {
                try request.lookup.validate();
                if (request.expected_revision == 0 or !std.mem.eql(u8, &request.lookup.id, &request.plan.id)) return error.InvalidCatalogRecord;
                const result = try physical.prepare(owned.arena.allocator(), backing, request.plan);
                return .{ .arena = owned.arena, .command = command, .prepared_physical = result, .digest = digest };
            },
            .ack => |request| {
                try request.lookup.validate();
                if (request.expected_revision == 0 or request.ordinal >= manifests.max_ranges) return error.InvalidCatalogRecord;
                return .{ .arena = owned.arena, .command = command, .digest = digest };
            },
            inline .cancel, .publish => |request| {
                try request.lookup.validate();
                if (request.expected_revision == 0) return error.InvalidCatalogRecord;
                return .{ .arena = owned.arena, .command = command, .digest = digest };
            },
        }
    }
    pub fn deinit(self: *@This()) void {
        if (self.prepared) |*value| value.deinit() else if (self.arena) |*value| value.deinit();
        self.* = undefined;
    }
};

pub fn participantTables(a: A, value: manifests.Manifest) ![]u64 {
    var result: std.ArrayList(u64) = .empty;
    errdefer result.deinit(a);
    try result.append(a, value.root.table_id);
    for (value.survivors) |pair| try result.append(a, pair.before.table_id);
    for (value.external_parents) |parent| try result.append(a, parent.table.table_id);
    for (value.view_backings) |item| try result.append(a, item.table.table_id);
    if (result.items.len > max_tables) return error.InvalidCatalogRecord;
    std.mem.sort(u64, result.items, {}, std.sort.asc(u64));
    for (result.items, 0..) |table, i| if (table == 0 or (i != 0 and result.items[i - 1] == table)) return error.InvalidCatalogRecord;
    return result.toOwnedSlice(a);
}
pub fn rangeCount(value: manifests.Manifest) usize {
    var count = value.root_ranges.len + value.survivor_ranges.len;
    for (value.external_parents) |parent| count += parent.ranges.len;
    for (value.view_backings) |item| count += item.ranges.len;
    return count;
}
pub const Member = struct { root: Lookup, table: u64, ordinal: u16 };
pub const MemberRecord = records.Record(Member, "TRMB", 256);
pub const Range = struct { root: Lookup, table: u64, group: u64 };
pub const RangeRecord = records.Record(Range, "TRRG", 256);
pub const ViewAlias = struct { root: Lookup, view: u64 };
pub const ViewAliasRecord = records.Record(ViewAlias, "TRVW", 256);
pub fn viewPrefix(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:table-retirement-view:", .{group});
}
pub fn viewKey(buffer: []u8, group: u64, view: u64) ![]const u8 {
    if (view == 0) return error.InvalidCatalogRecord;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try viewPrefix(&base, group), view });
}
pub fn viewRoot(reader: anytype, group: u64, view: u64) !?Lookup {
    var buffer: [192]u8 = undefined;
    const bytes = (try optional(reader, try viewKey(&buffer, group, view))) orelse return null;
    const value = try ViewAliasRecord.decode(bytes);
    try value.root.validate();
    if (value.root.group != group or value.view != view) return error.InvalidCatalogRecord;
    return value.root;
}
pub fn memberPrefix(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:table-retirement-member:", .{group});
}
pub fn memberKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    if (table == 0) return error.InvalidCatalogRecord;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try memberPrefix(&base, group), table });
}
pub fn rangePrefix(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:table-retirement-range:", .{group});
}
pub fn rangeKey(buffer: []u8, group: u64, range: u64) ![]const u8 {
    if (range == 0) return error.InvalidCatalogRecord;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try rangePrefix(&base, group), range });
}
pub fn member(reader: anytype, group: u64, table: u64) !?Member {
    var buffer: [192]u8 = undefined;
    const value = (try optional(reader, try memberKey(&buffer, group, table))) orelse return null;
    const result = try MemberRecord.decode(value);
    try result.root.validate();
    if (result.table != table or result.root.group != group or result.ordinal >= max_tables) return error.InvalidCatalogRecord;
    return result;
}
pub fn requireRange(reader: anytype, selected: Member, group: u64) !void {
    var buffer: [192]u8 = undefined;
    const bytes = (try optional(reader, try rangeKey(&buffer, selected.root.group, group))) orelse return error.InvalidCatalogRecord;
    const value = try RangeRecord.decode(bytes);
    if (!std.meta.eql(value.root, selected.root) or value.table != selected.table or value.group != group) return error.InvalidCatalogRecord;
}
fn forRanges(value: manifests.Manifest, context: anytype, comptime action: anytype) !void {
    for (value.root_ranges) |range| try action(context, range);
    for (value.survivor_ranges) |range| try action(context, range);
    for (value.external_parents) |parent| for (parent.ranges) |range| try action(context, range);
    for (value.view_backings) |item| for (item.ranges) |range| try action(context, range);
}
fn tableDigest(tables: []const u64) [32]u8 {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly/table-retirement-participants/v1\x00");
    var buffer: [8]u8 = undefined;
    for (tables) |table| {
        std.mem.writeInt(u64, &buffer, table, .big);
        state.update(&buffer);
    }
    var digest: [32]u8 = undefined;
    state.final(&digest);
    return digest;
}
/// Exact retries observe the accepted effects/scope. They cannot replace the
/// body, reacquire a released reservation or rerun admission after cancellation.
pub fn existing(reader: anytype, prepared: *const Prepared) !bool {
    const stored = (try head(reader, prepared.head.lookup)) orelse return false;
    if (!std.meta.eql(stored.effects, prepared.head.effects) or !std.meta.eql(stored.scope, prepared.head.scope) or
        !std.mem.eql(u8, &stored.incarnation, &prepared.head.incarnation) or stored.participant_count != prepared.head.participant_count or
        !std.mem.eql(u8, &stored.participants_digest, &prepared.head.participants_digest)) return error.GenerationPublicationChanged;
    var buffer: [256]u8 = undefined;
    const body = try optional(reader, try key(&buffer, stored.lookup, .effects));
    if (stored.phase == .canceled) {
        if (body != null) return error.InvalidCatalogRecord;
        return true;
    }
    if (body == null or !std.mem.eql(u8, body.?, prepared.encoded)) return error.InvalidCatalogRecord;
    try requireReservations(reader, stored, prepared.value, prepared.tables);
    return true;
}
pub fn requireVacant(txn: anytype, prepared: *const Prepared) !void {
    var buffer: [256]u8 = undefined;
    if (try optional(txn, try key(&buffer, prepared.head.lookup, .active)) != null or try optional(txn, try key(&buffer, prepared.head.lookup, .effects)) != null) return error.GenerationPublicationChanged;
    for (prepared.tables) |table| {
        try @import("schema_validation_dependencies.zig").requireUnleased(txn, prepared.head.lookup.group, table);
        if (try lifecycle.read(txn, prepared.head.lookup.group, table) != null or try member(txn, prepared.head.lookup.group, table) != null) return error.GenerationPublicationChanged;
    }
    for (prepared.value.counters) |counter| try counters.requireUnleased(txn, prepared.head.lookup.group, counter.resource.id);
    for (prepared.value.dependent_views) |view| try views.requireUnleased(txn, prepared.head.lookup.group, view.id);
}
pub fn stageBegin(txn: anytype, prepared: *const Prepared) !void {
    const value = prepared.head;
    var buffer: [256]u8 = undefined;
    // All expected rejection checks must have completed before staging.
    for (prepared.tables, 0..) |table, ordinal| {
        try lifecycle.acquire(txn, value.lookup.group, table, .{ .kind = .table_retirement, .id = value.lookup.id });
        const encoded_member = try MemberRecord.init(.{ .root = value.lookup, .table = table, .ordinal = @intCast(ordinal) });
        try txn.put(try memberKey(&buffer, value.lookup.group, table), encoded_member.slice());
    }
    const RangeWriter = struct {
        txn: @TypeOf(txn),
        root: Lookup,
        fn write(self: @This(), range: local.common_topology_records.RangeRecord) !void {
            var key_buffer: [192]u8 = undefined;
            const encoded_range = try RangeRecord.init(.{ .root = self.root, .table = range.table_id, .group = range.group_id });
            try self.txn.put(try rangeKey(&key_buffer, self.root.group, range.group_id), encoded_range.slice());
        }
    };
    try forRanges(prepared.value, RangeWriter{ .txn = txn, .root = value.lookup }, RangeWriter.write);
    for (prepared.value.counters) |counter| try counters.acquire(txn, value.lookup.group, counter.resource.id, value.lease());
    for (prepared.value.dependent_views) |view| {
        try views.acquire(txn, value.lookup.group, view.id, value.lease());
        const alias = try ViewAliasRecord.init(.{ .root = value.lookup, .view = view.id });
        try txn.put(try viewKey(&buffer, value.lookup.group, view.id), alias.slice());
    }
    try txn.put(try key(&buffer, value.lookup, .effects), prepared.encoded);
    const encoded = try HeadRecord.init(value);
    try txn.put(try key(&buffer, value.lookup, .head), encoded.slice());
    const active = try ActiveRecord.init(value.lookup);
    try txn.put(try key(&buffer, value.lookup, .active), active.slice());
}
pub fn requireReservations(reader: anytype, value: Head, manifest: manifests.Manifest, tables: []const u64) !void {
    if (!value.active() or value.participant_count != tables.len or !std.mem.eql(u8, &value.participants_digest, &tableDigest(tables)) or
        !std.meta.eql(value.effects, try manifest.reference())) return error.InvalidCatalogRecord;
    var buffer: [256]u8 = undefined;
    const active = try ActiveRecord.decode((try optional(reader, try key(&buffer, value.lookup, .active))) orelse return error.InvalidCatalogRecord);
    if (!std.meta.eql(active, value.lookup)) return error.InvalidCatalogRecord;
    for (tables, 0..) |table, ordinal| {
        const selected = (try member(reader, value.lookup.group, table)) orelse return error.InvalidCatalogRecord;
        if (!std.meta.eql(selected.root, value.lookup) or selected.ordinal != ordinal) return error.InvalidCatalogRecord;
        const lock = (try lifecycle.read(reader, value.lookup.group, table)) orelse return error.InvalidCatalogRecord;
        if (lock.kind != .table_retirement or !std.mem.eql(u8, &lock.id, &value.lookup.id)) return error.InvalidCatalogRecord;
    }
    const RangeReader = struct {
        reader: @TypeOf(reader),
        root: Lookup,
        fn read(self: @This(), range: local.common_topology_records.RangeRecord) !void {
            const selected = (try member(self.reader, self.root.group, range.table_id)) orelse return error.InvalidCatalogRecord;
            if (!std.meta.eql(selected.root, self.root)) return error.InvalidCatalogRecord;
            try requireRange(self.reader, selected, range.group_id);
        }
    };
    if (value.range_count != rangeCount(manifest)) return error.InvalidCatalogRecord;
    try forRanges(manifest, RangeReader{ .reader = reader, .root = value.lookup }, RangeReader.read);
    for (manifest.counters) |counter| try counters.require(reader, value.lookup.group, counter.resource.id, value.lease());
    for (manifest.dependent_views) |view| {
        try views.require(reader, value.lookup.group, view.id, value.lease());
        if (!std.meta.eql((try viewRoot(reader, value.lookup.group, view.id)) orelse return error.InvalidCatalogRecord, value.lookup)) return error.InvalidCatalogRecord;
    }
}
pub fn cancel(a: A, txn: anytype, lookup: Lookup, expected_revision: u64) !void {
    const before = (try head(txn, lookup)) orelse return error.GenerationPublicationChanged;
    if (before.phase == .canceled) return;
    if (before.revision != expected_revision or before.phase != .reserved) return error.GenerationPublicationChanged;
    var buffer: [256]u8 = undefined;
    var body = try manifests.Owned.parse(a, (try optional(txn, try key(&buffer, lookup, .effects))) orelse return error.InvalidCatalogRecord);
    defer body.deinit();
    const tables = try participantTables(a, body.value);
    defer a.free(tables);
    try requireReservations(txn, before, body.value, tables);
    // No physical work can start while the durable phase is reserved. The
    // complete preflight above precedes the first release; storage errors abort.
    for (tables) |table| {
        try lifecycle.release(txn, lookup.group, table, .{ .kind = .table_retirement, .id = lookup.id });
        try txn.delete(try memberKey(&buffer, lookup.group, table));
    }
    const RangeRemover = struct {
        txn: @TypeOf(txn),
        group: u64,
        fn remove(self: @This(), range: local.common_topology_records.RangeRecord) !void {
            var key_buffer: [192]u8 = undefined;
            try self.txn.delete(try rangeKey(&key_buffer, self.group, range.group_id));
        }
    };
    try forRanges(body.value, RangeRemover{ .txn = txn, .group = lookup.group }, RangeRemover.remove);
    for (body.value.counters) |counter| try counters.release(txn, lookup.group, counter.resource.id, before.lease());
    for (body.value.dependent_views) |view| {
        try views.release(txn, lookup.group, view.id, before.lease());
        try txn.delete(try viewKey(&buffer, lookup.group, view.id));
    }
    try txn.delete(try key(&buffer, lookup, .active));
    // The terminal reference/scope is enough to observe exact cancelled
    // retries. Retaining every large manifest would amplify restart/storage
    // cost after repeated reservation churn. Tombstone it in the same cut.
    try txn.delete(try key(&buffer, lookup, .effects));
    var after = before;
    after.revision += 1;
    after.phase = .canceled;
    const encoded = try HeadRecord.init(after);
    try txn.put(try key(&buffer, lookup, .head), encoded.slice());
}

pub fn authority(value: Head) !@import("table_retirement_authority.zig").Authority {
    const state = value.physical orelse return error.GenerationPublicationChanged;
    if (state.phase != .ready) return error.GenerationPublicationChanged;
    const result: @import("table_retirement_authority.zig").Authority = .{ .root = value.lookup, .effects = value.effects, .physical_plan = state.summary.plan_digest };
    try result.validate();
    return result;
}
/// Exact logical membership shared by native admission and cold checkpoint
/// verification. Historical publication checks immutable preimages, not today's
/// surviving tables: they may have evolved again after this cohort released.
pub fn requireSurvivorPlan(value: Head, manifest: manifests.Manifest, plan: anytype) !@import("table_retirement_authority.zig").Cohort {
    const root = try authority(value);
    if (plan.retirement_root == null or plan.selection != .dependent_object_retirement or
        !std.meta.eql(plan.retirement_root.?, root) or !std.meta.eql(try manifest.reference(), root.effects) or
        plan.metadata_group_id != value.lookup.group or !std.mem.eql(u8, &plan.metadata_incarnation, &value.incarnation) or
        !std.mem.eql(u8, &try manifest.selection.fingerprint(), &try plan.selection.dependent_object_retirement.fingerprint()) or
        plan.participantCount() != manifest.survivors.len or plan.owners.len != manifest.survivor_ranges.len) return error.GenerationPublicationChanged;
    for (manifest.survivors, 0..) |expected, index| {
        const actual = plan.participant(index);
        if (!@import("table_manager.zig").tableDefinitionsEqual(actual.before, expected.before) or
            !@import("table_manager.zig").tableDefinitionsEqual(actual.after, expected.after)) return error.GenerationPublicationChanged;
    }
    try physical.requireRanges(plan.owners, manifest.survivor_ranges);
    const result: @import("table_retirement_authority.zig").Cohort = .{ .table = plan.before.table_id, .id = plan.id, .digest = try plan.digest() };
    try result.validate();
    return result;
}
pub fn requireCohort(reader: anytype, expected: @import("table_retirement_authority.zig").Authority, candidate: @import("table_retirement_authority.zig").Cohort) !Head {
    const current = (try head(reader, expected.root)) orelse return error.GenerationPublicationChanged;
    if (!std.meta.eql(try authority(current), expected) or !std.meta.eql(current.survivor, @as(?@TypeOf(candidate), candidate)) or
        (current.phase != .validating_survivors and current.phase != .published)) return error.GenerationPublicationChanged;
    return current;
}
pub fn stageCohort(txn: anytype, expected: @import("table_retirement_authority.zig").Authority, candidate: @import("table_retirement_authority.zig").Cohort) !void {
    try candidate.validate();
    var current = (try head(txn, expected.root)) orelse return error.GenerationPublicationChanged;
    if (current.phase != .physical_ready or current.survivor != null or !std.meta.eql(try authority(current), expected)) return error.GenerationPublicationChanged;
    current.schema_attempts = std.math.add(u32, current.schema_attempts, 1) catch return error.CatalogGenerationExhausted;
    current.revision += 1;
    current.phase = .validating_survivors;
    current.survivor = candidate;
    const encoded = try HeadRecord.init(current);
    var buffer: [256]u8 = undefined;
    try txn.put(try key(&buffer, current.lookup, .head), encoded.slice());
}
pub fn finishCohort(txn: anytype, expected: @import("table_retirement_authority.zig").Authority, candidate: @import("table_retirement_authority.zig").Cohort, published: bool) !void {
    var current = try requireCohort(txn, expected, candidate);
    if (current.phase != .validating_survivors) return error.GenerationPublicationChanged;
    current.revision += 1;
    current.phase = if (published) .published else .physical_ready;
    if (!published) current.survivor = null;
    const encoded = try HeadRecord.init(current);
    var buffer: [256]u8 = undefined;
    try txn.put(try key(&buffer, current.lookup, .head), encoded.slice());
}
/// All physical barriers were accepted, and the native writer has staged
/// the exact effects and reservation release in this same transaction.
pub fn finishWithoutSurvivors(txn: anytype, before: Head, catalog_revision: u64) !void {
    if (before.phase != .physical_ready or before.effects.survivors != 0 or before.survivor != null or
        before.schema_attempts != 0 or catalog_revision == 0) return error.GenerationPublicationChanged;
    var current = before;
    current.phase = .published;
    current.revision += 1;
    current.direct_catalog_revision = catalog_revision;
    const encoded = try HeadRecord.init(current);
    var buffer: [256]u8 = undefined;
    try txn.put(try key(&buffer, current.lookup, .head), encoded.slice());
}
pub fn requireBorrowedLock(reader: anytype, expected: @import("table_retirement_authority.zig").Authority, candidate: @import("table_retirement_authority.zig").Cohort, table: u64) !void {
    const current = try requireCohort(reader, expected, candidate);
    const lock = (try lifecycle.read(reader, expected.root.group, table)) orelse return error.GenerationPublicationChanged;
    if (current.phase == .published) {
        if (lock.kind != .schema_validation or !std.mem.eql(u8, &lock.id, &candidate.id)) return error.GenerationPublicationChanged;
    } else {
        const selected = (try member(reader, expected.root.group, table)) orelse return error.InvalidCatalogRecord;
        if (!std.meta.eql(selected.root, expected.root) or lock.kind != .table_retirement or !std.mem.eql(u8, &lock.id, &expected.root.id)) return error.GenerationPublicationChanged;
    }
}

/// Called only after the transaction-local full reservation proof. Leases are
/// withdrawn in the same writer as dependent catalog deletion; there is no
/// externally visible unleased interval.
pub fn releaseCatalogLeases(txn: anytype, value: Head, manifest: manifests.Manifest) !void {
    var buffer: [256]u8 = undefined;
    for (manifest.counters) |counter| try counters.release(txn, value.lookup.group, counter.resource.id, value.lease());
    for (manifest.dependent_views) |view| {
        try views.release(txn, value.lookup.group, view.id, value.lease());
        try txn.delete(try viewKey(&buffer, value.lookup.group, view.id));
    }
}

/// Transfer surviving tables to the common schema-validation cleanup owner.
/// Root/parent reservations and range aliases disappear atomically with the
/// catalog postimage. A failed writer must abort this entire transition.
pub fn transferReservations(txn: anytype, value: Head, manifest: manifests.Manifest, tables: []const u64) !void {
    const direct = value.phase == .physical_ready and value.effects.survivors == 0 and value.survivor == null;
    if (!direct and (value.phase != .validating_survivors or value.survivor == null)) return error.GenerationPublicationChanged;
    if (direct and manifest.survivors.len != 0) return error.GenerationPublicationChanged;
    var buffer: [256]u8 = undefined;
    var survivor: usize = 0;
    for (tables) |table| {
        try lifecycle.release(txn, value.lookup.group, table, .{ .kind = .table_retirement, .id = value.lookup.id });
        try txn.delete(try memberKey(&buffer, value.lookup.group, table));
        if (survivor < manifest.survivors.len and manifest.survivors[survivor].before.table_id == table) {
            try lifecycle.acquire(txn, value.lookup.group, table, .{ .kind = .schema_validation, .id = value.survivor.?.id });
            survivor += 1;
        }
    }
    if (survivor != manifest.survivors.len) return error.InvalidCatalogRecord;
    const RangeRemover = struct {
        txn: @TypeOf(txn),
        group: u64,
        fn remove(self: @This(), range: local.common_topology_records.RangeRecord) !void {
            var selected: [192]u8 = undefined;
            try self.txn.delete(try rangeKey(&selected, self.group, range.group_id));
        }
    };
    try forRanges(manifest, RangeRemover{ .txn = txn, .group = value.lookup.group }, RangeRemover.remove);
    try txn.delete(try key(&buffer, value.lookup, .active));
}

pub fn stagePhysical(txn: anytype, before: Head, prepared: physical.Prepared) !void {
    var buffer: [256]u8 = undefined;
    try txn.put(try physical.key(&buffer, before.lookup, .plan, 0, .source_fenced), prepared.encoded);
    for (prepared.owners) |owner| {
        const encoded = try physical.OwnerRecord.init(owner);
        try txn.put(try physical.key(&buffer, before.lookup, .owner, owner.ordinal, .source_fenced), encoded.slice());
        inline for (std.meta.tags(physical.Step)) |step| if (owner.applies(step)) {
            try txn.put(try physical.key(&buffer, before.lookup, .work, owner.ordinal, step), &.{1});
        };
    }
    var after = before;
    after.revision += 1;
    after.phase = .retiring;
    after.physical = .{ .summary = prepared.summary };
    const encoded = try HeadRecord.init(after);
    try txn.put(try key(&buffer, before.lookup, .head), encoded.slice());
}
pub fn readPhysicalOwner(reader: anytype, value: Head, ordinal: u16) !physical.Owner {
    const state = value.physical orelse return error.InvalidCatalogRecord;
    if (ordinal >= state.summary.owner_count) return error.InvalidCatalogRecord;
    var buffer: [256]u8 = undefined;
    const owner = try physical.OwnerRecord.decode(try reader.get(try physical.key(&buffer, value.lookup, .owner, ordinal, .source_fenced)));
    try owner.validate();
    if (owner.ordinal != ordinal) return error.InvalidCatalogRecord;
    return owner;
}
pub const PhysicalAck = struct { after: Head, receipt: physical.ReceiptRecord };

/// Read-only rejection is separated from staging so a storage failure can
/// never be caught and committed as an otherwise successful no-op.
pub fn preparePhysicalAck(reader: anytype, before: Head, ordinal: u16, step: physical.Step, evidence: physical.Evidence) !?PhysicalAck {
    const state = before.physical orelse return error.InvalidCatalogRecord;
    if (ordinal >= state.summary.owner_count) return error.InvalidGenerationPublication;
    const owner = try readPhysicalOwner(reader, before, ordinal);
    const participant = (try member(reader, before.lookup.group, owner.table)) orelse return error.InvalidCatalogRecord;
    if (!std.meta.eql(participant.root, before.lookup)) return error.InvalidCatalogRecord;
    const commitment = try physical.verifyEvidence(before.lookup, state, owner, step, evidence);
    var buffer: [256]u8 = undefined;
    if (try optional(reader, try physical.key(&buffer, before.lookup, .receipt, ordinal, step))) |bytes| {
        const previous = try physical.ReceiptRecord.decode(bytes);
        if (!std.meta.eql(previous.root, before.lookup) or previous.ordinal != ordinal or previous.step != step) return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, &previous.evidence.digest(), &evidence.digest())) return error.InvalidGenerationPublication;
        return null;
    }
    // Reject a future barrier before checking its queue. All future work keys
    // are already durable, but do not authorize execution ahead of the head.
    var after = before;
    after.physical = try state.next(step, commitment);
    if (!std.mem.eql(u8, (try optional(reader, try physical.key(&buffer, before.lookup, .work, ordinal, step))) orelse return error.InvalidCatalogRecord, &.{1})) return error.InvalidCatalogRecord;
    after.phase = if (after.physical.?.phase == .ready) .physical_ready else .retiring;
    after.revision = std.math.add(u64, before.revision, 1) catch return error.CatalogGenerationExhausted;
    return .{ .after = after, .receipt = try physical.ReceiptRecord.init(.{ .root = before.lookup, .ordinal = ordinal, .step = step, .evidence = evidence }) };
}
pub fn stagePhysicalAck(txn: anytype, prepared: PhysicalAck) !void {
    const after = prepared.after;
    const receipt = try physical.ReceiptRecord.decode(prepared.receipt.slice());
    const encoded = try HeadRecord.init(after);
    var buffer: [256]u8 = undefined;
    try txn.put(try physical.key(&buffer, after.lookup, .receipt, receipt.ordinal, receipt.step), prepared.receipt.slice());
    try txn.delete(try physical.key(&buffer, after.lookup, .work, receipt.ordinal, receipt.step));
    try txn.put(try key(&buffer, after.lookup, .head), encoded.slice());
}
pub fn recordPhysical(txn: anytype, before: Head, ordinal: u16, step: physical.Step, evidence: physical.Evidence) !void {
    if (try preparePhysicalAck(txn, before, ordinal, step, evidence)) |prepared| try stagePhysicalAck(txn, prepared);
}
/// One prefix seek selects pending work for the current barrier. Completed
/// work is deleted with its receipt; restart does not rescan all old owners.
pub fn nextPhysical(reader: anytype, value: Head) !?physical.Owner {
    const state = value.physical orelse return null;
    if (state.phase == .ready) return null;
    const step: physical.Step = @fromBackingInt(@backingInt(state.phase));
    var buffer: [256]u8 = undefined;
    const prefix_key = try physical.workPrefix(&buffer, value.lookup, step);
    var cursor = try reader.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(prefix_key)) orelse return error.InvalidCatalogRecord;
    if (!std.mem.startsWith(u8, row.key, prefix_key) or !std.mem.eql(u8, row.value, &.{1})) return error.InvalidCatalogRecord;
    const selected = (try physical.address(value.lookup.group, row.key)) orelse return error.InvalidCatalogRecord;
    if (selected.kind != .work or selected.step != step or !std.meta.eql(selected.lookup, value.lookup)) return error.InvalidCatalogRecord;
    const owner = try readPhysicalOwner(reader, value, selected.ordinal);
    if (!owner.applies(step)) return error.InvalidCatalogRecord;
    return owner;
}

fn verifyPhysical(backing: A, reader: anytype, value: Head, manifest: manifests.Manifest) !u64 {
    const state = value.physical orelse return 0;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    var buffer: [256]u8 = undefined;
    const plan = try std.json.parseFromSliceLeaky(@import("fk_generation_publication.zig").Plan, a, try reader.get(try physical.key(&buffer, value.lookup, .plan, 0, .source_fenced)), .{ .allocate = .alloc_always });
    const prepared = try physical.prepare(a, backing, plan);
    try physical.requireManifest(backing, value.lookup, prepared, manifest);
    if (!std.meta.eql(prepared.summary, state.summary)) return error.InvalidCatalogRecord;
    var counts: physical.Counts = @splat(0);
    var digests: [physical.step_count][32]u8 = @splat(@splat(0));
    var expected: u64 = 1 + prepared.owners.len;
    for (prepared.owners) |owner| {
        if (!std.meta.eql(owner, try readPhysicalOwner(reader, value, owner.ordinal))) return error.InvalidCatalogRecord;
        inline for (std.meta.tags(physical.Step)) |step| {
            if (try optional(reader, try physical.key(&buffer, value.lookup, .receipt, owner.ordinal, step))) |bytes| {
                const receipt = try physical.ReceiptRecord.decode(bytes);
                if (!std.meta.eql(receipt.root, value.lookup) or receipt.ordinal != owner.ordinal or receipt.step != step) return error.InvalidCatalogRecord;
                const digest = try physical.verifyEvidence(value.lookup, state, owner, step, receipt.evidence);
                counts[@backingInt(step)] += 1;
                for (&digests[@backingInt(step)], digest) |*byte, input| byte.* ^= input;
                if (try optional(reader, try physical.key(&buffer, value.lookup, .work, owner.ordinal, step)) != null) return error.InvalidCatalogRecord;
                expected += 1;
            } else if (owner.applies(step)) {
                if (!std.mem.eql(u8, (try optional(reader, try physical.key(&buffer, value.lookup, .work, owner.ordinal, step))) orelse return error.InvalidCatalogRecord, &.{1})) return error.InvalidCatalogRecord;
                expected += 1;
            }
        }
    }
    if (!std.meta.eql(counts, state.counts) or !std.meta.eql(digests, state.receipts_xor)) return error.InvalidCatalogRecord;
    return expected;
}

/// Forward exact-key proof plus reverse cardinalities rejects omitted bodies,
/// aliases, locks and view leases. A bounded region is released for each root;
/// recovery never retains every manifest or scans all catalog rows per root.
pub fn verifyDirectory(backing: A, reader: anytype, group: u64, rows: anytype, context: anytype, canceled: ?*const std.atomic.Value(bool)) !void {
    var expected_physical: u64 = 0;
    var expected_active: u64 = 0;
    var expected_members: u64 = 0;
    var expected_ranges: u64 = 0;
    var expected_views: u64 = 0;
    var buffer: [256]u8 = undefined;
    const starts = try backing.dupe(u8, try prefix(&buffer, group));
    defer backing.free(starts);
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, starts)) continue;
        const selected = (try address(group, row.key)) orelse return error.InvalidCatalogRecord;
        if (selected.kind != .head) {
            const lookup = if (selected.kind == .active) try ActiveRecord.decode(row.value) else selected.lookup;
            const found = (try head(reader, lookup)) orelse return error.InvalidCatalogRecord;
            if (selected.kind == .active and (!found.active() or !std.mem.eql(u8, row.key, try key(&buffer, lookup, .active)))) return error.InvalidCatalogRecord;
            if (selected.kind == .effects and found.phase == .canceled) return error.InvalidCatalogRecord;
            continue;
        }
        const value = try HeadRecord.decode(row.value);
        if (!std.meta.eql(selected.lookup, value.lookup)) return error.InvalidCatalogRecord;
        try context.identity(reader, value);
        if (value.phase == .canceled) {
            if (try optional(reader, try key(&buffer, value.lookup, .effects)) != null) return error.InvalidCatalogRecord;
            continue;
        }
        var owned = try manifests.Owned.parse(backing, try reader.get(try key(&buffer, value.lookup, .effects)));
        defer owned.deinit();
        if (!std.meta.eql(value.effects, try owned.value.reference()) or !std.mem.eql(u8, &value.incarnation, &owned.value.metadata_incarnation) or value.lookup.group != owned.value.metadata_group_id) return error.InvalidCatalogRecord;
        if (!std.mem.allEqual(u8, &value.scope.target_digest, 0) and !std.mem.eql(u8, &value.scope.target_digest, &Scope.targetDigest(owned.value.guard.target.database, owned.value.guard.target.namespace, owned.value.guard.logical_table))) return error.InvalidCatalogRecord;
        const tables = try participantTables(backing, owned.value);
        defer backing.free(tables);
        if (value.participant_count != tables.len or value.range_count != rangeCount(owned.value) or !std.mem.eql(u8, &value.participants_digest, &tableDigest(tables))) return error.InvalidCatalogRecord;
        expected_physical += try verifyPhysical(backing, reader, value, owned.value);
        if (value.survivor != null) try context.verifySurvivorPlan(reader, value, owned.value);
        if (value.phase == .published) continue;
        try context.verify(reader, value, owned.value);
        try requireReservations(reader, value, owned.value, tables);
        expected_active += 1;
        expected_members += value.participant_count;
        expected_ranges += value.range_count;
        expected_views += value.effects.views;
    }
    var member_buffer: [192]u8 = undefined;
    const members_prefix = try memberPrefix(&member_buffer, group);
    var range_buffer: [192]u8 = undefined;
    const ranges_prefix = try rangePrefix(&range_buffer, group);
    var view_buffer: [192]u8 = undefined;
    const views_prefix = try views.prefix(&view_buffer, group);
    var alias_buffer: [192]u8 = undefined;
    const aliases_prefix = try viewPrefix(&alias_buffer, group);
    var aliases_count: u64 = 0;
    var physical_count: u64 = 0;
    var active_count: u64 = 0;
    var members_count: u64 = 0;
    var ranges_count: u64 = 0;
    var views_count: u64 = 0;
    var locks_count: u64 = 0;
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (std.mem.startsWith(u8, row.key, starts)) {
            if ((try address(group, row.key)).?.kind == .active) active_count += 1;
        } else if (std.mem.startsWith(u8, row.key, members_prefix)) {
            const selected = try MemberRecord.decode(row.value);
            if (selected.root.group != group or !std.mem.eql(u8, row.key, try memberKey(&buffer, group, selected.table))) return error.InvalidCatalogRecord;
            members_count += 1;
        } else if (std.mem.startsWith(u8, row.key, ranges_prefix)) {
            const selected = try RangeRecord.decode(row.value);
            if (selected.root.group != group or !std.mem.eql(u8, row.key, try rangeKey(&buffer, group, selected.group))) return error.InvalidCatalogRecord;
            ranges_count += 1;
        } else if (std.mem.startsWith(u8, row.key, aliases_prefix)) {
            const value = try ViewAliasRecord.decode(row.value);
            if (value.root.group != group or !std.mem.eql(u8, row.key, try viewKey(&buffer, group, value.view))) return error.InvalidCatalogRecord;
            aliases_count += 1;
        } else if (std.mem.startsWith(u8, row.key, views_prefix)) {
            _ = try views.parse(group, row.key, row.value);
            views_count += 1;
        } else if (try lifecycle.groupFromKey(row.key)) |lock_group| {
            const lock = try lifecycle.Lock.decode(row.value);
            if (lock_group == group and lock.kind == .table_retirement) {
                const table = (try lifecycle.tableFromKey(row.key)).?;
                const selected = (try member(reader, group, table)) orelse return error.InvalidCatalogRecord;
                if (!std.mem.eql(u8, &selected.root.id, &lock.id)) return error.InvalidCatalogRecord;
                locks_count += 1;
            }
        }
        if (try physical.address(group, row.key)) |selected| {
            const found = (try head(reader, selected.lookup)) orelse return error.InvalidCatalogRecord;
            if ((!found.active() and found.phase != .published) or found.physical == null) return error.InvalidCatalogRecord;
            physical_count += 1;
        }
        if (expected_active != 0) try context.inspectRow(reader, group, row);
    }
    if (physical_count != expected_physical) return error.InvalidCatalogRecord;
    if (active_count != expected_active or members_count != expected_members or locks_count != expected_members or ranges_count != expected_ranges or views_count != expected_views or aliases_count != expected_views) return error.InvalidCatalogRecord;
}

test "table retirement manifest input owns grown arenas and is atomic at every allocation failure" {
    const a = std.testing.allocator;
    const schema = "{\"version\":1}";
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema, &digest, .{});
    const large_description: [32768]u8 = @splat('x');
    const manifest: manifests.Manifest = .{
        .metadata_group_id = 41,
        .metadata_incarnation = @splat('1'),
        .guard = .{ .target = .{ .name = "roots" }, .logical_table = "roots", .owner = .{ .table_id = 7, .schema_version = 1, .schema_digest = digest, .kind = .table }, .incarnation = @splat(0x11) },
        .root = .{ .table_id = 7, .name = "roots", .schema_json = schema, .description = &large_description },
        .root_ranges = &.{},
        .incoming = .{},
        .selection = .{ .relations = &.{.{ .table_id = 7, .physical_name = "roots" }}, .sequences = &.{} },
        .counters = &.{},
        .survivors = &.{},
        .dependent_views = &.{},
        .view_backings = &.{},
        .survivor_ranges = &.{},
        .outgoing_parents = &.{},
        .external_parents = &.{},
        .view_edges = &.{},
    };
    const bytes = try (Command{ .begin = .{ .id = @splat(1), .scope = .{ .database = 1, .namespace = 2, .creator = .bootstrap }, .manifest = manifest } }).encodeAlloc(a);
    defer a.free(bytes);
    const Faults = struct {
        fn run(backing: A, encoded: []const u8) !void {
            var result = try Input.parse(backing, encoded);
            defer result.deinit();
            try std.testing.expectEqual(@as(u16, 1), result.prepared.?.head.participant_count);
        }
    };
    try Faults.run(a, bytes);
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Faults.run, .{bytes});
}
