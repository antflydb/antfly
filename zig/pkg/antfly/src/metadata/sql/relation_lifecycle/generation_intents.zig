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

//! Transaction-private relation definitions. They are inputs to the existing
//! catalog participant, never publication authority or initialized owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const A = std.mem.Allocator;
pub const max_entries = 128;
pub const max_bytes = 8 << 20;

pub const Entry = struct {
    transaction: [16]u8,
    nonce: [16]u8,
    generation: u64,
    logical_relation_id: u64,
    physical: [16]u8,
    birth: local.storage_pessimistic_lock_authority.CommandFence,
    birth_subtransaction: u64,
    subtransaction: u64,
    freeze_eligible: bool = true,
    operation: enum { create, truncate },
    target: domain.Target,
    /// Canonical typed preparation, encoded once. Session cloning copies bytes
    /// instead of reparsing schemas and placement for every SQL statement.
    preparation: []const u8,

    pub fn prepared(self: Entry, a: A) !@import("../../relation_create.zig").Prepared {
        return std.json.parseFromSliceLeaky(@import("../../relation_create.zig").Prepared, a, self.preparation, .{ .allocate = .alloc_always });
    }
    pub fn validate(self: Entry, a: A) !void {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        _ = try self.validatePrepared(scratch.allocator());
    }

    fn validatePrepared(self: Entry, a: A) !@import("../../relation_create.zig").Prepared {
        try self.target.validate();
        try self.birth.validate();
        if (std.mem.allEqual(u8, &self.transaction, 0) or std.mem.allEqual(u8, &self.nonce, 0) or
            self.subtransaction > self.birth_subtransaction or self.preparation.len == 0 or self.preparation.len > max_bytes)
            return error.InvalidTransactionSessionRecord;
        const value = try self.prepared(a);
        if (value.catalog_id == 0 or value.namespace_id == 0 or value.catalog_id != self.logical_relation_id or value.child.table_id != self.generation or self.generation == 0 or !@import("../../relation_create.zig").validPhysicalName(value.child.name) or
            value.child_ranges.len == 0 or (self.operation == .create and value.creation_guard == null) or value.child.sql_materialized_view_id != null) return error.InvalidTransactionSessionRecord;
        if (!std.mem.eql(u8, &value.metadata_incarnation, &std.fmt.bytesToHex(self.birth.backend.incarnation, .lower)) or
            (value.creation_guard != null and value.creation_guard.?.tablespace_id != value.tablespace_id)) return error.InvalidTransactionSessionRecord;
        if (self.generation != try @import("../../relation_create.zig").privateTableId(value.child.name)) return error.InvalidTransactionSessionRecord;
        var physical: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&physical, value.child.name[6..]) catch return error.InvalidTransactionSessionRecord;
        if (!std.mem.eql(u8, &physical, &self.physical)) return error.InvalidTransactionSessionRecord;
        try @import("../../table_manager.zig").validateCompleteKeyspaceRanges(value.child_ranges);
        for (value.child_ranges) |range| if (range.table_id != value.child.table_id) return error.InvalidTransactionSessionRecord;
        if (self.operation == .create and value.previous != null) return error.InvalidTransactionSessionRecord;
        if (value.previous) |before| {
            if (self.operation != .truncate or value.creation_guard != null or before.table.table_id == 0 or
                before.table.table_id == value.child.table_id or before.resource.kind != .table or
                before.resource.id != before.table.table_id or before.resource.effectiveLogicalRelationId() != value.catalog_id or
                before.resource.parent_id != value.namespace_id or before.resource.tablespace_id != value.tablespace_id or
                !std.mem.eql(u8, before.resource.name, self.target.table) or
                !std.mem.eql(u8, before.resource.storage_name, before.table.name))
                return error.InvalidTransactionSessionRecord;
            try @import("../../table_manager.zig").validateCompleteKeyspaceRanges(before.ranges);
            for (before.ranges) |range| if (range.table_id != before.table.table_id) return error.InvalidTransactionSessionRecord;
        }
        return value;
    }
    pub fn sameTarget(self: Entry, target: domain.Target) bool {
        return std.mem.eql(u8, self.target.database, target.database) and std.mem.eql(u8, self.target.namespace, target.namespace) and std.mem.eql(u8, self.target.table, target.table);
    }
    /// RELEASE may reparent ownership; every other declaration byte remains
    /// immutable while the same nonce survives a native session CAS.
    pub fn sameDeclaration(self: Entry, other: Entry) bool {
        return std.mem.eql(u8, &self.transaction, &other.transaction) and std.mem.eql(u8, &self.nonce, &other.nonce) and
            self.generation == other.generation and self.logical_relation_id == other.logical_relation_id and
            std.mem.eql(u8, &self.physical, &other.physical) and std.meta.eql(self.birth, other.birth) and
            self.birth_subtransaction == other.birth_subtransaction and self.operation == other.operation and
            self.sameTarget(other.target) and std.mem.eql(u8, self.preparation, other.preparation);
    }
    fn bytes(self: Entry) usize {
        return self.target.database.len +| self.target.namespace.len +| self.target.table.len +| self.preparation.len;
    }
    pub fn clone(self: Entry, a: A) !Entry {
        var out = self;
        out.target.database = try a.dupe(u8, self.target.database);
        errdefer a.free(out.target.database);
        out.target.namespace = try a.dupe(u8, self.target.namespace);
        errdefer a.free(out.target.namespace);
        out.target.table = try a.dupe(u8, self.target.table);
        errdefer a.free(out.target.table);
        out.preparation = try a.dupe(u8, self.preparation);
        return out;
    }
    fn deinit(self: Entry, a: A) void {
        a.free(self.target.database);
        a.free(self.target.namespace);
        a.free(self.target.table);
        a.free(self.preparation);
    }
    pub fn jsonStringify(self: Entry, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Entry).@"struct".field_names) |field| {
            try writer.objectField(field);
            if (comptime std.mem.eql(u8, field, "preparation")) {
                // A real control object keeps independent decoder declarations
                // visible to native admission and cold/checkpoint projection.
                try writer.print("{s}", .{self.preparation});
            } else try writer.write(@field(self, field));
        }
        try writer.endObject();
    }
    const Wire = struct {
        transaction: [16]u8,
        nonce: [16]u8,
        generation: u64,
        logical_relation_id: u64,
        physical: [16]u8,
        birth: local.storage_pessimistic_lock_authority.CommandFence,
        birth_subtransaction: u64,
        subtransaction: u64,
        freeze_eligible: bool = true,
        operation: @FieldType(Entry, "operation"),
        target: domain.Target,
        preparation: @import("../../relation_create.zig").Prepared,
    };
    fn fromWire(a: A, wire: Wire) !Entry {
        return .{ .transaction = wire.transaction, .nonce = wire.nonce, .generation = wire.generation, .logical_relation_id = wire.logical_relation_id, .physical = wire.physical, .birth = wire.birth, .birth_subtransaction = wire.birth_subtransaction, .subtransaction = wire.subtransaction, .freeze_eligible = wire.freeze_eligible, .operation = wire.operation, .target = wire.target, .preparation = try std.json.Stringify.valueAlloc(a, wire.preparation, .{}) };
    }
    pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) !Entry {
        var strict = options;
        strict.ignore_unknown_fields = false;
        return fromWire(a, try std.json.innerParse(Wire, a, source, strict));
    }
    pub fn jsonParseFromValue(a: A, value: std.json.Value, options: std.json.ParseOptions) !Entry {
        var strict = options;
        strict.ignore_unknown_fields = false;
        return fromWire(a, try std.json.innerParseFromValue(Wire, a, value, strict));
    }
};

pub const Pending = struct {
    entries: std.ArrayList(Entry) = .empty,
    retained_bytes: usize = 0,

    pub fn deinit(self: *Pending, a: A) void {
        self.truncate(a, 0);
        self.entries.deinit(a);
        self.* = .{};
    }
    pub fn truncate(self: *Pending, a: A, count: usize) void {
        std.debug.assert(count <= self.entries.items.len);
        for (self.entries.items[count..]) |entry| {
            self.retained_bytes -= entry.bytes();
            entry.deinit(a);
        }
        self.entries.shrinkRetainingCapacity(count);
    }
    /// PostgreSQL restores the predecessor image after rollback, while a
    /// rolled-back replacement invalidates its FREEZE eligibility.
    pub fn rollback(self: *Pending, a: A, count: usize) !void {
        if (count > self.entries.items.len) return error.InvalidTransactionSessionRecord;
        for (self.entries.items[count..]) |removed| {
            var cursor = count;
            while (cursor != 0) {
                cursor -= 1;
                const retained = &self.entries.items[cursor];
                if (!retained.sameTarget(removed.target)) continue;
                retained.freeze_eligible = false;
                break;
            }
        }
        self.truncate(a, count);
    }

    pub fn latest(self: *const Pending, target: domain.Target) ?Entry {
        var cursor = self.entries.items.len;
        while (cursor != 0) {
            cursor -= 1;
            const entry = self.entries.items[cursor];
            if (entry.sameTarget(target)) return entry;
        }
        return null;
    }
    pub fn requireFreeze(self: *const Pending, target: domain.Target, subtransaction: u64, retained_cursor: bool) !Entry {
        // PostgreSQL checks retained cursor ownership independently of the
        // transaction-level isolation snapshot; prior ordinary reads are legal.
        if (retained_cursor) return error.SqlCopyFreezeActiveCursor;
        const entry = self.latest(target) orelse return error.SqlCopyFreezeGenerationRequired;
        if (entry.subtransaction == subtransaction and entry.freeze_eligible) return entry;
        // CREATE owns the logical relation birth independently of its
        // replacement file. A child TRUNCATE rollback invalidates the
        // predecessor's truncate witness but preserves the creation witness.
        for (self.entries.items) |declaration| {
            if (declaration.operation == .create and declaration.subtransaction == subtransaction and declaration.sameTarget(target)) return entry;
        }
        return error.SqlCopyFreezeGenerationRequired;
    }
    pub fn release(self: *Pending, first_child: u64, parent: u64) !void {
        if (first_child == 0 or parent >= first_child) return error.InvalidTransactionSessionRecord;
        for (self.entries.items) |*entry| if (entry.subtransaction >= first_child) {
            entry.subtransaction = parent;
        };
    }
    pub fn append(self: *Pending, a: A, entry: Entry) !void {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const arena = scratch.allocator();
        const declared = try entry.validatePrepared(arena);
        var origin: ?Entry = null;
        for (self.entries.items) |previous| {
            if (!std.mem.eql(u8, &previous.transaction, &entry.transaction) or !previous.birth.backend.eql(entry.birth.backend) or
                previous.birth.database != entry.birth.database or previous.birth.group != entry.birth.group or
                previous.birth.standalone != entry.birth.standalone or !std.mem.eql(u8, &previous.birth.principal, &entry.birth.principal))
                return error.InvalidTransactionSessionRecord;
            if (std.mem.eql(u8, &previous.nonce, &entry.nonce)) {
                if (previous.sameDeclaration(entry) and previous.subtransaction == entry.subtransaction and previous.freeze_eligible == entry.freeze_eligible) return;
                return error.InvalidTransactionSessionRecord;
            }
            if (previous.generation == entry.generation or std.mem.eql(u8, &previous.physical, &entry.physical)) return error.InvalidTransactionSessionRecord;
            if (previous.sameTarget(entry.target)) {
                if (origin == null) origin = previous;
            } else if (previous.logical_relation_id == entry.logical_relation_id) return error.InvalidTransactionSessionRecord;
        }
        switch (entry.operation) {
            .create => if (origin != null) return error.CatalogAlreadyExists,
            .truncate => if (origin) |first| {
                const initial = try first.prepared(arena);
                // A second truncate discards a private image. Its authority
                // and published predecessor are still those of the original
                // declaration, including after release or cold session load.
                if (declared.catalog_id != initial.catalog_id or declared.namespace_id != initial.namespace_id or
                    declared.tablespace_id != initial.tablespace_id or
                    !std.mem.eql(u8, &hashValue(declared.previous), &hashValue(initial.previous)))
                    return error.InvalidTransactionSessionRecord;
            } else if (declared.previous == null) return error.InvalidTransactionSessionRecord,
        }
        try self.appendOwned(a, entry);
    }
    fn appendOwned(self: *Pending, a: A, entry: Entry) !void {
        if (self.entries.items.len >= max_entries or entry.bytes() > max_bytes -| self.retained_bytes) return error.SessionRecordTooLarge;
        try self.entries.ensureUnusedCapacity(a, 1);
        const owned = try entry.clone(a);
        self.entries.appendAssumeCapacity(owned);
        self.retained_bytes += entry.bytes();
    }
    pub fn clone(self: Pending, a: A) !Pending {
        var result: Pending = .{};
        errdefer result.deinit(a);
        for (self.entries.items) |entry| try result.appendOwned(a, entry);
        return result;
    }
    pub fn digest(self: Pending) [32]u8 {
        return hashValue(self);
    }

    /// Join every final target to the declaration actually retained by SQL.
    /// Intermediate private generations are savepoint history, never extra
    /// public tables or old native owners to fence during publication.
    pub fn ownsPhysical(self: Pending, name: []const u8) bool {
        if (!@import("../../relation_create.zig").validPhysicalName(name)) return false;
        var id: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, name[6..]) catch return false;
        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, &entry.physical, &id)) return true;
        }
        return false;
    }

    pub fn requireTargets(self: *const Pending, a: A, targets: []const @import("../../restore_staging.zig").Target) !void {
        var expected: usize = 0;
        for (self.entries.items, 0..) |entry, ordinal| {
            const superseded = for (self.entries.items[ordinal + 1 ..]) |later| {
                if (later.sameTarget(entry.target)) break true;
            } else false;
            if (superseded) continue;
            expected += 1;
            const target = for (targets) |candidate| {
                if (candidate.table.table_id == entry.generation) break candidate;
            } else return error.InvalidCatalogMutation;
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const prepared = try entry.prepared(scratch.allocator());
            const first = for (self.entries.items[0 .. ordinal + 1]) |origin| {
                if (origin.sameTarget(entry.target)) break origin;
            } else unreachable;
            const original = try first.prepared(scratch.allocator());
            const binding = target.catalog_binding orelse return error.InvalidCatalogMutation;
            if (binding.kind != .table or binding.id != entry.generation or binding.parent_id != original.namespace_id or binding.tablespace_id != original.tablespace_id or binding.effectiveLogicalRelationId() != original.catalog_id or
                target.materialized_view != null or
                !std.mem.eql(u8, binding.name, entry.target.table) or !std.mem.eql(u8, binding.storage_name, prepared.child.name) or
                !std.mem.eql(u8, &hashValue(target.table), &hashValue(prepared.child)) or
                !std.mem.eql(u8, &hashValue(target.ranges), &hashValue(prepared.child_ranges)) or
                !std.meta.eql(target.creation_guard, original.creation_guard)) return error.InvalidCatalogMutation;
            const capture = target.population orelse return error.InvalidCatalogMutation;
            const rows = capture.transaction_rows orelse return error.InvalidCatalogMutation;
            try capture.validate();
            if (rows.image != .current or !std.mem.eql(u8, &rows.transaction, &entry.transaction) or
                !std.mem.eql(u8, &rows.generation_nonce, &entry.nonce) or rows.partitions.len != target.ranges.len or
                !std.mem.eql(u8, &try rows.digest(), &capture.definition)) return error.InvalidCatalogMutation;
            const placement = @import("../../table_manager.zig");
            for (target.ranges) |range| {
                const shard = placement.rangeDocIdentityShardId(range);
                try rows.forPartition(shard).validate();
            }
            const staging = @import("../../restore_staging.zig");
            if (!std.mem.eql(u8, &capture.target_schema, &try staging.runtimeSchemaDigestForTable(scratch.allocator(), prepared.child.schema_json)) or
                !std.mem.eql(u8, &capture.definition, &capture.source_definition)) return error.InvalidCatalogMutation;
            if (target.frozen_population) |frozen| {
                const schema = try @import("frozen_schema.zig").derive(scratch.allocator(), prepared.child.schema_json);
                if (!std.mem.eql(u8, schema, frozen.schema_json) or
                    !std.mem.eql(u8, &frozen.capture.target_schema, &try staging.runtimeSchemaDigestForTable(scratch.allocator(), schema)) or
                    !std.mem.eql(u8, &frozen.capture.source_definition, &@import("frozen_schema.zig").sourceDefinition(frozen.capture.definition, schema)))
                    return error.InvalidCatalogMutation;
                const historical = frozen.capture.transaction_rows orelse return error.InvalidCatalogMutation;
                try frozen.capture.validate();
                if (historical.image != .frozen or historical.source_rows == 0 or
                    !std.mem.eql(u8, &historical.transaction, &entry.transaction) or
                    !std.mem.eql(u8, &historical.generation_nonce, &entry.nonce) or
                    historical.partitions.len != target.ranges.len or
                    !std.mem.eql(u8, &try historical.digest(), &frozen.capture.definition))
                    return error.InvalidCatalogMutation;
                for (target.ranges) |range| try historical.forPartition(placement.rangeDocIdentityShardId(range)).validate();
            }
            if (original.previous) |before| {
                const replace = target.replace orelse return error.InvalidCatalogMutation;
                if (target.source_table_id != before.table.table_id or
                    !try replacementBindingMatches(before.resource, binding, prepared.child) or
                    !std.mem.eql(u8, &hashValue(replace.table), &hashValue(before.table)) or
                    !std.mem.eql(u8, &hashValue(replace.ranges), &hashValue(before.ranges))) return error.InvalidCatalogMutation;
            } else {
                const expected_binding: domain.Resource = .{
                    .kind = .table,
                    .id = entry.generation,
                    .sql_relation_id = original.catalog_id,
                    .parent_id = original.namespace_id,
                    .tablespace_id = original.tablespace_id,
                    .name = entry.target.table,
                    .storage_name = prepared.child.name,
                    .sql_owner = original.sql_owner,
                };
                if (target.replace != null or target.source_table_id != 0 or
                    !std.mem.eql(u8, &hashValue(binding), &hashValue(expected_binding))) return error.InvalidCatalogMutation;
            }
        }
        if (expected == 0 or expected != targets.len) return error.InvalidCatalogMutation;
    }

    pub fn jsonStringify(self: Pending, writer: anytype) !void {
        try writer.write(self.entries.items);
    }
    fn fromEntries(a: A, entries: []const Entry) !Pending {
        var result: Pending = .{};
        errdefer result.deinit(a);
        for (entries) |entry| {
            const before = result.entries.items.len;
            result.append(a, entry) catch |err| return if (err == error.OutOfMemory) error.OutOfMemory else error.UnexpectedToken;
            if (result.entries.items.len != before + 1) return error.UnexpectedToken;
        }
        return result;
    }
    pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) !Pending {
        return fromEntries(a, try std.json.innerParse([]const Entry, a, source, options));
    }
    pub fn jsonParseFromValue(a: A, value: std.json.Value, options: std.json.ParseOptions) !Pending {
        return fromEntries(a, try std.json.innerParseFromValue([]const Entry, a, value, options));
    }
};

pub fn testingEntry(a: A, transaction: [16]u8, ordinal: u8, subtransaction: u64) !Entry {
    if (!@import("builtin").is_test) @compileError("test fixture");
    var physical: [16]u8 = @splat(0);
    physical[15] = ordinal;
    const name = try std.mem.concat(a, u8, &.{ "table:", &std.fmt.bytesToHex(physical, .lower) });
    defer a.free(name);
    const prep: @import("../../relation_create.zig").Prepared = .{
        .catalog_id = 501,
        .namespace_id = 2,
        .expected_catalog_revision = 7,
        .tablespace_id = 0,
        .min_ranges_explicit = false,
        .child = .{ .table_id = try @import("../../relation_create.zig").privateTableId(name), .name = name, .schema_json = "{}" },
        .child_ranges = &.{.{ .table_id = try @import("../../relation_create.zig").privateTableId(name), .group_id = 700 + @as(u64, ordinal), .start_key = "" }},
        .creation_guard = .{ .database = @splat(1), .namespace = @splat(2), .tablespace_id = 0, .tablespace = null },
        .transition_generation = 1,
        .metadata_incarnation = std.fmt.bytesToHex(@as([16]u8, @splat(1)), .lower),
        .metadata_epoch = 1,
    };
    return .{
        .transaction = transaction,
        .nonce = @splat(ordinal),
        .generation = prep.child.table_id,
        .logical_relation_id = prep.catalog_id,
        .physical = physical,
        .birth = .{ .group = 1, .backend = .{ .incarnation = @splat(1), .id = @splat(2), .fence = 1 }, .command = .{ .sequence = ordinal, .nonce = @splat(3), .executor = @splat(4) }, .database = 1, .principal = @splat(5), .standalone = true },
        .birth_subtransaction = subtransaction,
        .subtransaction = subtransaction,
        .operation = if (ordinal == 1) .create else .truncate,
        .target = .{ .database = "app", .namespace = "public", .table = "rows" },
        .preparation = try std.json.Stringify.valueAlloc(a, prep, .{}),
    };
}

fn replacementBindingMatches(before: domain.Resource, binding: domain.Resource, child: local.common_topology_records.TableRecord) !bool {
    var successor = try before.withPhysicalRelationId(child.table_id);
    successor.storage_name = child.name;
    return std.mem.eql(u8, &hashValue(binding), &hashValue(successor));
}

fn hashValue(value: anytype) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.sql.generation-intents.v1\x00");
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hash, &.{});
    std.json.Stringify.value(value, .{}, &writer.writer) catch unreachable;
    var result: [32]u8 = undefined;
    writer.hasher.final(&result);
    return result;
}

/// Check the durable savepoint frontier under the same metadata read cut that
/// validates the command owner. Skips row payloads rather than hydrating them.
pub fn requireActivePreparation(a: A, txn: anytype, group: u64, transaction: [16]u8, expected: u64) !u64 {
    const records = @import("../../transaction_session_record.zig");
    var key_name: [records.session_prefix.len + 32]u8 = undefined;
    @memcpy(key_name[0..records.session_prefix.len], records.session_prefix);
    key_name[records.session_prefix.len..][0..32].* = std.fmt.bytesToHex(transaction, .lower);
    const key = try records.keyAlloc(a, group, &key_name);
    defer a.free(key);
    const raw = txn.get(key) catch |err| return if (err == error.NotFound) error.SqlTransactionNotActive else err;
    const Header = struct {
        sql: ?struct {
            failed: bool = false,
            mode: enum { read_write, read_only } = .read_write,
            catalog_generations: []const struct { logical_relation_id: u64 } = &.{},
        } = null,
        next_savepoint_id: u64,
        savepoints: []const struct { id: u64 } = &.{},
        volatile_payload: ?struct {} = null,
        commit_execution_started: bool = false,
        terminal_abort_reason: ?[]const u8 = null,
    };
    const parsed = try std.json.parseFromSlice(Header, a, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const header = parsed.value;
    const sql = header.sql orelse return error.SqlTransactionNotActive;
    if (sql.failed or header.terminal_abort_reason != null) return error.SqlTransactionAborted;
    if (sql.mode == .read_only) return error.SqlReadOnlyTransaction;
    if (header.commit_execution_started) return error.TransactionCommitSealed;
    if (header.volatile_payload != null) return error.SqlRelationRecoveryIncomplete;
    var frontier: u64 = 0;
    for (header.savepoints) |point| frontier = @max(frontier, point.id);
    if (header.next_savepoint_id == 0 or frontier >= header.next_savepoint_id or frontier != expected)
        return error.InvalidTransactionSessionRecord;
    if (sql.catalog_generations.len > max_entries) return error.InvalidTransactionSessionRecord;
    var oid_floor: u64 = 1;
    for (sql.catalog_generations) |entry| {
        try domain.requireRelationOid(entry.logical_relation_id);
        oid_floor = @max(oid_floor, entry.logical_relation_id + 1);
    }
    return oid_floor;
}

test "SQL generation intents preparation requires the live writable savepoint frontier" {
    const Fake = struct {
        raw: []const u8,
        fn get(self: @This(), _: []const u8) ![]const u8 {
            return self.raw;
        }
    };
    const a = std.testing.allocator;
    const id: [16]u8 = @splat(21);
    const active: Fake = .{ .raw = "{\"sql\":{},\"next_savepoint_id\":7,\"savepoints\":[{\"id\":3},{\"id\":5}]}" };
    try std.testing.expectEqual(@as(u64, 1), try requireActivePreparation(a, active, 1, id, 5));
    const pending: Fake = .{ .raw = "{\"sql\":{\"catalog_generations\":[{\"logical_relation_id\":42}]},\"next_savepoint_id\":1}" };
    try std.testing.expectEqual(@as(u64, 43), try requireActivePreparation(a, pending, 1, id, 0));
    try std.testing.expectError(error.InvalidTransactionSessionRecord, requireActivePreparation(a, active, 1, id, 3));
    const read_only: Fake = .{ .raw = "{\"sql\":{\"mode\":\"read_only\"},\"next_savepoint_id\":1}" };
    try std.testing.expectError(error.SqlReadOnlyTransaction, requireActivePreparation(a, read_only, 1, id, 0));
    const sealed: Fake = .{ .raw = "{\"sql\":{},\"next_savepoint_id\":1,\"commit_execution_started\":true}" };
    try std.testing.expectError(error.TransactionCommitSealed, requireActivePreparation(a, sealed, 1, id, 0));
    const compact: Fake = .{ .raw = "{\"sql\":{},\"next_savepoint_id\":7,\"volatile_payload\":{\"format_version\":1}}" };
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, requireActivePreparation(a, compact, 1, id, 0));
}
