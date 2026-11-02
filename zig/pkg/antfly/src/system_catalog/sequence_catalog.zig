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

//! Bound sequence intent for the existing native metadata transaction owner.
//! A namespace ID and catalog incarnation fence name reuse without requiring
//! the global catalog revision to remain idle during unrelated table changes.
const std = @import("std");
const sources = @import("antfly_local_sources");
pub const sequence = sources.system_catalog_sequences;
pub const journal = sources.system_catalog_sequence_journal;
const domain = sources.system_catalog_domain;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Identity = outcomes.Identity;
pub const Result = outcomes.Result;
pub const Admission = outcomes.Admission;
pub const Outcome = outcomes.Outcome;

/// Private allocator intent. Scope and immutable identity are resolved by the
/// SQL owner; callers cannot turn a stale name into a new sequence allocation.
pub const CounterRequest = struct {
    target: domain.Target,
    namespace_id: u64,
    sequence_id: u64,
    incarnation: [16]u8,
    request: journal.Request,

    pub fn validate(self: @This()) !void {
        try self.target.validate();
        if (self.namespace_id == 0 or self.sequence_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSequenceRequest;
        try self.request.validate();
    }
};

pub const CounterOutcome = union(enum) {
    success: journal.Receipt,
    rejected: outcomes.Rejection,

    pub fn fromError(err: anyerror) ?CounterOutcome {
        return .{ .rejected = std.meta.stringToEnum(outcomes.Rejection, @errorName(err)) orelse return null };
    }
    pub fn result(self: @This()) anyerror!journal.Receipt {
        return switch (self) {
            .success => |value| value,
            .rejected => |reason| switch (reason) {
                inline else => |tag| @field(anyerror, @tagName(tag)),
            },
        };
    }
};

pub const CounterAdmission = union(enum) {
    ready,
    applied: journal.Receipt,
    rejected: outcomes.Rejection,
    unknown,
};

pub const max_lookup_targets = 64;
pub const SnapshotRequest = struct {
    targets: []const domain.Target,
    /// Early-bound SQL names retain their native identities across renames.
    /// The source incarnation prevents an old prepared reference from binding
    /// a different restored catalog which reused the same numeric IDs.
    sequence_ids: ?[]const u64 = null,
    incarnation: ?[16]u8 = null,
    include_counters: bool = false,
    /// Capture the bounded dependency/physical-owner cut for coordinated DROP.
    prepare_retirement: bool = false,

    /// Ingress has already authorized this exact table. Metadata rechecks
    /// its immutable owner/schema guard while resolving the attribute name.
    owner: ?struct { guard: domain.RelationMutationGuard, column: []const u8 } = null,
    /// Row preparation pins its actual physical table/epoch and field name.
    /// The returned attribute identity must independently match the reference.
    column_default: ?struct { scope: sources.system_catalog_column_sequences.Scope, column: []const u8 } = null,

    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "prepare_retirement")) or self.prepare_retirement) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn validate(self: @This()) !void {
        if (self.targets.len == 0 or self.targets.len > max_lookup_targets) return error.InvalidCatalogMutation;
        if (self.prepare_retirement and (self.targets.len != 1 or self.owner != null or self.column_default != null)) return error.InvalidCatalogMutation;
        for (self.targets) |target| try target.validate();
        if ((self.sequence_ids == null) != (self.incarnation == null)) return error.InvalidCatalogMutation;
        if (self.sequence_ids) |ids| {
            if (ids.len != self.targets.len or self.owner != null or std.mem.allEqual(u8, &self.incarnation.?, 0)) return error.InvalidCatalogMutation;
            for (ids) |id| if (id == 0) return error.InvalidCatalogMutation;
        }
        if (self.column_default) |value| {
            if (self.owner != null or self.sequence_ids == null or self.targets.len != 1 or value.scope.table_id == 0 or
                value.scope.storage_name.len == 0 or value.scope.storage_name.len > 1024 or
                !std.unicode.utf8ValidateSlice(value.scope.storage_name) or std.mem.indexOfScalar(u8, value.scope.storage_name, 0) != null)
                return error.InvalidCatalogMutation;
            try domain.validateTableName(value.column);
        }
        if (self.owner) |value| {
            try value.guard.validate();
            if (value.guard.owner.kind != .table or value.guard.constraint_name_admission != null) return error.InvalidCatalogMutation;
            try domain.validateTableName(value.column);
        }
    }
};
pub const Snapshot = struct {
    pub const Entry = struct {
        target: domain.Target,
        namespace_id: ?u64,
        value: union(enum) { missing_database, missing_namespace, absent, other, sequence: sequence.Descriptor },
        counter: ?sequence.State = null,
    };
    pub const Owner = struct {
        namespace_id: u64,
        column: sequence.OwnedBy,
        /// Only capability-70 column-default requests return this native name.
        table_name: ?[]const u8 = null,
        /// Attested from the actual stored default, never caller-supplied mode.
        default_binding_fingerprint: ?[32]u8 = null,

        pub fn requireDefaultBinding(self: @This(), binding: sources.system_catalog_column_sequences.Binding) !void {
            const actual = self.default_binding_fingerprint orelse return error.InvalidSqlBackendResponse;
            const expected = binding.fingerprint();
            if (!std.meta.eql(self.column, binding.owner) or !std.mem.eql(u8, &actual, &expected))
                return error.CatalogGenerationChanged;
        }

        pub fn jsonStringify(self: @This(), writer: anytype) !void {
            if (self.default_binding_fingerprint) |fingerprint| return writer.write(.{ .namespace_id = self.namespace_id, .column = self.column, .table_name = self.table_name, .default_binding_fingerprint = fingerprint });
            if (self.table_name) |name|
                try writer.write(.{ .namespace_id = self.namespace_id, .column = self.column, .table_name = name })
            else
                try writer.write(.{ .namespace_id = self.namespace_id, .column = self.column });
        }
    };
    pub const Retirement = struct {
        database_id: u64,
        references: @import("sequence_default_dependencies.zig").Stamp,
        dependents: []const sources.common_topology_records.TableRecord,
        activation: ?@import("../metadata/schema_validation_publication.zig").OwnerCut = null,
    };
    retirement: ?Retirement = null,
    metadata_group_id: u64 = 0,
    revision: u64,
    head: u64,
    incarnation: [16]u8,
    entries: []const Entry,
    /// Null means no requested owner, or no such attribute of the guarded
    /// table. The request distinguishes these; neither mints an identity.
    owner: ?Owner,

    pub const max_retirement_bytes = 16 * 1024 * 1024;
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (!(comptime std.mem.eql(u8, field, "retirement")) or self.retirement != null) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn validate(self: Snapshot, request: SnapshotRequest) !void {
        try request.validate();
        if (self.entries.len != request.targets.len or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSqlBackendResponse;
        if (request.incarnation) |expected| if (!std.mem.eql(u8, &expected, &self.incarnation)) return error.CatalogGenerationChanged;
        for (self.entries, request.targets, 0..) |entry, target, ordinal| {
            try entry.target.validate();
            if (request.sequence_ids) |ids| {
                if (entry.value != .sequence and entry.value != .absent) return error.InvalidSqlBackendResponse;
                if (entry.value == .sequence and entry.value.sequence.id != ids[ordinal]) return error.InvalidSqlBackendResponse;
                if (entry.value == .absent) {
                    if (entry.namespace_id != null or entry.counter != null) return error.InvalidSqlBackendResponse;
                    continue;
                }
            } else if (!std.mem.eql(u8, entry.target.database, target.database) or !std.mem.eql(u8, entry.target.namespace, target.namespace) or
                !std.mem.eql(u8, entry.target.table, target.table)) return error.InvalidSqlBackendResponse;
            const missing = entry.value == .missing_database or entry.value == .missing_namespace;
            if (missing != (entry.namespace_id == null) or entry.namespace_id == 0) return error.InvalidSqlBackendResponse;
            if (entry.value == .sequence) {
                const definition = entry.value.sequence.definition() catch return error.InvalidSqlBackendResponse;
                if (request.include_counters) {
                    const state = entry.counter orelse return error.InvalidSqlBackendResponse;
                    state.validate(definition) catch return error.InvalidSqlBackendResponse;
                } else if (entry.counter != null) return error.InvalidSqlBackendResponse;
            } else if (entry.counter != null) return error.InvalidSqlBackendResponse;
        }
        if (self.retirement) |retirement| {
            if (!request.prepare_retirement or self.entries.len != 1 or self.entries[0].value != .sequence or retirement.database_id == 0 or
                retirement.dependents.len > 128 or retirement.references.count > 128 * sources.system_catalog_column_identities.max_columns or
                (retirement.references.count != 0 and retirement.references.revision == 0) or
                (retirement.dependents.len == 0) != (retirement.references.count == 0)) return error.InvalidSqlBackendResponse;
            var previous: u64 = 0;
            var bytes: usize = 0;
            for (retirement.dependents) |table| {
                if (table.table_id <= previous) return error.InvalidSqlBackendResponse;
                previous = table.table_id;
                bytes +|= table.schema_json.len;
                bytes +|= table.read_schema_json.len;
                bytes +|= table.indexes_json.len;
                if (bytes > max_retirement_bytes) return error.SqlProgramLimitExceeded;
            }
            if (retirement.activation) |activation| {
                if (retirement.dependents.len == 0 or activation.metadata_group_id != self.metadata_group_id or activation.metadata_group_id == 0 or
                    !std.mem.eql(u8, &activation.metadata_incarnation, &std.fmt.bytesToHex(self.incarnation, .lower)) or
                    activation.ranges.len == 0 or activation.ranges.len > @import("../metadata/schema_validation_publication.zig").max_owners) return error.InvalidSqlBackendResponse;
                var ordinal: usize = 0;
                for (retirement.dependents) |table| {
                    const start = ordinal;
                    while (ordinal < activation.ranges.len and activation.ranges[ordinal].table_id == table.table_id) : (ordinal += 1) {}
                    if (ordinal == start) return error.InvalidSqlBackendResponse;
                    try @import("../metadata/table_manager.zig").validateCompleteKeyspaceRanges(activation.ranges[start..ordinal]);
                }
                if (ordinal != activation.ranges.len) return error.InvalidSqlBackendResponse;
            } else if (retirement.dependents.len != 0) return error.InvalidSqlBackendResponse;
        } else if (request.prepare_retirement and self.entries[0].value == .sequence) return error.InvalidSqlBackendResponse;
        if (request.owner) |wanted| {
            if (!std.mem.eql(u8, &wanted.guard.incarnation, &self.incarnation)) return error.InvalidSqlBackendResponse;
            if (self.owner) |value| {
                value.column.validate() catch return error.InvalidSqlBackendResponse;
                if (value.namespace_id == 0 or value.column.table_id != wanted.guard.owner.table_id or value.default_binding_fingerprint != null) return error.InvalidSqlBackendResponse;
            }
        } else if (request.column_default) |wanted| {
            if (self.owner) |value| {
                value.column.validate() catch return error.InvalidSqlBackendResponse;
                const fingerprint = value.default_binding_fingerprint orelse return error.InvalidSqlBackendResponse;
                if (value.namespace_id == 0 or value.column.table_id != wanted.scope.table_id or value.table_name == null or std.mem.allEqual(u8, &fingerprint, 0)) return error.InvalidSqlBackendResponse;
            }
        } else if (self.owner != null) return error.InvalidSqlBackendResponse;
    }
};

pub const Mutation = struct {
    admission: Admission,
    target: domain.Target,
    namespace_id: u64,
    incarnation: [16]u8,
    operation: union(enum) {
        create: struct { options: sequence.Options, if_not_exists: bool = false },
        alter: struct { expected: Identity, options: sequence.Options },
        rename: struct { expected: Identity, name: []const u8 },
        drop: struct { expected: ?Identity = null, if_exists: bool = false, cascade: bool = false },
    },
    /// Only explicit OWNED BY replacement needs a caller's authorized table
    /// guard. Retained dependencies are verified from the stored definition.
    owner_guard: ?domain.RelationMutationGuard = null,

    pub fn validate(self: Mutation) !void {
        try self.admission.validate();
        try self.target.validate();
        if (self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidCatalogMutation;
        const options: ?sequence.Options = switch (self.operation) {
            .create => |value| value.options,
            .alter => |value| blk: {
                try value.expected.validate();
                break :blk value.options;
            },
            .rename => |value| blk: {
                try value.expected.validate();
                try domain.validateTableName(value.name);
                break :blk null;
            },
            .drop => |value| blk: {
                if (value.expected) |expected| try expected.validate();
                if (!value.if_exists and value.expected == null) return error.InvalidCatalogMutation;
                break :blk null;
            },
        };
        const owner: ?sequence.OwnedBy = if (options) |value| if (value.owned_by == .column) value.owned_by.column else null else null;
        if (owner) |binding| {
            try binding.validate();
            const guard = self.owner_guard orelse return error.InvalidCatalogMutation;
            try guard.validate();
            if (guard.owner.kind != .table or guard.owner.table_id != binding.table_id or
                guard.constraint_name_admission != null or
                !std.mem.eql(u8, guard.target.database, self.target.database) or
                !std.mem.eql(u8, guard.target.namespace, self.target.namespace) or
                !std.mem.eql(u8, &guard.incarnation, &self.incarnation)) return error.InvalidCatalogMutation;
        } else if (self.owner_guard != null) return error.InvalidCatalogMutation;
    }
};

pub fn commandDigest(a: std.mem.Allocator, mutation: Mutation) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, command(mutation), .{});
    defer a.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}

/// Reverse ownership edges share the metadata transaction and snapshot. IDs,
/// not column names or layout ordinals, survive table/attribute renaming.
pub fn ownerPrefix(buffer: []u8, group: u64, table_id: u64) ![]const u8 {
    if (group == 0 or table_id == 0) return error.InvalidSequenceDefinition;
    const prefix = try std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sequence-owner:", .{group});
    if (buffer.len - prefix.len < 8) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], table_id, .big);
    return buffer[0 .. prefix.len + 8];
}

pub fn ownerKey(buffer: []u8, group: u64, owner: sequence.OwnedBy, id: u64) ![]const u8 {
    if (id == 0) return error.InvalidSequenceDefinition;
    try owner.validate();
    const prefix = try ownerPrefix(buffer, group, owner.table_id);
    if (buffer.len - prefix.len < 16) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], owner.column_id, .big);
    std.mem.writeInt(u64, buffer[prefix.len + 8 ..][0..8], id, .big);
    return buffer[0 .. prefix.len + 16];
}

pub const Ownership = struct { owner: sequence.OwnedBy, sequence_id: u64 };

/// Decode only the selected table's exact reverse edge. Malformed keys in an
/// ownership prefix are corruption, never permission to skip a dependency.
pub fn ownershipFromKey(bytes: []const u8, group: u64, table: u64) !Ownership {
    var buffer: [192]u8 = undefined;
    const prefix = try ownerPrefix(&buffer, group, table);
    if (!std.mem.startsWith(u8, bytes, prefix) or bytes.len != prefix.len + 16) return error.InvalidSequenceState;
    const column = std.mem.readInt(u64, bytes[prefix.len..][0..8], .big);
    const id = std.mem.readInt(u64, bytes[prefix.len + 8 ..][0..8], .big);
    if (column == 0 or id == 0) return error.InvalidSequenceState;
    return .{ .owner = .{ .table_id = table, .column_id = column }, .sequence_id = id };
}

test "sequence catalog lookup responses preserve requested names and cannot mint an unrequested owner" {
    const request: SnapshotRequest = .{ .targets = &.{.{ .table = "counter" }} };
    var entries = [_]Snapshot.Entry{.{ .target = .{ .table = "counter" }, .namespace_id = 2, .value = .absent }};
    var value: Snapshot = .{ .revision = 0, .head = 0, .incarnation = @splat(7), .entries = &entries, .owner = null };
    try value.validate(request);
    value.owner = .{ .namespace_id = 2, .column = .{ .table_id = 17, .column_id = 1 } };
    try std.testing.expectError(error.InvalidSqlBackendResponse, value.validate(request));
    value.owner = null;
    entries[0].target.table = "other";
    try std.testing.expectError(error.InvalidSqlBackendResponse, value.validate(request));
    entries[0].target.table = "counter";
    entries[0].namespace_id = null;
    try std.testing.expectError(error.InvalidSqlBackendResponse, value.validate(request));
    entries[0].value = .missing_namespace;
    try value.validate(request);
    entries[0].namespace_id = 0;
    try std.testing.expectError(error.InvalidSqlBackendResponse, value.validate(request));
    var many: [max_lookup_targets + 1]domain.Target = undefined;
    for (&many) |*target| target.* = .{ .table = "counter" };
    try std.testing.expectError(error.InvalidCatalogMutation, (SnapshotRequest{ .targets = &many }).validate());
}

test "sequence catalog lookup counters require the exact requested definition generation" {
    var request: SnapshotRequest = .{ .targets = &.{.{ .table = "counter" }} };
    const record = try sequence.Record.create(.{ .start = 100, .restart = .{ .value = 7 } });
    var entries = [_]Snapshot.Entry{.{ .target = .{ .table = "counter" }, .namespace_id = 2, .value = .{ .sequence = try sequence.Descriptor.init(44, record.definition) } }};
    const snapshot: Snapshot = .{ .revision = 9, .head = 3, .incarnation = @splat(7), .entries = &entries, .owner = null };
    try snapshot.validate(request);
    entries[0].counter = record.state;
    try std.testing.expectError(error.InvalidSqlBackendResponse, snapshot.validate(request));
    request.include_counters = true;
    try snapshot.validate(request);
    entries[0].counter.?.generation += 1;
    try std.testing.expectError(error.InvalidSqlBackendResponse, snapshot.validate(request));
    entries[0].counter = null;
    try std.testing.expectError(error.InvalidSqlBackendResponse, snapshot.validate(request));
    entries[0].counter = record.state;
    entries[0].value = .absent;
    try std.testing.expectError(error.InvalidSqlBackendResponse, snapshot.validate(request));
}

test "sequence catalog ownership keys fence group table and exact stable attribute identity" {
    var buffer: [192]u8 = undefined;
    const bytes = try ownerKey(&buffer, 3, .{ .table_id = 17, .column_id = 29 }, 41);
    try std.testing.expectEqualDeep(Ownership{ .owner = .{ .table_id = 17, .column_id = 29 }, .sequence_id = 41 }, try ownershipFromKey(bytes, 3, 17));
    try std.testing.expectError(error.InvalidSequenceState, ownershipFromKey(bytes, 4, 17));
    try std.testing.expectError(error.InvalidSequenceState, ownershipFromKey(bytes, 3, 18));
    try std.testing.expectError(error.InvalidSequenceState, ownershipFromKey(bytes[0 .. bytes.len - 1], 3, 17));
    const zero_column = try ownerKey(&buffer, 3, .{ .table_id = 17, .column_id = 29 }, 41);
    @memset(buffer[zero_column.len - 16 ..][0..8], 0);
    try std.testing.expectError(error.InvalidSequenceState, ownershipFromKey(zero_column, 3, 17));
}

pub fn command(mutation: Mutation) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{
        .version = 3,
        // This version uses object/namespace guards, not a global revision CAS.
        .expected_revision = 0,
        .mutation = .{
            .kind = .sequence,
            .action = switch (mutation.operation) {
                .create => .create,
                .alter => .alter,
                .rename => .rename,
                .drop => .drop,
            },
            .database = mutation.target.database,
            .namespace = mutation.target.namespace,
            .name = mutation.target.table,
        },
        .sql_sequence = mutation,
    };
}
