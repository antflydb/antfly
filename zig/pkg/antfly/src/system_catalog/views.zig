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

//! Durable view definitions and attribute bindings. The catalog transaction
//! supplies current source states; this module grants no execution permission.
const std = @import("std");
const sources = @import("antfly_local_sources");
const definitions = sources.sql_view_definition;
const identities = sources.system_catalog_column_identities;
const catalog = sources.sql_catalog;
const domain = sources.system_catalog_domain;
const A = std.mem.Allocator;

pub const max_bytes = domain.max_command_bytes;

pub const Identity = struct { id: u64, generation: u32 };
/// Private catalog intent. Ingress derives the owner and bound definition;
/// neither SQL text nor a public request may supply an execution credential.
pub const Mutation = struct {
    target: domain.Target,
    operation: union(enum) {
        publish: struct { definition: definitions.Definition, replace: bool = false, expected: ?Identity = null },
        rename: struct { expected: Identity, name: []const u8 },
        drop: struct { expected: ?Identity = null, if_exists: bool = false, cascade: bool = false },
    },

    pub fn validate(self: Mutation) !void {
        try self.target.validate();
        switch (self.operation) {
            .publish => |value| {
                if (!value.replace and value.expected != null) return error.InvalidCatalogMutation;
                if (value.expected) |identity| try validateIdentity(identity);
            },
            .rename => |value| {
                try validateIdentity(value.expected);
                var target = self.target;
                target.table = value.name;
                try target.validate();
            },
            .drop => |value| if (value.expected) |identity| try validateIdentity(identity),
        }
    }
};
fn validateIdentity(identity: Identity) !void {
    if (identity.id == 0 or identity.generation == 0) return error.InvalidCatalogMutation;
}
pub const MutationResult = struct { revision: u64, identity: ?Identity, removed: usize = 0 };
pub const Request = struct { expected_revision: u64, mutation: Mutation };

pub fn command(request: Request) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{
        .version = 2,
        .expected_revision = request.expected_revision,
        .mutation = .{
            .kind = .view,
            .action = switch (request.mutation.operation) {
                .publish => .create,
                .rename => .rename,
                .drop => .drop,
            },
            .database = request.mutation.target.database,
            .namespace = request.mutation.target.namespace,
            .name = request.mutation.target.table,
        },
        .sql_view = request.mutation,
    };
}

fn bindingDigest(a: A, definition: definitions.Definition, columns: []const []const u64) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .definition = definition, .source_columns = columns }, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:sql-view-binding:v1\x00");
    hash.update(bytes);
    return hash.finalResult();
}
pub const Dependency = struct { kind: definitions.SourceKind, id: u64, column: u64 };
const catalog_prefix = "\x00\x00__metadata__:system_catalog:";
pub fn allGroupsPrefix() []const u8 {
    return catalog_prefix;
}
pub fn groupFromKey(bytes: []const u8) !?u64 {
    if (!std.mem.startsWith(u8, bytes, catalog_prefix)) return null;
    const tail = bytes[catalog_prefix.len..];
    const colon = std.mem.indexOfScalar(u8, tail, ':') orelse return null;
    const suffix = tail[colon + 1 ..];
    if (!std.mem.startsWith(u8, suffix, "record:view:") and !std.mem.startsWith(u8, suffix, "view_dependency:")) return null;
    if (colon == 0 or tail[0] == '0') return error.InvalidSqlViewRecord;
    for (tail[0..colon]) |byte| if (byte < '0' or byte > '9') return error.InvalidSqlViewRecord;
    return std.fmt.parseInt(u64, tail[0..colon], 10) catch return error.InvalidSqlViewRecord;
}
pub fn dependencyNamespacePrefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidSqlViewRecord;
    return std.fmt.bufPrint(buffer, catalog_prefix ++ "{d}:view_dependency:", .{group});
}

pub fn recordPrefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidSqlViewRecord;
    return std.fmt.bufPrint(buffer, catalog_prefix ++ "{d}:record:view:", .{group});
}
pub fn recordId(bytes: []const u8, group: u64) !?u64 {
    var buffer: [160]u8 = undefined;
    const prefix = try recordPrefix(&buffer, group);
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    const text = bytes[prefix.len..];
    if (text.len == 0 or text[0] == '0') return error.InvalidSqlViewRecord;
    for (text) |byte| if (byte < '0' or byte > '9') return error.InvalidSqlViewRecord;
    return std.fmt.parseInt(u64, text, 10) catch return error.InvalidSqlViewRecord;
}

/// Authoritative reverse edges live beside the catalog, in the same snapshot
/// and transaction. Column zero denotes relation existence, including COUNT(*).
pub fn dependencyPrefix(buffer: []u8, group: u64, kind: definitions.SourceKind, id: u64, column: ?u64) ![]const u8 {
    if (group == 0 or id == 0) return error.InvalidSqlViewRecord;
    const prefix = try dependencyNamespacePrefix(buffer, group);
    const width: usize = 9 + if (column != null) @as(usize, 8) else 0;
    if (buffer.len - prefix.len < width) return error.NoSpaceLeft;
    const tail = buffer[prefix.len..];
    tail[0] = @backingInt(kind);
    std.mem.writeInt(u64, tail[1..9], id, .big);
    if (column) |value| std.mem.writeInt(u64, tail[9..17], value, .big);
    return buffer[0 .. prefix.len + width];
}
pub fn dependencyKey(buffer: []u8, group: u64, dependency: Dependency, view: u64) ![]const u8 {
    if (view == 0) return error.InvalidSqlViewRecord;
    const prefix = try dependencyPrefix(buffer, group, dependency.kind, dependency.id, dependency.column);
    if (buffer.len - prefix.len < 8) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], view, .big);
    return buffer[0 .. prefix.len + 8];
}
pub const DependencyKey = struct { dependency: Dependency, view: u64 };
pub fn dependencyFromKey(bytes: []const u8, group: u64) !?DependencyKey {
    var buffer: [160]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, catalog_prefix ++ "{d}:view_dependency:", .{group});
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    const tail = bytes[prefix.len..];
    if (tail.len != 25) return error.InvalidSqlViewRecord;
    const kind = std.enums.fromInt(definitions.SourceKind, tail[0]) orelse return error.InvalidSqlViewRecord;
    const result: DependencyKey = .{
        .dependency = Dependency{ .kind = kind, .id = std.mem.readInt(u64, tail[1..9], .big), .column = std.mem.readInt(u64, tail[9..17], .big) },
        .view = std.mem.readInt(u64, tail[17..25], .big),
    };
    if (result.dependency.id == 0 or result.view == 0 or group == 0) return error.InvalidSqlViewRecord;
    return result;
}

pub fn firstDependent(txn: anytype, group: u64, kind: definitions.SourceKind, id: u64, column: ?u64) !?u64 {
    var buffer: [160]u8 = undefined;
    const prefix = try dependencyPrefix(&buffer, group, kind, id, column);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(prefix)) orelse return null;
    if (!std.mem.startsWith(u8, row.key, prefix)) return null;
    if (!std.mem.eql(u8, row.value, &.{1})) return error.InvalidSqlViewRecord;
    return ((try dependencyFromKey(row.key, group)) orelse return error.InvalidSqlViewRecord).view;
}
pub const Record = struct {
    format: u8,
    id: u64,
    generation: u32,
    definition: definitions.Definition,
    /// Attribute IDs are scoped by (source kind, source ID). Vector positions
    /// correspond to definition.sources and their captured semantic columns.
    source_columns: []const []const u64,
    /// Embedded output identities are not physical-table catalog rows.
    attributes: identities.State,

    pub fn validate(self: Record, a: A) !void {
        const definition = self.definition;
        if (self.format != 1 or self.id == 0 or self.generation == 0 or definition.format != 1 or
            definition.query.len == 0 or definition.query.len > definitions.max_query_bytes or
            !std.unicode.utf8ValidateSlice(definition.query) or
            definition.columns.len == 0 or definition.columns.len > definitions.max_columns or
            definition.sources.len > definitions.max_sources or
            self.source_columns.len != definition.sources.len)
            return error.InvalidSqlViewRecord;
        switch (definition.owner) {
            .auth_disabled => {},
            .user, .role => |principal| {
                if (principal.name.len == 0 or principal.name.len > 128 or
                    !std.unicode.utf8ValidateSlice(principal.name) or std.mem.indexOfScalar(u8, principal.name, 0) != null)
                    return error.InvalidSqlViewRecord;
                if (std.mem.allEqual(u8, &principal.instance_id, 0)) return error.InvalidSqlViewRecord;
            },
        }
        try self.attributes.validate(a);
        if (self.attributes.table_id != self.id or self.attributes.epoch.version != self.generation or
            self.attributes.columns.len != definition.columns.len or
            !std.mem.eql(u8, &self.attributes.epoch.digest, &try bindingDigest(a, definition, self.source_columns)))
            return error.InvalidSqlViewRecord;
        for (definition.columns, self.attributes.columns) |column, attribute| {
            if (!std.mem.eql(u8, column.name, attribute.name) or
                !std.mem.eql(u8, &attribute.signature, &try outputSignature(a, column)))
                return error.InvalidSqlViewRecord;
        }
        var references: std.StringHashMapUnmanaged(void) = .empty;
        defer {
            var keys = references.keyIterator();
            while (keys.next()) |key| a.free(key.*);
            references.deinit(a);
        }
        for (definition.sources, self.source_columns) |source, columns| {
            if (source.table_id == 0 or (source.kind == .view and source.schema_version == 0) or
                std.mem.allEqual(u8, &source.schema_digest, 0) or
                source.columns.len > identities.max_columns or
                columns.len != source.columns.len or
                (source.kind == .table and source.physical_name.len == 0))
                return error.InvalidSqlViewRecord;
            domain.validateName(source.database) catch return error.InvalidSqlViewRecord;
            domain.validateName(source.namespace) catch return error.InvalidSqlViewRecord;
            domain.validateTableName(source.name) catch return error.InvalidSqlViewRecord;
            domain.validateTableName(source.reference.table) catch return error.InvalidSqlViewRecord;
            if (source.reference.database) |name| domain.validateName(name) catch return error.InvalidSqlViewRecord;
            if (source.reference.namespace) |name| domain.validateName(name) catch return error.InvalidSqlViewRecord;
            const key = try std.json.Stringify.valueAlloc(a, source.reference, .{});
            if (references.contains(key)) {
                a.free(key);
                return error.InvalidSqlViewRecord;
            }
            references.put(a, key, {}) catch |err| {
                a.free(key);
                return err;
            };
            var ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
            defer ids.deinit(a);
            var names: std.StringHashMapUnmanaged(void) = .empty;
            defer names.deinit(a);
            var ordinals: std.AutoHashMapUnmanaged(u32, void) = .empty;
            defer ordinals.deinit(a);
            for (source.columns, columns) |column, id| {
                if (id == 0 or std.mem.allEqual(u8, &column.signature, 0)) return error.InvalidSqlViewRecord;
                domain.validateTableName(column.name) catch return error.InvalidSqlViewRecord;
                if ((try ids.getOrPut(a, id)).found_existing or
                    (try names.getOrPut(a, column.name)).found_existing or
                    (try ordinals.getOrPut(a, column.ordinal)).found_existing)
                    return error.InvalidSqlViewRecord;
            }
        }
    }

    /// Publication admits the exact captured epoch. Execution/restore instead
    /// checks durable IDs and domains, permitting unrelated additions/renames.
    pub fn requireSource(self: Record, index: usize, state: identities.State, publication: bool) !void {
        if (index >= self.definition.sources.len or index >= self.source_columns.len) return error.InvalidSqlViewRecord;
        const source = self.definition.sources[index];
        if (state.table_id != source.table_id or self.source_columns[index].len != source.columns.len) return error.InvalidSqlViewRecord;
        if (publication and (state.epoch.version != source.schema_version or !std.mem.eql(u8, &state.epoch.digest, &source.schema_digest))) return error.CatalogGenerationChanged;
        for (source.columns, self.source_columns[index]) |column, id| _ = try state.requireDependency(id, column.signature);
    }

    pub fn dependencies(self: Record, a: A) ![]Dependency {
        var seen: std.AutoHashMapUnmanaged(Dependency, void) = .empty;
        defer seen.deinit(a);
        for (self.definition.sources, self.source_columns) |source, columns| {
            try seen.put(a, .{ .kind = source.kind, .id = source.table_id, .column = 0 }, {});
            for (columns) |column| try seen.put(a, .{ .kind = source.kind, .id = source.table_id, .column = column }, {});
        }
        const result = try a.alloc(Dependency, seen.count());
        var keys = seen.keyIterator();
        for (result) |*dependency| dependency.* = keys.next().?.*;
        std.mem.sort(Dependency, result, {}, struct {
            fn less(_: void, left: Dependency, right: Dependency) bool {
                if (left.kind != right.kind) return @backingInt(left.kind) < @backingInt(right.kind);
                if (left.id != right.id) return left.id < right.id;
                return left.column < right.column;
            }
        }.less);
        return result;
    }

    pub fn encode(self: Record, a: A) ![]u8 {
        try self.validate(a);
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
        return bytes;
    }
    pub fn decode(a: A, bytes: []const u8) !std.json.Parsed(Record) {
        if (bytes.len > max_bytes) return error.CatalogCommandTooLarge;
        var parsed = try std.json.parseFromSlice(Record, a, bytes, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try parsed.value.validate(a);
        const canonical = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(canonical);
        if (!std.mem.eql(u8, bytes, canonical)) return error.InvalidSqlViewRecord;
        return parsed;
    }
};

fn outputSignature(a: A, column: definitions.Output) ![32]u8 {
    return (catalog.Column{ .name = column.name, .path = column.name, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier }).identitySignature(a);
}

/// Borrowed input definitions/source vectors must survive serialization.
/// Only output attribute storage belongs to this move-only preparation.
pub const Prepared = struct {
    value: Record,
    output: identities.Owned,
    pub fn deinit(self: *Prepared) void {
        self.output.deinit();
        self.* = undefined;
    }
};

pub fn prepare(a: A, id: u64, definition_: definitions.Definition, previous: ?Record, columns: []const []const u64) !Prepared {
    var definition = definition_;
    const generation: u32 = if (previous) |prior| blk: {
        try prior.validate(a);
        if (prior.id != id) return error.CatalogGenerationChanged;
        if (definition.columns.len < prior.definition.columns.len) return error.SqlViewColumnDropped;
        for (prior.definition.columns, definition.columns[0..prior.definition.columns.len]) |before, after| {
            if (!std.mem.eql(u8, before.name, after.name)) return error.SqlViewColumnRename;
            if (!std.mem.eql(u8, &try outputSignature(a, before), &try outputSignature(a, after))) return error.SqlViewColumnTypeChanged;
        }
        // Replacement does not transfer ownership to the actor replacing it.
        definition.owner = prior.definition.owner;
        break :blk std.math.add(u32, prior.generation, 1) catch return error.SqlViewGenerationExhausted;
    } else 1;
    const declarations = try a.alloc(identities.Declaration, definition.columns.len);
    defer a.free(declarations);
    for (definition.columns, declarations) |column, *declaration| declaration.* = .{ .name = column.name, .signature = try outputSignature(a, column) };
    var output = try identities.prepare(a, id, if (previous) |prior| prior.attributes else null, .{ .version = generation, .digest = try bindingDigest(a, definition, columns) }, declarations, &.{});
    errdefer output.deinit();
    const record: Record = .{ .format = 1, .id = id, .generation = generation, .definition = definition, .source_columns = columns, .attributes = output.value };
    try record.validate(a);
    return .{ .value = record, .output = output };
}

/// Resolve names only during publication. Later readers use captured IDs.
pub fn bindSource(a: A, source: definitions.Source, state: identities.State) ![]const u64 {
    try state.validate(a);
    if (state.table_id != source.table_id or state.epoch.version != source.schema_version or !std.mem.eql(u8, &state.epoch.digest, &source.schema_digest)) return error.CatalogGenerationChanged;
    var names: std.StringHashMapUnmanaged(identities.Column) = .empty;
    defer names.deinit(a);
    try names.ensureTotalCapacity(a, @intCast(state.columns.len));
    for (state.columns) |column| names.putAssumeCapacityNoClobber(column.name, column);
    const result = try a.alloc(u64, source.columns.len);
    errdefer a.free(result);
    for (source.columns, result) |column, *id| {
        const found = names.get(column.name) orelse return error.SqlViewColumnDropped;
        if (!std.mem.eql(u8, &found.signature, &column.signature)) return error.SqlViewColumnTypeChanged;
        id.* = found.id;
    }
    return result;
}

fn testDefinition() definitions.Definition {
    return .{
        .format = 1,
        .query = "SELECT 1::INTEGER AS id",
        .owner = .{ .user = .{ .name = "alice@example.com", .instance_id = @splat(1) } },
        .security = .owner,
        .security_barrier = false,
        .catalog_revision = null,
        .setting_epoch = null,
        .columns = &.{.{ .name = "id", .type = .integer, .element_type = .int32 }},
        .sources = &.{},
    };
}

test "SQL view catalog replacement preserves owner output IDs and exact prefix domains" {
    const a = std.testing.allocator;
    var first = try prepare(a, 9, testDefinition(), null, &.{});
    defer first.deinit();
    var replacement = testDefinition();
    replacement.owner = .{ .role = .{ .name = "admin", .instance_id = @splat(2) } };
    replacement.columns = &.{
        .{ .name = "id", .type = .integer, .element_type = .int32 },
        .{ .name = "tags", .type = .array, .element_type = .text },
    };
    var second = try prepare(a, 9, replacement, first.value, &.{});
    defer second.deinit();
    try std.testing.expectEqual(@as(u32, 2), second.value.generation);
    try std.testing.expectEqualStrings("alice@example.com", second.value.definition.owner.user.name);
    try std.testing.expectEqual(first.value.attributes.columns[0].id, second.value.attributes.columns[0].id);
    try std.testing.expectEqual(@as(u64, 3), second.value.attributes.next_id);
    var invalid = replacement;
    invalid.columns = &.{.{ .name = "renamed", .type = .integer, .element_type = .int32 }};
    try std.testing.expectError(error.SqlViewColumnRename, prepare(a, 9, invalid, first.value, &.{}));
    invalid.columns = &.{.{ .name = "id", .type = .integer, .element_type = .int64 }};
    try std.testing.expectError(error.SqlViewColumnTypeChanged, prepare(a, 9, invalid, first.value, &.{}));
    try std.testing.expectError(error.SqlViewColumnDropped, prepare(a, 9, first.value.definition, second.value, &.{}));
    invalid = replacement;
    invalid.columns = &.{
        .{ .name = "id", .type = .integer, .element_type = .int32 },
        .{ .name = "tags", .type = .array, .element_type = .jsonb },
    };
    try std.testing.expectError(error.SqlViewColumnTypeChanged, prepare(a, 9, invalid, second.value, &.{}));
    const bytes = try second.value.encode(a);
    defer a.free(bytes);
    var decoded = try Record.decode(a, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 3), decoded.value.attributes.next_id);
    const noncanonical = try std.mem.concat(a, u8, &.{ " ", bytes });
    defer a.free(noncanonical);
    try std.testing.expectError(error.InvalidSqlViewRecord, Record.decode(a, noncanonical));
}

test "SQL view catalog source bindings follow IDs through rename but reject epoch substitution and reincarnation" {
    const a = std.testing.allocator;
    const signature = try outputSignature(a, .{ .name = "status", .type = .string });
    const declaration = identities.Declaration{ .name = "status", .signature = signature };
    var original = try identities.prepare(a, 7, null, .{ .version = 1, .digest = @splat(3) }, &.{declaration}, &.{});
    defer original.deinit();
    const source: definitions.Source = .{
        .reference = .{ .table = "rows" },
        .database = "default",
        .namespace = "public",
        .name = "rows",
        .physical_name = "table:7",
        .table_id = 7,
        .schema_version = 1,
        .schema_digest = @splat(3),
        .columns = &.{.{ .name = "status", .signature = signature, .ordinal = 0, .virtual = false }},
    };
    const columns = try bindSource(a, source, original.value);
    defer a.free(columns);
    var definition = testDefinition();
    definition.sources = &.{source};
    var view = try prepare(a, 9, definition, null, &.{columns});
    defer view.deinit();
    try view.value.requireSource(0, original.value, true);
    var renamed = try identities.prepare(a, 7, original.value, .{ .version = 2, .digest = @splat(4) }, &.{.{ .name = "state", .signature = signature }}, &.{.{ .from = "status", .to = "state" }});
    defer renamed.deinit();
    try view.value.requireSource(0, renamed.value, false);
    try std.testing.expectError(error.CatalogGenerationChanged, view.value.requireSource(0, renamed.value, true));
    var dropped = try identities.prepare(a, 7, renamed.value, .{ .version = 3, .digest = @splat(5) }, &.{}, &.{});
    defer dropped.deinit();
    var recreated = try identities.prepare(a, 7, dropped.value, .{ .version = 4, .digest = @splat(6) }, &.{declaration}, &.{});
    defer recreated.deinit();
    try std.testing.expectError(error.SqlViewColumnDropped, view.value.requireSource(0, recreated.value, false));
    try std.testing.expectError(error.CatalogGenerationChanged, bindSource(a, source, recreated.value));
}

test "SQL view catalog record preparation encoding and decode clean every allocator failure" {
    const Check = struct {
        fn run(a: A) !void {
            const signature = try outputSignature(a, .{ .name = "status", .type = .string });
            var state = try identities.prepare(a, 7, null, .{ .version = 1, .digest = @splat(3) }, &.{.{ .name = "status", .signature = signature }}, &.{});
            defer state.deinit();
            const source: definitions.Source = .{
                .reference = .{ .table = "rows" },
                .database = "default",
                .namespace = "public",
                .name = "rows",
                .physical_name = "rows",
                .table_id = 7,
                .schema_version = 1,
                .schema_digest = @splat(3),
                .columns = &.{.{ .name = "status", .signature = signature, .ordinal = 0, .virtual = false }},
            };
            const columns = try bindSource(a, source, state.value);
            defer a.free(columns);
            var definition = testDefinition();
            definition.query = "WITH unused AS (SELECT status FROM rows) SELECT 1::INTEGER AS id";
            definition.sources = &.{source};
            var prepared = try prepare(a, 9, definition, null, &.{columns});
            defer prepared.deinit();
            const bytes = try prepared.value.encode(a);
            defer a.free(bytes);
            var decoded = try Record.decode(a, bytes);
            defer decoded.deinit();
            const dependencies = try decoded.value.dependencies(a);
            defer a.free(dependencies);
            try std.testing.expectEqual(@as(usize, 2), dependencies.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL view catalog dependency keys distinguish source kinds columns and groups without prefix ambiguity" {
    for ([_]definitions.SourceKind{ .table, .view }) |kind| {
        var buffer: [160]u8 = undefined;
        const dependency: Dependency = .{ .kind = kind, .id = std.math.maxInt(u64), .column = 0 };
        const key = try dependencyKey(&buffer, 41, dependency, 99);
        const decoded = (try dependencyFromKey(key, 41)).?;
        try std.testing.expectEqual(dependency, decoded.dependency);
        try std.testing.expectEqual(@as(u64, 99), decoded.view);
        try std.testing.expectEqual(@as(?u64, 41), try groupFromKey(key));
        try std.testing.expect((try dependencyFromKey(key, 42)) == null);
        try std.testing.expectError(error.InvalidSqlViewRecord, dependencyFromKey(key[0 .. key.len - 1], 41));
        @memset(buffer[key.len - 8 .. key.len], 0);
        try std.testing.expectError(error.InvalidSqlViewRecord, dependencyFromKey(key, 41));
    }
    try std.testing.expectEqual(@as(?u64, 99), try recordId(catalog_prefix ++ "41:record:view:99", 41));
    try std.testing.expectError(error.InvalidSqlViewRecord, recordId(catalog_prefix ++ "41:record:view:099", 41));
    try std.testing.expectError(error.InvalidSqlViewRecord, recordId(catalog_prefix ++ "41:record:view:0", 41));
}
