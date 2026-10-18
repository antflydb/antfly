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

//! Private, owned view preparation. This is not a public catalog mutation or
//! an execution credential. Namespace/dependency admission and fresh owner/RLS
//! delegation must be implemented before exposing stored views to execution.
const std = @import("std");
const ast = @import("ast.zig");
const compiler = @import("compiler.zig");
const catalog = @import("catalog.zig");
const describe = @import("describe.zig");
const scalar = @import("scalar.zig");
const Budget = @import("memory_budget.zig");
const A = std.mem.Allocator;
test {
    _ = @import("../system_catalog/column_identities.zig");
}
pub const max_query_bytes = 64 * 1024;
pub const max_sources = 64;
pub const max_columns = 1024;

/// Populated by trusted identity admission, never by SQL text or a client
/// definition payload. Names are display/lookup hints, not reusable authority.
pub const Principal = struct { name: []const u8, instance_id: [16]u8 };
pub const Owner = union(enum) { auth_disabled, user: Principal, role: Principal };
pub const Security = enum { owner, invoker };
pub const Output = struct { name: []const u8, type: ast.ColumnType, element_type: ?@import("array_value.zig").ElementType = null, numeric_modifier: ?scalar.NumericModifier = null };
/// Names are resolved to durable attribute IDs by catalog publication in the
/// same transaction as source-epoch admission; they are not execution identity.
pub const ColumnDependency = struct {
    name: []const u8,
    signature: [32]u8,
    /// Creation-time order and hiddenness reconstruct a captured star even
    /// after compatible source-column additions or reordering.
    ordinal: u32,
    virtual: bool,
};
pub const Source = struct {
    reference: ast.Name,
    database: []const u8,
    namespace: []const u8,
    name: []const u8,
    physical_name: []const u8,
    table_id: u64,
    /// Publication guards at preparation, not permanent schema-change bans.
    schema_version: u32,
    schema_digest: [32]u8,
    columns: []const ColumnDependency = &.{},
};
pub const Definition = struct {
    format: u8,
    query: []const u8,
    owner: Owner,
    security: Security,
    security_barrier: bool,
    catalog_revision: ?u64,
    setting_epoch: ?u64,
    columns: []const Output,
    sources: []const Source,

    pub fn encode(self: Definition, a: A) ![]u8 {
        return std.json.Stringify.valueAlloc(a, self, .{});
    }

    pub fn digest(self: Definition, a: A) ![32]u8 {
        const bytes = try self.encode(a);
        defer a.free(bytes);
        var result: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
        return result;
    }
};
pub const ResolvedSource = struct { table: catalog.Table, schema_digest: [32]u8 };
/// Resolve only referenced names against one authorized metadata snapshot.
/// This is definition lookup (source-schema USAGE), not runtime source SELECT
/// admission. PostgreSQL permits creating a view without source-table SELECT.
/// Returned metadata must survive preparation (or be allocated in `a`). There
/// is deliberately no scan, snapshot acquisition or mutation capability here.
pub const Resolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (*anyopaque, A, ast.Name) anyerror!ResolvedSource,
    checkpoint: *const fn (*anyopaque) anyerror!void,
};
pub const Options = struct {
    owner: Owner,
    security: Security = .owner,
    security_barrier: bool = false,
    columns: []const []const u8 = &.{},
    settings: ?*const @import("setting_catalog.zig").View = null,
    memory_bytes: usize = 16 * 1024 * 1024,
};
pub const Owned = struct {
    backing: A,
    budget: *Budget,
    arena: std.heap.ArenaAllocator,
    value: Definition,

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self.budget);
        self.* = undefined;
    }
};

fn nameValid(name: []const u8) bool {
    return name.len != 0 and name.len <= 128 and std.mem.indexOfScalar(u8, name, 0) == null and std.unicode.utf8ValidateSlice(name);
}
fn principalValid(principal: Principal) bool {
    return nameValid(principal.name) and !std.mem.allEqual(u8, &principal.instance_id, 0);
}
fn cloneOwner(a: A, owner: Owner) !Owner {
    return switch (owner) {
        .auth_disabled => .auth_disabled,
        .user, .role => |principal| blk: {
            if (!principalValid(principal)) return error.InvalidSqlViewDefinition;
            const owned: Principal = .{ .name = try a.dupe(u8, principal.name), .instance_id = principal.instance_id };
            break :blk if (owner == .user) .{ .user = owned } else .{ .role = owned };
        },
    };
}
fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    return if (left) |value| if (right) |other| std.mem.eql(u8, value, other) else false else right == null;
}
fn sameName(left: ast.Name, right: ast.Name) bool {
    return optionalEqual(left.database, right.database) and optionalEqual(left.namespace, right.namespace) and std.mem.eql(u8, left.table, right.table);
}
fn cloneName(a: A, name: ast.Name) !ast.Name {
    return .{ .database = if (name.database) |value| try a.dupe(u8, value) else null, .namespace = if (name.namespace) |value| try a.dupe(u8, value) else null, .table = try a.dupe(u8, name.table) };
}
fn cloneSource(a: A, source: Source) !Source {
    var result = source;
    result.reference = try cloneName(a, source.reference);
    result.database = try a.dupe(u8, source.database);
    result.namespace = try a.dupe(u8, source.namespace);
    result.name = try a.dupe(u8, source.name);
    result.physical_name = try a.dupe(u8, source.physical_name);
    const columns = try a.dupe(ColumnDependency, source.columns);
    for (columns) |*column| column.name = try a.dupe(u8, column.name);
    result.columns = columns;
    return result;
}
fn lessSource(_: void, left: Source, right: Source) bool {
    for ([_]?[]const u8{ left.reference.database, left.reference.namespace, left.reference.table }, [_]?[]const u8{ right.reference.database, right.reference.namespace, right.reference.table }) |a, b| {
        if (a == null or b == null) {
            if (a != null or b != null) return a == null;
            continue;
        }
        switch (std.mem.order(u8, a.?, b.?)) {
            .lt => return true,
            .gt => return false,
            .eq => {},
        }
    }
    return false;
}
const Preparation = struct {
    resolver: Resolver,
    sources: std.ArrayList(Source) = .empty,
    tables: std.ArrayList(catalog.Table) = .empty,
    revision: ?u64 = null,

    fn resolve(raw: *anyopaque, a: A, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *Preparation = @ptrCast(@alignCast(raw));
        try self.resolver.checkpoint(self.resolver.ptr);
        if (action != .read) return error.InvalidSqlViewDefinition;
        for (self.sources.items, self.tables.items) |source, table| if (sameName(source.reference, name)) return table;
        if (self.sources.items.len == max_sources) return error.SqlProgramLimitExceeded;
        const resolved = try self.resolver.resolve(self.resolver.ptr, a, name);
        const table = resolved.table;
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        if (table.id == 0 or table.physical_name.len == 0 or !nameValid(scope.database) or !nameValid(scope.namespace) or !nameValid(scope.name) or std.mem.allEqual(u8, &resolved.schema_digest, 0)) return error.InvalidSqlBackendResponse;
        if (self.revision) |revision| {
            if (revision != scope.revision) return error.CatalogGenerationChanged;
        } else self.revision = scope.revision;
        // Different lexical qualifications can identify the same source. They
        // retain separate SQL references but cannot capture conflicting cuts.
        for (self.sources.items) |prior| if (prior.table_id == table.id) {
            if (prior.schema_version != table.schema_version or
                !std.mem.eql(u8, &prior.schema_digest, &resolved.schema_digest) or
                !std.mem.eql(u8, prior.physical_name, table.physical_name) or
                !std.mem.eql(u8, prior.database, scope.database) or
                !std.mem.eql(u8, prior.namespace, scope.namespace) or
                !std.mem.eql(u8, prior.name, scope.name)) return error.InvalidSqlBackendResponse;
        };
        try self.sources.append(a, .{ .reference = try cloneName(a, name), .database = try a.dupe(u8, scope.database), .namespace = try a.dupe(u8, scope.namespace), .name = try a.dupe(u8, scope.name), .physical_name = try a.dupe(u8, table.physical_name), .table_id = table.id, .schema_version = table.schema_version, .schema_digest = resolved.schema_digest });
        try self.tables.append(a, table);
        return table;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *Preparation = @ptrCast(@alignCast(raw));
        try self.resolver.checkpoint(self.resolver.ptr);
    }
    fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.InvalidSqlViewDefinition;
    }
    fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.InvalidSqlViewDefinition;
    }
};

pub fn prepare(backing: A, resolver: Resolver, query: []const u8, options: Options) !Owned {
    return prepareOwned(backing, resolver, query, options, null);
}

/// Trusted compiler intent, not deserialized public JSON. Reuse its SELECT
/// tree; do not parse the CREATE VIEW body again during ingress preparation.
pub fn prepareDdl(backing: A, resolver: Resolver, ddl: ast.CatalogDdl, options: Options) !Owned {
    if (ddl.kind != .view or ddl.action != .create) return error.InvalidSqlViewDefinition;
    const definition = ddl.view_definition orelse return error.InvalidSqlViewDefinition;
    var effective = options;
    effective.columns = definition.columns;
    effective.security = if (definition.security_invoker) .invoker else .owner;
    effective.security_barrier = definition.security_barrier;
    return prepareOwned(backing, resolver, definition.query_sql, effective, definition.query);
}

fn prepareOwned(backing: A, resolver: Resolver, query: []const u8, options: Options, parsed: ?*const ast.Select) !Owned {
    if (query.len == 0) return error.InvalidSqlViewDefinition;
    if (query.len > max_query_bytes or options.columns.len > max_columns) return error.SqlProgramLimitExceeded;
    const budget = try backing.create(Budget);
    errdefer backing.destroy(budget);
    budget.* = .{ .backing = backing, .limit = options.memory_bytes };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    errdefer arena.deinit();
    const value = prepareValue(budget.allocator(), arena.allocator(), resolver, query, options, parsed) catch |err| {
        if (err == error.OutOfMemory and budget.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    // Move the arena only after every allocation and inner deferred cleanup.
    return .{ .backing = backing, .budget = budget, .arena = arena, .value = value };
}
fn prepareValue(scratch_backing: A, output: A, resolver: Resolver, query: []const u8, options: Options, parsed: ?*const ast.Select) !Definition {
    // Compiler/binder regions must not stay resident with a saved definition.
    // Both temporary and retained capacity compete for the same admission.
    var scratch = std.heap.ArenaAllocator.init(scratch_backing);
    defer scratch.deinit();
    const a = scratch.allocator();
    try resolver.checkpoint(resolver.ptr);
    const owner = try cloneOwner(output, options.owner);
    var compiled: ?compiler.Compiled = null;
    defer if (compiled) |*value| value.deinit();
    const statement = if (parsed) |value| value.* else blk: {
        compiled = try compiler.compile(a, query, .{ .max_bytes = max_query_bytes, .max_nodes = 4096, .max_depth = 32 });
        if (compiled.?.statement != .select or compiled.?.parameter_count != 0) return error.InvalidSqlViewDefinition;
        break :blk compiled.?.statement.select;
    };
    var preparation: Preparation = .{ .resolver = resolver };
    const backend: catalog.Backend = .{ .ptr = &preparation, .settings_view = options.settings, .vtable = &.{ .resolve = Preparation.resolve, .scan = Preparation.scan, .mutate = Preparation.mutate, .checkpoint = Preparation.checkpoint } };
    const relations = @import("relation_binding.zig");
    const relation = try relations.bindDefinition(a, backend, statement);
    var adapter: relations.ResolveAdapter = .{ .backend = backend, .table = relation.table };
    const lowered: compiler.Compiled = .{ .arena = undefined, .statement = .{ .select = relation.statement }, .parameter_count = 0 };
    const bound = try describe.bind(a, adapter.iface(), &lowered, &.{});
    if (bound.columns.len == 0 or bound.columns.len > max_columns or options.columns.len > bound.columns.len) return error.InvalidSqlViewDefinition;
    const columns = try output.alloc(Output, bound.columns.len);
    for (bound.columns, columns, 0..) |column, *out, index| {
        if (index % 64 == 0) try resolver.checkpoint(resolver.ptr);
        const name = if (index < options.columns.len) options.columns[index] else column.name;
        if (!nameValid(name)) return error.InvalidSqlViewDefinition;
        for (columns[0..index]) |prior| if (std.mem.eql(u8, prior.name, name)) return error.DuplicateSqlColumn;
        out.* = .{ .name = try output.dupe(u8, name), .type = column.type, .element_type = scalar.columnElementType(column.type, column.element_type), .numeric_modifier = column.numeric_modifier };
    }
    const sources = try output.alloc(Source, preparation.sources.items.len);
    for (preparation.sources.items, sources) |source, *out| {
        out.* = try cloneSource(output, source);
        var unique: std.StringHashMapUnmanaged(ColumnDependency) = .empty;
        for (relation.column_dependencies) |dependency| {
            if (dependency.table_id != source.table_id) continue;
            const signature = try dependency.column.identitySignature(a);
            const entry = try unique.getOrPut(a, dependency.column.name);
            if (entry.found_existing) {
                if (!std.mem.eql(u8, &entry.value_ptr.signature, &signature) or entry.value_ptr.ordinal != dependency.ordinal or entry.value_ptr.virtual != dependency.virtual) return error.InvalidSqlBackendResponse;
            } else entry.value_ptr.* = .{ .name = dependency.column.name, .signature = signature, .ordinal = dependency.ordinal, .virtual = dependency.virtual };
        }
        const dependencies = try output.alloc(ColumnDependency, unique.count());
        var iterator = unique.iterator();
        for (dependencies) |*dependency| {
            const entry = iterator.next().?;
            dependency.* = entry.value_ptr.*;
            dependency.name = try output.dupe(u8, entry.key_ptr.*);
        }
        std.mem.sort(ColumnDependency, dependencies, {}, struct {
            fn less(_: void, left: ColumnDependency, right: ColumnDependency) bool {
                return std.mem.lessThan(u8, left.name, right.name);
            }
        }.less);
        out.columns = dependencies;
    }
    std.mem.sort(Source, sources, {}, lessSource);
    try resolver.checkpoint(resolver.ptr);
    return .{ .format = 1, .query = try output.dupe(u8, query), .owner = owner, .security = options.security, .security_barrier = options.security_barrier, .catalog_revision = preparation.revision, .setting_epoch = if (options.settings) |settings| settings.epoch else null, .columns = columns, .sources = sources };
}

const TestResolver = struct {
    calls: usize = 0,
    checkpoints: usize = 0,
    cancel_at: ?usize = null,
    mixed_revision: bool = false,
    mixed_schema: bool = false,
    invalid_source: enum { none, scope, identity, digest } = .none,
    physical: []const u8 = "native-things",

    fn resolver(self: *TestResolver) Resolver {
        return .{ .ptr = self, .resolve = resolve, .checkpoint = checkpoint };
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *TestResolver = @ptrCast(@alignCast(raw));
        self.checkpoints += 1;
        if (self.cancel_at == self.checkpoints) return error.Canceled;
    }
    fn resolve(raw: *anyopaque, _: A, name: ast.Name) !ResolvedSource {
        const self: *TestResolver = @ptrCast(@alignCast(raw));
        self.calls += 1;
        return .{
            .schema_digest = @splat(if (self.invalid_source == .digest) 0 else 7),
            .table = .{
                .id = if (self.invalid_source == .identity) 0 else std.hash.Wyhash.hash(0, name.table) | 1,
                .physical_name = self.physical,
                .schema_version = if (self.mixed_schema and self.calls > 1) 4 else 3,
                .scope = if (self.invalid_source == .scope) null else .{
                    .database = "app",
                    .namespace = "public",
                    .name = name.table,
                    .revision = if (self.mixed_revision and self.calls > 1) 11 else 10,
                },
                .columns = &.{
                    .{ .name = "id", .path = "id", .type = .integer, .element_type = .int32 },
                    .{ .name = "items", .path = "items", .type = .array, .element_type = .int16 },
                    .{ .name = "amount", .path = "amount", .type = .number, .element_type = .numeric, .numeric_modifier = .{ .precision = 12, .scale = 3 } },
                },
            },
        };
    }
};

test "SQL view DDL preparation reuses compiled query and owns its exact security and output contract" {
    var resolver: TestResolver = .{};
    var compiled = try compiler.compile(std.testing.allocator, "CREATE OR REPLACE VIEW summary(key) WITH (security_invoker=true,security_barrier=true) AS SELECT id,amount FROM things", .{});
    var definition = try prepareDdl(std.testing.allocator, resolver.resolver(), compiled.statement.catalog_ddl, .{ .owner = .auth_disabled });
    compiled.deinit();
    defer definition.deinit();
    try std.testing.expectEqual(Security.invoker, definition.value.security);
    try std.testing.expect(definition.value.security_barrier);
    try std.testing.expectEqualStrings("key", definition.value.columns[0].name);
    try std.testing.expectEqualStrings("amount", definition.value.columns[1].name);
    try std.testing.expectEqualStrings("SELECT id,amount FROM things", definition.value.query);
    try std.testing.expectEqual(@as(usize, 2), definition.value.sources[0].columns.len);
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
}

test "SQL view DDL preparation cleans allocation failures without consuming compiler ownership" {
    const Check = struct {
        fn run(a: A, ddl: ast.CatalogDdl) !void {
            var resolver: TestResolver = .{};
            var prepared = try prepareDdl(a, resolver.resolver(), ddl, .{ .owner = .auth_disabled });
            defer prepared.deinit();
            const bytes = try prepared.value.encode(a);
            defer a.free(bytes);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "CREATE VIEW v(key) AS SELECT a.id,b.amount FROM things a JOIN things b ON a.id=b.id", .{});
    defer compiled.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{compiled.statement.catalog_ddl});
}

test "SQL view definition owns descriptors and resolves repeated sources once without readers" {
    var physical = "native-things".*;
    var principal = "alice".*;
    var query = "SELECT a.id, a.items, a.amount FROM things a JOIN things b ON a.id=b.id".*;
    var resolver: TestResolver = .{ .physical = &physical };
    var definition = try prepare(std.testing.allocator, resolver.resolver(), &query, .{
        .owner = .{ .user = .{ .name = &principal, .instance_id = @splat(1) } },
        .security = .invoker,
        .security_barrier = true,
        .columns = &.{"key"},
    });
    defer definition.deinit();
    @memset(&physical, 'x');
    @memset(&principal, 'x');
    @memset(&query, 'x');
    try std.testing.expectEqual(@as(usize, 1), resolver.calls);
    try std.testing.expectEqual(@as(usize, 1), definition.value.sources.len);
    try std.testing.expectEqualStrings("native-things", definition.value.sources[0].physical_name);
    try std.testing.expectEqualStrings("alice", definition.value.owner.user.name);
    try std.testing.expect(std.mem.startsWith(u8, definition.value.query, "SELECT"));
    try std.testing.expectEqual(@as(?u64, 10), definition.value.catalog_revision);
    try std.testing.expectEqual(Security.invoker, definition.value.security);
    try std.testing.expect(definition.value.security_barrier);
    try std.testing.expectEqualStrings("key", definition.value.columns[0].name);
    try std.testing.expectEqual(.int32, definition.value.columns[0].element_type.?);
    try std.testing.expectEqual(.int16, definition.value.columns[1].element_type.?);
    try std.testing.expectEqual(@as(u16, 12), definition.value.columns[2].numeric_modifier.?.precision);
    try std.testing.expectEqual(@as(i16, 3), definition.value.columns[2].numeric_modifier.?.scale);
    _ = try definition.value.digest(std.testing.allocator);
    try std.testing.expect(definition.budget.peak > definition.budget.live);
}

test "SQL view definition binds derived array relations and canonical qualified dependencies" {
    var resolver: TestResolver = .{};
    var definition = try prepare(std.testing.allocator, resolver.resolver(), "WITH source AS (SELECT items FROM z) SELECT u.item FROM source s, LATERAL UNNEST(s.items) AS u(item) JOIN public.a a ON u.item=a.id", .{ .owner = .auth_disabled });
    defer definition.deinit();
    try std.testing.expectEqual(@as(usize, 2), resolver.calls);
    try std.testing.expectEqual(@as(usize, 2), definition.value.sources.len);
    try std.testing.expectEqualStrings("z", definition.value.sources[0].reference.table);
    try std.testing.expectEqualStrings("public", definition.value.sources[1].reference.namespace.?);
    try std.testing.expectEqualStrings("a", definition.value.sources[1].reference.table);
    try std.testing.expectEqual(.int16, definition.value.columns[0].element_type.?);
}

test "SQL view definition retains semantic column dependencies independently of scan demand" {
    const Case = struct { query: []const u8, names: []const []const u8 };
    for ([_]Case{
        .{ .query = "SELECT COUNT(*) FROM things", .names = &.{} },
        .{ .query = "SELECT COUNT(id) FROM things", .names = &.{"id"} },
        .{ .query = "SELECT 42 AS answer FROM things", .names = &.{} },
        .{ .query = "SELECT * FROM things", .names = &.{ "amount", "id", "items" } },
        .{ .query = "SELECT CASE WHEN FALSE THEN id ELSE 0 END AS value FROM things", .names = &.{"id"} },
        .{ .query = "SELECT a.id FROM things a JOIN things b ON a.amount=b.amount", .names = &.{ "amount", "id" } },
        .{ .query = "SELECT a.id FROM things a JOIN things b USING (items)", .names = &.{ "id", "items" } },
        .{ .query = "WITH x AS (SELECT id, amount FROM things) SELECT id FROM x", .names = &.{ "amount", "id" } },
        .{ .query = "WITH unused AS (SELECT amount FROM things) SELECT 42 AS answer FROM things", .names = &.{"amount"} },
        .{ .query = "SELECT a.id FROM things a WHERE EXISTS (SELECT 1 FROM things b WHERE b.amount=a.amount)", .names = &.{ "amount", "id" } },
        .{ .query = "SELECT _id FROM things", .names = &.{"_id"} },
        .{ .query = "SELECT id FROM things WHERE FALSE AND amount > 0", .names = &.{ "amount", "id" } },
        .{ .query = "SELECT SUM(amount) FILTER (WHERE id > 0) AS total FROM things", .names = &.{ "amount", "id" } },
        .{ .query = "SELECT id, ROW_NUMBER() OVER (ORDER BY amount) AS position FROM things", .names = &.{ "amount", "id" } },
        .{ .query = "SELECT id FROM things ORDER BY amount", .names = &.{ "amount", "id" } },
        .{ .query = "SELECT u.item FROM things t, LATERAL UNNEST(t.items) AS u(item)", .names = &.{"items"} },
    }) |case| {
        var resolver: TestResolver = .{};
        var definition = try prepare(std.testing.allocator, resolver.resolver(), case.query, .{ .owner = .auth_disabled });
        defer definition.deinit();
        try std.testing.expectEqual(@as(usize, 1), definition.value.sources.len);
        const columns = definition.value.sources[0].columns;
        try std.testing.expectEqual(case.names.len, columns.len);
        for (case.names, columns) |name, column| {
            try std.testing.expectEqualStrings(name, column.name);
            try std.testing.expect(!std.mem.allEqual(u8, &column.signature, 0));
            const ordinal: u32 = if (std.mem.eql(u8, name, "id")) 0 else if (std.mem.eql(u8, name, "items")) 1 else if (std.mem.eql(u8, name, "amount")) 2 else 3;
            try std.testing.expectEqual(ordinal, column.ordinal);
            try std.testing.expectEqual(std.mem.eql(u8, name, "_id"), column.virtual);
        }
    }
}

test "SQL view definition rejects writable parameterized and ambiguous output contracts" {
    var resolver: TestResolver = .{};
    for ([_][]const u8{ "SELECT $1", "DELETE FROM things", "INSERT INTO things (id) VALUES (1)" }) |query| {
        try std.testing.expectError(error.InvalidSqlViewDefinition, prepare(std.testing.allocator, resolver.resolver(), query, .{ .owner = .auth_disabled }));
    }
    try std.testing.expectEqual(@as(usize, 0), resolver.calls);
    try std.testing.expectError(error.InvalidSqlViewDefinition, prepare(std.testing.allocator, resolver.resolver(), "SELECT 1", .{ .owner = .{ .user = .{ .name = "alice", .instance_id = @splat(0) } } }));
    try std.testing.expectError(error.DuplicateSqlColumn, prepare(std.testing.allocator, resolver.resolver(), "SELECT 1 AS n, 2 AS n", .{ .owner = .auth_disabled }));
    try std.testing.expectError(error.DuplicateSqlColumn, prepare(std.testing.allocator, resolver.resolver(), "SELECT 1 AS a, 2 AS b", .{ .owner = .auth_disabled, .columns = &.{ "n", "n" } }));
    try std.testing.expectError(error.InvalidSqlViewDefinition, prepare(std.testing.allocator, resolver.resolver(), "SELECT 1", .{ .owner = .auth_disabled, .columns = &.{ "a", "b" } }));
}

test "SQL view definition fences mixed epochs invalid identities cancellation and memory admission" {
    var resolver: TestResolver = .{ .mixed_revision = true };
    try std.testing.expectError(error.CatalogGenerationChanged, prepare(std.testing.allocator, resolver.resolver(), "SELECT a.id FROM a JOIN b ON a.id=b.id", .{ .owner = .auth_disabled }));
    inline for (.{ .scope, .identity, .digest }) |invalid| {
        resolver = .{ .invalid_source = invalid };
        try std.testing.expectError(error.InvalidSqlBackendResponse, prepare(std.testing.allocator, resolver.resolver(), "SELECT id FROM a", .{ .owner = .auth_disabled }));
    }
    resolver = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, prepare(std.testing.allocator, resolver.resolver(), "SELECT id FROM a", .{ .owner = .auth_disabled, .memory_bytes = 1 }));
    resolver = .{};
    var valid = try prepare(std.testing.allocator, resolver.resolver(), "SELECT id FROM a", .{ .owner = .auth_disabled });
    valid.deinit();
    const checkpoints = resolver.checkpoints;
    for (1..checkpoints + 1) |at| {
        resolver = .{ .cancel_at = at };
        try std.testing.expectError(error.Canceled, prepare(std.testing.allocator, resolver.resolver(), "SELECT id FROM a", .{ .owner = .auth_disabled }));
    }
}

test "SQL view definition cleans every allocator failure before ownership transfer" {
    const Check = struct {
        fn run(a: A) !void {
            var resolver: TestResolver = .{};
            var definition = try prepare(a, resolver.resolver(), "SELECT a.items, a.amount FROM a JOIN b ON a.id=b.id", .{ .owner = .{ .role = .{ .name = "analyst", .instance_id = @splat(2) } } });
            defer definition.deinit();
            const encoded = try definition.value.encode(a);
            defer a.free(encoded);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "SQL view definition bounds unique point resolutions while preserving qualifier identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var resolver: TestResolver = .{};
    var preparation: Preparation = .{ .resolver = resolver.resolver() };
    for (0..max_sources) |index| {
        const name = try std.fmt.allocPrint(a, "t{d}", .{index});
        _ = try Preparation.resolve(&preparation, a, .{ .table = name }, .read);
    }
    _ = try Preparation.resolve(&preparation, a, .{ .table = "t0" }, .read);
    try std.testing.expectEqual(max_sources, resolver.calls);
    try std.testing.expectError(error.SqlProgramLimitExceeded, Preparation.resolve(&preparation, a, .{ .namespace = "public", .table = "t0" }, .read));
    try std.testing.expectEqual(max_sources, resolver.calls);
    try std.testing.expectError(error.InvalidSqlViewDefinition, Preparation.resolve(&preparation, a, .{ .table = "t0" }, .write));
}

test "SQL view definition output aliases and source ordering are deterministic" {
    const left: Source = .{ .reference = .{ .namespace = "public", .table = "a" }, .database = "app", .namespace = "public", .name = "a", .physical_name = "native-a", .table_id = 1, .schema_version = 1, .schema_digest = @splat(1) };
    const right: Source = .{ .reference = .{ .table = "z" }, .database = "app", .namespace = "public", .name = "z", .physical_name = "native-z", .table_id = 2, .schema_version = 1, .schema_digest = @splat(2) };
    var sources = [_]Source{ left, right };
    std.mem.sort(Source, &sources, {}, lessSource);
    try std.testing.expectEqualStrings("z", sources[0].reference.table);
    const value: Definition = .{ .format = 1, .query = "SELECT 1 AS value", .owner = .auth_disabled, .security = .owner, .security_barrier = false, .catalog_revision = 10, .setting_epoch = null, .columns = &.{.{ .name = "value", .type = .integer }}, .sources = &sources };
    const first = try value.digest(std.testing.allocator);
    var copy = value;
    try std.testing.expectEqual(first, try copy.digest(std.testing.allocator));
    copy.security = .invoker;
    try std.testing.expect(!std.mem.eql(u8, &first, &(try copy.digest(std.testing.allocator))));
    var resolver: TestResolver = .{};
    var constant = try prepare(std.testing.allocator, resolver.resolver(), "SELECT 1 AS value", .{ .owner = .auth_disabled });
    defer constant.deinit();
    try std.testing.expectEqual(@as(usize, 0), resolver.calls);
    try std.testing.expectEqual(@as(?u64, null), constant.value.catalog_revision);
}

test "SQL view definition preserves aliases of one identity without accepting mixed schema cuts" {
    var resolver: TestResolver = .{};
    const query = "SELECT a.id FROM a JOIN public.a b ON a.id=b.id";
    var definition = try prepare(std.testing.allocator, resolver.resolver(), query, .{ .owner = .auth_disabled });
    defer definition.deinit();
    try std.testing.expectEqual(@as(usize, 2), definition.value.sources.len);
    try std.testing.expectEqual(definition.value.sources[0].table_id, definition.value.sources[1].table_id);
    resolver = .{ .mixed_schema = true };
    try std.testing.expectError(error.InvalidSqlBackendResponse, prepare(std.testing.allocator, resolver.resolver(), query, .{ .owner = .auth_disabled }));
}
