// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Administrator SQL for durable policy drafts. No operation in this module
//! installs a bundle or changes the serving policy generation.
const std = @import("std");
const ast = @import("../sql/ast.zig");
const catalog = @import("../sql/catalog.zig");
const scalar = @import("../sql/scalar.zig");
const settings = @import("../sql/setting_catalog.zig");
const policies = @import("../system_catalog/policies.zig");
const domain = @import("../system_catalog/domain.zig");
const server_mod = @import("http_server.zig");
const operation = @import("operation.zig");

const SettingLoader = struct {
    server: *server_mod.ApiHttpServer,
    context: operation.RequestContext,

    fn load(raw: *anyopaque, alloc: std.mem.Allocator, scope: settings.Scope) !settings.RawSnapshot {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var context = self.context;
        context.setting_read_principal = scope.principal;
        const bytes = try self.server.source.systemCatalog(alloc, context, .{ .setting_snapshot = scope });
        defer alloc.free(bytes);
        return std.json.parseFromSliceLeaky(settings.RawSnapshot, alloc, bytes, .{ .allocate = .alloc_always });
    }
};

fn bindPredicate(alloc: std.mem.Allocator, expression: *const ast.Scalar, table: catalog.Table, view: *const settings.View) !scalar.Program {
    const columns = try alloc.alloc(scalar.Column, table.columns.len);
    for (table.columns, columns) |column, *out| out.* = .{ .name = column.name, .type = column.type, .nullable = column.nullable };
    return scalar.bindExpectedWithSettings(alloc, expression, columns, &.{}, .boolean, .{}, view);
}

fn commandScope(command: @FieldType(ast.PolicyDdl, "command")) policies.CommandScope {
    return switch (command) {
        .all => .{ .select = true, .insert = true, .update = true, .delete = true },
        .select => .{ .select = true },
        .insert => .{ .insert = true },
        .update => .{ .update = true },
        .delete => .{ .delete = true },
    };
}

pub fn execute(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, ddl: ast.PolicyDdl) !catalog.DdlOutcome {
    try context.ensureActive();
    const target: domain.Target = .{ .database = ddl.table.database orelse database, .namespace = ddl.table.namespace orelse namespace, .table = ddl.table.table };
    try target.validate();
    const logical = try target.resourceNameAlloc(alloc);
    defer alloc.free(logical);
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    if (identity) |authenticated| if (!(try server_mod.tablePermissionCurrentlyAllowed(authenticated, logical, .admin))) return error.Forbidden;
    if (!server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    // Publication is a separate topology-fenced operation; never turn a
    // definition mutation into a serving-generation switch implicitly.
    if (ddl.action == .enable or ddl.action == .disable) {
        try server.beginRowPolicyPublication(alloc, context, target, ddl.action == .enable);
        return .{ .mutation_outcome = .committed_pending };
    }

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const state_bytes = try server.source.systemCatalog(a, context, .snapshot);
    const state = try std.json.parseFromSliceLeaky(domain.State, a, state_bytes, .{ .allocate = .alloc_always });
    const resolved_bytes = try server.source.systemCatalog(a, context, .{ .resolve_many = .{ .targets = &.{target}, .include_query_definitions = true, .expected_revision = state.revision } });
    const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, resolved_bytes, .{ .allocate = .alloc_always });
    if (resolved.revision != state.revision or resolved.tables.len != 1) return error.CatalogGenerationChanged;
    const physical = resolved.tables[0] orelse return error.TableNotFound;
    const definition = physical.query_definition orelse return error.InvalidSqlBackendResponse;
    if (definition.table_id != physical.table_id) return error.InvalidSqlBackendResponse;
    var parsed_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(a, definition.schema_json);
    defer parsed_schema.deinit(a);
    const native = try @import("../schema/mod.zig").deriveRuntimeTableSchema(a, parsed_schema);
    if (native.storage_mode != .relational) return error.UnsupportedSqlShape;
    const layout = try @import("../storage/schema.zig").serializeSchema(a, native);
    var schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(layout, &schema_digest, .{});
    const table = try server.sql_schema_cache.resolve(server.sqlPlanCacheIo(), a, definition.schema_json, physical.table_id, physical.name);
    if (table.schema_version != native.version) return error.CatalogGenerationChanged;

    const prior: ?policies.Record = for (state.policies) |record| {
        if (record.table_id == physical.table_id and std.ascii.eqlIgnoreCase(record.name, ddl.name)) break record;
    } else null;
    const change: policies.Command = switch (ddl.action) {
        .enable, .disable => unreachable,
        .drop => blk: {
            const record = prior orelse {
                if (ddl.if_exists) return .{};
                return error.CatalogNotFound;
            };
            break :blk .{ .expected_revision = state.revision, .change = .{ .drop = .{ .id = record.id, .generation = record.generation, .table_id = physical.table_id } } };
        },
        .create, .alter => blk: {
            if (ddl.action == .create and prior != null) return error.CatalogAlreadyExists;
            if (ddl.action == .alter and prior == null) return error.CatalogNotFound;
            if (prior) |record| if (record.schema_version != table.schema_version or !std.mem.eql(u8, &record.schema_digest, &schema_digest)) return error.RowPolicyCatalogChanged;
            const principal = if (identity) |authenticated| server_mod.transactionPrincipal(authenticated) orelse authenticated.username else "internal_admin";
            var loader: SettingLoader = .{ .server = server, .context = context };
            var view = try settings.View.capture(a, .{ .ptr = &loader, .load = SettingLoader.load }, .{ .principal = principal, .database = target.database }, &.{});
            defer view.deinit();
            var using_program: ?scalar.Program = if (ddl.using) |expression| try bindPredicate(a, expression, table, &view) else null;
            defer if (using_program) |*program| program.deinit();
            var check_program: ?scalar.Program = if (ddl.with_check) |expression| try bindPredicate(a, expression, table, &view) else null;
            defer if (check_program) |*program| program.deinit();
            const commands = if (prior) |record| record.commands else commandScope(ddl.command);
            if (ddl.action == .alter) {
                if (ddl.using_specified and !commands.select and !commands.update and !commands.delete) return error.InvalidSqlSyntax;
                if (ddl.check_specified and !commands.insert and !commands.update) return error.InvalidSqlSyntax;
            }
            const record: policies.Record = .{
                .id = if (prior) |value| value.id else state.next_id,
                .generation = if (prior) |value| try std.math.add(u64, value.generation, 1) else 1,
                .table_id = physical.table_id,
                .schema_version = table.schema_version,
                .schema_digest = schema_digest,
                .name = ddl.name,
                .commands = commands,
                .roles = if (ddl.roles_specified) ddl.roles else if (prior) |value| value.roles else &.{"PUBLIC"},
                .permissive = if (prior) |value| value.permissive else ddl.permissive,
                .using = if (ddl.using_specified or ddl.action == .create) if (using_program) |program| .{ .instructions = program.instructions, .root = program.root } else null else prior.?.using,
                .with_check = if (ddl.check_specified or ddl.action == .create) if (check_program) |program| .{ .instructions = program.instructions, .root = program.root } else null else prior.?.with_check,
            };
            try record.validate(table, schema_digest, &view);
            break :blk .{ .expected_revision = state.revision, .change = .{ .put = record } };
        },
    };
    var internal_context = context;
    internal_context.setting_admin = true;
    _ = try server.source.systemCatalog(a, internal_context, .{ .policy_definition_mutate = change });
    return .{};
}

test "SQL policy DDL writes a durable draft only and never publishes it" {
    const Fixture = struct {
        const schema_json =
            \\{"version":7,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        mutation_calls: usize = 0,
        publication_calls: usize = 0,
        expected_enable: bool = true,

        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn systemCatalog(ptr: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: domain.Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return switch (call) {
                .snapshot => alloc.dupe(u8, "{\"revision\":7,\"next_id\":42}"),
                .resolve_many => |request| blk: {
                    if (request.include_query_definitions) try std.testing.expectEqual(@as(?u64, 7), request.expected_revision);
                    break :blk std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{ .revision = 7, .tables = &.{.{ .table_id = 17, .name = "table:immutable-17", .query_definition = .{ .table_id = 17, .schema_json = schema_json, .read_schema_json = "", .indexes_json = "{}" } }} }, .{});
                },
                .setting_snapshot => |scope| std.json.Stringify.valueAlloc(alloc, settings.RawSnapshot{ .scope = scope, .epoch = 1, .definitions = &.{} }, .{}),
                .policy_definition_mutate => |command| blk: {
                    try std.testing.expect(context.setting_admin);
                    try std.testing.expectEqual(@as(u64, 7), command.expected_revision);
                    try std.testing.expect(command.change == .put);
                    try std.testing.expectEqual(@as(u64, 42), command.change.put.id);
                    try std.testing.expectEqual(@as(u64, 1), command.change.put.generation);
                    try std.testing.expectEqual(@as(u64, 17), command.change.put.table_id);
                    try std.testing.expect(command.change.put.commands.select);
                    try std.testing.expect(command.change.put.using != null);
                    self.mutation_calls += 1;
                    break :blk alloc.dupe(u8, "{\"revision\":8}");
                },
                .policy_publication_begin => |begin| blk: {
                    try std.testing.expect(context.setting_admin);
                    try std.testing.expect(context.row_policy_install_authority);
                    try std.testing.expectEqual(@as(u64, 17), begin.table_id);
                    try std.testing.expectEqual(@as(u64, 7), begin.expected_revision);
                    try std.testing.expectEqual(self.expected_enable, begin.enable);
                    self.publication_calls += 1;
                    break :blk alloc.dupe(u8, "{\"revision\":8}");
                },
                else => error.UnexpectedCatalogCall,
            };
        }
    };

    const alloc = std.testing.allocator;
    var fixture: Fixture = .{};
    var server = server_mod.ApiHttpServer.init(alloc, .{}, .{ .ptr = &fixture, .vtable = &.{ .status = Fixture.status, .system_catalog = Fixture.systemCatalog, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    var create = try @import("../sql/compiler.zig").compile(alloc, "CREATE POLICY visible ON accounts FOR SELECT USING (id > 0)", .{});
    defer create.deinit();
    _ = try execute(&server, null, .{}, "default", "public", alloc, create.statement.policy_ddl);
    try std.testing.expectEqual(@as(usize, 1), fixture.mutation_calls);

    var missing_drop = try @import("../sql/compiler.zig").compile(alloc, "DROP POLICY IF EXISTS missing ON accounts", .{});
    defer missing_drop.deinit();
    _ = try execute(&server, null, .{}, "default", "public", alloc, missing_drop.statement.policy_ddl);
    try std.testing.expectEqual(@as(usize, 1), fixture.mutation_calls);

    var enable = try @import("../sql/compiler.zig").compile(alloc, "ALTER TABLE accounts ENABLE ROW LEVEL SECURITY", .{});
    defer enable.deinit();
    try std.testing.expectEqual(@as(?catalog.MutationOutcome, .committed_pending), (try execute(&server, null, .{}, "default", "public", alloc, enable.statement.policy_ddl)).mutation_outcome);
    try std.testing.expectEqual(@as(usize, 1), fixture.publication_calls);
    try std.testing.expectEqual(@as(usize, 1), fixture.mutation_calls);

    fixture.expected_enable = false;
    var disable = try @import("../sql/compiler.zig").compile(alloc, "ALTER TABLE accounts DISABLE ROW LEVEL SECURITY", .{});
    defer disable.deinit();
    try std.testing.expectEqual(@as(?catalog.MutationOutcome, .committed_pending), (try execute(&server, null, .{}, "default", "public", alloc, disable.statement.policy_ddl)).mutation_outcome);
    try std.testing.expectEqual(@as(usize, 2), fixture.publication_calls);
    try std.testing.expectEqual(@as(usize, 1), fixture.mutation_calls);

    // Missing authentication never becomes an implicit table administrator.
    server.cfg.auth_enabled = true;
    try std.testing.expectError(error.Forbidden, execute(&server, null, .{}, "default", "public", alloc, create.statement.policy_ddl));
    const read_only: server_mod.AuthenticatedIdentity = .{ .username = @constCast("reader") };
    try std.testing.expectError(error.Forbidden, execute(&server, read_only, .{}, "default", "public", alloc, enable.statement.policy_ddl));
    try std.testing.expectEqual(@as(usize, 1), fixture.mutation_calls);
    try std.testing.expectEqual(@as(usize, 2), fixture.publication_calls);
}
